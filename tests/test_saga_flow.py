"""Tests to verify SAGA completes all steps in one polling cycle."""
import time

import psycopg2
import pytest
import requests

ORDER_SERVICE_URL = "http://localhost:8001"
PORTAL_URL = "http://localhost:8080"
DB_URL = "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox"


class TestSAGAFlowCompleteness:
    """Verify SAGA runs all steps, not stuck at _can_proceed."""

    def test_new_order_starts_saga_flow(self):
        """A new order should start progressing through SAGA steps within 2 minutes."""
        order_id = f"SAGA-TEST-{int(time.time())}"
        resp = requests.post(
            f"{ORDER_SERVICE_URL}/api/v1/orders",
            json={
                "order_id": order_id,
                "items": [{"sku": "SKU-001", "qty": 1}],
                "delivery_address": "Test",
                "warehouse_id": "WH-MSK-S",
            },
            timeout=10,
        )
        assert resp.status_code == 201

        # Wait enough for first steps (inventory + payment) with realistic delays
        time.sleep(90)

        # Check order has progressed beyond 'created'
        resp = requests.get(f"{PORTAL_URL}/api/v1/orders/{order_id}", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        status = data.get("status")
        # Should be PROCESSING or COMPLETED (not stuck at 'created')
        assert status in ("PROCESSING", "COMPLETED"), f"Order stuck at {status}"

    def test_order_has_all_saga_events(self):
        """Order should have all expected SAGA events."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        # Find a recent completed order
        cur.execute(
            "SELECT id FROM orders WHERE status = 'COMPLETED' ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None, "No completed orders found"
        order_id = row[0]

        # Get all events for this order
        cur.execute(
            "SELECT event_type FROM events WHERE order_id = %s ORDER BY id",
            (order_id,),
        )
        event_types = [r[0] for r in cur.fetchall()]

        expected_events = [
            "order_created",
            "inventory_reserved",
            "payment_requested",
            "payment_succeeded",
            "picking_started",
            "picking_completed",
            "order_packed",
            "order_shipped",
            "delivery_assigned",
            "delivery_started",
            "delivery_completed",
            "order_completed",
        ]

        for expected in expected_events:
            assert expected in event_types, (
                f"Missing event '{expected}' for order {order_id}. Events: {event_types}"
            )

        cur.close()
        conn.close()

    def test_no_stuck_processing_orders(self):
        """No NEW orders should be stuck in PROCESSING status for > 30 seconds."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        # Only check orders created recently (within last 5 minutes) to avoid
        # counting old stuck orders from before the fix
        cur.execute(
            "SELECT COUNT(*) FROM orders "
            "WHERE status = 'PROCESSING' "
            "  AND created_at > NOW() - INTERVAL '5 minutes' "
            "  AND created_at < NOW() - INTERVAL '30 seconds'"
        )
        stuck = cur.fetchone()[0]
        assert stuck == 0, f"{stuck} NEW orders stuck in PROCESSING for > 30 seconds"

        cur.close()
        conn.close()

    def test_events_table_has_no_processed_without_continuation(self):
        """No NEW order_created should have processed=true without subsequent events."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        # Only check recent events (last 5 minutes) to avoid counting old stuck
        cur.execute(
            "SELECT COUNT(*) FROM events e "
            "WHERE e.event_type = 'order_created' "
            "  AND e.processed = true "
            "  AND e.created_at > NOW() - INTERVAL '5 minutes' "
            "  AND NOT EXISTS ("
            "      SELECT 1 FROM events e2 "
            "      WHERE e2.order_id = e.order_id "
            "        AND e2.event_type IN ('inventory_reserved', 'inventory_failed')"
            "  )"
        )
        stuck = cur.fetchone()[0]
        assert stuck == 0, (
            f"{stuck} NEW order_created events processed but no continuation"
        )

        cur.close()
        conn.close()

    def test_consumer_has_no_can_proceed_check(self):
        """consumer.py should NOT have _can_proceed blocking SAGA flow."""
        import os

        consumer_path = os.path.join(
            os.path.dirname(__file__), "..", "services", "event-consumer-service", "consumer.py"
        )
        with open(consumer_path) as f:
            content = f.read()

        # _can_proceed should NOT be called in handle_order_created
        # (function may exist but should not be used to block flow)
        assert "_can_proceed(order_id)" not in content, (
            "_can_proceed(order_id) still blocks SAGA flow — remove it"
        )
