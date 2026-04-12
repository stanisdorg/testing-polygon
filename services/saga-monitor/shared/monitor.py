"""SAGA Monitor — Compensation Handler for failed steps.

This monitor listens to ALL failure events and triggers compensation chains:
- inventory_failed → order_cancelled
- payment_failed → payment_refunded → inventory_restocked → order_cancelled
- delivery issues → delivery_cancelled → payment_refunded → inventory_restocked → order_cancelled
"""
import json
import os
import sys
import time

# Add parent directory to path to import shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.base_consumer import BaseConsumer
from shared.db_utils import (
    acquire_dedup_lock,
    release_dedup_lock,
    event_exists,
    insert_event,
    publish_kafka_event,
    redis_client,
    update_order_status,
    get_db,
    get_last_event_payload,
)

# ── Config ──
COMPENSATION_DELAY = int(os.environ.get("COMPENSATION_DELAY", "1"))


def _compensation_sleep():
    """Simulate realistic compensation processing time."""
    print(f"    ⏳ Processing compensation ({COMPENSATION_DELAY}s)...")
    time.sleep(COMPENSATION_DELAY)


def cancel_order(order_id: str, trace_id: str, saga_id: str, failed_step: str):
    """Cancel order and publish compensation events."""
    print(f"  🔄 Starting compensation for {order_id} (failed at: {failed_step})")

    # Get order items from inventory_reserved event
    inventory_payload = get_last_event_payload(order_id, "inventory_reserved")
    items = inventory_payload.get("items", []) if inventory_payload else []

    compensation_events = []

    # Determine compensation chain based on failed step
    if failed_step == "inventory_failed":
        # Only cancel order
        compensation_events = [
            ("order_cancelled", "order", {"reason": "inventory_unavailable"}),
        ]

    elif failed_step == "payment_failed":
        # Refund payment (if was charged), restock inventory, cancel order
        compensation_events = [
            ("payment_refunded", "payment", {"reason": "payment_failed_refund"}),
            ("inventory_restocked", "inventory", {"items": items}),
            ("order_cancelled", "order", {"reason": "payment_failed"}),
        ]

    elif failed_step in ("delivery_assigned", "delivery_started", "delivery_completed"):
        # Cancel delivery, refund, restock, cancel order
        compensation_events = [
            ("delivery_cancelled", "delivery", {"reason": "delivery_failed"}),
            ("payment_refunded", "payment", {"reason": "delivery_failed_refund"}),
            ("inventory_restocked", "inventory", {"items": items}),
            ("order_cancelled", "order", {"reason": "delivery_failed"}),
        ]

    else:
        # Generic compensation
        compensation_events = [
            ("order_cancelled", "order", {"reason": f"failed_at_{failed_step}"}),
        ]

    # Publish compensation events
    for event_type, entity_type, payload in compensation_events:
        if not event_exists(order_id, event_type, saga_id):
            insert_event(
                event_type=event_type,
                order_id=order_id,
                payload=payload,
                trace_id=trace_id,
                entity_type=entity_type,
                entity_id=order_id,
                saga_id=saga_id,
                step_name=event_type,
                is_compensation=True,
            )
            publish_kafka_event(
                topic="fulfilment.audit",
                event_type=event_type,
                order_id=order_id,
                payload=payload,
                trace_id=trace_id,
                entity_type=entity_type,
                entity_id=order_id,
                saga_id=saga_id,
                step_name=event_type,
                is_compensation=True,
            )
            print(f"  ↩ Compensation: {event_type} for {order_id}")

    # Update order status
    update_order_status(order_id, "CANCELLED")
    print(f"  ✅ Compensation completed for {order_id}")


def restock_inventory(order_id: str, items: list):
    """Release inventory reservation back to available stock."""
    for item in items:
        sku = item.get("sku")
        if sku:
            redis_client.delete(f"stock:{sku}")  # Invalidate cache
    print(f"  📦 Inventory restocked for {order_id}")


def refund_payment(order_id: str):
    """Mark payment as refunded in database."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE payments SET status = 'refunded' WHERE order_id = %s AND status = 'succeeded'",
            (order_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    print(f"  💰 Payment refunded for {order_id}")


class SAGAMonitor(BaseConsumer):
    """Monitor that listens for failure events and triggers compensation."""

    @property
    def _metric_prefix(self) -> str:
        return "saga_monitor"

    def handle_event(self, message: dict):
        """Handle ONLY failure events and trigger compensation."""
        event_type = message.get("event_type")
        order_id = message.get("order_id")
        payload = message.get("payload", {})
        trace_id = message.get("trace_id", order_id)
        saga_id = message.get("saga_id", order_id)

        # IGNORE non-failure events (inventory_reserved, payment_succeeded, etc.)
        failure_events = [
            "inventory_failed",
            "payment_failed",
            "delivery_cancelled",
        ]

        if event_type not in failure_events:
            # Not a failure event — skip
            return

        print(f"  ⚠ SAGA Monitor: Processing FAILURE event {event_type} for {order_id}")

        # Deduplication lock
        dedup_key = f"saga-compensation:{saga_id}"
        if not acquire_dedup_lock(dedup_key, ttl=120):
            print(f"  ⚠ Compensation already in progress for {order_id}")
            return

        try:
            _compensation_sleep()

            # Route to appropriate compensation handler
            if event_type == "inventory_failed":
                cancel_order(order_id, trace_id, saga_id, "inventory_failed")

            elif event_type == "payment_failed":
                # Check if payment was actually charged (might have succeeded but timeout)
                payment_payload = get_last_event_payload(order_id, "payment_succeeded")
                if payment_payload:
                    # Payment was charged, need to refund
                    refund_payment(order_id)
                    inventory_payload = get_last_event_payload(order_id, "inventory_reserved")
                    items = inventory_payload.get("items", []) if inventory_payload else []
                    restock_inventory(order_id, items)

                cancel_order(order_id, trace_id, saga_id, "payment_failed")

            elif event_type == "delivery_cancelled":
                cancel_order(order_id, trace_id, saga_id, "delivery_cancelled")

            else:
                print(f"  ⚠ Unknown failure event: {event_type} for {order_id}")

        finally:
            release_dedup_lock(dedup_key)


def main():
    """Start the SAGA Monitor."""
    print("🚀 Starting SAGA Monitor (Compensation Handler)...")
    consumer = SAGAMonitor(
        topics=["inventory_events", "payment_events", "delivery_events"],
        group_id="saga-monitor-group",
        metrics_port=int(os.environ.get("METRICS_PORT", "8014")),
    )
    consumer.start()


if __name__ == "__main__":
    main()
