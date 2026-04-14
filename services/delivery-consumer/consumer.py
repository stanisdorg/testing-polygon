"""Delivery Consumer — handles order_shipped → delivery_assigned/started/completed.

This consumer listens for order_shipped events and processes delivery:
1. delivery_assigned (assign courier)
2. delivery_started (courier on the way)
3. delivery_completed (delivered successfully)
"""
import json
import os
import random
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
    get_db,
    update_order_status,
)

# ── Config ──
SAGA_STEP_DELAY_MIN = int(os.environ.get("SAGA_STEP_DELAY_MIN", "2"))
SAGA_STEP_DELAY_MAX = int(os.environ.get("SAGA_STEP_DELAY_MAX", "5"))


def _saga_sleep(step_name: str = ""):
    """Simulate realistic delivery time."""
    delay = random.uniform(SAGA_STEP_DELAY_MIN, SAGA_STEP_DELAY_MAX)
    print(f"    ⏳ Waiting {delay:.0f}s for {step_name or 'delivery operation'}...")
    time.sleep(delay)


def assign_courier() -> str:
    """Assign a courier for delivery using round-robin."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM employees WHERE role = 'courier' ORDER BY name")
        couriers = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    if not couriers:
        print(f"  ⚠ No couriers available")
        return None

    # Get current loads
    loads = {}
    for c in couriers:
        key = f"load:courier:{c}"
        load = int(redis_client.get(key) or 0)
        loads[c] = load

    # Pick courier with minimum load
    min_load = min(loads.values())
    candidates = [c for c, l in loads.items() if l == min_load]

    # Use Redis counter for round-robin among candidates
    counter_key = "delivery:courier_counter"
    idx = int(redis_client.get(counter_key) or 0)
    chosen = candidates[idx % len(candidates)]
    redis_client.set(counter_key, (idx + 1) % len(couriers))

    # Increment load
    load_key = f"load:courier:{chosen}"
    redis_client.incr(load_key)
    redis_client.expire(load_key, 3600)

    return chosen


def decrement_courier_load(name: str):
    """Decrement courier load after completing delivery."""
    key = f"load:courier:{name}"
    redis_client.decr(key)
    redis_client.expire(key, 3600)


def insert_delivery_record(order_id: str, courier_name: str):
    """Insert delivery record into deliveries table."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO deliveries (order_id, courier_name, status) VALUES (%s, %s, 'assigned')",
            (order_id, courier_name),
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def update_delivery_status(order_id: str, status: str):
    """Update delivery status in deliveries table."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE deliveries SET status = %s WHERE order_id = %s",
            (status, order_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


class DeliveryConsumer(BaseConsumer):
    """Consumer that processes order_shipped events and handles delivery."""

    @property
    def _metric_prefix(self) -> str:
        return "delivery_consumer"

    def handle_event(self, message: dict):
        """Handle ONLY order_shipped events."""
        event_type = message.get("event_type")
        order_id = message.get("order_id")
        payload = message.get("payload", {})
        trace_id = message.get("trace_id", order_id)
        saga_id = message.get("saga_id", order_id)

        # ONLY process order_shipped — ignore picking_started, picking_completed, order_packed
        if event_type != "order_shipped":
            return

        # Idempotency check
        if event_exists(order_id, "delivery_completed", saga_id):
            print(f"  ⚠ Event already processed: delivery_completed for {order_id}")
            return

        # Deduplication lock
        dedup_key = f"delivery:{saga_id}"
        if not acquire_dedup_lock(dedup_key, ttl=120):
            print(f"  ⚠ Duplicate event detected, skipping: {order_id}")
            return

        # Check if order was already cancelled
        order = get_order(order_id)
        if order and order.get("status") in ("CANCELLED", "FAILED"):
            print(f"  ⚠ Order {order_id} is {order['status']}, skipping")
            release_dedup_lock(dedup_key)
            return

        courier_name = None

        try:
            print(f"  🔄 Processing delivery for {order_id}")

            # Step 1: Assign courier
            if not event_exists(order_id, "delivery_assigned", saga_id):
                courier_name = assign_courier()
                
                # Insert delivery record
                insert_delivery_record(order_id, courier_name)

                insert_event(
                    event_type="delivery_assigned",
                    order_id=order_id,
                    payload={"courier": courier_name},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_assigned",
                )
                publish_kafka_event(
                    topic="delivery_events",
                    event_type="delivery_assigned",
                    order_id=order_id,
                    payload={"courier": courier_name},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_assigned",
                )
                _saga_sleep("courier assignment")

            # Step 2: Delivery started
            if not event_exists(order_id, "delivery_started", saga_id):
                insert_event(
                    event_type="delivery_started",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_started",
                )
                publish_kafka_event(
                    topic="delivery_events",
                    event_type="delivery_started",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_started",
                )
                update_delivery_status(order_id, "in_transit")
                _saga_sleep("delivery in transit")

            # Step 3: Delivery completed
            if not event_exists(order_id, "delivery_completed", saga_id):
                insert_event(
                    event_type="delivery_completed",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_completed",
                )
                publish_kafka_event(
                    topic="delivery_events",
                    event_type="delivery_completed",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="delivery",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="delivery_completed",
                )
                update_delivery_status(order_id, "delivered")
                # Mark order as COMPLETED
                update_order_status(order_id, "COMPLETED")
                
                # Publish order_completed event for Funnel/Dashboard consistency
                insert_event(
                    event_type="order_completed",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="order",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_completed",
                )
                publish_kafka_event(
                    topic="delivery_events",
                    event_type="order_completed",
                    order_id=order_id,
                    payload={},
                    trace_id=trace_id,
                    entity_type="order",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_completed",
                )

            print(f"  ✅ Delivery completed for {order_id}")

            # Decrement courier load
            if courier_name:
                decrement_courier_load(courier_name)

        except Exception as e:
            print(f"  ❌ Error in delivery for {order_id}: {e}")
            if courier_name:
                decrement_courier_load(courier_name)
            raise
        finally:
            release_dedup_lock(dedup_key)


def main():
    """Start the Delivery Consumer."""
    print("🚀 Starting Delivery Consumer...")
    consumer = DeliveryConsumer(
        topics=["warehouse_events"],
        group_id="delivery-consumer-group",
        metrics_port=int(os.environ.get("METRICS_PORT", "8013")),
    )
    consumer.start()


if __name__ == "__main__":
    main()
