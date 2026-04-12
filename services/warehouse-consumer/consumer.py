"""Warehouse Consumer — handles payment_succeeded → picking/packing/shipping.

This consumer listens for payment_succeeded events and processes warehouse operations:
1. picking_started → picking_completed
2. order_packed
3. order_shipped
"""
import json
import os
import random
import sys
import threading
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
)

# ── Config ──
SAGA_STEP_DELAY_MIN = int(os.environ.get("SAGA_STEP_DELAY_MIN", "2"))
SAGA_STEP_DELAY_MAX = int(os.environ.get("SAGA_STEP_DELAY_MAX", "5"))

MAX_CONCURRENT = {
    "picker": 3,
    "packer": 4,
}

# Round-robin counters for employee assignment
_employee_counters = {"picker": 0, "packer": 0}
_employee_counters_lock = threading.Lock()


def _saga_sleep():
    """Simulate realistic warehouse operation time."""
    delay = random.uniform(SAGA_STEP_DELAY_MIN, SAGA_STEP_DELAY_MAX)
    print(f"    ⏳ Waiting {delay:.0f}s for warehouse operation...")
    time.sleep(delay)


def assign_worker_with_load(role: str) -> str:
    """Select employee with lowest load (load-aware round-robin)."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM employees WHERE role = %s ORDER BY name", (role,))
        workers = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    if not workers:
        print(f"  ⚠ No employees found for role: {role}")
        return None

    max_load = MAX_CONCURRENT.get(role, 5)

    # Get current loads from Redis
    loads = {}
    for w in workers:
        key = f"load:{role}:{w}"
        load = int(redis_client.get(key) or 0)
        loads[w] = load

    # Filter workers below max concurrent
    available = [w for w, l in loads.items() if l < max_load]

    if not available:
        print(f"  ⚠ All {role}s at max load ({max_load}), picking least loaded")
        available = workers

    # Pick worker with minimum load
    min_load = min(loads.get(w, 0) for w in available)
    candidates = [w for w in available if loads.get(w, 0) == min_load]

    # Round-robin among candidates
    with _employee_counters_lock:
        idx = _employee_counters.get(role, 0)
        chosen = candidates[idx % len(candidates)]
        _employee_counters[role] = (idx + 1) % len(candidates)

    # Increment load in Redis
    key = f"load:{role}:{chosen}"
    redis_client.incr(key)
    redis_client.expire(key, 3600)

    return chosen


def decrement_worker_load(role: str, name: str):
    """Decrement worker load after completing order."""
    key = f"load:{role}:{name}"
    redis_client.decr(key)
    redis_client.expire(key, 3600)


class WarehouseConsumer(BaseConsumer):
    """Consumer that processes payment events and handles warehouse operations."""

    @property
    def _metric_prefix(self) -> str:
        return "warehouse_consumer"

    def handle_event(self, message: dict):
        """Handle ONLY payment_succeeded events."""
        event_type = message.get("event_type")
        order_id = message.get("order_id")
        payload = message.get("payload", {})
        trace_id = message.get("trace_id", order_id)
        saga_id = message.get("saga_id", order_id)
        items = payload.get("items", [])
        warehouse_id = payload.get("warehouse_id", "WH-MSK-S")

        # ONLY process payment_succeeded — ignore payment_requested, payment_failed
        if event_type != "payment_succeeded":
            return

        # Idempotency check
        if event_exists(order_id, "order_shipped", saga_id):
            print(f"  ⚠ Event already processed: order_shipped for {order_id}")
            return

        # Deduplication lock
        dedup_key = f"warehouse:{saga_id}"
        if not acquire_dedup_lock(dedup_key, ttl=120):
            print(f"  ⚠ Duplicate event detected, skipping: {order_id}")
            return

        picker_name = None
        packer_name = None

        try:
            print(f"  🔄 Processing warehouse operations for {order_id}")

            # Step 1: Picking
            if not event_exists(order_id, "picking_completed", saga_id):
                picker_name = assign_worker_with_load("picker")
                
                insert_event(
                    event_type="picking_started",
                    order_id=order_id,
                    payload={"warehouse": warehouse_id, "picker": picker_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="picking_started",
                )
                publish_kafka_event(
                    topic="warehouse_events",
                    event_type="picking_started",
                    order_id=order_id,
                    payload={"warehouse": warehouse_id, "picker": picker_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="picking_started",
                )
                _saga_sleep()

                insert_event(
                    event_type="picking_completed",
                    order_id=order_id,
                    payload={"items_count": len(items), "picker": picker_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="picking_completed",
                )
                publish_kafka_event(
                    topic="warehouse_events",
                    event_type="picking_completed",
                    order_id=order_id,
                    payload={"items_count": len(items), "picker": picker_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="picking_completed",
                )
                _saga_sleep()

            # Step 2: Packing
            if not event_exists(order_id, "order_packed", saga_id):
                packer_name = assign_worker_with_load("packer")
                
                insert_event(
                    event_type="order_packed",
                    order_id=order_id,
                    payload={"box_size": "M", "packer": packer_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_packed",
                )
                publish_kafka_event(
                    topic="warehouse_events",
                    event_type="order_packed",
                    order_id=order_id,
                    payload={"box_size": "M", "packer": packer_name},
                    trace_id=trace_id,
                    entity_type="warehouse",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_packed",
                )
                _saga_sleep()

            # Step 3: Shipping
            if not event_exists(order_id, "order_shipped", saga_id):
                insert_event(
                    event_type="order_shipped",
                    order_id=order_id,
                    payload={"carrier": "CDEK"},
                    trace_id=trace_id,
                    entity_type="order",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_shipped",
                )
                publish_kafka_event(
                    topic="warehouse_events",
                    event_type="order_shipped",
                    order_id=order_id,
                    payload={"carrier": "CDEK"},
                    trace_id=trace_id,
                    entity_type="order",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="order_shipped",
                )

            print(f"  ✅ Warehouse operations completed for {order_id}")

            # Decrement worker loads
            if picker_name:
                decrement_worker_load("picker", picker_name)
            if packer_name:
                decrement_worker_load("packer", packer_name)

        except Exception as e:
            print(f"  ❌ Error in warehouse operations for {order_id}: {e}")
            # Decrement worker loads on error too
            if picker_name:
                decrement_worker_load("picker", picker_name)
            if packer_name:
                decrement_worker_load("packer", packer_name)
            raise
        finally:
            release_dedup_lock(dedup_key)


def main():
    """Start the Warehouse Consumer."""
    print("🚀 Starting Warehouse Consumer...")
    consumer = WarehouseConsumer(
        topics=["payment_events"],
        group_id="warehouse-consumer-group",
        metrics_port=int(os.environ.get("METRICS_PORT", "8012")),
    )
    consumer.start()


if __name__ == "__main__":
    main()
