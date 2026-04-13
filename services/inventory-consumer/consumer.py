"""Inventory Consumer — handles order_created → inventory_reserved/failed.

This consumer listens for order_created events and checks inventory availability.
If items are available, it reserves them and publishes inventory_reserved.
If items are not available, it publishes inventory_failed.
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
    update_order_status,
)
from shared.warehouse import WarehouseService
from shared.grpc_service import start_grpc_server, ConsumerStatusServicer

# ── Config ──
SAGA_STEP_DELAY_MIN = int(os.environ.get("SAGA_STEP_DELAY_MIN", "2"))
SAGA_STEP_DELAY_MAX = int(os.environ.get("SAGA_STEP_DELAY_MAX", "5"))
INVENTORY_FAILURE_RATE = float(os.environ.get("INVENTORY_FAILURE_RATE", "0.1"))  # 10% chance of failure

warehouse = WarehouseService()


def _saga_sleep():
    """Simulate realistic warehouse operation time."""
    delay = random.uniform(SAGA_STEP_DELAY_MIN, SAGA_STEP_DELAY_MAX)
    print(f"    ⏳ Waiting {delay:.0f}s before completing inventory check...")
    time.sleep(delay)


def check_inventory_availability(items: list) -> bool:
    """Check if all items are available in inventory."""
    for item in items:
        sku = item["sku"]
        qty = item["qty"]
        if not warehouse.check_availability(sku, qty):
            print(f"  ⚠ Item {sku} not available (requested: {qty})")
            return False
    return True


def reserve_inventory(items: list, order_id: str) -> bool:
    """Reserve items in inventory for this order."""
    for item in items:
        sku = item["sku"]
        qty = item["qty"]
        if not warehouse.reserve_stock(sku, qty):
            print(f"  ⚠ Failed to reserve {sku} (qty: {qty})")
            return False
        # Update Redis cache
        redis_client.setex(f"stock:{sku}", 10, "true")
    print(f"  📦 Inventory reserved for order {order_id}")
    return True


def release_inventory_reservation(items: list, order_id: str):
    """Release inventory reservation (for compensation)."""
    for item in items:
        sku = item["sku"]
        qty = item["qty"]
        warehouse.release_stock(sku, qty)
        redis_client.delete(f"stock:{sku}")  # Invalidate cache
    print(f"  ↩ Inventory reservation released for order {order_id}")


class InventoryConsumer(BaseConsumer):
    """Consumer that processes order_created events and reserves inventory."""

    @property
    def _metric_prefix(self) -> str:
        return "inventory_consumer"

    def handle_event(self, message: dict):
        """Handle order_created event."""
        event_type = message.get("event_type")
        order_id = message.get("order_id")
        payload = message.get("payload", {})
        trace_id = message.get("trace_id", order_id)
        saga_id = message.get("saga_id", order_id)
        items = payload.get("items", [])

        # Idempotency check
        if event_exists(order_id, "inventory_reserved", saga_id):
            print(f"  ⚠ Event already processed: inventory_reserved for {order_id}")
            return

        if event_exists(order_id, "inventory_failed", saga_id):
            print(f"  ⚠ Event already processed: inventory_failed for {order_id}")
            return

        # Deduplication lock
        dedup_key = f"inventory:{saga_id}"
        if not acquire_dedup_lock(dedup_key, ttl=60):
            print(f"  ⚠ Duplicate event detected, skipping: {order_id}")
            return

        try:
            print(f"  🔄 Processing inventory check for {order_id}")
            update_order_status(order_id, "PROCESSING")

            # Simulate realistic delay
            _saga_sleep()

            # Check availability
            if not check_inventory_availability(items):
                # Inventory not available
                insert_event(
                    event_type="inventory_failed",
                    order_id=order_id,
                    payload={"items": items, "reason": "out_of_stock"},
                    trace_id=trace_id,
                    entity_type="inventory",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="inventory_reserved",
                )
                publish_kafka_event(
                    topic="inventory_events",
                    event_type="inventory_failed",
                    order_id=order_id,
                    payload={"items": items, "reason": "out_of_stock"},
                    trace_id=trace_id,
                    entity_type="inventory",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="inventory_reserved",
                )
                update_order_status(order_id, "FAILED")
                print(f"  ❌ Inventory check failed for {order_id}")
                return

            # Reserve inventory
            if not reserve_inventory(items, order_id):
                insert_event(
                    event_type="inventory_failed",
                    order_id=order_id,
                    payload={"items": items, "reason": "reservation_failed"},
                    trace_id=trace_id,
                    entity_type="inventory",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="inventory_reserved",
                )
                publish_kafka_event(
                    topic="inventory_events",
                    event_type="inventory_failed",
                    order_id=order_id,
                    payload={"items": items, "reason": "reservation_failed"},
                    trace_id=trace_id,
                    entity_type="inventory",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="inventory_reserved",
                )
                update_order_status(order_id, "FAILED")
                print(f"  ❌ Failed to reserve inventory for {order_id}")
                return

            # Success - inventory reserved
            insert_event(
                event_type="inventory_reserved",
                order_id=order_id,
                payload={"items": items},
                trace_id=trace_id,
                entity_type="inventory",
                entity_id=order_id,
                saga_id=saga_id,
                step_name="inventory_reserved",
            )
            publish_kafka_event(
                topic="inventory_events",
                event_type="inventory_reserved",
                order_id=order_id,
                payload={"items": items},
                trace_id=trace_id,
                entity_type="inventory",
                entity_id=order_id,
                saga_id=saga_id,
                step_name="inventory_reserved",
            )
            print(f"  ✅ Inventory reserved for {order_id}")

        finally:
            # Update gRPC stats
            ConsumerStatusServicer.update_stats(event_type or "order_created")
            # Release dedup lock
            release_dedup_lock(dedup_key)


def main():
    """Start the Inventory Consumer with gRPC server."""
    print("🚀 Starting Inventory Consumer...")

    # Start gRPC server in background thread
    try:
        start_grpc_server(port=int(os.environ.get("GRPC_PORT", "50051")))
    except Exception as e:
        print(f"  ⚠ Failed to start gRPC server: {e}")

    consumer = InventoryConsumer(
        topics=["order_events"],
        group_id="inventory-consumer-group",
        metrics_port=int(os.environ.get("METRICS_PORT", "8010")),
    )
    consumer.start()


if __name__ == "__main__":
    main()
