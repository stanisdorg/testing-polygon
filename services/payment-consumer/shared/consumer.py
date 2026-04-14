"""Payment Consumer — handles inventory_reserved → payment_succeeded/failed.

This consumer listens for inventory_reserved events and processes payment.
Payment has 80% success rate and 20% failure rate (can be configured).
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
    update_order_status,
)

# ── Config ──
SAGA_STEP_DELAY_MIN = int(os.environ.get("SAGA_STEP_DELAY_MIN", "2"))
SAGA_STEP_DELAY_MAX = int(os.environ.get("SAGA_STEP_DELAY_MAX", "5"))
PAYMENT_SUCCESS_RATE = float(os.environ.get("PAYMENT_SUCCESS_RATE", "0.8"))  # 80% success


def _saga_sleep():
    """Simulate realistic payment processing time."""
    delay = random.uniform(SAGA_STEP_DELAY_MIN, SAGA_STEP_DELAY_MAX)
    print(f"    ⏳ Waiting {delay:.0f}s for payment gateway response...")
    time.sleep(delay)


def process_payment(order_id: str, amount: float) -> bool:
    """
    Process payment for the order.
    Returns True if payment succeeded, False otherwise.
    """
    success = random.random() < PAYMENT_SUCCESS_RATE
    if success:
        print(f"  💳 Payment succeeded for {order_id} (amount: {amount})")
    else:
        print(f"  ❌ Payment failed for {order_id} (amount: {amount})")
    return success


class PaymentConsumer(BaseConsumer):
    """Consumer that processes inventory events and handles payment."""

    @property
    def _metric_prefix(self) -> str:
        return "payment_consumer"

    def handle_event(self, message: dict):
        """Handle inventory_reserved event."""
        event_type = message.get("event_type")
        order_id = message.get("order_id")
        payload = message.get("payload", {})
        trace_id = message.get("trace_id", order_id)
        saga_id = message.get("saga_id", order_id)
        items = payload.get("items", [])

        # Check if order was already cancelled
        order = get_order(order_id)
        if order and order.get("status") in ("CANCELLED", "FAILED"):
            print(f"  ⚠ Order {order_id} is {order['status']}, skipping")
            return

        # Idempotency check
        if event_exists(order_id, "payment_succeeded", saga_id):
            print(f"  ⚠ Event already processed: payment_succeeded for {order_id}")
            return

        if event_exists(order_id, "payment_failed", saga_id):
            print(f"  ⚠ Event already processed: payment_failed for {order_id}")
            return

        # Deduplication lock
        dedup_key = f"payment:{saga_id}"
        if not acquire_dedup_lock(dedup_key, ttl=60):
            print(f"  ⚠ Duplicate event detected, skipping: {order_id}")
            return

        try:
            print(f"  🔄 Processing payment for {order_id}")

            # Calculate total amount (mock calculation)
            total_amount = sum(item.get("qty", 1) * 100 for item in items)  # Mock: 100 per item

            # Simulate payment gateway delay
            _saga_sleep()

            # Publish payment_requested event
            insert_event(
                event_type="payment_requested",
                order_id=order_id,
                payload={"amount": total_amount},
                trace_id=trace_id,
                entity_type="payment",
                entity_id=order_id,
                saga_id=saga_id,
                step_name="payment_requested",
            )
            publish_kafka_event(
                topic="payment_events",
                event_type="payment_requested",
                order_id=order_id,
                payload={"amount": total_amount},
                trace_id=trace_id,
                entity_type="payment",
                entity_id=order_id,
                saga_id=saga_id,
                step_name="payment_requested",
            )

            # Process payment
            payment_success = process_payment(order_id, total_amount)

            if payment_success:
                # Payment succeeded
                insert_event(
                    event_type="payment_succeeded",
                    order_id=order_id,
                    payload={"amount": total_amount},
                    trace_id=trace_id,
                    entity_type="payment",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="payment_succeeded",
                )
                publish_kafka_event(
                    topic="payment_events",
                    event_type="payment_succeeded",
                    order_id=order_id,
                    payload={"amount": total_amount},
                    trace_id=trace_id,
                    entity_type="payment",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="payment_succeeded",
                )
                print(f"  ✅ Payment processed for {order_id}")
            else:
                # Payment failed
                insert_event(
                    event_type="payment_failed",
                    order_id=order_id,
                    payload={"amount": total_amount, "reason": "payment_declined"},
                    trace_id=trace_id,
                    entity_type="payment",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="payment_requested",
                )
                publish_kafka_event(
                    topic="payment_events",
                    event_type="payment_failed",
                    order_id=order_id,
                    payload={"amount": total_amount, "reason": "payment_declined"},
                    trace_id=trace_id,
                    entity_type="payment",
                    entity_id=order_id,
                    saga_id=saga_id,
                    step_name="payment_requested",
                )
                print(f"  ❌ Payment declined for {order_id}")

        finally:
            # Release dedup lock
            release_dedup_lock(dedup_key)


def main():
    """Start the Payment Consumer."""
    print("🚀 Starting Payment Consumer...")
    consumer = PaymentConsumer(
        topics=["inventory_events"],
        group_id="payment-consumer-group",
        metrics_port=int(os.environ.get("METRICS_PORT", "8011")),
    )
    consumer.start()


if __name__ == "__main__":
    main()
