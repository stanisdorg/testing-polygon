"""Tests for Payment Consumer."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer import process_payment, PaymentConsumer


class TestPaymentProcessing(unittest.TestCase):
    """Test payment processing logic."""

    @patch("consumer.random")
    def test_payment_succeeds(self, mock_random):
        """Happy path: Payment succeeds when random < success_rate."""
        mock_random.random.return_value = 0.5  # Less than 0.8
        result = process_payment("ORD-TEST-1", 100.0)
        self.assertTrue(result)

    @patch("consumer.random")
    def test_payment_fails(self, mock_random):
        """Error case: Payment fails when random >= success_rate."""
        mock_random.random.return_value = 0.9  # Greater than 0.8
        result = process_payment("ORD-TEST-1", 100.0)
        self.assertFalse(result)


class TestPaymentConsumer(unittest.TestCase):
    """Test PaymentConsumer event handling."""

    def setUp(self):
        self.sample_message = {
            "event_type": "inventory_reserved",
            "order_id": "ORD-TEST-1",
            "payload": {
                "items": [{"sku": "SKU-001", "qty": 2}],
            },
            "trace_id": "test-trace-123",
            "saga_id": "ORD-TEST-1",
        }

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.process_payment")
    @patch("consumer.event_exists")
    def test_handle_event_payment_success(
        self,
        mock_event_exists,
        mock_process_payment,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Happy path: Payment succeeds."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_process_payment.return_value = True

        consumer = PaymentConsumer(
            topics=["inventory_events"],
            group_id="test-group",
            metrics_port=9999,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify payment_succeeded event was published
        self.assertTrue(mock_insert_event.called)
        self.assertTrue(mock_publish_event.called)

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.process_payment")
    @patch("consumer.event_exists")
    def test_handle_event_payment_failed(
        self,
        mock_event_exists,
        mock_process_payment,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Error case: Payment fails."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_process_payment.return_value = False

        consumer = PaymentConsumer(
            topics=["inventory_events"],
            group_id="test-group",
            metrics_port=9998,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify payment_failed event was published
        insert_calls = mock_insert_event.call_args_list
        self.assertTrue(any(
            call[1]["event_type"] == "payment_failed"
            for call in insert_calls
        ))


if __name__ == "__main__":
    unittest.main()
