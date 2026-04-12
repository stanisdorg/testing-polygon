"""Tests for Delivery Consumer."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer import DeliveryConsumer


class TestDeliveryConsumer(unittest.TestCase):
    """Test DeliveryConsumer event handling."""

    def setUp(self):
        self.sample_message = {
            "event_type": "order_shipped",
            "order_id": "ORD-TEST-1",
            "payload": {"carrier": "CDEK"},
            "trace_id": "test-trace-123",
            "saga_id": "ORD-TEST-1",
        }

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.assign_courier")
    @patch("consumer.insert_delivery_record")
    @patch("consumer.update_delivery_status")
    @patch("consumer.event_exists")
    def test_handle_event_delivery_success(
        self,
        mock_event_exists,
        mock_update_status,
        mock_insert_delivery,
        mock_assign_courier,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Happy path: Delivery completes successfully."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_assign_courier.return_value = "Test Courier"

        consumer = DeliveryConsumer(
            topics=["warehouse_events"],
            group_id="test-group",
            metrics_port=9999,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify delivery events were published
        self.assertTrue(mock_insert_event.called)
        self.assertTrue(mock_publish_event.called)

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.event_exists")
    def test_handle_event_duplicate(
        self,
        mock_event_exists,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Edge case: Duplicate event should be skipped."""
        mock_event_exists.return_value = True

        consumer = DeliveryConsumer(
            topics=["warehouse_events"],
            group_id="test-group",
            metrics_port=9998,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        mock_acquire_lock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
