"""Tests for Warehouse Consumer."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer import WarehouseConsumer


class TestWarehouseConsumer(unittest.TestCase):
    """Test WarehouseConsumer event handling."""

    def setUp(self):
        self.sample_message = {
            "event_type": "payment_succeeded",
            "order_id": "ORD-TEST-1",
            "payload": {
                "items": [{"sku": "SKU-001", "qty": 2}],
                "warehouse_id": "WH-MSK-S",
            },
            "trace_id": "test-trace-123",
            "saga_id": "ORD-TEST-1",
        }

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.assign_worker_with_load")
    @patch("consumer.event_exists")
    def test_handle_event_warehouse_success(
        self,
        mock_event_exists,
        mock_assign_worker,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Happy path: Warehouse operations succeed."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_assign_worker.return_value = "Test Picker"

        consumer = WarehouseConsumer(
            topics=["payment_events"],
            group_id="test-group",
            metrics_port=9999,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify warehouse events were published
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

        consumer = WarehouseConsumer(
            topics=["payment_events"],
            group_id="test-group",
            metrics_port=9998,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        mock_acquire_lock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
