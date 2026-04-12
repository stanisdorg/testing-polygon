"""Tests for Inventory Consumer."""
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer import (
    check_inventory_availability,
    reserve_inventory,
    release_inventory_reservation,
    InventoryConsumer,
)


class TestInventoryAvailability(unittest.TestCase):
    """Test inventory availability checking."""

    @patch("consumer.warehouse")
    def test_items_available(self, mock_warehouse):
        """Happy path: All items are available."""
        mock_warehouse.check_availability.return_value = True
        items = [{"sku": "SKU-001", "qty": 2}, {"sku": "SKU-002", "qty": 1}]

        result = check_inventory_availability(items)

        self.assertTrue(result)
        self.assertEqual(mock_warehouse.check_availability.call_count, 2)

    @patch("consumer.warehouse")
    def test_items_not_available(self, mock_warehouse):
        """Edge case: Item not available."""
        def check_availability_side_effect(sku, qty):
            return sku != "SKU-002"  # SKU-002 is not available

        mock_warehouse.check_availability.side_effect = check_availability_side_effect
        items = [{"sku": "SKU-001", "qty": 2}, {"sku": "SKU-002", "qty": 1}]

        result = check_inventory_availability(items)

        self.assertFalse(result)

    @patch("consumer.warehouse")
    def test_empty_items_list(self, mock_warehouse):
        """Edge case: Empty items list."""
        result = check_inventory_availability([])

        self.assertTrue(result)
        mock_warehouse.check_availability.assert_not_called()


class TestInventoryReservation(unittest.TestCase):
    """Test inventory reservation logic."""

    @patch("consumer.redis_client")
    @patch("consumer.warehouse")
    def test_reservation_success(self, mock_warehouse, mock_redis):
        """Happy path: Reservation succeeds."""
        mock_warehouse.reserve_stock.return_value = True
        items = [{"sku": "SKU-001", "qty": 2}]

        result = reserve_inventory(items, "ORD-TEST-1")

        self.assertTrue(result)
        mock_warehouse.reserve_stock.assert_called_once_with("SKU-001", 2)
        mock_redis.setex.assert_called_once()

    @patch("consumer.redis_client")
    @patch("consumer.warehouse")
    def test_reservation_fails(self, mock_warehouse, mock_redis):
        """Error case: Reservation fails."""
        mock_warehouse.reserve_stock.return_value = False
        items = [{"sku": "SKU-001", "qty": 2}]

        result = reserve_inventory(items, "ORD-TEST-1")

        self.assertFalse(result)

    @patch("consumer.redis_client")
    @patch("consumer.warehouse")
    def test_release_reservation(self, mock_warehouse, mock_redis):
        """Test releasing inventory reservation."""
        items = [{"sku": "SKU-001", "qty": 2}]

        release_inventory_reservation(items, "ORD-TEST-1")

        mock_warehouse.release_stock.assert_called_once_with("SKU-001", 2)
        mock_redis.delete.assert_called_once_with("stock:SKU-001")


class TestInventoryConsumer(unittest.TestCase):
    """Test InventoryConsumer event handling."""

    def setUp(self):
        """Setup test fixtures."""
        self.sample_message = {
            "event_type": "order_created",
            "order_id": "ORD-TEST-1",
            "payload": {
                "items": [{"sku": "SKU-001", "qty": 1}],
                "delivery_address": "Test Address",
                "warehouse_id": "WH-MSK-S",
            },
            "trace_id": "test-trace-123",
            "saga_id": "ORD-TEST-1",
        }

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.update_order_status")
    @patch("consumer.reserve_inventory")
    @patch("consumer.check_inventory_availability")
    @patch("consumer.event_exists")
    def test_handle_event_success(
        self,
        mock_event_exists,
        mock_check_avail,
        mock_reserve,
        mock_update_status,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Happy path: Successfully process order_created event."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_check_avail.return_value = True
        mock_reserve.return_value = True

        consumer = InventoryConsumer(
            topics=["order_events"],
            group_id="test-group",
            metrics_port=9999,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify order status was updated
        mock_update_status.assert_any_call("ORD-TEST-1", "PROCESSING")

        # Verify inventory was checked and reserved
        mock_check_avail.assert_called_once()
        mock_reserve.assert_called_once()

        # Verify events were published
        self.assertTrue(mock_insert_event.called)
        self.assertTrue(mock_publish_event.called)

    @patch("consumer.release_dedup_lock")
    @patch("consumer.acquire_dedup_lock")
    @patch("consumer.publish_kafka_event")
    @patch("consumer.insert_event")
    @patch("consumer.update_order_status")
    @patch("consumer.check_inventory_availability")
    @patch("consumer.event_exists")
    def test_handle_event_inventory_not_available(
        self,
        mock_event_exists,
        mock_check_avail,
        mock_update_status,
        mock_insert_event,
        mock_publish_event,
        mock_acquire_lock,
        mock_release_lock,
    ):
        """Error case: Inventory not available."""
        mock_event_exists.return_value = False
        mock_acquire_lock.return_value = True
        mock_check_avail.return_value = False

        consumer = InventoryConsumer(
            topics=["order_events"],
            group_id="test-group",
            metrics_port=9998,
            mock_consumer=True,
        )
        consumer.handle_event(self.sample_message)

        # Verify order was marked as FAILED
        mock_update_status.assert_any_call("ORD-TEST-1", "FAILED")

        # Verify inventory_failed event was published
        insert_calls = mock_insert_event.call_args_list
        self.assertTrue(any(
            call[1]["event_type"] == "inventory_failed"
            for call in insert_calls
        ))

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
        mock_event_exists.return_value = True  # Event already exists

        consumer = InventoryConsumer(
            topics=["order_events"],
            group_id="test-group",
            metrics_port=9997,
            mock_consumer=True,
        )

        # Should return early without processing
        consumer.handle_event(self.sample_message)

        mock_acquire_lock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
