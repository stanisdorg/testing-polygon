"""Integration tests for Prometheus metrics across all services."""
import requests

CONSUMER_URL = "http://localhost:8003/metrics"
PORTAL_URL = "http://localhost:8080/metrics"
ORDER_URL = "http://localhost:8001/metrics"
ORDER_API = "http://localhost:8001"
PORTAL_API = "http://localhost:8080"


def get_metrics(url):
    r = requests.get(url, timeout=10)
    assert r.status_code == 200, f"GET {url} returned {r.status_code}"
    return r.text


class TestConsumerMetrics:
    def test_error_total_exists(self):
        assert "error_total" in get_metrics(CONSUMER_URL)

    def test_retry_total_exists(self):
        assert "retry_total" in get_metrics(CONSUMER_URL)

    def test_events_failed_total_exists(self):
        assert "events_failed_total" in get_metrics(CONSUMER_URL)

    def test_events_retried_total_exists(self):
        assert "events_retried_total" in get_metrics(CONSUMER_URL)

    def test_worker_load_exists(self):
        assert "worker_load" in get_metrics(CONSUMER_URL)

    def test_saga_step_duration_exists(self):
        assert "saga_step_duration_seconds" in get_metrics(CONSUMER_URL)

    def test_processed_total_exists(self):
        assert "processed_total" in get_metrics(CONSUMER_URL)

    def test_compensation_triggered_exists(self):
        assert "compensation_triggered_total" in get_metrics(CONSUMER_URL)

    def test_compensation_completed_exists(self):
        assert "compensation_completed_total" in get_metrics(CONSUMER_URL)

    def test_inventory_check_exists(self):
        assert "inventory_check_total" in get_metrics(CONSUMER_URL)

    def test_payment_request_exists(self):
        assert "payment_request_total" in get_metrics(CONSUMER_URL)

    def test_order_processing_seconds_exists(self):
        assert "order_processing_seconds" in get_metrics(CONSUMER_URL)


class TestPortalMetrics:
    def test_active_websocket_connections_exists(self):
        assert "active_websocket_connections" in get_metrics(PORTAL_URL)

    def test_db_query_duration_exists(self):
        assert "db_query_duration_seconds" in get_metrics(PORTAL_URL)

    def test_events_streamed_total_exists(self):
        assert "events_streamed_total" in get_metrics(PORTAL_URL)

    def test_api_requests_total_exists(self):
        assert "api_requests_total" in get_metrics(PORTAL_URL)

    def test_api_request_duration_exists(self):
        assert "api_request_duration_seconds" in get_metrics(PORTAL_URL)


class TestOrderServiceMetrics:
    def test_orders_created_total_exists(self):
        assert "orders_created_total" in get_metrics(ORDER_URL)

    def test_orders_list_total_exists(self):
        assert "orders_list_total" in get_metrics(ORDER_URL)

    def test_order_creation_seconds_exists(self):
        assert "order_creation_seconds" in get_metrics(ORDER_URL)

    def test_kafka_publish_total_exists(self):
        assert "kafka_publish_total" in get_metrics(ORDER_URL)

    def test_orders_created_increments(self):
        """After creating an order, orders_created_total should increment."""
        import time
        before = get_metrics(ORDER_URL)
        before_val = 0
        for line in before.split("\n"):
            if line.startswith("orders_created_total"):
                before_val = float(line.split()[-1])
                break

        oid = f"TM{int(time.time())}"
        resp = requests.post(f"{ORDER_API}/api/v1/orders", json={
            "order_id": oid,
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Test",
            "warehouse_id": "WH-MSK-S",
        }, timeout=10)
        assert resp.status_code == 201, f"Order creation failed: {resp.text}"

        after = get_metrics(ORDER_URL)
        after_val = 0
        for line in after.split("\n"):
            if line.startswith("orders_created_total"):
                after_val = float(line.split()[-1])
                break

        assert after_val > before_val, f"orders_created_total did not increment: {before_val} -> {after_val}"
