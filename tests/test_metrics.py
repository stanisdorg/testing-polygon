"""Tests for Prometheus metrics on Student Portal and Event Consumer."""
import requests

PORTAL_URL = "http://localhost:8080"
CONSUMER_URL = "http://localhost:8003"


class TestStudentPortalMetrics:
    """Test Prometheus metrics for Student Portal API."""

    def test_metrics_endpoint_returns_200(self):
        """GET /metrics should return 200."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert resp.status_code == 200

    def test_metrics_contain_api_requests_total(self):
        """Metrics should include api_requests_total."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert "api_requests_total" in resp.text

    def test_metrics_contain_api_request_duration(self):
        """Metrics should include api_request_duration_seconds."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert "api_request_duration_seconds" in resp.text

    def test_metrics_contain_active_websocket_connections(self):
        """Metrics should include active_websocket_connections."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert "active_websocket_connections" in resp.text

    def test_metrics_contain_db_query_duration(self):
        """Metrics should include db_query_duration_seconds."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert "db_query_duration_seconds" in resp.text

    def test_metrics_contain_events_streamed_total(self):
        """Metrics should include events_streamed_total."""
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert "events_streamed_total" in resp.text

    def test_api_requests_incremented_after_request(self):
        """Making an API request should increment api_requests_total."""
        # Make a request first
        requests.get(f"{PORTAL_URL}/api/health", timeout=10)
        # Then check metrics
        resp = requests.get(f"{PORTAL_URL}/metrics", timeout=10)
        assert 'endpoint="/api/health"' in resp.text or 'endpoint="\\/api\\/health"' in resp.text


class TestConsumerMetrics:
    """Test Prometheus metrics for Event Consumer."""

    def test_metrics_endpoint_returns_200(self):
        """GET /metrics should return 200."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert resp.status_code == 200

    def test_metrics_contain_processed_total(self):
        """Metrics should include processed_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "processed_total" in resp.text

    def test_metrics_contain_success_total(self):
        """Metrics should include success_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "success_total" in resp.text

    def test_metrics_contain_error_total(self):
        """Metrics should include error_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "error_total" in resp.text

    def test_metrics_contain_retry_total(self):
        """Metrics should include retry_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "retry_total" in resp.text

    def test_metrics_contain_dlq_size(self):
        """Metrics should include dlq_size."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "dlq_size" in resp.text

    def test_metrics_contain_dlq_total(self):
        """Metrics should include dlq_total (for Grafana kafka-consumer)."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "dlq_total" in resp.text

    def test_metrics_contain_order_processing_seconds(self):
        """Metrics should include order_processing_seconds histogram."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "order_processing_seconds" in resp.text

    def test_metrics_contain_events_processed_total(self):
        """Metrics should include events_processed_total with labels."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "events_processed_total" in resp.text

    def test_metrics_contain_events_failed_total(self):
        """Metrics should include events_failed_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "events_failed_total" in resp.text

    def test_metrics_contain_events_retried_total(self):
        """Metrics should include events_retried_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "events_retried_total" in resp.text

    def test_metrics_contain_saga_step_duration(self):
        """Metrics should include saga_step_duration_seconds."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "saga_step_duration_seconds" in resp.text

    def test_metrics_contain_compensation_triggered(self):
        """Metrics should include compensation_triggered_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "compensation_triggered_total" in resp.text

    def test_metrics_contain_compensation_completed(self):
        """Metrics should include compensation_completed_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "compensation_completed_total" in resp.text

    def test_metrics_contain_inventory_check_total(self):
        """Metrics should include inventory_check_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "inventory_check_total" in resp.text

    def test_metrics_contain_payment_request_total(self):
        """Metrics should include payment_request_total."""
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert "payment_request_total" in resp.text
