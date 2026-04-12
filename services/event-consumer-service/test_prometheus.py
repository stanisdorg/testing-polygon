"""Тесты Prometheus метрик (unit)."""
from prometheus_client import generate_latest

# Импортируем consumer чтобы метрики зарегистрировались
import consumer  # noqa: F401


def test_prometheus_metrics_contain_expected_counters():
    """generate_latest содержит все ожидаемые метрики"""
    body = generate_latest().decode()

    assert "processed_total" in body
    assert "success_total" in body
    assert "error_total" in body
    assert "retry_total" in body
    assert "dlq_total" in body


def test_prometheus_metrics_have_help_text():
    """Метрики содержат HELP текст (формат Prometheus)"""
    body = generate_latest().decode()

    assert "# HELP processed_total" in body
    assert "# TYPE processed_total counter" in body


def test_latency_histogram_exists():
    """Histogram order_processing_seconds присутствует в метриках"""
    body = generate_latest().decode()

    assert "order_processing_seconds" in body
    assert "# TYPE order_processing_seconds histogram" in body
