"""Tests for observability upgrade: trace_id, metrics, structured logging.

Test 1: test_trace_id_propagation
  — создаём order_created → consumer двигает дальше
  — все события одного order_id имеют одинаковый trace_id

Test 2: test_metrics_increment
  — обрабатываем событие → counters увеличились

Test 3: test_dlq_metric
  — создаём failed событие → dlq_size > 0
"""
import json
import os
import sys
import time

import pytest
import requests

# Add consumer module path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "order-lifecycle-consumer"))
import consumer

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)

METRICS_URL = os.environ.get(
    "METRICS_URL",
    "http://localhost:8003/metrics",
)


def _get_conn():
    import psycopg2
    return psycopg2.connect(DB_URL)


def _ensure_trace_column():
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50),
                order_id VARCHAR(50),
                payload JSONB,
                processed BOOLEAN DEFAULT FALSE,
                failed BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                trace_id TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def _insert_order_created(order_id, trace_id=None):
    import uuid
    if trace_id is None:
        trace_id = str(uuid.uuid4())
    conn = _get_conn()
    cur = conn.cursor()
    payload = {"order_id": order_id, "items": [{"sku": "SKU-001", "qty": 1}], "trace_id": trace_id}
    cur.execute(
        """INSERT INTO events (event_type, order_id, payload, processed, failed, trace_id)
           VALUES (%s, %s, %s, false, false, %s)""",
        ("order_created", order_id, json.dumps(payload), trace_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def _get_events(order_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, event_type, order_id, payload, trace_id FROM events WHERE order_id = %s ORDER BY id ASC",
        (order_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "id": r[0],
            "event_type": r[1],
            "order_id": r[2],
            "payload": json.loads(r[3]) if isinstance(r[3], str) else r[3],
            "trace_id": r[4],
        }
        for r in rows
    ]


@pytest.fixture(autouse=True)
def setup_observability():
    _ensure_trace_column()
    consumer._chaos_config = None

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM events WHERE order_id LIKE 'ORD-OBS%'")
    cur.execute("DELETE FROM events WHERE order_id LIKE 'ORD-TEST-TRACE%'")
    conn.commit()
    cur.close()
    conn.close()

    yield

    consumer._chaos_config = None


# ═══════════════════════════════════════════════════════════
# TEST 1: trace_id propagation
# ═══════════════════════════════════════════════════════════

class TestTraceIdPropagation:
    """Все события одного order_id должны иметь одинаковый trace_id."""

    def test_trace_id_propagation(self):
        """order_created → order_picked → order_packed → order_shipped
        Все должны иметь одинаковый trace_id."""
        order_id = "ORD-OBS-TRACE"
        _insert_order_created(order_id)

        # Запускаем consumer 3 раза чтобы пройти всю цепочку
        for _ in range(4):
            consumer.run_once()

        events = _get_events(order_id)
        assert len(events) >= 4, f"Expected ≥4 events, got {len(events)}"

        # Все события должны иметь trace_id
        trace_ids = [e["trace_id"] for e in events]
        assert all(tid is not None for tid in trace_ids), "All events must have trace_id"

        # Все trace_id должны быть одинаковыми
        first_trace = trace_ids[0]
        assert all(tid == first_trace for tid in trace_ids), \
            f"trace_ids differ: {trace_ids}"


# ═══════════════════════════════════════════════════════════
# TEST 2: metrics increment
# ═══════════════════════════════════════════════════════════

class TestMetricsIncrement:
    """При обработке событий метрики должны увеличиваться."""

    def test_metrics_endpoint_accessible(self):
        """/metrics endpoint должен возвращать данные."""
        try:
            resp = requests.get(METRICS_URL, timeout=5)
            assert resp.status_code == 200
            # At minimum should have some Prometheus metrics
            assert "# HELP" in resp.text or "# TYPE" in resp.text
        except requests.ConnectionError:
            pytest.skip("Metrics service not running")

    def test_events_processed_counter_increments(self):
        """После обработки события events_processed_total должен увеличиться."""
        from prometheus_client import generate_latest
        reg = consumer.get_metrics_registry()
        if not reg:
            pytest.skip("prometheus_client not installed")

        before_text = generate_latest(reg).decode()
        before_val = _parse_counter(before_text, "events_processed_total")

        order_id = "ORD-OBS-METRICS"
        _insert_order_created(order_id)
        consumer.run_once()

        after_text = generate_latest(reg).decode()
        after_val = _parse_counter(after_text, "events_processed_total")

        assert after_val > before_val, f"events_processed_total did not increment: {before_val} → {after_val}"


# ═══════════════════════════════════════════════════════════
# TEST 3: DLQ metric
# ═══════════════════════════════════════════════════════════

class TestDLQMetric:
    """dlq_size должен отражать количество failed событий."""

    def test_dlq_size_reflects_failed_events(self):
        """После создания failed события dlq_size > 0."""
        from prometheus_client import generate_latest
        reg = consumer.get_metrics_registry()
        if not reg:
            pytest.skip("prometheus_client not installed")

        # Create a failed event
        import psycopg2
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        payload = json.dumps({"order_id": "ORD-OBS-DLQ", "items": [{"sku": "SKU-001", "qty": 1}]})
        cur.execute(
            """INSERT INTO events (event_type, order_id, payload, processed, failed, trace_id)
               VALUES (%s, %s, %s, false, true, %s)""",
            ("order_created", "ORD-OBS-DLQ", payload, "test-trace-dlq"),
        )
        conn.commit()
        cur.close()
        conn.close()

        # Run consumer to update DLQ gauge
        consumer.run_once()

        # Check dlq_size from registry
        text = generate_latest(reg).decode()
        for line in text.splitlines():
            if line.startswith("dlq_size ") and not line.startswith("dlq_size_"):
                val = float(line.split()[-1])
                assert val > 0, f"dlq_size should be > 0, got {val}"
                return
        pytest.fail("dlq_size metric not found in output")


def _parse_counter(text, name):
    """Парсит Prometheus counter из text exposition format. Handles labels."""
    total = 0
    for line in text.splitlines():
        if line.startswith(name + "{") or line.startswith(name + " "):
            # Extract value after last space
            val_str = line.rsplit(" ", 1)[-1]
            try:
                total += float(val_str)
            except ValueError:
                pass
    return total
