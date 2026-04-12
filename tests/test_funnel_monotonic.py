"""Tests for funnel monotonicity and correct counting."""
import requests

PORTAL_URL = "http://localhost:8080"


class TestFunnelMonotonicity:
    """Funnel must be monotonically non-increasing."""

    def test_funnel_is_monotonic(self):
        """Each stage count must be ≤ previous stage count."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/funnel", timeout=10)
        assert resp.status_code == 200
        data = resp.json()

        stages_order = ["created", "reserved", "paid", "picked", "packed", "shipped", "delivered"]
        counts = [data.get(s, 0) for s in stages_order]

        for i in range(1, len(counts)):
            prev_stage = stages_order[i - 1]
            curr_stage = stages_order[i]
            assert counts[i] <= counts[i - 1], (
                f"Funnel not monotonic: {prev_stage}={counts[i-1]} < {curr_stage}={counts[i]}"
            )

    def test_funnel_has_all_stages(self):
        """Funnel must contain all expected stages."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/funnel", timeout=10)
        data = resp.json()
        expected = ["created", "reserved", "paid", "picked", "packed", "shipped", "delivered"]
        for stage in expected:
            assert stage in data, f"Missing stage: {stage}"
            assert isinstance(data[stage], int), f"Stage {stage} must be int, got {type(data[stage])}"

    def test_funnel_created_is_positive(self):
        """Created stage should have orders if system is running."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/funnel", timeout=10)
        data = resp.json()
        assert data.get("created", 0) > 0, "No orders in funnel — system not generating orders"


class TestFunnelDistinctOrders:
    """Funnel must count distinct orders, not events."""

    def test_funnel_uses_distinct_orders(self):
        """Verify via SQL that funnel counts distinct orders."""
        import psycopg2

        conn = psycopg2.connect("postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT order_id) FROM events WHERE event_type = 'order_created'")
        expected_created = cur.fetchone()[0]
        cur.close()
        conn.close()

        resp = requests.get(f"{PORTAL_URL}/api/dashboard/funnel", timeout=10)
        data = resp.json()
        # Funnel created should match SQL distinct count
        assert data.get("created", 0) == expected_created, (
            f"Funnel created={data.get('created')}, expected {expected_created} (SQL DISTINCT)"
        )
