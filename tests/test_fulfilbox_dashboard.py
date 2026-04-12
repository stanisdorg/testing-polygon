"""Tests for FulfilBox Dashboard and Simulator.

Verifies:
1. Simulator service files exist
2. Simulator seeds database correctly
3. Dashboard API endpoints return data
4. Dashboard UI loads
5. Data integrity (warehouses, workers, inventory, events)
"""
import os

import pytest
import psycopg2
import requests
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIMULATOR_DIR = os.path.join(BASE_DIR, "services", "fulfilbox-simulator")
DASHBOARD_DIR = os.path.join(BASE_DIR, "ui-dashboard")
PORTAL_URL = "http://localhost:8080"
DB_URL = "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox"


# ═══════════════════════════════════════════════════════════
# Simulator Files
# ═══════════════════════════════════════════════════════════

class TestSimulatorFiles:
    """Simulator service must have all required files."""

    def test_simulator_py_exists(self):
        assert os.path.isfile(os.path.join(SIMULATOR_DIR, "simulator.py"))

    def test_dockerfile_exists(self):
        assert os.path.isfile(os.path.join(SIMULATOR_DIR, "Dockerfile"))

    def test_requirements_exists(self):
        assert os.path.isfile(os.path.join(SIMULATOR_DIR, "requirements.txt"))

    def test_requirements_has_deps(self):
        with open(os.path.join(SIMULATOR_DIR, "requirements.txt")) as f:
            content = f.read()
        assert "psycopg2" in content
        assert "redis" in content
        assert "kafka" in content

    def test_simulator_has_data_definitions(self):
        with open(os.path.join(SIMULATOR_DIR, "simulator.py")) as f:
            content = f.read()
        assert "WAREHOUSES" in content
        assert "SKUS" in content
        assert "seed_workers" in content or "FIRST_NAMES" in content
        assert "SELLERS" in content
        assert "ZONE_DEFS" in content or "ZONES" in content


# ═══════════════════════════════════════════════════════════
# Dashboard Files
# ═══════════════════════════════════════════════════════════

class TestDashboardFiles:
    """Dashboard UI must exist and have required elements."""

    def test_dashboard_html_exists(self):
        assert os.path.isfile(os.path.join(DASHBOARD_DIR, "index.html"))

    def test_dashboard_has_kpi_section(self):
        with open(os.path.join(DASHBOARD_DIR, "index.html")) as f:
            content = f.read()
        assert "kpi" in content.lower()
        assert "warehouse" in content.lower() or "склад" in content.lower()

    def test_dashboard_has_event_feed(self):
        with open(os.path.join(DASHBOARD_DIR, "index.html")) as f:
            content = f.read()
        assert "event" in content.lower() or "событи" in content.lower()

    def test_dashboard_has_polling(self):
        with open(os.path.join(DASHBOARD_DIR, "index.html")) as f:
            content = f.read()
        assert "setInterval" in content


# ═══════════════════════════════════════════════════════════
# Database Integrity
# ═══════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(DB_URL)
    yield conn
    conn.close()


class TestDatabaseIntegrity:
    """Simulator must seed and maintain database correctly."""

    def test_warehouses_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_warehouses")
        count = cur.fetchone()[0]
        assert count == 3, f"Expected 3 warehouses, got {count}"
        cur.close()

    def test_warehouse_names(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT name FROM fb_warehouses ORDER BY id")
        names = [r[0] for r in cur.fetchall()]
        assert any("Москва" in n for n in names)
        assert any("Казань" in n for n in names)
        cur.close()

    def test_zones_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_zones")
        count = cur.fetchone()[0]
        assert count == 12, f"Expected 12 zones, got {count}"  # 3 warehouses × 4 zones
        cur.close()

    def test_skus_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_skus")
        count = cur.fetchone()[0]
        assert count >= 50, f"Expected ≥50 SKUs, got {count}"
        cur.close()

    def test_sku_categories(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT category FROM fb_skus")
        categories = {r[0] for r in cur.fetchall()}
        assert "electronics" in categories
        assert "clothing" in categories
        cur.close()

    def test_workers_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_workers")
        count = cur.fetchone()[0]
        assert count >= 100, f"Expected ≥100 workers, got {count}"
        cur.close()

    def test_worker_roles(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT DISTINCT role FROM fb_workers")
        roles = {r[0] for r in cur.fetchall()}
        assert "picker" in roles
        assert "packer" in roles
        cur.close()

    def test_sellers_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_sellers")
        count = cur.fetchone()[0]
        assert count >= 5, f"Expected ≥5 sellers, got {count}"
        cur.close()

    def test_inventory_populated(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_inventory WHERE quantity > 0")
        count = cur.fetchone()[0]
        assert count >= 50, f"Expected ≥50 inventory records, got {count}"
        cur.close()

    def test_events_exist(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fb_events")
        count = cur.fetchone()[0]
        assert count >= 50, f"Expected ≥50 events, got {count}"
        cur.close()

    def test_event_types_diverse(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT event_type) FROM fb_events")
        count = cur.fetchone()[0]
        assert count >= 5, f"Expected ≥5 event types, got {count}"
        cur.close()

    def test_lifecycle_completeness(self, db_conn):
        """At least some orders should reach delivery."""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT order_id) FROM fb_events WHERE event_type='order_delivered'")
        delivered = cur.fetchone()[0]
        assert delivered >= 1, "No orders delivered — lifecycle broken"
        cur.close()


# ═══════════════════════════════════════════════════════════
# Dashboard API
# ═══════════════════════════════════════════════════════════

class TestDashboardAPI:
    """Dashboard API must return valid data."""

    def test_kpis_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kpis", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "orders_today" in data
        assert "total_inventory" in data

    def test_warehouses_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/warehouses", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3

    def test_events_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/events?limit=10", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "event_type" in data[0]

    def test_alerts_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/alerts", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_funnel_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/funnel", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "created" in data
        assert "delivered" in data

    def test_workers_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/workers?limit=5", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_summary_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/summary", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "pending" in data
        assert "in_transit" in data

    def test_health_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/health", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("api") == "ok"
        assert data.get("postgresql") == "ok"

    def test_chaos_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/chaos", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "random_failure" in data

    def test_chaos_update_endpoint(self):
        resp = requests.post(
            f"{PORTAL_URL}/api/dashboard/chaos",
            json={"scenario": "random_failure", "enabled": False},
            timeout=10,
        )
        assert resp.status_code == 200

    def test_trace_endpoint(self):
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/trace/TEST-123", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "steps" in data

    def test_dashboard_ui_loads(self):
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        assert resp.status_code == 200
        assert "FulfilBox" in resp.text
        assert "dashboard" in resp.text.lower() or "FulfilBox" in resp.text


# ═══════════════════════════════════════════════════════════
# Kanban Board API
# ═══════════════════════════════════════════════════════════

class TestKanbanAPI:
    """Kanban Board API must return valid data with filtering support."""

    def test_kanban_endpoint_exists(self):
        """GET /api/dashboard/kanban should return 200."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data

    def test_kanban_has_all_columns(self):
        """Kanban response must contain all SAGA stage columns."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", timeout=10)
        data = resp.json()
        expected_columns = ["created", "reserved", "paid", "picked", "packed", "shipped", "in_delivery", "delivered"]
        for col in expected_columns:
            assert col in data["columns"], f"Missing column: {col}"
            assert "count" in data["columns"][col]
            assert "orders" in data["columns"][col]

    def test_kanban_order_structure(self):
        """Each order in kanban must have required fields."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", timeout=10)
        data = resp.json()
        # Check at least one column has orders
        for col_name, col_data in data["columns"].items():
            if col_data["count"] > 0 and col_data["orders"]:
                order = col_data["orders"][0]
                required_fields = [
                    "order_id", "warehouse_id", "status", "current_stage",
                    "created_at", "stage_since", "stage_duration_minutes",
                    "sla_breach", "sla_remaining_minutes",
                ]
                for field in required_fields:
                    assert field in order, f"Missing field '{field}' in order of column '{col_name}'"
                break  # Only need to check one order

    def test_kanban_warehouse_filter(self):
        """Kanban should support filtering by warehouse."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", params={"warehouse": "WH-MSK-S"}, timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        # All orders should be from the filtered warehouse
        for col_data in data["columns"].values():
            for order in col_data["orders"]:
                assert order["warehouse_id"] == "WH-MSK-S"

    def test_kanban_search_by_order_id(self):
        """Kanban should support searching by order_id."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", params={"search": "ORD-"}, timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        # All returned orders should match the search
        for col_data in data["columns"].values():
            for order in col_data["orders"]:
                assert "ORD-" in order["order_id"] or not col_data["orders"]

    def test_kanban_filters_applied_in_response(self):
        """Kanban response should echo applied filters."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/kanban", params={"sla_breach_only": "true"}, timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "filters_applied" in data
        assert data["filters_applied"].get("sla_breach_only") is True
