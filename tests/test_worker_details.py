"""Tests for worker details API endpoint and dashboard UI improvements."""
import os

import psycopg2
import requests

PORTAL_URL = os.environ.get("PORTAL_URL", "http://localhost:8080")
DB_URL = "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox"


class TestWorkerDetailsAPI:
    """GET /api/dashboard/workers/{worker_id}/details endpoint tests."""

    def test_worker_details_endpoint_exists(self):
        """Endpoint should return 200 for valid worker."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT id FROM employees LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        assert row is not None, "No employees in DB"
        worker_id = row[0]

        resp = requests.get(
            f"{PORTAL_URL}/api/dashboard/workers/{worker_id}/details", timeout=10
        )
        assert resp.status_code == 200

    def test_worker_details_has_required_fields(self):
        """Response should contain all required fields."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT id FROM employees LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        assert row is not None

        resp = requests.get(
            f"{PORTAL_URL}/api/dashboard/workers/{row[0]}/details", timeout=10
        )
        data = resp.json()
        required_fields = [
            "id", "name", "role", "warehouse_id",
            "completed_orders", "completed_order_ids",
            "in_progress_orders", "in_progress_order_ids",
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_worker_details_404_for_invalid_id(self):
        """Should return 404 for non-existent worker."""
        resp = requests.get(
            f"{PORTAL_URL}/api/dashboard/workers/999999/details", timeout=10
        )
        assert resp.status_code == 404

    def test_worker_details_role_has_icon_mapping(self):
        """Response should include role icon."""
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT id FROM employees LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        assert row is not None

        resp = requests.get(
            f"{PORTAL_URL}/api/dashboard/workers/{row[0]}/details", timeout=10
        )
        data = resp.json()
        assert "role_icon" in data, "Missing role_icon field"
        assert data["role_icon"] in ("🎯", "📦", "🚚"), f"Unknown role icon: {data['role_icon']}"


class TestWorkersEndpointGrouped:
    """Workers endpoint should support grouping by role."""

    def test_workers_endpoint_returns_all_roles(self):
        """Workers list should include pickers, packers, and couriers."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/workers?limit=50", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        roles = {w["role"] for w in data}
        assert "picker" in roles, "No pickers in workers list"
        assert "packer" in roles, "No packers in workers list"
        assert "courier" in roles, "No couriers in workers list"

    def test_workers_have_warehouse_id(self):
        """Each worker should have warehouse_id for proper grouping."""
        resp = requests.get(f"{PORTAL_URL}/api/dashboard/workers?limit=50", timeout=10)
        data = resp.json()
        for w in data:
            assert "warehouse_id" in w, f"Worker {w['id']} missing warehouse_id"


class TestDashboardUI:
    """Verify dashboard UI has employee grouping and icons."""

    def test_dashboard_ui_has_worker_groups(self):
        """Dashboard HTML should contain role group containers."""
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        html = resp.text
        # Check for role-based grouping markers
        assert "picker" in html.lower() or "сборщик" in html.lower(), \
            "Dashboard should have picker group"
        assert "packer" in html.lower() or "упаковщик" in html.lower(), \
            "Dashboard should have packer group"
        assert "courier" in html.lower() or "курьер" in html.lower(), \
            "Dashboard should have courier group"

    def test_dashboard_ui_has_event_icons(self):
        """Dashboard HTML should have event icon mappings."""
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        html = resp.text
        # Check for event icon definitions in JS
        assert "order_created" in html, "Should have order_created event handling"
        assert "🎉" in html or "🎯" in html or "📦" in html, \
            "Should have emoji icons for events/roles"
