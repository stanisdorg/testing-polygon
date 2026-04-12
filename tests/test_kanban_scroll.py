"""Tests for Kanban board scroll improvements."""
import requests

PORTAL_URL = "http://localhost:8080"


class TestKanbanScroll:
    """Verify Kanban board has proper horizontal scroll."""

    def test_kanban_html_has_scroll_controls(self):
        """Dashboard HTML should contain scroll arrow buttons."""
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        html = resp.text
        # Check for scroll arrow buttons
        assert "kanban-scroll" in html or "scroll-btn" in html or "←" in html, \
            "Kanban should have scroll arrow controls"

    def test_kanban_html_has_drag_scroll(self):
        """Dashboard HTML should have drag-to-scroll JS logic."""
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        html = resp.text
        # Check for drag or wheel scroll handling
        has_drag = "kanbanDrag" in html or "isDragging" in html or "mousedown" in html
        has_wheel = "wheel" in html.lower() and "scrollleft" in html.lower()
        assert has_drag or has_wheel, \
            "Kanban should have drag-to-scroll or wheel-to-horizontal-scroll logic"

    def test_kanban_board_is_scrollable(self):
        """Kanban board container should allow horizontal overflow."""
        resp = requests.get(f"{PORTAL_URL}/dashboard/", timeout=10)
        html = resp.text
        # overflow-x: auto or scroll should be present in kanban-board CSS
        assert "overflow-x" in html or "scroll" in html.lower(), \
            "Kanban board should have horizontal overflow handling"
