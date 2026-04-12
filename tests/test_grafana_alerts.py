"""Tests for Grafana alert provisioning.

Verifies that:
1. alerts.yml exists and is valid YAML
2. Contains all 3 required alert rules
3. Grafana API serves the alert rules
4. Alert expressions are correct
"""
import os
import time

import pytest
import requests
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALERTS_FILE = os.path.join(BASE_DIR, "grafana", "provisioning", "alerting", "alerts.yml")
GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")

REQUIRED_ALERTS = {
    "DLQ Messages Detected": {
        "severity": "critical",
        "expr_contains": "dlq_total",
    },
    "High Error Rate": {
        "severity": "warning",
        "expr_contains": "error_total",
    },
    "High p95 Latency": {
        "severity": "warning",
        "expr_contains": "histogram_quantile",
    },
}


# ── File-level tests ─────────────────────────────────────────────────────

class TestAlertsFile:
    """Tests for the provisioning YAML file."""

    def test_alerts_file_exists(self):
        """alerts.yml must exist."""
        assert os.path.isfile(ALERTS_FILE), f"Missing: {ALERTS_FILE}"

    def test_alerts_file_valid_yaml(self):
        """alerts.yml must be valid YAML."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        assert data is not None, "alerts.yml is empty"
        assert "groups" in data, "Missing 'groups' key"

    def test_alerts_has_kafka_consumer_group(self):
        """Must have 'kafka-consumer-alerts' group."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        group_names = [g["name"] for g in data["groups"]]
        assert "kafka-consumer-alerts" in group_names

    def test_alerts_has_three_rules(self):
        """Must have exactly 3 alert rules."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        rules = data["groups"][0]["rules"]
        assert len(rules) == 3, f"Expected 3 rules, got {len(rules)}"

    def test_all_required_alerts_present(self):
        """All 3 required alert titles must be present."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        titles = {r["title"] for r in data["groups"][0]["rules"]}
        for required_title in REQUIRED_ALERTS:
            assert required_title in titles, f"Missing alert: {required_title}"

    def test_alert_severity_labels(self):
        """Each alert must have correct severity label."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        rules = data["groups"][0]["rules"]
        severity_map = {r["title"]: r["labels"]["severity"] for r in rules}

        assert severity_map["DLQ Messages Detected"] == "critical"
        assert severity_map["High Error Rate"] == "warning"
        assert severity_map["High p95 Latency"] == "warning"

    def test_alert_expressions(self):
        """Each alert must have correct PromQL expression."""
        with open(ALERTS_FILE) as f:
            data = yaml.safe_load(f)
        rules = data["groups"][0]["rules"]
        expr_map = {r["title"]: r["data"][0]["model"]["expr"] for r in rules}

        assert "dlq_total" in expr_map["DLQ Messages Detected"]
        assert "error_total" in expr_map["High Error Rate"]
        assert "histogram_quantile" in expr_map["High p95 Latency"]


# ── Grafana API tests ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def grafana_alerts():
    """Fetch alert rules from Grafana API (cached per module)."""
    resp = requests.get(
        f"{GRAFANA_URL}/api/v1/provisioning/alert-rules",
        auth=GRAFANA_AUTH,
        timeout=10,
    )
    assert resp.status_code == 200, f"Grafana API error: {resp.text}"
    return resp.json()


class TestGrafanaAlertsAPI:
    """Tests for Grafana alert rules via API."""

    def test_grafana_is_healthy(self):
        """Grafana must be running and healthy."""
        resp = requests.get(f"{GRAFANA_URL}/api/health", auth=GRAFANA_AUTH, timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        assert data["database"] == "ok"

    def test_alert_rules_returned(self, grafana_alerts):
        """Grafana must return alert rules."""
        assert isinstance(grafana_alerts, list)
        assert len(grafana_alerts) >= 3, f"Expected ≥3 rules, got {len(grafana_alerts)}"

    def test_all_required_alerts_in_api(self, grafana_alerts):
        """All required alert titles must be in API response."""
        titles = {r["title"] for r in grafana_alerts}
        for required_title in REQUIRED_ALERTS:
            assert required_title in titles, f"Missing in Grafana API: {required_title}"

    def test_alert_severity_via_api(self, grafana_alerts):
        """Severity labels must be correct via API."""
        by_title = {r["title"]: r for r in grafana_alerts}
        assert by_title["DLQ Messages Detected"]["labels"]["severity"] == "critical"
        assert by_title["High Error Rate"]["labels"]["severity"] == "warning"
        assert by_title["High p95 Latency"]["labels"]["severity"] == "warning"

    def test_alert_not_paused(self, grafana_alerts):
        """Alerts must be active (not paused)."""
        by_title = {r["title"]: r for r in grafana_alerts}
        for title in REQUIRED_ALERTS:
            assert by_title[title]["isPaused"] is False, f"Alert {title} is paused"

    def test_alert_folder(self, grafana_alerts):
        """Alerts must be in 'Kafka Alerts' folder."""
        # All rules should have the same folderUID
        folder_uids = {r["folderUID"] for r in grafana_alerts}
        assert len(folder_uids) == 1, f"Multiple folders: {folder_uids}"

    def test_alert_group_name(self, grafana_alerts):
        """Alerts must be in 'kafka-consumer-alerts' rule group."""
        groups = {r["ruleGroup"] for r in grafana_alerts}
        assert "kafka-consumer-alerts" in groups

    def test_dlq_alert_expr_via_api(self, grafana_alerts):
        """DLQ alert expression must reference dlq_total."""
        dlq = [r for r in grafana_alerts if r["title"] == "DLQ Messages Detected"][0]
        expr = dlq["data"][0]["model"]["expr"]
        assert "dlq_total" in expr

    def test_error_rate_alert_expr_via_api(self, grafana_alerts):
        """Error rate alert expression must use rate()."""
        err = [r for r in grafana_alerts if r["title"] == "High Error Rate"][0]
        expr = err["data"][0]["model"]["expr"]
        assert "error_total" in expr
        assert "rate(" in expr
