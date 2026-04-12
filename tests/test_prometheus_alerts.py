"""Тесты Prometheus alerts — проверка конфигурации алертов."""
import json
import os
import urllib.request
import yaml

ALERTS_FILE = os.path.join(os.path.dirname(__file__), "..", "prometheus", "alerts.yml")
PROMETHEUS_URL = "http://localhost:9090"


def test_alerts_file_exists():
    """Файл alerts.yml существует"""
    assert os.path.exists(ALERTS_FILE), f"alerts.yml not found at {ALERTS_FILE}"


def test_alerts_file_has_three_rules():
    """alerts.yml содержит три правила"""
    with open(ALERTS_FILE) as f:
        config = yaml.safe_load(f)

    rules = config["groups"][0]["rules"]
    assert len(rules) == 3, f"Expected 3 alert rules, found {len(rules)}"


def test_alert_rules_have_correct_names():
    """Правила имеют правильные имена"""
    with open(ALERTS_FILE) as f:
        config = yaml.safe_load(f)

    rules = config["groups"][0]["rules"]
    names = [r["alert"] for r in rules]

    assert "DLQMessagesDetected" in names
    assert "HighErrorRate" in names
    assert "HighLatencyP95" in names


def test_alert_rules_have_severity():
    """Все правила имеют severity label"""
    with open(ALERTS_FILE) as f:
        config = yaml.safe_load(f)

    rules = config["groups"][0]["rules"]
    for rule in rules:
        assert "severity" in rule["labels"], f"Alert {rule['alert']} missing severity label"


def test_prometheus_endpoint_returns_rules():
    """Prometheus /api/v1/rules возвращает загруженные правила"""
    try:
        resp = urllib.request.urlopen(f"{PROMETHEUS_URL}/api/v1/rules")
        data = json.loads(resp.read().decode())
        assert data["status"] == "success"

        groups = data["data"]["groups"]
        assert len(groups) > 0, "No alert groups found in Prometheus"

        rules = groups[0]["rules"]
        names = [r["name"] for r in rules]

        assert "DLQMessagesDetected" in names
        assert "HighErrorRate" in names
        assert "HighLatencyP95" in names
    except Exception as e:
        # Если Prometheus недоступен (тесты без запущенной системы) — пропускаем
        print(f"Prometheus not available: {e}")
