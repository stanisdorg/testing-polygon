"""Tests for gRPC system health endpoint."""
import sys
import os
from unittest.mock import MagicMock, patch

import grpc
from fastapi.testclient import TestClient

from main import app, CONSUMER_GRPC_CONFIG

client = TestClient(app)


# ── Tests ──────────────────────────────────────────────────────────


def test_system_health_all_serving():
    """All consumers are SERVING."""
    import main
    with patch.object(main, "_check_consumer_grpc", return_value="SERVING"):
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        data = resp.json()
        for name in CONSUMER_GRPC_CONFIG:
            assert data[name] == "SERVING", f"{name} should be SERVING"


def test_system_health_unavailable():
    """Consumer is UNAVAILABLE when gRPC fails."""
    import main
    with patch.object(main, "_check_consumer_grpc", return_value="UNAVAILABLE"):
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        data = resp.json()
        for name in CONSUMER_GRPC_CONFIG:
            assert data[name] == "UNAVAILABLE", f"{name} should be UNAVAILABLE"


def test_system_health_mixed_statuses():
    """Some SERV, some UNAVAILABLE."""
    import main
    def fake_check(name, port):
        return "SERVING" if port < 50054 else "UNAVAILABLE"

    with patch.object(main, "_check_consumer_grpc", side_effect=fake_check):
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["inventory"] == "SERVING"
        assert data["payment"] == "SERVING"
        assert data["warehouse"] == "SERVING"
        assert data["delivery"] == "UNAVAILABLE"
        assert data["saga"] == "UNAVAILABLE"


def test_check_consumer_grpc_graceful_degradation():
    """_check_consumer_grpc returns UNAVAILABLE on any exception."""
    import main
    with patch("grpc.insecure_channel", side_effect=Exception("connection refused")):
        result = main._check_consumer_grpc("inventory", 50051)
        assert result == "UNAVAILABLE"


def test_health_endpoint_returns_json():
    """System health endpoint returns valid JSON with all consumers."""
    import main
    with patch.object(main, "_check_consumer_grpc", return_value="UNAVAILABLE"):
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert len(data) == len(CONSUMER_GRPC_CONFIG)
        for key in data:
            assert key in CONSUMER_GRPC_CONFIG
            assert data[key] in ("SERVING", "NOT_SERVING", "UNKNOWN", "UNAVAILABLE")
