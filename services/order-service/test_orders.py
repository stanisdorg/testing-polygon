import uuid
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_create_order_success():
    """Happy path: успешное создание заказа"""
    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
            "items": [{"sku": "SKU-001", "qty": 2}],
            "delivery_address": "Москва, ул. Ленина 42",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "created"
    assert data["order_id"].startswith("ORD-")


def test_create_order_missing_order_id():
    """Error: order_id не передан"""
    response = client.post(
        "/api/v1/orders",
        json={
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Москва, ул. Ленина 42",
        },
    )
    assert response.status_code == 422
    assert "detail" in response.json()


def test_create_order_empty_items():
    """Error: items — пустой массив"""
    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
            "items": [],
            "delivery_address": "Москва, ул. Ленина 42",
        },
    )
    assert response.status_code == 422
    assert "detail" in response.json()
