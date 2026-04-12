"""Тесты Swagger/OpenAPI документации Order Service."""
import json
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_docs_endpoint_returns_200():
    """GET /docs → возвращает Swagger UI (200 OK)"""
    response = client.get("/docs")
    assert response.status_code == 200
    assert "swagger" in response.text.lower()


def test_redoc_endpoint_returns_200():
    """GET /redoc → возвращает ReDoc UI (200 OK)"""
    response = client.get("/redoc")
    assert response.status_code == 200
    assert "redoc" in response.text.lower()


def test_openapi_json_returns_schema():
    """GET /openapi.json → содержит OpenAPI схему"""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()

    # Проверяем метаданные
    assert schema["info"]["title"] == "FulfilBox — Order Service"
    assert "version" in schema["info"]
    assert "description" in schema["info"]


def test_openapi_contains_all_endpoints():
    """OpenAPI схема содержит все endpoint'ы"""
    response = client.get("/openapi.json")
    schema = response.json()
    paths = list(schema["paths"].keys())

    assert "/health" in paths
    assert "/api/v1/orders" in paths


def test_openapi_contains_models():
    """OpenAPI схема содержит модели (schemas)"""
    response = client.get("/openapi.json")
    schema = response.json()
    components = schema.get("components", {}).get("schemas", {})

    assert "OrderItem" in components
    assert "CreateOrderRequest" in components


def test_order_item_has_field_descriptions():
    """Модель OrderItem имеет описания полей"""
    response = client.get("/openapi.json")
    schema = response.json()
    order_item = schema["components"]["schemas"]["OrderItem"]

    assert "description" in order_item["properties"]["sku"]
    assert "description" in order_item["properties"]["qty"]


def test_create_order_request_has_description():
    """Модель CreateOrderRequest имеет описания"""
    response = client.get("/openapi.json")
    schema = response.json()
    create_order = schema["components"]["schemas"]["CreateOrderRequest"]

    assert "description" in create_order["properties"]["order_id"]
    assert "description" in create_order["properties"]["items"]
    assert "description" in create_order["properties"]["delivery_address"]


def test_endpoints_have_tags():
    """Endpoint'ы имеют теги для группировки"""
    response = client.get("/openapi.json")
    schema = response.json()

    health_tags = schema["paths"]["/health"]["get"].get("tags", [])
    orders_get_tags = schema["paths"]["/api/v1/orders"]["get"].get("tags", [])
    orders_post_tags = schema["paths"]["/api/v1/orders"]["post"].get("tags", [])

    assert "Health" in health_tags
    assert "Orders" in orders_get_tags
    assert "Orders" in orders_post_tags


def test_endpoints_have_summary():
    """Endpoint'ы имеют summary (краткое описание)"""
    response = client.get("/openapi.json")
    schema = response.json()

    health_summary = schema["paths"]["/health"]["get"].get("summary", "")
    orders_post_summary = schema["paths"]["/api/v1/orders"]["post"].get("summary", "")

    assert len(health_summary) > 0
    assert len(orders_post_summary) > 0
