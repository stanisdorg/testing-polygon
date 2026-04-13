"""Tests for GraphQL Gateway — queries, mutations, health check."""
import os
import uuid

import psycopg2
import pytest
from httpx import ASGITransport, AsyncClient

from main import app

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def _get_conn():
    return psycopg2.connect(DB_URL)


def _seed_product(sku="SKU-GQL-001", name="GQL Товар", price=100):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO products (sku, name, price) VALUES (%s, %s, %s) ON CONFLICT (sku) DO NOTHING",
        (sku, name, price),
    )
    conn.commit()
    cur.close()
    conn.close()


def _seed_order(order_id=None):
    if order_id is None:
        order_id = f"ORD-GQL-{uuid.uuid4().hex[:8]}"
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO orders (id, status, warehouse_id) VALUES (%s, 'created', 'WH-MSK-S') ON CONFLICT (id) DO NOTHING",
        (order_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    return order_id


@pytest.fixture(autouse=True)
def clean_gql_data():
    """Очищает тестовые данные после каждого теста."""
    yield
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM events WHERE order_id LIKE 'ORD-GQL-%'")
        cur.execute("DELETE FROM order_items WHERE order_id LIKE 'ORD-GQL-%'")
        cur.execute("DELETE FROM orders WHERE id LIKE 'ORD-GQL-%'")
        cur.execute("DELETE FROM products WHERE sku LIKE 'SKU-GQL-%'")
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


# ── Helpers ──────────────────────────────────────────────────────────


def _graphql_query(query: str, variables: dict | None = None):
    """Синхронный GraphQL запрос через TestClient."""
    from fastapi.testclient import TestClient

    client = TestClient(app)
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    return client.post("/graphql", json=payload)


# ── Health check ─────────────────────────────────────────────────────


def test_health_check():
    """GET /health returns ok."""
    from fastapi.testclient import TestClient

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── Products queries ─────────────────────────────────────────────────


def test_query_products():
    """Query products returns list."""
    _seed_product(sku="SKU-GQL-P1", name="Товар 1", price=100)
    _seed_product(sku="SKU-GQL-P2", name="Товар 2", price=200)

    resp = _graphql_query("""
        query {
            products(limit: 10) {
                id
                sku
                name
                price
            }
        }
    """)
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert "errors" not in data or data["errors"] is None
    products = data["data"]["products"]
    assert len(products) >= 2
    assert any(p["sku"] == "SKU-GQL-P1" for p in products)


def test_query_product_by_id():
    """Query product(id) returns single product."""
    _seed_product(sku="SKU-GQL-PID", name="Один товар", price=500)

    # Get the id first
    resp = _graphql_query("""
        query {
            products(search: "SKU-GQL-PID") {
                id
            }
        }
    """)
    product_id = resp.json()["data"]["products"][0]["id"]

    resp2 = _graphql_query(
        """
        query($id: Int!) {
            product(id: $id) {
                id
                sku
                name
                price
            }
        }
        """,
        {"id": product_id},
    )
    assert resp2.status_code == 200
    data = resp2.json()["data"]["product"]
    assert data["sku"] == "SKU-GQL-PID"
    assert data["name"] == "Один товар"


def test_query_product_search():
    """Query products with search filter."""
    _seed_product(sku="SKU-GQL-SEARCH", name="Наушники Bluetooth", price=3000)

    resp = _graphql_query("""
        query {
            products(search: "Наушники") {
                id
                sku
                name
            }
        }
    """)
    assert resp.status_code == 200
    products = resp.json()["data"]["products"]
    assert any("Наушники" in p["name"] for p in products)


# ── Orders queries ───────────────────────────────────────────────────


def test_query_orders():
    """Query orders returns list."""
    oid = _seed_order("ORD-GQL-LIST")
    resp = _graphql_query("""
        query {
            orders(limit: 10) {
                id
                status
            }
        }
    """)
    assert resp.status_code == 200
    orders = resp.json()["data"]["orders"]
    assert any(o["id"] == "ORD-GQL-LIST" for o in orders)


def test_query_order_by_id():
    """Query order(id) returns single order."""
    oid = _seed_order("ORD-GQL-BYID")
    resp = _graphql_query(
        """
        query($id: ID!) {
            order(id: $id) {
                id
                status
            }
        }
        """,
        {"id": oid},
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["order"]
    assert data["id"] == oid
    assert data["status"] == "created"


# ── Warehouses query ─────────────────────────────────────────────────


def test_query_warehouses():
    """Query warehouses returns list."""
    resp = _graphql_query("""
        query {
            warehouses {
                id
                name
                location
            }
        }
    """)
    assert resp.status_code == 200
    warehouses = resp.json()["data"]["warehouses"]
    assert len(warehouses) >= 1


# ── Employees query ──────────────────────────────────────────────────


def test_query_employees():
    """Query employees returns list."""
    resp = _graphql_query("""
        query {
            employees {
                id
                name
                role
            }
        }
    """)
    assert resp.status_code == 200
    employees = resp.json()["data"]["employees"]
    assert len(employees) >= 1


def test_query_employees_by_role():
    """Query employees with role filter."""
    resp = _graphql_query("""
        query {
            employees(role: "picker") {
                id
                name
                role
            }
        }
    """)
    assert resp.status_code == 200
    employees = resp.json()["data"]["employees"]
    assert all(e["role"] == "picker" for e in employees)


# ── Mutations ────────────────────────────────────────────────────────


def test_mutation_create_order():
    """Mutation createOrder creates an order."""
    oid = f"ORD-GQL-NEW-{uuid.uuid4().hex[:6]}"
    resp = _graphql_query(
        """
        mutation($input: CreateOrderInput!) {
            createOrder(input: $input) {
                success
                order {
                    id
                    status
                }
                error
            }
        }
        """,
        {
            "input": {
                "orderId": oid,
                "items": [{"sku": "SKU-001", "qty": 2}],
                "deliveryAddress": "Москва, тест",
                "warehouseId": "WH-MSK-S",
            }
        },
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["createOrder"]
    assert data["success"] is True
    assert data["order"]["id"] == oid
    assert data["error"] is None


def test_mutation_create_order_duplicate():
    """Mutation createOrder returns error for duplicate."""
    oid = f"ORD-GQL-DUP-{uuid.uuid4().hex[:6]}"
    # Create first
    _seed_order(oid)
    resp = _graphql_query(
        """
        mutation($input: CreateOrderInput!) {
            createOrder(input: $input) {
                success
                error
            }
        }
        """,
        {
            "input": {
                "orderId": oid,
                "items": [{"sku": "SKU-001", "qty": 1}],
                "deliveryAddress": "Moscow",
                "warehouseId": "WH-MSK-S",
            }
        },
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["createOrder"]
    assert data["success"] is False
    assert data["error"] is not None


def test_mutation_update_order_status():
    """Mutation updateOrderStatus updates order status."""
    oid = _seed_order("ORD-GQL-UPDATE")
    resp = _graphql_query(
        """
        mutation($id: ID!, $status: String!) {
            updateOrderStatus(id: $id, status: $status) {
                success
                order {
                    id
                    status
                }
                error
            }
        }
        """,
        {"id": oid, "status": "PROCESSING"},
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["updateOrderStatus"]
    assert data["success"] is True
    assert data["order"]["status"] == "PROCESSING"


def test_mutation_update_order_status_invalid():
    """Mutation updateOrderStatus rejects invalid status."""
    oid = _seed_order("ORD-GQL-BAD")
    resp = _graphql_query(
        """
        mutation($id: ID!, $status: String!) {
            updateOrderStatus(id: $id, status: $status) {
                success
                error
            }
        }
        """,
        {"id": oid, "status": "INVALID_STATUS_XYZ"},
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["updateOrderStatus"]
    assert data["success"] is False
    assert "Invalid status" in data["error"]


def test_mutation_update_order_not_found():
    """Mutation updateOrderStatus returns error for non-existent order."""
    resp = _graphql_query(
        """
        mutation($id: ID!, $status: String!) {
            updateOrderStatus(id: $id, status: $status) {
                success
                error
            }
        }
        """,
        {"id": "ORD-NONEXISTENT-XYZ", "status": "PROCESSING"},
    )
    assert resp.status_code == 200
    data = resp.json()["data"]["updateOrderStatus"]
    assert data["success"] is False
    assert "not found" in data["error"].lower()
