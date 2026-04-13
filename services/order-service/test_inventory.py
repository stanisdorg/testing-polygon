"""Tests for Inventory CRUD endpoints: POST/GET/PUT/DELETE + stock operations."""
import uuid
import psycopg2
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def _get_db_conn():
    import os
    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
    )
    return psycopg2.connect(db_url)


def _clean_test_inventory():
    """Удаляет тестовые записи inventory и гарантирует наличие test products."""
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE sku LIKE 'SKU-INV-%' OR sku LIKE 'SKU-TEST-%'")
    # Ensure test products exist
    cur.execute("INSERT INTO products (sku, name, price) VALUES ('SKU-INV-001', 'Тестовый товар 1', 100), ('SKU-INV-002', 'Тестовый товар 2', 200) ON CONFLICT DO NOTHING")
    conn.commit()
    cur.close()
    conn.close()


def _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100, reserved_qty=0):
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO inventory (sku, warehouse_id, available_qty, reserved_qty) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (sku, warehouse_id) DO UPDATE SET available_qty = %s, reserved_qty = %s RETURNING id",
        (sku, warehouse_id, available_qty, reserved_qty, available_qty, reserved_qty),
    )
    inv_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return inv_id


# ── POST /api/v1/inventory ──────────────────────────────────────────

def test_create_inventory_success():
    """Happy path: создаём запись inventory, получаем 201."""
    _clean_test_inventory()
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-INV-001", "warehouse_id": "WH-MSK-S", "available_qty": 100, "reserved_qty": 0},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["sku"] == "SKU-INV-001"
    assert data["warehouse_id"] == "WH-MSK-S"
    assert data["available_qty"] == 100
    assert data["reserved_qty"] == 0
    assert "id" in data


def test_create_inventory_duplicate():
    """Error: duplicate (sku, warehouse_id) → 409."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=50)
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-INV-001", "warehouse_id": "WH-MSK-S", "available_qty": 30, "reserved_qty": 0},
    )
    assert response.status_code == 409


def test_create_inventory_invalid_sku():
    """Error: sku не существует в products → 404."""
    _clean_test_inventory()
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-NONEXISTENT", "warehouse_id": "WH-MSK-S", "available_qty": 10, "reserved_qty": 0},
    )
    assert response.status_code == 404


def test_create_inventory_invalid_warehouse():
    """Error: warehouse_id не существует → 404."""
    _clean_test_inventory()
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-INV-001", "warehouse_id": "WH-NONEXISTENT", "available_qty": 10, "reserved_qty": 0},
    )
    assert response.status_code == 404


def test_create_inventory_negative_qty():
    """Error: negative available_qty → 422."""
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-INV-001", "warehouse_id": "WH-MSK-S", "available_qty": -10, "reserved_qty": 0},
    )
    assert response.status_code == 422


def test_create_inventory_negative_reserved():
    """Error: negative reserved_qty → 422."""
    response = client.post(
        "/api/v1/inventory",
        json={"sku": "SKU-INV-001", "warehouse_id": "WH-MSK-S", "available_qty": 10, "reserved_qty": -5},
    )
    assert response.status_code == 422


# ── GET /api/v1/inventory ──────────────────────────────────────────

def test_list_inventory_success():
    """Happy path: список inventory."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100)
    _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-N", available_qty=200)

    response = client.get("/api/v1/inventory")
    assert response.status_code == 200
    data = response.json()
    assert "inventory" in data
    assert "total" in data
    assert data["total"] >= 2


def test_list_inventory_filter_sku():
    """Happy path: фильтр по sku."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100)
    _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-N", available_qty=200)

    response = client.get("/api/v1/inventory?sku=SKU-INV-001")
    assert response.status_code == 200
    data = response.json()
    assert all(i["sku"] == "SKU-INV-001" for i in data["inventory"])


def test_list_inventory_filter_warehouse():
    """Happy path: фильтр по warehouse_id."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100)
    _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-N", available_qty=200)

    response = client.get("/api/v1/inventory?warehouse_id=WH-MSK-N")
    assert response.status_code == 200
    data = response.json()
    assert all(i["warehouse_id"] == "WH-MSK-N" for i in data["inventory"])


def test_list_inventory_pagination():
    """Edge case: page=1, limit=1."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100)
    _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-N", available_qty=200)

    response = client.get("/api/v1/inventory?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["inventory"]) == 1
    assert data["total"] >= 2


# ── GET /api/v1/inventory/{inventory_id} ───────────────────────────

def test_get_inventory_success():
    """Happy path: получаем запись по ID."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-KZN", available_qty=150, reserved_qty=25)
    response = client.get(f"/api/v1/inventory/{inv_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == inv_id
    assert data["sku"] == "SKU-INV-001"
    assert data["warehouse_id"] == "WH-KZN"
    assert data["available_qty"] == 150
    assert data["reserved_qty"] == 25


def test_get_inventory_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/inventory/999999")
    assert response.status_code == 404


# ── PUT /api/v1/inventory/{inventory_id} ───────────────────────────

def test_update_inventory_success():
    """Happy path: обновление available_qty."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100)
    response = client.put(
        f"/api/v1/inventory/{inv_id}",
        json={"available_qty": 200},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available_qty"] == 200


def test_update_inventory_negative_qty():
    """Error: negative qty → 422."""
    inv_id = _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-S", available_qty=100)
    response = client.put(
        f"/api/v1/inventory/{inv_id}",
        json={"available_qty": -10},
    )
    assert response.status_code == 422


def test_update_inventory_not_found():
    """Error: обновление несуществующего → 404."""
    response = client.put(
        "/api/v1/inventory/999999",
        json={"available_qty": 100},
    )
    assert response.status_code == 404


# ── DELETE /api/v1/inventory/{inventory_id} ────────────────────────

def test_delete_inventory_success():
    """Happy path: удаляем запись → 204."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-N", available_qty=50, reserved_qty=0)
    response = client.delete(f"/api/v1/inventory/{inv_id}")
    assert response.status_code == 204

    resp_get = client.get(f"/api/v1/inventory/{inv_id}")
    assert resp_get.status_code == 404


def test_delete_inventory_with_reserved():
    """Error: reserved_qty > 0 → 409."""
    inv_id = _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-KZN", available_qty=100, reserved_qty=10)
    response = client.delete(f"/api/v1/inventory/{inv_id}")
    assert response.status_code == 409


def test_delete_inventory_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/inventory/999999")
    assert response.status_code == 404


# ── POST /api/v1/inventory/{inventory_id}/add-stock ────────────────

def test_add_stock_success():
    """Happy path: добавляем stock."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100, reserved_qty=0)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/add-stock",
        json={"quantity": 50},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available_qty"] == 150


def test_add_stock_negative_quantity():
    """Error: quantity <= 0 → 422."""
    inv_id = _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-S", available_qty=100)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/add-stock",
        json={"quantity": -10},
    )
    assert response.status_code == 422


def test_add_stock_zero_quantity():
    """Error: quantity = 0 → 422."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-KZN", available_qty=100)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/add-stock",
        json={"quantity": 0},
    )
    assert response.status_code == 422


# ── POST /api/v1/inventory/{inventory_id}/reserve ──────────────────

def test_reserve_success():
    """Happy path: резервируем товар."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100, reserved_qty=0)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/reserve",
        json={"quantity": 10},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available_qty"] == 90
    assert data["reserved_qty"] == 10


def test_reserve_not_enough_stock():
    """Error: available_qty < quantity → 409."""
    inv_id = _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-S", available_qty=5, reserved_qty=0)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/reserve",
        json={"quantity": 100},
    )
    assert response.status_code == 409


def test_reserve_negative_quantity():
    """Error: quantity <= 0 → 422."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-N", available_qty=100)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/reserve",
        json={"quantity": -5},
    )
    assert response.status_code == 422


# ── POST /api/v1/inventory/{inventory_id}/release ──────────────────

def test_release_success():
    """Happy path: освобождаем резерв."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=90, reserved_qty=10)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/release",
        json={"quantity": 5},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["available_qty"] == 95
    assert data["reserved_qty"] == 5


def test_release_more_than_reserved():
    """Error: reserved_qty < quantity → 409."""
    inv_id = _seed_inventory(sku="SKU-INV-002", warehouse_id="WH-MSK-S", available_qty=100, reserved_qty=3)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/release",
        json={"quantity": 100},
    )
    assert response.status_code == 409


def test_release_negative_quantity():
    """Error: quantity <= 0 → 422."""
    inv_id = _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-KZN", available_qty=100, reserved_qty=10)
    response = client.post(
        f"/api/v1/inventory/{inv_id}/release",
        json={"quantity": -1},
    )
    assert response.status_code == 422


# ── GET /api/v1/inventory/sku/{sku}/available ──────────────────────

def test_get_sku_available_success():
    """Happy path: доступное кол-во по SKU на всех складах."""
    _clean_test_inventory()
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-S", available_qty=100, reserved_qty=10)
    _seed_inventory(sku="SKU-INV-001", warehouse_id="WH-MSK-N", available_qty=200, reserved_qty=20)

    response = client.get("/api/v1/inventory/sku/SKU-INV-001/available")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2
    wh_map = {item["warehouse_id"]: item for item in data}
    assert wh_map["WH-MSK-S"]["available_qty"] == 100
    assert wh_map["WH-MSK-N"]["available_qty"] == 200


def test_get_sku_available_not_found():
    """Edge case: sku не существует → пустой список."""
    response = client.get("/api/v1/inventory/sku/SKU-NONEXISTENT/available")
    assert response.status_code == 200
    data = response.json()
    assert data == []
