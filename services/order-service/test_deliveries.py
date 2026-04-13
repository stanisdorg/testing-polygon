"""Tests for Deliveries CRUD endpoints: POST/GET/PUT/DELETE + cancel."""
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


def _seed_order(order_id=None):
    if order_id is None:
        order_id = f"ORD-DEL-{uuid.uuid4().hex[:8]}"
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO orders (id, status, warehouse_id) VALUES (%s, 'created', 'WH-MSK-S') ON CONFLICT (id) DO NOTHING",
        (order_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    return order_id


def _seed_delivery(order_id=None, courier_name="Тест Курьер", status="assigned", cancelled=False):
    if order_id is None:
        order_id = _seed_order()
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO deliveries (order_id, courier_name, status, cancelled) VALUES (%s, %s, %s, %s) RETURNING id",
        (order_id, courier_name, status, cancelled),
    )
    did = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return did


def _clean_test_deliveries():
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM deliveries WHERE order_id LIKE 'ORD-DEL-%'")
    cur.execute("DELETE FROM orders WHERE id LIKE 'ORD-DEL-%'")
    conn.commit()
    cur.close()
    conn.close()


# ── POST /api/v1/deliveries ────────────────────────────────────────

def test_create_delivery_success():
    """Happy path: создаём доставку, status=assigned, cancelled=false."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-001")
    response = client.post(
        "/api/v1/deliveries",
        json={"order_id": order_id, "courier_name": "Иван Иванов"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["order_id"] == order_id
    assert data["courier_name"] == "Иван Иванов"
    assert data["status"] == "assigned"
    assert data["cancelled"] is False
    assert "id" in data
    assert "created_at" in data


def test_create_delivery_invalid_order():
    """Error: order_id не существует → 404."""
    response = client.post(
        "/api/v1/deliveries",
        json={"order_id": "ORD-NONEXISTENT", "courier_name": "Тест"},
    )
    assert response.status_code == 404


def test_create_delivery_missing_courier():
    """Error: courier_name не передан → 422."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-NOCOUR")
    response = client.post(
        "/api/v1/deliveries",
        json={"order_id": order_id},
    )
    assert response.status_code == 422


# ── GET /api/v1/deliveries ─────────────────────────────────────────

def test_list_deliveries_success():
    """Happy path: список доставок."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-LIST")
    _seed_delivery(order_id=order_id, courier_name="Курьер A", status="assigned")
    _seed_delivery(order_id=order_id, courier_name="Курьер B", status="in_transit")

    response = client.get("/api/v1/deliveries")
    assert response.status_code == 200
    data = response.json()
    assert "deliveries" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2


def test_list_deliveries_filter_status():
    """Happy path: фильтр по status."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-STAT")
    _seed_delivery(order_id=order_id, courier_name="Курьер 1", status="assigned")
    _seed_delivery(order_id=order_id, courier_name="Курьер 2", status="delivered")

    response = client.get("/api/v1/deliveries?status=delivered")
    assert response.status_code == 200
    data = response.json()
    assert all(d["status"] == "delivered" for d in data["deliveries"])


def test_list_deliveries_filter_courier():
    """Happy path: фильтр по courier_name."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-COUR")
    _seed_delivery(order_id=order_id, courier_name="Уникальный Курьер", status="assigned")
    _seed_delivery(order_id=order_id, courier_name="Другой Курьер", status="assigned")

    response = client.get("/api/v1/deliveries?courier_name=Уникальный Курьер")
    assert response.status_code == 200
    data = response.json()
    assert all("Уникальный Курьер" in d["courier_name"] for d in data["deliveries"])


def test_list_deliveries_pagination():
    """Edge case: page=1, limit=1."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-PAGE")
    _seed_delivery(order_id=order_id, courier_name="Курьер X", status="assigned")
    _seed_delivery(order_id=order_id, courier_name="Курьер Y", status="assigned")

    response = client.get("/api/v1/deliveries?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["deliveries"]) == 1
    assert data["total"] >= 2


# ── GET /api/v1/deliveries/{delivery_id} ───────────────────────────

def test_get_delivery_success():
    """Happy path: получаем доставку по ID."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-GET")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер GET", status="in_transit")
    response = client.get(f"/api/v1/deliveries/{did}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == did
    assert data["courier_name"] == "Курьер GET"
    assert data["status"] == "in_transit"
    assert data["cancelled"] is False


def test_get_delivery_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/deliveries/999999")
    assert response.status_code == 404


# ── PUT /api/v1/deliveries/{delivery_id}/status ────────────────────

def test_update_delivery_status_to_in_transit():
    """Happy path: assigned -> in_transit."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-PUT1")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер 1", status="assigned")
    response = client.put(
        f"/api/v1/deliveries/{did}/status",
        json={"status": "in_transit"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "in_transit"


def test_update_delivery_status_to_delivered():
    """Happy path: in_transit -> delivered."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-PUT2")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер 2", status="in_transit")
    response = client.put(
        f"/api/v1/deliveries/{did}/status",
        json={"status": "delivered"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "delivered"


def test_update_delivery_status_invalid():
    """Error: invalid status → 422."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-PUT3")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер 3", status="assigned")
    response = client.put(
        f"/api/v1/deliveries/{did}/status",
        json={"status": "invalid_status"},
    )
    assert response.status_code == 422


def test_update_delivery_status_not_found():
    """Error: несуществующая доставка → 404."""
    response = client.put(
        "/api/v1/deliveries/999999/status",
        json={"status": "in_transit"},
    )
    assert response.status_code == 404


# ── POST /api/v1/deliveries/{delivery_id}/cancel ───────────────────

def test_cancel_delivery_assigned():
    """Happy path: отменяем доставку со статусом assigned."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-CAN1")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер CAN", status="assigned")
    response = client.post(
        f"/api/v1/deliveries/{did}/cancel",
        json={"reason": "Customer unavailable"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["cancelled"] is True
    assert data["status"] == "cancelled"


def test_cancel_delivery_in_transit():
    """Happy path: отменяем доставку со статусом in_transit."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-CAN2")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер CAN2", status="in_transit")
    response = client.post(
        f"/api/v1/deliveries/{did}/cancel",
        json={},
    )
    assert response.status_code == 200
    assert response.json()["cancelled"] is True


def test_cancel_already_delivered():
    """Error: нельзя отменить доставленную → 409."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-CAN3")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер CAN3", status="delivered")
    response = client.post(
        f"/api/v1/deliveries/{did}/cancel",
        json={"reason": "test"},
    )
    assert response.status_code == 409


def test_cancel_already_cancelled():
    """Error: нельзя отменить уже отменённую → 409."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-CAN4")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер CAN4", status="assigned", cancelled=True)
    response = client.post(
        f"/api/v1/deliveries/{did}/cancel",
        json={"reason": "test"},
    )
    assert response.status_code == 409


def test_cancel_not_found():
    """Error: несуществующая доставка → 404."""
    response = client.post(
        "/api/v1/deliveries/999999/cancel",
        json={"reason": "test"},
    )
    assert response.status_code == 404


# ── DELETE /api/v1/deliveries/{delivery_id} ────────────────────────

def test_delete_delivery_assigned():
    """Happy path: удаляем доставку со статусом assigned → 204."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-DEL1")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер DEL", status="assigned")
    response = client.delete(f"/api/v1/deliveries/{did}")
    assert response.status_code == 204

    resp_get = client.get(f"/api/v1/deliveries/{did}")
    assert resp_get.status_code == 404


def test_delete_delivery_cancelled():
    """Happy path: удаляем отменённую доставку → 204."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-DEL2")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер DEL2", status="cancelled", cancelled=True)
    response = client.delete(f"/api/v1/deliveries/{did}")
    assert response.status_code == 204


def test_delete_delivery_delivered():
    """Error: нельзя удалить доставленную → 409."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-DEL3")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер DEL3", status="delivered")
    response = client.delete(f"/api/v1/deliveries/{did}")
    assert response.status_code == 409


def test_delete_delivery_in_transit():
    """Error: нельзя удалить в пути → 409."""
    _clean_test_deliveries()
    order_id = _seed_order("ORD-DEL-DEL4")
    did = _seed_delivery(order_id=order_id, courier_name="Курьер DEL4", status="in_transit")
    response = client.delete(f"/api/v1/deliveries/{did}")
    assert response.status_code == 409


def test_delete_delivery_not_found():
    """Error: удаление несуществующей → 404."""
    response = client.delete("/api/v1/deliveries/999999")
    assert response.status_code == 404
