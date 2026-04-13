"""Tests for Payments CRUD endpoints: POST/GET/PUT/DELETE /api/v1/payments."""
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
    """Создаёт тестовый заказ и возвращает его ID."""
    if order_id is None:
        order_id = f"ORD-PAY-{uuid.uuid4().hex[:8]}"
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


def _seed_payment(order_id=None, amount=1000.0, status="requested"):
    """Создаёт тестовый платёж и возвращает его ID."""
    if order_id is None:
        order_id = _seed_order()
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO payments (order_id, amount, status) VALUES (%s, %s, %s) RETURNING id",
        (order_id, amount, status),
    )
    pid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return pid


def _clean_test_payments():
    """Удаляет тестовые платежи и заказы."""
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM payments WHERE order_id LIKE 'ORD-PAY-%'")
    cur.execute("DELETE FROM orders WHERE id LIKE 'ORD-PAY-%'")
    conn.commit()
    cur.close()
    conn.close()


# ── POST /api/v1/payments ──────────────────────────────────────────

def test_create_payment_success():
    """Happy path: создаём платёж, получаем 201, status = requested."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-001")
    response = client.post(
        "/api/v1/payments",
        json={"order_id": order_id, "amount": 5990},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["order_id"] == order_id
    assert data["amount"] == 5990
    assert data["status"] == "requested"
    assert "id" in data
    assert "created_at" in data


def test_create_payment_invalid_order():
    """Error: order_id не существует → 404."""
    response = client.post(
        "/api/v1/payments",
        json={"order_id": "ORD-NONEXISTENT", "amount": 100},
    )
    assert response.status_code == 404


def test_create_payment_zero_amount():
    """Error: amount = 0 → 422."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-ZERO")
    response = client.post(
        "/api/v1/payments",
        json={"order_id": order_id, "amount": 0},
    )
    assert response.status_code == 422


def test_create_payment_negative_amount():
    """Error: amount < 0 → 422."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-NEG")
    response = client.post(
        "/api/v1/payments",
        json={"order_id": order_id, "amount": -500},
    )
    assert response.status_code == 422


# ── GET /api/v1/payments ───────────────────────────────────────────

def test_list_payments_success():
    """Happy path: список платежей."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-LIST")
    _seed_payment(order_id=order_id, amount=1000, status="requested")
    _seed_payment(order_id=order_id, amount=2000, status="succeeded")

    response = client.get("/api/v1/payments")
    assert response.status_code == 200
    data = response.json()
    assert "payments" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2


def test_list_payments_filter_order_id():
    """Happy path: фильтр по order_id."""
    _clean_test_payments()
    oid1 = _seed_order("ORD-PAY-FILT1")
    oid2 = _seed_order("ORD-PAY-FILT2")
    _seed_payment(order_id=oid1, amount=1000, status="requested")
    _seed_payment(order_id=oid2, amount=2000, status="succeeded")

    response = client.get(f"/api/v1/payments?order_id={oid1}")
    assert response.status_code == 200
    data = response.json()
    assert all(p["order_id"] == oid1 for p in data["payments"])


def test_list_payments_filter_status():
    """Happy path: фильтр по status."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-STATUS")
    _seed_payment(order_id=order_id, amount=1000, status="requested")
    _seed_payment(order_id=order_id, amount=2000, status="succeeded")

    response = client.get("/api/v1/payments?status=succeeded")
    assert response.status_code == 200
    data = response.json()
    assert all(p["status"] == "succeeded" for p in data["payments"])


def test_list_payments_pagination():
    """Edge case: page=1, limit=1."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-PAGE")
    _seed_payment(order_id=order_id, amount=1000, status="requested")
    _seed_payment(order_id=order_id, amount=2000, status="succeeded")

    response = client.get("/api/v1/payments?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["payments"]) == 1
    assert data["total"] >= 2


# ── GET /api/v1/payments/{payment_id} ──────────────────────────────

def test_get_payment_success():
    """Happy path: получаем платёж по ID."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-GET")
    pid = _seed_payment(order_id=order_id, amount=3500, status="requested")
    response = client.get(f"/api/v1/payments/{pid}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == pid
    assert data["order_id"] == order_id
    assert data["amount"] == 3500
    assert data["status"] == "requested"


def test_get_payment_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/payments/999999")
    assert response.status_code == 404


# ── PUT /api/v1/payments/{payment_id}/status ───────────────────────

def test_update_payment_status_to_succeeded():
    """Happy path: обновляем статус на succeeded."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-PUT1")
    pid = _seed_payment(order_id=order_id, amount=1000, status="requested")
    response = client.put(
        f"/api/v1/payments/{pid}/status",
        json={"status": "succeeded"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "succeeded"

    # Проверяем что изменения сохранены
    resp_get = client.get(f"/api/v1/payments/{pid}")
    assert resp_get.json()["status"] == "succeeded"


def test_update_payment_status_to_failed():
    """Happy path: обновляем статус на failed."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-PUT2")
    pid = _seed_payment(order_id=order_id, amount=1000, status="requested")
    response = client.put(
        f"/api/v1/payments/{pid}/status",
        json={"status": "failed"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "failed"


def test_update_payment_status_to_refunded():
    """Happy path: обновляем статус на refunded."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-PUT3")
    pid = _seed_payment(order_id=order_id, amount=1000, status="succeeded")
    response = client.put(
        f"/api/v1/payments/{pid}/status",
        json={"status": "refunded"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "refunded"


def test_update_payment_status_invalid():
    """Error: invalid status → 422."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-PUT4")
    pid = _seed_payment(order_id=order_id, amount=1000, status="requested")
    response = client.put(
        f"/api/v1/payments/{pid}/status",
        json={"status": "invalid_status"},
    )
    assert response.status_code == 422


def test_update_payment_status_not_found():
    """Error: несуществующий платёж → 404."""
    response = client.put(
        "/api/v1/payments/999999/status",
        json={"status": "succeeded"},
    )
    assert response.status_code == 404


# ── DELETE /api/v1/payments/{payment_id} ───────────────────────────

def test_delete_payment_requested():
    """Happy path: удаляем платёж со статусом requested → 204."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-DEL1")
    pid = _seed_payment(order_id=order_id, amount=500, status="requested")
    response = client.delete(f"/api/v1/payments/{pid}")
    assert response.status_code == 204

    resp_get = client.get(f"/api/v1/payments/{pid}")
    assert resp_get.status_code == 404


def test_delete_payment_failed():
    """Happy path: удаляем платёж со статусом failed → 204."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-DEL2")
    pid = _seed_payment(order_id=order_id, amount=500, status="failed")
    response = client.delete(f"/api/v1/payments/{pid}")
    assert response.status_code == 204


def test_delete_payment_succeeded():
    """Error: нельзя удалить succeeded → 409."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-DEL3")
    pid = _seed_payment(order_id=order_id, amount=500, status="succeeded")
    response = client.delete(f"/api/v1/payments/{pid}")
    assert response.status_code == 409


def test_delete_payment_refunded():
    """Error: нельзя refunded → 409."""
    _clean_test_payments()
    order_id = _seed_order("ORD-PAY-DEL4")
    pid = _seed_payment(order_id=order_id, amount=500, status="refunded")
    response = client.delete(f"/api/v1/payments/{pid}")
    assert response.status_code == 409


def test_delete_payment_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/payments/999999")
    assert response.status_code == 404
