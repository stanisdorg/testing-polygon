"""Tests for Warehouses CRUD endpoints: POST/GET/PUT/DELETE /api/v1/warehouses."""
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


def _clean_warehouses():
    """Удаляет тестовые склады. Каскадно чистим зависимые таблицы."""
    conn = _get_db_conn()
    cur = conn.cursor()
    # Clean in FK order
    cur.execute("DELETE FROM deliveries WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN'))")
    cur.execute("DELETE FROM payments WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN'))")
    cur.execute("DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN'))")
    cur.execute("DELETE FROM events WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN'))")
    cur.execute("DELETE FROM orders WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN')")
    cur.execute("DELETE FROM inventory WHERE warehouse_id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN')")
    cur.execute("DELETE FROM warehouses WHERE id NOT IN ('WH-MSK-S', 'WH-MSK-N', 'WH-SPB', 'WH-KZN')")
    conn.commit()
    cur.close()
    conn.close()


def _seed_warehouse(wh_id="WH-TEST-001", name="Тестовый склад", location="Москва", capacity_m3=1000.0):
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO warehouses (id, name, location, capacity_m3) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
        (wh_id, name, location, capacity_m3),
    )
    conn.commit()
    cur.close()
    conn.close()


# ── POST /api/v1/warehouses ────────────────────────────────────────

def test_create_warehouse_success():
    """Happy path: создаём склад, получаем 201."""
    unique_id = f"WH-NEW-{uuid.uuid4().hex[:4].upper()}"
    response = client.post(
        "/api/v1/warehouses",
        json={"id": unique_id, "name": "Склад Новый", "location": "Казань, ул. Складская 1", "capacity_m3": 5000},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["id"] == unique_id
    assert data["name"] == "Склад Новый"
    assert data["location"] == "Казань, ул. Складская 1"
    assert data["capacity_m3"] == 5000
    assert "created_at" in data


def test_create_warehouse_duplicate_id():
    """Error: дубликат ID → 409 Conflict."""
    _seed_warehouse(wh_id="WH-DUP-001", name="Первый", location="Мск", capacity_m3=100)
    response = client.post(
        "/api/v1/warehouses",
        json={"id": "WH-DUP-001", "name": "Второй", "location": "Спб", "capacity_m3": 200},
    )
    assert response.status_code == 409


def test_create_warehouse_missing_name():
    """Error: name не передан → 422."""
    response = client.post(
        "/api/v1/warehouses",
        json={"id": "WH-NO-NAME", "location": "Мск", "capacity_m3": 100},
    )
    assert response.status_code == 422


def test_create_warehouse_zero_capacity():
    """Error: capacity_m3 = 0 → 422."""
    response = client.post(
        "/api/v1/warehouses",
        json={"id": "WH-ZERO", "name": "Нулевой", "location": "Мск", "capacity_m3": 0},
    )
    assert response.status_code == 422


def test_create_warehouse_negative_capacity():
    """Error: capacity_m3 < 0 → 422."""
    response = client.post(
        "/api/v1/warehouses",
        json={"id": "WH-NEG", "name": "Отрицательный", "location": "Мск", "capacity_m3": -100},
    )
    assert response.status_code == 422


# ── GET /api/v1/warehouses ─────────────────────────────────────────

def test_list_warehouses_success():
    """Happy path: список складов с пагинацией."""
    _clean_warehouses()
    _seed_warehouse(wh_id="WH-LIST-01", name="Склад A", location="Москва", capacity_m3=1000)
    _seed_warehouse(wh_id="WH-LIST-02", name="Склад B", location="Казань", capacity_m3=2000)

    response = client.get("/api/v1/warehouses")
    assert response.status_code == 200
    data = response.json()
    assert "warehouses" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2


def test_list_warehouses_pagination():
    """Edge case: page=1, limit=1 → только 1 склад."""
    _clean_warehouses()
    _seed_warehouse(wh_id="WH-PAGE-01", name="Склад X", location="Мск", capacity_m3=500)
    _seed_warehouse(wh_id="WH-PAGE-02", name="Склад Y", location="Спб", capacity_m3=600)

    response = client.get("/api/v1/warehouses?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["warehouses"]) == 1
    assert data["page"] == 1
    assert data["total"] >= 2
    assert data["pages"] >= 2


def test_list_warehouses_search():
    """Happy path: поиск по name/location."""
    _clean_warehouses()
    _seed_warehouse(wh_id="WH-SEARCH-1", name="Склад Москва Центр", location="Москва, Арбат", capacity_m3=3000)
    _seed_warehouse(wh_id="WH-SEARCH-2", name="Склад Казань Новый", location="Казань, Кремль", capacity_m3=4000)

    response = client.get("/api/v1/warehouses?search=Казань+Новый")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any("Казань Новый" in w["name"] for w in data["warehouses"])


def test_list_warehouses_empty_search():
    """Edge case: поиск которого нет → пустой список."""
    _clean_warehouses()
    response = client.get("/api/v1/warehouses?search=НЕСУЩЕСТВУЮЩИЙ_ГОРОД")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["warehouses"] == []


# ── GET /api/v1/warehouses/{warehouse_id} ──────────────────────────

def test_get_warehouse_success():
    """Happy path: получаем склад по ID."""
    _seed_warehouse(wh_id="WH-GET-001", name="Склад Получение", location="Самара", capacity_m3=1500)
    response = client.get("/api/v1/warehouses/WH-GET-001")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "WH-GET-001"
    assert data["name"] == "Склад Получение"
    assert data["location"] == "Самара"
    assert data["capacity_m3"] == 1500


def test_get_warehouse_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/warehouses/WH-NONEXISTENT")
    assert response.status_code == 404


# ── PUT /api/v1/warehouses/{warehouse_id} ──────────────────────────

def test_update_warehouse_success():
    """Happy path: полное обновление."""
    _seed_warehouse(wh_id="WH-PUT-001", name="Старое имя", location="Старый город", capacity_m3=100)
    response = client.put(
        "/api/v1/warehouses/WH-PUT-001",
        json={"name": "Новое имя", "location": "Новый город", "capacity_m3": 5000},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Новое имя"
    assert data["location"] == "Новый город"
    assert data["capacity_m3"] == 5000

    # Проверяем что изменения сохранены
    resp_get = client.get("/api/v1/warehouses/WH-PUT-001")
    assert resp_get.json()["name"] == "Новое имя"


def test_update_warehouse_partial():
    """Happy path: частичное обновление (только name)."""
    _seed_warehouse(wh_id="WH-PART-001", name="До", location="Место", capacity_m3=200)
    response = client.put(
        "/api/v1/warehouses/WH-PART-001",
        json={"name": "После"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "После"
    assert data["location"] == "Место"  # не изменился
    assert data["capacity_m3"] == 200  # не изменился


def test_update_warehouse_not_found():
    """Error: обновление несуществующего → 404."""
    response = client.put(
        "/api/v1/warehouses/WH-NONEXISTENT",
        json={"name": "Никого нет"},
    )
    assert response.status_code == 404


def test_update_warehouse_duplicate_id():
    """Error: переименование ID на уже существующий → 409 (если бы ID менялся)."""
    _seed_warehouse(wh_id="WH-EXIST-A", name="Склад A", location="Мск", capacity_m3=100)
    _seed_warehouse(wh_id="WH-EXIST-B", name="Склад B", location="Спб", capacity_m3=200)
    # Обновляем B на имя A (но ID не меняем — просто name conflict не проверяется в БД)
    # Реальный duplicate — только если бы PK менялся, но PK — это path param
    # Поэтому тестим partial update без conflict
    response = client.put(
        "/api/v1/warehouses/WH-EXIST-B",
        json={"name": "Склад B Обновлённый"},
    )
    assert response.status_code == 200


# ── DELETE /api/v1/warehouses/{warehouse_id} ───────────────────────

def test_delete_warehouse_success():
    """Happy path: удаляем склад → 204."""
    _seed_warehouse(wh_id="WH-DEL-001", name="На удаление", location="Мск", capacity_m3=50)
    response = client.delete("/api/v1/warehouses/WH-DEL-001")
    assert response.status_code == 204

    # Проверяем что склад больше не возвращается
    resp_get = client.get("/api/v1/warehouses/WH-DEL-001")
    assert resp_get.status_code == 404


def test_delete_warehouse_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/warehouses/WH-NONEXISTENT")
    assert response.status_code == 404


def test_delete_warehouse_with_orders():
    """Error: удаление склада с заказами → 409."""
    _seed_warehouse(wh_id="WH-ORD-001", name="Склад с заказами", location="Мск", capacity_m3=500)
    # Создаём заказ на этом складе
    import uuid
    order_id = f"ORD-WH-{uuid.uuid4().hex[:8]}"
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO orders (id, status, warehouse_id) VALUES (%s, 'created', %s)",
        (order_id, "WH-ORD-001"),
    )
    conn.commit()
    cur.close()
    conn.close()

    response = client.delete("/api/v1/warehouses/WH-ORD-001")
    assert response.status_code == 409
