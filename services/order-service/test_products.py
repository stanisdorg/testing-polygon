"""Tests for Products CRUD endpoints: POST/GET/PUT/DELETE /api/v1/products."""
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


def _clean_products():
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE sku IN (SELECT sku FROM products)")
    cur.execute("DELETE FROM order_items WHERE sku IN (SELECT sku FROM products)")
    cur.execute("DELETE FROM products")
    conn.commit()
    cur.close()
    conn.close()


def _seed_product(sku="SKU-TEST-001", name="Тестовый товар", price=1000.0):
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO products (sku, name, price) VALUES (%s, %s, %s) RETURNING id",
        (sku, name, price),
    )
    pid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return pid


# ── POST /api/v1/products ──────────────────────────────────────────

def test_create_product_success():
    """Happy path: создаём продукт, получаем 201 с корректной схемой."""
    response = client.post(
        "/api/v1/products",
        json={"sku": "SKU-NEW-001", "name": "Наушники", "price": 2990},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["sku"] == "SKU-NEW-001"
    assert data["name"] == "Наушники"
    assert data["price"] == 2990
    assert "id" in data
    assert "created_at" in data


def test_create_product_duplicate_sku():
    """Error: дубликат SKU → 409 Conflict."""
    _seed_product(sku="SKU-DUP-001", name="Первый", price=100)
    response = client.post(
        "/api/v1/products",
        json={"sku": "SKU-DUP-001", "name": "Второй", "price": 200},
    )
    assert response.status_code == 409


def test_create_product_missing_name():
    """Error: name не передан → 422."""
    response = client.post(
        "/api/v1/products",
        json={"sku": "SKU-NO-NAME", "price": 100},
    )
    assert response.status_code == 422


def test_create_product_zero_price():
    """Error: price = 0 → 422."""
    response = client.post(
        "/api/v1/products",
        json={"sku": "SKU-ZERO", "name": "Бесплатный", "price": 0},
    )
    assert response.status_code == 422


def test_create_product_negative_price():
    """Error: price < 0 → 422."""
    response = client.post(
        "/api/v1/products",
        json={"sku": "SKU-NEG", "name": "Отрицательный", "price": -50},
    )
    assert response.status_code == 422


# ── GET /api/v1/products ───────────────────────────────────────────

def test_list_products_success():
    """Happy path: список продуктов с пагинацией."""
    _clean_products()
    _seed_product(sku="SKU-LIST-001", name="Товар 1", price=100)
    _seed_product(sku="SKU-LIST-002", name="Товар 2", price=200)

    response = client.get("/api/v1/products")
    assert response.status_code == 200
    data = response.json()
    assert "products" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2
    assert len(data["products"]) >= 2


def test_list_products_pagination():
    """Edge case: page=1, limit=1 → только 1 продукт."""
    _clean_products()
    _seed_product(sku="SKU-PAGE-001", name="Товар A", price=100)
    _seed_product(sku="SKU-PAGE-002", name="Товар B", price=200)

    response = client.get("/api/v1/products?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["products"]) == 1
    assert data["page"] == 1
    assert data["total"] >= 2
    assert data["pages"] >= 2


def test_list_products_search():
    """Happy path: поиск по name/sku."""
    _clean_products()
    _seed_product(sku="SKU-SEARCH-001", name="Наушники Sony", price=5000)
    _seed_product(sku="SKU-SEARCH-002", name="Клавиатура Logitech", price=3000)

    response = client.get("/api/v1/products?search=Наушники")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["products"][0]["name"] == "Наушники Sony"


def test_list_products_empty_search():
    """Edge case: поиск которого нет → пустой список."""
    _clean_products()
    response = client.get("/api/v1/products?search=НЕСУЩЕСТВУЮЩИЙ")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["products"] == []


# ── GET /api/v1/products/{product_id} ──────────────────────────────

def test_get_product_success():
    """Happy path: получаем продукт по ID."""
    pid = _seed_product(sku="SKU-GET-001", name="Мышка", price=1500)
    response = client.get(f"/api/v1/products/{pid}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == pid
    assert data["sku"] == "SKU-GET-001"
    assert data["name"] == "Мышка"
    assert data["price"] == 1500


def test_get_product_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/products/999999")
    assert response.status_code == 404


# ── PUT /api/v1/products/{product_id} ──────────────────────────────

def test_update_product_success():
    """Happy path: полное обновление."""
    pid = _seed_product(sku="SKU-PUT-001", name="Старое имя", price=100)
    response = client.put(
        f"/api/v1/products/{pid}",
        json={"sku": "SKU-PUT-NEW", "name": "Новое имя", "price": 500},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["sku"] == "SKU-PUT-NEW"
    assert data["name"] == "Новое имя"
    assert data["price"] == 500

    # Проверяем что изменения сохранены в БД
    resp_get = client.get(f"/api/v1/products/{pid}")
    assert resp_get.json()["name"] == "Новое имя"


def test_update_product_partial():
    """Happy path: частичное обновление (только name)."""
    pid = _seed_product(sku="SKU-PARTIAL-001", name="До", price=100)
    response = client.put(
        f"/api/v1/products/{pid}",
        json={"name": "После"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "После"
    assert data["sku"] == "SKU-PARTIAL-001"  # не изменился
    assert data["price"] == 100  # не изменился


def test_update_product_not_found():
    """Error: обновление несуществующего продукта → 404."""
    response = client.put(
        "/api/v1/products/999999",
        json={"name": "Никого нет"},
    )
    assert response.status_code == 404


def test_update_product_duplicate_sku():
    """Error: обновление SKU на уже существующий → 409."""
    _seed_product(sku="SKU-EXIST-001", name="Существующий", price=100)
    pid = _seed_product(sku="SKU-EXIST-002", name="Другой", price=200)
    response = client.put(
        f"/api/v1/products/{pid}",
        json={"sku": "SKU-EXIST-001"},
    )
    assert response.status_code == 409


# ── DELETE /api/v1/products/{product_id} ───────────────────────────

def test_delete_product_success():
    """Happy path: удаляем продукт → 204."""
    pid = _seed_product(sku="SKU-DEL-001", name="На удаление", price=50)
    response = client.delete(f"/api/v1/products/{pid}")
    assert response.status_code == 204

    # Проверяем что продукт больше не возвращается
    resp_get = client.get(f"/api/v1/products/{pid}")
    assert resp_get.status_code == 404


def test_delete_product_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/products/999999")
    assert response.status_code == 404
