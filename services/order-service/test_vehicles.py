"""Tests for Vehicles CRUD endpoints: POST/GET/PUT/DELETE /api/v1/vehicles."""
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


def _clean_test_vehicles():
    """Удаляет тестовые автомобили."""
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM vehicles WHERE plate_number LIKE 'TEST-%'")
    conn.commit()
    cur.close()
    conn.close()


def _seed_vehicle(plate="TEST-A001AA", capacity=500.0):
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO vehicles (plate_number, capacity) VALUES (%s, %s) RETURNING id",
        (plate, capacity),
    )
    vid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return vid


# ── POST /api/v1/vehicles ──────────────────────────────────────────

def test_create_vehicle_success():
    """Happy path: создаём авто, получаем 201."""
    _clean_test_vehicles()
    response = client.post(
        "/api/v1/vehicles",
        json={"plate_number": "TEST-A123BC77", "capacity": 500},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["plate_number"] == "TEST-A123BC77"
    assert data["capacity"] == 500
    assert "id" in data
    assert "created_at" in data


def test_create_vehicle_duplicate_plate():
    """Error: duplicate plate_number → 409."""
    _clean_test_vehicles()
    _seed_vehicle(plate="TEST-DUP001", capacity=300)
    response = client.post(
        "/api/v1/vehicles",
        json={"plate_number": "TEST-DUP001", "capacity": 400},
    )
    assert response.status_code == 409


def test_create_vehicle_missing_plate():
    """Error: plate_number не передан → 422."""
    response = client.post(
        "/api/v1/vehicles",
        json={"capacity": 500},
    )
    assert response.status_code == 422


def test_create_vehicle_zero_capacity():
    """Error: capacity = 0 → 422."""
    response = client.post(
        "/api/v1/vehicles",
        json={"plate_number": "TEST-ZERO", "capacity": 0},
    )
    assert response.status_code == 422


def test_create_vehicle_negative_capacity():
    """Error: capacity < 0 → 422."""
    response = client.post(
        "/api/v1/vehicles",
        json={"plate_number": "TEST-NEG", "capacity": -100},
    )
    assert response.status_code == 422


# ── GET /api/v1/vehicles ───────────────────────────────────────────

def test_list_vehicles_success():
    """Happy path: список автомобилей."""
    _clean_test_vehicles()
    _seed_vehicle(plate="TEST-LIST01", capacity=300)
    _seed_vehicle(plate="TEST-LIST02", capacity=600)

    response = client.get("/api/v1/vehicles")
    assert response.status_code == 200
    data = response.json()
    assert "vehicles" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2


def test_list_vehicles_pagination():
    """Edge case: page=1, limit=1."""
    _clean_test_vehicles()
    _seed_vehicle(plate="TEST-PAGE01", capacity=400)
    _seed_vehicle(plate="TEST-PAGE02", capacity=500)

    response = client.get("/api/v1/vehicles?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["vehicles"]) == 1
    assert data["page"] == 1
    assert data["total"] >= 2


def test_list_vehicles_filter_min_capacity():
    """Happy path: фильтр по min_capacity."""
    _clean_test_vehicles()
    _seed_vehicle(plate="TEST-CAP01", capacity=200)
    _seed_vehicle(plate="TEST-CAP02", capacity=800)

    response = client.get("/api/v1/vehicles?min_capacity=500")
    assert response.status_code == 200
    data = response.json()
    assert all(v["capacity"] >= 500 for v in data["vehicles"])


# ── GET /api/v1/vehicles/{vehicle_id} ──────────────────────────────

def test_get_vehicle_success():
    """Happy path: получаем авто по ID."""
    vid = _seed_vehicle(plate="TEST-GET001", capacity=750)
    response = client.get(f"/api/v1/vehicles/{vid}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == vid
    assert data["plate_number"] == "TEST-GET001"
    assert data["capacity"] == 750


def test_get_vehicle_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/vehicles/999999")
    assert response.status_code == 404


# ── PUT /api/v1/vehicles/{vehicle_id} ──────────────────────────────

def test_update_vehicle_success():
    """Happy path: полное обновление."""
    vid = _seed_vehicle(plate="TEST-PUT001", capacity=300)
    response = client.put(
        f"/api/v1/vehicles/{vid}",
        json={"plate_number": "TEST-PUT001-NEW", "capacity": 900},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["plate_number"] == "TEST-PUT001-NEW"
    assert data["capacity"] == 900

    # Проверяем что изменения сохранены
    resp_get = client.get(f"/api/v1/vehicles/{vid}")
    assert resp_get.json()["capacity"] == 900


def test_update_vehicle_partial():
    """Happy path: частичное обновление (только capacity)."""
    vid = _seed_vehicle(plate="TEST-PART01", capacity=400)
    response = client.put(
        f"/api/v1/vehicles/{vid}",
        json={"capacity": 600},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["capacity"] == 600
    assert data["plate_number"] == "TEST-PART01"  # не изменился


def test_update_vehicle_not_found():
    """Error: обновление несуществующего → 404."""
    response = client.put(
        "/api/v1/vehicles/999999",
        json={"capacity": 100},
    )
    assert response.status_code == 404


def test_update_vehicle_duplicate_plate():
    """Error: обновление plate_number на уже существующий → 409."""
    _clean_test_vehicles()
    _seed_vehicle(plate="TEST-EXIST-A", capacity=300)
    vid = _seed_vehicle(plate="TEST-EXIST-B", capacity=400)
    response = client.put(
        f"/api/v1/vehicles/{vid}",
        json={"plate_number": "TEST-EXIST-A"},
    )
    assert response.status_code == 409


def test_update_vehicle_zero_capacity():
    """Error: capacity = 0 → 422."""
    vid = _seed_vehicle(plate="TEST-ZERO01", capacity=300)
    response = client.put(
        f"/api/v1/vehicles/{vid}",
        json={"capacity": 0},
    )
    assert response.status_code == 422


# ── DELETE /api/v1/vehicles/{vehicle_id} ───────────────────────────

def test_delete_vehicle_success():
    """Happy path: удаляем авто → 204."""
    vid = _seed_vehicle(plate="TEST-DEL001", capacity=250)
    response = client.delete(f"/api/v1/vehicles/{vid}")
    assert response.status_code == 204

    resp_get = client.get(f"/api/v1/vehicles/{vid}")
    assert resp_get.status_code == 404


def test_delete_vehicle_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/vehicles/999999")
    assert response.status_code == 404


def test_delete_vehicle_double_delete():
    """Error: повторное удаление → 404."""
    vid = _seed_vehicle(plate="TEST-DBLDEL", capacity=100)
    resp1 = client.delete(f"/api/v1/vehicles/{vid}")
    assert resp1.status_code == 204
    resp2 = client.delete(f"/api/v1/vehicles/{vid}")
    assert resp2.status_code == 404
