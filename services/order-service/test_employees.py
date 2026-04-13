"""Tests for Employees CRUD endpoints: POST/GET/PUT/DELETE /api/v1/employees."""
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


def _clean_test_employees():
    """Удаляет тестовых сотрудников (с префиксом TEST-)."""
    conn = _get_db_conn()
    cur = conn.cursor()
    # deliveries.courier_name — текстовое поле, не FK к employees
    cur.execute("DELETE FROM employees WHERE name LIKE 'TEST-%'")
    conn.commit()
    cur.close()
    conn.close()


def _seed_employee(name="Тест Сотрудник", role="picker"):
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO employees (name, role) VALUES (%s, %s) RETURNING id",
        (name, role),
    )
    eid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return eid


VALID_ROLES = {"picker", "packer", "courier"}


# ── POST /api/v1/employees ─────────────────────────────────────────

def test_create_employee_success():
    """Happy path: создаём сотрудника, получаем 201."""
    response = client.post(
        "/api/v1/employees",
        json={"name": f"TEST-Новый Сотрудник", "role": "packer"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "TEST-Новый Сотрудник"
    assert data["role"] == "packer"
    assert "id" in data
    assert "created_at" in data


def test_create_employee_all_roles():
    """Happy path: создаём сотрудников всех трёх ролей."""
    for role in VALID_ROLES:
        response = client.post(
            "/api/v1/employees",
            json={"name": f"TEST-{role.title()} Сотрудник", "role": role},
        )
        assert response.status_code == 201
        assert response.json()["role"] == role


def test_create_employee_missing_name():
    """Error: name не передан → 422."""
    response = client.post(
        "/api/v1/employees",
        json={"role": "picker"},
    )
    assert response.status_code == 422


def test_create_employee_invalid_role():
    """Error: invalid role → 422."""
    response = client.post(
        "/api/v1/employees",
        json={"name": "Тест", "role": "manager"},
    )
    assert response.status_code == 422


def test_create_employee_empty_name():
    """Error: empty name → 422."""
    response = client.post(
        "/api/v1/employees",
        json={"name": "", "role": "picker"},
    )
    assert response.status_code == 422


# ── GET /api/v1/employees ──────────────────────────────────────────

def test_list_employees_success():
    """Happy path: список сотрудников."""
    _clean_test_employees()
    _seed_employee(name="TEST-Сотрудник A", role="picker")
    _seed_employee(name="TEST-Сотрудник B", role="packer")

    response = client.get("/api/v1/employees")
    assert response.status_code == 200
    data = response.json()
    assert "employees" in data
    assert "total" in data
    assert "page" in data
    assert "pages" in data
    assert data["total"] >= 2


def test_list_employees_pagination():
    """Edge case: page=1, limit=1 → только 1 сотрудник."""
    _clean_test_employees()
    _seed_employee(name="TEST-Паж 1", role="picker")
    _seed_employee(name="TEST-Паж 2", role="courier")

    response = client.get("/api/v1/employees?page=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["employees"]) == 1
    assert data["page"] == 1
    assert data["total"] >= 2
    assert data["pages"] >= 2


def test_list_employees_filter_by_role():
    """Happy path: фильтр по role."""
    _clean_test_employees()
    _seed_employee(name="TEST-Роль Пикер", role="picker")
    _seed_employee(name="TEST-Роль Пакер", role="packer")
    _seed_employee(name="TEST-Роль Курьер", role="courier")

    response = client.get("/api/v1/employees?role=picker")
    assert response.status_code == 200
    data = response.json()
    assert all(e["role"] == "picker" for e in data["employees"])
    assert data["total"] >= 1


def test_list_employees_filter_invalid_role():
    """Edge case: фильтр по несуществующей роли → пустой список."""
    response = client.get("/api/v1/employees?role=invalid_role")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["employees"] == []


def test_list_employees_no_filter():
    """Happy path: без фильтра возвращает всех."""
    response = client.get("/api/v1/employees")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 30  # seed data


# ── GET /api/v1/employees/{employee_id} ────────────────────────────

def test_get_employee_success():
    """Happy path: получаем сотрудника по ID."""
    eid = _seed_employee(name="TEST-Получение", role="courier")
    response = client.get(f"/api/v1/employees/{eid}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == eid
    assert data["name"] == "TEST-Получение"
    assert data["role"] == "courier"
    assert "created_at" in data


def test_get_employee_not_found():
    """Error: несуществующий ID → 404."""
    response = client.get("/api/v1/employees/999999")
    assert response.status_code == 404


# ── PUT /api/v1/employees/{employee_id} ────────────────────────────

def test_update_employee_success():
    """Happy path: полное обновление."""
    eid = _seed_employee(name="TEST-До Обновления", role="picker")
    response = client.put(
        f"/api/v1/employees/{eid}",
        json={"name": "TEST-После Обновления", "role": "packer"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "TEST-После Обновления"
    assert data["role"] == "packer"

    # Проверяем что изменения сохранены
    resp_get = client.get(f"/api/v1/employees/{eid}")
    assert resp_get.json()["role"] == "packer"


def test_update_employee_partial_name():
    """Happy path: частичное обновление (только name)."""
    eid = _seed_employee(name="TEST-Старое Имя", role="courier")
    response = client.put(
        f"/api/v1/employees/{eid}",
        json={"name": "TEST-Новое Имя"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "TEST-Новое Имя"
    assert data["role"] == "courier"  # не изменился


def test_update_employee_partial_role():
    """Happy path: частичное обновление (только role)."""
    eid = _seed_employee(name="TEST-Ролевой", role="picker")
    response = client.put(
        f"/api/v1/employees/{eid}",
        json={"role": "courier"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["role"] == "courier"
    assert data["name"] == "TEST-Ролевой"  # не изменился


def test_update_employee_not_found():
    """Error: обновление несуществующего → 404."""
    response = client.put(
        "/api/v1/employees/999999",
        json={"name": "Никого нет"},
    )
    assert response.status_code == 404


def test_update_employee_invalid_role():
    """Error: обновление на невалидную роль → 422."""
    eid = _seed_employee(name="TEST-Инвалид", role="picker")
    response = client.put(
        f"/api/v1/employees/{eid}",
        json={"role": "supervisor"},
    )
    assert response.status_code == 422


# ── DELETE /api/v1/employees/{employee_id} ─────────────────────────

def test_delete_employee_success():
    """Happy path: удаляем сотрудника → 204."""
    eid = _seed_employee(name="TEST-На Удаление", role="packer")
    response = client.delete(f"/api/v1/employees/{eid}")
    assert response.status_code == 204

    # Проверяем что сотрудник больше не возвращается
    resp_get = client.get(f"/api/v1/employees/{eid}")
    assert resp_get.status_code == 404


def test_delete_employee_not_found():
    """Error: удаление несуществующего → 404."""
    response = client.delete("/api/v1/employees/999999")
    assert response.status_code == 404


def test_delete_employee_double_delete():
    """Error: повторное удаление → 404."""
    eid = _seed_employee(name="TEST-Двойное Удаление", role="picker")
    resp1 = client.delete(f"/api/v1/employees/{eid}")
    assert resp1.status_code == 204
    resp2 = client.delete(f"/api/v1/employees/{eid}")
    assert resp2.status_code == 404
