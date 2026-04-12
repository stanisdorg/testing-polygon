# API & Data Model Specification

## Database Schema (PostgreSQL)

### 1. `events` (Event Store)
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | Уникальный ID события (используется для WS cursor). |
| `event_type` | VARCHAR(50) | Тип события (напр. `order_created`). |
| `order_id` | VARCHAR(30) | ID заказа. |
| `payload` | JSONB | Дополнительные данные события. |
| `trace_id` | VARCHAR(50) | Сквозной ID для трассировки. |
| `entity_type` | VARCHAR(30) | Тип сущности (напр. `order`, `payment`). |
| `entity_id` | VARCHAR(50) | ID сущности. |
| `saga_id` | VARCHAR(50) | ID SAGA-цепочки. |
| `step_name` | VARCHAR(50) | Название шага SAGA. |
| `is_compensation` | BOOLEAN | Флаг компенсационного события. |
| `created_at` | TIMESTAMP | Время создания. |

### 2. `orders`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | VARCHAR(30) (PK) | ID заказа (напр. `ORD-123`). |
| `status` | VARCHAR(20) | `created`, `PROCESSING`, `COMPLETED`, `CANCELLED`, `FAILED`. |
| `created_at` | TIMESTAMP | Время создания. |
| `total_price` | NUMERIC | Сумма заказа. |
| `saga_id` | VARCHAR(50) | Ссылка на SAGA. |
| `warehouse_id`| VARCHAR(20) | ID склада назначения. |

### 3. `products`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | Внутренний ID. |
| `sku` | VARCHAR(30) (Unique) | Артикул. |
| `name` | VARCHAR(200) | Название. |
| `price` | NUMERIC | Цена. |
| `created_at` | TIMESTAMP | |

### 4. `inventory`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | |
| `sku` | VARCHAR(30) | Ссылка на товар. |
| `warehouse_id`| VARCHAR(20) | Ссылка на склад. |
| `available_qty`| INTEGER | Доступное кол-во. |
| `reserved_qty` | INTEGER | Зарезервировано под заказы. |

### 5. `warehouses`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | VARCHAR(20) (PK) | ID склада (напр. `WH-MSK`). |
| `name` | VARCHAR(100) | Название. |
| `location` | VARCHAR(200) | Адрес. |
| `capacity_m3`| NUMERIC | Вместимость. |

### 6. `employees`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | |
| `name` | VARCHAR(100) | Имя сотрудника. |
| `role` | VARCHAR(30) | `picker`, `packer`, `courier`. |

### 7. `vehicles`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | |
| `plate_number`| VARCHAR(20) (Unique) | Номер машины. |
| `capacity` | NUMERIC | Грузоподъемность. |

### 8. `deliveries`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | |
| `order_id` | VARCHAR(30) | Ссылка на заказ. |
| `courier_name`| VARCHAR(100) | Имя курьера. |
| `status` | VARCHAR(20) | `assigned`, `in_transit`, `delivered`. |
| `cancelled` | BOOLEAN | Флаг отмены доставки. |

### 9. `payments`
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL (PK) | |
| `order_id` | VARCHAR(30) | Ссылка на заказ. |
| `amount` | NUMERIC | Сумма. |
| `status` | VARCHAR(20) | `requested`, `succeeded`, `failed`, `refunded`. |
| `created_at` | TIMESTAMP | |

---

## Event Types Reference

| Категория | Типы |
|-----------|------|
| **Order** | `order_created`, `order_completed`, `order_failed`, `order_cancelled` |
| **Inventory** | `inventory_reserved`, `inventory_failed`, `inventory_restocked` |
| **Payment** | `payment_requested`, `payment_succeeded`, `payment_failed`, `payment_refunded` |
| **Warehouse** | `picking_started`, `picking_completed`, `order_packed` |
| **Shipping** | `order_shipped`, `delivery_assigned`, `delivery_started`, `delivery_completed`, `delivery_cancelled` |

---

## API Endpoints

### Business API

#### `POST /api/v1/orders`
Создание заказа.
*   **Request:**
    ```json
    {
      "order_id": "ORD-TEST-1",
      "items": [{"sku": "SKU-001", "qty": 1}],
      "delivery_address": "Moscow, ..."
    }
    ```
*   **Response:** `201 Created`
    ```json
    {
      "status": "created",
      "order_id": "ORD-TEST-1",
      "trace_id": "uuid..."
    }
    ```

#### `GET /api/v1/orders/{order_id}`
Получение статуса заказа.

#### `GET /api/v1/deliveries/{order_id}`
Получение информации о доставке.

### Dashboard API (Read-Only)

*   `GET /api/dashboard/kpis` — Метрики (orders, revenue, inventory).
*   `GET /api/dashboard/funnel` — Воронка статусов.
*   `GET /api/dashboard/warehouses` — Список складов с загрузкой.
*   `GET /api/dashboard/workers` — Список сотрудников.
*   `GET /api/dashboard/events` — Лента событий.
*   `GET /api/dashboard/trace/{order_id}` — Полный трейс заказа по ID.
*   `GET /api/dashboard/summary` — Общая сводка.

### WebSocket

*   `GET /ws/events` — Подключение к WebSocket.
    *   Клиент отправляет: `{"last_id": N}`
    *   Сервер шлет: JSON объекта события (см. Kafka Message Structure).

---

## Kafka Message Structure

**Topic:** `fulfilment.events`

**Payload Example:**
```json
{
  "event_type": "order_created",
  "order_id": "ORD-TEST-1",
  "trace_id": "550e8400-e29b...",
  "entity_type": "order",
  "entity_id": "ORD-TEST-1",
  "saga_id": "ORD-TEST-1",
  "step_name": "order_created",
  "is_compensation": false,
  "payload": {
    "items": [{"sku": "SKU-001", "qty": 1}],
    "delivery_address": "..."
  },
  "timestamp": "2024-04-09T12:00:00Z"
}
```