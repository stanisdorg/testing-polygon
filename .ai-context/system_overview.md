# System Overview: FulfilBox Event-Driven Architecture

## Архитектура

Система представляет собой **настоящую Event-Driven платформу** управления фулфилментом с **микросервисной архитектурой**.

### Компоненты

| Компонент | Описание | Путь |
|-----------|----------|------|
| **Order Service** (REST API) | Точка входа для создания заказов. Публикует `order_created` в Kafka. | `services/order-service/main.py` |
| **Inventory Consumer** | Слушает `order_created` → проверяет склад → публикует `inventory_reserved/failed`. | `services/inventory-consumer/consumer.py` |
| **Payment Consumer** | Слушает `inventory_reserved` → обрабатывает оплату → публикует `payment_succeeded/failed`. | `services/payment-consumer/consumer.py` |
| **Warehouse Consumer** | Слушает `payment_succeeded` → picking/packing/shipping → публикует `order_shipped`. | `services/warehouse-consumer/consumer.py` |
| **Delivery Consumer** | Слушает `order_shipped` → назначает курьера → публикует `delivery_completed`. | `services/delivery-consumer/consumer.py` |
| **SAGA Monitor** | Слушает все fail-события → запускает compensation chain. | `services/saga-monitor/monitor.py` |
| **Student Portal** (Web UI + API) | Frontend: `ui-dashboard/index.html`. Backend: FastAPI (`services/student-portal/portal.py`). WebSocket: `/ws/events`. | `services/student-portal/` |
| **PostgreSQL** | Event Store (таблица `events`) + Business State (`orders`, `inventory`, `deliveries` и др.). | БД `fulfilbox` |
| **Kafka** | **Асинхронная шина для всех событий.** Топики: `order_events`, `inventory_events`, `payment_events`, `warehouse_events`, `delivery_events`, `fulfilment.audit`. | Topics: см. ниже |
| **Redis** | Кэш, блокировки (deduplication), кэш доступности товаров, worker load tracking. | БД 0 |

### Взаимодействие

```
[Browser] ←→ [Student Portal API + WebSocket]
                    ↑ (HTTP POST)                    ↑ (WS: /ws/events)
               [Order Service API]              [Student Portal Backend]
                    ↓ (HTTP POST /api/v1/orders)       ↑ (Polls events table)
               [Order Service]
                    ↓ (Publish to Kafka)
           Kafka: order_events
                    ↓
           [Inventory Consumer]
                    ↓ (Publish to Kafka)
           Kafka: inventory_events
                    ↓
           [Payment Consumer]
                    ↓ (Publish to Kafka)
           Kafka: payment_events
                    ↓
           [Warehouse Consumer]
                    ↓ (Publish to Kafka)
           Kafka: warehouse_events
                    ↓
           [Delivery Consumer]
                    ↓ (Publish to Kafka)
           Kafka: delivery_events
                    ↓
           [SAGA Monitor] (listens to all fail events)
                    ↓ (Compensation events)
           PostgreSQL (Event Store)
```

### Kafka Topics

| Topic | Producer | Consumers | Описание |
|-------|----------|-----------|----------|
| `order_events` | Order Service | Inventory Consumer | `order_created` |
| `inventory_events` | Inventory Consumer | Payment Consumer, SAGA Monitor | `inventory_reserved`, `inventory_failed` |
| `payment_events` | Payment Consumer | Warehouse Consumer, SAGA Monitor | `payment_requested`, `payment_succeeded`, `payment_failed` |
| `warehouse_events` | Warehouse Consumer | Delivery Consumer, SAGA Monitor | `picking_started/completed`, `order_packed`, `order_shipped` |
| `delivery_events` | Delivery Consumer | SAGA Monitor, Student Portal | `delivery_assigned/started/completed/cancelled` |
| `fulfilment.audit` | Все сервисы | Logging, Monitoring | Все события для аудита |

---

## Event-Driven Flow

### Happy Path (Успешная обработка)

Каждый шаг обрабатывается **отдельным микросервисом** асинхронно через Kafka:

```
[Order Service] order_created
    ↓ Kafka: order_events
[Inventory Consumer] inventory_reserved
    ↓ Kafka: inventory_events
[Payment Consumer] payment_requested → payment_succeeded
    ↓ Kafka: payment_events
[Warehouse Consumer] picking_started → picking_completed → order_packed → order_shipped
    ↓ Kafka: warehouse_events
[Delivery Consumer] delivery_assigned → delivery_started → delivery_completed
    ↓ Kafka: delivery_events
[Order marked as COMPLETED in DB]
```

**Все события записываются в таблицу `events`** для трассировки и мониторинга.

### Error & Compensation (Обработка сбоев)

Если любой шаг падает, **SAGA Monitor** обнаруживает fail-событие и запускает компенсацию.

**Пример: Сбой оплаты (Payment Failed)**
1.  **Event:** `payment_failed` (опубликован Payment Consumer)
2.  **SAGA Monitor:** Обнаруживает `payment_failed`
3.  **Compensation Chain:**
    *   `payment_refunded` (возврат средств)
    *   `inventory_restocked` (возврат товара на склад)
    *   `order_cancelled` (финализация заказа)

**Пример: Сбой доставки (Delivery Failed)**
1.  **Event:** Ошибка на этапе доставки (например, курьер застрял)
2.  **SAGA Monitor:** Обнаруживает сбой
3.  **Compensation Chain:**
    *   `delivery_cancelled`
    *   `payment_refunded`
    *   `inventory_restocked`
    *   `order_cancelled`

**Компенсационные события (is_compensation=true):**
*   `payment_refunded`
*   `inventory_restocked`
*   `order_cancelled`
*   `delivery_cancelled`

**Ключевое отличие от старой архитектуры:** 
- ❌ Раньше: Всё в одном consumer'е, polling БД каждые 2 сек
- ✅ Теперь: Каждый шаг — отдельный микросервис, асинхронная обработка через Kafka

---

## SAGA Implementation

Реализована через **5 независимых микросервисов**, общающихся через Kafka.

*   **Saga ID:** Генерируется при создании заказа (`order_id` или `saga_id`). Все события одной цепочки имеют один `saga_id`.
*   **Step Name:** Записывается в поле `step_name` таблицы `events` для отладки.
*   **State Machine:** Распределена по consumer'ам. Каждый consumer обрабатывает свой шаг и публикует результат.
*   **Idempotency:**
    *   DB: Unique Index `events_order_event_unique` (order_id, event_type, saga_id).
    *   Redis: Keys `inventory:{saga_id}`, `payment:{saga_id}`, etc. с TTL для deduplication.
*   **Compensation:** SAGA Monitor слушает все fail-события и запускает компенсационную цепочку.

---

## Retry + DLQ Logic

*   **Retry:** Встроен в каждый Consumer. При ошибке выполнения шага consumer может повторить обработку (настраивается).
*   **DLQ (Dead Letter Queue):**
    *   Если consumer не может обработать сообщение после нескольких попыток, событие попадает в DLQ.
    *   SAGA Monitor отслеживает такие события и запускает компенсацию.
    *   Заказ помечается как `FAILED` или `CANCELLED`.

---

## WebSocket Implementation

*   **Endpoint:** `GET /ws/events` в `student-portal/portal.py`.
*   **Механизм:**
    *   Клиент подключается, отправляет JSON `{"last_id": N}`.
    *   Сервер поллит таблицу `events` (`SELECT ... WHERE id > last_id`).
    *   Новые события отправляются через `websocket.send_json()`.
    *   Функция `fetch_new()` использует `asyncio.to_thread` для неблокирующего polling.

---

## Kafka

Kafka — **центральная шина** для всей event-driven коммуникации.

### Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `order_events` | 1 | Order Service → Inventory Consumer |
| `inventory_events` | 1 | Inventory Consumer → Payment Consumer, SAGA Monitor |
| `payment_events` | 1 | Payment Consumer → Warehouse Consumer, SAGA Monitor |
| `warehouse_events` | 1 | Warehouse Consumer → Delivery Consumer, SAGA Monitor |
| `delivery_events` | 1 | Delivery Consumer → SAGA Monitor, Student Portal |
| `fulfilment.audit` | 3 | Все сервисы → Audit logging (все события) |

### Message Structure

**Все события имеют единую структуру:**
```json
{
  "event_type": "order_created",
  "order_id": "ORD-TEST-1",
  "trace_id": "uuid...",
  "entity_type": "order",
  "entity_id": "ORD-TEST-1",
  "saga_id": "ORD-TEST-1",
  "step_name": "order_created",
  "is_compensation": false,
  "payload": { "items": [...], "warehouse_id": "WH-MSK-S" },
  "timestamp": "2024-04-09T12:00:00Z"
}
```

### Producers & Consumers

- **Order Service:** Публикует `order_created` в `order_events`
- **Inventory Consumer:** Публикует `inventory_reserved/failed` в `inventory_events`
- **Payment Consumer:** Публикует `payment_requested/succeeded/failed` в `payment_events`
- **Warehouse Consumer:** Публикует warehouse events в `warehouse_events`
- **Delivery Consumer:** Публикует delivery events в `delivery_events`
- **SAGA Monitor:** Публикует compensation events в `fulfilment.audit`
