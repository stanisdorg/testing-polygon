# FulfilBox — Event-Driven Architecture

## 📦 Что изменилось

FulfilBox был полностью переработан из polling-архитектуры в **настоящую event-driven микросервисную архитектуру**.

### Было (старая архитектура):
- ❌ Один монолитный consumer (`event-consumer-service`)
- ❌ Polling БД каждые 2 секунды
- ❌ Все шаги SAGA выполнялись в одном потоке последовательно
- ❌ Невозможно отследить промежуточные состояния
- ❌ Невозможно имитировать сбой на конкретном шаге

### Стало (новая архитектура):
- ✅ **5 независимых микросервисов**, общающихся через Kafka
- ✅ **Асинхронная обработка** — каждый шаг реагирует на события
- ✅ **Полный параллелизм** — заказы обрабатываются одновременно
- ✅ **Каждый шаг — отдельное событие** (можно отследить застревание)
- ✅ **SAGA Monitor** для автоматической компенсации при ошибках
- ✅ **Реалистичные задержки** на каждом шаге (настраиваются)

---

## 🏗 Архитектура

```
Order Service → Kafka: order_events
                   ↓
            Inventory Consumer → Kafka: inventory_events
                                    ↓
                             Payment Consumer → Kafka: payment_events
                                                   ↓
                                            Warehouse Consumer → Kafka: warehouse_events
                                                                    ↓
                                                             Delivery Consumer → Kafka: delivery_events
                                                                                    ↓
                                                                             SAGA Monitor (compensation)
```

### Микросервисы:

| Сервис | Port | Что делает |
|--------|------|------------|
| `order-service` | 8001 | REST API для создания заказов |
| `inventory-consumer` | 8010 | Проверяет склад, резервирует товары |
| `payment-consumer` | 8011 | Обрабатывает оплату (80% успех, 20% fail) |
| `warehouse-consumer` | 8012 | Picking → Packing → Shipping |
| `delivery-consumer` | 8013 | Назначает курьера, доставляет заказ |
| `saga-monitor` | 8014 | Compensation handler при ошибках |
| `student-portal` | 8080 | Dashboard + WebSocket для мониторинга |

---

## 🚀 Быстрый старт

### 1. Запуск системы

```bash
# Запуск всех сервисов
docker compose up -d

# Проверка статуса
docker compose ps

# Логи конкретного consumer'а
docker compose logs -f inventory-consumer
docker compose logs -f payment-consumer
```

### 2. Создание заказа

```bash
# Создать заказ
curl -X POST http://localhost:8001/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-TEST-001",
    "items": [{"sku": "SKU-001", "qty": 2}],
    "delivery_address": "Москва, ул. Тестовая 1"
  }'

# Ответ:
# {"status": "created", "order_id": "ORD-TEST-001", "trace_id": "uuid..."}
```

### 3. Мониторинг

```bash
# Dashboard UI
open http://localhost:8080

# Trace заказа
curl http://localhost:8080/api/dashboard/trace/ORD-TEST-001

# Prometheus metrics
curl http://localhost:8010/metrics  # Inventory Consumer
curl http://localhost:8011/metrics  # Payment Consumer
curl http://localhost:8012/metrics  # Warehouse Consumer
curl http://localhost:8013/metrics  # Delivery Consumer
curl http://localhost:8014/metrics  # SAGA Monitor
```

---

## 🎯 Для студентов — как тренироваться

### Сценарий 1: Отловить застревание заказа

```bash
# 1. Создайте заказ
curl -X POST http://localhost:8001/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "ORD-STUCK-001", "items": [{"sku": "SKU-001", "qty": 1}]}'

# 2. Проверьте статус заказа через 10 секунд
curl http://localhost:8080/api/dashboard/trace/ORD-STUCK-001

# 3. Если заказ застрял — посмотрите логи
docker compose logs payment-consumer | grep "ORD-STUCK-001"

# 4. Проверьте Kafka
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment_events \
  --from-beginning | jq 'select(.order_id == "ORD-STUCK-001")'
```

### Сценарий 2: Имитация сбоя оплаты

Payment Consumer имеет 20% шанс отказа. Можно настроить:

```bash
# Изменить шанс успеха оплаты (10% вместо 80%)
docker compose exec payment-consumer sh -c "export PAYMENT_SUCCESS_RATE=0.1"

# Или через docker-compose.yml (перезапустить после изменения)
# PAYMENT_SUCCESS_RATE: "0.1"
```

### Сценарий 3: Проверка компенсации (SAGA rollback)

```bash
# 1. Создайте несколько заказов
for i in {1..5}; do
  curl -X POST http://localhost:8001/api/v1/orders \
    -H "Content-Type: application/json" \
    -d "{\"order_id\": \"ORD-COMP-$i\", \"items\": [{\"sku\": \"SKU-001\", \"qty\": 1}]}"
done

# 2. Проверьте, какие заказы были отменены
docker compose exec postgres psql -U fulfilbox -d fulfilbox -c \
  "SELECT order_id, status FROM orders WHERE status = 'CANCELLED';"

# 3. Посмотрите компенсационные события
docker compose exec postgres psql -U fulfilbox -d fulfilbox -c \
  "SELECT order_id, event_type, is_compensation FROM events 
   WHERE is_compensation = true ORDER BY created_at DESC;"
```

### Сценарий 4: Мониторинг worker load

```bash
# Посмотреть загрузку сотрудников
docker compose exec redis redis-cli keys "load:*"

# Проверить конкретный load
docker compose exec redis redis-cli get "load:picker:Иван Петров"
```

---

## 🔧 Конфигурация

Все consumer'ы настраиваются через environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGA_STEP_DELAY_MIN` | 1 | Минимальная задержка между шагами (сек) |
| `SAGA_STEP_DELAY_MAX` | 3 | Максимальная задержка между шагами (сек) |
| `PAYMENT_SUCCESS_RATE` | 0.8 | Шанс успешной оплаты (0.0 - 1.0) |
| `INVENTORY_FAILURE_RATE` | 0.1 | Шанс сбоя инвентаря (0.0 - 1.0) |
| `COMPENSATION_DELAY` | 0 | Задержка перед компенсацией (сек) |

---

## 📊 Kafka Topics

| Topic | Producer | Consumers |
|-------|----------|-----------|
| `order_events` | Order Service | Inventory Consumer |
| `inventory_events` | Inventory Consumer | Payment Consumer, SAGA Monitor |
| `payment_events` | Payment Consumer | Warehouse Consumer, SAGA Monitor |
| `warehouse_events` | Warehouse Consumer | Delivery Consumer, SAGA Monitor |
| `delivery_events` | Delivery Consumer | SAGA Monitor, Student Portal |
| `fulfilment.audit` | Все сервисы | Logging, Monitoring |

---

## 🐛 Troubleshooting

### Consumer не обрабатывает заказы

```bash
# Проверить логи
docker compose logs -f payment-consumer

# Проверить Kafka consumer group
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-consumer-group

# Перезапустить consumer
docker compose restart payment-consumer
```

### Заказ застрял в обработке

```bash
# Посмотреть события заказа
docker compose exec postgres psql -U fulfilbox -d fulfilbox -c \
  "SELECT event_type, step_name, created_at, is_compensation 
   FROM events WHERE order_id = 'ORD-XXX' ORDER BY created_at;"

# Проверить dedup locks в Redis
docker compose exec redis redis-cli keys "*:ORD-XXX"
```

### Kafka topics не созданы

```bash
# Создать topics вручную
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic order_events --partitions 1 --replication-factor 1
```

---

## 📝 Архитектурные решения

### Почему Kafka, а не polling БД?

1. **Масштабируемость** — каждый consumer может масштабироваться независимо
2. **Реактивность** — события обрабатываются сразу, а не через polling interval
3. **Надёжность** — Kafka гарантирует доставку сообщений
4. **Наблюдаемость** — можно отследить каждое событие в топике
5. **Реалистичность** — так строят настоящие event-driven системы

### Почему отдельные consumer'ы?

1. **Изоляция ошибок** — сбой в Payment не ломает Inventory
2. **Независимый деплой** — можно обновлять каждый сервис отдельно
3. **Разные технологии** — каждый consumer может использовать свой стек
4. **Учебный процесс** — студенты могут отключать/ломать отдельные сервисы

---

## 🎓 Educational Goals

После развёртывания этой системы студенты смогут:

1. ✅ **Понимать event-driven архитектуру** — видеть, как события проходят через систему
2. ✅ **Отлавливать race conditions** — конкурентная обработка заказов
3. ✅ **Дебажить SAGA compensation** — наблюдать rollback при ошибках
4. ✅ **Мониторить Kafka** — проверять, доходят ли события
5. ✅ **Имитировать сбои** — chaos engineering на каждом шаге
6. ✅ **Оптимизировать производительность** — настраивать задержки, параллелизм

---

## 📚 Документация

- [System Overview](.ai-context/system_overview.md) — полная архитектура
- [API & Data Model](.ai-context/api_and_data.md) — БД, события, API
- [Dev Guidelines](.ai-context/dev_guidelines.md) — правила разработки

---

## 🤝 Contributing

При добавлении новых consumer'ов:
1. Создать директорию по аналогии (`services/<name>-consumer/`)
2. Использовать `shared/` модуль для общих утилит
3. Написать тесты (минимум 3: happy path, edge case, error)
4. Обновить `docker-compose.yml`
5. Обновить документацию

---

**FulfilBox** — учебная платформа для тренировки event-driven и SAGE patterns.
