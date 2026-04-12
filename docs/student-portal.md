# Студенческий портал — ваш центр управления

FulfilBox Student Portal — единый интерфейс для изучения системы.

## Как запустить

1. Убедитесь что система запущена:
   ```bash
   docker compose up -d
   ```

2. Откройте: **http://localhost:8080**

---

## Вкладка "Kafka" 📨

Просмотр сообщений из Kafka topics (только чтение).

1. Кликните на топик (`order_events` или `order_events_dlq`)
2. Нажмите "Загрузить последние сообщения"
3. Изучите структуру события

**Пример структуры события:**
```json
{
  "event_type": "order_created",
  "order_id": "ORD-12345",
  "timestamp": "2024-01-15T14:30:00Z",
  "payload": {
    "items": [{"sku": "SKU-001", "qty": 2}],
    "delivery_address": "Москва, ул. Тестовая 1"
  }
}
```

---

## Вкладка "Redis" 🔑

Поиск и просмотр ключей Redis (только чтение).

1. Введите префикс ключа:
   - `stock:` — кэш доступности товаров
   - `event:` — дедупликация событий
   - `retry:` — счётчик повторных попыток
2. Нажмите "Найти ключи"
3. Кликните на ключ чтобы увидеть значение

**Примеры команд для проверки:**
- Посмотрите TTL ключей `event:*` — они удалятся автоматически
- Проверьте `stock:SKU-001` — сколько доступно

---

## Вкладка "SQL" 🗄️

Выполнение SELECT-запросов к PostgreSQL (только чтение).

**Разрешено:**
- `SELECT ... FROM events`
- `SELECT ... FROM orders`
- Группировки, сортировки, JOIN

**Заблокировано:**
- `DELETE`, `UPDATE`, `INSERT`, `DROP`, `TRUNCATE`, `ALTER`, `CREATE`, `GRANT`
- Автоматически добавляется `LIMIT 100`

### Полезные запросы

```sql
-- Типы событий и их количество
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;

-- Последние 10 заказов
SELECT order_id, items, delivery_address FROM orders ORDER BY created_at DESC LIMIT 10;

-- Обработанные и необработанные события
SELECT processed, COUNT(*) FROM events GROUP BY processed;

-- Заказы с ошибками (не прошли до shipped)
SELECT order_id FROM events WHERE event_type = 'order_shipped'
EXCEPT
SELECT order_id FROM events WHERE event_type = 'order_created';
```

⚠️ Запросы с DELETE/UPDATE/DROP будут отклонены

---

## Вкладка "Links" 🔗

Быстрый доступ к инструментам:

| Инструмент | URL | Назначение |
|-----------|-----|------------|
| **Grafana** | http://localhost:3000 | Дашборды метрик |
| **Prometheus** | http://localhost:9090 | Метрики и алерты |
| **Alerts** | http://localhost:9090/alerts | Список алертов |
| **Order API** | http://localhost:8001/docs | Swagger документация |
| **Consumer Metrics** | http://localhost:8003/metrics | Prometheus формат |
| **WS Gateway** | http://localhost:8002 | WebSocket gateway |

---

## Задания для практики

### Задание 1: Анализ событий

Откройте вкладку SQL. Выполните запрос:
```sql
SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type ORDER BY cnt DESC;
```

**Вопросы:**
1. Сколько типов событий вы видите?
2. Какой тип встречается чаще всего?
3. Какой тип встречается реже всего?

### Задание 2: Поиск проблемных заказов

Найдите заказы, которые не дошли до `order_shipped`:
```sql
SELECT order_id FROM events WHERE event_type = 'order_created'
EXCEPT
SELECT order_id FROM events WHERE event_type = 'order_shipped';
```

Сколько таких заказов?

### Задание 3: Kafka события

Откройте вкладку Kafka, выберите `order_events`. Какие поля содержит каждое событие?

### Задание 4: Redis кэш

Откройте вкладку Redis, префикс `stock:`. Какие товары закешированы? Какое значение у `stock:SKU-001`?

---

## Безопасность

Все операции в портале — **только чтение**:
- ✅ Kafka consumer (без producer)
- ✅ Redis read-only (GET, KEYS, SCAN)
- ✅ SQL SELECT only (блокировка DDL/DML)
- ✅ Rate limit: 10 запросов в минуту
