# Как работать с алертами в FulfilBox

Prometheus алерты сигнализируют о проблемах в системе. Студенты учатся реагировать на сигналы и находить корень проблемы.

---

## Где смотреть алерты

Откройте: **http://localhost:9090/alerts**

Вы увидите список правил и их статус:

| Статус | Значение |
|--------|----------|
| 🟢 **inactive** | Всё хорошо, условие не выполняется |
| 🟡 **pending** | Условие выполняется, ждём `for` секунд |
| 🔴 **firing** | Проблема! Алерт активен |

---

## Три алерта в системе

### 1. DLQMessagesDetected (critical)

**Условие:** `dlq_total > 0`  
**Время:** 10 секунд

**Что значит:** Сообщения уходят в Dead Letter Queue — это значит что после 3+ retry обработка всё равно не удалась.

**Что делать:**
1. Открыть логи consumer:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "DLQ\|retry 3"
   ```

2. Проверить Kafka DLQ topic:
   ```bash
   # В студенческом портале → вкладка Kafka → order_events_dlq
   ```

3. Найти trace_id упавших заказов и проследить путь

---

### 2. HighErrorRate (warning)

**Условие:** `rate(error_total[1m]) / rate(processed_total[1m]) > 0.1`  
**Время:** 30 секунд

**Что значит:** Больше 10% заказов падают с ошибкой за последнюю минуту.

**Что делать:**
1. Открыть Grafana → дашборд "Kafka Consumer Metrics" → график "Errors"
2. Найти упавшие заказы в логах:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "failed\|error"
   ```

3. Проверить chaos конфиг:
   ```bash
   cat services/event-consumer-service/chaos_config.json
   ```
   Возможно включён `random_failure`.

4. Найти конкретные trace_id упавших заказов:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "trace_id=" | grep "failed"
   ```

---

### 3. HighLatencyP95 (warning)

**Условие:** `p95 latency > 500ms`  
**Время:** 30 секунд

**Что значит:** 95% заказов обрабатываются медленнее 500ms — система тормозит.

**Что делать:**
1. Проверить chaos конфиг на `random_delay`:
   ```bash
   cat services/event-consumer-service/chaos_config.json
   ```

2. Проверить cache hit rate в Redis:
   ```bash
   curl http://localhost:8003/metrics | grep stock:
   ```

3. Проверить нагрузку на БД:
   ```bash
   # В студенческом портале → вкладка SQL
   SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
   ```

---

## Практическое задание

### Цель: Увидеть алерт в действии

**Шаги:**

1. Откройте `/docs/chaos-scenarios.md`

2. Включите `random_failure` с вероятностью 20%:
   ```json
   {
     "enabled": true,
     "failure_scenarios": {
       "random_failure": {
         "enabled": true,
         "probability": 0.2,
         "error_message": "Warehouse timeout"
       }
     }
   }
   ```

3. Перезапустите consumer:
   ```bash
   docker compose restart event-consumer-service
   ```

4. Подождите 1-2 минуты

5. Откройте **http://localhost:9090/alerts**

6. Ответьте на вопросы:
   - Какой алерт сработал?
   - Почему он сработал?
   - Какие trace_id у упавших заказов?
   - Сколько заказов упало за последнюю минуту?

7. Выключите chaos и перезапустите consumer:
   ```json
   {
     "enabled": false,
     "failure_scenarios": {}
   }
   ```

---

## Связь: Метрика → Алерт → Локализация

```
Метрика: error_total увеличивается
    ↓
Алерт: HighErrorRate → FIRING
    ↓
Действие: Открыть логи → найти trace_id → понять причину
    ↓
Решение: Выключить chaos / исправить проблему
    ↓
Результат: Алерт → inactive
```

---

## Полезные ссылки

| Инструмент | URL | Назначение |
|-----------|-----|------------|
| **Prometheus Alerts** | http://localhost:9090/alerts | Список алертов |
| **Prometheus Query** | http://localhost:9090/graph | Проверка метрик |
| **Grafana** | http://localhost:3000 | Визуализация |
| **Student Portal** | http://localhost:8080 | SQL, Redis, Kafka |
| **Consumer Metrics** | http://localhost:8003/metrics | Prometheus формат |
| **Chaos Config** | services/event-consumer-service/chaos_config.json | Управление сбоями |
