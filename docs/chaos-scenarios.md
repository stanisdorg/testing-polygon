# Сценарии сбоев для отладки (Chaos Engineering)

FulfilBox поддерживает управляемые сбои — студенты могут "сломать" систему и потренироваться находить проблемы.

---

## Как включить сбой

1. Откройте `services/event-consumer-service/chaos_config.json`
2. Включите нужный сценарий: `"enabled": true`
3. Перезапустите consumer:
   ```bash
   docker compose restart event-consumer-service
   ```

---

## Сценарий 1: Задержки (`random_delay`)

**Что происходит:** заказы обрабатываются медленнее (случайная задержка 100-2000ms с вероятностью 10%).

**Конфиг:**
```json
{
  "enabled": true,
  "failure_scenarios": {
    "random_delay": {
      "enabled": true,
      "probability": 0.1,
      "delay_range_ms": [100, 2000]
    }
  }
}
```

**Что увидишь в Grafana:** рост p95/p99 latency.

**Задание для студента:**
> "Найдите, почему p95 вырос с 50мс до 2с. Определите какой заказ тормозит и почему."

**Как искать:**
```bash
# Найти медленные заказы в логах
docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS.*random_delay"

# Проверить метрики latency
curl http://localhost:8003/metrics | grep order_processing_seconds
```

---

## Сценарий 2: Случайные ошибки (`random_failure`)

**Что происходит:** 5% заказов падают с ошибкой "Warehouse timeout".

**Конфиг:**
```json
{
  "enabled": true,
  "failure_scenarios": {
    "random_failure": {
      "enabled": true,
      "probability": 0.05,
      "error_message": "Warehouse timeout"
    }
  }
}
```

**Что увидишь:** рост `error_total`, срабатывание алерта "High Error Rate" в Prometheus/Grafana.

**Задание для студента:**
> "Алерт 'High Error Rate' сработал. Найдите причину в логах consumer. Сколько заказов упало за последний час? Найдите trace_id упавших заказов."

**Как искать:**
```bash
# Найти упавшие заказы с trace_id
docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS.*random_failure"

# Пример лога:
# [CHAOS][trace_id=abc-123] Applied random_failure: Warehouse timeout for ORD-12345

# Проверить метрики ошибок
curl http://localhost:8003/metrics | grep error_total
```

---

## Сценарий 3: Рассинхрон склада (`inventory_mismatch`)

**Что происходит:** система думает что товар `SKU-003` есть на складе, но физически его нет. Заказы "застревают".

**Конфиг:**
```json
{
  "enabled": true,
  "failure_scenarios": {
    "inventory_mismatch": {
      "enabled": true,
      "affected_skus": ["SKU-003"],
      "behavior": "report_available_but_missing"
    }
  }
}
```

**Что увидишь:** заказы с SKU-003 не доходят до `order_shipped`, рост retry → DLQ.

**Задание для студента:**
> "Заказ ORD-XXX не дошёл до shipped. Разберитесь почему. Проверьте warehouse и найдите рассинхрон. Используйте trace_id для трассировки."

**Как искать:**
```bash
# Найти заказы с SKU-003
curl -X POST http://localhost:8080/api/sql/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT order_id, payload FROM events WHERE payload::text LIKE '%SKU-003%' LIMIT 10"}'

# Проверить какие заказы застряли
curl -X POST http://localhost:8080/api/sql/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT order_id FROM events WHERE event_type = '\''order_created'\'' EXCEPT SELECT order_id FROM events WHERE event_type = '\''order_shipped'\'' LIMIT 10"}'
```

---

## Как выключить все сбои

```json
{
  "enabled": false,
  "failure_scenarios": {}
}
```

Затем:
```bash
docker compose restart event-consumer-service
```

---

## Полезные команды

```bash
# Посмотреть логи consumer
docker logs -f testing-polygon-event-consumer-service-1

# Найти chaos события в логах
docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS"

# Посмотреть метрики
curl http://localhost:8003/metrics

# Prometheus alerts
http://localhost:9090/alerts

# Grafana дашборд
http://localhost:3000
```

---

## Трассировка заказа по trace_id

Каждый заказ имеет уникальный `trace_id`. Его можно использовать для отслеживания пути заказа через всю систему:

1. **Взять trace_id из UI:**
   - Откройте http://localhost:8080
   - Найдите заказ в таблице
   - Скопируйте Trace ID (кликните по нему)

2. **Найти в логах consumer:**
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "trace_id=ВАШ_ID"
   ```

3. **Найти в Kafka:**
   - Откройте вкладку "Kafka" в студенческом портале
   - Найдите сообщение с вашим trace_id

4. **Найти в БД:**
   ```bash
   curl -X POST http://localhost:8080/api/sql/query \
     -H "Content-Type: application/json" \
     -d '{"query": "SELECT event_type, order_id FROM events WHERE payload::text LIKE '\''%ВАШ_TRACE_ID%'\''"}'
   ```

---

## Пример комплексного задания

> "Включите `random_failure` с вероятностью 10%.
> Найдите в логах 3 упавших заказа по trace_id.
> Объясните почему сработал алерт 'High Error Rate'.
> Проверьте как изменились метрики p95/p99 latency."

**Шаги выполнения:**
1. Включите chaos в конфиге
2. Перезапустите consumer
3. Подождите 1-2 минуты
4. Проверьте Grafana (метрики)
5. Найдите упавшие заказы в логах
6. Проследите путь заказа по trace_id
7. Выключите chaos и перезапустите consumer
