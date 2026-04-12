# Как работать с алертами в FulfilBox

## Где смотреть алерты

### Способ 1: Grafana Alerting (рекомендуется)

1. Откройте Grafana: **http://localhost:3000** (логин: `admin`, пароль: `admin`)
2. Перейдите в: **Alerting → Alert rules** (или откройте **http://localhost:3000/alerting/list**)
3. Вы увидите список правил и их статус:
   - 🟢 **Normal** — всё хорошо
   - 🔴 **Firing** — проблема!

### Способ 2: Prometheus Alerts

1. Откройте: **http://localhost:9090/alerts**
2. Статусы:
   - 🟢 **inactive** — всё хорошо
   - 🔴 **firing** — проблема

---

## Три алерта в системе

### 1. DLQ Messages Detected (🔴 critical)

| Параметр | Значение |
|----------|----------|
| Условие | `dlq_total > 0` |
| For | 10 секунд |
| Что значит | Сообщения уходят в Dead Letter Queue — это критическая ошибка |

**Что делать:**
1. Открыть логи consumer:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | tail -50
   ```
2. Найти `trace_id` упавшего заказа:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "trace_id="
   ```
3. Проверить причину ошибки в логах
4. Проследить путь заказа по `trace_id` через всю систему

---

### 2. High Error Rate (🟡 warning)

| Параметр | Значение |
|----------|----------|
| Условие | `rate(error_total[1m]) / rate(processed_total[1m]) > 0.1` |
| For | 30 секунд |
| Что значит | Больше 10% заказов падают с ошибкой |

**Что делать:**
1. Открыть Grafana → Dashboard → Errors график
2. Найти упавшие заказы по `trace_id` в логах:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "trace_id=" | tail -10
   ```
3. Проверить chaos config — возможно включены случайные ошибки:
   ```bash
   cat services/event-consumer-service/chaos_config.json
   ```
4. Проверить warehouse availability

---

### 3. High p95 Latency (🟡 warning)

| Параметр | Значение |
|----------|----------|
| Условие | `p95 latency > 500ms` |
| For | 30 секунд |
| Что значит | Система тормозит — 95-й перцентиль времени обработки выше 500мс |

**Что делать:**
1. Проверить chaos config — возможно включены случайные задержки:
   ```bash
   cat services/event-consumer-service/chaos_config.json
   ```
2. Проверить cache hit rate в Redis:
   ```bash
   curl http://localhost:8080/api/redis/keys?prefix=stock:
   ```
3. Проверить нагрузку на БД:
   ```sql
   SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY cnt DESC;
   ```
4. Найти медленные запросы в логах:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS.*random_delay"
   ```

---

## Разница между warning и critical

| Уровень | Когда | Действие |
|---------|-------|----------|
| 🟡 **warning** | Система работает, но есть проблемы | Разобраться в ближайшее время |
| 🔴 **critical** | Критическая ошибка, данные теряются | Немедленно! |

---

## Практическое задание

### Задание 1: Активируйте алерт High Error Rate

1. Откройте `services/event-consumer-service/chaos_config.json`
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
5. Откройте Grafana → **Alerting → Alert rules**
6. **Вопрос:** Какой алерт сработал? Почему?

### Задание 2: Найдите причину

1. Найдите в логах 3 упавших заказа по `trace_id`:
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS.*random_failure" | tail -3
   ```
2. Скопируйте `trace_id` одного из заказов
3. Откройте **Live Events UI** (http://localhost:8080/events/live) — видите ли вы этот заказ?
4. **Вопрос:** Объясните, почему сработал алерт и как это связано с chaos-конфигом

### Задание 3: Почините систему

1. Выключите chaos в `chaos_config.json`:
   ```json
   {
     "enabled": false,
     "failure_scenarios": {}
   }
   ```
2. Перезапустите consumer:
   ```bash
   docker compose restart event-consumer-service
   ```
3. Подождите 1-2 минуты
4. Откройте Grafana → **Alerting** — алерты вернулись в Normal?
5. Откройте Prometheus → **Alerts** — алерты вернулись в inactive?

---

## Полезные команды

```bash
# Логи consumer
docker logs -f testing-polygon-event-consumer-service-1

# Найти chaos события
docker logs testing-polygon-event-consumer-service-1 | grep "CHAOS"

# Найти конкретный trace_id
docker logs testing-polygon-event-consumer-service-1 | grep "trace_id=ВАШ_ID"

# Метрики consumer
curl http://localhost:8003/metrics | grep -E "error_total|processed_total|dlq_total"

# Проверить алерты через API
curl -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | python3 -m json.tool
```

---

## Путь заказа по trace_id

Каждый заказ имеет уникальный `trace_id`. Его можно использовать для отслеживания:

1. **Взять trace_id из UI:**
   - Откройте http://localhost:8080/events/live
   - Найдите заказ в таблице
   - Скопируйте Trace ID (кликните по нему)

2. **Найти в логах consumer:**
   ```bash
   docker logs testing-polygon-event-consumer-service-1 | grep "trace_id=ВАШ_ID"
   ```

3. **Найти в БД:**
   ```sql
   SELECT event_type, order_id FROM events 
   WHERE payload::text LIKE '%ВАШ_TRACE_ID%'
   ORDER BY created_at DESC;
   ```

4. **Связать с алертом:**
   - Если алерт firing → найдите trace_id в логах
   - Посмотрите что произошло с этим заказом
   - Объясните причину ошибки
