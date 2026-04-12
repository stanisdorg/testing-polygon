# Development Guidelines

## Architectural Principles

1.  **Event-Driven First:** Любое изменение статуса заказа должно генерировать событие в таблице `events`.
2.  **Idempotency is Key:** Все обработчики событий должны быть идемпотентны. Проверка существования события (`_event_exists`) обязательна.
3.  **Compensation over 2PC:** Не используем распределенные транзакции. Используем SAGA с компенсационными транзакциями.
4.  **Database as Message Bus:** Consumer использует polling таблицы `events` как основную очередь задач (до тех пор, пока Kafka не станет источником правды для Consumer).

## Development Workflow

1.  **Analysis:** Определить, какие новые события нужны для фичи.
2.  **DB:** Добавить миграцию (таблицы/колонки).
3.  **Consumer Logic:**
    *   Реализовать обработку нового события в `consumer.py`.
    *   Реализовать обработку ошибок и компенсацию.
4.  **API:** Добавить эндпоинты в `student-portal/portal.py`.
5.  **UI:** Обновить `ui-dashboard/index.html`.

## How to write prompts for AI

При работе с новым ИИ следуй этому шаблону:

1.  **Контекст:** "Я работаю над проектом FulfilBox. Изучи файлы в папке `.ai-context/`".
2.  **Задача:** Опиши, что нужно сделать (например, "Добавить этап 'Quality Check' после Picking").
3.  **Требования:**
    *   Сначала напиши тест.
    *   Потом реализацию.
    *   Убедись, что старые тесты не сломались.
    *   Не ломай существующий flow.
4.  **Проверка:** Запроси SQL-запросы для проверки данных в БД.

## Testing Checklist

Перед коммитом проверить вручную:

1.  **Create Order:**
    *   `curl -X POST ...`
    *   Проверить, что статус изменился на `COMPLETED`.
2.  **Trace:**
    *   Найти `trace_id` в ответе.
    *   Проверить `GET /api/dashboard/trace/{id}` — все шаги должны быть на месте.
3.  **Events Table:**
    *   `SELECT * FROM events WHERE order_id = ...` — проверить, что нет дублей.
4.  **Kafka:**
    *   Проверить консьюмером, что сообщение пришло в `fulfilment.events`.
5.  **Error Scenario:**
    *   Имитировать сбой (например, `inventory_failed`).
    *   Проверить, что заказ перешел в `CANCELLED`.
    *   Проверить наличие компенсационных событий (`is_compensation = true`).

## Known Limitations

*   **Polling Latency:** Consumer опрашивает БД раз в 2 секунды.
*   **No Auth:** API и Dashboard не имеют аутентификации.
*   **No Rate Limiting:** Нет защиты от DDOS на эндпоинтах.
*   **Kafka Usage:** Kafka сейчас используется только для аудита/логов. Consumer не читает из Kafka.
*   **WebSocket:** Реализован через polling внутри WebSocket хендлера, а не через push-уведомления от БД (LISTEN/NOTIFY).