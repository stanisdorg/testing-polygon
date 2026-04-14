# FulfilBox AI Bootstrap — Phase 2 Roadmap

## 1. Current State

Ты работаешь с системой FulfilBox — учебная DevOps платформа для QA инженеров.

### Что УЖЕ работает:
- ✅ **Docker Compose** — полностью рабочий стек
- ✅ **Kubernetes (Kind)** — полностью рабочий кластер с 3 нодами
- ✅ **Event-Driven архитектура** — 5 независимых Kafka consumers
- ✅ **SAGA Compensation** — автоматический rollback при ошибках
- ✅ **Dashboard UI** — воронка, KPI, сотрудники, склады, события (http://localhost:8080/dashboard/)
- ✅ **Kubernetes Dashboard** — pods, deployments, HPA, nodes, events
- ✅ **PostgreSQL** — Event Store + business data (10 таблиц)
- ✅ **Kafka** — 6 topics для межсервисной коммуника
- ✅ **Redis** — кеш, dedup locks, worker load tracking, rate limiting
- ✅ **Student Portal** — FastAPI + Dashboard + WebSocket + K8s API
- ✅ **Prometheus + Grafana** — мониторинг и алерты
- ✅ **Chaos Engineering** — конфигурируемые сценарии сбоев
- ✅ **Postman Collections** — 16 папок с CRUD endpoints
- ✅ **Favicon** — кастомная иконка вкладки (складская коробка)

### Текущая архитектура:
```
Order Service (REST) → Kafka: order_events → Inventory Consumer
                                                    ↓ Kafka: inventory_events
                                              Payment Consumer
                                                    ↓ Kafka: payment_events
                                              Warehouse Consumer
                                                    ↓ Kafka: warehouse_events
                                              Delivery Consumer
                                                    ↓ Kafka: delivery_events
                                              SAGA Monitor (compensation)
```

### Файлы которые ОБЯЗАТЕЛЬНО изучить:
- `.ai-context/system_overview.md` — текущая архитектура
- `.ai-context/api_and_data.md` — БД, события, API endpoints
- `.ai-context/dev_guidelines.md` — правила разработки
- `EVENT_DRIVEN_ARCHITECTURE.md` — описание миграции на event-driven
- `README.md` — общий overview проекта

---

## 2. Phase 2 Roadmap (ЧТО нужно сделать)

### Фаза 1: База + K8s ✅ ЗАВЕРШЕНО
- [x] Исправить Kafka StatefulSet в K8s (Zookeeper readiness probe → tcpSocket)
- [x] Добавить 3 новых Kafka topics: `dead_letter_queue`, `audit_log`, `metrics_stream`
- [x] Обновить K8s манифесты для всех 5 consumer'ов
- [x] Полноценно запустить Kind кластер
- [x] Создать SQL init скрипт (init-db.sql)
- [x] Настроить RBAC для student-portal (dashboard-reader service account)
- [x] Создать ConfigMap для student-portal (config-k8s.json)

### Фаза 2: Redis расширение ✅ ЗАВЕРШЕНО
- [x] Redis pub/sub для real-time событий (вместо polling)
- [x] Redis cache для заказов (TTL 5 мин)
- [x] Redis rate limiting для API (500 req/min)
- [x] Вкладка Redis на дашборде (keys, memory, pub/sub channels)
- [x] Rate limit очищается при сбросе данных

### Фаза 3: gRPC inter-service communication ✅ ЗАВЕРШЕНО
- [x] Создать `.proto` файлы для consumer коммуникации (health.proto, consumer_status.proto)
- [x] Добавить gRPC серверы в consumer'ы (port 50051-50055)
- [x] gRPC health check endpoints
- [x] Вкладка gRPC на дашборде (health, methods, latency, proto files)

### Фаза 4: GraphQL API Gateway 🎯 в процессе
- [ ] Создать GraphQL Gateway (Strawberry GraphQL)
- [ ] Schema: orders, events, warehouses, employees, inventory, deliveries
- [ ] Resolvers к существующим сервисам
- [ ] GraphQL Playground в дашборде
- [ ] Subscriptions для real-time событий

### Фаза 5: WebSocket расширение ✅ ЗАВЕРШЕНО
- [x] Расширить ws-gateway для bidirectional communication
- [x] Подключить к Redis pub/sub
- [x] Live события на дашборде (без polling!)
- [x] Вкладка WebSocket (connected clients, messages/sec, ping)

### Фаза 6: RESTful CRUD для Postman ✅ ЗАВЕРШЕНО
- [x] Products CRUD (full)
- [x] Warehouses CRUD (full)
- [x] Employees CRUD (full)
- [x] Inventory CRUD (full)
- [x] Vehicles CRUD (full)
- [x] Payments CRUD (full)
- [x] Deliveries CRUD (full)
- [x] Events & Tracing API
- [x] Chaos Engineering API
- [x] Dashboard & Monitoring API

### Фаза 7: Kubernetes полный деплой ✅ ЗАВЕРШЕНО
- [x] Исправить все K8s манифесты (Kafka StatefulSet, Zookeeper)
- [x] Helm chart v2 с новыми сервисами
- [x] HPA для consumer'ов (autoscaling по lag)
- [x] K8s Dashboard вкладка (pods, deployments, HPA, events, nodes)

---

## 3. Architecture после Phase 2

```
┌──────────────┐     gRPC      ┌─────────────────────┐
│ Order Service │─────────────▶│ Inventory Consumer  │
│ (REST+GraphQL)│              │                     │
└──────┬───────┘     gRPC      └─────────────────────┘
       │                      │
       ├── Kafka: order_events │ (audit only)
       │                      ▼
       │              ┌─────────────────────┐     gRPC      ┌─────────────────┐
       │              │ Payment Consumer    │─────────────▶│ Warehouse Cons. │
       │              └─────────────────────┘              └─────────────────┘
       │                      │                                  │
       │                      ▼                                  ▼
       │              ┌─────────────────────┐     gRPC      ┌─────────────────┐
       │              │   SAGA Monitor      │◀──────────────│ Delivery Cons.  │
       │              └─────────────────────┘              └─────────────────┘
       │
       ▼
┌─────────────────────┐
│   GraphQL Gateway   │◀── WebSocket ──▶  Student Portal
│  (Strawberry)       │                    (Dashboard UI)
└─────────────────────┘
       │
       ├── Redis: pub/sub (real-time updates)
       ├── Redis: cache (orders, inventory)
       └── Redis: rate limiting (API protection)
```

### Технологии в проекте:
| Технология | Назначение |
|------------|-----------|
| **Kafka** | Event sourcing, audit log, DLQ |
| **gRPC** | Inter-service communication (consumer ↔ consumer) |
| **REST** | External API (Postman collections, CRUD) |
| **GraphQL** | API Gateway для дашборда и клиентов |
| **WebSocket** | Real-time events в UI |
| **Redis** | Cache, pub/sub, rate limiting, dedup |
| **PostgreSQL** | Event Store + business data |
| **Kubernetes** | Orchestration, HPA, service discovery |
| **Prometheus** | Metrics + alerts |
| **Grafana** | Dashboards + alerting UI |

---

## 4. Development Rules (STRICT)

1. **Сначала тесты** — никакой реализации без тестов
2. **Минимальные изменения** — не трогай работающий код без нужды
3. **Не ломай backward compatibility** — старые API должны работать
4. **Postman-friendly** — все endpoints должны быть тестируемы через Postman
5. **Idempotency** — все обработчики событий должны быть идемпотентны
6. **Compensation over 2PC** — SAGA rollback при ошибках

---

## 5. Критические исправления и багфиксы

### 5.1. Синхронизация данных (Funnel ↔ KPI ↔ Kanban)
- **Проблема:** Воронка, KPI и Канбан показывали разные цифры
- **Решение:** Добавлена колонка `orders.current_stage` — все компоненты читают из одного источника
- **Файлы:** `services/student-portal/portal.py` (dashboard_funnel, dashboard_kpis, dashboard_kanban)

### 5.2. Race condition: consumer'ы обрабатывали отменённые заказы
- **Проблема:** SAGA Monitor отменял заказ, но consumer'ы продолжали обработку
- **Решение:** Все consumer'ы проверяют `get_order(order_id).status` перед обработкой
- **Файлы:** `services/*/consumer.py` (inventory, payment, warehouse, delivery)

### 5.3. Rate limit блокировал simulation-service
- **Проблема:** Лимит был 5 запросов/мин — simulation-service получал 429
- **Решение:** Увеличен лимит до 500, очищаются при сбросе данных
- **Файлы:** `services/order-service/main.py`

### 5.4. Inventory застревал (SKU закончились)
- **Проблема:** Все заказы падали с `inventory_failed` — товара не было на складе
- **Решение:** Увеличены остатки до 1000 единиц каждого SKU
- **Файлы:** `k8s/base/init-db.sql`

### 5.5. Kafka не запускалась в K8s
- **Проблема:** KRaft mode конфликт с Zookeeper
- **Решение:** Перешли на Zookeeper mode + tcpSocket readiness probe
- **Файлы:** `k8s/base/infrastructure.yaml`

### 5.6. Dashboard не показывал иконку и кнопку сброса
- **Проблема:** Не было favicon и кнопки сброса данных
- **Решение:** Добавлены `ui-dashboard/favicon.svg` и кнопка "🧹 Сбросить всё"
- **Файлы:** `ui-dashboard/index.html`, `ui-dashboard/favicon.svg`

---

## 6. Project Structure (ключевые директории)

```
testing-polygon/
├── services/
│   ├── order-service/              # REST API + Rate Limiting
│   ├── inventory-consumer/         # Kafka → Inventory (gRPC:50051)
│   ├── payment-consumer/           # Kafka → Payment (gRPC:50052)
│   ├── warehouse-consumer/         # Kafka → Warehouse (gRPC:50053)
│   ├── delivery-consumer/          # Kafka → Delivery (gRPC:50054)
│   ├── saga-monitor/               # Compensation handler (gRPC:50055)
│   ├── student-portal/             # FastAPI + Dashboard + WS + K8s API
│   ├── simulation-service/         # Order generator
│   ├── ws-gateway/                 # WebSocket gateway
│   └── shared/                     # Общие утилиты (db_utils, base_consumer, grpc_service, proto files)
├── k8s/                            # Kubernetes манифесты
│   ├── base/                       # Kustomize base (26 resources)
│   │   ├── infrastructure.yaml     # Postgres, Redis, Zookeeper, Kafka StatefulSets
│   │   ├── init-db.sql             # SQL init script (tables + seed data)
│   │   ├── rbac.yaml               # Service account + clusterrolebinding
│   │   └── *.yaml                  # Deployment + Service для каждого сервиса
│   └── overlays/                   # Lite/Full overlays
├── charts/                         # Helm chart v2
├── ui-dashboard/                   # Frontend дашборда
│   ├── index.html                  # Dashboard UI (WebSocket, Funnel, Kanban, KPI)
│   └── favicon.svg                 # Custom warehouse box icon
├── scripts/                        # Launch scripts
│   ├── ЗАПУСТИТЬ_DOCKER.command    # Docker Compose launch
│   ├── ЗАПУСТИТЬ_KUBERNETES.command # K8s cluster launch (auto-init)
│   └── ОСТАНОВИТЬ_ВСЁ.command      # Full stop + cleanup
├── .ai-context/                    # AI bootstrap документы
└── docker-compose.yml              # Основной compose файл
```

---

## 7. Important Notes

- **Docker Compose** — основной режим для разработки (работает стабильно)
- **Kubernetes** — опциональный режим для обучения (Kind кластер, 3 ноды)
- **Kafka** — используется для audit + async communication
- **gRPC** — используется для sync communication между consumer'ами
- **GraphQL** — будет API Gateway поверх всех сервисов
- **Postman** — ключевой инструмент для студентов (16 collections)
- **Кнопка "Сбросить всё"** — очищает заказы, события, rate limits, начинает заново
- **Скрипты запуска** — автоматически инициализируют БД, настраивают K8s, запускают всё

---

## 8. First Step

Сейчас:
1. Прочитай все файлы из `.ai-context/`
2. Прочитай `EVENT_DRIVEN_ARCHITECTURE.md`
3. Опиши кратко текущую архитектуру
4. Подтверди понимание Phase 2 Roadmap
5. Подтверди готовность работать

**НЕ ПИШИ КОД НА ЭТОМ ЭТАПЕ** — только подтверждение понимания.