# FulfilBox AI Bootstrap — Phase 2 Roadmap

## 1. Current State

Ты работаешь с системой FulfilBox — учебная DevOps платформа для QA инженеров.

### Что УЖЕ работает:
- ✅ **Docker Compose** — полностью рабочий стек
- ✅ **Event-Driven архитектура** — 5 независимых Kafka consumers
- ✅ **SAGA Compensation** — автоматический rollback при ошибках
- ✅ **Dashboard UI** — воронка, KPI, сотрудники, склады, события (http://localhost:8080/dashboard/)
- ✅ **PostgreSQL** — Event Store + business data (9 таблиц)
- ✅ **Kafka** — 6 topics для межсервисной коммуника
- ✅ **Redis** — кеш, dedup locks, worker load tracking
- ✅ **Student Portal** — UI с WebSocket для real-time событий
- ✅ **Prometheus + Grafana** — мониторинг и алерты
- ✅ **Chaos Engineering** — конфигурируемые сценарии сбоев

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

### Фаза 1: База + K8s ✅ в процессе
- [ ] Исправить Kafka StatefulSet в K8s
- [ ] Добавить 3 новых Kafka topics: `dead_letter_queue`, `audit_log`, `metrics_stream`
- [ ] Обновить K8s манифесты для всех 5 consumer'ов
- [ ] Полноценно запустить Kind кластер

### Фаза 2: Redis расширение 🔴
- [ ] Redis pub/sub для real-time событий (вместо polling)
- [ ] Redis cache для заказов (TTL 5 мин)
- [ ] Redis rate limiting для API
- [ ] Вкладка Redis на дашборде (keys, memory, pub/sub channels)

### Фаза 3: gRPC inter-service communication 🔌
- [ ] Создать `.proto` файлы для consumer коммуникации
- [ ] Добавить gRPC серверы в consumer'ы
- [ ] Заменить Kafka на gRPC для critical path (оставить Kafka для audit)
- [ ] gRPC health check endpoints
- [ ] Вкладка gRPC на дашборде (health, methods, latency, proto files)

### Фаза 4: GraphQL API Gateway 🎯
- [ ] Создать GraphQL Gateway (Strawberry GraphQL)
- [ ] Schema: orders, events, warehouses, employees, inventory, deliveries
- [ ] Resolvers к существующим сервисам
- [ ] GraphQL Playground в дашборде
- [ ] Subscriptions для real-time событий

### Фаза 5: WebSocket расширение 🔌
- [ ] Расширить ws-gateway для bidirectional communication
- [ ] Подключить к Redis pub/sub
- [ ] Live события на дашборде (без polling!)
- [ ] Вкладка WebSocket (connected clients, messages/sec, ping)

### Фаза 6: RESTful CRUD для Postman 📮
- [ ] Products CRUD (full)
- [ ] Warehouses CRUD (full)
- [ ] Employees CRUD (full)
- [ ] Inventory CRUD (full)
- [ ] Vehicles CRUD (full)
- [ ] Payments CRUD (full)
- [ ] Deliveries CRUD (full)
- [ ] Events & Tracing API
- [ ] Chaos Engineering API
- [ ] Dashboard & Monitoring API
- [ ] Postman Collection JSON (16 папок, pre-scripts, tests, environments)

### Фаза 7: Kubernetes полный деплой ☸️
- [ ] Исправить все K8s манифесты
- [ ] Helm chart v2 с новыми сервисами
- [ ] HPA для consumer'ов (autoscaling по lag)
- [ ] K8s Dashboard вкладка (pods, deployments, HPA, events)

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

## 5. Project Structure (ключевые директории)

```
testing-polygon/
├── services/
│   ├── order-service/              # REST API + GraphQL Gateway
│   ├── inventory-consumer/         # Kafka → Inventory (gRPC server)
│   ├── payment-consumer/           # Kafka → Payment (gRPC server)
│   ├── warehouse-consumer/         # Kafka → Warehouse (gRPC server)
│   ├── delivery-consumer/          # Kafka → Delivery (gRPC server)
│   ├── saga-monitor/               # Compensation handler
│   ├── student-portal/             # FastAPI + Dashboard + WS
│   ├── simulation-service/         # Order generator
│   ├── ws-gateway/                 # WebSocket gateway
│   └── shared/                     # Общие утилиты (db_utils, base_consumer)
├── k8s/                            # Kubernetes манифесты
│   ├── base/                       # Kustomize base
│   └── overlays/                   # Lite/Full overlays
├── charts/                         # Helm chart
├── ui-dashboard/                   # Frontend дашборда
├── .ai-context/                    # AI bootstrap документы
└── docker-compose.yml              # Основной compose файл
```

---

## 6. Important Notes

- **Docker Compose** — основной режим для разработки (работает стабильно)
- **Kubernetes** — опциональный режим для обучения (Kind кластер)
- **Kafka** — используется для audit + async communication
- **gRPC** — будет использоваться для sync communication между consumer'ами
- **GraphQL** — будет API Gateway поверх всех сервисов
- **Postman** — ключевой инструмент для студентов (16 collections planned)

---

## 7. First Step

Сейчас:
1. Прочитай все файлы из `.ai-context/`
2. Прочитай `EVENT_DRIVEN_ARCHITECTURE.md`
3. Опиши кратко текущую архитектуру
4. Подтверди понимание Phase 2 Roadmap
5. Подтверди готовность работать

**НЕ ПИШИ КОД НА ЭТОМ ЭТАПЕ** — только подтверждение понимания.
