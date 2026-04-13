# 🎓 FulfilBox — Учебная DevOps платформа

Полноценная лабораторная среда для обучения QA-инженеров и разработчиков работе с микросервисами, Kubernetes, Kafka, gRPC, GraphQL, WebSocket и Redis.

---

## 🏗️ Архитектура (После Phase 2)

```
┌─────────────────────────────────────────────────────────────┐
│              СТУДЕНЧЕСКИЙ ДАШБОРД (8080)                    │
│                                                             │
│  Вкладки: Dashboard | Kanban | Kafka | gRPC | GraphQL |     │
│           Redis | WebSocket | K8s | Postman Training        │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┼────────────────────────────────────┐
│                    API GATEWAY LAYER                        │
│                                                             │
│  [REST API (8001)]  [GraphQL (8005)]  [WebSocket (8002)]    │
│       │                    │                    │           │
└───────┼────────────────────┼────────────────────┼───────────┘
        │                    │                    │
┌───────┼────────────────────┼────────────────────┼───────────┐
│       ▼                    ▼                    ▼           │
│  [Order Service] ◄── gRPC ──► [Inventory Consumer (50051)] │
│       │                            │                        │
│       ├── Kafka (audit) ───────────┘                        │
│       │                                                     │
│       ▼                                                     │
│  [Payment Consumer (50052)] ──gRPC──▶ [Warehouse (50053)]  │
│       │                                      │              │
│       │                                      ▼              │
│  [Delivery Consumer (50054)] ◄──gRPC── [SAGA Monitor]       │
│                                                             │
│  Redis (Pub/Sub, Cache, Rate Limit, Dedup)                  │
│  PostgreSQL (Event Store, 9 Business Tables)                │
│  Kafka (6 Topics + DLQ, Audit, Metrics Stream)              │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Режим 1: Docker Compose (Разработка/Отладка)       │   │
│  │  Режим 2: Kubernetes (Production/Обучение)          │   │
│  │  - StatefulSet: Postgres, Zookeeper, Kafka (KRaft)  │   │
│  │  - Deployments: 9 микросервисов + gRPC health       │   │
│  │  - HPA: Autoscaling по Kafka lag                    │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Два режима запуска

Проект поддерживает **два режима** работы. Выберите тот, который подходит вашему железу и задачам.

### 🔹 Режим 1: Docker Compose (Рекомендуется для слабых ПК)

**Минимальные требования:** 4 GB RAM, 2 CPU cores
**Время запуска:** 2-3 минуты
**Для кого:** Локальная разработка, быстрая отладка, изучение API

```bash
# Запуск
cd testing-polygon
docker compose up -d

# Остановка
docker compose down

# Логи
docker compose logs -f order-service
docker compose logs -f inventory-consumer

# Перезапуск одного сервиса
docker compose restart student-portal
```

**Что работает:**
- ✅ Все REST API (39 endpoints)
- ✅ GraphQL Gateway (8005)
- ✅ WebSocket (8002)
- ✅ gRPC Health Checks
- ✅ Redis Cache + Rate Limiting
- ✅ Kafka (6 topics)
- ✅ PostgreSQL + Event Store
- ✅ Prometheus + Grafana
- ✅ Chaos Engineering

**Что НЕ работает:**
- ❌ Kubernetes Dashboard
- ❌ HPA (Autoscaling)
- ❌ Service Discovery (K8s style)
- ❌ Liveness/Readiness Probes

**Как проверить:**
```bash
curl http://localhost:8001/api/v1/orders          # REST API
curl http://localhost:8005/graphql                 # GraphQL Playground
curl http://localhost:8001/api/v1/system/health    # gRPC Health
curl http://localhost:8080/api/dashboard/redis     # Redis Stats
open http://localhost:8080/dashboard/              # Dashboard UI
```

---

### 🔹 Режим 2: Kubernetes (Для полноценного обучения)

**Минимальные требования:** 8 GB RAM, 4 CPU cores
**Время запуска:** 10-15 минут
**Для кого:** Production-подобная среда, изучение K8s, Helm, Operators

```bash
# Создать кластер и развернуть всё
./k8s-setup.sh create

# Проверить поды
kubectl get pods -n fulfilbox

# Логи
kubectl logs -n fulfilbox deployment/order-service
kubectl logs -n fulfilbox fulfilbox-kafka-0

# Масштабирование
kubectl scale deployment inventory-consumer -n fulfilbox --replicas=3

# Удалить кластер
./k8s-setup.sh destroy
```

**Что работает:**
- ✅ Всё что в Docker Compose
- ✅ Kubernetes Dashboard
- ✅ HPA (Autoscaling по Kafka lag)
- ✅ Service Discovery
- ✅ Liveness/Readiness Probes (gRPC + HTTP)
- ✅ StatefulSet (Kafka KRaft, Postgres)
- ✅ Helm Chart для деплоя
- ✅ Resource Limits/Requests
- ✅ Network Policies (опционально)

**Что отличается от Docker Compose:**
| Фича | Docker Compose | Kubernetes |
|------|---------------|------------|
| **URL доступа** | localhost:8080 | NodePort (30080) или Ingress |
| **Масштабирование** | Вручную (`docker compose up --scale`) | Автоматически (HPA) |
| **Самовосстановление** | Перезапуск контейнера | Restart Policy + Probes |
| **Service Discovery** | DNS по имени сервиса | ClusterIP + FQDN |
| **Хранилище** | Volumes на хосте | PersistentVolumeClaims (PVC) |
| **Обновление** | Пересборка образа | Rolling Update |
| **gRPC Health** | Только эндпоинт | K8s Probes (автоматическая проверка) |

---

## 📡 Порты сервисов

| Сервис | Docker Compose | Kubernetes | Описание |
|--------|---------------|------------|----------|
| **Student Portal** | 8080 | 30080 (NodePort) | Дашборд + Training UI |
| **Order API (REST)** | 8001 | 8001 (ClusterIP) | 39 CRUD endpoints |
| **GraphQL Gateway** | 8005 | 8005 (ClusterIP) | Schema-first API |
| **WS Gateway** | 8002 | 8002 (ClusterIP) | Real-time events |
| **Inventory Consumer** | 8010 | 8010 + 50051 (gRPC) | Stock reservation |
| **Payment Consumer** | 8011 | 8011 + 50052 (gRPC) | Payment processing |
| **Warehouse Consumer** | 8012 | 8012 + 50053 (gRPC) | Picking → Packing |
| **Delivery Consumer** | 8013 | 8013 + 50054 (gRPC) | Courier assignment |
| **SAGA Monitor** | 8014 | 8014 + 50055 (gRPC) | Compensation handler |
| **Kafka** | 9092 | 9092 (Headless) | Event streaming |
| **PostgreSQL** | 5432 | 5432 (StatefulSet) | Event Store |
| **Redis** | 6379 | 6379 (ClusterIP) | Cache, Pub/Sub |
| **Prometheus** | 9090 | 9090 | Metrics |
| **Grafana** | 3000 | 3000 | Dashboards |
| **AKHQ (Kafka UI)** | 8081 | 8081 | Topics/Messages UI |

---

## 🛠️ Команды

### Docker Compose
```bash
make up          # Запустить всё
make down        # Остановить
make restart     # Перезапустить
make test        # Все тесты (178 passed)
make status      # Статус сервисов
make logs        # Логи всех сервисов
make clean       # Полная очистка
make help        # Все команды
```

### Kubernetes
```bash
./k8s-setup.sh create    # Создать кластер + деплой
./k8s-setup.sh status    # Статус подов
./k8s-setup.sh destroy   # Удалить кластер
kubectl get pods -n fulfilbox
kubectl get svc -n fulfilbox
kubectl logs -f deployment/order-service -n fulfilbox
```

---

## 📚 Технологии в проекте

| Технология | Где используется | Что практикуют студенты |
|------------|------------------|-------------------------|
| **REST API** | Order Service (39 endpoints) | CRUD, Postman, Auth, Pagination |
| **GraphQL** | GraphQL Gateway (8005) | Queries, Mutations, Subscriptions |
| **gRPC** | Consumer Health (50051-50055) | Proto files, Sync RPC, Health Check |
| **WebSocket** | WS Gateway (8002) | Real-time events, Reconnection |
| **Redis** | Cache, Rate Limit, Pub/Sub | Caching strategies, TTL, Channels |
| **Kafka** | 6 topics + DLQ | Event sourcing, Consumer groups |
| **PostgreSQL** | Event Store + 9 tables | SQL, Migrations, Indexes |
| **Kubernetes** | Deployments, StatefulSets | K8s API, HPA, Helm, Probes |
| **Prometheus** | Metrics | Alerting rules, Queries |
| **Grafana** | Dashboards | Visualization, Alerting |

---

## 📦 Структура проекта

```
testing-polygon/
├── services/
│   ├── order-service/              # REST API (39 endpoints)
│   ├── graphql-gateway/            # GraphQL Gateway (Strawberry)
│   ├── ws-gateway/                 # WebSocket + Redis Pub/Sub
│   ├── inventory-consumer/         # Kafka + gRPC (50051)
│   ├── payment-consumer/           # Kafka + gRPC (50052)
│   ├── warehouse-consumer/         # Kafka + gRPC (50053)
│   ├── delivery-consumer/          # Kafka + gRPC (50054)
│   ├── saga-monitor/               # Compensation + gRPC (50055)
│   ├── student-portal/             # Dashboard UI + APIs
│   ├── simulation-service/         # Order generator
│   └── shared/                     # Общие утилиты (db_utils, grpc_service)
├── k8s/                            # Kubernetes манифесты
│   ├── base/                       # Kustomize base (26 resources)
│   └── overlays/                   # Lite/Full profiles
├── charts/fulfilbox/               # Helm chart v2
├── protos/                         # gRPC .proto файлы
│   ├── health.proto
│   └── consumer_status.proto
├── ui-dashboard/                   # Frontend (HTML/JS/CSS)
├── .ai-context/                    # AI bootstrap документы
├── docs/                           # Документация для студентов
├── docker-compose.yml              # Docker Compose (все сервисы)
├── k8s-setup.sh                    # K8s кластер + деплой
└── Makefile                        # Команды для Docker Compose
```

---

## 🧪 Задания для студентов

Откройте **http://localhost:8080/dashboard/** — там 16 модулей с 100+ практическими заданиями:

### Postman Training (16 Collections)
1. **Authentication & Tokens** — JWT, Pre-scripts
2. **Products CRUD** — 5 endpoints, 17 тестов
3. **Warehouses CRUD** — 5 endpoints, 18 тестов
4. **Employees CRUD** — 5 endpoints, 20 тестов
5. **Inventory CRUD** — 8 endpoints, 29 тестов
6. **Vehicles CRUD** — 5 endpoints, 18 тестов
7. **Payments CRUD** — 5 endpoints, 20 тестов
8. **Deliveries CRUD** — 6 endpoints, 23 теста
9. **Orders Full Flow** — Complete SAGA lifecycle
10. **Events & Tracing** — Trace ID, Event Store
11. **Chaos Engineering** — Random delays, failures
12. **Dashboard & Monitoring** — KPIs, Funnel
13. **Kafka Operations** — Topics, Consumer Groups
14. **GraphQL Queries** — Schema, Resolvers
15. **gRPC Health Checks** — Service status
16. **Kubernetes API** — Pods, Deployments, HPA

### Дополнительные модули
- ✅ **Docker:** Контейнеры, образы, Compose
- ✅ **Kubernetes:** Деплой, масштабирование, отладка
- ✅ **Redis:** Кэш, Pub/Sub, Rate Limiting
- ✅ **Kafka:** Топики, consumer groups, DLQ
- ✅ **Chaos Engineering:** Управляемые сбои
- ✅ **Prometheus Alerts:** Метрики, алерты
- ✅ **gRPC:** Proto файлы, Health Checks
- ✅ **GraphQL:** Queries, Mutations, Subscriptions

---

## 🔍 Как отследить заказ

### Через REST API
```bash
# 1. Создать заказ
curl -X POST http://localhost:8001/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id":"ORD-001","items":[{"sku":"SKU-001","qty":1}]}'

# 2. Получить trace_id из ответа
# 3. Отследить все события
curl http://localhost:8001/api/v1/orders/ORD-001/trace

# 4. Проверить в Kafka UI
open http://localhost:8081
```

### Через GraphQL
```bash
curl -X POST http://localhost:8005/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ order(id: \"ORD-001\") { id status trace { eventType createdAt } } }"}'
```

### Через gRPC Health
```bash
# Проверить статус всех консьюмеров
curl http://localhost:8001/api/v1/system/health
```

### Через Дашборд
1. Откройте http://localhost:8080/dashboard/
2. Вкладка **WebSocket** → 🟢 Online
3. Создайте заказ → событие появится мгновенно
4. Вкладка **Kafka** → посмотрите сообщения в topics
5. Вкладка **Redis** → проверьте кэш и pub/sub каналы

---

## ⚙️ Конфигурация chaos

```bash
# Включить случайные ошибки
vi services/saga-monitor/chaos_config.json
# "random_failure": {"enabled": true, "probability": 0.1}

# Перезапустить consumer
docker compose restart saga-monitor

# Проверить алерты
open http://localhost:9090/alerts
```

---

## 🎓 Для преподавателей

### Как добавить новое задание
1. Создайте файл в `docs/assignments/`
2. Добавьте ссылку в Dashboard UI
3. Обновите `.ai-context/` если меняете архитектуру

### Как проверить прогресс студентов
1. Dashboard → Вкладка Monitoring
2. Grafana Dashboards → Student Progress
3. Prometheus Queries → Endpoint usage stats

---

## 📞 Поддержка

- 🐛 **Баг репорт:** `/bug` в этом чате
- 💡 **Фича реквест:** Обсуждение в диалоге
- 📖 **Документация:** `.ai-context/` и `docs/`

---

**FulfilBox** — создайте свой первый заказ и наблюдайте, как события проходят через всю систему! 🚀
