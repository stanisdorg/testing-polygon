# 🎓 FulfilBox — Учебная DevOps платформа

Полноценная лабораторная среда для обучения QA-инженеров работе с микросервисами, мониторингом и chaos engineering.

---

## 🏗️ Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    FulfilBox Platform                       │
│                                                             │
│  Student Portal (8080) ──┐                                  │
│                          │                                  │
│  Order Service (8001) ───┼──► Kafka ───► Event Consumer     │
│                          │          │                       │
│  WS Gateway (8002) ◄─────┘          ▼                       │
│                          │     Redis Cache                   │
│  Prometheus (9090)       │     DLQ → Grafana (3000)         │
│  Grafana (3000)          │                                  │
│                          │     PostgreSQL (5432)             │
│  Live Orders UI (/ui)    │                                  │
│  Tasks UI (/tasks)       │                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Быстрый старт

### Вариант 1: Lite (для локальной разработки, ~3.5 GB RAM)
```bash
docker compose up -d          # Loki + Grafana для логов
open http://localhost:8080
```

### Вариант 2: Full (для сервера, ~7 GB RAM)
```bash
docker compose --profile full up -d   # ELK (Elasticsearch + Kibana)
open http://localhost:8080
```

### Запустить тесты
```bash
make test
```

---

## 📡 Порты сервисов

| Сервис | Порт | URL |
|--------|------|-----|
| **Student Portal** | 8080 | http://localhost:8080 |
| **Order API** | 8001 | http://localhost:8001 |
| **Swagger UI** | 8001 | http://localhost:8001/docs |
| **ReDoc** | 8001 | http://localhost:8001/redoc |
| **WS Gateway** | 8002 | http://localhost:8002 |
| **Consumer Metrics** | 8003 | http://localhost:8003/metrics |
| **AKHQ (Kafka UI)** | 8081 | http://localhost:8081 |
| **Kibana** | 5601 | http://localhost:5601 (full profile) |
| **Elasticsearch** | 9200 | http://localhost:9200 (full profile) |
| **Loki** | 3100 | http://localhost:3100 (default) |
| **Redis Commander** | 8085 | http://localhost:8085 (default) |
| **RedisInsight** | 8086 | http://localhost:8086 (full profile) |
| **Prometheus** | 9090 | http://localhost:9090 |
| **Prometheus Alerts** | 9090 | http://localhost:9090/alerts |
| **Grafana** | 3000 | http://localhost:3000 (admin/fulfilbox2024) |
| **K8s Dashboard** | — | см. `./k8s-setup.sh` |
| **Live Events UI** | — | http://localhost:8080/events/live |
| **Tasks UI** | — | http://localhost:8080/tasks/ |

---

## 🛠️ Команды

```bash
make up          # Запустить всё
make down        # Остановить
make restart     # Перезапустить
make test        # Все тесты
make status      # Статус сервисов
make logs        # Логи всех сервисов
make clean       # Полная очистка
make help        # Все команды
```

---

## 📚 Модули курса

| Модуль | Тема | Что изучает студент |
|--------|------|---------------------|
| 01 | Docker | Контейнеры, образы, Docker Compose |
| 02 | REST API + Swagger | API документация, Postman |
| 03 | Kubernetes | Деплой, масштабирование, отладка |
| 04 | Redis | Кэш, структуры данных, TTL |
| 05 | Kafka | Топики, consumer groups, DLQ |
| 06 | Chaos Engineering | Управляемые сбои, resilience |
| 07 | Prometheus Alerts | Метрики, алерты, локализация |

---

## 🧪 Задания для студентов

Откройте **http://localhost:8080/tasks/** — там 7 модулей с 30+ практическими заданиями:

- ✅ Docker: запуск контейнеров, сборка образов
- ✅ REST API: curl, Swagger, Postman
- ✅ Kubernetes: деплой, логи, масштабирование
- ✅ Redis: структуры данных, hit ratio
- ✅ Kafka: топики, trace_id, consumer groups
- ✅ Chaos: задержки, ошибки, inventory mismatch
- ✅ Alerts: Prometheus, firing alerts, root cause

---

## 🔍 Как отследить заказ

1. Создай заказ через API → получи `trace_id`
2. Найди `trace_id` в логах consumer
3. Посмотри событие в Kafka (Student Portal → Kafka)
4. Проверь метрики в Grafana
5. Включи chaos → наблюдай влияние на алерты

---

## ⚙️ Конфигурация chaos

```bash
# Включить случайные ошибки
vi services/event-consumer-service/chaos_config.json
# "random_failure": {"enabled": true, "probability": 0.1}

# Перезапустить consumer
make restart

# Проверить алерты
open http://localhost:9090/alerts
```
