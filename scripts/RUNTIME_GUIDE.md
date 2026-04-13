# 🚀 Руководство по запуску и остановке FulfilBox

В этом файле собраны все команды и скрипты для управления проектом на Mac.

---

## 💻 Сценарии использования

### 🟢 Сценарий 1: "Я хочу учиться и тренироваться" (Docker Compose)
**Когда использовать:** Локальная разработка, тестирование API, работа с Postman.
**Требования:** 4 GB RAM.

**Быстрый старт:**
```bash
./start-docker.sh
```

**Остановка:**
```bash
./stop-docker.sh
```

---

### 🔵 Сценарий 2: "Я изучаю Kubernetes" (K8s Cluster)
**Когда использовать:** Изучение оркестрации, Helm, StatefulSets, масштабирования.
**Требования:** 8 GB RAM.

**Быстрый старт:**
```bash
./start-k8s.sh
```

**Остановка:**
```bash
./stop-k8s.sh
```

---

### 🔴 Сценарий 3: "Устали мак/нужно освободить ресурсы"
**Когда использовать:** После работы, перед сном, если мак начал тормозить.

**Полная остановка всего:**
```bash
./stop-all.sh
```

---

## 📡 Порты и доступы

| Сервис | Порт | URL |
|--------|------|-----|
| **Student Portal (Dashboard)** | 8080 | http://localhost:8080 |
| **Order API (REST)** | 8001 | http://localhost:8001 |
| **GraphQL Gateway** | 8005 | http://localhost:8005 |
| **WS Gateway** | 8002 | http://localhost:8002 |
| **Prometheus** | 9090 | http://localhost:9090 |
| **Grafana** | 3000 | http://localhost:3000 (admin/fulfilbox2024) |
| **AKHQ (Kafka UI)** | 8081 | http://localhost:8081 |

---

## 🛠️ Ручные команды (если скрипты не работают)

### Docker Compose
```bash
# Запуск
docker compose up -d

# Остановка
docker compose down

# Перезапуск одного сервиса
docker compose restart student-portal

# Логи
docker compose logs -f order-service

# Очистка
docker compose down -v  # удаляет и данные (осторожно!)
```

### Kubernetes
```bash
# Создание кластера и деплой
kind create cluster --name fulfilbox --wait 60s
kind load docker-image $(docker images -q "fulfilbox/*") --name fulfilbox
kubectl apply -k k8s/base/

# Проверка статуса
kubectl get pods -n fulfilbox

# Удаление кластера
kind delete cluster --name fulfilbox
```

---

## ⚠️ Частые проблемы

### 1. "Port already in use"
Значит, старый процесс не был остановлен.
**Решение:** Запусти `./stop-all.sh`, подожди 10 секунд, затем запусти нужный скрипт.

### 2. "No space left on device"
Docker съел всё место.
**Решение:**
```bash
docker system prune -a
docker volume prune
```

### 3. K8s кластер не запускается
Возможно, конфликтует с Docker Compose.
**Решение:** Убедись, что Docker Compose остановлен (`./stop-docker.sh`), прежде чем запускать K8s.

---

## 📞 Поддержка

Если что-то пошло не так:
1. Проверь логи: `docker compose logs -f <имя_сервиса>`
2. Проверь K8s поды: `kubectl get pods -n fulfilbox`
3. Перезапусти сервис: `docker compose restart <имя_сервиса>`
