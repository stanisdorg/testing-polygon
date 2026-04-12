# 🔐 Доступ к сервисам FulfilBox

## 📊 Kubernetes Dashboard

**URL:** https://localhost:8443

**Токен для входа (действует 24 часа):**
```
eyJhbGciOiJSUzI1NiIsImtpZCI6IlJjcFh4aEE5V2ZaVmVnSDhkTUwzN19mRkxtcTRJMDg4c09sRVRH
UmM5Y0EifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jY
WwiXSwiZXhwIjoxNzc2MTA4ODA4LCJpYXQiOjE3NzYwMjI0MDgsImlzcyI6Imh0dHBzOi8va3ViZXJuZ
XRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwianRpIjoiYmE0NzBmNjQtOWJmOC00NDViLThiZ
TktNWQxNGVlYjJkZmMzIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc
2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiOTQ3MzRiM
DItNTRiYy00ZjZhLThiN2QtNGRjNGJjMDNiYzgyIn19LCJuYmYiOjE3NzYwMjI0MDgsInN1YiI6InN5c
3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.vBSBbZ6MY
DOmS-NMuVwfKSvzMOpepP599kwELv8pF7AkKQPu1O2e7azFsQC_QFrrPafSRYdGkM89JMtpEqkqX3teI
9O78e75bcNTRi0bLC1X1ZLIaWwbs_yMfyrZhpj0tRkJQKUtKr3xbY2bRAABOQ-nCdcKTtgCaYIEkJbI3
MbvZ2aUj6pjUjNFWmv_cJSQFUS_gH3fAjtN7Vi6XtgcvkeAPIJIrv9SWrxQs7bSImOxh-a7SgxOQG-KZ
rLv-PSvO9uqelzKuk94S2MI8k9dU_Sna2lGipjmNWutHQyhcVLc_51MhU-3gzv2ijJlF_5449n6vxdzh
NciQ6JGyyBI2A
```

**Или получи новый токен:**
```bash
kubectl -n kubernetes-dashboard create token admin-user
```

---

## 🌐 Все сервисы

### Docker Compose (основные)
| Сервис | URL |
|--------|-----|
| Student Portal | http://localhost:8080 |
| Order Service API | http://localhost:8001 |
| Kafka UI (AKHQ) | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Redis Commander | http://localhost:8085 |
| Loki | http://localhost:3100 |

### Kubernetes
| Сервис | URL |
|--------|-----|
| Student Portal (K8s) | http://localhost:30080 |
| Kubernetes Dashboard | https://localhost:8443 |

---

## 🚀 Быстрая проверка

### 1. Создать тестовый заказ
```bash
curl -X POST http://localhost:8001/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-TEST-001",
    "items": [{"sku": "SKU-001", "qty": 1}],
    "delivery_address": "Moscow, Test St, 1"
  }'
```

### 2. Проверить события
```bash
curl http://localhost:8080/api/dashboard/events
```

### 3. Проверить трейс заказа
```bash
curl http://localhost:8080/api/dashboard/trace/ORD-TEST-001
```

### 4. Проверить Kafka топики
Открой: http://localhost:8081

### 5. Проверить в БД
```bash
docker exec testing-polygon-postgres-1 psql -U postgres -d fulfilbox -c \
  "SELECT event_type, order_id, step_name, created_at FROM events ORDER BY id DESC LIMIT 10;"
```

---

## ⚙️ Управление

### Перезапустить Docker Compose
```bash
cd /Users/mac/Documents/курс\ молодого\ бойца/testing-polygon
make restart
```

### Перезапустить Kubernetes
```bash
cd /Users/mac/Documents/курс\ молодого\ бойца/testing-polygon
bash k8s-setup.sh destroy
bash k8s-setup.sh create
```

### Проверить статус подов
```bash
kubectl get pods -n fulfilbox
```

### Логи конкретного пода
```bash
kubectl logs -n fulfilbox -f <pod-name>
```
