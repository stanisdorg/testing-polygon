# 🔐 Доступ к сервисам FulfilBox

## 📊 Kubernetes Dashboard

**URL:** https://localhost:8443

### 🎯 Способ 1: Kubeconfig файл (рекомендуется - без копирования)

1. На странице входа выбери **"Kubeconfig"**
2. Нажми **"Choose kubeconfig file"**
3. Выбери файл: `/Users/mac/Documents/курс молодого бойца/testing-polygon/dashboard-kubeconfig.yaml`
4. Нажми **Sign In**

> ✅ Этот файл содержит постоянный токен - будет работать всегда!

### 🔑 Способ 2: Токен

1. Выбери **"Token"**
2. Вставь этот токен:
```
eyJhbGciOiJSUzI1NiIsImtpZCI6IlJjcFh4aEE5V2ZaVmVnSDhkTUwzN19mRkxtcTRJMDg4c09sRVRH
UmM5Y0EifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZ
XJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsImt1YmVybmV0ZXMua
W8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuIiwia3ViZXJuZXRlc
y5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlc
m5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiI5NDczNGIwMi01NGJjL
TRmNmEtOGI3ZC00ZGM0YmMwM2JjODIiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZXJuZ
XRlcy1kYXNoYm9hcmQ6YWRtaW4tdXNlciJ9.WF_5zqGd2_Qbpf7NyPDRE3wq0Axq_hFMDR7ERVJMnSt2
6A4WpEiGBVP75ldGbCExVwEXVCib6R_OGzwaTfOqrP9_mdJoMR8nlRRrrt0EGwjElf0tNQFPIsTc0eEx
nwUAUCfNBls7fzi2Lt5jVlY7Vn8tIaWifmkATdDkgoLAL6sUOfuihLwB6kjahe8--9fAPjLoghD7s-Lx
sMPMoOItOkRtVsSi9_g9EppYXSg7ktFtwdJbPYngw6ePH_3sGY2YlWPS11Yl7umkqc7JMriZoN0adwH3
PswpD7d0JmXMg6bFM4p8nSDLsjsnFhalVNTbMoDDNju5icaHzqaLuSXz_Q
```

### 🔄 Если токен всё равно не работает

Получи новый постоянный токен:
```bash
kubectl get secret admin-user-token -n kubernetes-dashboard -o jsonpath='{.data.token}' | base64 -d
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
