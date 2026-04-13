#!/bin/bash
# Переходим в корень проекта (на уровень выше папки scripts)
cd "$(dirname "$0")/.."

echo "🚀 Запуск FulfilBox (Kubernetes)..."

# 0. АВТО-ТЕГИРОВАНИЕ ОБРАЗОВ
echo "🏷️  Проверка и тегирование образов..."
for img in $(docker images --filter "reference=testing-polygon*" --format "{{.Repository}}:{{.Tag}}"); do 
    new_name=$(echo $img | sed 's/testing-polygon/fulfilbox/')
    docker tag $img $new_name 2>/dev/null
done

# 1. Проверка/Создание кластера
if kind get clusters 2>/dev/null | grep -q "fulfilbox"; then
    echo "✅ Кластер уже существует."
else
    echo "⏳ Создание кластера (это займёт пару минут)..."
    kind create cluster --name fulfilbox --wait 60s
fi

# 2. Загрузка образов в кластер
echo "⏳ Загрузка образов в K8s..."
IMAGES=$(docker images --filter "reference=fulfilbox/*" -q)
if [ -n "$IMAGES" ]; then
    echo "📦 Найдено образов: $(echo $IMAGES | wc -w | tr -d ' ')"
    kind load docker-image $IMAGES --name fulfilbox
else
    echo "❌ Образы не найдены!"
    exit 1
fi

# 3. Инициализация базы данных
echo "⏳ Применение манифестов..."
kubectl apply -k k8s/base/

echo "⏳ Ожидание готовности инфраструктуры..."
kubectl wait --for=condition=ready pod -l app=fulfilbox-postgres -n fulfilbox --timeout=120s 2>/dev/null || true
kubectl wait --for=condition=ready pod -l app=fulfilbox-zookeeper -n fulfilbox --timeout=120s 2>/dev/null || true
kubectl wait --for=condition=ready pod -l app=fulfilbox-kafka -n fulfilbox --timeout=180s 2>/dev/null || true

echo "⏳ Инициализация базы данных..."
kubectl cp k8s/base/init-db.sql fulfilbox/fulfilbox-postgres-0:/tmp/init-db.sql 2>/dev/null
kubectl exec statefulset/fulfilbox-postgres -n fulfilbox -- psql -U fulfilbox -d fulfilbox -f /tmp/init-db.sql 2>/dev/null

# 4. Настройка Student Portal (ConfigMap для конфига)
echo "⏳ Настройка Dashboard..."
kubectl create configmap student-portal-config \
  --from-file=config.json=services/student-portal/config-k8s.json \
  -n fulfilbox --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null

# Проверяем, нужно ли добавить volumes (только при первом деплое)
HAS_VOL=$(kubectl get deployment student-portal -n fulfilbox -o json 2>/dev/null | grep -c "config-vol" || echo "0")
if [ "$HAS_VOL" -eq 0 ]; then
    echo "📦 Монтирование ConfigMap..."
    kubectl get deployment student-portal -n fulfilbox -o json | \
      jq '.spec.template.spec.volumes += [{"name":"config-vol","configMap":{"name":"student-portal-config"}}] | 
          .spec.template.spec.containers[0].volumeMounts += [{"name":"config-vol","mountPath":"/app/config.json","subPath":"config.json"}]' | \
      kubectl apply -f - 2>/dev/null
fi

kubectl rollout restart deployment student-portal -n fulfilbox 2>/dev/null

# 5. Ожидание всех подов
echo "⏳ Ожидание готовности всех подов (до 3 мин)..."
kubectl wait --for=condition=ready pod --all -n fulfilbox --timeout=180s 2>/dev/null || echo "⚠️  Часть подов ещё запускается"

echo ""
echo "📊 Статус подов:"
kubectl get pods -n fulfilbox

echo ""
echo "🎉 K8s запущен и полностью настроен!"
echo ""
echo "📌 Чтобы открыть дашборд:"
echo "   1. В новом терминале выполни:"
echo "      kubectl port-forward svc/student-portal -n fulfilbox 8080:8080"
echo "   2. Открой в браузере: http://127.0.0.1:8080/dashboard/"
echo ""
echo "🛑 Чтобы остановить:"
echo "   kind delete cluster --name fulfilbox"
echo ""
echo "Нажми любую клавишу..."
read -n 1
