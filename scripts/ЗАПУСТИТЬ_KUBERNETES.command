#!/bin/bash
# Переходим в корень проекта (на уровень выше папки scripts)
cd "$(dirname "$0")/.."

echo "🚀 Запуск FulfilBox (Kubernetes)..."

# 0. АВТО-ТЕГИРОВАНИЕ ОБРАЗОВ
# K8s манифесты ждут имена "fulfilbox/*", а Docker Compose создает "testing-polygon/*"
echo "🏷️  Проверка и тегирование образов..."
for img in $(docker images --filter "reference=testing-polygon*" --format "{{.Repository}}:{{.Tag}}"); do 
    new_name=$(echo $img | sed 's/testing-polygon/fulfilbox/')
    docker tag $img $new_name 2>/dev/null
done

# 1. Проверка/Создание кластера
if kind get clusters 2>/dev/null | grep -q "fulfilbox"; then
    echo "✅ Кластер уже существует."
else
    echo "⏳ Создание кластера..."
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

# 3. Деплой
echo "⏳ Деплой манифестов..."
kubectl apply -k k8s/base/

echo "⏳ Ожидание готовности подов (до 3 мин)..."
kubectl wait --for=condition=ready pod --all -n fulfilbox --timeout=180s 2>/dev/null || echo "⚠️  Часть подов ещё запускается"

echo ""
echo "📊 Статус подов:"
kubectl get pods -n fulfilbox

echo ""
echo "🎉 K8s запущен!"
echo "📊 Dashboard: http://127.0.0.1:30080/dashboard/"
echo "   (требуется port-forward: kubectl port-forward svc/student-portal -n fulfilbox 30080:8080)"
echo ""
echo "Нажми любую клавишу..."
read -n 1
