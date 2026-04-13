#!/bin/bash
# Переходим в корень проекта (на уровень выше папки scripts)
cd "$(dirname "$0")/.."

echo "🚀 Запуск FulfilBox (Kubernetes)..."

if kind get clusters 2>/dev/null | grep -q "fulfilbox"; then
    echo "⚠️  Кластер уже есть. Пересоздаю..."
    kind delete cluster --name fulfilbox
fi

echo "⏳ Создание кластера..."
kind create cluster --name fulfilbox --wait 60s

echo "⏳ Загрузка образов..."
IMAGES=$(docker images --filter "reference=fulfilbox/*" -q)
if [ -n "$IMAGES" ]; then
    kind load docker-image $IMAGES --name fulfilbox
fi

echo "⏳ Деплой..."
kubectl apply -k k8s/base/
kubectl wait --for=condition=ready pod --all -n fulfilbox --timeout=120s 2>/dev/null

echo ""
echo "✅ K8s запущен!"
echo "📊 Dashboard: http://127.0.0.1:30080/dashboard/"
echo ""
echo "Нажми любую клавишу, чтобы закрыть это окно..."
read -n 1
