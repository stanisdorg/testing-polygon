#!/bin/bash
# Переходим в корень проекта (на уровень выше папки scripts)
cd "$(dirname "$0")/.."

echo "🧹 Остановка всего..."

docker compose down
kind delete cluster --name fulfilbox 2>/dev/null
docker container prune -f 2>/dev/null

echo ""
echo "✅ Всё остановлено и очищено!"
echo "💡 Mac теперь должен работать быстрее."
echo ""
echo "Нажми любую клавишу, чтобы закрыть это окно..."
read -n 1
