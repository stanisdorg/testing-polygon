#!/bin/bash
# Переходим в корень проекта (на уровень выше папки scripts)
cd "$(dirname "$0")/.."

echo "🚀 Запуск FulfilBox (Docker Compose)..."

# Проверка: запущен ли уже?
if docker compose ps --quiet 2>/dev/null | grep -q .; then
    echo "⚠️  Docker Compose уже запущен. Перезапуск..."
    docker compose down
fi

# Запуск
docker compose up -d

echo "⏳ Ожидание запуска..."
sleep 10

echo ""
echo "✅ Статус:"
docker compose ps --format "table {{.Name}}\t{{.Status}}" | head -10

echo ""
echo "🎉 FulfilBox запущен!"
echo "📊 Dashboard: http://localhost:8080/dashboard/"
echo ""
echo "Нажми любую клавишу, чтобы закрыть это окно..."
read -n 1
