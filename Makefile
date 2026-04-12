#!/usr/bin/env bash
# ============================================================
# FulfilBox — Учебная DevOps платформа
# ============================================================
# Использование: make <цель>
# ============================================================

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.PHONY: up down restart test logs clean status help

up: ## Запустить все сервисы
	@echo "🚀 Запуск FulfilBox..."
	docker compose up -d

down: ## Остановить все сервисы
	@echo "🛑 Остановка FulfilBox..."
	docker compose down

restart: ## Перезапустить все сервисы
	@echo "🔄 Перезапуск FulfilBox..."
	docker compose down
	docker compose up -d

status: ## Показать статус сервисов
	@docker compose ps

logs: ## Показать логи всех сервисов
	docker compose logs -f

logs-consumer: ## Логи event-consumer
	docker compose logs -f event-consumer-service

logs-order: ## Логи order-service
	docker compose logs -f order-service

logs-portal: ## Логи student-portal
	docker compose logs -f student-portal

logs-akhq: ## Логи AKHQ (Kafka UI)
	docker compose logs -f akhq

logs-elk: ## Логи ELK Stack
	docker compose logs -f elasticsearch kibana filebeat

kibana-setup: ## Инициализировать Kibana (index patterns, searches)
	bash kibana-setup.sh

test-order: ## Тесты order-service
	cd services/order-service && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pytest httpx kafka-python-ng -q 2>/dev/null && pytest -v

test-consumer: ## Тесты event-consumer-service
	cd services/event-consumer-service && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pytest redis kafka-python -q 2>/dev/null && pytest -v

test-ws: ## Тесты ws-gateway
	cd services/ws-gateway && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pytest -q 2>/dev/null && pytest -v

test-sim: ## Тесты simulation-service
	cd services/simulation-service && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pytest -q 2>/dev/null && pytest -v

test-alerts: ## Тесты Prometheus alerts
	cd tests && python3 -m venv .venv && source .venv/bin/activate && pip install pyyaml pytest -q 2>/dev/null && pytest test_prometheus_alerts.py -v

test: test-order test-consumer test-ws test-sim test-alerts ## Запустить все тесты

build: ## Пересобрать все образы
	docker compose build --no-cache

clean: ## Очистить контейнеры, образы, volumes
	docker compose down --rmi local --volumes --remove-orphans

help: ## Показать справку
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
