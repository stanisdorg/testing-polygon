-- FulfilBox: Cleanup stuck orders (stuck at order_created, never progressed)
-- Run: docker compose exec postgres psql -U fulfilbox -d fulfilbox -f /app/cleanup_stuck_orders.sql
-- Or:  docker compose exec postgres psql -U fulfilbox -d fulfilbox < cleanup_stuck_orders.sql

BEGIN;

-- ── Шаг 1: Показать сколько stuck заказов ──────────────────────────────────
\echo '=== Before cleanup: stuck orders count ==='
SELECT COUNT(*) as stuck_orders
FROM events e
WHERE e.event_type = 'order_created' 
  AND e.processed = true
  AND NOT EXISTS (
      SELECT 1 FROM events e2 
      WHERE e2.order_id = e.order_id 
        AND e2.event_type IN ('inventory_reserved', 'inventory_failed', 'order_completed', 'order_cancelled', 'order_failed')
  );

-- ── Шаг 2: Удалить stuck order_created события ─────────────────────────────
\echo '=== Deleting stuck order_created events ==='
DELETE FROM events 
WHERE event_type = 'order_created' 
  AND processed = true
  AND NOT EXISTS (
      SELECT 1 FROM events e2 
      WHERE e2.order_id = events.order_id 
        AND e2.event_type IN ('inventory_reserved', 'inventory_failed', 'order_completed', 'order_cancelled', 'order_failed')
  );
\echo 'Stuck order_created events deleted.'

-- ── Шаг 3: Удалить order_items для заказов без событий ──────────────────────
\echo '=== Deleting order_items for orders with no events ==='
DELETE FROM order_items 
WHERE order_id IN (
    SELECT o.id FROM orders o
    WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.order_id = o.id)
);
\echo 'Done.'

-- ── Шаг 4: Удалить deliveries для заказов без событий ───────────────────────
\echo '=== Deleting deliveries for orders with no events ==='
DELETE FROM deliveries 
WHERE order_id IN (
    SELECT o.id FROM orders o
    WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.order_id = o.id)
);
\echo 'Done.'

-- ── Шаг 5: Удалить orphan заказы (у которых нет ни одного события) ──────────
\echo '=== Deleting orphan orders ==='
DELETE FROM orders 
WHERE id NOT IN (SELECT DISTINCT order_id FROM events WHERE order_id IS NOT NULL);
\echo 'Orphan orders deleted.'

-- ── Шаг 5b: Удалить старые заказы без warehouse_id (до фикса) ───────────────
\echo '=== Deleting legacy orders without warehouse_id ==='
DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id IS NULL);
DELETE FROM deliveries WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id IS NULL);
DELETE FROM events WHERE order_id IN (SELECT id FROM orders WHERE warehouse_id IS NULL);
DELETE FROM orders WHERE warehouse_id IS NULL;
\echo 'Legacy orders without warehouse_id deleted.'

-- ── Шаг 6: Верификация — распределение по последнему событию ────────────────
\echo '=== Event type distribution (last event per order) ==='
SELECT le.event_type, COUNT(*) as cnt
FROM (
    SELECT DISTINCT ON (e.order_id) e.order_id, e.event_type
    FROM events e
    ORDER BY e.order_id, e.id DESC
) le
GROUP BY le.event_type
ORDER BY cnt DESC;

-- ── Шаг 7: Проверка warehouse_id ───────────────────────────────────────────
\echo '=== Orders by warehouse_id ==='
SELECT COALESCE(warehouse_id, '<NULL>') as warehouse, COUNT(*) as cnt
FROM orders 
GROUP BY warehouse_id 
ORDER BY cnt DESC;

-- ── Шаг 8: Проверка что stuck заказов больше нет ───────────────────────────
\echo '=== Remaining stuck orders (expected: 0) ==='
SELECT COUNT(*) as remaining_stuck
FROM events e
WHERE e.event_type = 'order_created' 
  AND e.processed = true
  AND NOT EXISTS (
      SELECT 1 FROM events e2 
      WHERE e2.order_id = e.order_id 
        AND e2.event_type IN ('inventory_reserved', 'inventory_failed', 'order_completed', 'order_cancelled', 'order_failed')
  );

COMMIT;

\echo '=== Cleanup completed ==='
