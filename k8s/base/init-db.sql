-- FulfilBox Database Initialization Script

-- 1. Orders
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(30) PRIMARY KEY,
    status VARCHAR(20) DEFAULT 'created',
    warehouse_id VARCHAR(20),
    total_price NUMERIC DEFAULT 0,
    saga_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. Order Items
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) NOT NULL REFERENCES orders(id),
    sku VARCHAR(30) NOT NULL,
    qty INTEGER NOT NULL DEFAULT 1
);

-- 3. Events
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    order_id VARCHAR(30) NOT NULL,
    payload JSONB,
    trace_id VARCHAR(50),
    entity_type VARCHAR(30),
    entity_id VARCHAR(50),
    saga_id VARCHAR(50),
    step_name VARCHAR(50),
    is_compensation BOOLEAN DEFAULT FALSE,
    processed BOOLEAN DEFAULT FALSE,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS events_order_event_unique ON events (order_id, event_type, saga_id);
CREATE INDEX IF NOT EXISTS idx_events_order_id ON events (order_id);

-- 4. Products
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(30) UNIQUE NOT NULL,
    name VARCHAR(200),
    price NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 5. Inventory
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(30),
    warehouse_id VARCHAR(20),
    available_qty INTEGER DEFAULT 0,
    reserved_qty INTEGER DEFAULT 0
);

-- 6. Warehouses
CREATE TABLE IF NOT EXISTS warehouses (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(200),
    capacity_m3 NUMERIC
);

-- 7. Employees
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    role VARCHAR(30)
);

-- 8. Vehicles
CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    plate_number VARCHAR(20) UNIQUE,
    capacity NUMERIC
);

-- 9. Deliveries
CREATE TABLE IF NOT EXISTS deliveries (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) REFERENCES orders(id),
    courier_name VARCHAR(100),
    status VARCHAR(20) DEFAULT 'assigned',
    cancelled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 10. Payments
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) REFERENCES orders(id),
    amount NUMERIC,
    status VARCHAR(20) DEFAULT 'requested',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Seed data: Warehouses
INSERT INTO warehouses (id, name, location, capacity_m3) VALUES
('WH-MSK-S', 'Склад Москва-Юг', 'Москва, ул. Складская 1', 5000),
('WH-MSK-N', 'Склад Москва-Север', 'Москва, ул. Северная 10', 3000),
('WH-KZN', 'Склад Казань', 'Казань, ул. Промышленная 5', 2500)
ON CONFLICT (id) DO NOTHING;

-- Seed data: Products
INSERT INTO products (sku, name, price) VALUES
('SKU-001', 'Наушники', 2990),
('SKU-002', 'Чехол', 990),
('SKU-003', 'Кабель', 490)
ON CONFLICT (sku) DO NOTHING;

-- Seed data: Inventory
INSERT INTO inventory (sku, warehouse_id, available_qty, reserved_qty) VALUES
('SKU-001', 'WH-MSK-S', 100, 0),
('SKU-002', 'WH-MSK-S', 200, 0),
('SKU-003', 'WH-MSK-S', 50, 0)
ON CONFLICT DO NOTHING;

-- Seed data: Employees (pickers)
INSERT INTO employees (name, role) VALUES
('Иван Петров', 'picker'),
('Мария Сидорова', 'picker'),
('Андрей Борисов', 'picker'),
('Ольга Морозова', 'picker'),
('Николай Дмитриев', 'picker'),
('Анна Козлова', 'picker'),
('Екатерина Новикова', 'picker'),
('Алексей Кузнецов', 'packer'),
('Михаил Фёдоров', 'packer'),
('Ирина Тимофеева', 'packer'),
('Светлана Александрова', 'packer'),
('Роман Захаров', 'packer'),
('Юлия Егорова', 'packer'),
('Олег Николаев', 'packer'),
('Виктор Соколов', 'packer'),
('Денис Шевченко', 'packer'),
('Наталья Романова', 'packer'),
('Артём Жуков', 'courier'),
('Валентина Павлова', 'courier'),
('Владислав Яковлев', 'courier'),
('Григорий Уткин', 'courier'),
('Дмитрий Васильев', 'courier'),
('Елена Рябова', 'courier'),
('Кирилл Орлов', 'courier'),
('Лариса Хохлова', 'courier'),
('Максим Ильин', 'courier'),
('Полина Чёрная', 'courier')
ON CONFLICT DO NOTHING;
