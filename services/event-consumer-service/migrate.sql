-- FulfilBox Production Schema Migration
-- Расширение схемы для полноценного бизнес-флоу

-- Products
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(30) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    price NUMERIC(10,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Warehouses
CREATE TABLE IF NOT EXISTS warehouses (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200) NOT NULL
);

-- Inventory
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(30) NOT NULL REFERENCES products(sku),
    warehouse_id VARCHAR(20) NOT NULL REFERENCES warehouses(id),
    available_qty INTEGER NOT NULL DEFAULT 0,
    reserved_qty INTEGER NOT NULL DEFAULT 0,
    UNIQUE(sku, warehouse_id)
);

-- Employees
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    role VARCHAR(30) NOT NULL CHECK (role IN ('picker','packer','courier'))
);

-- Vehicles
CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    plate_number VARCHAR(20) UNIQUE NOT NULL,
    capacity NUMERIC(5,1) NOT NULL DEFAULT 1.0
);

-- Обновляем orders (добавляем поля)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_price NUMERIC(10,2) DEFAULT 0;

-- Deliveries
CREATE TABLE IF NOT EXISTS deliveries (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) NOT NULL REFERENCES orders(id),
    courier_id INTEGER REFERENCES employees(id),
    vehicle_id INTEGER REFERENCES vehicles(id),
    status VARCHAR(20) DEFAULT 'assigned' CHECK (status IN ('assigned','in_transit','delivered'))
);

ALTER TABLE events ADD COLUMN IF NOT EXISTS entity_type VARCHAR(30);
ALTER TABLE events ADD COLUMN IF NOT EXISTS entity_id VARCHAR(50);
ALTER TABLE warehouses ADD COLUMN IF NOT EXISTS location VARCHAR(200) DEFAULT '';

-- Seed данные
INSERT INTO warehouses (id, name, location) VALUES
('WH-MSK-S', 'Склад Москва-Юг', 'Москва, Южный район'),
('WH-MSK-N', 'Склад Москва-Север', 'Москва, Северный район'),
('WH-SPB', 'Склад Санкт-Петербург', 'Санкт-Петербург, Центр')
ON CONFLICT (id) DO NOTHING;

INSERT INTO employees (name, role) VALUES
-- Pickers (10)
('Иван П.', 'picker'), ('Мария С.', 'picker'), ('Анна К.', 'picker'),
('Сергей Л.', 'picker'), ('Ольга М.', 'picker'), ('Николай Д.', 'picker'),
('Татьяна В.', 'picker'), ('Андрей Б.', 'picker'), ('Екатерина Н.', 'picker'),
('Павел Г.', 'picker'),
-- Packers (10)
('Алексей К.', 'packer'), ('Олег Н.', 'packer'), ('Ирина Т.', 'packer'),
('Виктор С.', 'packer'), ('Наталья Р.', 'packer'), ('Михаил Ф.', 'packer'),
('Светлана А.', 'packer'), ('Денир Ш.', 'packer'), ('Юлия Е.', 'packer'),
('Роман З.', 'packer'),
-- Couriers (10)
('Дмитрий В.', 'courier'), ('Елена Р.', 'courier'), ('Артём Ж.', 'courier'),
('Кирилл О.', 'courier'), ('Валентина П.', 'courier'), ('Максим И.', 'courier'),
('Лариса Х.', 'courier'), ('Григорий У.', 'courier'), ('Полина Ч.', 'courier'),
('Владислав Я.', 'courier')
ON CONFLICT DO NOTHING;

INSERT INTO vehicles (plate_number, capacity) VALUES
('А001АА77', 2.0), ('Б002ББ77', 1.5), ('В003ВВ77', 3.0)
ON CONFLICT (plate_number) DO NOTHING;
