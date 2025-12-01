-- fleetlogix_schema.sql
-- DROP + CREATE esquema normalizado (ciudades, vehicle_models, vehicles, drivers, routes, trips, deliveries, maintenance)

-- eliminar tablas en orden dependiente
DROP TABLE IF EXISTS maintenance CASCADE;
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS routes CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS vehicle_models CASCADE;
DROP TABLE IF EXISTS cities CASCADE;

-- CIUDADES (maestra)
CREATE TABLE cities (
  city_id        SERIAL PRIMARY KEY,
  city_name      VARCHAR(100) NOT NULL UNIQUE
);

-- Modelos/tipos de vehículo (normalización)
CREATE TABLE vehicle_models (
  model_id       SERIAL PRIMARY KEY,
  model_name     VARCHAR(100) NOT NULL UNIQUE,
  capacity_kg    INTEGER NOT NULL,
  fuel_type      VARCHAR(32) NOT NULL
);

-- Vehículos
CREATE TABLE vehicles (
  vehicle_id     SERIAL PRIMARY KEY,
  license_plate  VARCHAR(16) NOT NULL UNIQUE,
  model_id       INTEGER NOT NULL REFERENCES vehicle_models(model_id),
  acquisition_date DATE,
  status         VARCHAR(32) NOT NULL DEFAULT 'active'
);

-- Conductores
CREATE TABLE drivers (
  driver_id      SERIAL PRIMARY KEY,
  employee_code  VARCHAR(16) NOT NULL UNIQUE,
  first_name     VARCHAR(80),
  last_name      VARCHAR(80),
  license_number VARCHAR(32),
  license_type   VARCHAR(16),
  license_expiry DATE,
  phone          VARCHAR(32),
  hire_date      DATE,
  status         VARCHAR(32) DEFAULT 'active'
);

-- Rutas (origén / destino referencian cities)
CREATE TABLE routes (
  route_id             SERIAL PRIMARY KEY,
  route_code           VARCHAR(20) NOT NULL UNIQUE,
  origin_city_id       INTEGER NOT NULL REFERENCES cities(city_id),
  destination_city_id  INTEGER NOT NULL REFERENCES cities(city_id),
  distance_km          NUMERIC(8,2) NOT NULL,
  estimated_duration_hours NUMERIC(6,2),
  toll_cost            NUMERIC(12,2)
);

-- Viajes / trips
CREATE TABLE trips (
  trip_id              SERIAL PRIMARY KEY,
  vehicle_id           INTEGER NOT NULL REFERENCES vehicles(vehicle_id),
  driver_id            INTEGER NOT NULL REFERENCES drivers(driver_id),
  route_id             INTEGER NOT NULL REFERENCES routes(route_id),
  departure_datetime   TIMESTAMP WITHOUT TIME ZONE,
  arrival_datetime     TIMESTAMP WITHOUT TIME ZONE,
  fuel_consumed_liters NUMERIC(10,2),
  total_weight_kg      NUMERIC(10,2),
  status               VARCHAR(32)
);

-- Entregas / deliveries
CREATE TABLE deliveries (
  delivery_id          SERIAL PRIMARY KEY,
  trip_id              INTEGER NOT NULL REFERENCES trips(trip_id),
  tracking_number      VARCHAR(40) UNIQUE,
  customer_name        VARCHAR(200),
  delivery_address     TEXT,
  package_weight_kg    NUMERIC(8,2),
  scheduled_datetime   TIMESTAMP WITHOUT TIME ZONE,
  delivered_datetime   TIMESTAMP WITHOUT TIME ZONE,
  delivery_status      VARCHAR(32),
  recipient_signature  BOOLEAN
);

-- Mantenimientos
CREATE TABLE maintenance (
  maintenance_id       SERIAL PRIMARY KEY,
  vehicle_id           INTEGER NOT NULL REFERENCES vehicles(vehicle_id),
  maintenance_date     DATE NOT NULL,
  maintenance_type     VARCHAR(100),
  description          TEXT,
  cost                 NUMERIC(12,2),
  next_maintenance_date DATE,
  performed_by         VARCHAR(120)
);

-- Indexes para performance básica
CREATE INDEX idx_trips_vehicle ON trips(vehicle_id);
CREATE INDEX idx_trips_driver ON trips(driver_id);
CREATE INDEX idx_deliveries_trip ON deliveries(trip_id);

-- Insertar 5 ciudades base
INSERT INTO cities (city_name) VALUES
('Bogotá'), ('Medellín'), ('Cali'), ('Barranquilla'), ('Cartagena')
ON CONFLICT DO NOTHING;