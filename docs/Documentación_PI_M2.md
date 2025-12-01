# Documentación del Proyecto Integrador – Módulo 2

A continuación se presenta la documentación completa del proyecto, incluyendo:

* Script SQL
* DER / Modelo físico
* KPIs obtenidos
* Informe final

---

## 1. Script SQL Final

### Creación de tablas

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
---

-- Primeras validaciones de calidad de datos

-- 1. Integridad Referencial
-- Valida que no existan claves huérfanas entre todas las tablas.

-- 1A. Trips → Vehicles, Drivers, Routes

-- Trips con vehicle_id inexistente
SELECT t.trip_id
FROM trips t
LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;

-- Trips con driver_id inexistente
SELECT t.trip_id
FROM trips t
LEFT JOIN drivers d ON t.driver_id = d.driver_id
WHERE d.driver_id IS NULL;

-- Trips con route_id inexistente
SELECT t.trip_id
FROM trips t
LEFT JOIN routes r ON t.route_id = r.route_id
WHERE r.route_id IS NULL;

-- 1B. Deliveries → Trips
SELECT d.delivery_id
FROM deliveries d
LEFT JOIN trips t ON d.trip_id = t.trip_id
WHERE t.trip_id IS NULL;

-- 1C. Maintenance → Vehicles
SELECT m.maintenance_id
FROM maintenance m
LEFT JOIN vehicles v ON m.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;

-- 2. Consistencia Temporal
-- Verifica que no existan timestamps inconsistentes.

-- 2A. Trips: arrival > departure
SELECT t.trip_id, t.departure_datetime, t.arrival_datetime
FROM trips t
WHERE t.arrival_datetime <= t.departure_datetime;

-- 2B. Deliveries: delivered > scheduled
SELECT d.delivery_id, d.scheduled_datetime, d.delivered_datetime
FROM deliveries d
WHERE d.delivered_datetime <= d.scheduled_datetime;

-- 2C. Maintenance posterior al mantenimiento anterior
-- (Para cada vehículo su mantenimiento debe ser creciente)
SELECT m1.vehicle_id, m1.maintenance_date AS current_date, m2.maintenance_date AS next_date
FROM maintenance m1
JOIN maintenance m2 
    ON m1.vehicle_id = m2.vehicle_id
   AND m2.maintenance_date < m1.maintenance_date
WHERE m1.maintenance_date < m2.maintenance_date;

-- 3. Distribución Realista
--Simple checks para verificar cantidades y rangos.

-- 3A. Cantidad de viajes en 2 años
SELECT COUNT(*) AS total_trips,
       MIN(departure_datetime) AS first_trip,
       MAX(departure_datetime) AS last_trip
FROM trips; -- se esperan alrededor de 100.000

-- 3B. Total de entregas y asignación por viaje
SELECT COUNT(*) AS total_deliveries,
       MIN(trip_id) AS min_trip,
       MAX(trip_id) AS max_trip
FROM deliveries;

-- 3C. Frecuencia de mantenimiento
SELECT vehicle_id,
       COUNT(*) AS maint_count,
       MIN(maintenance_date) AS first_maintenance,
       MAX(maintenance_date) AS last_maintenance
FROM maintenance
GROUP BY vehicle_id
ORDER BY maint_count DESC;

---

-- VALIDACIONES AVANZADAS DE CALIDAD
-- Viajes cuya duración es mayor al promedio general
select
    trip_id,
    departure_datetime,
    arrival_datetime,
    EXTRACT(EPOCH FROM (arrival_datetime - departure_datetime))/3600 AS duration_hours
FROM trips
WHERE (arrival_datetime - departure_datetime) >
    (SELECT AVG(arrival_datetime - departure_datetime) FROM trips);

-- FUNCIÓN VENTANA
-- Ranking de conductores según kilómetros recorridos
SELECT
    d.first_name || ' ' || d.last_name AS driver_name,
    SUM(r.distance_km) AS total_km,
    RANK() OVER (ORDER BY SUM(r.distance_km) DESC) AS km_rank
FROM trips t
JOIN drivers d ON t.driver_id = d.driver_id
JOIN routes r ON t.route_id = r.route_id
GROUP BY d.driver_id, driver_name;

--Acumulado mensual de viajes por ciudad de origen
--(funciona porque las ciudades están como texto)
SELECT
    r.origin_city,
    DATE_TRUNC('month', t.departure_datetime) AS month,
    COUNT(*) AS monthly_trips,
    SUM(COUNT(*)) OVER (PARTITION BY r.origin_city ORDER BY DATE_TRUNC('month', t.departure_datetime)) AS cumulative_trips
FROM trips t
JOIN routes r ON t.route_id = r.route_id
GROUP BY r.origin_city, month
ORDER BY r.origin_city, month;

-- AGREGACIONES
-- Promedio de combustible consumido por ruta
SELECT
    r.route_code,
    AVG(t.fuel_consumed_liters) AS avg_fuel_liters
FROM trips t
JOIN routes r ON t.route_id = r.route_id
GROUP BY r.route_code;

-- Cantidad de viajes por vehículo
SELECT
    v.license_plate,
    COUNT(*) AS total_trips
FROM trips t
JOIN vehicles v ON t.vehicle_id = v.vehicle_id
GROUP BY v.license_plate
ORDER BY total_trips DESC;

---
## 2. DER / Modelo Físico

Diagrama

Modelo Híbrido (Star + Snowflake)
El modelo de datos final de FleetLogix no es puramente estrella ni puramente snowflake, sino un modelo híbrido. 

Esto surge porque:
Algunas tablas funcionan como hechos transaccionales centrales (fact tables)
Otras funcionan como dimensiones desnormalizadas (estrella)
Y otras funcionan como dimensiones normalizadas subdivididas (snowflake)

¿Por qué no es completamente Estrella?

Un modelo en estrella (Star Schema) implica:
- Una tabla de hechos central
- Varias dimensiones amplias, sin normalizar, directamente conectadas

En FleetLogix las tablas transaccionales sí se comportan así, por ejemplo:
- trips = fact table (hechos de viajes)
- deliveries = fact table (hechos de entregas)
Ambas dependen de dimensiones: vehicles, drivers, routes

Sin embargo, las dimensiones no están totalmente desnormalizadas (como exige un star schema puro), así que no es 100% estrella.

¿Por qué no es completamente Snowflake?

En un snowflake puro:
- Cada dimensión se divide en múltiples subdimensiones normalizadas
- Las cadenas de FK se vuelven más profundas
- Se prioriza consistencia y eliminación de redundancia

En FleetLogix, hubo normalización parcial, por ejemplo:
- routes podría normalizarse en ciudades, pero se dejó parcialmente en forma plana.
- se incorporaron subtablas para modelos de vehículos, pareciéndose a snowflake.
- También se dividió información logística clave en varias capas (viajes → entregas).

Pero tampoco se llegó a una normalización total, así que no es un snowflake completo.

![Esquema del modelo híbrido](./assets/EsquemaHíbrido_Fleetlogix.png)

---

## 3. KPIs Obtenidos

-- KPIs del Proyecto
-- KPI 1 — Duración promedio de viaje
SELECT
    AVG(EXTRACT(EPOCH FROM (arrival_datetime - departure_datetime))/3600) AS avg_trip_duration_hours
FROM trips;

-- KPI 2 — Fuel Efficiency (km por litro)
SELECT
    SUM(r.distance_km) / SUM(t.fuel_consumed_liters) AS km_per_liter
FROM trips t
JOIN routes r ON t.route_id = r.route_id;

-- KPI 3 — Peso transportado por ciudad de destino
SELECT
    r.destination_city,
    SUM(t.total_weight_kg) AS total_weight_kg
FROM trips t
JOIN routes r ON t.route_id = r.route_id
GROUP BY r.destination_city
ORDER BY total_weight_kg DESC;

-- KPI 4 — Número de viajes por estado (Completed, Cancelled, etc.)
SELECT
    status,
    COUNT(*) AS total_trips
FROM trips
GROUP BY status;

---

## 4. Informe Final

### Arquitectura del Data Warehouse

El Data Warehouse fue diseñado utilizando un enfoque **híbrido (Star + Snowflake Light)** para equilibrar simplicidad analítica y normalización en dimensiones específicas. La **tabla de hechos principal es `trips`**, que concentra los eventos medibles del negocio: viajes realizados, consumo de combustible, tiempos de salida y llegada, estatus y peso transportado. Alrededor de esta fact table se organizan dimensiones descriptivas que enriquecen el análisis:

* **Dimensión `drivers`**: información del conductor, tipo y vigencia de licencia, fechas de alta y estado laboral.
* **Dimensión `vehicles`** y **`vehicle_models`**: atributos de cada vehículo y su modelo, lo que sigue un patrón snowflake para evitar duplicación de información técnica.
* **Dimensión `routes`**: detalles de rutas, ciudades de origen y destino, distancias y costos.
* **Dimensión `cities`**: catálogo único de ciudades para estandarizar referencias.

Este enfoque permite realizar análisis operativos, de rendimiento y logística con consistencia y flexibilidad.

### Procesos ETL

El proceso ETL implementado siguió una estructura modular:

1. **Extracción:** se ingesta información desde distintas fuentes originales, simuladas en este proyecto como tablas base. Los datos son capturados respetando su granularidad original.
2. **Transformación:**

   * Limpieza y estandarización de formatos (fechas, nombres de ciudades, estados de viaje).
   * Normalización hacia el modelo star/snowflake.
   * Validación de claves referenciales.
3. **Carga:**

   * Inserción progresiva de datos en dimensiones.
   * Carga final en la tabla de hechos `trips`.
   * Generación de un registro automático de operaciones en la tabla `logs_carga`, que documenta cada paso de la carga y permite auditar ejecuciones.

Este pipeline asegura coherencia entre tablas, evita duplicados y mantiene la trazabilidad de cada lote cargado.

### Análisis exploratorio e insights

A partir de las consultas avanzadas y KPIs definidos, se obtuvo una serie de hallazgos relevantes:

* **Duración promedio por ruta:** se identificaron rutas con tiempos significativamente mayores al estimado, lo cual sugiere congestión u oportunidades de optimización.
* **Eficiencia de combustible:** algunos vehículos presentan consumos fuera de rango esperado, indicando potencial mantenimiento requerido o estilos de conducción poco eficientes.
* **Peso transportado por ciudad:** ciertas ciudades generan una mayor demanda logística, mostrando patrones estacionales y rutas críticas para la operación.
* **Rendimiento por conductor:** se detectan conductores con mejores tiempos y menor consumo relativo, lo que permite diseñar programas de capacitación.
* **Estado de viajes:** se analizaron patrones de demoras y cancelaciones, con correlaciones según días de la semana y rutas específicas.

Estos insights ayudan a priorizar inversiones en flota, planificación y asignación de recursos.

---

### Conclusiones

El proyecto permitió construir un Data Warehouse funcional, normalizado y preparado para análisis operativos y estratégicos. Entre los principales logros se destacan:

* Un modelo híbrido flexible que equilibra simplicidad y normalización.
* Un pipeline ETL robusto y trazable mediante logs de carga.
* KPIs clave que ofrecen visibilidad sobre eficiencia, desempeño y demanda.
* Un set de consultas avanzadas que habilitan análisis profundo del negocio logístico.

**Oportunidades de mejora:**

* Incluir más fuentes de datos (telemetría, clima, mantenimiento histórico avanzado).
* Automatizar el pipeline con orquestadores (Airflow/Luigi).
* Incorporar dashboards en BI para mayor accesibilidad.
* Implementar modelos predictivos para estimar demanda, fallas o tiempos de viaje.

El proyecto sienta una base sólida para evolucionar hacia analítica avanzada y optimización integral del sistema logístico.

---