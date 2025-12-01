-- # Control de calidad y validaciones

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