-- SUBQUERIES
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
