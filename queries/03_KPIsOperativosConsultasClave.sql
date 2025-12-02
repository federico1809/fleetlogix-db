-- Entregas a tiempo vs con retraso
WITH delivered AS (
  SELECT
    delivery_id,
    CASE
      WHEN delivered_datetime IS NULL THEN NULL
      WHEN delivered_datetime <= scheduled_datetime + INTERVAL '30 minutes' THEN 'on_time'
      ELSE 'late'
    END AS status
  FROM deliveries
)
SELECT status, COUNT(*) AS cnt, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM delivered
WHERE status IS NOT NULL
GROUP BY status;

-- Consumo de combustible por 100 km y tipo de vehículo
SELECT
  v.vehicle_type,
  ROUND(100.0 * SUM(t.fuel_consumed_liters) / NULLIF(SUM(r.distance_km),0), 2) AS liters_per_100km
FROM trips t
JOIN vehicles v ON v.vehicle_id = t.vehicle_id
JOIN routes r ON r.route_id = t.route_id
GROUP BY v.vehicle_type
ORDER BY liters_per_100km DESC;

-- Utilización de capacidad (promedio y distribución)
SELECT
  v.vehicle_type,
  ROUND(AVG(t.total_weight_kg / v.capacity_kg), 3) AS avg_utilization
FROM trips t
JOIN vehicles v ON v.vehicle_id = t.vehicle_id
GROUP BY v.vehicle_type
ORDER BY avg_utilization DESC;

-- Viajes por conductor (top 10)
SELECT d.driver_id, d.first_name || ' ' || d.last_name AS driver, COUNT(*) AS trips_count
FROM trips t
JOIN drivers d ON d.driver_id = t.driver_id
GROUP BY d.driver_id, driver
ORDER BY trips_count DESC
LIMIT 10;

-- Promedio de entregas por viaje
SELECT 
    ROUND(CAST(COUNT(*) AS numeric) / CAST((SELECT COUNT(*) FROM trips) AS numeric), 2) 
    AS promedio_entregas_por_viaje
FROM deliveries;

