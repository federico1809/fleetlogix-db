-- ## Integridad referencial

-- ### Trips sin vehículo válido (debería ser 0):
SELECT COUNT(*) AS invalid_trips
FROM trips t
LEFT JOIN vehicles v ON v.vehicle_id = t.vehicle_id
WHERE v.vehicle_id IS NULL;

-- ### Deliveries sin trip válido (debería ser 0):
SELECT COUNT(*) AS invalid_deliveries
FROM deliveries d
LEFT JOIN trips t ON t.trip_id = d.trip_id
WHERE t.trip_id IS NULL;


-- ## Consistencia temporal

-- ### Arribos antes de partida (debería ser 0):
SELECT COUNT(*) AS bad_times
FROM trips
WHERE arrival_datetime IS NOT NULL
  AND arrival_datetime < departure_datetime;

-- ### Entregas con entrega real antes del programado (tolerancia ±30 min ya contemplada):
SELECT COUNT(*) AS early_delivery_outliers
FROM deliveries
WHERE delivered_datetime IS NOT NULL
  AND delivered_datetime < scheduled_datetime - INTERVAL '45 minutes';


-- ## Reglas de negocio

-- ### Peso total del viaje ≤ capacidad del vehículo (debería ser 0):
SELECT COUNT(*) AS overweight_trips
FROM trips t
JOIN vehicles v ON v.vehicle_id = t.vehicle_id
WHERE t.total_weight_kg > v.capacity_kg;

-- ### Tracking numbers únicos (debería ser 0 duplicados):
SELECT COUNT(*) AS duplicates
FROM (
  SELECT tracking_number
  FROM deliveries
  GROUP BY tracking_number
  HAVING COUNT(*) > 1
) s;

-- ### Entregas por viaje dentro de rango [2..6]: (resultado arroja 4.0020110055)
SELECT MIN(c) AS min_del, MAX(c) AS max_del, AVG(c) AS avg_del
FROM (
  SELECT trip_id, COUNT(*) AS c
  FROM deliveries
  GROUP BY trip_id
) x;