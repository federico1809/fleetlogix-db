# Fleetlogix - Proyecto Integrador Data Science

Fleetlogix es una empresa de transporte y logística que opera una flota de 200 vehículos realizando entregas de última milla en cinco ciudades principales de Colombia.  
Este proyecto integra modelado relacional, generación de datos sintéticos masivos, validaciones de calidad, KPIs logísticos y una arquitectura cloud escalable.

---

## Objetivos
- Poblar una base de datos PostgreSQL con más de **500.000 registros sintéticos** generados con Python.
- Garantizar integridad referencial y reglas de negocio mediante claves primarias, foráneas e índices.
- Documentar el modelo relacional con un **diagrama ERD** y un **modelo dimensional tipo estrella** para OLAP.
- Implementar queries SQL para validaciones de calidad y KPIs operativos.
- Proponer una arquitectura cloud con Kafka, Flink/Spark, Data Lake y Power BI para análisis en tiempo real.

---

## Modelo Relacional (ERD)
El modelo relacional se compone de seis tablas:

- **Maestras:** `vehicles`, `drivers`, `routes`  
- **Transaccionales:** `trips`, `deliveries`, `maintenance`

Restricciones técnicas:
- Claves primarias y foráneas para integridad referencial.
- Restricciones de unicidad en placas, licencias y tracking numbers.
- Índices en campos críticos para optimizar queries.

![Diagrama ERD](assets/Diagrama_Fleetlogix.png)

---

## Generación de Datos Sintéticos
**Herramientas:** Python + Faker + pandas + numpy + psycopg2  

**Volumen generado:**
- 200 vehículos  
- 400 conductores  
- 50 rutas  
- 100.000 viajes  
- 400.001 entregas  
- 5.000 mantenimientos  

> Nota: Se generaron 400.001 entregas en lugar de 400.000 debido a la naturaleza probabilística del generador.  
> Cada viaje recibe entre 2 y 6 entregas según distribución aleatoria controlada, lo que produce pequeñas variaciones alrededor del valor esperado.

---

## Validaciones de Calidad
- Integridad referencial: todas las claves foráneas válidas.  
- Consistencia temporal: `arrival_datetime > departure_datetime`.  
- Reglas de negocio:  
  - Peso ≤ capacidad del vehículo  
  - Tracking numbers únicos  
  - Entregas por viaje entre 2 y 6  

Ejemplo de validación:
```sql
-- Trips sin vehículo válido (resultado esperado: 0)
SELECT COUNT(*) AS invalid_trips
FROM trips t
LEFT JOIN vehicles v ON v.vehicle_id = t.vehicle_id
WHERE v.vehicle_id IS NULL;
```
## KPIs Operativos
- Porcentaje de entregas a tiempo vs retrasadas
- Consumo promedio de combustible por tipo de vehículo
- Utilización de capacidad por viaje
- Mantenimientos por cada 1.000 km
- Promedio de entregas por viaje

Ejemplo:

```sql
-- Entregas a tiempo vs con retraso
WITH delivered AS (
    SELECT delivery_id,
           CASE WHEN delivered_datetime <= scheduled_datetime + INTERVAL '30 minutes'
                THEN 'on_time' ELSE 'late' END AS status
    FROM deliveries
)
SELECT status, COUNT(*) AS cnt,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM delivered
WHERE status IS NOT NULL
GROUP BY status;
```
Resultado: 90.15% entregas a tiempo, 9.85% con retraso.

## Propuesta de Arquitectura en la Nube
- Ingesta: PostgreSQL + CDC (Debezium)
- Streaming: Apache Kafka
- Procesamiento: Apache Flink / Spark Streaming
- Almacenamiento: Data Lake + Data Warehouse
- Visualización: Power BI / Looker
- Gobernanza: Data Catalog + Lineage

La arquitectura cloud funciona como un flujo continuo que integra captura, transmisión, procesamiento, almacenamiento y visualización de datos. Los cambios en PostgreSQL se detectan en tiempo real mediante CDC y se envían a través de Kafka. Flink o Spark Streaming procesan los eventos de manera paralela, los resultados se guardan en un Data Lake y en un Data Warehouse, y finalmente dashboards interactivos permiten decisiones rápidas con gobernanza que asegura calidad y trazabilidad.


## Ejecución
1. Abrir PowerShell y ubicarse en la carpeta de scripts:

```powershell
cd "RUTA\LOCAL\DE\LA\CARPETA"
.\run_fleetlogix.ps1
```
2. Crear conexión en DBeaver:
    - Host: localhost
    - Port: 5432
    - Database: fleetlogixdb
    - Username: postgres
    - Password: definida en cada script de acuerdo a cada configuración local (o al servidor)

3. Ejecutar queries de validación y KPIs:
    - 01_InventarioDeTablasPKsFKsIndices.sql
    - 02_ValidacionesCalidadConsistencia.sql
    - 03_KPIsOperativosConsultasClave.sql


## Elaborado por Federico Ceballos Torres
### Avance 1 del Módulo 2
### Proyecto integrador
### Carrera Data Science
## SoyHenry