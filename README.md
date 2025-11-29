🚛 FleetLogix: Modernización de Infraestructura de Datos📋 Descripción del ProyectoFleetLogix es una empresa de transporte y logística que opera una flota de 200 vehículos en 5 ciudades principales. Este proyecto integrador tiene como objetivo la migración de sistemas legacy a una arquitectura de datos moderna y robusta.Como parte del equipo de Data Science, la misión es diseñar, poblar y validar una base de datos relacional capaz de soportar análisis operativos y toma de decisiones en tiempo real.🎯 Objetivos del Avance #1Modelado de Datos: Diseño de un esquema relacional (Estrella/Copo de Nieve) eficiente.Generación de Datos Sintéticos: Creación de un dataset masivo (+505,000 registros) que simule 2 años de operación histórica con coherencia de negocio.Implementación ETL: Desarrollo de scripts en Python para la ingesta de datos.Calidad de Datos: Validación de integridad referencial y consistencia temporal.🗄️ Modelo de DatosEl esquema se compone de 6 tablas interconectadas. A continuación se presenta el Diagrama Entidad-Relación (ERD):erDiagram
    VEHICLES ||--o{ TRIPS : "realiza"
    VEHICLES ||--o{ MAINTENANCE : "recibe"
    DRIVERS ||--o{ TRIPS : "conduce"
    ROUTES ||--o{ TRIPS : "define"
    TRIPS ||--o{ DELIVERIES : "contiene"

    VEHICLES {
        int vehicle_id PK
        string license_plate
        string status
    }
    DRIVERS {
        int driver_id PK
        string license_number
        string employee_code
    }
    ROUTES {
        int route_id PK
        string origin_city
        string destination_city
    }
    TRIPS {
        int trip_id PK
        int vehicle_id FK
        int driver_id FK
        timestamp departure_time
        timestamp arrival_time
    }
    DELIVERIES {
        int delivery_id PK
        int trip_id FK
        string status
    }
    MAINTENANCE {
        int maintenance_id PK
        int vehicle_id FK
        date date
        decimal cost
    }

Diccionario de Datos Simplificado| Tabla | Tipo | Descripción | Relaciones || vehicles | Maestro | Flota de camiones, vans y motos. | 1:N con Trips y Maintenance. || drivers | Maestro | Información de conductores y licencias. | 1:N con Trips. || routes | Maestro | Rutas logísticas predefinidas. | 1:N con Trips. || trips | Transaccional | Registro histórico de viajes. | Tabla central de hechos. || deliveries | Transaccional | Detalle de entregas por viaje. | Depende de Trips. || maintenance | Transaccional | Historial de reparaciones y costos. | Depende de Vehicles. |⚙️ Flujo de Carga de Datos (ETL)Para poblar la base de datos se utilizó una estrategia híbrida utilizando Python (Faker, Pandas, Psycopg2) y SQL.1. Ingesta Directa (Script 02_data_generation_estudiantes.py)Este script es el motor principal de la generación de datos.Conexión: Se conecta directamente a PostgreSQL usando la librería psycopg2.Lógica: Genera datos en memoria y los inserta en lotes (execute_batch) para optimizar el rendimiento.Alcance: Pobló exitosamente las tablas vehicles, drivers, routes, trips y deliveries.Validaciones: Asegura que arrival_time > departure_time y que no existan IDs huérfanos.2. Generación de Archivos Planos (Script Generate_CSV.py)Script auxiliar diseñado para exportar la data generada a formato físico (.csv).Uso: Se utilizó para regenerar los datos de la tabla maintenance y tener respaldos físicos de las tablas maestras.Salida: Genera archivos en la carpeta /data o /csv.3. Carga Manual (DBeaver)La tabla maintenance se cargó importando el archivo maintenance.csv (generado en el paso anterior) directamente a través de la herramienta de importación de DBeaver, permitiendo un control granular sobre el mapeo de columnas y tipos de datos.✅ Control de Calidad y ValidacionesSe implementaron reglas de negocio estrictas para asegurar el realismo de los datos sintéticos:Integridad Referencial: Todos los trip_id en entregas existen en la tabla de viajes. Todos los conductores asignados existen en la tabla drivers.Consistencia Temporal:Los viajes duran una cantidad de horas coherente con la distancia de la ruta.Las fechas de entrega están comprendidas dentro del rango de tiempo del viaje.Los mantenimientos ocurren dentro de la vida útil del vehículo.Distribución de Datos:~4 entregas promedio por viaje.Mantenimientos periódicos cada ~20 viajes.☁️ Arquitectura Conceptual (Próximos Pasos)El proyecto evoluciona hacia una arquitectura Cloud para permitir analítica en tiempo real:Ingesta: PostgreSQL (Operacional).Orquestación: Apache Airflow para los pipelines de ETL.Data Warehouse: Snowflake o Redshift para almacenamiento analítico.Visualización: PowerBI / Tableau conectados al DW.Instrucciones de EjecuciónConfigurar Base de Datos:Crear DB fleetlogix en PostgreSQL.Ejecutar el script DDL inicial para crear tablas vacías.Carga Principal:Configurar credenciales en 02_data_generation_estudiantes.py.Ejecutar: python 02_data_generation_estudiantes.py.Carga Complementaria (Maintenance):Ejecutar Generate_CSV.py para obtener maintenance.csv.Importar el CSV a la tabla maintenance usando DBeaver o el comando COPY de SQL.Proyecto desarrollado para el Módulo 2 de Data Science - Henry.
