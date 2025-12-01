# PI - 1er avance - FleetLogix

## Síntesis Ejecutiva

### Contexto
FleetLogix es una empresa de logística con operaciones en 5 ciudades de Colombia y una flota de 200 vehículos. El proyecto integrador tuvo como objetivo modernizar la gestión de datos, migrando de sistemas legacy a una base de datos PostgreSQL robusta, poblarla con datos sintéticos y diseñar una arquitectura cloud para análisis en tiempo real.

### Modelo Relacional
Se diseñó un modelo con 6 tablas principales: vehicles, drivers, routes, trips, deliveries, maintenance, garantizando integridad referencial y consistencia temporal. El modelo permite relacionar viajes con vehículos, conductores y rutas, así como vincular entregas y mantenimientos.

### Generación de Datos
Se generaron más de 505,000 registros sintéticos con Python y Faker:
- 100k viajes distribuidos en 2 años.
- 400k entregas con tracking y firma.
- 5k mantenimientos por vehículo. Los datos cumplen reglas de negocio: peso ≤ capacidad, arrival > departure, tracking único.

### Queries Operativas
Se desarrollaron consultas SQL para responder problemas clave:
- KPIs de viajes: volumen por ciudad y día, consumo de combustible.
- Puntualidad: retraso promedio por ciudad.
- Conductores: ranking por desempeño y licencias próximas a vencer.
- Mantenimiento: frecuencia y costos por tipo.

### Resultados Clave
##### Retrasos promedio detectados en ciertas ciudades → oportunidad de optimizar rutas.
##### Top 10 vehículos concentran gran parte de los viajes → riesgo de sobreuso.
##### Conductores con licencias próximas a vencer → necesidad de gestión preventiva.
##### Tipos de mantenimiento más frecuentes → foco en reducción de costos.

### Recomendaciones de Negocio
Optimizar rutas en ciudades con mayores retrasos para mejorar puntualidad.
Redistribuir viajes para evitar sobrecarga en vehículos más usados.
Implementar alertas para renovación de licencias de conductores.
Adoptar mantenimiento predictivo para reducir costos y mejorar disponibilidad.
Monitorear KPIs en dashboards para decisiones operativas en tiempo real.

### Arquitectura Cloud
Se diseñó una arquitectura conceptual con PostgreSQL como base transaccional, Airflow/dbt para ETL, un Data Warehouse (Redshift/BigQuery/Snowflake) para análisis histórico, Kafka/Kinesis para streaming en tiempo real y Power BI/Tableau/Looker para visualización. Esta arquitectura garantiza escalabilidad, integridad y capacidad de respuesta inmediata.

### Conclusión
FleetLogix ahora cuenta con una base sólida para análisis de datos y modernización tecnológica. El proyecto integrador demuestra cómo una infraestructura bien diseñada puede transformar operaciones logísticas, habilitando decisiones basadas en datos y mejorando la eficiencia en tiempo real.