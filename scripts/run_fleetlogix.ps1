# --- Crear la base de datos (si no existe) ---
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -c "CREATE DATABASE fleetlogixdb;"

# --- Aplicar el esquema SQL en la base fleetlogix22 ---
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d fleetlogixdb -f "C:\Users\feder\Documents\Data_Science\m02\fleetlogix_pi\scripts\fleetlogix_db_schema.sql"

# --- Ejecutar el script Python para generar y cargar datos ---
python "C:\Users\feder\Documents\Data_Science\m02\fleetlogix_pi\scripts\fleetlogix_data_generator.py"