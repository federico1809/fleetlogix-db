#!/usr/bin/env python3
"""
fleetlogix_data_generator.py

- Genera CSVs coherentes con fleetlogix_schema.sql
- Opcional: crea esquema en DB y/o inserta los datos directamente.
"""

import os
import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

# -------------------------
# CONFIG
# -------------------------
OUTPUT_FOLDER = r"C:\Users\feder\Documents\Data Science\m02\fleetlogix_pi\data_output"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Database config: EDITÁ esto con tus credenciales antes de usar INSERT_INTO_DB
DB_CONFIG = {
    "host": "localhost",
    "database": "fleetlogix",
    "user": "fleetlogixub",
    "password": "fede0309",
    "port": 5433
}

# Comportamiento
CREATE_SCHEMA = True        # si True, ejecuta el SQL de schema (ver instrucciones)
INSERT_INTO_DB = True      # si True, conectará y hará inserts (usa execute_batch)
GENERATE_CSV_ONLY = True    # si True (default), solo genera CSVs; si False y INSERT_INTO_DB True, hará insert

# Tamaños solicitados
N_VEHICLES = 200
N_DRIVERS = 400
N_ROUTES = 50
N_TRIPS = 100_000
N_DELIVERIES = 400_000
N_MAINTENANCE = 5_000

seed = 42
random.seed(seed)
np.random.seed(seed)
fake = Faker('es_CO')
Faker = Faker  # alias if used

# -------------------------
# HELPERS
# -------------------------
def hourly_distribution():
    probs = np.ones(24) * 0.02
    probs[6:20] = 0.06
    probs[8:12] = 0.08
    probs[14:18] = 0.07
    return probs / probs.sum()

# -------------------------
# CSV WRITERS (streaming for big tables)
# -------------------------
def write_vehicles_csv(df, path):
    df.to_csv(path, index=False, encoding='utf-8')

def write_drivers_csv(df, path):
    df.to_csv(path, index=False, encoding='utf-8')

def write_routes_csv(df, path):
    df.to_csv(path, index=False, encoding='utf-8')

# -------------------------
# GENERATORS
# -------------------------
def gen_vehicles(n=N_VEHICLES):
    models = [
        ("Camión Grande", 5000, "diesel"),
        ("Camión Mediano", 3000, "diesel"),
        ("Van", 1500, "gasolina"),
        ("Motocicleta", 50, "gasolina")
    ]
    rows = []
    for i in range(n):
        model_name, capacity, fuel = random.choices(models, weights=[0.3,0.3,0.3,0.1])[0]
        plate = ''.join([fake.random_uppercase_letter() for _ in range(3)]) + str(random.randint(100,999))
        acquisition_date = fake.date_between(start_date='-5y', end_date='-1m')
        status = random.choice(['active']*9 + ['maintenance'])
        rows.append({
            "vehicle_id": i+1,
            "license_plate": plate,
            "model_name": model_name,
            "capacity_kg": capacity,
            "fuel_type": fuel,
            "acquisition_date": acquisition_date,
            "status": status
        })
    df = pd.DataFrame(rows)
    return df

def gen_drivers(n=N_DRIVERS):
    rows = []
    license_types = ['C1','C2','C3','A2']
    for i in range(n):
        rows.append({
            "driver_id": i+1,
            "employee_code": f"EMP{str(i+1).zfill(4)}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "license_number": str(random.randint(10**9, 10**10-1)),
            "license_type": random.choice(license_types),
            "license_expiry": fake.date_between(start_date='-1m', end_date='+3y'),
            "phone": f"3{random.randint(100000000,999999999)}",
            "hire_date": fake.date_between(start_date='-5y', end_date='-1w'),
            "status": random.choice(['active']*19 + ['inactive'])
        })
    df = pd.DataFrame(rows)
    return df

def gen_routes(n=N_ROUTES, cities=None):
    if cities is None:
        cities = ['Bogotá','Medellín','Cali','Barranquilla','Cartagena']
    rows = []
    counter = 1
    def get_distance(o,d):
        table = {
            ('Bogotá','Medellín'):440,('Bogotá','Cali'):460,('Bogotá','Barranquilla'):1000,('Bogotá','Cartagena'):1050,
            ('Medellín','Cali'):420,('Medellín','Barranquilla'):640,('Medellín','Cartagena'):640,
            ('Cali','Barranquilla'):1100,('Cali','Cartagena'):1100,('Barranquilla','Cartagena'):120
        }
        key = tuple(sorted([o,d]))
        return table.get(key, 500)
    for origin in cities:
        for destination in cities:
            if origin == destination:
                continue
            num_routes = 3 if origin == 'Bogotá' or destination == 'Bogotá' else 2
            for _ in range(num_routes):
                if counter > n:
                    break
                base = get_distance(origin, destination)
                distance = base + random.uniform(-50,50)
                speed = random.uniform(60,80)
                duration = distance / speed
                toll = int(distance // 100) * 15000
                rows.append({
                    "route_id": counter,
                    "route_code": f"R{str(counter).zfill(3)}",
                    "origin_city": origin,
                    "destination_city": destination,
                    "distance_km": round(distance,2),
                    "estimated_duration_hours": round(duration,2),
                    "toll_cost": toll
                })
                counter += 1
            if counter > n:
                break
        if counter > n:
            break
    df = pd.DataFrame(rows[:n])
    return df

def gen_trips_df(vehicles_df, drivers_df, routes_df, n=N_TRIPS):
    # sample with replacement to produce n rows
    start = datetime.now() - timedelta(days=730)
    rows = []
    current_time = start
    hour_probs = hourly_distribution()
    active_drivers = drivers_df[drivers_df.status == 'active']
    for i in tqdm(range(n), desc="Generando trips"):
        v = vehicles_df.sample(1).iloc[0]
        d = active_drivers.sample(1).iloc[0]
        r = routes_df.sample(1).iloc[0]
        departure_hour = int(np.random.choice(range(24), p=hour_probs))
        departure = current_time.replace(hour=departure_hour, minute=random.randint(0,59), second=random.randint(0,59), microsecond=0)
        est_duration = float(r['estimated_duration_hours'])
        actual_duration = est_duration * random.uniform(0.8,1.3)
        arrival = departure + timedelta(hours=actual_duration)
        fuel = float(r['distance_km']) * random.uniform(0.08, 0.15)
        weight = float(v['capacity_kg']) * random.uniform(0.4, 0.9)
        status = 'completed' if arrival < datetime.now() else 'in_progress'
        rows.append({
            "trip_id": i+1,
            "vehicle_id": int(v['vehicle_id']),
            "driver_id": int(d['driver_id']),
            "route_id": int(r['route_id']),
            "departure_datetime": departure,
            "arrival_datetime": arrival if status=='completed' else None,
            "fuel_consumed_liters": round(fuel,2),
            "total_weight_kg": round(weight,2),
            "status": status
        })
        # advance time to spread trips
        current_time += timedelta(minutes=max(1,int(1440 * 2 * 365 / n)))
    df = pd.DataFrame(rows)
    return df

def stream_generate_deliveries(trips_df, out_csv_path, target_count=N_DELIVERIES):
    header = ["delivery_id","trip_id","tracking_number","customer_name","delivery_address",
              "package_weight_kg","scheduled_datetime","delivered_datetime","delivery_status","recipient_signature"]
    writer_file = open(out_csv_path, "w", newline='', encoding='utf-8')
    writer = csv.writer(writer_file)
    writer.writerow(header)
    counter = 1
    for _, row in tqdm(trips_df.iterrows(), total=len(trips_df), desc="Generando deliveries"):
        if counter > target_count:
            break
        trip_id = int(row['trip_id'])
        departure = row['departure_datetime']
        arrival = row['arrival_datetime']
        total_weight = float(row['total_weight_kg'])
        num = np.random.choice([2,3,4,5,6], p=[0.1,0.2,0.4,0.2,0.1])
        weights = np.random.exponential(scale=1.0, size=num)
        weights = weights / weights.sum() * total_weight * 0.95
        weights = np.maximum(weights, 0.5)
        if pd.notnull(arrival):
            duration_hours = (arrival - departure).total_seconds()/3600.0
            time_per = duration_hours / num if duration_hours>0 else 0.5
        else:
            time_per = 0.5
        for i in range(num):
            if counter > target_count:
                break
            scheduled = departure + timedelta(hours=time_per * (i + 0.5))
            if pd.notnull(arrival):
                if random.random() < 0.9:
                    delivered = scheduled + timedelta(minutes=random.randint(-30,30))
                else:
                    delivered = scheduled + timedelta(minutes=random.randint(60,180))
                status = "delivered"
                signature = random.random() < 0.95
            else:
                delivered = ""
                status = "pending"
                signature = False
            tracking = f"FL{datetime.now().year}{str(counter).zfill(8)}"
            customer_name = fake.name()
            delivery_address = f"{fake.street_address()}, {row.get('destination_city','')}"
            writer.writerow([
                counter, trip_id, tracking, customer_name, delivery_address,
                round(float(weights[i]),2), scheduled.isoformat(sep=' '),
                delivered.isoformat(sep=' ') if delivered!="" else "",
                status, bool(signature)
            ])
            counter += 1
    writer_file.close()

def gen_maintenance_stream(trips_df, vehicles_df, out_csv_path, target_count=N_MAINTENANCE):
    header = ["maintenance_id","vehicle_id","maintenance_date","maintenance_type","description","cost","next_maintenance_date","performed_by"]
    f = open(out_csv_path, "w", newline='', encoding='utf-8')
    writer = csv.writer(f)
    writer.writerow(header)
    maintenance_types = [
        ('Cambio de aceite', 150000, 30),
        ('Revisión de frenos', 250000, 60),
        ('Cambio de llantas', 450000, 90),
        ('Mantenimiento general', 350000, 45),
        ('Revisión de motor', 500000, 60),
        ('Alineación y balanceo', 180000, 30)
    ]
    # compute vehicle stats from trips
    grouped = trips_df.groupby("vehicle_id").agg(
        first_trip=("departure_datetime","min"),
        last_trip=("arrival_datetime","max"),
        trip_count=("trip_id","count")
    ).reset_index()
    counter = 1
    for _, row in grouped.iterrows():
        if counter > target_count:
            break
        num = max(1, int(row['trip_count']//20))
        first_trip = row['first_trip']
        last_trip = row['last_trip'] if pd.notnull(row['last_trip']) else row['first_trip']
        if pd.isnull(first_trip) or pd.isnull(last_trip):
            continue
        total_days = (last_trip - first_trip).days if (last_trip-first_trip).days>0 else 90
        for i in range(num):
            if counter > target_count:
                break
            offset = int(total_days * (i+1) / (num+1))
            maint_date = (first_trip + timedelta(days=offset)).date()
            mtype, base_cost, next_days = random.choice(maintenance_types)
            cost = round(base_cost * random.uniform(0.8,1.2),2)
            description = f"{mtype} programado para {maint_date.strftime('%Y-%m-%d')}"
            next_maint = maint_date + timedelta(days=next_days)
            performer = f"{fake.first_name()} {fake.last_name()}"
            writer.writerow([counter, int(row['vehicle_id']), maint_date.isoformat(), mtype, description, cost, next_maint.isoformat(), performer])
            counter += 1
    f.close()

# -------------------------
# DB helpers
# -------------------------
def run_sql_file(conn, path):
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def insert_df_to_db(conn, df, table_name, columns=None, page_size=1000):
    cur = conn.cursor()
    if columns is None:
        columns = list(df.columns)
    cols = ",".join(columns)
    placeholders = ",".join(["%s"]*len(columns))
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    values = df[columns].values.tolist()
    execute_batch(cur, query, values, page_size=page_size)
    conn.commit()
    cur.close()

# -------------------------
# MAIN
# -------------------------
def main():
    print("FleetLogix data generator - start")
    # 1) generate master CSVs
    vehicles_df = gen_vehicles()
    # extract unique models to vehicle_models table CSV
    models_df = vehicles_df[['model_name','capacity_kg','fuel_type']].drop_duplicates().reset_index(drop=True)
    models_df['model_id'] = models_df.index + 1
    # map model_id into vehicles_df
    model_map = {r['model_name']: r['model_id'] for _, r in models_df.iterrows()}
    vehicles_df['model_id'] = vehicles_df['model_name'].map(model_map)
    vehicles_out = vehicles_df[['vehicle_id','license_plate','model_id','acquisition_date','status','capacity_kg','fuel_type','model_name']]
    # we write vehicles CSV with vehicle_id and model_id (model_id aligns with vehicle_models.csv)
    write_vehicles_csv(vehicles_out[['vehicle_id','license_plate','model_id','acquisition_date','status']], os.path.join(OUTPUT_FOLDER,"vehicles.csv"))
    models_out = models_df[['model_id','model_name','capacity_kg','fuel_type']]
    models_out.to_csv(os.path.join(OUTPUT_FOLDER,"vehicle_models.csv"), index=False, encoding='utf-8')

    drivers_df = gen_drivers()
    write_drivers_csv(drivers_df, os.path.join(OUTPUT_FOLDER,"drivers.csv"))

    routes_df = gen_routes()
    routes_df.to_csv(os.path.join(OUTPUT_FOLDER,"routes.csv"), index=False, encoding='utf-8')

    print("Master CSVs written:", OUTPUT_FOLDER)

    # 2) generate trips (in memory)
    trips_df = gen_trips_df(vehicles_out, drivers_df, routes_df)
    trips_df.to_csv(os.path.join(OUTPUT_FOLDER,"trips.csv"), index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

    # 3) stream deliveries (~400k)
    stream_generate_deliveries(trips_df, os.path.join(OUTPUT_FOLDER,"deliveries.csv"), target_count=N_DELIVERIES)

    # 4) maintenance
    gen_maintenance_stream(trips_df, vehicles_out, os.path.join(OUTPUT_FOLDER,"maintenance.csv"), target_count=N_MAINTENANCE)

    print("Large CSVs written (trips, deliveries, maintenance).")

    # Optional DB ops
    if INSERT_INTO_DB or CREATE_SCHEMA:
        print("Connecting to DB...")
        conn = psycopg2.connect(**DB_CONFIG)
        if CREATE_SCHEMA:
            sql_path = os.path.join(os.path.dirname(__file__), "fleetlogix_schema.sql")
            if os.path.exists(sql_path):
                print("Creating schema from fleetlogix_schema.sql")
                run_sql_file(conn, sql_path)
            else:
                print("fleetlogix_schema.sql not found in script folder. Skipping schema creation.")
        if INSERT_INTO_DB:
            print("Inserting master data into DB (vehicles -> vehicle_models -> drivers -> routes -> trips -> deliveries -> maintenance)")
            # Insert vehicle_models first
            insert_df_to_db(conn, models_out.rename(columns={'model_id':'model_id','model_name':'model_name'}),'vehicle_models',columns=['model_id','model_name','capacity_kg','fuel_type'])
            # Vehicles
            insert_df_to_db(conn, vehicles_out[['vehicle_id','license_plate','model_id','acquisition_date','status']],'vehicles',columns=['vehicle_id','license_plate','model_id','acquisition_date','status'])
            # Drivers
            insert_df_to_db(conn, drivers_df,'drivers',columns=['driver_id','employee_code','first_name','last_name','license_number','license_type','license_expiry','phone','hire_date','status'])
            # Routes: need to map origin/destination city names into cities table first
            # Create cities table entries if needed
            cur = conn.cursor()
            city_names = pd.concat([routes_df['origin_city'], routes_df['destination_city']]).unique()
            for c in city_names:
                cur.execute("INSERT INTO cities (city_name) VALUES (%s) ON CONFLICT (city_name) DO NOTHING", (c,))
            conn.commit()
            # fetch city ids
            cur.execute("SELECT city_id, city_name FROM cities")
            city_map = {r[1]: r[0] for r in cur.fetchall()}
            # prepare routes with mapped city ids
            routes_db = routes_df.copy()
            routes_db['origin_city_id'] = routes_db['origin_city'].map(city_map)
            routes_db['destination_city_id'] = routes_db['destination_city'].map(city_map)
            insert_df_to_db(conn, routes_db[['route_id','route_code','origin_city_id','destination_city_id','distance_km','estimated_duration_hours','toll_cost']],'routes',
                            columns=['route_id','route_code','origin_city_id','destination_city_id','distance_km','estimated_duration_hours','toll_cost'])
            # trips
            # convert datetimes to naive timestamps for DB insert
            trips_db = trips_df.copy()
            insert_df_to_db(conn, trips_db[['trip_id','vehicle_id','driver_id','route_id','departure_datetime','arrival_datetime','fuel_consumed_liters','total_weight_kg','status']],'trips',
                            columns=['trip_id','vehicle_id','driver_id','route_id','departure_datetime','arrival_datetime','fuel_consumed_liters','total_weight_kg','status'])
            # deliveries & maintenance: for large tables use COPY would be ideal; here we'll use execute_batch in chunks reading CSVs
            # deliveries
            with open(os.path.join(OUTPUT_FOLDER,"deliveries.csv"), 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = []
                cur = conn.cursor()
                batch = []
                for r in reader:
                    batch.append((int(r['delivery_id']), int(r['trip_id']), r['tracking_number'], r['customer_name'], r['delivery_address'],
                                  float(r['package_weight_kg']) if r['package_weight_kg']!='' else None,
                                  r['scheduled_datetime'] if r['scheduled_datetime']!='' else None,
                                  r['delivered_datetime'] if r['delivered_datetime']!='' else None,
                                  r['delivery_status'], r['recipient_signature'] in ['True','true','1']))
                    if len(batch) >= 1000:
                        execute_batch(cur, """
                            INSERT INTO deliveries (delivery_id, trip_id, tracking_number, customer_name, delivery_address,
                            package_weight_kg, scheduled_datetime, delivered_datetime, delivery_status, recipient_signature)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, batch, page_size=500)
                        conn.commit()
                        batch = []
                if batch:
                    execute_batch(cur, """
                        INSERT INTO deliveries (delivery_id, trip_id, tracking_number, customer_name, delivery_address,
                        package_weight_kg, scheduled_datetime, delivered_datetime, delivery_status, recipient_signature)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, batch, page_size=500)
                    conn.commit()
            # maintenance
            with open(os.path.join(OUTPUT_FOLDER,"maintenance.csv"), 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                cur = conn.cursor()
                batch = []
                for r in reader:
                    batch.append((int(r['maintenance_id']), int(r['vehicle_id']), r['maintenance_date'], r['maintenance_type'], r['description'],
                                  float(r['cost']) if r['cost']!='' else None, r['next_maintenance_date'] if r['next_maintenance_date']!='' else None, r['performed_by']))
                    if len(batch) >= 1000:
                        execute_batch(cur, """
                            INSERT INTO maintenance (maintenance_id, vehicle_id, maintenance_date, maintenance_type, description, cost, next_maintenance_date, performed_by)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """, batch, page_size=500)
                        conn.commit()
                        batch = []
                if batch:
                    execute_batch(cur, """
                        INSERT INTO maintenance (maintenance_id, vehicle_id, maintenance_date, maintenance_type, description, cost, next_maintenance_date, performed_by)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, batch, page_size=500)
                    conn.commit()
            cur.close()
        conn.close()

    print("Done. CSVs in:", OUTPUT_FOLDER)
    if INSERT_INTO_DB:
        print("Data inserted into DB.")

if __name__ == "__main__":
    main()