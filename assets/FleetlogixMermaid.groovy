erDiagram
    fact_deliveries ||--o{ dim_vehicle : "utiliza"
    fact_deliveries ||--o{ dim_driver : "realizado por"
    fact_deliveries ||--o{ dim_route : "recorre"
    fact_deliveries ||--o{ dim_time : "programada en"

    dim_vehicle {
        int vehicle_id PK
        string license_plate UNIQUE
        string vehicle_type
        int capacity_kg
        string status
    }

    dim_driver {
        int driver_id PK
        string first_name
        string last_name
        string license_number UNIQUE
        date license_expiry
        string employment_status
    }

    dim_route {
        int route_id PK
        string route_code UNIQUE
        string origin_city
        string destination_city
        int distance_km
        float duration_hours
    }

    dim_time {
        date scheduled_date PK
        int year
        int month
        int day
        string weekday
        string period_of_day
    }

    fact_deliveries {
        int delivery_id PK
        int vehicle_id FK
        int driver_id FK
        int route_id FK
        date scheduled_date FK
        string tracking_number UNIQUE
        datetime scheduled_datetime
        datetime delivered_datetime
        string delivery_status
        float package_weight_kg
    }
