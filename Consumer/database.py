import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "nyc_taxi",
    "user": "nyc_user",
    "password": "nyc_password",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS taxi_trips (
            event_id UUID PRIMARY KEY,
            pickup_datetime TIMESTAMP NOT NULL,
            dropoff_datetime TIMESTAMP NOT NULL,
            passenger_count INTEGER,
            trip_distance FLOAT,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            payment_type INTEGER,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            trip_duration_minutes FLOAT,
            fare_per_mile FLOAT
        );
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("Table taxi_trips is ready.")


def insert_event(event):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO taxi_trips (
            event_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_location_id,
            dropoff_location_id,
            payment_type,
            fare_amount,
            tip_amount,
            total_amount,
            trip_duration_minutes,
            fare_per_mile
        )
        VALUES (
            %(event_id)s,
            %(pickup_datetime)s,
            %(dropoff_datetime)s,
            %(passenger_count)s,
            %(trip_distance)s,
            %(pickup_location_id)s,
            %(dropoff_location_id)s,
            %(payment_type)s,
            %(fare_amount)s,
            %(tip_amount)s,
            %(total_amount)s,
            %(trip_duration_minutes)s,
            %(fare_per_mile)s
        )
        ON CONFLICT (event_id) DO NOTHING;
    """, event)

    conn.commit()

    cursor.close()
    conn.close()