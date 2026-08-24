import json
import time
import uuid

import pandas as pd
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "taxi_trips"
FILE_PATH = "data/yellow_tripdata_2026-01.parquet"

BATCH_SIZE = 100
EVENT_DELAY = 0.1
MAX_RECORDS = 1_000


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def create_event(row):
    return {
        "event_id": str(uuid.uuid4()),
        "pickup_datetime": row["tpep_pickup_datetime"].isoformat(),
        "dropoff_datetime": row["tpep_dropoff_datetime"].isoformat(),
        "passenger_count": row["passenger_count"],
        "trip_distance": row["trip_distance"],
        "pickup_location_id": row["PULocationID"],
        "dropoff_location_id": row["DOLocationID"],
        "payment_type": row["payment_type"],
        "fare_amount": row["fare_amount"],
        "tip_amount": row["tip_amount"],
        "total_amount": row["total_amount"]
    }


df = pd.read_parquet(FILE_PATH)
df = df.head(MAX_RECORDS)

print(f"Total records: {len(df):,}")

for start in range(0, len(df), BATCH_SIZE):

    batch = df.iloc[start:start + BATCH_SIZE]

    print(
        f"Processing rows "
        f"{start:,} - {start + len(batch) - 1:,}"
    )

    for _, row in batch.iterrows():

        event = create_event(row)

        producer.send(
            TOPIC_NAME,
            value=event
        )

        print(f"Sent: {event['event_id']}")
        time.sleep(EVENT_DELAY)

    producer.flush()
producer.close()
print("Streaming completed.")