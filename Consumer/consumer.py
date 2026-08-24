import json
import pandas as pd
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "taxi_trips"
GROUP_ID = "taxi-transformation-test"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

def validation(event):
    """
    Validate NYC Taxi Event
    """
    # 1. event_id
    if not event.get("event_id"):
        return False, "Missing event_id"

    # 2. passenger_count
    if event.get("passenger_count") is None:
        return False, "Missing passenger_count"
    if event["passenger_count"] <= 0:
        return False, "passenger_count must be > 0"

    # 3. trip_distance
    if event.get("trip_distance") is None:
        return False, "Missing trip_distance"
    if event["trip_distance"] <= 0:
        return False, "trip_distance must be > 0"

    # 4. fare_amount
    if event.get("fare_amount") is None:
        return False, "Missing fare_amount"
    if event["fare_amount"] < 0:
        return False, "fare_amount must be >= 0"

    # 5. total_amount
    if event.get("total_amount") is None:
        return False, "Missing total_amount"
    if event["total_amount"] < 0:
        return False, "total_amount must be >= 0"

    # 6. datetime
    if not event.get("pickup_datetime"):
        return False, "Missing pickup_datetime"
    if not event.get("dropoff_datetime"):
        return False, "Missing dropoff_datetime"
    if event["pickup_datetime"] >= event["dropoff_datetime"]:
        return False, "pickup_datetime must be before dropoff_datetime"

    return True, None

def transform_event(event):
    pickup = pd.to_datetime(event["pickup_datetime"])
    dropoff = pd.to_datetime(event["dropoff_datetime"])

    trip_duration_minutes = (
        dropoff - pickup
    ).total_seconds() / 60

    fare_per_mile = (
        event["fare_amount"] / event["trip_distance"]
    )

    return {
        **event,
        "trip_duration_minutes": trip_duration_minutes,
        "fare_per_mile": fare_per_mile
    }

print("Consumer started...")
print(f"Listening to topic: {TOPIC_NAME}")


for message in consumer:
    event = message.value
    is_valid, reason = validation(event)
    if not is_valid:
        print(f"Invalid event {event.get('event_id')}: {reason}")
        continue

    transformed_event = transform_event(event)

    print(f"Transformed event: {transformed_event['event_id']}")
    print(f"Duration: {transformed_event['trip_duration_minutes']:.2f} minutes")
    print(f"Fare/mile: ${transformed_event['fare_per_mile']:.2f}")