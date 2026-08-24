import json
from kafka import KafkaConsumer
from validation import validation
from transform import transform_event
from database import create_table, insert_event
from dlq_producer import send_to_dlq
from collections import Counter

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "taxi_trips"
GROUP_ID = "taxi-dlq-test"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

create_table()
print("Consumer started...")
print(f"Listening to topic: {TOPIC_NAME}")
total_events = 0
valid_events = 0
invalid_events = 0
error_counts = Counter()

try:
    for message in consumer:
        total_events += 1
        event = message.value
        is_valid, reason = validation(event)
        if not is_valid:
            invalid_events += 1
            print(f"Invalid event {event.get('event_id')}: {reason}")
            send_to_dlq(event, reason)
            continue
        valid_events += 1
        transformed_event = transform_event(event)
        insert_event(transformed_event)
        print(f"Valid and Saved to PostgreSQL: {transformed_event['event_id']}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")
    print(f"Total events   : {total_events}")
    print(f"Valid events   : {valid_events}")
    print(f"Invalid events : {invalid_events}")
    for reason, count in error_counts.items():
        print(f"{reason}: {count}")