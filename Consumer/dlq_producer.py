import json
from kafka import KafkaProducer

TOPIC_NAME = "taxi_trips_dlq"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_dlq(event, reason):
    dlq_event = {
        "event": event,
        "error_reason": reason
    }

    producer.send(TOPIC_NAME, value=dlq_event).get(timeout=10)
    print(f"Sent to DLQ: {event.get('event_id')} | {reason}")