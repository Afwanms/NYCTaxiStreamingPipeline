import pandas as pd

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