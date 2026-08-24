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