import random
from datetime import datetime, timedelta, timezone

from generator.reference import BARGES, WAREHOUSES


ROUTE_STATE = {}
FILE_NUMBER = 0


def _format_timestamp(timestamp: datetime) -> str:
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _get_or_create_route_state(barge_id: str, current_timestamp: datetime) -> dict:
    if barge_id in ROUTE_STATE:
        return ROUTE_STATE[barge_id]

    travel_duration = timedelta(days=BARGES[barge_id]["estimated_travel_days"])

    # First barge appearance
    elapsed_seconds = random.uniform(0, travel_duration.total_seconds() * 0.8)

    departure_at = current_timestamp - timedelta(seconds=elapsed_seconds)

    state = {
        "route_id": f"RTE-{barge_id}-0001",
        "departure_at": departure_at,
        "last_position_at": departure_at,
        "last_position_sequence": 0,
    }

    ROUTE_STATE[barge_id] = state

    return state


def generate_barge_position_file(current_timestamp: datetime) -> dict:
    global FILE_NUMBER

    if current_timestamp.tzinfo is None:
        raise ValueError("current_timestamp must contain timezone information")

    current_timestamp = current_timestamp.astimezone(timezone.utc)

    barge_id = random.choice(list(BARGES))
    barge = BARGES[barge_id]

    departure_port_id = barge["hosting_port"]
    target_port_id = barge["target_port"]

    departure_port = WAREHOUSES[departure_port_id]
    target_port = WAREHOUSES[target_port_id]

    route_state = _get_or_create_route_state(
        barge_id=barge_id,
        current_timestamp=current_timestamp,
    )

    departure_at = route_state["departure_at"]
    last_position_at = route_state["last_position_at"]

    expected_arrival_at = departure_at + timedelta(days=barge["estimated_travel_days"])

    batch_end_at = min(current_timestamp, expected_arrival_at)

    if last_position_at >= batch_end_at:
        return {
            "file_id": None,
            "records": [],
            "status": "route_completed",
        }

    point_count = random.randint(1, 10)

    interval_seconds = (batch_end_at - last_position_at).total_seconds()

    position_timestamps = []

    for _ in range(point_count - 1):
        random_offset = random.uniform(0, interval_seconds)

        position_timestamps.append(last_position_at + timedelta(seconds=random_offset))

    position_timestamps.append(batch_end_at)
    position_timestamps.sort()

    records = []

    total_route_seconds = (expected_arrival_at - departure_at).total_seconds()

    for position_timestamp in position_timestamps:
        progress = (position_timestamp - departure_at).total_seconds() / total_route_seconds

        latitude = (departure_port["lat"] + (target_port["lat"] - departure_port["lat"]) * progress)

        longitude = (departure_port["lon"] + (target_port["lon"] - departure_port["lon"]) * progress)

        remaining_minutes = max(0, int((expected_arrival_at - position_timestamp).total_seconds() / 60))

        route_state["last_position_sequence"] += 1

        records.append(
            {
                "barge_id": barge_id,
                "route_id": route_state["route_id"],
                "position_sequence": route_state["last_position_sequence"],
                "departure_port": departure_port_id,
                "target_port": target_port_id,
                "position_timestamp": _format_timestamp(position_timestamp),
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "expected_arrival_at": _format_timestamp(
                    expected_arrival_at
                ),
                "remaining_minutes": remaining_minutes,
            }
        )

    route_state["last_position_at"] = batch_end_at

    FILE_NUMBER += 1

    return {
        "file_id": (
            f"barge-positions-"
            f"{current_timestamp.strftime('%Y%m%d')}-"
            f"{FILE_NUMBER:06d}"
        ),
        "schema_version": 1,
        "generated_at": _format_timestamp(current_timestamp),
        "records": records,
        "status": "generated",
    }