import random
import uuid
from datetime import datetime, timezone

from generator.reference import BARGES, METALS, WAREHOUSES


def _select_random_keys(elements: dict, min_items: int = 1) -> list[str]:
    keys = list(elements)
    number_of_items = random.randint(min_items, len(keys))
    return random.sample(keys, number_of_items)


def _generate_stock(location_type: str) -> tuple[list[dict], list[dict]]:
    available_stock = []
    reserved_stock = []

    for metal_id in _select_random_keys(METALS):
        if location_type == "warehouse":
            available_quantity = random.randint(100, 5000)
        else:
            available_quantity = random.randint(10, 500)

        reserved_quantity = random.randint(0, available_quantity)

        available_stock.append(
            {
                "metal_id": metal_id,
                "quantity": available_quantity,
            }
        )

        if reserved_quantity > 0:
            reserved_stock.append(
                {
                    "metal_id": metal_id,
                    "quantity": reserved_quantity,
                }
            )

    return available_stock, reserved_stock


def _generate_stock_request_body(key: str) -> dict:
    location_type = random.choice(["warehouse", "barge"])

    locations = (
        _select_random_keys(WAREHOUSES) if location_type == "warehouse"
        else _select_random_keys(BARGES)
    )

    items = []

    for location in locations:
        available_stock, reserved_stock = _generate_stock(location_type)

        items.append(
            {
                "location": location,
                "location_type": location_type,
                "available_stock": available_stock,
                "reserved_stock": reserved_stock,
            }
        )

    return {
        "snapshot_id": key,
        "observed_at": datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "items": items,
    }


def generate_stock_request(request_number: int) -> dict:
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    key = f"inv-{date}-{request_number:06d}"

    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": key,
        "X-Source": "synthetic-inventory-generator",
        "X-Schema-Version": "1",
        "X-Request-Id": str(uuid.uuid4()),
    }

    return {
        "headers": headers,
        "body": _generate_stock_request_body(key),
    }
