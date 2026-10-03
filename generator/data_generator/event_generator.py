"""
fix event properties
mask = "var1=value|var2=value...etc"
8 - protocol version - default value = FIX.4.4
35 - execution report - default value = 8
34 - message number in a fix session - iterator +1
52 - timestamp - generated
17 - execution id or event id - mask EX-000001 (iterator +1)
55 - instrument id (metal) - code from reference
54 - side - Buy:1, Sell:2
32 - quantity - random [1 - 100]
31 - price
60 - trade timestamp

"""
import random
from datetime import datetime, timezone, timedelta

from generator.reference import METALS, REGIONS, WAREHOUSES


def _generate_fix_body(message_number:int) -> tuple(str, str):

    now = datetime.now(timezone.utc)
    event_timestamp = now.strftime("%Y%m%d-%H:%M:%S.%f")[:-3]

    delay = timedelta(seconds=random.randint(60, 600))
    deal_timestamp = (now - delay).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]

    metal_id = random.choice(list(METALS))
    side = random.randint(1,2)
    quantity = random.randint(1,100)


    min_price = METALS[metal_id]['price_min']
    max_price = METALS[metal_id]['price_max']
    price = round(random.uniform(min_price, max_price), 2)

    fields = [
        "8=FIX.4.4",
        "35=8",
        f"34={message_number}",
        f"52={event_timestamp}",
        f"17=EX-{message_number:06d}",
        f"55={metal_id}",
        f"54={side}",
        f"32={quantity}",
        f"31={price}",
        f"60={deal_timestamp}"
    ]

    return "|".join(fields) + "|", metal_id

def _generate_fix_event(message_number: int, event_type: str) -> dict:
    event_id = f"EX-{message_number:06d}"
    value, metal_id = _generate_fix_body(message_number)

    return {
        "key": metal_id,
        "value": value,
        "headers": {
            "event_type": event_type,
            "schema_version": "1",
            "source": "synthetic_fix",
            "content_type": "text/plain",
            "idempotency_key": event_id
        }
    }

def _generate_regional_event(message_number: int, event_type: str) -> dict:
    region = random.choice(list(REGIONS))
    event_id = f"EX-{region}-{message_number:04d}"
    conditions = {}

    regional_warehouses = {wh: data for wh, data in WAREHOUSES.items() if data["region"] == region}

    if region in ["ME", "AS"]:
        warehouse = random.choice(list(regional_warehouses))
        conditions = {
            "region": REGIONS[region],
            "warehouse": {
                "warehouse_id": warehouse,
                "city": WAREHOUSES[warehouse]["city"]
            }
        }
    elif region == "EU":
        warehouse = random.choice(list(regional_warehouses))
        conditions = {
            "region": REGIONS[region],
            "warehouse": {
                "warehouse_id": warehouse,
                "city": WAREHOUSES[warehouse]["city"]
            },
            "delivery": {
                "period": random.randint(1,30),
                "period_type": "days"
            }
        }

    metal_id = random.choice(list(METALS))
    common_fix_data = {
        "event_type": event_type,
        "schema_version": 1,
        "execution_id": event_id,
        "instrument": metal_id,
        "side": random.choice(["sell", "buy"]),
        "quantity": random.randint(1,100),
        "price": round(random.uniform(METALS[metal_id]["price_min"], METALS[metal_id]["price_max"]), 2),
        "currency": "USD",
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        "conditions": conditions
    }

    return {
        "key": f"{metal_id}_{region}",
        "value": common_fix_data,
        "headers": {
            "event_type": event_type,
            "schema_version": "1",
            "source": "synthetic_regional_market",
            "content_type": "application/json",
            "idempotency_key": event_id
        }
    }


def generate_kafka_event(event_type: str, message_number: int) -> dict:
    """Generate Kafka event wrapper with headers and body"""
    if event_type == "trade_execution_report":
        return _generate_fix_event(message_number, event_type)
    if event_type == "regional_price_quote":
        return _generate_regional_event(message_number, event_type)
    else:
        raise ValueError(f"Unknown event type: {event_type}")











