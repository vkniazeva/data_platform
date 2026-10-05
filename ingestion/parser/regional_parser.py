import json
from datetime import datetime

def parse_regional_event(event: dict) -> dict:
    """event example:
    "{\"event_type\": \"regional_price_quote\", \"schema_version\": 1, \"execution_id\": \"EX-ME-2039\", \"instrument\": \"PB\", \"side\": \"sell\", \"quantity\": 24, \"price\": 2196.44, \"currency\": \"USD\", \"event_timestamp\": \"20261005-21:11:47.968\", \"conditions\": {\"region\": \"Middle East\", \"warehouse\": {\"warehouse_id\": \"WH_DBX\", \"city\": \"Dubai\"}}}"
    """
    raw_event = event["value"].decode("utf-8")
    return json.loads(raw_event)

def return_event_date(event_value_dict: dict) -> str:
    raw_date = event_value_dict["event_timestamp"]
    dt_object = datetime.strptime(raw_date, "%Y%m%d-%H:%M:%S.%f")
    return datetime.strftime(dt_object, "%Y-%m-%d")



