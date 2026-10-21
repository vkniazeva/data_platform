import json
from datetime import datetime

PARSER_VERSION = "1.0.0"

def parse_regional_event(event: bytes) -> dict:
    """Decode a JSON-encoded regional market event into a dict."""
    raw_event = event.decode("utf-8")
    return json.loads(raw_event)

def return_regional_event_date(event_value_dict: dict) -> str:
    """Extract event date from event_timestamp field as YYYY-MM-DD."""
    raw_date = event_value_dict["event_timestamp"]
    dt_object = datetime.strptime(raw_date, "%Y%m%d-%H:%M:%S.%f")
    return datetime.strftime(dt_object, "%Y-%m-%d")



