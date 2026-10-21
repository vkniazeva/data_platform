from datetime import datetime

PARSER_VERSION = "1.0.0"
def parse_fix_event(event: bytes) -> dict:
    """Parse a pipe-delimited FIX 4.4 message into a tag→value dict (e.g. {"17": "EX-002052", "55": "AH", ...})."""
    raw_value = event.decode("utf-8")
    parsed_event = raw_value.split("|")
    event_value_dict = {}
    for event_part in parsed_event[:-1]:
        event_value_dict[event_part.split("=")[0]] = event_part.split("=")[1]
    return event_value_dict

def return_fix_event_date(event_value_dict: dict) -> str:
    """Extract trade date from FIX tag 60 (TransactTime) as YYYY-MM-DD."""
    raw_date = event_value_dict["60"]
    dt_object = datetime.strptime(raw_date, "%Y%m%d-%H:%M:%S.%f")
    return datetime.strftime(dt_object, "%Y-%m-%d")

