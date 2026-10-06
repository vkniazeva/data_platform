from datetime import datetime


def parse_fix_event(event: bytes) -> dict:
    """
    event value example:
    8=FIX.4.4|35=8|34=2052|52=20261005-21:12:26.047|17=EX-002052|55=AH|54=1|32=33|31=2664.29|60=20261005-21:04:16.047|
    """
    raw_value = event.decode("utf-8")
    parsed_event = raw_value.split("|")
    event_value_dict = {}
    for event_part in parsed_event[:-1]:
        event_value_dict[event_part.split("=")[0]] = event_part.split("=")[1]
    return event_value_dict

def return_fix_event_date(event_value_dict: dict) -> str:
    raw_date = event_value_dict["60"]
    dt_object = datetime.strptime(raw_date, "%Y%m%d-%H:%M:%S.%f")
    return datetime.strftime(dt_object, "%Y-%m-%d")

