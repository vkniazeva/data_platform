import copy
import random

import logging

from config import TOPIC_FIX_RAW, TOPIC_EVENTS

logger = logging.getLogger(__name__)

_HEADER_ERRORS = ["missing_idempotency_key", "unknown_metal", "unknown_event_type", "unknown_header"]
_CONTENT_ERRORS = ["missing_mandatory_field", "wrong_field_type", "invalid_business_value", "negative_price", "unknown_field"]

ERROR_POOL = {
    TOPIC_FIX_RAW: _HEADER_ERRORS,
    TOPIC_EVENTS:  _HEADER_ERRORS + _CONTENT_ERRORS,
}

_MANDATORY_REGIONAL_FIELDS = ["execution_id", "quantity", "price"]


class ErrorInjector:
    def inject(self, event: dict, topic: str) -> dict:
        pool = ERROR_POOL.get(topic, _HEADER_ERRORS)
        error_type = random.choice(pool)
        corrupted = copy.deepcopy(event)
        getattr(self, f"_{error_type}")(corrupted)
        logger.warning(f"[error_injector] injected '{error_type}' into {topic}")
        return corrupted

    def _missing_idempotency_key(self, event: dict) -> None:
        event["headers"].pop("idempotency_key", None)

    def _unknown_metal(self, event: dict) -> None:
        event["key"] = "XX"

    def _unknown_event_type(self, event: dict) -> None:
        event["headers"]["event_type"] = "invalid_event"

    def _missing_mandatory_field(self, event: dict) -> None:
        field = random.choice(_MANDATORY_REGIONAL_FIELDS)
        event["value"].pop(field, None)

    def _wrong_field_type(self, event: dict) -> None:
        event["value"]["price"] = "not_a_number"

    def _invalid_business_value(self, event: dict) -> None:
        event["value"]["side"] = "sideways"

    def _negative_price(self, event: dict) -> None:
        event["value"]["price"] = -1.0

    def _unknown_header(self, event: dict) -> None:
        event["headers"]["unknown_header_xyz"] = "unexpected_value"

    def _unknown_field(self, event: dict) -> None:
        event["value"]["unknown_field_xyz"] = "unexpected_value"
