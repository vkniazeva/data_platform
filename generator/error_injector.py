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
        """Pick a random error type for the topic and apply it to a deep copy of the event."""
        pool = ERROR_POOL.get(topic, _HEADER_ERRORS)
        error_type = random.choice(pool)
        corrupted = copy.deepcopy(event)
        getattr(self, f"_{error_type}")(corrupted)
        logger.warning(f"[error_injector] injected '{error_type}' into {topic}")
        return corrupted

    def _missing_idempotency_key(self, event: dict) -> None:
        """Remove the idempotency_key header."""
        event["headers"].pop("idempotency_key", None)

    def _unknown_metal(self, event: dict) -> None:
        """Replace the Kafka key with an invalid metal code."""
        event["key"] = "XX"

    def _unknown_event_type(self, event: dict) -> None:
        """Set event_type header to an invalid value."""
        event["headers"]["event_type"] = "invalid_event"

    def _missing_mandatory_field(self, event: dict) -> None:
        """Remove a random mandatory field from the event value."""
        field = random.choice(_MANDATORY_REGIONAL_FIELDS)
        event["value"].pop(field, None)

    def _wrong_field_type(self, event: dict) -> None:
        """Set price to a non-numeric string to trigger type validation failure."""
        event["value"]["price"] = "not_a_number"

    def _invalid_business_value(self, event: dict) -> None:
        """Set side to an invalid value to trigger business constraint failure."""
        event["value"]["side"] = "sideways"

    def _negative_price(self, event: dict) -> None:
        """Set price to -1.0 to trigger the min-value business rule."""
        event["value"]["price"] = -1.0

    def _unknown_header(self, event: dict) -> None:
        """Add an unexpected header (intentionally ignored by the pipeline — tolerance test)."""
        event["headers"]["unknown_header_xyz"] = "unexpected_value"

    def _unknown_field(self, event: dict) -> None:
        """Add an unexpected field to the event value (intentionally ignored — tolerance test)."""
        event["value"]["unknown_field_xyz"] = "unexpected_value"
