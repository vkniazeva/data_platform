import re
from pathlib import Path

import yaml

from ingestion.validation import ValidationResult

_RULES_PATH = Path(__file__).parent / "config" / "header_rules.yml"


class HeaderValidator:
    def __init__(self):
        with open(_RULES_PATH) as f:
            rules = yaml.safe_load(f)
        self._known_metals: set = set(rules["known_metals"])
        self._known_event_types: set = set(rules["known_event_types"])
        self._idempotency_pattern = re.compile(rules["idempotency_key_pattern"])
        self._known_regions: dict[str, set] = {
            topic: set(regions)
            for topic, regions in rules.get("known_regions_by_topic", {}).items()
        }

    def validate(self, topic: str, headers: dict, kafka_key: str) -> ValidationResult:
        """Validate idempotency key, event type, metal, and region from headers and Kafka key."""
        idempotency_key = headers.get("idempotency_key")
        if not idempotency_key:
            return ValidationResult(valid=False, reason="missing_header:idempotency_key")
        if not self._idempotency_pattern.match(idempotency_key):
            return ValidationResult(valid=False, reason=f"invalid_header:idempotency_key:{idempotency_key}")

        event_type = headers.get("event_type")
        if event_type not in self._known_event_types:
            return ValidationResult(valid=False, reason=f"invalid_header:event_type:{event_type}")

        metal = kafka_key.split("_")[0]
        if metal not in self._known_metals:
            return ValidationResult(valid=False, reason=f"invalid_header:metal:{metal}")

        known_regions = self._known_regions.get(topic)
        if known_regions:
            parts = kafka_key.split("_")
            if len(parts) < 2 or parts[1] not in known_regions:
                region = parts[1] if len(parts) >= 2 else "missing"
                return ValidationResult(valid=False, reason=f"invalid_header:region:{region}")

        return ValidationResult(valid=True)
