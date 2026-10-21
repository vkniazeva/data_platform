from pathlib import Path

import yaml

from ingestion.validation import ValidationResult

_CONFIG_DIR = Path(__file__).parent / "config"

_RULES_BY_TOPIC = {
    "market.fix.raw": _CONFIG_DIR / "fix_content_rules.yml",
    "market.events":  _CONFIG_DIR / "regional_content_rules.yml",
}


def _load_rules(path: Path) -> dict:
    """Load YAML validation rules from disk."""
    with open(path) as f:
        return yaml.safe_load(f)


class ContentValidator:
    def __init__(self):
        self._rules = {
            topic: _load_rules(path)
            for topic, path in _RULES_BY_TOPIC.items()
        }

    def validate(self, topic: str, event: dict) -> ValidationResult:
        """Check mandatory fields, field types, and business constraints for the given topic's rules."""
        rules = self._rules.get(topic)
        if not rules:
            return ValidationResult(valid=True)

        for field in rules.get("mandatory_fields", []):
            if field not in event:
                return ValidationResult(valid=False, reason=f"missing_field:{field}")

        for field, rule in rules.get("business_rules", {}).items():
            if field not in event:
                continue

            value = event[field]

            field_type = rule.get("type")
            if field_type:
                try:
                    value = int(value) if field_type == "int" else float(value)
                except (ValueError, TypeError):
                    return ValidationResult(valid=False, reason=f"wrong_type:{field}:{value}")

            if "allowed_values" in rule and value not in rule["allowed_values"]:
                return ValidationResult(valid=False, reason=f"invalid_value:{field}:{value}")

            if "min" in rule and value < rule["min"]:
                return ValidationResult(valid=False, reason=f"below_min:{field}:{value}")

        return ValidationResult(valid=True)
