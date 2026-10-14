from dataclasses import dataclass


@dataclass
class ValidationResult:
    valid: bool
    reason: str | None = None
