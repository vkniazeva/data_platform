import unittest

from ingestion.validation.header_validator import HeaderValidator


class TestHeaderValidatorFix(unittest.TestCase):

    def setUp(self):
        self.validator = HeaderValidator()
        self.valid_headers = {
            "idempotency_key": "EX-000001",
            "event_type": "trade_execution_report",
        }

    def test_valid_fix_event(self):
        result = self.validator.validate("market.fix.raw", self.valid_headers, "AH")
        self.assertTrue(result.valid)

    def test_missing_idempotency_key(self):
        headers = {**self.valid_headers}
        del headers["idempotency_key"]
        result = self.validator.validate("market.fix.raw", headers, "AH")
        self.assertFalse(result.valid)
        self.assertEqual(result.reason, "missing_header:idempotency_key")

    def test_invalid_idempotency_key_format(self):
        headers = {**self.valid_headers, "idempotency_key": "BADFORMAT"}
        result = self.validator.validate("market.fix.raw", headers, "AH")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:idempotency_key", result.reason)

    def test_unknown_event_type(self):
        headers = {**self.valid_headers, "event_type": "invalid_event"}
        result = self.validator.validate("market.fix.raw", headers, "AH")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:event_type", result.reason)

    def test_unknown_metal(self):
        result = self.validator.validate("market.fix.raw", self.valid_headers, "XX")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:metal", result.reason)


class TestHeaderValidatorRegional(unittest.TestCase):

    def setUp(self):
        self.validator = HeaderValidator()
        self.valid_headers = {
            "idempotency_key": "EX-ME-0001",
            "event_type": "regional_price_quote",
        }

    def test_valid_regional_event(self):
        result = self.validator.validate("market.events", self.valid_headers, "AH_ME")
        self.assertTrue(result.valid)

    def test_unknown_region(self):
        result = self.validator.validate("market.events", self.valid_headers, "AH_XX")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:region", result.reason)

    def test_missing_region_in_key(self):
        result = self.validator.validate("market.events", self.valid_headers, "AH")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:region", result.reason)

    def test_unknown_metal_regional(self):
        result = self.validator.validate("market.events", self.valid_headers, "XX_ME")
        self.assertFalse(result.valid)
        self.assertIn("invalid_header:metal", result.reason)

    def test_unknown_header_extra_field_passes(self):
        headers = {**self.valid_headers, "unknown_header_xyz": "unexpected_value"}
        result = self.validator.validate("market.events", headers, "AH_ME")
        self.assertTrue(result.valid)
