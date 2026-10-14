import unittest

from ingestion.validation.content_validator import ContentValidator


def _valid_fix():
    return {"8": "FIX.4.4", "35": "8", "34": "1", "52": "20261014-10:00:00.000",
            "17": "EX-000001", "55": "AH", "54": "1", "32": "10", "31": "2500.00",
            "60": "20261014-09:55:00.000"}


def _valid_regional():
    return {"event_type": "regional_price_quote", "schema_version": 1,
            "execution_id": "EX-ME-0001", "instrument": "AH", "side": "buy",
            "quantity": 10, "price": 2500.0, "currency": "USD",
            "event_timestamp": "20261014-10:00:00.000",
            "conditions": {"region": "Middle East"}}


class TestContentValidatorFix(unittest.TestCase):

    def setUp(self):
        self.validator = ContentValidator()

    def test_valid_fix_event(self):
        result = self.validator.validate("market.fix.raw", _valid_fix())
        self.assertTrue(result.valid)

    def test_missing_mandatory_field(self):
        event = _valid_fix()
        del event["31"]
        result = self.validator.validate("market.fix.raw", event)
        self.assertFalse(result.valid)
        self.assertEqual(result.reason, "missing_field:31")

    def test_invalid_side(self):
        event = {**_valid_fix(), "54": "99"}
        result = self.validator.validate("market.fix.raw", event)
        self.assertFalse(result.valid)
        self.assertIn("invalid_value:54", result.reason)

    def test_negative_price(self):
        event = {**_valid_fix(), "31": "-1.0"}
        result = self.validator.validate("market.fix.raw", event)
        self.assertFalse(result.valid)
        self.assertIn("below_min:31", result.reason)

    def test_wrong_price_type(self):
        event = {**_valid_fix(), "31": "not_a_number"}
        result = self.validator.validate("market.fix.raw", event)
        self.assertFalse(result.valid)
        self.assertIn("wrong_type:31", result.reason)

    def test_zero_quantity(self):
        event = {**_valid_fix(), "32": "0"}
        result = self.validator.validate("market.fix.raw", event)
        self.assertFalse(result.valid)
        self.assertIn("below_min:32", result.reason)


class TestContentValidatorRegional(unittest.TestCase):

    def setUp(self):
        self.validator = ContentValidator()

    def test_valid_regional_event(self):
        result = self.validator.validate("market.events", _valid_regional())
        self.assertTrue(result.valid)

    def test_missing_mandatory_field(self):
        event = _valid_regional()
        del event["price"]
        result = self.validator.validate("market.events", event)
        self.assertFalse(result.valid)
        self.assertEqual(result.reason, "missing_field:price")

    def test_invalid_side(self):
        event = {**_valid_regional(), "side": "sideways"}
        result = self.validator.validate("market.events", event)
        self.assertFalse(result.valid)
        self.assertIn("invalid_value:side", result.reason)

    def test_negative_price(self):
        event = {**_valid_regional(), "price": -1.0}
        result = self.validator.validate("market.events", event)
        self.assertFalse(result.valid)
        self.assertIn("below_min:price", result.reason)

    def test_wrong_price_type(self):
        event = {**_valid_regional(), "price": "not_a_number"}
        result = self.validator.validate("market.events", event)
        self.assertFalse(result.valid)
        self.assertIn("wrong_type:price", result.reason)

    def test_invalid_currency(self):
        event = {**_valid_regional(), "currency": "JPY"}
        result = self.validator.validate("market.events", event)
        self.assertFalse(result.valid)
        self.assertIn("invalid_value:currency", result.reason)

    def test_unknown_field_passes(self):
        event = {**_valid_regional(), "unknown_field_xyz": "unexpected"}
        result = self.validator.validate("market.events", event)
        self.assertTrue(result.valid)

    def test_unknown_topic_passes(self):
        result = self.validator.validate("market.unknown", _valid_regional())
        self.assertTrue(result.valid)
