import unittest

from generator.data_generator.event_generator import _generate_fix_event, _generate_regional_event
from generator.error_injector import ErrorInjector, ERROR_POOL


class TestErrorInjector(unittest.TestCase):

    def setUp(self):
        self.injector = ErrorInjector()
        self.fix_event = _generate_fix_event(1, "trade_execution_report")
        self.regional_event = _generate_regional_event(1, "regional_price_quote")

    def test_original_event_not_mutated(self):
        original_key = self.fix_event["key"]
        self.injector._unknown_metal({"key": "CA", "headers": {}, "value": ""})
        self.assertEqual(self.fix_event["key"], original_key)

    def test_inject_returns_deep_copy(self):
        corrupted = self.injector.inject(self.fix_event, "market.fix.raw")
        self.assertIsNot(corrupted, self.fix_event)

    # --- header errors (both topics) ---

    def test_missing_idempotency_key(self):
        event = {"key": "CA", "headers": {"idempotency_key": "EX-001"}, "value": ""}
        self.injector._missing_idempotency_key(event)
        self.assertNotIn("idempotency_key", event["headers"])

    def test_unknown_metal(self):
        event = {"key": "CA", "headers": {}, "value": ""}
        self.injector._unknown_metal(event)
        self.assertEqual(event["key"], "XX")

    def test_unknown_event_type(self):
        event = {"key": "CA", "headers": {"event_type": "trade_execution_report"}, "value": ""}
        self.injector._unknown_event_type(event)
        self.assertEqual(event["headers"]["event_type"], "invalid_event")

    def test_unknown_header(self):
        event = {"key": "CA", "headers": {}, "value": ""}
        self.injector._unknown_header(event)
        self.assertIn("unknown_header_xyz", event["headers"])

    # --- content errors (market.events only) ---

    def test_missing_mandatory_field(self):
        event = {"key": "CA", "headers": {}, "value": {"execution_id": "EX-1", "quantity": 10, "price": 100.0}}
        self.injector._missing_mandatory_field(event)
        removable = {"execution_id", "quantity", "price"}
        self.assertTrue(len(removable & set(event["value"].keys())) < 3)

    def test_wrong_field_type(self):
        event = {"key": "CA", "headers": {}, "value": {"price": 100.0}}
        self.injector._wrong_field_type(event)
        self.assertEqual(event["value"]["price"], "not_a_number")

    def test_invalid_business_value(self):
        event = {"key": "CA", "headers": {}, "value": {"side": "buy"}}
        self.injector._invalid_business_value(event)
        self.assertEqual(event["value"]["side"], "sideways")

    def test_negative_price(self):
        event = {"key": "CA", "headers": {}, "value": {"price": 100.0}}
        self.injector._negative_price(event)
        self.assertEqual(event["value"]["price"], -1.0)

    def test_unknown_field(self):
        event = {"key": "CA", "headers": {}, "value": {}}
        self.injector._unknown_field(event)
        self.assertIn("unknown_field_xyz", event["value"])

    # --- pool coverage ---

    def test_fix_pool_only_header_errors(self):
        content_errors = {"missing_mandatory_field", "wrong_field_type", "invalid_business_value", "negative_price", "unknown_field"}
        self.assertTrue(content_errors.isdisjoint(set(ERROR_POOL["market.fix.raw"])))

    def test_regional_pool_has_all_errors(self):
        all_errors = {"missing_idempotency_key", "unknown_metal", "unknown_event_type",
                      "unknown_header", "missing_mandatory_field", "wrong_field_type",
                      "invalid_business_value", "negative_price", "unknown_field"}
        self.assertEqual(all_errors, set(ERROR_POOL["market.events"]))
