import unittest
from ingestion.raw.parser.regional_parser import parse_regional_event, return_regional_event_date


class TestRegionalParser(unittest.TestCase):
    def setUp(self):
        self.raw_event = b'{"event_type": "regional_price_quote", "schema_version": 1, "execution_id": "EX-ME-2039", "instrument": "PB", "side": "sell", "quantity": 24, "price": 2196.44, "currency": "USD", "event_timestamp": "20261005-21:11:47.968", "conditions": {"region": "Middle East", "warehouse": {"warehouse_id": "WH_DBX", "city": "Dubai"}}}'

    def test_parse_regional_event_returns_dict(self):
        result = parse_regional_event(self.raw_event)
        self.assertIsInstance(result, dict)

    def test_parse_regional_event_fields(self):
        result = parse_regional_event(self.raw_event)
        self.assertEqual(result["execution_id"], "EX-ME-2039")
        self.assertEqual(result["instrument"], "PB")
        self.assertEqual(result["side"], "sell")
        self.assertEqual(result["quantity"], 24)
        self.assertAlmostEqual(result["price"], 2196.44)
        self.assertEqual(result["currency"], "USD")

    def test_parse_regional_event_conditions(self):
        result = parse_regional_event(self.raw_event)
        self.assertEqual(result["conditions"]["region"], "Middle East")
        self.assertEqual(result["conditions"]["warehouse"]["warehouse_id"], "WH_DBX")

    def test_return_regional_event_date(self):
        event = parse_regional_event(self.raw_event)
        date = return_regional_event_date(event)
        self.assertEqual(date, "2026-10-05")

    def test_parse_invalid_json_raises(self):
        with self.assertRaises(Exception):
            parse_regional_event(b"not valid json")
