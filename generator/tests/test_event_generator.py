from generator.data_generator.event_generator import generate_kafka_event, _generate_fix_event, _generate_regional_event
import unittest

from generator.reference import METALS
from unittest.mock import patch


class TestEventGenerator(unittest.TestCase):

    def setUp(self):
        self.message_number = 1
        self.event_type = "trade_execution_report"
        self.event = _generate_fix_event(self.message_number, self.event_type)

    def test_fix_event_structure_success(self):
        required_keys = {"key", "value", "headers"}
        event_keys = set(self.event.keys())

        self.assertTrue(required_keys.issubset(event_keys))
        self.assertTrue(isinstance(self.event["value"], str))
        self.assertIn(self.event["key"], METALS)

    def test_fix_event_headers_success(self):
        required_headers = {"event_type", "schema_version", "source", "content_type", "idempotency_key"}
        self.assertTrue(required_headers.issubset(self.event["headers"]))

    def test_fix_event_value_required_tags_success(self):
        protocol_version = "8=FIX.4.4"
        message_type = "35=8"
        message_number = "34=1"
        execution_id = "17=EX-000001"
        required_tags = [52, 55, 54, 32, 31, 60]

        self.assertIn(protocol_version, self.event["value"])
        self.assertIn(message_type, self.event["value"])
        self.assertIn(message_number, self.event["value"])
        self.assertIn(execution_id, self.event["value"])
        print(self.event["value"])
        self.assertEqual(self.event["value"][-1], "|")

        all_tags_present = all(f"{tag}=" in self.event["value"] for tag in required_tags)
        self.assertTrue(all_tags_present)

    def test_fix_event_tag_values_not_nan_success(self):
        required_tags = ["52", "55", "54", "32", "31", "60"]
        parsed_event = self.event["value"].split("|")
        event_dict = {}
        # removing last empty element because of the last "|"
        for event_part in parsed_event[:-1]:
            event_dict[event_part.split("=")[0]] = event_part.split("=")[1]
        for tag in required_tags:
            self.assertIsNotNone(event_dict[tag])


class TestRegionalEventGenerator(unittest.TestCase):

    def setUp(self):
        self.message_number = 1
        self.event_type = "regional_price_quote"
        self.event = _generate_regional_event(self.message_number, self.event_type)

    def test_regional_event_structure_success(self):
        required_keys = {"key", "value", "headers"}
        self.assertTrue(required_keys.issubset(self.event.keys()))
        self.assertIsInstance(self.event["value"], dict)
        parts = self.event["key"].split("_")
        self.assertEqual(len(parts), 2)

    def test_regional_event_headers_success(self):
        required_headers = {"event_type", "schema_version", "source", "content_type",
                            "idempotency_key"}
        self.assertTrue(required_headers.issubset(self.event["headers"]))

    def test_eu_event_has_delivery_in_conditions(self):
        with patch("generator.data_generator.event_generator.random.choice") as mock_choice:
            mock_choice.side_effect = ["EU", "WH_RTM", "CA", "sell"]
            event = _generate_regional_event(1, "regional_price_quote")
            self.assertIn("delivery", event["value"]["conditions"])


class TestGenerateKafkaEvent(unittest.TestCase):

    def test_unknown_event_type_raises_value_error(self):
        with self.assertRaises(ValueError):
            generate_kafka_event("unknown_type", 1)


