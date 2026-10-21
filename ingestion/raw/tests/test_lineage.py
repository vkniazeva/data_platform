import unittest
from unittest.mock import MagicMock
from ingestion.raw.lineage import build_lineage


def make_msg(schema_id=None, offset=42, partition=0):
    msg = MagicMock()
    msg.offset.return_value = offset
    msg.partition.return_value = partition
    if schema_id is not None:
        msg.headers.return_value = [("schema_id", schema_id.encode("utf-8"))]
    else:
        msg.headers.return_value = []
    return msg


class TestBuildLineage(unittest.TestCase):
    def test_fields_present(self):
        msg = make_msg(schema_id="2")
        result = build_lineage(msg, parser_version="1.0.0", producer_id="fix_generator_v1")
        self.assertIn("_ingested_at", result)
        self.assertIn("_producer_id", result)
        self.assertIn("_parser_version", result)
        self.assertIn("_schema_id", result)
        self.assertIn("_kafka_offset", result)
        self.assertIn("_kafka_partition", result)

    def test_values(self):
        msg = make_msg(schema_id="2", offset=99, partition=1)
        result = build_lineage(msg, parser_version="1.0.0", producer_id="fix_generator_v1")
        self.assertEqual(result["_producer_id"], "fix_generator_v1")
        self.assertEqual(result["_parser_version"], "1.0.0")
        self.assertEqual(result["_schema_id"], "2")
        self.assertEqual(result["_kafka_offset"], 99)
        self.assertEqual(result["_kafka_partition"], 1)

    def test_schema_id_missing(self):
        msg = make_msg(schema_id=None)
        result = build_lineage(msg, parser_version="1.0.0", producer_id="fix_generator_v1")
        self.assertIsNone(result["_schema_id"])

    def test_no_headers(self):
        msg = make_msg()
        msg.headers.return_value = None
        result = build_lineage(msg, parser_version="1.0.0", producer_id="fix_generator_v1")
        self.assertIsNone(result["_schema_id"])

    def test_ingested_at_is_iso_format(self):
        from datetime import datetime, timezone
        msg = make_msg(schema_id="1")
        result = build_lineage(msg, parser_version="1.0.0", producer_id="regional_generator_v1")
        parsed = datetime.fromisoformat(result["_ingested_at"])
        self.assertEqual(parsed.tzinfo, timezone.utc)
