import io
import json
import unittest
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.loader.bronze_loader import BronzeLoader


def _make_parquet_bytes(data: dict) -> bytes:
    table = pa.Table.from_pydict(data)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


class TestBronzeLoaderFixEvents(unittest.TestCase):
    def setUp(self):
        self.s3_client = MagicMock()
        self.ch_client = MagicMock()
        self.loader = BronzeLoader(self.s3_client, self.ch_client)

    def test_load_fix_events_calls_insert(self):
        parquet_bytes = _make_parquet_bytes({
            "8": ["FIX.4.4"], "35": ["8"], "34": ["1"],
            "52": ["20261005-21:12:26.047"], "17": ["EX-000001"],
            "55": ["AH"], "54": ["1"], "32": ["33"],
            "31": ["2664.29"], "60": ["20261005-21:04:16.047"]
        })
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(parquet_bytes)}
        self.loader.load_fix_events("market.fix.raw/2026-10-05.parquet")
        self.ch_client.insert_arrow.assert_called_once()

    def test_load_fix_events_adds_source_file(self):
        parquet_bytes = _make_parquet_bytes({
            "8": ["FIX.4.4"], "35": ["8"], "34": ["1"],
            "52": ["20261005-21:12:26.047"], "17": ["EX-000001"],
            "55": ["AH"], "54": ["1"], "32": ["33"],
            "31": ["2664.29"], "60": ["20261005-21:04:16.047"]
        })
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(parquet_bytes)}
        self.loader.load_fix_events("market.fix.raw/2026-10-05.parquet")
        table = self.ch_client.insert_arrow.call_args[0][1]
        self.assertIn("source_file", table.schema.names)
        self.assertEqual(table["source_file"][0].as_py(), "market.fix.raw/2026-10-05.parquet")

    def test_load_fix_events_adds_pipeline_version(self):
        parquet_bytes = _make_parquet_bytes({
            "8": ["FIX.4.4"], "35": ["8"], "34": ["1"],
            "52": ["20261005-21:12:26.047"], "17": ["EX-000001"],
            "55": ["AH"], "54": ["1"], "32": ["33"],
            "31": ["2664.29"], "60": ["20261005-21:04:16.047"]
        })
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(parquet_bytes)}
        self.loader.load_fix_events("market.fix.raw/2026-10-05.parquet")
        table = self.ch_client.insert_arrow.call_args[0][1]
        self.assertEqual(table["pipeline_version"][0].as_py(), "1.0")


class TestBronzeLoaderRegionalEvents(unittest.TestCase):
    def setUp(self):
        self.s3_client = MagicMock()
        self.ch_client = MagicMock()
        self.loader = BronzeLoader(self.s3_client, self.ch_client)

        conditions = [{"region": "Middle East", "warehouse": {"warehouse_id": "WH_DBX", "city": "Dubai"}}]
        self.parquet_bytes = _make_parquet_bytes({
            "event_type": ["regional_price_quote"],
            "schema_version": [1],
            "execution_id": ["EX-ME-001"],
            "instrument": ["PB"],
            "side": ["sell"],
            "quantity": [24],
            "price": [2196.44],
            "currency": ["USD"],
            "event_timestamp": ["20261005-21:11:47.968"],
            "conditions": [json.dumps(conditions[0])]
        })

    def test_load_regional_events_calls_insert(self):
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(self.parquet_bytes)}
        self.loader.load_regional_events("market.events/2026-10-05.parquet")
        self.ch_client.insert_arrow.assert_called_once()

    def test_load_regional_events_adds_source_file(self):
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(self.parquet_bytes)}
        self.loader.load_regional_events("market.events/2026-10-05.parquet")
        table = self.ch_client.insert_arrow.call_args[0][1]
        self.assertIn("source_file", table.schema.names)
        self.assertEqual(table["source_file"][0].as_py(), "market.events/2026-10-05.parquet")

    def test_load_regional_events_conditions_serialized_as_string(self):
        self.s3_client.get_object.return_value = {"Body": io.BytesIO(self.parquet_bytes)}
        self.loader.load_regional_events("market.events/2026-10-05.parquet")
        table = self.ch_client.insert_arrow.call_args[0][1]
        conditions_val = table["conditions"][0].as_py()
        self.assertIsInstance(conditions_val, str)
        parsed = json.loads(conditions_val)
        self.assertIn("region", parsed)
