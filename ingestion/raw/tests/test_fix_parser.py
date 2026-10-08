import unittest
from ingestion.raw.parser.fix_parser import parse_fix_event, return_fix_event_date


class TestFixParser(unittest.TestCase):
    def setUp(self):
        self.raw_event = b"8=FIX.4.4|35=8|34=2052|52=20261005-21:12:26.047|17=EX-002052|55=AH|54=1|32=33|31=2664.29|60=20261005-21:04:16.047|"

    def test_parse_fix_event_returns_dict(self):
        result = parse_fix_event(self.raw_event)
        self.assertIsInstance(result, dict)

    def test_parse_fix_event_fields(self):
        result = parse_fix_event(self.raw_event)
        self.assertEqual(result["17"], "EX-002052")
        self.assertEqual(result["55"], "AH")
        self.assertEqual(result["54"], "1")
        self.assertEqual(result["32"], "33")
        self.assertEqual(result["31"], "2664.29")

    def test_return_fix_event_date(self):
        event = parse_fix_event(self.raw_event)
        date = return_fix_event_date(event)
        self.assertEqual(date, "2026-10-05")

    def test_parse_fix_event_missing_trailing_pipe(self):
        raw = b"8=FIX.4.4|35=8|34=1|52=20261005-21:12:26.047|17=EX-000001|55=SN|54=2|32=10|31=100.0|60=20261005-21:00:00.000"
        result = parse_fix_event(raw)
        self.assertEqual(result["17"], "EX-000001")
