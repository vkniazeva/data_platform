import unittest
from unittest.mock import MagicMock, patch


class TestParquetWriter(unittest.TestCase):
    def setUp(self):
        with patch("ingestion.raw.writer.parquet_writer.boto3"):
            from ingestion.raw.writer import ParquetWriter
            self.writer = ParquetWriter(topic="market.fix.raw")
            self.writer._s3_client = MagicMock()

    def _make_msg(self):
        msg = MagicMock()
        msg.partition.return_value = 0
        msg.offset.return_value = 1
        return msg

    def test_write_to_buffer_appends_event(self):
        self.writer.current_date = "2026-10-08"
        event = {"17": "EX-001", "55": "AH"}
        msg = self._make_msg()
        self.writer.write_to_buffer(event, "2026-10-08", msg)
        self.assertIn(event, self.writer._buffer)

    def test_write_to_buffer_updates_last_msg(self):
        msg = self._make_msg()
        self.writer.write_to_buffer({"17": "EX-001"}, "2026-10-08", msg)
        self.writer.current_date = "2026-10-08"
        self.writer.write_to_buffer({"17": "EX-002"}, "2026-10-08", msg)
        self.assertEqual(self.writer._last_msg, msg)

    def test_flush_empty_buffer_returns_false(self):
        result = self.writer.flush("2026-10-08")
        self.assertFalse(result)
        self.writer._s3_client.put_object.assert_not_called()

    def test_flush_writes_to_s3_and_returns_true(self):
        self.writer.current_date = "2026-10-08"
        msg = self._make_msg()
        for i in range(5):
            self.writer._buffer.append({"17": f"EX-00{i}", "55": "AH"})
        self.writer._last_msg = msg
        result = self.writer.flush("2026-10-08")
        self.assertTrue(result)
        self.writer._s3_client.put_object.assert_called_once()
        self.assertEqual(len(self.writer._buffer), 0)

    def test_flush_clears_buffer(self):
        self.writer.current_date = "2026-10-08"
        self.writer._buffer = [{"17": "EX-001"}]
        self.writer.flush("2026-10-08")
        self.assertEqual(self.writer._buffer, [])

    def test_write_to_buffer_flushes_on_100_messages(self):
        self.writer.current_date = "2026-10-08"
        msg = self._make_msg()
        for i in range(99):
            self.writer._buffer.append({"17": f"EX-{i:03d}"})
        flushed = self.writer.write_to_buffer({"17": "EX-099"}, "2026-10-08", msg)
        self.assertTrue(flushed)
        self.writer._s3_client.put_object.assert_called_once()

    def test_write_to_buffer_flushes_on_date_change(self):
        self.writer.current_date = "2026-10-07"
        self.writer._buffer = [{"17": "EX-001"}]
        msg = self._make_msg()
        flushed = self.writer.write_to_buffer({"17": "EX-002"}, "2026-10-08", msg)
        self.assertTrue(flushed)
        self.writer._s3_client.put_object.assert_called_once()

    def test_flush_retries_on_s3_failure(self):
        self.writer.current_date = "2026-10-08"
        self.writer._buffer = [{"17": "EX-001"}]
        self.writer._s3_client.put_object.side_effect = [Exception("S3 error"), None]
        self.writer.flush("2026-10-08")
        self.assertEqual(self.writer._s3_client.put_object.call_count, 2)

    def test_flush_raises_after_3_failed_attempts(self):
        self.writer.current_date = "2026-10-08"
        self.writer._buffer = [{"17": "EX-001"}]
        self.writer._s3_client.put_object.side_effect = Exception("S3 error")
        with self.assertRaises(Exception):
            self.writer.flush("2026-10-08")
