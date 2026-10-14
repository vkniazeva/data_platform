import unittest
from unittest.mock import MagicMock, patch


class TestQuarantineWriter(unittest.TestCase):

    def _make_msg(self, offset=1, partition=0, headers=None):
        msg = MagicMock()
        msg.offset.return_value = offset
        msg.partition.return_value = partition
        msg.headers.return_value = headers or [("event_type", b"regional_price_quote")]
        return msg

    def _make_writer(self, mock_s3):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        writer._s3_client = mock_s3
        writer.bucket = "test-bucket"
        return writer

    @patch("ingestion.raw.writer.quarantine_writer.boto3.client")
    def test_write_adds_quarantine_metadata(self, _):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        writer._s3_client = MagicMock()
        writer.bucket = "test-bucket"

        event = {"execution_id": "EX-1", "price": 100.0}
        writer.write(event, "2026-10-14", "below_min:price:-1.0", self._make_msg())

        record = writer._buffer[0]
        self.assertEqual(record["validation_error"], "below_min:price:-1.0")
        self.assertIn("original_headers", record)
        self.assertIn("ingestion_timestamp", record)
        self.assertEqual(record["kafka_offset"], 1)
        self.assertEqual(record["kafka_partition"], 0)

    @patch("ingestion.raw.writer.quarantine_writer.boto3.client")
    def test_flush_on_100_messages(self, _):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        mock_s3 = MagicMock()
        writer._s3_client = mock_s3
        writer.bucket = "test-bucket"

        for i in range(100):
            writer.write({"execution_id": f"EX-{i}"}, "2026-10-14", "missing_field:price", self._make_msg(i))

        mock_s3.put_object.assert_called_once()
        self.assertEqual(len(writer._buffer), 0)

    @patch("ingestion.raw.writer.quarantine_writer.boto3.client")
    def test_flush_on_date_change(self, _):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        mock_s3 = MagicMock()
        writer._s3_client = mock_s3
        writer.bucket = "test-bucket"

        writer.write({"execution_id": "EX-1"}, "2026-10-14", "missing_field:price", self._make_msg())
        writer.write({"execution_id": "EX-2"}, "2026-10-15", "missing_field:price", self._make_msg())

        mock_s3.put_object.assert_called_once()

    @patch("ingestion.raw.writer.quarantine_writer.boto3.client")
    def test_s3_key_uses_quarantine_prefix(self, _):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        mock_s3 = MagicMock()
        writer._s3_client = mock_s3
        writer.bucket = "test-bucket"

        for i in range(100):
            writer.write({"execution_id": f"EX-{i}"}, "2026-10-14", "reason", self._make_msg(i))

        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertTrue(call_kwargs["Key"].startswith("quarantine/market.events/"))

    @patch("ingestion.raw.writer.quarantine_writer.boto3.client")
    def test_flush_remaining(self, _):
        from ingestion.raw.writer.quarantine_writer import QuarantineWriter
        writer = QuarantineWriter("market.events")
        mock_s3 = MagicMock()
        writer._s3_client = mock_s3
        writer.bucket = "test-bucket"

        writer.write({"execution_id": "EX-1"}, "2026-10-14", "reason", self._make_msg())
        writer.flush_remaining()

        mock_s3.put_object.assert_called_once()
        self.assertEqual(len(writer._buffer), 0)
