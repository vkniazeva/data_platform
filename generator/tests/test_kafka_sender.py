import unittest
from unittest.mock import patch, MagicMock
from generator.sender.kafka_sender import KafkaSender


class TestKafkaSender(unittest.TestCase):
    def setUp(self):
        with patch("generator.sender.kafka_sender.Producer") as mock_producer_class:
            mock_producer_class.return_value = MagicMock()
            self.sender = KafkaSender(bootstrap_servers="localhost:19092")
            self.sender._producer.flush.return_value = 0

    def test_on_delivery_callback_no_errors(self):
        self.sender._on_delivery_callback(None, MagicMock())
        self.assertEqual(self.sender._delivery_errors, [])

    def test_on_delivery_callback_error(self):
        self.sender._on_delivery_callback("some error", MagicMock())
        self.assertEqual(self.sender._delivery_errors, ["some error"])

    def test_flush_with_error(self):
        self.sender._delivery_errors = ["some error"]
        with self.assertRaises(RuntimeError) as error:
            self.sender.flush()

        self.assertIn("some error", str(error.exception))
        self.assertEqual(len(self.sender._delivery_errors), 0)

    def test_flush_not_sent_messages(self):
        self.sender._producer.flush.return_value = 2
        self.sender._delivery_errors = []
        with self.assertLogs(level="WARNING") as log_warning:
            self.sender.flush()

        self.assertIn("Number of not processed messages: 2", log_warning.output[0])
