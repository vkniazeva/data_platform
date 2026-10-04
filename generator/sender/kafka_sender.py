import json

from confluent_kafka import Producer

import logging
logger = logging.getLogger(__name__)

class KafkaSender:
    def __init__(self, bootstrap_servers: str):

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "metals-data-generator",
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self._delivery_errors = []

    def _on_delivery_callback(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors.append(str(err))
            logger.error(f"Message is delivered with an error: {err}")
        else:
            logger.debug(f"Message delivered to partition {msg.partition()}")

    def send(self, topic: str, event: dict) -> None:
        value = event["value"]

        if isinstance(value, dict):
            value = json.dumps(value)

        self._producer.produce(
            topic=topic,
            key=event["key"],
            value=value,
            headers=event["headers"],
            callback=self._on_delivery_callback
        )

        self._producer.poll(0)

    def flush(self) -> None:
        unprocessed_messages = self._producer.flush()
        if unprocessed_messages > 0:
            logger.warning(f"Number of not processed messages: {unprocessed_messages}")

        if self._delivery_errors:
            error_str =  ", ".join(self._delivery_errors)
            self._delivery_errors.clear()
            raise RuntimeError(f"Kafka delivery failed with errors: {error_str}")



