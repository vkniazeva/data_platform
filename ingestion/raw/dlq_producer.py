from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)


class DlqProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "dlq-producer",
        })

    def send(self, msg, reason: str) -> None:
        dlq_topic = f"{msg.topic()}.dlq"
        headers = list(msg.headers() or []) + [("dlq_reason", reason)]
        self._producer.produce(
            topic=dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
        )
        self._producer.poll(0)
        logger.warning(f"Message sent to DLQ {dlq_topic}: {reason}")
