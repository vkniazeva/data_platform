import json

from confluent_kafka import Producer

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

    def send(self, topic: str, event: dict) -> None:
        value = event["value"]

        if isinstance(value, dict):
            value = json.dumps(value)

        self._producer.produce(
            topic=topic,
            key=event["key"],
            value=value,
            headers=event["headers"],
        )

        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()



