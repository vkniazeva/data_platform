from confluent_kafka import Consumer

import logging

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(self, topic, group_id, bootstrap_servers):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self._consumer = Consumer( {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False
        })

    def consume_messages(self, stop_event):
        while not stop_event.is_set():
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                raise RuntimeError(f"Message with a key: {msg.key()} return an error: {msg.error()}")

            yield msg

    def commit_message(self, msg):
        self._consumer.commit(message=msg)
        logger.debug(f"Message has been successfully committed to a partition: {msg.partition()} with offset: {msg.offset()}")

    def _on_assign(self, consumer, partitions):
        logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")

    def subscribe_topic(self):
        self._consumer.subscribe(topics=[self.topic], on_assign=self._on_assign)

    def close_consumer(self):
        self._consumer.close()
