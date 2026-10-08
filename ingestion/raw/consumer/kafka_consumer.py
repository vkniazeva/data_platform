from confluent_kafka import Consumer

import logging

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(self, topic, group_id, bootstrap_servers, schema_registry=None, dlq_producer=None, requires_schema=False):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self._schema_registry = schema_registry
        self._dlq_producer = dlq_producer
        self._requires_schema = requires_schema
        self._consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "log_level": 0
        })

    def consume_messages(self, stop_event):
        while not stop_event.is_set():
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                raise RuntimeError(f"Message with a key: {msg.key()} return an error: {msg.error()}")

            if not self._validate(msg):
                continue

            yield msg

    def _validate(self, msg) -> bool:
        schema_id = self._get_schema_id(msg)

        if schema_id is None:
            if self._requires_schema:
                logger.warning(f"[{self.topic}] Message missing schema_id — sending to DLQ")
                if self._dlq_producer:
                    self._dlq_producer.send(msg, reason="missing_schema_id")
                return False
            return True

        if self._schema_registry:
            if not self._schema_registry.schema_exists(int(schema_id)):
                logger.warning(f"[{self.topic}] Unknown schema_id {schema_id} — sending to DLQ")
                if self._dlq_producer:
                    self._dlq_producer.send(msg, reason=f"unknown_schema_id:{schema_id}")
                return False

        logger.debug(f"Message schema_id: {schema_id}, topic: {self.topic}, partition: {msg.partition()}")
        return True

    def _get_schema_id(self, msg) -> str | None:
        if msg.headers() is None:
            return None
        for key, value in msg.headers():
            if key == "schema_id":
                return value.decode("utf-8")
        return None

    def commit_message(self, msg):
        self._consumer.commit(message=msg)
        logger.debug(f"Message has been successfully committed to a partition: {msg.partition()} with offset: {msg.offset()}")

    def _on_assign(self, consumer, partitions):
        logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")

    def subscribe_topic(self):
        self._consumer.subscribe(topics=[self.topic], on_assign=self._on_assign)

    def close_consumer(self):
        self._consumer.close()
