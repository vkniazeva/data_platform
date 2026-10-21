import os
import signal
import subprocess
import threading
import time

import boto3
import clickhouse_connect
from dotenv import load_dotenv

from config import TOPIC_FIX_RAW, TOPIC_EVENTS, GROUP_FIX, GROUP_EVENTS
from ingestion.loader.bronze_loader import BronzeLoader
from ingestion.raw.consumer import KafkaConsumer
from ingestion.raw.dlq_producer import DlqProducer
from ingestion.raw.parser.fix_parser import parse_fix_event, return_fix_event_date
from ingestion.raw.parser.regional_parser import parse_regional_event, return_regional_event_date
from ingestion.raw.schema_registry import SchemaRegistryClient
from ingestion.raw.writer import ParquetWriter
from ingestion.raw.writer.quarantine_writer import QuarantineWriter
from ingestion.validation.content_validator import ContentValidator
from ingestion.validation.header_validator import HeaderValidator

import logging
logger = logging.getLogger(__name__)

load_dotenv()

stop_event = threading.Event()

_schema_registry = SchemaRegistryClient(url=os.getenv("SCHEMA_REGISTRY_URL"))
_dlq_producer = DlqProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), '..', 'transforms')

WORKERS = [
    {
        "topic": TOPIC_FIX_RAW,
        "group_id": GROUP_FIX,
        "parser": parse_fix_event,
        "date_parser": return_fix_event_date,
        "loader_method": "load_fix_events",
        "schema_registry": _schema_registry,
        "dlq_producer": _dlq_producer,
        "requires_schema": True,
    },
    {
        "topic": TOPIC_EVENTS,
        "group_id": GROUP_EVENTS,
        "parser": parse_regional_event,
        "date_parser": return_regional_event_date,
        "loader_method": "load_regional_events",
        "schema_registry": _schema_registry,
        "dlq_producer": _dlq_producer,
        "requires_schema": True,
    },
]


def _make_s3_client():
    """Build a boto3 S3 client pointed at SeaweedFS using env config."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
        aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD"),
    )


def _make_ch_client():
    """Build a ClickHouse client using env config."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USERNAME"),
        password=os.getenv("CLICKHOUSE_PASSWORD") or "",
    )


def run_worker(topic: str, group_id: str, parser, date_parser, loader_method: str,
               schema_registry=None, dlq_producer=None, requires_schema: bool = False) -> None:
    """Consume one Kafka topic: validate → parse → buffer → S3 → ClickHouse → commit offset."""
    kafka_consumer = KafkaConsumer(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        schema_registry=schema_registry,
        dlq_producer=dlq_producer,
        requires_schema=requires_schema,
    )
    kafka_consumer.subscribe_topic()

    writer = ParquetWriter(topic)
    quarantine_writer = QuarantineWriter(topic)
    bronze_loader = BronzeLoader(_make_s3_client(), _make_ch_client())
    load = getattr(bronze_loader, loader_method)
    header_validator = HeaderValidator()
    content_validator = ContentValidator()

    msg_count = 0
    t_start = time.monotonic()

    for msg in kafka_consumer.consume_messages(stop_event):
        try:
            headers = {k: v.decode("utf-8") for k, v in (msg.headers() or [])}
            kafka_key = msg.key().decode("utf-8") if msg.key() else ""
            result = header_validator.validate(topic, headers, kafka_key)
            if not result.valid:
                logger.warning(f"[{topic}] Header validation failed: {result.reason} — sending to DLQ")
                dlq_producer.send(msg, reason=result.reason)
                continue

            event = parser(msg.value())
            event_date = date_parser(event)

            content_result = content_validator.validate(topic, event)
            if not content_result.valid:
                logger.warning(f"[{topic}] Content validation failed: {content_result.reason} — quarantine + DLQ")
                quarantine_writer.write(event, event_date, content_result.reason, msg)
                dlq_producer.send(msg, reason=f"invalid_content:{content_result.reason}")
                continue

            flushed_key = writer.write_to_buffer(event, event_date, msg)
            if flushed_key:
                load(flushed_key)
                kafka_consumer.commit_message(writer._last_msg)
            msg_count += 1
            if msg_count % 100 == 0:
                elapsed = time.monotonic() - t_start
                logger.info(f"[{topic}] Throughput: {msg_count / elapsed:.1f} msg/sec ({msg_count} messages in {elapsed:.1f}s)")
            if stop_event.is_set():
                break
        except Exception as e:
            logger.error(f"[{topic}] Error processing message: {e}", exc_info=True)

    quarantine_writer.flush_remaining()
    final_key = writer.flush(writer.current_date)
    if final_key:
        load(final_key)
        kafka_consumer.commit_message(writer._last_msg)
    kafka_consumer.close_consumer()


def worker(config: dict) -> None:
    """Wrap run_worker with auto-restart on crash (5-second delay between retries)."""
    while not stop_event.is_set():
        try:
            run_worker(**config)
        except Exception as e:
            logger.error(f"[{config['topic']}] Worker crashed: {e}", exc_info=True)
            logger.info(f"[{config['topic']}] Restarting in 5 seconds")
            time.sleep(5)


def handler(signum, frame):
    """Handle SIGINT/SIGTERM by setting the stop event for graceful shutdown."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    stop_event.set()

def dbt_executor():
    """Run dbt transforms every 30 seconds until the stop event is set."""
    while not stop_event.is_set():
        stop_event.wait(30)
        if stop_event.is_set():
            break
        logger.info("[dbt] running transforms...")
        try:
            subprocess.run(["dbt", "run"], cwd=DBT_PROJECT_DIR, check=True)
            logger.info("[dbt] done")
        except subprocess.CalledProcessError as e:
            logger.error(f"[dbt] failed: {e}")


def main():
    """Start worker threads (one per topic) and dbt executor as daemon threads."""
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    threads = [threading.Thread(target=worker, daemon=True, args=(cfg,)) for cfg in WORKERS]
    threads.append(threading.Thread(target=dbt_executor, daemon=True))
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
