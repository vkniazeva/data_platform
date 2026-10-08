import os
import signal
import threading
import time

import boto3
import clickhouse_connect
from dotenv import load_dotenv

from ingestion.loader.bronze_loader import BronzeLoader
from ingestion.raw.consumer import KafkaConsumer
from ingestion.raw.parser.fix_parser import parse_fix_event, return_fix_event_date
from ingestion.raw.parser.regional_parser import parse_regional_event, return_regional_event_date
from ingestion.raw.writer import ParquetWriter

import logging
logger = logging.getLogger(__name__)

load_dotenv()

stop_event = threading.Event()

WORKERS = [
    {
        "topic": "market.fix.raw",
        "group_id": "fix_event_consumer",
        "parser": parse_fix_event,
        "date_parser": return_fix_event_date,
        "loader_method": "load_fix_events",
    },
    {
        "topic": "market.events",
        "group_id": "regional_price_consumer",
        "parser": parse_regional_event,
        "date_parser": return_regional_event_date,
        "loader_method": "load_regional_events",
    },
]


def _make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
        aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD"),
    )


def _make_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USERNAME"),
        password=os.getenv("CLICKHOUSE_PASSWORD") or "",
    )


def run_worker(topic: str, group_id: str, parser, date_parser, loader_method: str) -> None:
    kafka_consumer = KafkaConsumer(topic=topic, group_id=group_id, bootstrap_servers="localhost:19092")
    kafka_consumer.subscribe_topic()

    writer = ParquetWriter(topic)
    bronze_loader = BronzeLoader(_make_s3_client(), _make_ch_client())
    load = getattr(bronze_loader, loader_method)

    msg_count = 0
    t_start = time.monotonic()

    for msg in kafka_consumer.consume_messages(stop_event):
        try:
            event = parser(msg.value())
            event_date = date_parser(event)
            flushed = writer.write_to_buffer(event, event_date, msg)
            if flushed:
                load(f"{topic}/{writer.current_date}.parquet")
                kafka_consumer.commit_message(writer._last_msg)
            msg_count += 1
            if msg_count % 100 == 0:
                elapsed = time.monotonic() - t_start
                logger.info(f"[{topic}] Throughput: {msg_count / elapsed:.1f} msg/sec ({msg_count} messages in {elapsed:.1f}s)")
            if stop_event.is_set():
                break
        except Exception as e:
            logger.error(f"[{topic}] Error processing message: {e}", exc_info=True)

    if writer.flush(writer.current_date):
        load(f"{topic}/{writer.current_date}.parquet")
        kafka_consumer.commit_message(writer._last_msg)
    kafka_consumer.close_consumer()


def worker(config: dict) -> None:
    while not stop_event.is_set():
        try:
            run_worker(**config)
        except Exception as e:
            logger.error(f"[{config['topic']}] Worker crashed: {e}", exc_info=True)
            logger.info(f"[{config['topic']}] Restarting in 5 seconds")
            time.sleep(5)


def handler(signum, frame):
    stop_event.set()


def main():
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGINT, handler)
    threads = [threading.Thread(target=worker, args=(cfg,)) for cfg in WORKERS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
