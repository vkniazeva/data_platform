import signal
import threading
import time

from dotenv import load_dotenv

from ingestion.raw.consumer import KafkaConsumer
from ingestion.raw.parser.fix_parser import parse_fix_event, return_fix_event_date
from ingestion.raw.parser.regional_parser import return_regional_event_date, parse_regional_event
from ingestion.raw.writer import ParquetWriter

import logging
logger = logging.getLogger(__name__)

load_dotenv()

stop_event = threading.Event()

def worker_fix():
    topic = "market.fix.raw"
    group_id = "fix_event_consumer"

    while not stop_event.is_set():
        try:
            create_worker(topic, group_id, parse_fix_event, return_fix_event_date)
        except Exception as e:
            logger.error(f"Worker {topic} crashed: {e}", exc_info=True)
            logger.info(f"Restarting worker {topic} in 5 seconds")
            time.sleep(5)

def worker_regional():
    topic = "market.events"
    group_id = "regional_price_consumer"

    while not stop_event.is_set():
        try:
            create_worker(topic, group_id, parse_regional_event ,return_regional_event_date)
        except Exception as e:
            logger.error(f"Worker {topic} crashed: {e}", exc_info=True)
            logger.info(f"Restarting worker {topic} in 5 seconds")
            time.sleep(5)

def create_worker(topic: str, group_id: str, parser, date_parser) -> None:
    bootstrap_servers = "localhost:19092"

    kafka_consumer = KafkaConsumer(topic=topic, group_id=group_id, bootstrap_servers=bootstrap_servers)
    kafka_consumer.subscribe_topic()
    logger.info('subscribed')

    writer = ParquetWriter(topic)

    msg_count = 0
    t_start = time.monotonic()

    for msg in kafka_consumer.consume_messages(stop_event):
        logger.debug('started processing message')
        try:
            t0 = time.monotonic()
            event = parser(msg.value())
            elapsed = time.monotonic() - t0
            logger.debug(f"Message parsed in {elapsed * 1000:.1f} ms")
            event_date = date_parser(event)
            flushed = writer.write_to_buffer(event, event_date, msg)
            logger.debug('writing to buffer')
            if flushed:
                kafka_consumer.commit_message(writer._last_msg)
            msg_count += 1
            if msg_count % 100 == 0:
                elapsed = time.monotonic() - t_start
                logger.info(f"Throughput: {msg_count / elapsed:.1f} msg/sec ({msg_count} messages in {elapsed:.1f}s)")
            if stop_event.is_set():
                break
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    if writer.flush(writer.current_date):
        kafka_consumer.commit_message(writer._last_msg)
    kafka_consumer.close_consumer()


def handler(signum, frame):
    stop_event.set()

def main():
    logging.basicConfig(level=logging.INFO)
    t1 = threading.Thread(target=worker_fix)
    t2 = threading.Thread(target=worker_regional)
    t1.start()
    t2.start()
    signal.signal(signal.SIGINT, handler)
    t1.join()
    t2.join()




if __name__ == "__main__":
    main()