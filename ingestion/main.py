import threading

from dotenv import load_dotenv

from ingestion.consumer.kafka_consumer import KafkaConsumer
from ingestion.parser.fix_parser import parse_fix_event, return_fix_event_date
from ingestion.parser.regional_parser import return_regional_event_date, parse_regional_event
from ingestion.writer.parquet_writer import ParquetWriter

import logging
logger = logging.getLogger(__name__)

load_dotenv()


def worker_fix():
    topic = "market.fix.raw"
    group_id = "fix_event_consumer"
    create_worker(topic, group_id, parse_fix_event, return_fix_event_date)

def worker_regional():
    topic = "market.events"
    group_id = "regional_price_consumer"
    create_worker(topic, group_id, parse_regional_event ,return_regional_event_date)

def create_worker(topic: str, group_id: str, parser, date_parser) -> None:
    bootstrap_servers = "localhost:19092"

    kafka_consumer = KafkaConsumer(topic=topic, group_id=group_id, bootstrap_servers=bootstrap_servers)
    kafka_consumer.subscribe_topic()
    logger.info('subscribed')

    writer = ParquetWriter(topic)

    for msg in kafka_consumer.consume_messages():
        logger.info('started processing message')
        try:
            event = parser(msg.value())
            event_date = date_parser(event)
            writer.write_to_buffer(event, event_date)
            logger.info('writing to buffer')
            kafka_consumer.commit_message(msg)
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)



def main():
    logging.basicConfig(level=logging.INFO)
    t1 = threading.Thread(target=worker_fix)
    t2 = threading.Thread(target=worker_regional)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

if __name__ == "__main__":
    main()