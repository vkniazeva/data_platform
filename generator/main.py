import logging
import os
import random
from time import sleep

from dotenv import load_dotenv

from config import TOPIC_FIX_RAW, TOPIC_EVENTS, SCHEMA_IDS
from generator.data_generator.api_generator import generate_stock_request
from generator.data_generator.event_generator import generate_kafka_event
from generator.error_injector import ErrorInjector
from generator.sender.api_sender import ApiSender
from generator.sender.kafka_sender import KafkaSender

load_dotenv()

KAFKA_STATE = {
    "trade_execution_report": {"message_number": 1, "topic": TOPIC_FIX_RAW},
    "regional_price_quote":   {"message_number": 1, "topic": TOPIC_EVENTS},
}

API_MESSAGE_NUMBER = 1


_error_injector = ErrorInjector()
_ERROR_RATE = 0.05


def send_next_event(kafka_sender: KafkaSender, event_type: str) -> None:
    event_config = KAFKA_STATE[event_type]
    message_number = event_config["message_number"]
    topic = event_config["topic"]

    event = generate_kafka_event(event_type=event_type, message_number=message_number)

    duplicate_count = random.choices(population=[0, 1, 2], weights=[80, 15, 5])[0]

    outgoing = _error_injector.inject(event, topic) if random.random() < _ERROR_RATE else event
    for _ in range(duplicate_count + 1):
        kafka_sender.send(topic=topic, event=outgoing, schema_id=SCHEMA_IDS.get(topic))

    event_config["message_number"] += 1

def send_next_api_request(api_sender: ApiSender) -> None:
    global API_MESSAGE_NUMBER
    inventory_request = generate_stock_request(API_MESSAGE_NUMBER)

    duplicate_count = random.choices(population=[0, 1, 2], weights=[80, 15, 5])[0]

    for _ in range(duplicate_count+1):
        api_sender.send_inventory_snapshot(inventory_request)

    API_MESSAGE_NUMBER += 1



def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    kafka_sender = KafkaSender(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    # api_sender = ApiSender(base_url="http://localhost:8000")

    count = 0
    while True:
        event_type = random.choice(["trade_execution_report", "regional_price_quote"])
        send_next_event(kafka_sender=kafka_sender, event_type=event_type)
        count += 1
        if count % 100 == 0:
            kafka_sender.flush()
        sleep(random.uniform(0.01, 0.05))

        # send_next_api_request(api_sender)
        # sleep(random.uniform(1, 5))



if __name__ == '__main__':
    main()


