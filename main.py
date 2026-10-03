import random
from time import sleep

from generator.data_generator.api_generator import generate_stock_request
from generator.data_generator.event_generator import generate_kafka_event
from generator.sender.api_sender import ApiSender
from generator.sender.kafka_sender import KafkaSender

KAFKA_STATE = {
    "trade_execution_report": {"message_number": 1, "topic": "market.fix.raw"},
    "regional_price_quote": {"message_number": 1, "topic": "market.events"}
}

API_MESSAGE_NUMBER = 1


def send_next_event(kafka_sender: KafkaSender, event_type: str) -> None:
    event_config = KAFKA_STATE[event_type]
    message_number = event_config["message_number"]

    event = generate_kafka_event(event_type=event_type, message_number=message_number)

    duplicate_count = random.choices(population=[0, 1, 2], weights=[80, 15, 5])[0]

    for _ in range(duplicate_count+1):
        kafka_sender.send(topic=event_config["topic"], event=event)

    kafka_sender.flush()
    event_config["message_number"] += 1

def send_next_api_request(api_sender: ApiSender) -> None:
    global API_MESSAGE_NUMBER
    inventory_request = generate_stock_request(API_MESSAGE_NUMBER)

    duplicate_count = random.choices(population=[0, 1, 2], weights=[80, 15, 5])[0]

    for _ in range(duplicate_count+1):
        api_sender.send_inventory_snapshot(inventory_request)

    API_MESSAGE_NUMBER += 1



def main():
    kafka_sender = KafkaSender(bootstrap_servers="localhost:19092")
    api_sender = ApiSender(base_url="http://localhost:8000")

    while True:
        event_type = random.choice(["trade_execution_report", "regional_price_quote"])
        send_next_event(kafka_sender=kafka_sender, event_type=event_type)
        sleep(random.uniform(1, 5))

        send_next_api_request(api_sender)
        sleep(random.uniform(1, 5))


if __name__ == '__main__':
    main()


