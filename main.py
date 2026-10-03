from generator.data_generator.event_generator import generate_kafka_event
from generator.sender.kafka_sender import KafkaSender

KAFKA_STATE = {
    "trade_execution_report": {"message_number": 1, "topic": "market.fix.raw"},
    "regional_price_quote": {"message_number": 1, "topic": "market.events"}
}


def send_next_event(kafka_sender: KafkaSender, event_type: str) -> None:
    event_config = KAFKA_STATE[event_type]
    message_number = event_config["message_number"]

    event = generate_kafka_event(event_type=event_type, message_number=message_number)
    kafka_sender.send(topic=event_config["topic"], event=event)

    kafka_sender.flush()
    event_config["message_number"] += 1

def main():
    kafka_sender = KafkaSender(bootstrap_servers="localhost:19092")

    send_next_event(kafka_sender=kafka_sender, event_type="trade_execution_report")

    send_next_event(kafka_sender=kafka_sender, event_type="regional_price_quote")

if __name__ == '__main__':
    main()


