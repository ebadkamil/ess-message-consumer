import argparse
import logging
import time
import uuid
from typing import List

from confluent_kafka import Consumer
from streaming_data_types import (
    deserialise_6s4t,
    deserialise_answ,
    deserialise_ev42,
    deserialise_f142,
    deserialise_hs00,
    deserialise_pl72,
    deserialise_wrdn,
    deserialise_x5f2,
)


def get_logger(name, level: int = logging.DEBUG):
    logger = logging.getLogger(name)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.setLevel(level)
    return logger


def validate_broker(url):
    if ":" not in url:
        raise RuntimeError(
            f"Unable to parse URL {url}, should be of form localhost:9092"
        )


logger = get_logger("file-writer-messages")


class FWMessageConsumer:
    def __init__(self, broker: str):
        validate_broker(broker)
        self.broker = broker
        conf = {
            "bootstrap.servers": self.broker,
            "auto.offset.reset": "latest",
            "group.id": uuid.uuid4(),
        }
        self._consumer = Consumer(conf)

        self._message_handler = {
            b"x5f2": self._on_status_message,
            b"answ": self._on_response_message,
            b"wrdn": self._on_stopped_message,
            b"6s4t": self._on_stop_message,
            b"pl72": self._on_start_message,
            b"f142": self._on_log_data,
            b"ev42": self._on_event_data,
            b"hs00": self._on_histogram_data,
        }

    @property
    def consumer(self):
        return self._consumer

    def subscribe(self, topics: List[str]):
        if not topics:
            logger.error("Empty topic list")
            return
        # Remove all the subscribed topics
        self._consumer.unsubscribe()

        existing_topics = self._consumer.list_topics().topics
        for topic in topics:
            if topic not in existing_topics:
                raise RuntimeError(f"Provided topic {topic} does not exist")

        self._consumer.subscribe(topics)

        self._consume()

    def _consume(self):
        while True:
            time.sleep(1)
            msg = self._consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Error: {msg.error()}")
            else:
                value = msg.value()
                if value[4:8] in self._message_handler:
                    self._message_handler[value[4:8]](value)
                else:
                    logger.error(f"Unrecognized serialized type message: {value}")

    def _on_stopped_message(self, message):
        logger.debug(deserialise_wrdn(message))

    def _on_response_message(self, message):
        logger.debug(deserialise_answ(message))

    def _on_status_message(self, message):
        logger.debug(deserialise_x5f2(message))

    def _on_start_message(self, message):
        logger.debug(deserialise_pl72(message))

    def _on_stop_message(self, message):
        logger.debug(deserialise_6s4t(message))

    def _on_log_data(self, message):
        logger.debug(deserialise_f142(message))

    def _on_histogram_data(self, message):
        logger.debug(deserialise_hs00(message))

    def _on_event_data(self, message):
        logger.debug(deserialise_ev42(message))


def start_consumer():
    parser = argparse.ArgumentParser(prog="FileWriter Message consumer")
    parser.add_argument(
        "-t",
        "--topics",
        required=True,
        type=str,
        help="List of topics to consume messages from",
    )

    parser.add_argument(
        "-b",
        "--broker",
        type=str,
        default="localhost:9092",
        help="Kafka broker address",
    )
    args = parser.parse_args()

    topics = [x.strip() for x in args.topics.split(",") if x.strip()]
    broker = args.broker
    consumer = FWMessageConsumer(broker)
    consumer.subscribe(topics)


if __name__ == "__main__":
    start_consumer
