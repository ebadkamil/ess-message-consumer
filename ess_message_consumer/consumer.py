import argparse
import logging
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from functools import wraps
from threading import Lock, Thread
from typing import List

from confluent_kafka import Consumer
from rich.logging import RichHandler
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

from ess_message_consumer.console_output import Console


def get_logger(name: str, level: int = logging.DEBUG):
    logger = logging.getLogger(name)
    console_handler = RichHandler(show_level=False, show_path=False)

    logger.addHandler(console_handler)
    logger.setLevel(level)

    return logger


def validate_broker(url: str):
    if ":" not in url:
        raise RuntimeError(
            f"Unable to parse URL {url}, should be of form localhost:9092"
        )


logger = get_logger("file-writer-messages")


def run_in_thread(original):
    @wraps(original)
    def wrapper(*args, **kwargs):
        t = Thread(target=original, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    return wrapper


class EssMessageConsumer:
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
            b"answ": self._on_fw_command_response_message,
            b"wrdn": self._on_fw_finished_writing_message,
            b"6s4t": self._on_run_stop_message,
            b"pl72": self._on_run_start_message,
            b"f142": self._on_log_data,
            b"ev42": self._on_event_data,
            b"hs00": self._on_histogram_data,
        }
        self._msg_lock = Lock()

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

        self._messages = {topic: OrderedDict() for topic in topics}
        self.console = Console(topics, self._messages)
        self._consumer.subscribe(topics)
        self._consume()
        self._update_console()

    @run_in_thread
    def _consume(self):
        while True:
            time.sleep(1)
            msg = self._consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                logger.error(f"Error: {msg.error()}")
            else:
                value = msg.value()
                topic = msg.topic()
                type = value[4:8]
                if type in self._message_handler:
                    self._message_handler[type](topic, value)
                else:
                    logger.error(
                        f"Unrecognized serialized type {type}: message: {value}"
                    )

    def _on_fw_finished_writing_message(self, topic, message):
        self._update_message_table(topic, deserialise_wrdn(message))

    def _on_fw_command_response_message(self, topic, message):
        self._update_message_table(topic, deserialise_answ(message))

    def _on_status_message(self, topic, message):
        self._update_message_table(topic, deserialise_x5f2(message))

    def _on_run_start_message(self, topic, message):
        self._update_message_table(topic, deserialise_pl72(message))

    def _on_run_stop_message(self, topic, message):
        self._update_message_table(topic, deserialise_6s4t(message))

    def _on_log_data(self, topic, message):
        self._update_message_table(topic, deserialise_f142(message))

    def _on_histogram_data(self, topic, message):
        self._update_message_table(topic, deserialise_hs00(message))

    def _on_event_data(self, topic, message):
        self._update_message_table(topic, deserialise_ev42(message))

    def _update_message_table(self, topic, value):
        with self._msg_lock:
            key = datetime.now().ctime()
            self._messages[topic][str(key)] = str(value)

    def _update_console(self):
        self.console.update_console()


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
    consumer = EssMessageConsumer(broker)
    consumer.subscribe(topics)


if __name__ == "__main__":
    start_consumer()
