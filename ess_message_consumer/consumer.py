import argparse
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from threading import Lock
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

from ess_message_consumer.console_output import NormalConsole, RichConsole
from ess_message_consumer.utils import get_logger, run_in_thread, validate_broker


class EssMessageConsumer:
    def __init__(self, broker: str, topics: List[str], logger, rich_console=False):
        validate_broker(broker)
        self._broker = broker
        self._topics = topics
        self.log = logger

        conf = {
            "bootstrap.servers": self._broker,
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
        self._messages = {topic: OrderedDict() for topic in self._topics}

        if rich_console:
            self._console = RichConsole(topics, self._messages)
        else:
            self._console = NormalConsole(topics, self._messages, logger)

    @property
    def console(self):
        return self._console

    @property
    def consumer(self):
        return self._consumer

    def subscribe(self):
        if not self._topics:
            self.log.error("Empty topic list")
            return
        # Remove all the subscribed topics
        self._consumer.unsubscribe()

        existing_topics = self._consumer.list_topics().topics
        for topic in self._topics:
            if topic not in existing_topics:
                raise RuntimeError(f"Provided topic {topic} does not exist")

        self._consumer.subscribe(self._topics)
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
                self.log.error(f"Error: {msg.error()}")
            else:
                value = msg.value()
                topic = msg.topic()
                type = value[4:8]
                if type in self._message_handler:
                    self._message_handler[type](topic, value)
                else:
                    self.log.error(
                        f"Unrecognized serialized type {type}: message: {value}"
                    )

    def _on_fw_finished_writing_message(self, topic, message):
        self._update_message_container(topic, deserialise_wrdn(message))

    def _on_fw_command_response_message(self, topic, message):
        self._update_message_container(topic, deserialise_answ(message))

    def _on_status_message(self, topic, message):
        self._update_message_container(topic, deserialise_x5f2(message))

    def _on_run_start_message(self, topic, message):
        self._update_message_container(topic, deserialise_pl72(message))

    def _on_run_stop_message(self, topic, message):
        self._update_message_container(topic, deserialise_6s4t(message))

    def _on_log_data(self, topic, message):
        self._update_message_container(topic, deserialise_f142(message))

    def _on_histogram_data(self, topic, message):
        self._update_message_container(topic, deserialise_hs00(message))

    def _on_event_data(self, topic, message):
        self._update_message_container(topic, deserialise_ev42(message))

    def _update_message_container(self, topic, value):
        with self._msg_lock:
            key = datetime.now().ctime()
            self._messages[topic][key] = value
            # print("Hello ", self._messages)

    def _update_console(self):
        self._console.update_console()


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
    parser.add_argument(
        "--rich_console", action="store_true", help="To get rich layout"
    )

    args = parser.parse_args()

    topics = [x.strip() for x in args.topics.split(",") if x.strip()]
    broker = args.broker
    rich_console = args.rich_console

    logger = get_logger("file-writer-messages", rich_console)

    consumer = EssMessageConsumer(broker, topics, logger, rich_console=rich_console)
    consumer.subscribe()


if __name__ == "__main__":
    start_consumer()
