import time
import uuid
from collections import OrderedDict
from getpass import getuser
from logging import Logger
from typing import Dict, List

from confluent_kafka import Consumer  # type: ignore

from ess_message_consumer.console_output import NormalConsole, RichConsole
from ess_message_consumer.deserializer import DeserializerFactory
from ess_message_consumer.utils import cli_parser, get_logger, run_in_thread


class EssMessageConsumer:
    def __init__(
        self, broker: str, topics: List[str], logger: Logger, rich_console: bool = False
    ):
        self._broker = broker
        self._topics = topics
        self._logger = logger

        self._message_buffer: Dict[str, OrderedDict] = {
            topic: OrderedDict() for topic in self._topics
        }
        self._existing_topics: List[str] = []

        self._consumers = {}
        try:
            for topic in self._topics:
                conf = {
                    "bootstrap.servers": self._broker,
                    "auto.offset.reset": "latest",
                    "group.id": uuid.uuid4(),
                }
                self._consumers[topic] = Consumer(conf)
        except Exception as error:
            self._logger.error(f"Unable to create consumers: {error}")
            raise

        if rich_console:
            self._console = RichConsole(
                topics, self._message_buffer, self._existing_topics
            )  # type: ignore
        else:
            self._console = NormalConsole(self._message_buffer, logger)  # type: ignore

    @property
    def consumers(self):
        return self._consumers.values()

    @property
    def console(self):
        return self._console

    @property
    def message_buffer(self):
        return self._message_buffer

    def subscribe(self):
        if not self._topics:
            self._logger.error("Empty topic list")
            return

        for topic, consumer in self._consumers.items():
            # Remove all the subscribed topics
            consumer.unsubscribe()
            existing_topics = consumer.list_topics().topics
            self._existing_topics.extend(list(existing_topics.keys()))

            if topic not in existing_topics:
                self._logger.error(
                    f"Provided topic {topic} does not exist. \n"
                    f"Available topics are {list(existing_topics.keys())}"
                )
                consumer.close()
                return

            consumer.subscribe([topic])
            self._consume(topic)
        self._update_console()

    @run_in_thread
    def _consume(self, topic: str):
        while True:
            time.sleep(1)
            msg = self._consumers[topic].poll(1)
            if msg is None:
                continue
            if msg.error():
                self._logger.error(f"Error: {msg.error()}")
            else:
                value = msg.value()
                topic = msg.topic()
                type = value[4:8]
                try:
                    deserialized_message = DeserializerFactory.from_serialized_type(
                        type
                    ).deserialize(value)
                    self._update_message_buffer(topic, deserialized_message)
                except Exception as error:
                    self._logger.error(
                        f"Unrecognized serialized type {type}: message: {value}"
                        f"\nError:{error}"
                    )

    def _update_message_buffer(self, topic, value):
        self._message_buffer[topic][time.time()] = value

    def _update_console(self):
        self._console.update_console()


def start_consumer():
    args = cli_parser()

    topics = [x.strip() for x in args.topics.split(",") if x.strip()]
    broker = args.broker
    rich_console = args.rich_console

    logger = get_logger("ess-message-consumer", rich_console)

    ess_msg_consumer = EssMessageConsumer(
        broker, topics, logger, rich_console=rich_console
    )

    try:
        ess_msg_consumer.subscribe()
    except KeyboardInterrupt:
        logger.info(f"Interrupted by user: {getuser()}. Closing consumers ...")
    finally:
        for consumer in ess_msg_consumer.consumers:
            consumer.close()


if __name__ == "__main__":
    start_consumer()
