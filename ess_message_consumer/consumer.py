import time
import uuid
from logging import Logger
from threading import Thread
from typing import Dict, List

from confluent_kafka import Consumer  # type: ignore

from ess_message_consumer.deserializer import DeserializerFactory
from ess_message_consumer.utils import BoundOrderedDict


class EssMessageConsumer:
    def __init__(self, broker: str, topics: List[str], logger: Logger):
        self._broker = broker
        self._topics = topics
        self._logger = logger

        self._stop = False

        self._message_buffer: Dict[str, BoundOrderedDict] = {
            topic: BoundOrderedDict(maxlen=50) for topic in self._topics
        }

        self._consumers = {}
        self._consumer_threads = {}
        try:
            for topic in self._topics:
                conf = {
                    "bootstrap.servers": self._broker,
                    "auto.offset.reset": "latest",
                    "group.id": uuid.uuid4(),
                }
                self._consumers[topic] = Consumer(conf)
                self._consumer_threads[topic] = Thread(
                    target=self._consume, args=(topic,)
                )
        except Exception as error:
            self._logger.error(f"Unable to create consumers: {error}")
            raise

    @property
    def consumers(self):
        return self._consumers.values()

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

            if topic not in existing_topics:
                self._logger.error(
                    f"Provided topic {topic} does not exist. \n"
                    f"Available topics are {list(existing_topics.keys())}"
                )
                consumer.close()
                continue

            consumer.subscribe([topic])
            self._consumer_threads[topic].start()

    def _consume(self, topic: str):
        while not self._stop:
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

    def close(self):
        self._stop = True
        for _, thread in self._consumer_threads.items():
            if thread and thread.is_alive():
                thread.join()

        for _, consumer in self._consumers.items():
            consumer.close()
