import time
import uuid
from logging import Logger

from confluent_kafka import Consumer

from ess_message_consumer.utils import run_in_thread


class TopicWatchDog:
    def __init__(self, broker: str, logger: Logger = None):
        self._broker = broker
        self._logger = logger.error if logger else print
        self._existing_topics = set()

        try:
            conf = {
                "bootstrap.servers": self._broker,
                "auto.offset.reset": "latest",
                "group.id": uuid.uuid4(),
            }
            self._consumer = Consumer(conf)
        except Exception as error:
            msg = f"Unable to create consumers: {error}"
            self._logger(msg)
            raise

    @run_in_thread
    def track_topics(self):
        while True:
            existing_topics = self.get_available_topics()
            self._existing_topics.update(existing_topics)
            time.sleep(5)

    def get_available_topics(self):
        return list(self._consumer.list_topics().topics.keys())

    @property
    def existing_topics(self):
        return self._existing_topics
