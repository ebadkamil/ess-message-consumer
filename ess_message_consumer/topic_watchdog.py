import time
import uuid
from logging import Logger

from confluent_kafka import Consumer

from ess_message_consumer.utils import run_in_thread


class TopicWatchDog:
    def __init__(self, broker: str, logger: Logger):
        self._broker = broker
        self._logger = logger
        self._existing_topics = set()

        try:
            conf = {
                "bootstrap.servers": self._broker,
                "auto.offset.reset": "latest",
                "group.id": uuid.uuid4(),
            }
            self._consumer = Consumer(conf)
        except Exception as error:
            self._logger.error(f"Unable to create consumers: {error}")
            raise

    @run_in_thread
    def track_topics(self):
        while True:
            self._existing_topics.update(
                list(self._consumer.list_topics().topics.keys())
            )
            time.sleep(2)

    @property
    def existing_topics(self):
        return self._existing_topics
