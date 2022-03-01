import time
import uuid
from logging import Logger
from threading import Thread

from confluent_kafka import Consumer


class TopicWatchDog:
    def __init__(self, broker: str, logger: Logger = None):
        self._broker = broker
        self._logger = logger.error if logger else print
        self._stop = False
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
        self._watch_thread = Thread(target=self._update_exisiting_topics)

    def track_topics(self):
        self._watch_thread.start()

    def _update_exisiting_topics(self):
        while not self._stop:
            existing_topics = self.get_available_topics()
            self._existing_topics.update(existing_topics)
            time.sleep(5)

    def get_available_topics(self):
        return list(self._consumer.list_topics().topics.keys())

    @property
    def existing_topics(self):
        return self._existing_topics

    def close(self):
        self._stop = True
        if self._watch_thread and self._watch_thread.is_alive():
            self._watch_thread.join()
