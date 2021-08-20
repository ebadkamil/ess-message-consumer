import json
import time
from getpass import getuser
from logging import Logger

from confluent_kafka import Producer

from ess_message_consumer.utils import broker_cli, run_in_thread


class EssMessageProducer:
    def __init__(self, broker: str, logger: Logger = None):
        self._broker = broker
        self._producer = Producer({"bootstrap.servers": self._broker})

        self._logger = logger.info if logger else print
        self._stop = False
        self._poller_t = self._start_polling_producer()

    @run_in_thread
    def _start_polling_producer(self):
        while not self._stop:
            self._producer.poll(0.5)

    def close(self):
        self._stop = True
        self._poller_t.join()
        self._producer.flush(2)

    def produce(self, topic: str, payload: bytes, timestamp: int):
        def ack(err, _):
            if err:
                self._logger(f"Message failed delivery: {err} \n")

        try:
            self._producer.produce(topic, payload, on_delivery=ack, timestamp=timestamp)
        except Exception as error:
            self._logger(f"Message failed delivery: {error} \n")
        self._producer.poll(0)


def _get_topic_and_message():
    topic = input("Topic name: ")
    message = input("Message to send: ")
    return topic, message


def _to_exit():
    exit = False
    if input("Exit? (y/n): ").lower() in ["yes", "y"]:
        exit = True
    return exit


def start_producer():
    args = broker_cli()
    broker = args.broker

    ess_message_producer = EssMessageProducer(broker)
    try:
        while True:
            topic, message = _get_topic_and_message()
            ess_message_producer.produce(topic, json.dumps(message), int(time.time()))
            if _to_exit():
                ess_message_producer.close()
                break
    except KeyboardInterrupt:
        print(f"Interrupted by user: {getuser()}. Closing producer ...")
    finally:
        ess_message_producer.close()


if __name__ == "__main__":
    start_producer()
