import argparse
import logging
import time
from collections import OrderedDict
from functools import wraps
from threading import Thread

from confluent_kafka import Producer  # type: ignore
from confluent_kafka.admin import AdminClient  # type: ignore
from rich.logging import RichHandler


class BoundOrderedDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        self.maxlen = kwargs.pop("maxlen", None)
        super().__init__(*args, **kwargs)
        self._maintain_length()

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._maintain_length()

    def _maintain_length(self):
        if self.maxlen is not None:
            while len(self) > self.maxlen:
                self.popitem(last=False)


class Profiler:
    """Profiler class
    Usage:
        with Profiler(label="some name"):
            do_something()
    """

    def __init__(self, label="", enabled=True):
        self._label = label
        self._enabled = enabled
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        if self._enabled:
            duration = time.perf_counter() - self._start
            print(f"Time for Execution of {self._label}: {duration * 1000:.4f} ms.")


def run_in_thread(original):
    @wraps(original)
    def wrapper(*args, **kwargs):
        t = Thread(target=original, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    return wrapper


def get_logger(
    name: str, level: int = logging.DEBUG, rich_console: bool = False
) -> logging.Logger:
    logger = logging.getLogger(name)
    if rich_console:
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
    else:
        console_handler = RichHandler(show_level=False, show_path=False)

    logger.addHandler(console_handler)
    logger.setLevel(level)

    return logger


def check_kafka_connection(broker_url: str):
    kafka_ready = False
    msg = None
    if ":" not in broker_url:
        msg = f"Unable to parse URL {broker_url}, should be of form localhost:9092"
        return kafka_ready, msg

    conf = {"bootstrap.servers": broker_url}
    try:
        producer = Producer(conf)
    except Exception as error:
        msg = error
        return kafka_ready, msg

    def delivery_callback(err, msg):
        nonlocal kafka_ready
        if not err:
            kafka_ready = True

    n_polls = 0
    while n_polls < 5 and not kafka_ready:
        producer.produce(
            "__waitUntilUp", value="Check if up", on_delivery=delivery_callback
        )
        producer.poll(6)
        n_polls += 1

    msg = (
        "Kafka up .."
        if kafka_ready
        else f"Cannot connect to broker {broker_url} in 30 secs."
    )

    if kafka_ready:
        admin = AdminClient(conf)
        futures = admin.delete_topics(["__waitUntilUp"])
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as error:
                msg = f"failed to delete topic {topic}: {error}"

    return kafka_ready, msg


def cli_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ESS Message consumer")
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

    return parser.parse_args()


def broker_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ESS Message consumer")
    parser.add_argument(
        "-b",
        "--broker",
        type=str,
        default="localhost:9092",
        help="Kafka broker address",
    )
    return parser.parse_args()
