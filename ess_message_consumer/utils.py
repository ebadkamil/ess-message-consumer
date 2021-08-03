import logging
from functools import wraps
from threading import Thread

from rich.logging import RichHandler


def run_in_thread(original):
    @wraps(original)
    def wrapper(*args, **kwargs):
        t = Thread(target=original, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    return wrapper


def get_logger(name: str, level: int = logging.DEBUG, rich_console: bool = False):
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


def validate_broker(url: str):
    if ":" not in url:
        raise RuntimeError(
            f"Unable to parse URL {url}, should be of form localhost:9092"
        )
