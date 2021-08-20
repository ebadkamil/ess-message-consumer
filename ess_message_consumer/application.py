from getpass import getuser
from logging import Logger
from typing import List

from ess_message_consumer.console_output import NormalConsole, RichConsole
from ess_message_consumer.consumer import EssMessageConsumer
from ess_message_consumer.topic_watchdog import TopicWatchDog
from ess_message_consumer.utils import broker_cli, cli_parser, get_logger


class Application:
    def __init__(
        self, broker: str, topics: List[str], logger: Logger, rich_console: bool = False
    ):

        self._ess_message_consumer = EssMessageConsumer(broker, topics, logger)
        message_buffer = self._ess_message_consumer.message_buffer

        self._topic_watchdog = TopicWatchDog(broker, logger)
        existing_topics = self._topic_watchdog.existing_topics

        if rich_console:
            self._console = RichConsole(
                topics, message_buffer, existing_topics
            )  # type: ignore
        else:
            self._console = NormalConsole(message_buffer, logger)  # type: ignore

    def start(self):
        self._topic_watchdog.track_topics()
        self._ess_message_consumer.subscribe()
        self._console.update_console()

    def stop(self):
        for consumer in self._ess_message_consumer.consumers:
            consumer.close()


def start_application():
    args = cli_parser()

    topics = [x.strip() for x in args.topics.split(",") if x.strip()]
    broker = args.broker
    rich_console = args.rich_console

    logger = get_logger("ess-message-consumer", rich_console)

    app = Application(broker, topics, logger, rich_console=rich_console)

    try:
        app.start()
    except KeyboardInterrupt:
        logger.info(f"Interrupted by user: {getuser()}. Closing consumers ...")
    finally:
        app.stop()


def list_available_topics():
    args = broker_cli()
    broker = args.broker

    topic_watchdog = TopicWatchDog(broker)
    topics = topic_watchdog.get_available_topics()
    to_print = ""
    for index, value in enumerate(topics):
        to_print += f"  {index} : {value}\n"
    print(to_print)


if __name__ == "__main__":
    start_application()
