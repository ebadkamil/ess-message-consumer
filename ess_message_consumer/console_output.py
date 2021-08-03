import time
from datetime import datetime
from typing import List

from rich import box
from rich.align import Align
from rich.console import RenderGroup
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


class TopicConsoleRenderer:
    def __init__(self, topic, container):
        self._container = container
        self._topic = topic

        self._table = Table(expand=True)
        self._table.add_column("Timestamp", style="green", justify="center")
        self._table.add_column("Message", style="blue", justify="left")

    def __rich__(self) -> Panel:
        if self._container:
            items = self._container.popitem(last=False)
            timestamp, value = str(items[0]), str(items[1])
            self._table.add_row(timestamp, value)

        return Panel(
            Align.center(
                RenderGroup(self._table),
                vertical="middle",
            ),
            box=box.ROUNDED,
            padding=(1, 2),
            title=f"[b red] Messages from Topic: {self._topic}",
            border_style="bright_blue",
        )


class Header:
    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="left")
        grid.add_row(
            "[b]ESS console-message-consumer[/b]",
            datetime.now().ctime().replace(":", "[blink]:[/]"),
        )
        return Panel(grid, style="white on blue")


class RichConsole:
    def __init__(self, topics, message_container):
        self._layout = self.make_layout(topics)
        self._layout["header"].update(Header())
        for topic in topics:
            self._layout[topic].update(
                TopicConsoleRenderer(topic, message_container[topic])
            )

    def update_console(self):
        with Live(self._layout, refresh_per_second=1, screen=True):
            while True:
                pass

    @staticmethod
    def make_layout(topics: List[str]) -> Layout:
        """Define the layout."""
        layout = Layout(name="root")

        layout.split(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
        )
        args = [Layout(name=topic) for topic in topics]
        layout["main"].split(*args)
        return layout


class NormalConsole:
    def __init__(self, topics, message_container, logger):
        self._message_container = message_container
        self._log = logger

    def update_console(self):
        while True:
            time.sleep(1)
            if self._message_container:
                for key in self._message_container:
                    container = self._message_container[key]
                    if container:
                        items = container.popitem(last=False)
                        self._log.error(items[1])
