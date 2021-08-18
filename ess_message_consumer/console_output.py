from collections import OrderedDict
from datetime import datetime
from logging import Logger
from typing import Dict, List, Optional

from rich import box
from rich.align import Align
from rich.console import RenderGroup
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree


class MessagePanel:
    def __init__(self, topic: str, message_buffer: "OrderedDict[float, str]"):
        self._message_buffer = message_buffer
        self._topic = topic
        self._last_timestamp: Optional[str] = None
        self._last_value: Optional[str] = None

    def __rich__(self) -> Panel:
        table = Table(
            expand=True,
            header_style="bold blue",
            border_style="white",
            highlight=True,
            style="white",
        )
        table.add_column("Timestamp", justify="left")
        table.add_column("Message", justify="left")

        if self._message_buffer:
            items = self._message_buffer.popitem(last=False)
            self._last_timestamp, self._last_value = (
                str(datetime.fromtimestamp(items[0])),
                str(items[1]),
            )

        if self._last_timestamp and self._last_value:
            table.add_row(self._last_timestamp, self._last_value)

        return Panel(
            Align.center(
                RenderGroup(table),
                vertical="middle",
            ),
            box=box.ROUNDED,
            padding=(1, 2),
            title=f"[b blue] Messages from Topic: {self._topic}",
            border_style="bright_blue",
        )


class Header:
    def __rich__(self) -> Panel:
        table = Table.grid(expand=True)
        table.add_column(justify="center", ratio=1)
        table.add_column(justify="left")
        table.add_row(
            "[b]ESS console-message-consumer[/b]",
            datetime.now().ctime().replace(":", "[blink]:[/]"),
        )
        return Panel(table, style="white on blue")


class TopicsTreePanel:
    def __init__(self, existing_topics: List[str]):
        self._existing_topics = existing_topics

    def __rich__(self) -> Panel:
        tree = Tree(
            "Topics",
            guide_style="bright_blue",
            highlight=True,
            style="blue",
        )
        if self._existing_topics:
            for topic in self._existing_topics:
                icon = "🔐 " if topic.startswith("__") else "📁 "
                tree.add(Text(icon) + f" {topic}")

        return Panel(
            Align.center(
                RenderGroup(tree),
                vertical="middle",
            ),
            title="[b blue] All available topics",
            border_style="bright_blue",
            box=box.ROUNDED,
            padding=(1, 2),
        )


class RichConsole:
    def __init__(
        self,
        topics: List[str],
        message_buffer: Dict[str, OrderedDict],
        existing_topics: List[str],
    ):
        self._layout = self.make_layout(topics)
        self._layout["header"].update(Header())
        self._layout["topics_tree"].update(TopicsTreePanel(existing_topics))
        for topic in topics:
            self._layout[topic].update(MessagePanel(topic, message_buffer[topic]))

    def update_console(self):
        with Live(self._layout, screen=True, refresh_per_second=1):
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
        layout["main"].split_row(
            Layout(name="topics_tree"),
            Layout(name="updates", ratio=2, minimum_size=60),
        )
        args = [Layout(name=topic) for topic in topics]
        layout["updates"].split(*args)
        return layout


class NormalConsole:
    def __init__(self, message_buffer: Dict[str, OrderedDict], logger: Logger):
        self._message_buffer = message_buffer
        self._logger = logger

    def update_console(self):
        while True:
            if self._message_buffer:
                for _, buffer in self._message_buffer.items():
                    if buffer:
                        items = buffer.popitem(last=False)
                        self._logger.error(items[1])
