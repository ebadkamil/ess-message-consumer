from datetime import datetime
from typing import List

from rich import box
from rich.align import Align
from rich.console import RenderGroup
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree


class TopicConsoleRenderer:
    def __init__(self, topic, container):
        self._container = container
        self._topic = topic
        self._last_timestamp = None
        self._last_value = None

    def __rich__(self) -> Panel:
        self._table = Table(
            expand=True,
            header_style="bold blue",
            border_style="white",
            highlight=True,
            style="white",
        )
        self._table.add_column("Timestamp", justify="left")
        self._table.add_column("Message", justify="left")

        if self._container:
            items = self._container.popitem(last=False)
            self._last_timestamp, self._last_value = (
                str(datetime.fromtimestamp(items[0])),
                str(items[1]),
            )

        if self._last_timestamp and self._last_value:
            self._table.add_row(self._last_timestamp, self._last_value)

        return Panel(
            Align.center(
                RenderGroup(self._table),
                vertical="middle",
            ),
            box=box.ROUNDED,
            padding=(1, 2),
            title=f"[b blue] Messages from Topic: {self._topic}",
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


class TopicsTreeRenderer:
    def __init__(self, topics):
        self._topics = topics

    def __rich__(self) -> Panel:
        tree = Tree(
            "",
            guide_style="bright_blue",
        )
        if self._topics:
            for topic in self._topics["topics"]:
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
    def __init__(self, topics, message_container, existing_topics):
        self._layout = self.make_layout(topics)
        self._layout["header"].update(Header())
        self._layout["topics_tree"].update(TopicsTreeRenderer(existing_topics))
        for topic in topics:
            self._layout[topic].update(
                TopicConsoleRenderer(topic, message_container[topic])
            )

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
    def __init__(self, topics, message_container, logger):
        self._message_container = message_container
        self._log = logger

    def update_console(self):
        while True:
            if self._message_container:
                for key in self._message_container:
                    container = self._message_container[key]
                    if container:
                        items = container.popitem(last=False)
                        self._log.error(items[1])
