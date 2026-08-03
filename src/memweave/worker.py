"""Local in-process worker for transactional outbox items."""

from typing import Callable, Mapping, Optional

from .outbox import OutboxItem, OutboxStore


OutboxHandler = Callable[[OutboxItem], None]


class LocalWorker:
    def __init__(
        self,
        outbox: OutboxStore,
        handlers: Mapping[str, OutboxHandler],
    ):
        self.outbox = outbox
        self.handlers = dict(handlers)

    def run_once(self, topic: Optional[str] = None) -> int:
        item = self.outbox.claim(topic=topic)
        if item is None:
            return 0
        handler = self.handlers.get(item.topic)
        if handler is None:
            raise KeyError(f"no handler registered for topic: {item.topic}")
        handler(item)
        self.outbox.mark_applied(item.id)
        return 1
