"""Local in-process worker for transactional outbox items."""

from datetime import datetime, timedelta
from typing import Callable, Mapping, Optional

from .clock import utc_now
from .outbox import ConsumerClaimResult, OutboxItem, OutboxStore


OutboxHandler = Callable[[OutboxItem], None]


class LocalWorker:
    def __init__(
        self,
        outbox: OutboxStore,
        handlers: Mapping[str, OutboxHandler],
        max_attempts: int = 5,
        base_delay_seconds: int = 1,
        max_delay_seconds: int = 300,
        consumer_id: str = "local-worker",
        clock: Callable[[], datetime] = utc_now,
    ):
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool):
            raise TypeError("max_attempts must be an integer")
        if not isinstance(base_delay_seconds, (int, float)) or isinstance(base_delay_seconds, bool):
            raise TypeError("base_delay_seconds must be numeric")
        if not isinstance(max_delay_seconds, (int, float)) or isinstance(max_delay_seconds, bool):
            raise TypeError("max_delay_seconds must be numeric")
        if not isinstance(consumer_id, str):
            raise TypeError("consumer_id must be a string")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must not be negative")
        if max_delay_seconds < base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be blank")
        self.outbox = outbox
        self.handlers = dict(handlers)
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.consumer_id = consumer_id
        self.clock = clock

    def run_once(self, topic: Optional[str] = None) -> int:
        item = self.outbox.claim(topic=topic)
        if item is None:
            return 0
        handler = self.handlers.get(item.topic)
        if handler is None:
            error = f"no handler registered for topic: {item.topic}"
            if item.attempts >= self.max_attempts:
                self.outbox.mark_dead_letter(item.id, error, item.lease_token)
            else:
                delay = min(
                    self.base_delay_seconds * (2 ** (item.attempts - 1)),
                    self.max_delay_seconds,
                )
                self.outbox.mark_retryable(
                    item.id,
                    error,
                    item.lease_token,
                    available_at=self.clock() + timedelta(seconds=delay),
                )
            return 1
        claim_result = self.outbox.begin_consume(
            item.id, self.consumer_id, item.lease_token
        )
        if claim_result is ConsumerClaimResult.ALREADY_APPLIED:
            self.outbox.mark_applied(item.id, item.lease_token)
            return 1
        if claim_result is ConsumerClaimResult.BUSY:
            return 1
        try:
            handler(item)
        except Exception as exc:
            self.outbox.release_consume(item.id, self.consumer_id, item.lease_token)
            if item.attempts >= self.max_attempts:
                self.outbox.mark_dead_letter(
                    item.id,
                    str(exc) or exc.__class__.__name__,
                    item.lease_token,
                )
            else:
                delay = min(
                    self.base_delay_seconds * (2 ** (item.attempts - 1)),
                    self.max_delay_seconds,
                )
                self.outbox.mark_retryable(
                    item.id,
                    str(exc) or exc.__class__.__name__,
                    item.lease_token,
                    available_at=self.clock() + timedelta(seconds=delay),
                )
            return 1
        self.outbox.mark_consumed(item.id, self.consumer_id, item.lease_token)
        self.outbox.mark_applied(item.id, item.lease_token)
        return 1
