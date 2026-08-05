"""Transactional outbox task storage."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from sqlalchemy import and_, insert, or_, select, update

from .clock import utc_now
from .storage.schema import outbox_table


class OutboxStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRYABLE = "retryable"
    APPLIED = "applied"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class OutboxItem:
    id: UUID
    event_id: UUID
    topic: str
    payload: Dict[str, Any]
    idempotency_key: str
    status: OutboxStatus
    attempts: int
    available_at: datetime
    locked_at: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime


class OutboxStore:
    def __init__(
        self,
        database,
        lease_seconds: int = 300,
        clock: Callable[[], datetime] = utc_now,
    ):
        if lease_seconds < 0:
            raise ValueError("lease_seconds must not be negative")
        self.database = database
        self.lease_seconds = lease_seconds
        self.clock = clock

    def enqueue(
        self,
        event_id: UUID,
        topic: str,
        payload: Dict[str, Any],
        idempotency_key: str,
    ) -> OutboxItem:
        if not topic.strip():
            raise ValueError("topic must not be blank")
        if not idempotency_key.strip():
            raise ValueError("idempotency_key must not be blank")
        if not isinstance(event_id, UUID):
            raise TypeError("event_id must be a UUID")
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        now = self.clock()
        item_id = uuid4()
        values = {
            "event_id": str(event_id),
            "topic": topic,
            "payload_json": payload_json,
            "idempotency_key": idempotency_key,
        }
        with self.database.begin() as connection:
            existing = connection.execute(
                select(outbox_table).where(
                    outbox_table.c.idempotency_key == idempotency_key
                )
            ).mappings().first()
            if existing is not None:
                if any(existing[key] != value for key, value in values.items()):
                    raise ValueError("idempotency key conflicts with existing outbox item")
                return self._row_to_item(existing)
            connection.execute(
                insert(outbox_table).values(
                    id=str(item_id),
                    **values,
                    status=OutboxStatus.PENDING.value,
                    attempts=0,
                    available_at=now.isoformat(),
                    locked_at=None,
                    last_error=None,
                    created_at=now.isoformat(),
                    updated_at=now.isoformat(),
                )
            )
            row = connection.execute(
                select(outbox_table).where(outbox_table.c.id == str(item_id))
            ).mappings().one()
            return self._row_to_item(row)

    def get(self, item_id: UUID) -> OutboxItem:
        with self.database.read() as connection:
            row = connection.execute(
                select(outbox_table).where(outbox_table.c.id == str(item_id))
            ).mappings().first()
        if row is None:
            raise KeyError(item_id)
        return self._row_to_item(row)

    def claim(self, topic: Optional[str] = None) -> Optional[OutboxItem]:
        now = self.clock()
        cutoff = (now - timedelta(seconds=self.lease_seconds)).isoformat()
        available = [
            outbox_table.c.status == OutboxStatus.PENDING.value,
            outbox_table.c.status == OutboxStatus.RETRYABLE.value,
        ]
        ready = and_(
            or_(*available),
            outbox_table.c.available_at <= now.isoformat(),
        )
        expired_processing = (
            (outbox_table.c.status == OutboxStatus.PROCESSING.value)
            & (outbox_table.c.locked_at <= cutoff)
        )
        conditions = [or_(ready, expired_processing)]
        if topic is not None:
            conditions.append(outbox_table.c.topic == topic)

        with self.database.begin() as connection:
            row = connection.execute(
                select(outbox_table)
                .where(*conditions)
                .order_by(outbox_table.c.created_at, outbox_table.c.id)
                .limit(1)
                .with_for_update()
            ).mappings().first()
            if row is None:
                return None
            claimed_at = now.isoformat()
            connection.execute(
                update(outbox_table)
                .where(outbox_table.c.id == row["id"])
                .values(
                    status=OutboxStatus.PROCESSING.value,
                    attempts=row["attempts"] + 1,
                    locked_at=claimed_at,
                    updated_at=claimed_at,
                )
            )
            claimed = connection.execute(
                select(outbox_table).where(outbox_table.c.id == row["id"])
            ).mappings().one()
            return self._row_to_item(claimed)

    def mark_applied(self, item_id: UUID) -> None:
        self._transition(item_id, OutboxStatus.APPLIED, last_error=None, locked_at=None)

    def mark_retryable(
        self,
        item_id: UUID,
        error: str,
        available_at: Optional[datetime] = None,
    ) -> None:
        if not error.strip():
            raise ValueError("error must not be blank")
        self._transition(
            item_id,
            OutboxStatus.RETRYABLE,
            last_error=error,
            locked_at=None,
            available_at=(available_at or self.clock()).isoformat(),
        )

    def mark_dead_letter(self, item_id: UUID, error: str) -> None:
        if not error.strip():
            raise ValueError("error must not be blank")
        self._transition(
            item_id,
            OutboxStatus.DEAD_LETTER,
            last_error=error,
            locked_at=None,
        )

    def _transition(self, item_id: UUID, status: OutboxStatus, **values: Any) -> None:
        now = self.clock().isoformat()
        with self.database.begin() as connection:
            result = connection.execute(
                update(outbox_table)
                .where(
                    outbox_table.c.id == str(item_id),
                    outbox_table.c.status == OutboxStatus.PROCESSING.value,
                )
                .values(status=status.value, updated_at=now, **values)
            )
            if result.rowcount != 1:
                raise ValueError("outbox item is missing or not processing")

    @staticmethod
    def _row_to_item(row) -> OutboxItem:
        return OutboxItem(
            id=UUID(row["id"]),
            event_id=UUID(row["event_id"]),
            topic=row["topic"],
            payload=json.loads(row["payload_json"]),
            idempotency_key=row["idempotency_key"],
            status=OutboxStatus(row["status"]),
            attempts=row["attempts"],
            available_at=datetime.fromisoformat(row["available_at"]),
            locked_at=datetime.fromisoformat(row["locked_at"]) if row["locked_at"] else None,
            last_error=row["last_error"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
