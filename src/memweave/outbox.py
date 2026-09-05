"""Transactional outbox task storage."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from sqlalchemy import and_, insert, or_, select, update
from sqlalchemy.exc import IntegrityError

from .clock import utc_now
from .storage.schema import outbox_consumer_receipts_table, outbox_table


class OutboxStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRYABLE = "retryable"
    APPLIED = "applied"
    DEAD_LETTER = "dead_letter"


class ConsumerReceiptStatus(str, Enum):
    PROCESSING = "processing"
    APPLIED = "applied"


class ConsumerClaimResult(str, Enum):
    ACQUIRED = "acquired"
    ALREADY_APPLIED = "already_applied"
    BUSY = "busy"


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
    lease_token: Optional[UUID]
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
        if not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool):
            raise TypeError("lease_seconds must be an integer")
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
        if not isinstance(topic, str):
            raise TypeError("topic must be a string")
        if not isinstance(idempotency_key, str):
            raise TypeError("idempotency_key must be a string")
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dictionary")
        if not topic.strip():
            raise ValueError("topic must not be blank")
        if not idempotency_key.strip():
            raise ValueError("idempotency_key must not be blank")
        if not isinstance(event_id, UUID):
            raise TypeError("event_id must be a UUID")
        payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        now = self.clock()
        item_id = uuid4()
        values = {
            "event_id": str(event_id),
            "topic": topic,
            "payload_json": payload_json,
            "idempotency_key": idempotency_key,
        }
        try:
            with self.database.begin() as connection:
                existing = connection.execute(
                    select(outbox_table).where(
                        outbox_table.c.idempotency_key == idempotency_key
                    )
                ).mappings().first()
                if existing is not None:
                    return self._validate_existing_enqueue(existing, values)
                connection.execute(
                    insert(outbox_table).values(
                        id=str(item_id),
                        **values,
                        status=OutboxStatus.PENDING.value,
                        attempts=0,
                        available_at=now.isoformat(),
                        locked_at=None,
                        lease_token=None,
                        last_error=None,
                        created_at=now.isoformat(),
                        updated_at=now.isoformat(),
                    )
                )
                row = connection.execute(
                    select(outbox_table).where(outbox_table.c.id == str(item_id))
                ).mappings().one()
                return self._row_to_item(row)
        except IntegrityError:
            # A concurrent writer may win the unique idempotency-key race after
            # our pre-check. Re-read after rollback and apply the same immutable
            # request validation; unrelated integrity errors remain visible.
            with self.database.read() as connection:
                existing = connection.execute(
                    select(outbox_table).where(
                        outbox_table.c.idempotency_key == idempotency_key
                    )
                ).mappings().first()
            if existing is None:
                raise
            return self._validate_existing_enqueue(existing, values)

    def get(self, item_id: UUID) -> OutboxItem:
        self._validate_item_id(item_id)
        with self.database.read() as connection:
            row = connection.execute(
                select(outbox_table).where(outbox_table.c.id == str(item_id))
            ).mappings().first()
        if row is None:
            raise KeyError(item_id)
        return self._row_to_item(row)

    def claim(self, topic: Optional[str] = None) -> Optional[OutboxItem]:
        if topic is not None and not isinstance(topic, str):
            raise TypeError("topic must be a string")
        if topic is not None and not topic.strip():
            raise ValueError("topic must not be blank")
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
            & or_(
                outbox_table.c.locked_at.is_(None),
                outbox_table.c.locked_at <= cutoff,
            )
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
            lease_token = uuid4()
            result = connection.execute(
                update(outbox_table)
                .where(
                    outbox_table.c.id == row["id"],
                    or_(ready, expired_processing),
                )
                .values(
                    status=OutboxStatus.PROCESSING.value,
                    attempts=row["attempts"] + 1,
                    locked_at=claimed_at,
                    lease_token=str(lease_token),
                    updated_at=claimed_at,
                )
            )
            if result.rowcount != 1:
                return None
            claimed = connection.execute(
                select(outbox_table).where(outbox_table.c.id == row["id"])
            ).mappings().one()
            return self._row_to_item(claimed)

    def mark_applied(self, item_id: UUID, lease_token: UUID) -> None:
        self._transition(
            item_id,
            lease_token,
            OutboxStatus.APPLIED,
            last_error=None,
            locked_at=None,
            lease_token=None,
        )

    def mark_retryable(
        self,
        item_id: UUID,
        error: str,
        lease_token: UUID,
        available_at: Optional[datetime] = None,
    ) -> None:
        self._validate_item_id(item_id)
        if not isinstance(error, str):
            raise TypeError("error must be a string")
        if not error.strip():
            raise ValueError("error must not be blank")
        retry_at = self._normalize_datetime(
            available_at if available_at is not None else self.clock(),
            "available_at",
        )
        self._transition(
            item_id,
            lease_token,
            OutboxStatus.RETRYABLE,
            last_error=error,
            locked_at=None,
            lease_token=None,
            available_at=retry_at.isoformat(),
        )

    def mark_dead_letter(self, item_id: UUID, error: str, lease_token: UUID) -> None:
        self._validate_item_id(item_id)
        if not isinstance(error, str):
            raise TypeError("error must be a string")
        if not error.strip():
            raise ValueError("error must not be blank")
        self._transition(
            item_id,
            lease_token,
            OutboxStatus.DEAD_LETTER,
            last_error=error,
            locked_at=None,
            lease_token=None,
        )

    def begin_consume(
        self, item_id: UUID, consumer_id: str, lease_token: UUID
    ) -> ConsumerClaimResult:
        """Reserve a delivery and distinguish completed, busy, and new work."""
        self._validate_item_id(item_id)
        if not isinstance(consumer_id, str):
            raise TypeError("consumer_id must be a string")
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be blank")
        self._validate_lease_token(lease_token)
        now = self.clock()
        cutoff = (now - timedelta(seconds=self.lease_seconds)).isoformat()
        try:
            with self.database.begin() as connection:
                item = connection.execute(
                    select(outbox_table).where(
                        outbox_table.c.id == str(item_id),
                        outbox_table.c.status == OutboxStatus.PROCESSING.value,
                        outbox_table.c.lease_token == str(lease_token),
                    )
                ).mappings().first()
                if item is None:
                    return ConsumerClaimResult.BUSY
                receipt = connection.execute(
                    select(outbox_consumer_receipts_table)
                    .where(
                        outbox_consumer_receipts_table.c.consumer_id == consumer_id,
                        outbox_consumer_receipts_table.c.idempotency_key
                        == item["idempotency_key"],
                    )
                    .with_for_update()
                ).mappings().first()
                if receipt is not None:
                    return self._claim_existing_receipt(
                        connection, receipt, lease_token, now, cutoff
                    )
                connection.execute(
                    insert(outbox_consumer_receipts_table).values(
                        id=str(uuid4()),
                        outbox_id=str(item_id),
                        consumer_id=consumer_id,
                        idempotency_key=item["idempotency_key"],
                        status=ConsumerReceiptStatus.PROCESSING.value,
                        locked_at=now.isoformat(),
                        lease_token=str(lease_token),
                        consumed_at=None,
                        created_at=now.isoformat(),
                        updated_at=now.isoformat(),
                    )
                )
                return ConsumerClaimResult.ACQUIRED
        except IntegrityError:
            # Two consumers can pass the receipt pre-check before either insert
            # commits. Re-read the winner after rollback and preserve normal
            # BUSY/ALREADY_APPLIED semantics; unrelated integrity failures stay
            # visible to the caller.
            with self.database.read() as connection:
                item = connection.execute(
                    select(outbox_table.c.idempotency_key).where(
                        outbox_table.c.id == str(item_id)
                    )
                ).mappings().first()
                if item is None:
                    raise
                receipt = connection.execute(
                    select(outbox_consumer_receipts_table)
                    .where(
                        outbox_consumer_receipts_table.c.consumer_id == consumer_id,
                        outbox_consumer_receipts_table.c.idempotency_key
                        == item["idempotency_key"],
                    )
                ).mappings().first()
            if receipt is None:
                raise
            if receipt["status"] == ConsumerReceiptStatus.APPLIED.value:
                return ConsumerClaimResult.ALREADY_APPLIED
            if receipt["status"] != ConsumerReceiptStatus.PROCESSING.value:
                raise ValueError(
                    f"unknown consumer receipt status: {receipt['status']!r}"
                )
            if (
                receipt["status"] == ConsumerReceiptStatus.PROCESSING.value
                and receipt["locked_at"]
                and receipt["locked_at"] > cutoff
            ):
                return ConsumerClaimResult.BUSY
            return ConsumerClaimResult.BUSY

    def _claim_existing_receipt(
        self, connection, receipt, lease_token: UUID, now: datetime, cutoff: str
    ) -> ConsumerClaimResult:
        if receipt["status"] == ConsumerReceiptStatus.APPLIED.value:
            return ConsumerClaimResult.ALREADY_APPLIED
        if receipt["status"] != ConsumerReceiptStatus.PROCESSING.value:
            raise ValueError(
                f"unknown consumer receipt status: {receipt['status']!r}"
            )
        if (
            receipt["status"] == ConsumerReceiptStatus.PROCESSING.value
            and receipt["locked_at"]
            and receipt["locked_at"] > cutoff
        ):
            return ConsumerClaimResult.BUSY
        connection.execute(
            update(outbox_consumer_receipts_table)
            .where(outbox_consumer_receipts_table.c.id == receipt["id"])
            .values(
                status=ConsumerReceiptStatus.PROCESSING.value,
                locked_at=now.isoformat(),
                lease_token=str(lease_token),
                updated_at=now.isoformat(),
            )
        )
        return ConsumerClaimResult.ACQUIRED

    def mark_consumed(self, item_id: UUID, consumer_id: str, lease_token: UUID) -> None:
        self._validate_item_id(item_id)
        if not isinstance(consumer_id, str):
            raise TypeError("consumer_id must be a string")
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be blank")
        self._validate_lease_token(lease_token)
        now = self.clock().isoformat()
        with self.database.begin() as connection:
            item = connection.execute(
                select(outbox_table).where(
                    outbox_table.c.id == str(item_id),
                    outbox_table.c.status == OutboxStatus.PROCESSING.value,
                    outbox_table.c.lease_token == str(lease_token),
                )
            ).mappings().first()
            if item is None:
                raise ValueError("outbox item is missing or not processing")
            result = connection.execute(
                update(outbox_consumer_receipts_table)
                .where(
                    outbox_consumer_receipts_table.c.outbox_id == str(item_id),
                    outbox_consumer_receipts_table.c.consumer_id == consumer_id,
                    outbox_consumer_receipts_table.c.status
                    == ConsumerReceiptStatus.PROCESSING.value,
                    outbox_consumer_receipts_table.c.lease_token
                    == str(lease_token),
                )
                .values(
                    status=ConsumerReceiptStatus.APPLIED.value,
                    locked_at=None,
                    consumed_at=now,
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                raise ValueError("consumer receipt is missing or not processing")

    def release_consume(self, item_id: UUID, consumer_id: str, lease_token: UUID) -> None:
        self._validate_item_id(item_id)
        if not isinstance(consumer_id, str):
            raise TypeError("consumer_id must be a string")
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be blank")
        self._validate_lease_token(lease_token)
        with self.database.begin() as connection:
            item = connection.execute(
                select(outbox_table.c.id).where(
                    outbox_table.c.id == str(item_id),
                    outbox_table.c.status == OutboxStatus.PROCESSING.value,
                    outbox_table.c.lease_token == str(lease_token),
                )
            ).first()
            if item is None:
                raise ValueError("outbox item is missing or not processing")
            connection.execute(
                update(outbox_consumer_receipts_table)
                .where(
                    outbox_consumer_receipts_table.c.outbox_id == str(item_id),
                    outbox_consumer_receipts_table.c.consumer_id == consumer_id,
                    outbox_consumer_receipts_table.c.status
                    == ConsumerReceiptStatus.PROCESSING.value,
                    outbox_consumer_receipts_table.c.lease_token
                    == str(lease_token),
                )
                .values(
                    locked_at=None,
                    updated_at=self.clock().isoformat(),
                )
            )

    def _transition(
        self,
        item_id: UUID,
        fencing_token: UUID,
        status: OutboxStatus,
        **values: Any,
    ) -> None:
        self._validate_item_id(item_id)
        self._validate_lease_token(fencing_token)
        now = self.clock().isoformat()
        with self.database.begin() as connection:
            result = connection.execute(
                update(outbox_table)
                .where(
                    outbox_table.c.id == str(item_id),
                    outbox_table.c.status == OutboxStatus.PROCESSING.value,
                    outbox_table.c.lease_token == str(fencing_token),
                )
                .values(status=status.value, updated_at=now, **values)
            )
            if result.rowcount != 1:
                raise ValueError("outbox item is missing or not processing")

    @staticmethod
    def _validate_item_id(item_id: UUID) -> None:
        if not isinstance(item_id, UUID):
            raise TypeError("item_id must be a UUID")

    @staticmethod
    def _validate_existing_enqueue(existing, values):
        if any(existing[key] != value for key, value in values.items()):
            raise ValueError("idempotency key conflicts with existing outbox item")
        return OutboxStore._row_to_item(existing)

    @staticmethod
    def _validate_lease_token(lease_token: UUID) -> None:
        if not isinstance(lease_token, UUID):
            raise TypeError("lease_token must be a UUID")

    @staticmethod
    def _normalize_datetime(value: datetime, name: str) -> datetime:
        if not isinstance(value, datetime):
            raise TypeError(f"{name} must be a datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        return value.astimezone(timezone.utc)

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
            lease_token=UUID(row["lease_token"]) if row["lease_token"] else None,
            last_error=row["last_error"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
