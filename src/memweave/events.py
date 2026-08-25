"""Append-only event repository backed by a relational database adapter."""

import json
import time
from datetime import datetime
from typing import Any, Dict, Mapping, Optional
from uuid import UUID, uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError

from .clock import utc_now
from .db import Database
from .models import Event, EventType
from .storage.schema import events_table, stream_heads_table


def _json_default(value: Any) -> str:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    raise TypeError("payload contains a non-serializable value")


class EventStore:
    def __init__(self, database: Database, max_append_retries: int = 8):
        if max_append_retries < 0:
            raise ValueError("max_append_retries must not be negative")
        self.database = database
        self.max_append_retries = max_append_retries

    def append(
        self,
        stream_id: str,
        event_type: EventType | str,
        payload: Dict[str, Any],
        actor: str,
        request_id: UUID,
        event_id: Optional[UUID] = None,
        occurred_at: Optional[datetime] = None,
        causation_id: Optional[UUID] = None,
        correlation_id: Optional[UUID] = None,
        idempotency_key: Optional[str] = None,
    ) -> Event:
        if not isinstance(stream_id, str):
            raise TypeError("stream_id must be a string")
        if not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        if not isinstance(actor, str):
            raise TypeError("actor must be a string")
        if not actor.strip():
            raise ValueError("actor must not be blank")
        if not isinstance(request_id, UUID):
            raise TypeError("request_id must be a UUID")
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dictionary")
        if isinstance(event_type, EventType):
            event_type_value = event_type.value
        elif isinstance(event_type, str):
            if not event_type.strip():
                raise ValueError("event_type must not be blank")
            event_type_value = event_type
        else:
            raise TypeError("event_type must be a string or EventType")
        if idempotency_key is not None:
            if not isinstance(idempotency_key, str):
                raise TypeError("idempotency_key must be a string")
            if not idempotency_key.strip():
                raise ValueError("idempotency_key must not be blank")

        event_id = event_id or uuid4()
        occurred_at_was_provided = occurred_at is not None
        occurred_at = occurred_at or utc_now()
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default)
        immutable_values = {
            "stream_id": stream_id,
            "event_type": event_type_value,
            "payload_json": payload_json,
            "actor": actor,
            "request_id": str(request_id),
            "idempotency_key": idempotency_key,
            "causation_id": str(causation_id) if causation_id else None,
            "correlation_id": str(correlation_id) if correlation_id else None,
            "schema_version": 1,
            "protocol_version": "1.0",
        }
        if occurred_at_was_provided:
            immutable_values["occurred_at"] = occurred_at.isoformat()

        for attempt in range(self.max_append_retries + 1):
            try:
                return self._append_once(
                    stream_id=stream_id,
                    event_id=event_id,
                    event_type_value=event_type_value,
                    payload_json=payload_json,
                    actor=actor,
                    request_id=request_id,
                    occurred_at=occurred_at,
                    causation_id=causation_id,
                    correlation_id=correlation_id,
                    idempotency_key=idempotency_key,
                    immutable_values=immutable_values,
                )
            except (IntegrityError, OperationalError) as exc:
                if attempt >= self.max_append_retries or not self._is_retryable(exc):
                    raise
                time.sleep(0.005 * (2**attempt))

        raise AssertionError("unreachable")

    def _append_once(
        self,
        *,
        stream_id: str,
        event_id: UUID,
        event_type_value: str,
        payload_json: str,
        actor: str,
        request_id: UUID,
        occurred_at: datetime,
        causation_id: Optional[UUID],
        correlation_id: Optional[UUID],
        idempotency_key: Optional[str],
        immutable_values: Mapping[str, Any],
    ) -> Event:
        with self.database.begin() as connection:
            existing = connection.execute(
                select(events_table).where(events_table.c.event_id == str(event_id))
            ).mappings().first()
            if existing is not None:
                if not self._matches_existing(existing, immutable_values):
                    raise ValueError("event is immutable")
                return self._row_to_event(existing)

            if idempotency_key is not None:
                duplicate = connection.execute(
                    select(events_table).where(
                        events_table.c.stream_id == stream_id,
                        events_table.c.idempotency_key == idempotency_key,
                    )
                ).mappings().first()
                if duplicate is not None:
                    if not self._matches_existing(duplicate, immutable_values):
                        raise ValueError("idempotency key conflicts with existing event")
                    return self._row_to_event(duplicate)

            head = connection.execute(
                select(stream_heads_table.c.last_seq).where(
                    stream_heads_table.c.stream_id == stream_id
                )
            ).scalar_one_or_none()
            seq = (head or 0) + 1
            if head is None:
                connection.execute(
                    insert(stream_heads_table).values(stream_id=stream_id, last_seq=seq)
                )
            else:
                connection.execute(
                    update(stream_heads_table)
                    .where(stream_heads_table.c.stream_id == stream_id)
                    .values(last_seq=seq)
                )

            ingested_at = utc_now()
            connection.execute(
                insert(events_table).values(
                    event_id=str(event_id),
                    stream_id=stream_id,
                    seq=seq,
                    event_type=event_type_value,
                    actor=actor,
                    payload_json=payload_json,
                    schema_version=1,
                    protocol_version="1.0",
                    request_id=str(request_id),
                    idempotency_key=idempotency_key,
                    occurred_at=occurred_at.isoformat(),
                    ingested_at=ingested_at.isoformat(),
                    causation_id=str(causation_id) if causation_id else None,
                    correlation_id=str(correlation_id) if correlation_id else None,
                )
            )
            row = connection.execute(
                select(events_table).where(events_table.c.event_id == str(event_id))
            ).mappings().one()
            return self._row_to_event(row)

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        if isinstance(error, IntegrityError):
            return True
        if isinstance(error, OperationalError):
            message = str(error).lower()
            return any(
                marker in message
                for marker in (
                    "database is locked",
                    "deadlock",
                    "serialization failure",
                    "could not serialize",
                )
            )
        return False

    def list_after(self, stream_id: str, seq: int) -> list[Event]:
        self._validate_stream_id(stream_id)
        self._validate_seq(seq)
        with self.database.read() as connection:
            rows = connection.execute(
                select(events_table)
                .where(events_table.c.stream_id == stream_id, events_table.c.seq > seq)
                .order_by(events_table.c.seq)
            ).mappings().all()
        return [self._row_to_event(row) for row in rows]

    def last_seq(self, stream_id: str) -> int:
        self._validate_stream_id(stream_id)
        with self.database.read() as connection:
            value = connection.execute(
                select(stream_heads_table.c.last_seq).where(
                    stream_heads_table.c.stream_id == stream_id
                )
            ).scalar_one_or_none()
        return int(value or 0)

    @staticmethod
    def _validate_stream_id(stream_id: str) -> None:
        if not isinstance(stream_id, str):
            raise TypeError("stream_id must be a string")
        if not stream_id.strip():
            raise ValueError("stream_id must not be blank")

    @staticmethod
    def _validate_seq(seq: int) -> None:
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise TypeError("seq must be an integer")
        if seq < 0:
            raise ValueError("seq must not be negative")

    @staticmethod
    def _matches_existing(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
        return all(row[key] == value for key, value in values.items())

    @staticmethod
    def _row_to_event(row: Mapping[str, Any]) -> Event:
        return Event(
            event_id=UUID(row["event_id"]),
            event_type=row["event_type"],
            stream_id=row["stream_id"],
            seq=row["seq"],
            actor=row["actor"],
            payload=json.loads(row["payload_json"]),
            schema_version=row["schema_version"],
            protocol_version=row["protocol_version"],
            request_id=UUID(row["request_id"]),
            idempotency_key=row["idempotency_key"],
            occurred_at=datetime.fromisoformat(row["occurred_at"]),
            ingested_at=datetime.fromisoformat(row["ingested_at"]),
            causation_id=UUID(row["causation_id"]) if row["causation_id"] else None,
            correlation_id=UUID(row["correlation_id"]) if row["correlation_id"] else None,
        )
