"""Append-only event repository backed by a relational database adapter."""

import json
from datetime import datetime
from typing import Any, Dict, Mapping, Optional
from uuid import UUID, uuid4

from .clock import utc_now
from .db import Database
from .models import Event, EventType


def _json_default(value: Any) -> str:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    raise TypeError("payload contains a non-serializable value")


class EventStore:
    def __init__(self, database: Database):
        self.database = database

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
        if not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        if not actor.strip():
            raise ValueError("actor must not be blank")
        if not isinstance(request_id, UUID):
            raise TypeError("request_id must be a UUID")

        event_id = event_id or uuid4()
        occurred_at = occurred_at or utc_now()
        event_type_value = event_type.value if isinstance(event_type, EventType) else event_type
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default)

        with self.database.begin() as connection:
            existing = connection.exec_driver_sql(
                "SELECT * FROM events WHERE event_id = ?", (str(event_id),)
            ).mappings().fetchone()
            if existing is not None:
                if not self._matches_existing(
                    existing,
                    stream_id=stream_id,
                    event_type=event_type_value,
                    payload_json=payload_json,
                    actor=actor,
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                    causation_id=causation_id,
                    correlation_id=correlation_id,
                ):
                    raise ValueError("event is immutable")
                return self._row_to_event(existing)

            if idempotency_key is not None:
                duplicate = connection.exec_driver_sql(
                    "SELECT * FROM events WHERE stream_id = ? AND idempotency_key = ?",
                    (stream_id, idempotency_key),
                ).mappings().fetchone()
                if duplicate is not None:
                    if not self._matches_existing(
                        duplicate,
                        stream_id=stream_id,
                        event_type=event_type_value,
                        payload_json=payload_json,
                        actor=actor,
                        request_id=request_id,
                        idempotency_key=idempotency_key,
                        causation_id=causation_id,
                        correlation_id=correlation_id,
                    ):
                        raise ValueError("idempotency key conflicts with existing event")
                    return self._row_to_event(duplicate)

            head = connection.exec_driver_sql(
                "SELECT last_seq FROM stream_heads WHERE stream_id = ?", (stream_id,)
            ).mappings().fetchone()
            seq = (head["last_seq"] if head else 0) + 1
            if head is None:
                connection.exec_driver_sql(
                    "INSERT INTO stream_heads(stream_id, last_seq) VALUES (?, ?)",
                    (stream_id, seq),
                )
            else:
                connection.exec_driver_sql(
                    "UPDATE stream_heads SET last_seq = ? WHERE stream_id = ?",
                    (seq, stream_id),
                )

            ingested_at = utc_now()
            connection.exec_driver_sql(
                """
                INSERT INTO events (
                    event_id, stream_id, seq, event_type, actor, payload_json,
                    schema_version, protocol_version, request_id, idempotency_key,
                    occurred_at, ingested_at, causation_id, correlation_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event_id),
                    stream_id,
                    seq,
                    event_type_value,
                    actor,
                    payload_json,
                    1,
                    "1.0",
                    str(request_id),
                    idempotency_key,
                    occurred_at.isoformat(),
                    ingested_at.isoformat(),
                    str(causation_id) if causation_id else None,
                    str(correlation_id) if correlation_id else None,
                ),
            )

            row = connection.exec_driver_sql(
                "SELECT * FROM events WHERE event_id = ?", (str(event_id),)
            ).mappings().fetchone()
            return self._row_to_event(row)

    def list_after(self, stream_id: str, seq: int) -> list[Event]:
        with self.database.read() as connection:
            rows = connection.exec_driver_sql(
                "SELECT * FROM events WHERE stream_id = ? AND seq > ? ORDER BY seq",
                (stream_id, seq),
            ).mappings().fetchall()
        return [self._row_to_event(row) for row in rows]

    def last_seq(self, stream_id: str) -> int:
        with self.database.read() as connection:
            row = connection.exec_driver_sql(
                "SELECT last_seq FROM stream_heads WHERE stream_id = ?", (stream_id,)
            ).mappings().fetchone()
        return int(row["last_seq"]) if row else 0

    @staticmethod
    def _matches_existing(
        row: Mapping[str, Any],
        *,
        stream_id: str,
        event_type: str,
        payload_json: str,
        actor: str,
        request_id: UUID,
        idempotency_key: Optional[str],
        causation_id: Optional[UUID],
        correlation_id: Optional[UUID],
    ) -> bool:
        return (
            row["stream_id"] == stream_id
            and row["event_type"] == event_type
            and row["payload_json"] == payload_json
            and row["actor"] == actor
            and row["request_id"] == str(request_id)
            and row["idempotency_key"] == idempotency_key
            and row["causation_id"] == (str(causation_id) if causation_id else None)
            and row["correlation_id"] == (str(correlation_id) if correlation_id else None)
        )

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
