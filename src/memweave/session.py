"""Synchronous session projection for current working memory."""

import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, select, update

from .errors import StaleWriteError
from .models import (
    Event,
    EventType,
    MemoryKind,
    MemoryOperation,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    OperationType,
)


_metadata = MetaData()
session_states_table = Table(
    "session_states",
    _metadata,
    Column("session_id", String(255), primary_key=True),
    Column("last_seq", Integer, nullable=False),
    Column("recent_messages_json", Text, nullable=False),
    Column("active_memories_json", Text, nullable=False),
)


@dataclass
class SessionState:
    session_id: str
    last_seq: int = 0
    recent_messages: list[dict[str, Any]] = field(default_factory=list)
    active_memories: list[MemoryRecord] = field(default_factory=list)


class SessionStore:
    """Durable, synchronous projection of one session's working state."""

    def __init__(self, database, recent_limit: int = 50):
        if not isinstance(recent_limit, int) or isinstance(recent_limit, bool):
            raise TypeError("recent_limit must be an integer")
        if recent_limit < 1:
            raise ValueError("recent_limit must be positive")
        self.database = database
        self.recent_limit = recent_limit
        with self.database.begin() as connection:
            session_states_table.create(connection, checkfirst=True)

    def apply_event(self, event: Event) -> SessionState:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        session_id = self._session_id_from_stream(event.stream_id)
        with self.database.begin() as connection:
            state = self._read(connection, session_id)
            if event.seq <= state.last_seq:
                return state
            if event.event_type in {
                EventType.USER_MESSAGE.value,
                EventType.MODEL_INPUT.value,
                EventType.MODEL_OUTPUT.value,
                EventType.TOOL_CALLED.value,
                EventType.TOOL_COMPLETED.value,
            } or event.event_type.startswith("turn."):
                state.recent_messages.append(
                    {
                        "event_id": str(event.event_id),
                        "seq": event.seq,
                        "event_type": event.event_type,
                        "actor": event.actor,
                        "payload": event.payload,
                    }
                )
                state.recent_messages = state.recent_messages[-self.recent_limit :]
            state.last_seq = event.seq
            self._write(connection, state)
            return state

    def get(self, session_id: str) -> SessionState:
        self._validate_session_id(session_id)
        with self.database.read() as connection:
            return self._read(connection, session_id)

    def upsert_active(self, memory: MemoryRecord) -> None:
        if not isinstance(memory, MemoryRecord):
            raise TypeError("memory must be a MemoryRecord")
        if memory.scope is not MemoryScope.SESSION:
            raise ValueError("session projection requires session-scoped memory")
        if memory.scope_id.strip() == "":
            raise ValueError("memory scope_id must not be blank")
        session_id = memory.scope_id
        with self.database.begin() as connection:
            state = self._read(connection, session_id)
            existing_index = next(
                (i for i, item in enumerate(state.active_memories) if item.key == memory.key),
                None,
            )
            if existing_index is not None and state.active_memories[existing_index].source_seq > memory.source_seq:
                return
            if existing_index is None:
                state.active_memories.append(memory)
            else:
                state.active_memories[existing_index] = memory
            self._write(connection, state)

    def apply_operation(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_event_id: UUID | str,
    ) -> SessionState:
        """Apply one trusted explicit operation to the session projection.

        ``source_seq`` and ``source_event_id`` are supplied by the event/adapter
        boundary, never parsed from user text.  The method is intentionally
        session-scoped; durable cross-session authority is handled later.
        """
        if not isinstance(operation, MemoryOperation):
            raise TypeError("operation must be a MemoryOperation")
        if operation.scope is not MemoryScope.SESSION:
            raise ValueError("session projection requires session-scoped operation")
        if not isinstance(source_seq, int) or isinstance(source_seq, bool):
            raise TypeError("source_seq must be an integer")
        if source_seq < 1:
            raise ValueError("source_seq must be positive")
        if not isinstance(source_event_id, (UUID, str)):
            raise TypeError("source_event_id must be a UUID or string")
        source_event_id_value = str(source_event_id)
        if not source_event_id_value.strip():
            raise ValueError("source_event_id must not be blank")

        session_id = operation.scope_id
        with self.database.begin() as connection:
            state = self._read(connection, session_id)
            existing_index = self._memory_index(state, operation)
            existing = (
                state.active_memories[existing_index]
                if existing_index is not None
                else None
            )

            if operation.operation is OperationType.FORGET:
                if existing is None:
                    return state
                if source_seq < existing.source_seq:
                    return state
                del state.active_memories[existing_index]
                self._write(connection, state)
                return state

            if operation.operation not in (OperationType.REMEMBER, OperationType.UPDATE):
                raise ValueError(
                    f"unsupported session operation: {operation.operation.value}"
                )
            if existing is not None:
                if source_seq < existing.source_seq:
                    return state
                if source_seq == existing.source_seq:
                    if (
                        existing.value == operation.value
                        and source_event_id_value in existing.source.event_ids
                    ):
                        return state
                    raise StaleWriteError(
                        f"conflicting session write for key {operation.key!r} at source_seq {source_seq}"
                    )

            if operation.operation is OperationType.UPDATE:
                if existing is None or existing.version != operation.expected_version:
                    actual = existing.version if existing is not None else None
                    raise StaleWriteError(
                        f"expected session version {operation.expected_version}, got {actual}"
                    )
                version = existing.version + 1
                created_at = existing.created_at
            else:
                version = (existing.version + 1) if existing is not None else 1
                created_at = existing.created_at if existing is not None else None

            record = MemoryRecord(
                kind=operation.kind or MemoryKind.WORKING,
                scope=MemoryScope.SESSION,
                scope_id=session_id,
                key=operation.key,
                value=operation.value,
                status=MemoryStatus.SESSION_ONLY,
                confidence=1.0,
                source=operation.source
                or MemorySource(type="explicit", event_ids=[source_event_id_value]),
                source_seq=source_seq,
                version=version,
                **({"created_at": created_at} if created_at is not None else {}),
            )
            if existing_index is None:
                state.active_memories.append(record)
            else:
                state.active_memories[existing_index] = record
            self._write(connection, state)
            return state

    @staticmethod
    def _memory_index(
        state: SessionState, operation: MemoryOperation
    ) -> int | None:
        if operation.key is not None:
            return next(
                (i for i, item in enumerate(state.active_memories) if item.key == operation.key),
                None,
            )
        if operation.memory_id is not None:
            return next(
                (i for i, item in enumerate(state.active_memories) if item.id == operation.memory_id),
                None,
            )
        return None

    @staticmethod
    def _read(connection, session_id: str) -> SessionState:
        row = connection.execute(
            select(session_states_table).where(session_states_table.c.session_id == session_id)
        ).mappings().first()
        if row is None:
            return SessionState(session_id=session_id)
        return SessionState(
            session_id=session_id,
            last_seq=int(row["last_seq"]),
            recent_messages=json.loads(row["recent_messages_json"]),
            active_memories=[MemoryRecord.model_validate(item) for item in json.loads(row["active_memories_json"])],
        )

    @staticmethod
    def _write(connection, state: SessionState) -> None:
        values = {
            "last_seq": state.last_seq,
            "recent_messages_json": json.dumps(state.recent_messages, sort_keys=True),
            "active_memories_json": json.dumps(
                [item.model_dump(mode="json") for item in state.active_memories],
                sort_keys=True,
            ),
        }
        exists = connection.execute(
            select(session_states_table.c.session_id).where(
                session_states_table.c.session_id == state.session_id
            )
        ).scalar_one_or_none()
        if exists is None:
            connection.execute(session_states_table.insert().values(session_id=state.session_id, **values))
        else:
            connection.execute(
                update(session_states_table)
                .where(session_states_table.c.session_id == state.session_id)
                .values(**values)
            )

    @staticmethod
    def _validate_session_id(session_id: str) -> None:
        if not isinstance(session_id, str):
            raise TypeError("session_id must be a string")
        if not session_id.strip():
            raise ValueError("session_id must not be blank")

    @staticmethod
    def _session_id_from_stream(stream_id: str) -> str:
        marker = "session:"
        if marker in stream_id:
            return stream_id.rsplit(marker, 1)[1]
        return stream_id
