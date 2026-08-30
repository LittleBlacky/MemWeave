"""Synchronous session projection for current working memory."""

import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, select, update

from .models import Event, EventType, MemoryRecord, MemoryScope


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
