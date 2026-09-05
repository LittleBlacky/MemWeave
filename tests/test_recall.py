from datetime import datetime, timezone
from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.durable import DurableMemoryStore
from memweave.models import (
    Event,
    EventType,
    MemoryKind,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    RecallRequest,
    MemoryOperation,
    OperationType,
)
from memweave.recall import RecallService
from memweave.session import SessionStore


def record(*, scope, scope_id, key, value, version=1, source_seq=1, status=MemoryStatus.ACTIVE):
    return MemoryRecord(
        id=uuid4(),
        kind=MemoryKind.FACT,
        scope=scope,
        scope_id=scope_id,
        key=key,
        value=value,
        status=status,
        confidence=1.0,
        source=MemorySource(type="test", event_ids=[str(uuid4())]),
        source_seq=source_seq,
        version=version,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def request(**kwargs):
    values = {
        "query": "database",
        "session_id": "s1",
        "visible_scopes": ["session:s1", "user:u1", "project:p1"],
        "top_k": 10,
        "max_tokens": 1000,
    }
    values.update(kwargs)
    return RecallRequest(**values)


def test_session_memory_precedes_durable_and_duplicate_key_is_suppressed(tmp_path):
    database = Database(str(tmp_path / "recall.db"))
    session = SessionStore(database)
    durable = DurableMemoryStore(database)
    session_memory = record(
        scope=MemoryScope.SESSION,
        scope_id="s1",
        key="database.engine",
        value="SQLite",
        status=MemoryStatus.SESSION_ONLY,
        source_seq=1,
    )
    session.apply_event(
        Event(
            event_type=EventType.USER_MESSAGE,
            stream_id="session:s1",
            seq=1,
            actor="user:u1",
            payload={"text": "set memory"},
        )
    )
    session.upsert_active(session_memory, stream_id="session:s1")
    durable.create(
        record(scope=MemoryScope.USER, scope_id="u1", key="database.engine", value="PostgreSQL"),
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )
    service = RecallService(session, durable)

    result = service.recall(request())

    assert [item.value for item in result.items] == ["SQLite"]
    assert result.degraded is False
    assert result.watermarks["session"] == 1
    assert result.watermarks["durable"] == 0


def test_scope_isolation_and_tombstone_filtering(tmp_path):
    database = Database(str(tmp_path / "recall.db"))
    session = SessionStore(database)
    durable = DurableMemoryStore(database)
    created = durable.create(
        record(scope=MemoryScope.USER, scope_id="u1", key="database.engine", value="PostgreSQL"),
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )
    durable.forget(
        MemoryOperation(
            operation=OperationType.FORGET,
            scope=MemoryScope.USER,
            scope_id="u1",
            key="database.engine",
            memory_id=created.id,
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )
    durable.create(
        record(scope=MemoryScope.USER, scope_id="u2", key="database.secret", value="leak"),
        source_event_id=uuid4(),
        source_stream_id="user:u2",
    )
    service = RecallService(session, durable)

    result = service.recall(request(visible_scopes=["session:s1", "user:u1"]))

    assert result.items == []


def test_old_durable_version_and_unmatched_memory_are_not_returned(tmp_path):
    database = Database(str(tmp_path / "recall.db"))
    session = SessionStore(database)
    durable = DurableMemoryStore(database)
    created = durable.create(
        record(scope=MemoryScope.USER, scope_id="u1", key="database.engine", value="PostgreSQL"),
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )
    durable.update(
        MemoryOperation(
            operation=OperationType.UPDATE,
            scope=MemoryScope.USER,
            scope_id="u1",
            key="database.engine",
            value="SQLite",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )
    durable.create(
        record(scope=MemoryScope.USER, scope_id="u1", key="language", value="Python"),
        source_event_id=uuid4(),
        source_stream_id="user:u1",
    )

    result = RecallService(session, durable).recall(request())

    assert [item.value for item in result.items] == ["SQLite"]


def test_token_budget_and_top_k_are_enforced(tmp_path):
    database = Database(str(tmp_path / "recall.db"))
    session = SessionStore(database)
    durable = DurableMemoryStore(database)
    for index in range(3):
        durable.create(
            record(
                scope=MemoryScope.USER,
                scope_id="u1",
                key=f"database.{index}",
                value="a value with several words",
            ),
            source_event_id=uuid4(),
            source_stream_id="user:u1",
        )
    service = RecallService(session, durable)

    result = service.recall(request(top_k=1, max_tokens=3))

    assert len(result.items) <= 1
    assert result.items == []


def test_durable_failure_returns_session_fallback(tmp_path):
    database = Database(str(tmp_path / "recall.db"))
    session = SessionStore(database)
    memory = record(
        scope=MemoryScope.SESSION,
        scope_id="s1",
        key="database.engine",
        value="SQLite",
        status=MemoryStatus.SESSION_ONLY,
    )
    session.apply_event(
        Event(
            event_type=EventType.USER_MESSAGE,
            stream_id="session:s1",
            seq=1,
            actor="user:u1",
            payload={"text": "set memory"},
        )
    )
    session.upsert_active(memory, stream_id="session:s1")

    class BrokenDurable:
        def list_active(self, scope, scope_id):
            raise RuntimeError("durable unavailable")

    result = RecallService(session, BrokenDurable()).recall(request())

    assert [item.value for item in result.items] == ["SQLite"]
    assert result.degraded is True
