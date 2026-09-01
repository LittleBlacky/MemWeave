from datetime import datetime, timezone
from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.errors import StaleWriteError
from memweave.models import (
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
from memweave.session import SessionStore


def operation(operation_type, *, key, value=None, expected_version=None):
    return MemoryOperation(
        operation=operation_type,
        scope=MemoryScope.SESSION,
        scope_id="s1",
        key=key,
        value=value,
        expected_version=expected_version,
    )


def project_event(store, seq):
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=seq,
        actor="user:u1",
        payload={"text": f"event-{seq}"},
    )
    store.apply_event(event)
    return event


def record(*, event_id, key="language", value="Python", source_seq=1):
    return MemoryRecord(
        kind=MemoryKind.WORKING,
        scope=MemoryScope.SESSION,
        scope_id="s1",
        key=key,
        value=value,
        status=MemoryStatus.SESSION_ONLY,
        confidence=1.0,
        source=MemorySource(type="explicit", event_ids=[str(event_id)]),
        source_seq=source_seq,
        version=1,
    )


def test_explicit_operations_update_session_memory_synchronously(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))

    first_event = project_event(store, 1)
    remembered = store.apply_operation(
        operation(OperationType.REMEMBER, key="database.engine", value="PostgreSQL"),
        source_seq=1,
        source_event_id=first_event.event_id,
    )
    assert remembered.active_memories[0].value == "PostgreSQL"
    assert remembered.active_memories[0].version == 1

    second_event = project_event(store, 2)
    updated = store.apply_operation(
        operation(
            OperationType.UPDATE,
            key="database.engine",
            value="MySQL",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=second_event.event_id,
    )
    assert updated.active_memories[0].value == "MySQL"
    assert updated.active_memories[0].version == 2

    third_event = project_event(store, 3)
    forgotten = store.apply_operation(
        operation(OperationType.FORGET, key="database.engine"),
        source_seq=3,
        source_event_id=third_event.event_id,
    )
    assert forgotten.active_memories == []


def test_stale_explicit_update_does_not_overwrite_session_memory(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    first_event = project_event(store, 1)
    store.apply_operation(
        operation(OperationType.REMEMBER, key="editor", value="VS Code"),
        source_seq=1,
        source_event_id=first_event.event_id,
    )
    second_event = project_event(store, 2)
    store.apply_operation(
        operation(
            OperationType.UPDATE,
            key="editor",
            value="PyCharm",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=second_event.event_id,
    )

    third_event = project_event(store, 3)
    with pytest.raises(StaleWriteError):
        store.apply_operation(
            operation(
                OperationType.UPDATE,
                key="editor",
                value="Vim",
                expected_version=1,
            ),
            source_seq=3,
            source_event_id=third_event.event_id,
        )

    assert store.get("s1").active_memories[0].value == "PyCharm"


def test_stale_explicit_forget_does_not_delete_newer_session_memory(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    first_event = project_event(store, 1)
    store.apply_operation(
        operation(OperationType.REMEMBER, key="editor", value="VS Code"),
        source_seq=1,
        source_event_id=first_event.event_id,
    )
    second_event = project_event(store, 2)
    store.apply_operation(
        operation(
            OperationType.UPDATE,
            key="editor",
            value="PyCharm",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=second_event.event_id,
    )

    third_event = project_event(store, 3)
    with pytest.raises(StaleWriteError):
        store.apply_operation(
            operation(
                OperationType.FORGET,
                key="editor",
                expected_version=1,
            ),
            source_seq=3,
            source_event_id=third_event.event_id,
        )

    assert store.get("s1").active_memories[0].value == "PyCharm"


def test_duplicate_explicit_operation_is_idempotent(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    first_event = project_event(store, 1)
    remember = operation(OperationType.REMEMBER, key="language", value="Python")

    first = store.apply_operation(remember, source_seq=1, source_event_id=first_event.event_id)
    second = store.apply_operation(remember, source_seq=1, source_event_id=first_event.event_id)

    assert second.active_memories == first.active_memories
    assert len(second.active_memories) == 1


def test_same_source_sequence_with_different_content_is_rejected(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    first_event = project_event(store, 1)
    store.apply_operation(
        operation(OperationType.REMEMBER, key="language", value="Python"),
        source_seq=1,
        source_event_id=first_event.event_id,
    )

    with pytest.raises(StaleWriteError):
        store.apply_operation(
            operation(OperationType.REMEMBER, key="language", value="Rust"),
            source_seq=1,
            source_event_id=uuid4(),
        )


def test_low_level_operation_cannot_bypass_event_projection(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))

    with pytest.raises(ValueError, match="session watermark"):
        store.apply_operation(
            operation(OperationType.REMEMBER, key="language", value="Python"),
            source_seq=1,
            source_event_id=uuid4(),
        )

    state = store.get("s1")
    assert state.last_seq == 0
    assert state.active_memories == []


def test_upsert_active_uses_event_watermark_and_conflict_rules(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    source_event = project_event(store, 1)
    first = record(event_id=source_event.event_id)

    store.upsert_active(first)
    duplicate = record(event_id=source_event.event_id)
    store.upsert_active(duplicate)
    assert store.get("s1").active_memories == [first]

    with pytest.raises(StaleWriteError):
        store.upsert_active(
            record(event_id=uuid4(), value="Rust", source_seq=1)
        )

    with pytest.raises(ValueError, match="session watermark"):
        store.upsert_active(record(event_id=uuid4(), source_seq=2))


def test_upsert_active_rejects_memory_version_rollback(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    source_event = project_event(store, 1)
    store.upsert_active(record(event_id=source_event.event_id, source_seq=1))
    project_event(store, 2)
    with pytest.raises(StaleWriteError, match="version must increase"):
        store.upsert_active(
            record(event_id=uuid4(), value="Rust", source_seq=2)
        )


def test_session_store_namespaces_state_by_tenant(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    tenant_a = SessionStore(database, tenant_id="tenant-a")
    tenant_b = SessionStore(database, tenant_id="tenant-b")

    event_a = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-a:session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "A"},
    )
    event_b = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-b:session:s1",
        seq=1,
        actor="user:b",
        payload={"text": "B"},
    )
    tenant_a.apply_event(event_a)
    tenant_b.apply_event(event_b)

    assert tenant_a.get("s1").recent_messages[0]["payload"]["text"] == "A"
    assert tenant_b.get("s1").recent_messages[0]["payload"]["text"] == "B"


def test_session_projection_normalizes_json_payload_at_write_boundary(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    store = SessionStore(database)
    event_id = uuid4()
    occurred = datetime(2026, 8, 31, tzinfo=timezone.utc)
    event = Event(
        event_id=event_id,
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:a",
        payload={"event_id": event_id, "occurred_at": occurred},
    )

    state = store.apply_event(event)
    assert state.recent_messages[0]["payload"] == {
        "event_id": str(event_id),
        "occurred_at": str(occurred),
    }
    assert store.get("s1").recent_messages == state.recent_messages


def test_tenant_session_store_rejects_foreign_stream_id(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")), tenant_id="tenant-a")
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-b:session:s1",
        seq=1,
        actor="user:b",
        payload={"text": "B"},
    )

    with pytest.raises(ValueError, match="tenant"):
        store.apply_event(event)


def test_unscoped_session_store_rejects_tenant_streams(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-a:session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "A"},
    )

    with pytest.raises(ValueError, match="tenant-scoped"):
        store.apply_event(event)

    assert store.get("s1").last_seq == 0
