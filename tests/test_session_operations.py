from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from memweave.db import Database
from memweave.events import EventStore
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
from memweave.storage.schema import (
    events_table,
    schema_migrations_table,
    session_event_receipts_table,
    session_states_table,
)
from sqlalchemy import delete, select


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


def test_explicit_empty_stream_id_is_rejected_instead_of_using_canonical(tmp_path):
    store = SessionStore(Database(str(tmp_path / "empty-stream.db")), tenant_id="t1")
    source_event_id = uuid4()

    with pytest.raises(ValueError, match="stream_id"):
        store.get("s1", stream_id="")
    with pytest.raises(ValueError, match="stream_id"):
        store.apply_operation(
            operation(
                OperationType.REMEMBER,
                key="database.engine",
                value="PostgreSQL",
            ),
            source_seq=1,
            source_event_id=source_event_id,
            stream_id="",
        )
    with pytest.raises(ValueError, match="stream_id"):
        store.upsert_active(record(event_id=source_event_id), stream_id="")

    assert store.get("s1").last_seq == 0
    assert store.get("s1").active_memories == []


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
        stream_id="tenant:tenant-a/session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "A"},
    )
    event_b = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-b/session:s1",
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


def test_same_sequence_conflicting_event_is_rejected(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    first = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "original"},
    )
    conflict = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:b",
        payload={"text": "conflict"},
    )

    store.apply_event(first)

    with pytest.raises(ValueError, match="conflicting event"):
        store.apply_event(conflict)


def test_same_sequence_conflict_is_detected_after_database_reopen(tmp_path):
    path = tmp_path / "memory.db"
    first = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "original"},
    )
    SessionStore(Database(str(path))).apply_event(first)

    conflict = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:b",
        payload={"text": "conflict"},
    )
    reopened = SessionStore(Database(str(path)))

    with pytest.raises(ValueError, match="conflicting event"):
        reopened.apply_event(conflict)


def test_receipts_are_backfilled_for_applied_events_on_migration_retry(tmp_path):
    path = str(tmp_path / "memory.db")
    database = Database(path)
    events = EventStore(database)
    event = events.append(
        "session:s1",
        EventType.USER_MESSAGE,
        {"text": "original"},
        "user:a",
        request_id=uuid4(),
    )
    SessionStore(database).apply_event(event)

    with database.begin() as connection:
        connection.execute(
            delete(session_event_receipts_table).where(
                session_event_receipts_table.c.session_id == "s1"
            )
        )
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version.in_(
                    [
                        "0008_session_event_receipts",
                        "0009_projection_event_receipts",
                        "0010_validate_projection_receipts",
                        "0011_validate_session_receipts",
                    ]
                )
            )
        )

    assert database.apply_migrations() == [
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
    ]
    conflict = event.model_copy(update={"actor": "user:b", "payload": {"text": "conflict"}})
    with pytest.raises(ValueError, match="conflicting event"):
        SessionStore(database).apply_event(conflict)


def test_session_receipt_backfill_rejects_incomplete_event_stream(tmp_path):
    database = Database(str(tmp_path / "session-receipt-gap.db"))
    events = EventStore(database)
    for text in ("one", "two", "three"):
        event = events.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {"text": text},
            "user:a",
            request_id=uuid4(),
        )
        SessionStore(database).apply_event(event)

    with database.begin() as connection:
        connection.execute(
            delete(events_table).where(
                events_table.c.stream_id == "session:s1",
                events_table.c.seq == 2,
            )
        )
        connection.execute(
            delete(session_event_receipts_table).where(
                session_event_receipts_table.c.session_id == "s1"
            )
        )
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version.in_(
                    [
                        "0008_session_event_receipts",
                        "0009_projection_event_receipts",
                        "0010_validate_projection_receipts",
                        "0011_validate_session_receipts",
                    ]
                )
            )
        )

    with pytest.raises(ValueError, match="incomplete event stream"):
        database.apply_migrations()

    with database.read() as connection:
        assert connection.execute(
            select(session_event_receipts_table.c.seq).where(
                session_event_receipts_table.c.session_id == "s1"
            )
        ).scalars().all() == []
        assert connection.execute(
            select(schema_migrations_table.c.version).where(
                schema_migrations_table.c.version.in_(
                    [
                        "0008_session_event_receipts",
                        "0009_projection_event_receipts",
                        "0010_validate_projection_receipts",
                        "0011_validate_session_receipts",
                    ]
                )
            )
        ).scalars().all() == []


def test_snapshot_behind_existing_receipt_can_be_rebuilt(tmp_path):
    path = str(tmp_path / "memory.db")
    database = Database(path)
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "original"},
    )
    store = SessionStore(database)
    store.apply_event(event)
    with database.begin() as connection:
        connection.execute(
            delete(session_states_table).where(
                session_states_table.c.session_id == "s1"
            )
        )

    rebuilt = store.apply_event(event)
    assert rebuilt.last_seq == 1
    assert rebuilt.recent_messages[0]["payload"] == {"text": "original"}


def test_session_store_rejects_new_event_when_receipt_prefix_is_incomplete(tmp_path):
    database = Database(str(tmp_path / "runtime-receipt-gap.db"))
    events = EventStore(database)
    store = SessionStore(database)
    projected = []
    for text in ("one", "two"):
        event = events.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {"text": text},
            "user:a",
            request_id=uuid4(),
        )
        store.apply_event(event)
        projected.append(event)
    next_event = events.append(
        "session:s1",
        EventType.USER_MESSAGE,
        {"text": "three"},
        "user:a",
        request_id=uuid4(),
    )
    with database.begin() as connection:
        connection.execute(
            delete(session_event_receipts_table).where(
                session_event_receipts_table.c.session_id == "s1",
                session_event_receipts_table.c.seq == projected[0].seq,
            )
        )

    with pytest.raises(RuntimeError, match="receipts are incomplete"):
        store.apply_event(next_event)

    with database.read() as connection:
        assert connection.execute(
            select(session_states_table.c.last_seq).where(
                session_states_table.c.session_id == "s1"
            )
        ).scalar_one() == 2
        assert connection.execute(
            select(session_event_receipts_table.c.seq)
            .where(session_event_receipts_table.c.session_id == "s1")
            .order_by(session_event_receipts_table.c.seq)
        ).scalars().all() == [2]


def test_tenant_session_store_rejects_foreign_stream_id(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")), tenant_id="tenant-a")
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-b/session:s1",
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
        stream_id="tenant:tenant-a/session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "A"},
    )

    with pytest.raises(ValueError, match="tenant-scoped"):
        store.apply_event(event)

    assert store.get("s1").last_seq == 0


def test_session_id_rejects_reserved_namespace_delimiter(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))

    with pytest.raises(ValueError, match="must not contain.*:"):
        store.get("tenant:session:s1")


def test_tenant_session_store_accepts_extensible_scope_segments(tmp_path):
    store = SessionStore(
        Database(str(tmp_path / "memory.db")), tenant_id="tenant-a"
    )
    event = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id="tenant:tenant-a/project:p1/session:s1",
        seq=1,
        actor="user:a",
        payload={"text": "A"},
    )

    assert store.apply_event(event).session_id == "s1"
    assert store.stream_id_for_session("s1") == "tenant:tenant-a/session:s1"


def test_tenant_project_streams_with_same_session_id_are_isolated(tmp_path):
    store = SessionStore(
        Database(str(tmp_path / "memory.db")), tenant_id="tenant-a"
    )
    project_one_stream = "tenant:tenant-a/project:p1/session:s1"
    project_two_stream = "tenant:tenant-a/project:p2/session:s1"
    project_one = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id=project_one_stream,
        seq=1,
        actor="user:a",
        payload={"text": "project one"},
    )
    project_two = Event(
        event_type=EventType.USER_MESSAGE,
        stream_id=project_two_stream,
        seq=1,
        actor="user:a",
        payload={"text": "project two"},
    )

    store.apply_event(project_one)
    store.apply_event(project_two)

    assert store.get("s1", stream_id=project_one_stream).recent_messages[0][
        "payload"
    ]["text"] == "project one"
    assert store.get("s1", stream_id=project_two_stream).recent_messages[0][
        "payload"
    ]["text"] == "project two"


def test_legacy_extended_snapshot_requires_replay(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    store = SessionStore(database, tenant_id="tenant-a")
    stream_id = "tenant:tenant-a/project:p1/session:s1"
    stream_identity = uuid5(NAMESPACE_URL, f"memweave:session-stream:{stream_id}")
    storage_id = f"stream:{stream_identity}"
    with database.begin() as connection:
        connection.execute(
            session_states_table.insert().values(
                session_id=storage_id,
                last_seq=1,
                recent_messages_json="[]",
                active_memories_json="[]",
            )
        )

    with pytest.raises(RuntimeError, match="replay required"):
        store.get("s1", stream_id=stream_id)


def test_legacy_extended_snapshot_rebuild_starts_only_at_sequence_one(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    store = SessionStore(database, tenant_id="tenant-a")
    stream_id = "tenant:tenant-a/project:p1/session:s1"
    stream_identity = uuid5(NAMESPACE_URL, f"memweave:session-stream:{stream_id}")
    storage_id = f"stream:{stream_identity}"
    with database.begin() as connection:
        connection.execute(
            session_states_table.insert().values(
                session_id=storage_id,
                last_seq=2,
                recent_messages_json="[]",
                active_memories_json="[]",
            )
        )

    with pytest.raises(RuntimeError, match="replay required"):
        store.apply_event(
            Event(
                event_type=EventType.USER_MESSAGE,
                stream_id=stream_id,
                seq=2,
                actor="user:a",
                payload={"text": "replay-2"},
            )
        )

    first = store.apply_event(
        Event(
            event_type=EventType.USER_MESSAGE,
            stream_id=stream_id,
            seq=1,
            actor="user:a",
            payload={"text": "replay-1"},
        )
    )
    assert first.last_seq == 1
    assert first.recent_messages[0]["payload"]["text"] == "replay-1"
    assert store.get("s1", stream_id=stream_id).recent_messages == first.recent_messages
