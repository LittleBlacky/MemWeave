from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from memweave.db import Database
from memweave.durable import DurableMemoryStore
from memweave.errors import StaleWriteError
from memweave.models import (
    MemoryKind,
    MemoryOperation,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    OperationType,
)


def make_record(
    *,
    value="PostgreSQL",
    source_seq=1,
    version=1,
    key="database.engine",
    memory_id=None,
    status=MemoryStatus.ACTIVE,
):
    return MemoryRecord(
        id=memory_id or uuid4(),
        kind=MemoryKind.FACT,
        scope=MemoryScope.USER,
        scope_id="u1",
        key=key,
        value=value,
        status=status,
        confidence=1.0,
        source=MemorySource(type="explicit", event_ids=[f"event-{source_seq}"]),
        source_seq=source_seq,
        version=version,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def update_operation(*, value, expected_version=1, key="database.engine"):
    return MemoryOperation(
        operation=OperationType.UPDATE,
        scope=MemoryScope.USER,
        scope_id="u1",
        key=key,
        value=value,
        expected_version=expected_version,
    )


def forget_operation(*, key="database.engine", memory_id=None, expected_version=None):
    return MemoryOperation(
        operation=OperationType.FORGET,
        scope=MemoryScope.USER,
        scope_id="u1",
        key=key,
        memory_id=memory_id,
        expected_version=expected_version,
    )


def test_create_update_and_reopen_preserves_version_history(tmp_path):
    path = str(tmp_path / "durable.db")
    store = DurableMemoryStore(Database(path))
    original = store.create(make_record())

    updated = store.update(
        update_operation(value="MySQL", expected_version=1), source_seq=2
    )

    assert updated.id == original.id
    assert updated.version == 2
    assert updated.value == "MySQL"
    assert store.get_active(MemoryScope.USER, "u1", "database.engine") == updated
    history = store.list_versions(MemoryScope.USER, "u1", "database.engine")
    assert [item.version for item in history] == [1, 2]
    assert history[0].status is MemoryStatus.SUPERSEDED
    assert DurableMemoryStore(Database(path)).get_active(
        MemoryScope.USER, "u1", "database.engine"
    ) == updated


def test_create_same_record_is_idempotent(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "idempotent.db")))
    record = make_record()

    first = store.create(record)
    second = store.create(record)

    assert second == first
    assert len(store.list_versions(MemoryScope.USER, "u1", record.key)) == 1


def test_create_replay_by_source_event_is_idempotent(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "source-replay.db")))
    record = make_record()
    first = store.create(record)
    replay = record.model_copy(
        update={
            "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
        }
    )

    assert store.create(replay) == first
    assert len(store.list_versions(MemoryScope.USER, "u1", record.key)) == 1


def test_create_rejects_source_event_conflict_and_memory_id_change(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "create-conflict.db")))
    original = store.create(make_record())

    conflicting_source = make_record(
        value="MySQL", source_seq=2, version=2, memory_id=original.id
    ).model_copy(
        update={"source": MemorySource(type="explicit", event_ids=["event-1"])}
    )
    with pytest.raises(StaleWriteError, match="source event"):
        store.create(conflicting_source)

    replacement = make_record(
        value="MySQL", source_seq=2, version=2, memory_id=uuid4()
    )
    with pytest.raises(StaleWriteError, match="memory_id"):
        store.create(replacement)
    assert store.get_active(MemoryScope.USER, "u1", original.key) == original


def test_create_requires_contiguous_versions(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "version-gaps.db")))

    with pytest.raises(StaleWriteError, match="first memory version must be 1"):
        store.create(make_record(version=2))

    original = store.create(make_record())
    with pytest.raises(StaleWriteError, match="contiguous"):
        store.create(
            make_record(
                value="MySQL",
                source_seq=2,
                version=4,
                memory_id=original.id,
            )
        )

    assert [item.version for item in store.list_versions(
        MemoryScope.USER, "u1", original.key
    )] == [1]


def test_durable_values_are_strict_json_and_round_trip_after_reopen(tmp_path):
    path = str(tmp_path / "value-roundtrip.db")
    value = {"nested": [None, True, 3, 1.25, "text"]}
    store = DurableMemoryStore(Database(path))
    original = store.create(make_record(value=value))

    reopened = DurableMemoryStore(Database(path))
    assert reopened.get_active(MemoryScope.USER, "u1", original.key).value == value

    for invalid in (UUID(int=0), ("tuple",), {"set-value"}):
        with pytest.raises(TypeError, match="JSON-native"):
            store.create(make_record(key=f"invalid-{type(invalid).__name__}", value=invalid))

    with pytest.raises(TypeError, match="JSON-native"):
        store.create(make_record(key="invalid-datetime", value=datetime.now(timezone.utc)))

    with pytest.raises(ValueError, match="finite"):
        store.create(make_record(key="invalid-nan", value=float("nan")))


def test_update_requires_expected_version_when_supplied_and_rejects_stale_source(
    tmp_path,
):
    store = DurableMemoryStore(Database(str(tmp_path / "stale.db")))
    store.create(make_record())
    store.update(update_operation(value="MySQL", expected_version=1), source_seq=2)

    with pytest.raises(StaleWriteError):
        store.update(
            update_operation(value="SQLite", expected_version=1), source_seq=3
        )
    with pytest.raises(StaleWriteError):
        store.update(
            update_operation(value="MariaDB", expected_version=2), source_seq=1
        )

    assert store.get_active(MemoryScope.USER, "u1", "database.engine").value == "MySQL"


def test_update_requires_expected_version(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "missing-version.db")))
    store.create(make_record())

    with pytest.raises(ValueError, match="expected_version"):
        store.update(
            update_operation(value="MySQL", expected_version=None), source_seq=2
        )

    assert store.get_active(MemoryScope.USER, "u1", "database.engine").value == "PostgreSQL"


def test_update_replay_by_source_event_is_idempotent(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "update-replay.db")))
    store.create(make_record())
    source_event_id = uuid4()
    operation = update_operation(value="MySQL", expected_version=1)

    first = store.update(operation, source_seq=2, source_event_id=source_event_id)
    replay = store.update(
        operation, source_seq=2, source_event_id=source_event_id
    )

    assert replay == first
    assert [item.version for item in store.list_versions(
        MemoryScope.USER, "u1", "database.engine"
    )] == [1, 2]


def test_update_rejects_compare_and_swap_loss_without_writing_a_new_version(
    tmp_path, monkeypatch
):
    store = DurableMemoryStore(Database(str(tmp_path / "cas-race.db")))
    store.create(make_record())

    monkeypatch.setattr(
        DurableMemoryStore,
        "_supersede_latest",
        staticmethod(lambda connection, latest: False),
    )

    with pytest.raises(StaleWriteError, match="concurrently"):
        store.update(update_operation(value="MySQL", expected_version=1), source_seq=2)

    history = store.list_versions(MemoryScope.USER, "u1", "database.engine")
    assert [item.version for item in history] == [1]
    assert history[0].status is MemoryStatus.ACTIVE


def test_unique_version_race_is_exposed_as_stale_write(tmp_path, monkeypatch):
    store = DurableMemoryStore(Database(str(tmp_path / "integrity-race.db")))
    store.create(make_record())

    def raise_duplicate(connection, record):
        raise IntegrityError("duplicate durable version", {}, RuntimeError("duplicate"))

    monkeypatch.setattr(
        DurableMemoryStore, "_insert_record", staticmethod(raise_duplicate)
    )

    with pytest.raises(StaleWriteError, match="concurrently"):
        store.update(update_operation(value="MySQL", expected_version=1), source_seq=2)

    current = store.get_active(MemoryScope.USER, "u1", "database.engine")
    assert current is not None
    assert current.version == 1
    assert current.value == "PostgreSQL"


def test_forget_creates_tombstone_and_is_idempotent(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "forget.db")))
    original = store.create(make_record())
    forget = forget_operation(expected_version=1)

    tombstone = store.forget(forget, source_seq=2)
    repeated = store.forget(forget, source_seq=2)

    assert tombstone is not None
    assert tombstone.status is MemoryStatus.RETRACTED
    assert repeated == tombstone
    assert store.get_active(MemoryScope.USER, "u1", "database.engine") is None
    history = store.list_versions(MemoryScope.USER, "u1", "database.engine")
    assert [item.version for item in history] == [1, 2]
    assert history[0].id == original.id


def test_forget_requires_expected_version_and_rejects_stale_delete(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "forget-version.db")))
    store.create(make_record())

    with pytest.raises(ValueError, match="expected_version"):
        store.forget(forget_operation(expected_version=None), source_seq=2)

    store.update(update_operation(value="MySQL", expected_version=1), source_seq=2)
    with pytest.raises(StaleWriteError, match="expected memory version"):
        store.forget(forget_operation(expected_version=1), source_seq=3)

    current = store.get_active(MemoryScope.USER, "u1", "database.engine")
    assert current is not None
    assert current.value == "MySQL"


def test_forget_rejects_reuse_of_source_event_from_another_version(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "forget-source-conflict.db")))
    store.create(make_record())

    with pytest.raises(StaleWriteError, match="different memory operation"):
        store.forget(
            forget_operation(expected_version=1),
            source_seq=2,
            source_event_id="event-1",
        )


def test_forget_rejects_same_key_with_different_memory_id(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "forget-identity-conflict.db")))
    original = store.create(make_record())
    legacy = make_record(
        value="MySQL", source_seq=2, version=2, memory_id=uuid4()
    )
    with store.database.begin() as connection:
        DurableMemoryStore._insert_record(connection, legacy)

    with pytest.raises(ValueError, match="different memories"):
        store.forget(
            forget_operation(
                key=legacy.key, memory_id=original.id, expected_version=2
            ),
            source_seq=3,
        )

    assert store.get_active(MemoryScope.USER, "u1", legacy.key) == legacy


def test_newer_create_can_replace_tombstone(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "restore.db")))
    original = store.create(make_record())
    store.forget(forget_operation(expected_version=1), source_seq=2)

    replacement = make_record(
        value="SQLite", source_seq=3, version=3, memory_id=original.id
    )
    active = store.create(replacement)

    assert active.status is MemoryStatus.ACTIVE
    assert store.get_active(MemoryScope.USER, "u1", "database.engine") == active
    assert [item.status for item in store.list_versions(
        MemoryScope.USER, "u1", "database.engine"
    )] == [MemoryStatus.SUPERSEDED, MemoryStatus.RETRACTED, MemoryStatus.ACTIVE]


def test_scopes_are_isolated_and_non_active_records_are_rejected(tmp_path):
    store = DurableMemoryStore(Database(str(tmp_path / "scope.db")))
    active = store.create(make_record())
    other = make_record()
    other = other.model_copy(update={"scope": MemoryScope.PROJECT, "scope_id": "p1"})
    store.create(other)

    assert store.get_active(MemoryScope.USER, "u1", other.key) is not None
    assert store.get_active(MemoryScope.PROJECT, "p1", other.key) == other
    for status in (
        MemoryStatus.CANDIDATE,
        MemoryStatus.NEEDS_CONFIRMATION,
        MemoryStatus.SUPERSEDED,
        MemoryStatus.RETRACTED,
        MemoryStatus.EXPIRED,
        MemoryStatus.SESSION_ONLY,
    ):
        with pytest.raises(ValueError, match="active"):
            store.create(make_record(status=status))
    assert store.get_active(MemoryScope.USER, "u1", active.key) == active
