from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.errors import StaleWriteError
from memweave.models import MemoryOperation, MemoryScope, OperationType
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


def test_explicit_operations_update_session_memory_synchronously(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))

    remembered = store.apply_operation(
        operation(OperationType.REMEMBER, key="database.engine", value="PostgreSQL"),
        source_seq=1,
        source_event_id=uuid4(),
    )
    assert remembered.active_memories[0].value == "PostgreSQL"
    assert remembered.active_memories[0].version == 1

    updated = store.apply_operation(
        operation(
            OperationType.UPDATE,
            key="database.engine",
            value="MySQL",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=uuid4(),
    )
    assert updated.active_memories[0].value == "MySQL"
    assert updated.active_memories[0].version == 2

    forgotten = store.apply_operation(
        operation(OperationType.FORGET, key="database.engine"),
        source_seq=3,
        source_event_id=uuid4(),
    )
    assert forgotten.active_memories == []


def test_stale_explicit_update_does_not_overwrite_session_memory(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    store.apply_operation(
        operation(OperationType.REMEMBER, key="editor", value="VS Code"),
        source_seq=1,
        source_event_id=uuid4(),
    )
    store.apply_operation(
        operation(
            OperationType.UPDATE,
            key="editor",
            value="PyCharm",
            expected_version=1,
        ),
        source_seq=2,
        source_event_id=uuid4(),
    )

    with pytest.raises(StaleWriteError):
        store.apply_operation(
            operation(
                OperationType.UPDATE,
                key="editor",
                value="Vim",
                expected_version=1,
            ),
            source_seq=3,
            source_event_id=uuid4(),
        )

    assert store.get("s1").active_memories[0].value == "PyCharm"


def test_duplicate_explicit_operation_is_idempotent(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    event_id = uuid4()
    remember = operation(OperationType.REMEMBER, key="language", value="Python")

    first = store.apply_operation(remember, source_seq=1, source_event_id=event_id)
    second = store.apply_operation(remember, source_seq=1, source_event_id=event_id)

    assert second.active_memories == first.active_memories
    assert len(second.active_memories) == 1


def test_same_source_sequence_with_different_content_is_rejected(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))
    store.apply_operation(
        operation(OperationType.REMEMBER, key="language", value="Python"),
        source_seq=1,
        source_event_id=uuid4(),
    )

    with pytest.raises(StaleWriteError):
        store.apply_operation(
            operation(OperationType.REMEMBER, key="language", value="Rust"),
            source_seq=1,
            source_event_id=uuid4(),
        )
