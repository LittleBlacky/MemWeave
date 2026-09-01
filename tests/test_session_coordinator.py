from concurrent.futures import ThreadPoolExecutor
import time
from threading import Event as ThreadEvent, Lock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from memweave.db import Database
from memweave.events import EventStore
from memweave.models import EventType, MemoryOperation, MemoryScope, OperationType
from memweave.policy import ExplicitOperationParser, ParseContext
from memweave.session import SessionCommandCoordinator, SessionStore
from memweave.storage.sqlalchemy import SQLAlchemyDatabase


def remember(scope_id="s1", key="database.engine", value="PostgreSQL"):
    return MemoryOperation(
        operation=OperationType.REMEMBER,
        scope=MemoryScope.SESSION,
        scope_id=scope_id,
        key=key,
        value=value,
    )


def test_coordinator_appends_authoritative_event_before_projecting(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)

    result = coordinator.append_explicit(
        remember(),
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
        idempotency_key="cmd-1",
    )

    assert result.event.event_type == EventType.MEMORY_COMMAND.value
    assert result.event.seq == 1
    assert result.state.active_memories[0].value == "PostgreSQL"
    assert events.list_after("session:s1", 0) == [result.event]


def test_event_store_failure_does_not_create_session_memory(tmp_path):
    database = Database(str(tmp_path / "memory.db"))

    class FailingEventStore:
        def append(self, **_kwargs):
            raise RuntimeError("event store unavailable")

    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(FailingEventStore(), sessions)

    with pytest.raises(RuntimeError, match="event store unavailable"):
        coordinator.append_explicit(
            remember(),
            stream_id="session:s1",
            actor="user:u1",
            request_id=uuid4(),
        )

    state = sessions.get("s1")
    assert state.last_seq == 0
    assert state.active_memories == []


def test_projection_failure_leaves_event_for_replay(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)
    original_apply_event = sessions.apply_event
    calls = 0

    def fail_once(event, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("projection unavailable")
        return original_apply_event(event)

    sessions.apply_event = fail_once
    with pytest.raises(RuntimeError, match="projection unavailable"):
        coordinator.append_explicit(
            remember(),
            stream_id="session:s1",
            actor="user:u1",
            request_id=uuid4(),
            idempotency_key="cmd-1",
        )

    stored_events = events.list_after("session:s1", 0)
    assert len(stored_events) == 1
    assert sessions.get("s1").last_seq == 0

    recovered = original_apply_event(stored_events[0])
    assert recovered.last_seq == 1
    assert recovered.active_memories[0].value == "PostgreSQL"


def test_replaying_same_memory_command_event_is_idempotent(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)

    result = coordinator.append_explicit(
        remember(),
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
        idempotency_key="cmd-1",
    )
    replayed = sessions.apply_event(result.event)

    assert replayed == result.state
    assert len(replayed.active_memories) == 1


def test_full_rebuild_preserves_memory_id_for_id_based_forget(tmp_path):
    database = Database(str(tmp_path / "authority.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)

    remembered = coordinator.append_explicit(
        remember(),
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
    )
    original_memory = remembered.state.active_memories[0]
    memory_id = original_memory.id
    forgotten = coordinator.append_explicit(
        MemoryOperation(
            operation=OperationType.FORGET,
            scope=MemoryScope.SESSION,
            scope_id="s1",
            memory_id=memory_id,
        ),
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
    )
    assert forgotten.state.active_memories == []

    rebuilt = SessionStore(Database(str(tmp_path / "rebuilt.db")))
    rebuilt_after_remember = rebuilt.apply_event(
        events.list_after("session:s1", 0)[0]
    )
    assert rebuilt_after_remember.active_memories == [original_memory]

    for event in events.list_after("session:s1", 1):
        rebuilt.apply_event(event)

    rebuilt_state = rebuilt.get("s1")
    assert rebuilt_state.last_seq == 2
    assert rebuilt_state.active_memories == []


def test_scope_mismatch_is_rejected_before_event_append(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)

    with pytest.raises(ValueError, match="scope_id"):
        coordinator.append_explicit(
            remember(scope_id="s2"),
            stream_id="session:s1",
            actor="user:u1",
            request_id=uuid4(),
        )

    assert events.last_seq("session:s1") == 0


def test_invalid_memory_command_does_not_advance_projection_watermark(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    event = events.append(
        stream_id="session:s1",
        event_type=EventType.MEMORY_COMMAND,
        payload={"operation": {"scope": "session", "scope_id": "s1"}},
        actor="user:u1",
        request_id=uuid4(),
    )

    with pytest.raises(ValueError, match="invalid operation"):
        sessions.apply_event(event)

    state = sessions.get("s1")
    assert state.last_seq == 0
    assert state.recent_messages == []


def test_contiguous_session_projection_rejects_event_sequence_gap(tmp_path):
    sessions = SessionStore(Database(str(tmp_path / "memory.db")))
    first = events = EventStore(sessions.database).append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "first"},
        actor="user:u1",
        request_id=uuid4(),
    )
    sessions.apply_event(first)
    second = EventStore(sessions.database).append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "second"},
        actor="user:u1",
        request_id=uuid4(),
    )
    third = EventStore(sessions.database).append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "third"},
        actor="user:u1",
        request_id=uuid4(),
    )

    with pytest.raises(ValueError, match="sequence gap"):
        sessions.apply_event(third)
    assert sessions.get("s1").last_seq == 1

    recovered = sessions.apply_event(second)
    assert recovered.last_seq == 2
    recovered = sessions.apply_event(third)
    assert recovered.last_seq == 3


def test_parser_updates_bind_the_current_session_version_at_execution(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)
    parser = ExplicitOperationParser()

    coordinator.append_explicit(
        remember(key="editor", value="VS Code"),
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
    )
    context = ParseContext("t1", "u1", "s1", None, 1)
    first_update = parser.parse("更新 editor = PyCharm", context)[0]
    assert first_update.expected_version is None
    coordinator.append_explicit(
        first_update,
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
    )

    second_update = parser.parse("更新 editor = Vim", context)[0]
    assert second_update.expected_version is None
    result = coordinator.append_explicit(
        second_update,
        stream_id="session:s1",
        actor="user:u1",
        request_id=uuid4(),
    )

    memory = result.state.active_memories[0]
    assert memory.value == "Vim"
    assert memory.version == 3


def test_concurrent_commands_for_one_session_are_serialized(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    events = EventStore(database)
    sessions = SessionStore(database)
    coordinator = SessionCommandCoordinator(events, sessions)
    original_apply_event = sessions.apply_event
    first_projection_entered = ThreadEvent()
    release_first_projection = ThreadEvent()
    calls_lock = Lock()
    calls = 0

    def delayed_apply(event, **_kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            first_projection_entered.set()
            assert release_first_projection.wait(timeout=5)
        return original_apply_event(event)

    sessions.apply_event = delayed_apply

    def submit(value):
        return coordinator.append_explicit(
            remember(key=f"key-{value}", value=value),
            stream_id="session:s1",
            actor="user:u1",
            request_id=uuid4(),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(submit, "first")
        assert first_projection_entered.wait(timeout=5)
        second = executor.submit(submit, "second")
        assert not second.done()
        release_first_projection.set()
        first_result = first.result(timeout=5)
        second_result = second.result(timeout=5)

    assert {first_result.event.seq, second_result.event.seq} == {1, 2}
    state = sessions.get("s1")
    assert state.last_seq == 2
    assert {memory.value for memory in state.active_memories} == {"first", "second"}


def test_database_lease_serializes_separate_session_store_instances(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    first = SessionStore(database)
    second = SessionStore(database)
    entered = ThreadEvent()
    release = ThreadEvent()

    def hold_lease():
        with first.command_lease(
            "session:s1", owner_id="process-a", wait_timeout=1
        ):
            entered.set()
            assert release.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        holder = executor.submit(hold_lease)
        assert entered.wait(timeout=5)
        blocked = executor.submit(
            lambda: second.command_lease(
                "session:s1", owner_id="process-b", wait_timeout=0.05
            ).__enter__()
        )
        with pytest.raises(TimeoutError):
            blocked.result(timeout=5)
        release.set()
        holder.result(timeout=5)

    with second.command_lease("session:s1", owner_id="process-b", wait_timeout=1) as lease:
        assert lease.fencing_token == 2


def test_fencing_token_rejects_projection_from_expired_owner(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    first = SessionStore(database)
    second = SessionStore(database)
    event = EventStore(database).append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "hello"},
        actor="user:u1",
        request_id=uuid4(),
    )

    with first.command_lease(
        "session:s1", owner_id="process-a", lease_seconds=0.01, wait_timeout=1
    ) as old_lease:
        time.sleep(0.03)
        with second.command_lease("session:s1", owner_id="process-b", wait_timeout=1) as new_lease:
            assert new_lease.fencing_token > old_lease.fencing_token
            with pytest.raises(RuntimeError, match="lease is no longer valid"):
                first.apply_event(event, lease=old_lease)


def test_lease_is_bound_to_tenant_storage_namespace(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    tenant_a = SessionStore(database, tenant_id="tenant-a")
    tenant_b = SessionStore(database, tenant_id="tenant-b")
    event = EventStore(database).append(
        stream_id="tenant:tenant-a/session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "hello"},
        actor="user:u1",
        request_id=uuid4(),
    )

    with tenant_b.command_lease("tenant:tenant-b/session:s1", owner_id="process-a") as lease:
        with pytest.raises(ValueError, match="lease does not match"):
            tenant_a.apply_event(event, lease=lease)


def test_command_lease_does_not_mask_permanent_database_errors(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'unmigrated.db'}")
    store = SessionStore(database)

    with pytest.raises(OperationalError, match="no such table"):
        with store.command_lease(
            "session:s1", owner_id="process-a", wait_timeout=0
        ):
            raise AssertionError("lease must not be acquired")
