from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.events import EventStore
from memweave.models import EventType, MemoryOperation, MemoryScope, OperationType
from memweave.session import SessionCommandCoordinator, SessionStore


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

    def fail_once(event):
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
