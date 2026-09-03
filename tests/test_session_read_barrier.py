from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.events import EventStore
from memweave.models import Event, EventType
from memweave.session import (
    SessionProjectionBackend,
    SessionReadBarrier,
    SessionStore,
)
from memweave.storage.checkpoints import RelationalProjectionCheckpointStore
from memweave.storage.coordinator import ProjectionDispatcher
from memweave.storage.recovery import ProjectionRuntime
from memweave.storage.schema import (
    session_event_receipts_table,
    session_states_table,
)
from memweave.storage.sqlalchemy import SQLAlchemyDatabase
from sqlalchemy import delete


def append_messages(event_store, count):
    return [
        event_store.append(
            stream_id="session:s1",
            event_type=EventType.USER_MESSAGE,
            payload={"text": f"message-{seq}"},
            actor="user:u1",
            request_id=uuid4(),
        )
        for seq in range(1, count + 1)
    ]


def make_runtime(database, session_store, event_store):
    checkpoints = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoints)
    dispatcher.register_backend(SessionProjectionBackend(session_store))
    return ProjectionRuntime(dispatcher, event_store)


def test_runtime_buffers_gap_and_applies_session_events_in_order(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 2)
    sessions = SessionStore(database)
    runtime = make_runtime(database, sessions, event_store)

    runtime.recover("session:s1")
    event_store.append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "message-3"},
        actor="user:u1",
        request_id=uuid4(),
    )
    event4 = event_store.append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "message-4"},
        actor="user:u1",
        request_id=uuid4(),
    )

    runtime.publish(event4)
    assert sessions.get("s1").last_seq == 2

    runtime.recover("session:s1")
    state = sessions.get("s1")
    assert state.last_seq == 4
    assert [item["seq"] for item in state.recent_messages] == [1, 2, 3, 4]


def test_runtime_rebuilds_session_snapshot_when_checkpoint_is_ahead(tmp_path):
    database = Database(str(tmp_path / "snapshot-rebuild.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 1)
    sessions = SessionStore(database)
    runtime = make_runtime(database, sessions, event_store)
    runtime.recover("session:s1")

    with database.begin() as connection:
        connection.execute(
            delete(session_states_table).where(
                session_states_table.c.session_id == "s1"
            )
        )

    assert sessions.get("s1").last_seq == 0
    assert runtime.recover("session:s1") == 1
    rebuilt = sessions.get("s1")
    assert rebuilt.last_seq == 1
    assert rebuilt.recent_messages[0]["payload"]["text"] == "message-1"


def test_read_barrier_recovers_lagging_session_before_returning(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 2)
    sessions = SessionStore(database)
    runtime = make_runtime(database, sessions, event_store)
    runtime.recover("session:s1")

    event3 = event_store.append(
        stream_id="session:s1",
        event_type=EventType.USER_MESSAGE,
        payload={"text": "message-3"},
        actor="user:u1",
        request_id=uuid4(),
    )
    runtime.publish(event3)
    barrier = SessionReadBarrier(sessions, runtime)

    result = barrier.read("s1")

    assert result.requested_seq == 3
    assert result.applied_seq == 3
    assert result.lagging is False
    assert result.degraded is False
    assert result.state.recent_messages[-1]["payload"]["text"] == "message-3"


def test_read_barrier_reports_lag_when_recovery_cannot_cover_target(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 2)
    sessions = SessionStore(database)
    runtime = make_runtime(database, sessions, event_store)
    runtime.recover("session:s1")

    class MissingEventSource:
        def last_seq(self, stream_id):
            return 4

        def list_after(self, stream_id, seq):
            return [events[1]]

    broken_runtime = ProjectionRuntime(runtime.dispatcher, MissingEventSource())
    result = SessionReadBarrier(sessions, broken_runtime).read(
        "s1", target_seq=4
    )

    assert result.applied_seq == 2
    assert result.requested_seq == 4
    assert result.lagging is True
    assert result.degraded is True
    assert result.error


def test_read_barrier_degrades_when_catchup_returns_without_reaching_target(tmp_path):
    database = Database(str(tmp_path / "silent-catchup.db"))
    event_store = EventStore(database)
    append_messages(event_store, 2)
    sessions = SessionStore(database)
    runtime = make_runtime(database, sessions, event_store)

    class SilentCatchup:
        def target_seq(self, stream_id):
            return 2

        def catch_up(self, stream_id, target_seq):
            return 0

    result = SessionReadBarrier(sessions, SilentCatchup()).read("s1")

    assert result.applied_seq == 0
    assert result.requested_seq == 2
    assert result.lagging is True
    assert result.degraded is True
    assert "remains behind" in result.error


def test_read_barrier_depends_on_catchup_contract_not_runtime_internals(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 3)
    sessions = SessionStore(database)
    sessions.apply_event(events[0])
    sessions.apply_event(events[1])

    class FakeCatchup:
        def __init__(self):
            self.calls = []

        def target_seq(self, stream_id):
            return 3

        def catch_up(self, stream_id, target_seq):
            self.calls.append((stream_id, target_seq))
            sessions.apply_event(events[2])
            return 3

    catchup = FakeCatchup()
    result = SessionReadBarrier(sessions, catchup).read("s1")

    assert catchup.calls == [("session:s1", 3)]
    assert result.applied_seq == 3
    assert result.lagging is False


def test_read_barrier_returns_local_state_when_target_watermark_is_unavailable(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    event_store = EventStore(database)
    event = append_messages(event_store, 1)[0]
    sessions = SessionStore(database)
    sessions.apply_event(event)

    class UnavailableTarget:
        def target_seq(self, stream_id):
            raise RuntimeError("event authority unavailable")

        def catch_up(self, stream_id, target_seq):
            raise AssertionError("catch_up must not run without a target")

    result = SessionReadBarrier(sessions, UnavailableTarget()).read("s1")

    assert result.state.last_seq == 1
    assert result.requested_seq == 1
    assert result.applied_seq == 1
    assert result.lagging is False
    assert result.degraded is True
    assert result.error == "event authority unavailable"


@pytest.mark.parametrize("invalid_target", [-1, True, "2"])
def test_read_barrier_rejects_invalid_runtime_target_watermark(
    tmp_path, invalid_target
):
    database = Database(str(tmp_path / "invalid-target.db"))
    sessions = SessionStore(database)

    class InvalidTarget:
        def target_seq(self, stream_id):
            return invalid_target

        def catch_up(self, stream_id, target_seq):
            raise AssertionError("catch_up must not run for an invalid target")

    result = SessionReadBarrier(sessions, InvalidTarget()).read("s1")

    assert result.requested_seq == 0
    assert result.applied_seq == 0
    assert result.lagging is False
    assert result.degraded is True
    assert "target_seq" in result.error


def test_read_barrier_reports_equal_numeric_watermark_as_degraded_when_receipts_are_incomplete(
    tmp_path,
):
    database = Database(str(tmp_path / "receipt-gap.db"))
    event_store = EventStore(database)
    events = append_messages(event_store, 2)
    sessions = SessionStore(database)
    for event in events:
        sessions.apply_event(event)
    with database.begin() as connection:
        connection.execute(
            delete(session_event_receipts_table).where(
                session_event_receipts_table.c.session_id == "s1",
                session_event_receipts_table.c.seq == 1,
            )
        )

    result = SessionReadBarrier(sessions, make_runtime(database, sessions, event_store)).read(
        "s1"
    )

    assert result.requested_seq == 2
    assert result.applied_seq == 2
    assert result.lagging is True
    assert result.degraded is True
    assert "receipts are incomplete" in result.error
    assert result.state.last_seq == 2


def test_read_barrier_reads_the_requested_project_session_stream(tmp_path):
    database = Database(str(tmp_path / "memory.db"))
    sessions = SessionStore(database, tenant_id="tenant-a")
    project_one_stream = "tenant:tenant-a/project:p1/session:s1"
    project_two_stream = "tenant:tenant-a/project:p2/session:s1"
    sessions.apply_event(
        Event(
            event_type=EventType.USER_MESSAGE,
            stream_id=project_one_stream,
            seq=1,
            actor="user:a",
            payload={"text": "project one"},
        )
    )
    sessions.apply_event(
        Event(
            event_type=EventType.USER_MESSAGE,
            stream_id=project_two_stream,
            seq=1,
            actor="user:a",
            payload={"text": "project two"},
        )
    )

    class AlreadyCurrent:
        def target_seq(self, stream_id):
            assert stream_id == project_two_stream
            return 1

        def catch_up(self, stream_id, target_seq):
            raise AssertionError("the requested stream is already current")

    result = SessionReadBarrier(sessions, AlreadyCurrent()).read(
        "s1", stream_id=project_two_stream
    )

    assert result.lagging is False
    assert result.state.recent_messages[0]["payload"]["text"] == "project two"


def test_session_projection_health_reports_missing_schema(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'unmigrated.db'}")
    sessions = SessionStore(database)
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(SessionProjectionBackend(sessions))

    assert dispatcher.health() == {"session": False}
    assert "no such table" in dispatcher.errors()["__system__"]["session"]
