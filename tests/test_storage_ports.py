from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from threading import Barrier, Event as ThreadEvent, Lock
from uuid import uuid4

import pytest
from sqlalchemy import text

from memweave.events import EventStore
from memweave.models import Event
from memweave.models import EventType
from memweave.storage.checkpoints import RelationalProjectionCheckpointStore
from memweave.storage.coordinator import ProjectionDispatcher, StorageCoordinator
from memweave.storage.migrations import MigrationRunner
from memweave.storage.ports import EventProjector, EventRepository, ProjectionBackend, VectorIndex
from memweave.storage.sqlalchemy import SQLAlchemyDatabase
from memweave.storage.sqlite import SQLiteDatabase


class RecordingBackend:
    def __init__(self, name="recording"):
        self.name = name
        self.events = []
        self.last_seq = {}

    def apply(self, event: Event) -> None:
        self.events.append(event.event_id)
        self.last_seq[event.stream_id] = event.seq

    def health(self) -> bool:
        return True

    def watermark(self, stream_id: str) -> int:
        return self.last_seq.get(stream_id, 0)


def test_sqlite_database_migrations_are_versioned_and_idempotent(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "memory.db"))

    assert database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
    ]


def test_default_migration_runner_discovers_packaged_migrations():
    runner = MigrationRunner()

    assert list(runner.discover()) == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
    ]


def test_migration_runner_applied_returns_empty_only_when_table_is_missing(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'unmigrated.db'}")
    runner = MigrationRunner()
    with database.read() as connection:
        assert runner.applied(connection) == []


def test_migration_runner_applied_propagates_unexpected_database_errors():
    class BrokenConnection:
        def execute(self, *args, **kwargs):
            raise RuntimeError("database connection lost")

    with pytest.raises(RuntimeError, match="database connection lost"):
        MigrationRunner().applied(BrokenConnection())


def test_generic_sqlalchemy_database_can_apply_core_migration(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'generic.db'}")

    assert database.apply_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
    ]
    assert database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
    ]
    assert database.apply_migrations() == []
    assert database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
    ]


def test_migration_runner_executes_python_migration_with_semicolons_in_values(tmp_path):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "0001_complex.py").write_text(
        "from sqlalchemy import text\n"
        "\n"
        "def upgrade(connection):\n"
        "    connection.execute(text(\"CREATE TABLE notes (content TEXT NOT NULL)\"))\n"
        "    connection.execute(text(\"INSERT INTO notes(content) VALUES ('remember; this')\"))\n",
        encoding="utf-8",
    )
    database = SQLAlchemyDatabase(
        f"sqlite+pysqlite:///{tmp_path / 'complex.db'}",
        migration_dir=str(migration_dir),
    )

    assert database.apply_migrations() == ["0001_complex"]
    with database.read() as connection:
        assert connection.execute(text("SELECT content FROM notes")).scalar_one() == "remember; this"


def test_generic_sqlalchemy_database_serializes_concurrent_event_appends(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'concurrent-generic.db'}")
    database.apply_migrations()
    store = EventStore(database)

    def append_one(index):
        return store.append(
            "session:generic-concurrent",
            EventType.TOOL_COMPLETED,
            {"index": index},
            "agent:a1",
            request_id=uuid4(),
        ).seq

    with ThreadPoolExecutor(max_workers=8) as executor:
        sequences = list(executor.map(append_one, range(20)))

    assert sorted(sequences) == list(range(1, 21))


def test_memory_sqlite_database_is_shared_across_worker_threads():
    database = SQLiteDatabase(":memory:")
    store = EventStore(database)

    def append_one(index):
        return store.append(
            "session:memory-concurrent",
            EventType.TOOL_COMPLETED,
            {"index": index},
            "agent:a1",
            request_id=uuid4(),
        ).seq

    with ThreadPoolExecutor(max_workers=8) as executor:
        sequences = list(executor.map(append_one, range(20)))

    assert sorted(sequences) == list(range(1, 21))


def test_storage_coordinator_projects_to_multiple_backends():
    coordinator = StorageCoordinator()
    first = RecordingBackend()
    second = RecordingBackend("recording-secondary")
    coordinator.register_backend(first)
    coordinator.register_backend(second)
    event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:s1",
        seq=4,
        actor="agent:codex",
        payload={"exit_code": 0},
    )

    result = coordinator.project(event)

    assert result == {"recording": 4, "recording-secondary": 4}
    assert first.events == [event.event_id]
    assert second.events == [event.event_id]
    assert coordinator.watermarks("session:s1") == {
        "recording": 4,
        "recording-secondary": 4,
    }


def test_projection_backend_is_a_runtime_checkable_contract():
    assert isinstance(RecordingBackend(), ProjectionBackend)
    assert isinstance(RecordingBackend(), EventProjector)


class RecordingVectorIndex:
    name = "vector"

    def upsert(self, memory):
        pass

    def delete(self, memory_id: str) -> None:
        pass

    def health(self) -> bool:
        return True

    def watermark(self) -> int:
        return 0


def test_index_backend_is_not_an_event_projector():
    assert isinstance(RecordingVectorIndex(), VectorIndex)
    assert not isinstance(RecordingVectorIndex(), EventProjector)
    with pytest.raises(TypeError, match="EventProjector"):
        StorageCoordinator().register_backend(RecordingVectorIndex())


def test_event_repository_append_declares_explicit_contract():
    parameters = signature(EventRepository.append).parameters

    assert list(parameters) == [
        "self",
        "stream_id",
        "event_type",
        "payload",
        "actor",
        "request_id",
        "event_id",
        "occurred_at",
        "causation_id",
        "correlation_id",
        "idempotency_key",
    ]
    assert parameters["event_id"].default is None
    assert parameters["occurred_at"].default is None
    assert parameters["causation_id"].default is None
    assert parameters["correlation_id"].default is None
    assert parameters["idempotency_key"].default is None


def test_in_process_projection_dispatcher_has_explicit_name_with_compatibility_alias():
    assert ProjectionDispatcher.__name__ == "ProjectionDispatcher"
    assert StorageCoordinator is ProjectionDispatcher


class BrokenWatermarkBackend(RecordingBackend):
    def __init__(self):
        super().__init__("broken-watermark")

    def watermark(self, stream_id: str) -> int:
        raise RuntimeError("watermark unavailable")


def test_projection_dispatcher_watermarks_isolate_backend_failures():
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(RecordingBackend())
    dispatcher.register_backend(BrokenWatermarkBackend())

    assert dispatcher.watermarks("session:s1") == {"recording": 0}
    assert dispatcher.errors() == {
        "session:s1": {"broken-watermark": "watermark unavailable"}
    }


class BrokenHealthBackend(RecordingBackend):
    def __init__(self):
        super().__init__("broken-health")

    def health(self) -> bool:
        raise RuntimeError("health unavailable")


def test_projection_dispatcher_health_isolates_backend_failures():
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(RecordingBackend())
    dispatcher.register_backend(BrokenHealthBackend())

    assert dispatcher.health() == {"recording": True, "broken-health": False}
    assert dispatcher.errors() == {
        "__system__": {"broken-health": "health unavailable"}
    }


def test_projection_dispatcher_keeps_errors_isolated_between_streams():
    class SelectiveFailureBackend(RecordingBackend):
        def apply(self, event):
            if event.stream_id == "session:error":
                raise RuntimeError("stream projection failed")
            super().apply(event)

    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(SelectiveFailureBackend())

    def event(stream_id):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id=stream_id,
            seq=1,
            actor="agent:codex",
            payload={},
        )

    dispatcher.project(event("session:error"))
    dispatcher.project(event("session:healthy"))

    assert dispatcher.errors() == {
        "session:error": {"recording": "stream projection failed"}
    }
    assert dispatcher.errors("session:error") == {
        "recording": "stream projection failed"
    }


def test_projection_dispatcher_rejects_invalid_backend_and_event_arguments():
    dispatcher = ProjectionDispatcher()

    class InvalidNameBackend(RecordingBackend):
        def __init__(self):
            super().__init__()
            self.name = None

    with pytest.raises(TypeError, match="backend name must be a string"):
        dispatcher.register_backend(InvalidNameBackend())
    with pytest.raises(TypeError, match="event must be an Event"):
        dispatcher.project(None)


def test_projection_dispatcher_bounds_gap_pending_cache_and_supports_explicit_clear(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "gap-limit.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(
        checkpoint_store=checkpoint_store,
        max_pending_events=2,
    )
    backend = RecordingBackend()
    dispatcher.register_backend(backend)

    def event(seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:gap-limit",
            seq=seq,
            actor="agent:codex",
            payload={},
        )

    assert dispatcher.project(event(3)) == {"recording": 0}
    assert dispatcher.project(event(4)) == {"recording": 0}
    assert dispatcher.project(event(5)) == {}
    assert "pending gap buffer full" in dispatcher.errors()["session:gap-limit"]["recording"]
    assert dispatcher.clear_pending("session:gap-limit") == 2
    assert dispatcher.clear_pending("session:gap-limit") == 0


def test_projection_dispatcher_rejects_invalid_pending_capacity():
    with pytest.raises(ValueError, match="max_pending_events must be positive"):
        ProjectionDispatcher(max_pending_events=0)


def test_projection_checkpoint_survives_dispatcher_recreation(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoints.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    first_backend = RecordingBackend()
    dispatcher.register_backend(first_backend)
    event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:checkpoint",
        seq=7,
        actor="agent:codex",
        payload={"exit_code": 0},
    )

    checkpoint_store.save_max("recording", "session:checkpoint", 6)
    dispatcher.project(event)
    assert first_backend.events == [event.event_id]

    recreated = RelationalProjectionCheckpointStore(database)
    assert recreated.get("recording", "session:checkpoint") == 7

    second_backend = RecordingBackend()
    restarted = ProjectionDispatcher(checkpoint_store=recreated)
    restarted.register_backend(second_backend)

    assert restarted.project(event) == {"recording": 7}
    assert second_backend.events == []


def test_projection_checkpoint_is_monotonic(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoint-monotonic.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)

    assert checkpoint_store.save_max("recording", "session:s1", 7) == 7
    assert checkpoint_store.save_max("recording", "session:s1", 5) == 7
    assert checkpoint_store.get("recording", "session:s1") == 7


def test_projection_checkpoint_store_rejects_invalid_identifiers(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoint-invalid.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)

    with pytest.raises(TypeError, match="projection must be a string"):
        checkpoint_store.get(None, "session:s1")
    with pytest.raises(ValueError, match="stream_id must not be blank"):
        checkpoint_store.get("recording", "   ")


def test_projection_watermarks_are_isolated_per_stream():
    dispatcher = ProjectionDispatcher()
    backend = RecordingBackend()
    dispatcher.register_backend(backend)

    def event(stream_id, seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id=stream_id,
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )

    dispatcher.project(event("session:a", 100))
    dispatcher.project(event("session:b", 1))

    assert dispatcher.watermarks("session:a") == {"recording": 100}
    assert dispatcher.watermarks("session:b") == {"recording": 1}


def test_projection_checkpoint_does_not_skip_gaps_in_out_of_order_events(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoint-gap.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    backend = RecordingBackend()
    dispatcher.register_backend(backend)

    def event(seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:gap",
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )

    third = event(3)
    first = event(1)
    second = event(2)

    assert dispatcher.project(third) == {"recording": 0}
    assert backend.events == []
    assert checkpoint_store.get("recording", "session:gap") == 0

    assert dispatcher.project(first) == {"recording": 1}
    assert backend.events == [first.event_id]
    assert dispatcher.project(second) == {"recording": 3}
    assert backend.events == [first.event_id, second.event_id, third.event_id]


def test_projection_checkpoint_save_max_handles_concurrent_initial_insert(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'checkpoint-race.db'}")
    database.apply_migrations()
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    barrier = Barrier(8)

    def save_concurrently(seq):
        barrier.wait(timeout=5)
        return checkpoint_store.save_max("recording", "session:race", seq)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(save_concurrently, range(1, 9)))

    assert max(results) == 8
    assert checkpoint_store.get("recording", "session:race") == 8


def test_projection_dispatcher_replay_from_uses_slowest_registered_checkpoint(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "replay-start.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    checkpoint_store.save_max("fast", "session:replay", 5)
    checkpoint_store.save_max("slow", "session:replay", 3)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(RecordingBackend("fast"))
    dispatcher.register_backend(RecordingBackend("slow"))

    assert dispatcher.replay_from("session:replay") == 3


def test_projection_dispatcher_replay_from_defaults_to_zero_without_checkpoints():
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(RecordingBackend())

    assert dispatcher.replay_from("session:replay") == 0
    with pytest.raises(ValueError, match="stream_id must not be blank"):
        dispatcher.replay_from("   ")


def test_projection_dispatcher_uses_backend_watermark_to_avoid_reapplying_after_checkpoint_failure():
    class FailOnceCheckpointStore:
        def __init__(self):
            self.value = 0
            self.failed = False

        def get(self, projection, stream_id):
            return self.value

        def save_max(self, projection, stream_id, seq):
            if not self.failed:
                self.failed = True
                raise RuntimeError("checkpoint unavailable")
            self.value = max(self.value, seq)
            return self.value

    checkpoint_store = FailOnceCheckpointStore()
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    backend = RecordingBackend()
    dispatcher.register_backend(backend)
    event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:idempotent",
        seq=1,
        actor="agent:codex",
        payload={},
    )

    assert dispatcher.project(event) == {}
    assert dispatcher.project(event) == {"recording": 1}
    assert backend.events == [event.event_id]


def test_projection_dispatcher_serializes_same_backend_and_stream():
    class ConcurrentBackend(RecordingBackend):
        def __init__(self):
            super().__init__()
            self.entered = ThreadEvent()
            self.release = ThreadEvent()
            self.active = 0
            self.max_active = 0
            self.state_lock = Lock()

        def apply(self, event: Event) -> None:
            with self.state_lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                self.entered.set()
            self.release.wait(timeout=5)
            with self.state_lock:
                self.active -= 1
            super().apply(event)

    dispatcher = ProjectionDispatcher()
    backend = ConcurrentBackend()
    dispatcher.register_backend(backend)

    def event(seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:concurrent",
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )

    first = ThreadPoolExecutor(max_workers=2)
    try:
        first_future = first.submit(dispatcher.project, event(1))
        assert backend.entered.wait(timeout=5)
        second_future = first.submit(dispatcher.project, event(2))
        backend.release.set()
        first_future.result(timeout=5)
        second_future.result(timeout=5)
    finally:
        first.shutdown(wait=True)

    assert backend.max_active == 1
