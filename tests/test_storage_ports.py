from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from uuid import uuid4

import pytest
from sqlalchemy import text

from memweave.events import EventStore
from memweave.models import Event
from memweave.models import EventType
from memweave.storage.coordinator import ProjectionDispatcher, StorageCoordinator
from memweave.storage.migrations import MigrationRunner
from memweave.storage.ports import EventProjector, EventRepository, ProjectionBackend, VectorIndex
from memweave.storage.sqlalchemy import SQLAlchemyDatabase
from memweave.storage.sqlite import SQLiteDatabase


class RecordingBackend:
    def __init__(self, name="recording"):
        self.name = name
        self.events = []
        self.last_seq = 0

    def apply(self, event: Event) -> None:
        self.events.append(event.event_id)
        self.last_seq = event.seq

    def health(self) -> bool:
        return True

    def watermark(self) -> int:
        return self.last_seq


def test_sqlite_database_migrations_are_versioned_and_idempotent(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "memory.db"))

    assert database.applied_migrations() == ["0001_core"]


def test_default_migration_runner_discovers_packaged_migrations():
    runner = MigrationRunner()

    assert list(runner.discover()) == ["0001_core"]


def test_generic_sqlalchemy_database_can_apply_core_migration(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'generic.db'}")

    assert database.apply_migrations() == ["0001_core"]
    assert database.applied_migrations() == ["0001_core"]
    assert database.apply_migrations() == []
    assert database.applied_migrations() == ["0001_core"]


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
    assert coordinator.watermarks() == {"recording": 4, "recording-secondary": 4}


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
