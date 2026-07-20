from uuid import uuid4

from memweave.models import Event
from memweave.storage.coordinator import StorageCoordinator
from memweave.storage.ports import ProjectionBackend
from memweave.storage.sqlalchemy import SQLAlchemyDatabase
from memweave.storage.sqlite import SQLiteDatabase


class RecordingBackend:
    def __init__(self, name="recording"):
        self.name = name
        self.events = []
        self.last_seq = 0

    def project(self, event: Event) -> None:
        self.events.append(event.event_id)
        self.last_seq = event.seq

    def health(self) -> bool:
        return True

    def watermark(self) -> int:
        return self.last_seq


def test_sqlite_database_migrations_are_versioned_and_idempotent(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "memory.db"))

    assert database.applied_migrations() == ["0001_core"]


def test_generic_sqlalchemy_database_can_apply_core_migration(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'generic.db'}")

    assert database.apply_migrations() == ["0001_core"]
    assert database.applied_migrations() == ["0001_core"]
    assert database.apply_migrations() == []
    assert database.applied_migrations() == ["0001_core"]


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
