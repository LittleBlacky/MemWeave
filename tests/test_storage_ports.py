from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from threading import Barrier, Event as ThreadEvent, Lock
from uuid import uuid4

import pytest
from sqlalchemy import delete, inspect, select, text
from sqlalchemy.exc import ProgrammingError

from memweave.events import EventStore
from memweave.models import Event
from memweave.models import EventType
from memweave.storage.checkpoints import RelationalProjectionCheckpointStore
from memweave.storage.coordinator import ProjectionDispatcher, StorageCoordinator
from memweave.storage.event_receipts import event_fingerprint
from memweave.storage.migrations import MigrationRunner
from memweave.storage.ports import EventProjector, EventRepository, ProjectionBackend, VectorIndex
from memweave.storage.sqlalchemy import SQLAlchemyDatabase
from memweave.storage.schema import (
    durable_memories_table,
    durable_memory_identities_table,
    events_table,
    projection_event_receipts_table,
    projection_watermarks_table,
    schema_migrations_table,
)
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
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
    ]


def test_default_migration_runner_discovers_packaged_migrations():
    runner = MigrationRunner()

    assert list(runner.discover()) == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
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


def test_migration_runner_applied_handles_cross_database_missing_table_errors():
    class UnmigratedConnection:
        def execute(self, *args, **kwargs):
            raise ProgrammingError(
                "SELECT ...",
                {},
                Exception('relation "schema_migrations" does not exist'),
            )

    assert MigrationRunner().applied(UnmigratedConnection()) == []


def test_generic_sqlalchemy_database_can_apply_core_migration(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'generic.db'}")

    assert database.apply_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
    ]
    assert database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
    ]


def test_stream_identity_migration_upgrades_legacy_session_tables(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'legacy.db'}")
    with database.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE session_states ("
            "session_id VARCHAR(255) PRIMARY KEY, last_seq INTEGER NOT NULL, "
            "recent_messages_json TEXT NOT NULL, active_memories_json TEXT NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE session_command_leases ("
            "session_id VARCHAR(255) PRIMARY KEY, owner_id VARCHAR(255) NOT NULL, "
            "lease_until FLOAT NOT NULL, fencing_token INTEGER NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE schema_migrations (version VARCHAR(255) PRIMARY KEY, applied_at VARCHAR(64) NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE events (stream_id VARCHAR(255) NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE projection_watermarks (projection VARCHAR(255) NOT NULL, stream_id VARCHAR(255) NOT NULL, last_seq INTEGER NOT NULL)"
        )
        connection.execute(
            text(
                "INSERT INTO schema_migrations(version, applied_at) VALUES "
                "('0001_core', 'now'), ('0002_outbox', 'now'), "
                "('0003_outbox_consumer_receipts', 'now'), ('0004_session_states', 'now'), "
                "('0005_session_command_leases', 'now')"
            )
        )

    assert database.apply_migrations() == [
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
    ]
    with database.read() as connection:
        assert "stream_id" in {
            column["name"]
            for column in inspect(connection).get_columns("session_states")
        }
        assert "stream_id" in {
            column["name"]
            for column in inspect(connection).get_columns("session_command_leases")
        }


def test_durable_identity_migration_backfills_versions_and_rejects_conflicts(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'identity.db'}")
    database.apply_migrations()
    with database.begin() as connection:
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version == "0013_durable_memory_identity"
            )
        )
        durable_memory_identities_table.drop(connection)
        connection.execute(
            durable_memories_table.insert(),
            [
                {
                    "memory_id": "memory-1",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.engine",
                    "version": 1,
                    "kind": "fact",
                    "value_json": '"PostgreSQL"',
                    "status": "superseded",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-1"]}',
                    "source_seq": 1,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
                {
                    "memory_id": "memory-1",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.engine",
                    "version": 2,
                    "kind": "fact",
                    "value_json": '"SQLite"',
                    "status": "active",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-2"]}',
                    "source_seq": 2,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
            ],
        )

    assert database.apply_migrations() == ["0013_durable_memory_identity"]
    with database.read() as connection:
        identity = connection.execute(
            select(durable_memory_identities_table)
        ).mappings().all()
    assert [dict(row) for row in identity] == [
        {
            "scope": "user",
            "scope_id": "u1",
            "memory_id": "memory-1",
            "key": "database.engine",
        }
    ]

    conflict_database = SQLAlchemyDatabase(
        f"sqlite+pysqlite:///{tmp_path / 'identity-conflict.db'}"
    )
    conflict_database.apply_migrations()
    with conflict_database.begin() as connection:
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version == "0013_durable_memory_identity"
            )
        )
        durable_memory_identities_table.drop(connection)
        connection.execute(
            durable_memories_table.insert(),
            [
                {
                    "memory_id": "memory-1",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.engine",
                    "version": 1,
                    "kind": "fact",
                    "value_json": '"PostgreSQL"',
                    "status": "active",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-1"]}',
                    "source_seq": 1,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
                {
                    "memory_id": "memory-1",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.host",
                    "version": 1,
                    "kind": "fact",
                    "value_json": '"db"',
                    "status": "active",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-2"]}',
                    "source_seq": 2,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
            ],
        )

    with pytest.raises(ValueError, match="bound to multiple keys"):
        conflict_database.apply_migrations()

    assert conflict_database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
    ]

    key_conflict_database = SQLAlchemyDatabase(
        f"sqlite+pysqlite:///{tmp_path / 'identity-key-conflict.db'}"
    )
    key_conflict_database.apply_migrations()
    with key_conflict_database.begin() as connection:
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version == "0013_durable_memory_identity"
            )
        )
        durable_memory_identities_table.drop(connection)
        connection.execute(
            durable_memories_table.insert(),
            [
                {
                    "memory_id": "memory-1",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.engine",
                    "version": 1,
                    "kind": "fact",
                    "value_json": '"PostgreSQL"',
                    "status": "active",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-1"]}',
                    "source_seq": 1,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
                {
                    "memory_id": "memory-2",
                    "scope": "user",
                    "scope_id": "u1",
                    "key": "database.engine",
                    "version": 2,
                    "kind": "fact",
                    "value_json": '"SQLite"',
                    "status": "active",
                    "confidence": 1.0,
                    "source_json": '{"type":"explicit","event_ids":["event-2"]}',
                    "source_seq": 2,
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
            ],
        )

    with pytest.raises(ValueError, match="multiple memory identities"):
        key_conflict_database.apply_migrations()


def test_projection_receipt_backfill_rejects_incomplete_event_stream(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "projection-receipt-gap.db"))
    events = EventStore(database)
    events.append(
        "session:gap-migration",
        EventType.USER_MESSAGE,
        {"text": "one"},
        "user:test",
        request_id=uuid4(),
    )
    events.append(
        "session:gap-migration",
        EventType.USER_MESSAGE,
        {"text": "two"},
        "user:test",
        request_id=uuid4(),
    )
    third = events.append(
        "session:gap-migration",
        EventType.USER_MESSAGE,
        {"text": "three"},
        "user:test",
        request_id=uuid4(),
    )
    with database.begin() as connection:
        connection.execute(
            delete(events_table).where(
                events_table.c.stream_id == "session:gap-migration",
                events_table.c.seq == 2,
            )
        )
        connection.execute(
            projection_watermarks_table.insert().values(
                projection="recording",
                stream_id="session:gap-migration",
                last_seq=third.seq,
            )
        )
        connection.execute(
            delete(projection_event_receipts_table).where(
                projection_event_receipts_table.c.stream_id == "session:gap-migration"
            )
        )
        connection.execute(
            delete(schema_migrations_table).where(
                schema_migrations_table.c.version.in_([
                    "0009_projection_event_receipts",
                    "0010_validate_projection_receipts",
                ])
            )
        )

    with pytest.raises(ValueError, match="incomplete event stream"):
        database.apply_migrations()

    with database.read() as connection:
        assert connection.execute(
            text(
                "SELECT COUNT(*) FROM projection_event_receipts "
                "WHERE stream_id = 'session:gap-migration'"
            )
        ).scalar_one() == 0
        assert connection.execute(
            select(schema_migrations_table.c.version).where(
                schema_migrations_table.c.version == "0009_projection_event_receipts"
            )
        ).scalar_one_or_none() is None


def test_stream_recovery_migration_resets_only_ambiguous_sessions(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'recovery.db'}")
    with database.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE session_states ("
            "session_id VARCHAR(255) PRIMARY KEY, stream_id VARCHAR(512), "
            "last_seq INTEGER NOT NULL, recent_messages_json TEXT NOT NULL, "
            "active_memories_json TEXT NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE session_command_leases ("
            "session_id VARCHAR(255) PRIMARY KEY, stream_id VARCHAR(512), "
            "owner_id VARCHAR(255) NOT NULL, lease_until FLOAT NOT NULL, "
            "fencing_token INTEGER NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE schema_migrations (version VARCHAR(255) PRIMARY KEY, applied_at VARCHAR(64) NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE events (stream_id VARCHAR(255) NOT NULL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE projection_watermarks (projection VARCHAR(255) NOT NULL, stream_id VARCHAR(255) NOT NULL, last_seq INTEGER NOT NULL)"
        )
        connection.execute(
            text(
                "INSERT INTO schema_migrations(version, applied_at) VALUES "
                "('0001_core', 'now'), ('0002_outbox', 'now'), "
                "('0003_outbox_consumer_receipts', 'now'), ('0004_session_states', 'now'), "
                "('0005_session_command_leases', 'now'), "
                "('0006_session_stream_identity', 'now')"
            )
        )
        connection.exec_driver_sql(
            "INSERT INTO session_states(session_id, stream_id, last_seq, recent_messages_json, active_memories_json) "
            "VALUES ('stream:legacy', NULL, 3, '[]', '[]'), "
            "('t:session:s', 'tenant:t/session:s', 3, "
            "'[{\"payload\":{\"text\":\"project data\"}}]', '[]'), "
            "('t:session:plain', 'tenant:t/session:plain', 2, '[]', '[]')"
        )
        connection.exec_driver_sql(
            "INSERT INTO session_command_leases(session_id, stream_id, owner_id, lease_until, fencing_token) "
            "VALUES ('stream:legacy', NULL, 'old', 0, 1), "
            "('t:session:s', 'tenant:t/session:s', 'new', 10, 2)"
        )
        connection.exec_driver_sql(
            "INSERT INTO projection_watermarks(projection, stream_id, last_seq) VALUES "
            "('custom-session', 'tenant:t/project:p/session:s', 3), "
            "('custom-session', 'tenant:t/session:s', 3), "
            "('other', 'unrelated:stream', 7)"
        )
        connection.exec_driver_sql(
            "INSERT INTO events(stream_id) VALUES "
            "('tenant:t/project:p/session:s'), ('tenant:t/session:s'), "
            "('tenant:t/session:plain')"
        )

    assert database.apply_migrations() == ["0007_session_stream_recovery", "0008_session_event_receipts", "0009_projection_event_receipts", "0010_validate_projection_receipts", "0011_validate_session_receipts", "0012_durable_memories", "0013_durable_memory_identity"]
    with database.read() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM session_states WHERE session_id = 'stream:legacy'")
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT COUNT(*) FROM session_command_leases WHERE session_id = 'stream:legacy'")
        ).scalar_one() == 0
        assert connection.execute(
            text(
                "SELECT COUNT(*) FROM projection_watermarks "
                "WHERE stream_id IN ('tenant:t/project:p/session:s', 'tenant:t/session:s')"
            )
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT COUNT(*) FROM session_states WHERE session_id = 't:session:s'")
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT COUNT(*) FROM session_command_leases WHERE session_id = 't:session:s'")
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT COUNT(*) FROM session_states WHERE session_id = 't:session:plain'")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT last_seq FROM projection_watermarks WHERE projection = 'other'")
        ).scalar_one() == 7


def test_generic_sqlalchemy_database_migrations_are_safe_under_concurrent_startup(tmp_path):
    database = SQLAlchemyDatabase(f"sqlite+pysqlite:///{tmp_path / 'migration-race.db'}")
    barrier = Barrier(8)

    def apply_migrations(_):
        barrier.wait(timeout=5)
        return database.apply_migrations()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(apply_migrations, range(8)))

    assert sum(result == ["0001_core", "0002_outbox", "0003_outbox_consumer_receipts", "0004_session_states", "0005_session_command_leases", "0006_session_stream_identity", "0007_session_stream_recovery", "0008_session_event_receipts", "0009_projection_event_receipts", "0010_validate_projection_receipts", "0011_validate_session_receipts", "0012_durable_memories", "0013_durable_memory_identity"] for result in results) == 1
    assert sum(result == [] for result in results) == 7
    assert database.apply_migrations() == []
    assert database.applied_migrations() == [
        "0001_core",
        "0002_outbox",
        "0003_outbox_consumer_receipts",
        "0004_session_states",
        "0005_session_command_leases",
        "0006_session_stream_identity",
        "0007_session_stream_recovery",
        "0008_session_event_receipts",
        "0009_projection_event_receipts",
        "0010_validate_projection_receipts",
        "0011_validate_session_receipts",
        "0012_durable_memories",
        "0013_durable_memory_identity",
    ]


def test_migration_retry_recognizes_cross_database_already_exists_errors():
    already_exists = ProgrammingError(
        "CREATE TABLE schema_migrations",
        {},
        Exception('relation "schema_migrations" already exists'),
    )
    syntax_error = ProgrammingError(
        "CREATE TABLE schema_migrations",
        {},
        Exception("syntax error at or near TABLE"),
    )

    assert SQLAlchemyDatabase._is_retryable_migration_error(already_exists)
    assert not SQLAlchemyDatabase._is_retryable_migration_error(syntax_error)


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
        "protocol_version",
    ]
    assert parameters["event_id"].default is None
    assert parameters["occurred_at"].default is None
    assert parameters["causation_id"].default is None
    assert parameters["correlation_id"].default is None
    assert parameters["idempotency_key"].default is None
    assert parameters["protocol_version"].default == "1.0"


def test_in_process_projection_dispatcher_has_explicit_name_with_compatibility_alias():
    assert ProjectionDispatcher.__name__ == "ProjectionDispatcher"
    assert StorageCoordinator is ProjectionDispatcher


class BrokenWatermarkBackend(RecordingBackend):
    def __init__(self):
        super().__init__("broken-watermark")

    def watermark(self, stream_id: str) -> int:
        raise RuntimeError("watermark unavailable")


def test_projection_dispatcher_clears_watermark_error_after_recovery():
    class FlakyWatermarkBackend(RecordingBackend):
        def __init__(self):
            super().__init__("flaky-watermark")
            self.failed = True

        def watermark(self, stream_id: str) -> int:
            if self.failed:
                raise RuntimeError("watermark unavailable")
            return super().watermark(stream_id)

    dispatcher = ProjectionDispatcher()
    backend = FlakyWatermarkBackend()
    dispatcher.register_backend(backend)

    assert dispatcher.watermarks("session:flaky") == {}
    assert dispatcher.errors("session:flaky") == {
        "flaky-watermark": "watermark unavailable"
    }
    backend.failed = False
    assert dispatcher.watermarks("session:flaky") == {"flaky-watermark": 0}
    assert dispatcher.errors("session:flaky") == {}


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


def test_projection_dispatcher_clears_health_error_after_recovery():
    class FlakyHealthBackend(RecordingBackend):
        def __init__(self):
            super().__init__("flaky-health")
            self.failed = True

        def health(self) -> bool:
            if self.failed:
                raise RuntimeError("health unavailable")
            return True

    dispatcher = ProjectionDispatcher()
    backend = FlakyHealthBackend()
    dispatcher.register_backend(backend)

    assert dispatcher.health() == {"flaky-health": False}
    assert dispatcher.errors() == {
        "__system__": {"flaky-health": "health unavailable"}
    }
    backend.failed = False
    assert dispatcher.health() == {"flaky-health": True}
    assert dispatcher.errors() == {}


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


def test_projection_dispatcher_rejects_checkpoint_store_without_receipts():
    class LegacyCheckpointStore:
        def get(self, projection, stream_id):
            return 0

        def save_max(self, projection, stream_id, seq):
            return seq

    with pytest.raises(TypeError, match="ProjectionCheckpointReceiptStore"):
        ProjectionDispatcher(checkpoint_store=LegacyCheckpointStore())


def test_projection_dispatcher_rejects_receipt_only_checkpoint_store():
    class ReceiptOnlyCheckpointStore:
        def receipts_complete(self, projection, stream_id, through_seq):
            return True

        def get_receipt(self, projection, stream_id, seq):
            return None

        def save_receipt(self, projection, stream_id, seq, event_id, fingerprint):
            return None

    with pytest.raises(TypeError, match="ProjectionCheckpointReceiptStore"):
        ProjectionDispatcher(checkpoint_store=ReceiptOnlyCheckpointStore())


def test_projection_dispatcher_distinguishes_invalid_stream_id_types(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "dispatcher-invalid-stream.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(RecordingBackend())

    for method in (dispatcher.watermarks, dispatcher.replay_from, dispatcher.clear_pending):
        with pytest.raises(TypeError, match="stream_id must be a string"):
            method(None)
    with pytest.raises(TypeError, match="stream_id must be a string"):
        dispatcher.errors(123)


def test_projection_dispatcher_reclaims_completed_stream_state(tmp_path):
    import gc

    database = SQLiteDatabase(str(tmp_path / "dispatcher-state-cleanup.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(RecordingBackend())

    event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:one-shot",
        seq=1,
        actor="agent:codex",
        payload={},
    )
    assert dispatcher.project(event) == {"recording": 1}
    assert dispatcher._pending == {}
    gc.collect()
    assert len(dispatcher._projection_locks) == 0


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

    third = event(3)
    assert dispatcher.project(third) == {"recording": 0}
    assert dispatcher.project(event(4)) == {"recording": 0}
    assert dispatcher.project(event(5)) == {}
    assert "pending gap buffer full" in dispatcher.errors()["session:gap-limit"]["recording"]
    assert dispatcher.clear_pending("session:gap-limit") == 2
    assert dispatcher.clear_pending("session:gap-limit") == 0


def test_projection_dispatcher_accepts_gap_filling_events_when_pending_is_full(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "gap-fill.db"))
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
            stream_id="session:gap-fill",
            seq=seq,
            actor="agent:codex",
            payload={},
        )

    dispatcher.project(event(3))
    dispatcher.project(event(4))
    assert dispatcher.project(event(1)) == {"recording": 1}
    assert dispatcher.project(event(2)) == {"recording": 4}
    assert len(backend.events) == 4
    assert checkpoint_store.get("recording", "session:gap-fill") == 4


def test_projection_dispatcher_bounds_pending_events_across_all_streams(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "gap-global-limit.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(
        checkpoint_store=checkpoint_store,
        max_pending_events=10,
        max_pending_events_total=2,
    )
    dispatcher.register_backend(RecordingBackend())

    def event(stream_id, seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id=stream_id,
            seq=seq,
            actor="agent:codex",
            payload={},
        )

    dispatcher.project(event("session:one", 3))
    dispatcher.project(event("session:two", 3))
    assert dispatcher.project(event("session:three", 3)) == {}
    assert "total pending gap buffer full" in dispatcher.errors()["session:three"]["recording"]
    assert dispatcher.clear_pending("session:one") == 1
    assert dispatcher.project(event("session:three", 3)) == {"recording": 0}


def test_projection_dispatcher_rejects_invalid_total_pending_capacity():
    with pytest.raises(ValueError, match="max_pending_events_total must be positive"):
        ProjectionDispatcher(max_pending_events_total=0)


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
    second_backend.last_seq[event.stream_id] = 7
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


def test_projection_dispatcher_rejects_conflicting_duplicate_pending_sequence(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "pending-sequence-conflict.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(RecordingBackend())

    def event(text):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:conflict",
            seq=2,
            actor="agent:codex",
            payload={"text": text},
        )

    assert dispatcher.project(event("first")) == {"recording": 0}
    assert dispatcher.project(event("second")) == {}
    assert "conflicting events for stream_id=session:conflict, seq=2" in dispatcher.errors()[
        "session:conflict"
    ]["recording"]


def test_projection_dispatcher_rejects_conflicting_event_after_checkpoint(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoint-sequence-conflict.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    backend = RecordingBackend()
    dispatcher.register_backend(backend)

    first = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:checkpoint-conflict",
        seq=1,
        actor="agent:codex",
        payload={"value": "first"},
    )
    conflict = first.model_copy(
        update={"event_id": uuid4(), "payload": {"value": "conflict"}}
    )

    assert dispatcher.project(first) == {"recording": 1}
    assert dispatcher.project(conflict) == {}
    assert "conflicting event for projection=recording" in dispatcher.errors()[
        "session:checkpoint-conflict"
    ]["recording"]
    assert backend.events == [first.event_id]


def test_projection_dispatcher_fails_closed_when_checkpoint_receipt_is_missing(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "checkpoint-missing-receipt.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(RecordingBackend())
    event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:missing-receipt",
        seq=1,
        actor="agent:codex",
        payload={},
    )

    checkpoint_store.save_max("recording", event.stream_id, 1)
    backend = dispatcher._backends["recording"]
    backend.last_seq[event.stream_id] = 1
    assert dispatcher.project(event) == {}
    assert "checkpoint receipt is missing" in dispatcher.errors()[
        event.stream_id
    ]["recording"]


def test_projection_dispatcher_clears_pending_events_covered_by_external_checkpoint(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "pending-checkpoint-cleanup.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    backend = RecordingBackend()
    dispatcher.register_backend(backend)

    def event(seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:cleanup",
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )

    third = event(3)
    assert dispatcher.project(third) == {"recording": 0}
    assert dispatcher._pending_count == 1

    checkpoint_store.save_receipt(
        "recording",
        "session:cleanup",
        3,
        str(third.event_id),
        event_fingerprint(third),
    )
    checkpoint_store.save_max("recording", "session:cleanup", 3)
    backend.last_seq["session:cleanup"] = 3

    assert dispatcher.project(third) == {"recording": 3}
    assert dispatcher._pending == {}
    assert dispatcher._pending_count == 0


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
    fast = RecordingBackend("fast")
    slow = RecordingBackend("slow")
    fast.last_seq["session:replay"] = 5
    slow.last_seq["session:replay"] = 3
    dispatcher.register_backend(fast)
    dispatcher.register_backend(slow)

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
            self.receipts = {}

        def get(self, projection, stream_id):
            return self.value

        def save_max(self, projection, stream_id, seq):
            if not self.failed:
                self.failed = True
                raise RuntimeError("checkpoint unavailable")
            self.value = max(self.value, seq)
            return self.value

        def receipts_complete(self, projection, stream_id, through_seq):
            return all(
                (projection, stream_id, seq) in self.receipts
                for seq in range(1, through_seq + 1)
            )

        def get_receipt(self, projection, stream_id, seq):
            return self.receipts.get((projection, stream_id, seq))

        def save_receipt(self, projection, stream_id, seq, event_id, fingerprint):
            key = (projection, stream_id, seq)
            existing = self.receipts.get(key)
            if existing is not None and existing != (event_id, fingerprint):
                raise ValueError("conflicting projection receipt")
            self.receipts[key] = (event_id, fingerprint)

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
