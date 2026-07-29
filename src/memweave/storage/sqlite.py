"""SQLite relational adapter and SQLite-specific transaction settings."""

from contextlib import contextmanager, nullcontext
from pathlib import Path
from threading import RLock
from typing import Iterator, List, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection
from sqlalchemy.pool import StaticPool

from .migrations import MigrationRunner
from .sqlalchemy import SQLAlchemyDatabase


class SQLiteDatabase(SQLAlchemyDatabase):
    def __init__(self, path: str, migration_dir: Optional[str] = None):
        self.path = path
        self._shared_memory = path == ":memory:"
        self._access_lock = RLock()
        if path == ":memory:":
            url = "sqlite+pysqlite:///:memory:"
        else:
            absolute = Path(path).resolve().as_posix()
            url = "sqlite+pysqlite:///" + absolute
        self.url = url
        engine_options = {
            "future": True,
            "connect_args": {"check_same_thread": False, "timeout": 30},
        }
        if self._shared_memory:
            engine_options["poolclass"] = StaticPool
        self.engine = create_engine(url, **engine_options)

        @event.listens_for(self.engine, "connect")
        def _configure_sqlite(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

        self.migrations = MigrationRunner(migration_dir)
        self.apply_migrations()

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        lock = self._access_lock if self._shared_memory else nullcontext()
        with lock:
            connection = self.engine.connect()
            try:
                connection.exec_driver_sql("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @contextmanager
    def read(self) -> Iterator[Connection]:
        lock = self._access_lock if self._shared_memory else nullcontext()
        with lock:
            with self.engine.connect() as connection:
                yield connection

    def transaction(self):
        return self.begin()

    def applied_migrations(self) -> List[str]:
        with self.read() as connection:
            return self.migrations.applied(connection)
