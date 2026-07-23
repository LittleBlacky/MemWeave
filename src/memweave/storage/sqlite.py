"""SQLite relational adapter and SQLite-specific transaction settings."""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection

from .migrations import MigrationRunner
from .sqlalchemy import SQLAlchemyDatabase


class SQLiteDatabase(SQLAlchemyDatabase):
    def __init__(self, path: str, migration_dir: Optional[str] = None):
        self.path = path
        if path == ":memory:":
            url = "sqlite+pysqlite:///:memory:"
        else:
            absolute = Path(path).resolve().as_posix()
            url = "sqlite+pysqlite:///" + absolute
        self.url = url
        self.engine = create_engine(
            url,
            future=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )

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

    def transaction(self):
        return self.begin()

    def applied_migrations(self) -> List[str]:
        with self.read() as connection:
            return self.migrations.applied(connection)
