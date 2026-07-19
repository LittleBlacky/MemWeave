"""SQLAlchemy Core relational database foundation."""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from .migrations import MigrationRunner


class SQLAlchemyDatabase:
    def __init__(self, url: str, migration_dir: Optional[str] = None):
        self.url = url
        self.engine: Engine = create_engine(url, future=True)
        default_dir = Path(__file__).resolve().parents[3] / "migrations"
        self.migrations = MigrationRunner(migration_dir or str(default_dir))

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.engine.begin() as connection:
            yield connection

    @contextmanager
    def read(self) -> Iterator[Connection]:
        with self.engine.connect() as connection:
            yield connection

    def apply_migrations(self) -> List[str]:
        with self.begin() as connection:
            return self.migrations.apply(connection)

    def applied_migrations(self) -> List[str]:
        with self.read() as connection:
            return self.migrations.applied(connection)
