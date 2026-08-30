"""SQLAlchemy Core relational database foundation."""

from contextlib import contextmanager
import time
from typing import Iterator, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError

from .migrations import MigrationRunner


class SQLAlchemyDatabase:
    def __init__(
        self,
        url: str,
        migration_dir: Optional[str] = None,
        max_migration_retries: int = 8,
    ):
        self._validate_retry_count(max_migration_retries)
        self.url = url
        self.engine: Engine = create_engine(url, future=True)
        self.migrations = MigrationRunner(migration_dir)
        self.max_migration_retries = max_migration_retries

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.engine.begin() as connection:
            yield connection

    @contextmanager
    def read(self) -> Iterator[Connection]:
        with self.engine.connect() as connection:
            yield connection

    def apply_migrations(self) -> List[str]:
        for attempt in range(self.max_migration_retries + 1):
            try:
                with self.begin() as connection:
                    return self.migrations.apply(connection)
            except (IntegrityError, OperationalError, ProgrammingError) as exc:
                if (
                    attempt >= self.max_migration_retries
                    or not self._is_retryable_migration_error(exc)
                ):
                    raise
                time.sleep(0.005 * (2**attempt))
        raise AssertionError("unreachable")

    def applied_migrations(self) -> List[str]:
        with self.read() as connection:
            return self.migrations.applied(connection)

    @staticmethod
    def _validate_retry_count(value: int) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("max_migration_retries must be an integer")
        if value < 0:
            raise ValueError("max_migration_retries must not be negative")

    @staticmethod
    def _is_retryable_migration_error(error: Exception) -> bool:
        if isinstance(error, IntegrityError):
            return True
        if isinstance(error, (OperationalError, ProgrammingError)):
            message = str(error).lower()
            return any(
                marker in message
                for marker in (
                    "already exists",
                    "database is locked",
                    "deadlock",
                    "serialization failure",
                    "could not serialize",
                )
            )
        return False
