"""Versioned Python migration runner."""

import importlib.util
import importlib
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Iterable, List
from importlib.resources import files

from sqlalchemy import insert, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError, ProgrammingError

from .schema import schema_migrations_table

class MigrationRunner:
    def __init__(
        self,
        directory: str | None = None,
        package: str = "memweave.migrations.versions",
    ):
        self.directory = Path(directory) if directory is not None else None
        self.package = package

    def discover(self) -> Iterable[str]:
        if self.directory is not None:
            return [
                path.stem
                for path in sorted(self.directory.glob("[0-9][0-9][0-9][0-9]_*.py"))
            ]
        return sorted(
            resource.name[:-3]
            for resource in files(self.package).iterdir()
            if resource.name.endswith(".py")
            and re.match(r"^[0-9]{4}_.+\.py$", resource.name)
        )

    @staticmethod
    def _load_upgrade_from_path(path: Path) -> Callable[[Connection], None]:
        module_name = f"_memweave_migration_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load migration module: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        upgrade = getattr(module, "upgrade", None)
        if not callable(upgrade):
            raise ValueError(f"migration {path.name} must define upgrade(connection)")
        return upgrade

    def _load_upgrade(self, version: str) -> Callable[[Connection], None]:
        if self.directory is not None:
            return self._load_upgrade_from_path(self.directory / f"{version}.py")
        module = importlib.import_module(f"{self.package}.{version}")
        upgrade = getattr(module, "upgrade", None)
        if not callable(upgrade):
            raise ValueError(f"migration {version} must define upgrade(connection)")
        return upgrade

    def apply(self, connection: Connection) -> List[str]:
        schema_migrations_table.create(connection, checkfirst=True)
        applied = {
            row[0]
            for row in connection.execute(
                select(schema_migrations_table.c.version).order_by(schema_migrations_table.c.version)
            ).fetchall()
        }
        applied_now: List[str] = []
        for version in self.discover():
            if version in applied:
                continue
            self._load_upgrade(version)(connection)
            connection.execute(
                insert(schema_migrations_table).values(
                    version=version,
                    applied_at=datetime.now(timezone.utc).isoformat(),
                )
            )
            applied_now.append(version)
        return applied_now

    def applied(self, connection: Connection) -> List[str]:
        try:
            rows = connection.execute(
                select(schema_migrations_table.c.version).order_by(schema_migrations_table.c.version)
            ).fetchall()
        except (OperationalError, ProgrammingError) as exc:
            if self._is_missing_migrations_table(exc):
                return []
            raise
        return [row[0] for row in rows]

    @staticmethod
    def _is_missing_migrations_table(error: OperationalError | ProgrammingError) -> bool:
        message = str(error).lower()
        return any(
            marker in message
            for marker in (
                "no such table",
                "table schema_migrations does not exist",
                "relation \"schema_migrations\" does not exist",
            )
        )
