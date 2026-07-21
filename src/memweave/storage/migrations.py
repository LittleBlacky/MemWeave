"""Versioned Python migration runner."""

import importlib.util
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Iterable, List

from sqlalchemy import insert, select
from sqlalchemy.engine import Connection

from .schema import schema_migrations_table

class MigrationRunner:
    def __init__(self, directory: str):
        self.directory = Path(directory)

    def discover(self) -> Iterable[Path]:
        return sorted(
            path
            for path in self.directory.glob("[0-9][0-9][0-9][0-9]_*.py")
            if path.name != "__init__.py"
        )

    @staticmethod
    def _load_upgrade(path: Path) -> Callable[[Connection], None]:
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

    def apply(self, connection: Connection) -> List[str]:
        schema_migrations_table.create(connection, checkfirst=True)
        applied = {
            row[0]
            for row in connection.execute(
                select(schema_migrations_table.c.version).order_by(schema_migrations_table.c.version)
            ).fetchall()
        }
        applied_now: List[str] = []
        for path in self.discover():
            version = path.stem
            if version in applied:
                continue
            self._load_upgrade(path)(connection)
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
        except Exception:
            return []
        return [row[0] for row in rows]
