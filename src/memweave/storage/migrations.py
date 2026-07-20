"""Versioned SQL migration runner."""

from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable, List

from sqlalchemy import insert, select
from sqlalchemy.engine import Connection

from .schema import schema_migrations_table

class MigrationRunner:
    def __init__(self, directory: str):
        self.directory = Path(directory)

    def discover(self) -> Iterable[Path]:
        return sorted(self.directory.glob("[0-9][0-9][0-9][0-9]_*.sql"))

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
            script = path.read_text(encoding="utf-8")
            for statement in script.split(";"):
                statement = statement.strip()
                if statement:
                    connection.exec_driver_sql(statement)
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
