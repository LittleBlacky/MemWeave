"""Versioned SQL migration runner."""

from pathlib import Path
from typing import Iterable, List

from sqlalchemy.engine import Connection


class MigrationRunner:
    def __init__(self, directory: str):
        self.directory = Path(directory)

    def discover(self) -> Iterable[Path]:
        return sorted(self.directory.glob("[0-9][0-9][0-9][0-9]_*.sql"))

    def apply(self, connection: Connection) -> List[str]:
        connection.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = {
            row[0]
            for row in connection.exec_driver_sql(
                "SELECT version FROM schema_migrations ORDER BY version"
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
            connection.exec_driver_sql(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, CURRENT_TIMESTAMP)",
                (version,),
            )
            applied_now.append(version)
        return applied_now

    def applied(self, connection: Connection) -> List[str]:
        try:
            rows = connection.exec_driver_sql(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        except Exception:
            return []
        return [row[0] for row in rows]
