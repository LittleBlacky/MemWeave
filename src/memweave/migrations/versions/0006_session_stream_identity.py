"""Record the complete stream identity for session projections and leases."""

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from memweave.storage.schema import (
    session_command_leases_table,
    session_states_table,
)


def _add_column_if_missing(connection: Connection, table, column_name: str) -> None:
    existing = {item["name"] for item in inspect(connection).get_columns(table.name)}
    if column_name not in existing:
        # The migration targets fixed, internal identifiers; quote them so the
        # DDL remains valid across SQLite and PostgreSQL-compatible dialects.
        connection.exec_driver_sql(
            f'ALTER TABLE "{table.name}" ADD COLUMN "{column_name}" VARCHAR(512)'
        )


def upgrade(connection: Connection) -> None:
    _add_column_if_missing(connection, session_states_table, "stream_id")
    _add_column_if_missing(connection, session_command_leases_table, "stream_id")
