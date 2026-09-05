"""Add immutable request fingerprints to durable write identities."""

from sqlalchemy import inspect
from sqlalchemy.engine import Connection


def upgrade(connection: Connection) -> None:
    columns = {
        column["name"]
        for column in inspect(connection).get_columns("durable_memory_writes")
    }
    if "operation_type" not in columns:
        connection.exec_driver_sql(
            "ALTER TABLE durable_memory_writes "
            "ADD COLUMN operation_type VARCHAR(32)"
        )
    if "request_fingerprint" not in columns:
        connection.exec_driver_sql(
            "ALTER TABLE durable_memory_writes "
            "ADD COLUMN request_fingerprint VARCHAR(64)"
        )
