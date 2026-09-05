"""Make durable write event identities unique across all source streams."""

from sqlalchemy import func, select
from sqlalchemy.engine import Connection

from memweave.storage.schema import (
    durable_memory_write_event_id_index,
    durable_memory_writes_table,
)


def upgrade(connection: Connection) -> None:
    duplicates = connection.execute(
        select(durable_memory_writes_table.c.write_event_id)
        .group_by(durable_memory_writes_table.c.write_event_id)
        .having(func.count() > 1)
        .limit(1)
    ).first()
    if duplicates is not None:
        raise ValueError(
            "cannot enforce global durable write event identity: "
            f"duplicate write_event_id {duplicates[0]!r} already exists"
        )
    durable_memory_write_event_id_index.create(connection, checkfirst=True)
