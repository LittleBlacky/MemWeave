"""Create the relational authority tables used by the event store."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import (
    events_table,
    projection_watermarks_table,
    stream_heads_table,
)


def upgrade(connection: Connection) -> None:
    events_table.create(connection, checkfirst=True)
    stream_heads_table.create(connection, checkfirst=True)
    projection_watermarks_table.create(connection, checkfirst=True)
