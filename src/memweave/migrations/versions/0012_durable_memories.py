"""Create the versioned durable memory authority table."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import durable_memories_table


def upgrade(connection: Connection) -> None:
    durable_memories_table.create(connection, checkfirst=True)
