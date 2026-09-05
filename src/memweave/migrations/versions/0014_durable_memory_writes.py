"""Create the durable memory write-identity registry."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import durable_memory_writes_table


def upgrade(connection: Connection) -> None:
    # Historical source evidence is intentionally not promoted to a write
    # identity because it may be shared by multiple memory versions.
    durable_memory_writes_table.create(connection, checkfirst=True)
