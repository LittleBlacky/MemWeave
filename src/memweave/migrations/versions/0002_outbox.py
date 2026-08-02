"""Create the durable outbox task table."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import outbox_table


def upgrade(connection: Connection) -> None:
    outbox_table.create(connection, checkfirst=True)
