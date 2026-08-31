"""Create the durable session working-memory projection table."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import session_states_table


def upgrade(connection: Connection) -> None:
    session_states_table.create(connection, checkfirst=True)
