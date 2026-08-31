"""Create cross-process session command lease state."""

from sqlalchemy.engine import Connection

from memweave.storage.schema import session_command_leases_table


def upgrade(connection: Connection) -> None:
    session_command_leases_table.create(connection, checkfirst=True)
