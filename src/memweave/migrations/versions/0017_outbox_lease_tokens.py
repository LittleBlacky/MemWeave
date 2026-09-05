"""Add fencing tokens to outbox tasks and consumer receipts."""

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from memweave.storage.schema import outbox_consumer_receipts_table, outbox_table


def upgrade(connection: Connection) -> None:
    outbox_table.create(connection, checkfirst=True)
    outbox_consumer_receipts_table.create(connection, checkfirst=True)
    inspector = inspect(connection)
    outbox_columns = {
        column["name"] for column in inspector.get_columns("outbox")
    }
    if "lease_token" not in outbox_columns:
        connection.exec_driver_sql(
            "ALTER TABLE outbox ADD COLUMN lease_token VARCHAR(36)"
        )

    receipt_columns = {
        column["name"]
        for column in inspector.get_columns("outbox_consumer_receipts")
    }
    if "lease_token" not in receipt_columns:
        connection.exec_driver_sql(
            "ALTER TABLE outbox_consumer_receipts ADD COLUMN lease_token VARCHAR(36)"
        )
