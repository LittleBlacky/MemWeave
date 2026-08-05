"""Create the durable outbox consumer receipt table."""

from memweave.storage.schema import outbox_consumer_receipts_table


def upgrade(connection):
    outbox_consumer_receipts_table.create(connection, checkfirst=True)
