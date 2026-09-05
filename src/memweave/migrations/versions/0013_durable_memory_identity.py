"""Ensure a memory identity maps to one key within a scope."""

from sqlalchemy import insert, select
from sqlalchemy.engine import Connection

from memweave.storage.schema import (
    durable_memories_table,
    durable_memory_identities_table,
)


def upgrade(connection: Connection) -> None:
    """Create and backfill the identity registry without changing history.

    The version table intentionally contains one row per version, so its
    ``memory_id`` cannot be unique.  The registry carries the identity
    constraint separately and makes pre-existing ambiguity a migration error
    instead of silently selecting a winner.
    """
    durable_memory_identities_table.create(connection, checkfirst=True)

    rows = connection.execute(
        select(
            durable_memories_table.c.scope,
            durable_memories_table.c.scope_id,
            durable_memories_table.c.memory_id,
            durable_memories_table.c.key,
        ).order_by(
            durable_memories_table.c.scope,
            durable_memories_table.c.scope_id,
            durable_memories_table.c.memory_id,
            durable_memories_table.c.version,
        )
    ).mappings().all()

    identities: dict[tuple[str, str, str], str] = {}
    keys: dict[tuple[str, str, str], str] = {}
    for row in rows:
        identity = (row["scope"], row["scope_id"], row["memory_id"])
        key_identity = (row["scope"], row["scope_id"], row["key"])
        key = row["key"]
        previous_key = identities.setdefault(identity, key)
        if previous_key != key:
            raise ValueError(
                "durable memory identity is bound to multiple keys: "
                f"scope={identity[0]!r}, scope_id={identity[1]!r}, "
                f"memory_id={identity[2]!r}"
            )
        previous_memory_id = keys.setdefault(key_identity, row["memory_id"])
        if previous_memory_id != row["memory_id"]:
            raise ValueError(
                "durable memory key is bound to multiple memory identities: "
                f"scope={key_identity[0]!r}, scope_id={key_identity[1]!r}, "
                f"key={key_identity[2]!r}"
            )

    for (scope, scope_id, memory_id), key in identities.items():
        connection.execute(
            insert(durable_memory_identities_table).values(
                scope=scope,
                scope_id=scope_id,
                memory_id=memory_id,
                key=key,
            )
        )
