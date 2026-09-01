"""Reset ambiguous legacy session projections for authoritative replay."""

from sqlalchemy import delete, select
from sqlalchemy.engine import Connection

from memweave.storage.schema import (
    events_table,
    projection_watermarks_table,
    session_command_leases_table,
    session_states_table,
)


def upgrade(connection: Connection) -> None:
    # Hash-keyed rows created before stream_id was persisted cannot be assigned
    # safely to a project. Remove them so the authoritative stream replays.
    connection.execute(
        delete(session_command_leases_table).where(
            session_command_leases_table.c.session_id.like("stream:%"),
            session_command_leases_table.c.stream_id.is_(None),
        )
    )
    connection.execute(
        delete(session_states_table).where(
            session_states_table.c.session_id.like("stream:%"),
            session_states_table.c.stream_id.is_(None),
        )
    )

    extended_streams: set[str] = set()
    canonical_storage_ids: set[str] = set()
    canonical_streams: set[str] = set()
    extended_stream_query = (
        select(events_table.c.stream_id)
        .where(events_table.c.stream_id.like("%/session:%"))
        .distinct()
    )
    for (stream_id,) in connection.execute(extended_stream_query):
        segments = stream_id.split("/")
        if (
            len(segments) < 3
            or not segments[0].startswith("tenant:")
            or not segments[-1].startswith("session:")
        ):
            continue
        tenant_id = segments[0][len("tenant:") :]
        session_id = segments[-1][len("session:") :]
        if not tenant_id or not session_id:
            continue
        extended_streams.add(stream_id)
        canonical_storage_ids.add(f"{tenant_id}:session:{session_id}")
        canonical_streams.add(f"tenant:{tenant_id}/session:{session_id}")

    if canonical_storage_ids:
        connection.execute(
            delete(session_command_leases_table).where(
                session_command_leases_table.c.session_id.in_(canonical_storage_ids),
            )
        )
        connection.execute(
            delete(session_states_table).where(
                session_states_table.c.session_id.in_(canonical_storage_ids),
            )
        )

    streams_to_replay = extended_streams | canonical_streams
    if streams_to_replay:
        connection.execute(
            delete(projection_watermarks_table).where(
                projection_watermarks_table.c.stream_id.in_(streams_to_replay)
            )
        )
