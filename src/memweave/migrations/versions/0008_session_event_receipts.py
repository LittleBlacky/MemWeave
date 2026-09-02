"""Create durable receipts for applied session events."""

import hashlib
import json
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import inspect, insert, select
from sqlalchemy.engine import Connection

from memweave.models import Event
from memweave.storage.schema import (
    events_table,
    session_event_receipts_table,
    session_states_table,
)


def _storage_session_id(stream_id: str, session_id: str) -> str | None:
    """Resolve a persisted stream identity without requiring a SessionStore."""

    if stream_id == f"session:{session_id}":
        return session_id
    segments = stream_id.split("/")
    if (
        len(segments) == 2
        and segments[0].startswith("tenant:")
        and segments[1] == f"session:{session_id}"
    ):
        return f"{segments[0][len('tenant:') :]}:session:{session_id}"
    if len(segments) >= 3 and segments[0].startswith("tenant:") and segments[-1] == f"session:{session_id}":
        identity = uuid5(NAMESPACE_URL, f"memweave:session-stream:{stream_id}")
        return f"stream:{identity}"
    return None


def _fingerprint(row: dict) -> str:
    payload = json.loads(row["payload_json"])
    values = dict(row)
    values.pop("payload_json", None)
    values["payload"] = payload
    event = Event.model_validate(values)
    values = event.model_dump(mode="json", exclude={"ingested_at"})
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _backfill(connection: Connection) -> None:
    columns = {item["name"] for item in inspect(connection).get_columns(events_table.name)}
    required = {
        "event_id",
        "stream_id",
        "seq",
        "event_type",
        "actor",
        "payload_json",
        "schema_version",
        "protocol_version",
        "request_id",
        "occurred_at",
        "ingested_at",
        "causation_id",
        "correlation_id",
    }
    # Some pre-event-log fixtures only contain stream_id. There is no safe way
    # to reconstruct a receipt without the immutable event fields.
    if not required.issubset(columns):
        return

    state_rows = connection.execute(
        select(
            session_states_table.c.session_id,
            session_states_table.c.stream_id,
            session_states_table.c.last_seq,
        )
    ).mappings()
    for state in state_rows:
        stream_id = state["stream_id"]
        if stream_id is None:
            storage_id = str(state["session_id"])
            if storage_id.startswith("stream:"):
                # 0007 removes ambiguous extended snapshots; they must replay.
                continue
            if ":session:" in storage_id:
                tenant, logical_session = storage_id.split(":session:", 1)
                stream_id = f"tenant:{tenant}/session:{logical_session}"
            else:
                stream_id = f"session:{storage_id}"
        else:
            logical_session = str(stream_id).split("/")[-1]
            if not logical_session.startswith("session:"):
                continue
            logical_session = logical_session[len("session:") :]
            storage_id = _storage_session_id(str(stream_id), logical_session)
            if storage_id is None:
                continue
        events = connection.execute(
            select(events_table).where(
                events_table.c.stream_id == stream_id,
                events_table.c.seq <= int(state["last_seq"]),
            ).order_by(events_table.c.seq)
        ).mappings()
        for event in events:
            connection.execute(
                insert(session_event_receipts_table).values(
                    session_id=storage_id,
                    seq=int(event["seq"]),
                    event_id=str(event["event_id"]),
                    fingerprint=_fingerprint(dict(event)),
                )
            )


def upgrade(connection: Connection) -> None:
    session_event_receipts_table.create(connection, checkfirst=True)
    _backfill(connection)
