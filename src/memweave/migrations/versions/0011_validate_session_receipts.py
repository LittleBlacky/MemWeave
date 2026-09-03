"""Validate and complete session event receipt backfills from migration 0008."""

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
    """Resolve the persisted session key using the migration 0008 rules."""

    if stream_id == f"session:{session_id}":
        return session_id
    segments = stream_id.split("/")
    if (
        len(segments) == 2
        and segments[0].startswith("tenant:")
        and segments[1] == f"session:{session_id}"
    ):
        return f"{segments[0][len('tenant:') :]}:session:{session_id}"
    if (
        len(segments) >= 3
        and segments[0].startswith("tenant:")
        and segments[-1] == f"session:{session_id}"
    ):
        identity = uuid5(NAMESPACE_URL, f"memweave:session-stream:{stream_id}")
        return f"stream:{identity}"
    return None


def _resolve_state_stream(state: dict) -> tuple[str, str] | None:
    """Return ``(stream_id, storage_session_id)`` for a legacy session row."""

    stream_id = state["stream_id"]
    if stream_id is None:
        storage_id = str(state["session_id"])
        if storage_id.startswith("stream:"):
            # 0007 removes ambiguous extended snapshots; they must replay.
            return None
        if ":session:" in storage_id:
            tenant, logical_session = storage_id.split(":session:", 1)
            stream_id = f"tenant:{tenant}/session:{logical_session}"
        else:
            stream_id = f"session:{storage_id}"
        return stream_id, storage_id

    stream_id = str(stream_id)
    logical_session = stream_id.split("/")[-1]
    if not logical_session.startswith("session:"):
        return None
    logical_session = logical_session[len("session:") :]
    storage_id = _storage_session_id(stream_id, logical_session)
    if storage_id is None:
        return None
    return stream_id, storage_id


def _fingerprint(row: dict) -> str:
    values = dict(row)
    values["payload"] = json.loads(values.pop("payload_json"))
    event = Event.model_validate(values)
    encoded = json.dumps(
        event.model_dump(mode="json", exclude={"ingested_at"}),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validate_and_backfill(connection: Connection) -> None:
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
    columns = {item["name"] for item in inspect(connection).get_columns(events_table.name)}
    # Legacy fixtures may have an events stub without immutable event fields.
    # There is no safe receipt repair to perform for those databases.
    if not required.issubset(columns):
        return

    states = connection.execute(select(session_states_table)).mappings().all()
    for state in states:
        resolved = _resolve_state_stream(dict(state))
        if resolved is None:
            continue
        stream_id, storage_session_id = resolved
        try:
            checkpoint = int(state["last_seq"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "cannot validate session receipts for an invalid session checkpoint: "
                f"session_id={state['session_id']}, last_seq={state['last_seq']}"
            ) from exc
        if checkpoint < 0:
            raise ValueError(
                "cannot validate session receipts for a negative checkpoint: "
                f"session_id={state['session_id']}, stream_id={stream_id}, "
                f"last_seq={checkpoint}"
            )

        events = connection.execute(
            select(events_table)
            .where(
                events_table.c.stream_id == stream_id,
                events_table.c.seq <= checkpoint,
            )
            .order_by(events_table.c.seq)
        ).mappings().all()
        sequences = [int(event["seq"]) for event in events]
        expected = list(range(1, checkpoint + 1))
        if sequences != expected:
            raise ValueError(
                "cannot validate session receipts across an incomplete event stream: "
                f"session_id={state['session_id']}, stream_id={stream_id}, "
                f"last_seq={checkpoint}, observed_sequences={sequences}"
            )

        for event in events:
            seq = int(event["seq"])
            fingerprint = _fingerprint(dict(event))
            existing = connection.execute(
                select(session_event_receipts_table).where(
                    session_event_receipts_table.c.session_id == storage_session_id,
                    session_event_receipts_table.c.seq == seq,
                )
            ).mappings().first()
            if existing is not None:
                if (
                    existing["event_id"] != str(event["event_id"])
                    or existing["fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "conflicting session receipt during validation for "
                        f"session_id={state['session_id']}, stream_id={stream_id}, seq={seq}"
                    )
                continue
            connection.execute(
                insert(session_event_receipts_table).values(
                    session_id=storage_session_id,
                    seq=seq,
                    event_id=str(event["event_id"]),
                    fingerprint=fingerprint,
                )
            )


def upgrade(connection: Connection) -> None:
    session_event_receipts_table.create(connection, checkfirst=True)
    _validate_and_backfill(connection)
