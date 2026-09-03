"""Validate and complete projection receipt backfills from migration 0009."""

import hashlib
import json

from sqlalchemy import inspect, insert, select
from sqlalchemy.engine import Connection

from memweave.models import Event
from memweave.storage.schema import (
    events_table,
    projection_event_receipts_table,
    projection_watermarks_table,
)


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

    watermarks = connection.execute(select(projection_watermarks_table)).mappings().all()
    for watermark in watermarks:
        projection = str(watermark["projection"])
        stream_id = str(watermark["stream_id"])
        checkpoint = int(watermark["last_seq"])
        if checkpoint < 0:
            raise ValueError(
                "cannot validate projection receipts for a negative checkpoint: "
                f"projection={projection}, stream_id={stream_id}, checkpoint={checkpoint}"
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
                "cannot validate projection receipts across an incomplete event stream: "
                f"projection={projection}, stream_id={stream_id}, "
                f"checkpoint={checkpoint}, observed_sequences={sequences}"
            )

        for event in events:
            seq = int(event["seq"])
            fingerprint = _fingerprint(dict(event))
            existing = connection.execute(
                select(projection_event_receipts_table).where(
                    projection_event_receipts_table.c.projection == projection,
                    projection_event_receipts_table.c.stream_id == stream_id,
                    projection_event_receipts_table.c.seq == seq,
                )
            ).mappings().first()
            if existing is not None:
                if (
                    existing["event_id"] != str(event["event_id"])
                    or existing["fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "conflicting projection receipt during validation for "
                        f"projection={projection}, stream_id={stream_id}, seq={seq}"
                    )
                continue
            connection.execute(
                insert(projection_event_receipts_table).values(
                    projection=projection,
                    stream_id=stream_id,
                    seq=seq,
                    event_id=str(event["event_id"]),
                    fingerprint=fingerprint,
                )
            )


def upgrade(connection: Connection) -> None:
    projection_event_receipts_table.create(connection, checkfirst=True)
    _validate_and_backfill(connection)
