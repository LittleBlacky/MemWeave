"""Create receipts for events applied by generic projections."""

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


def _backfill(connection: Connection) -> None:
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
    if not required.issubset(columns):
        return

    watermarks = connection.execute(select(projection_watermarks_table)).mappings()
    for watermark in watermarks:
        events = connection.execute(
            select(events_table).where(
                events_table.c.stream_id == watermark["stream_id"],
                events_table.c.seq <= int(watermark["last_seq"]),
            ).order_by(events_table.c.seq)
        ).mappings()
        for event in events:
            existing = connection.execute(
                select(projection_event_receipts_table).where(
                    projection_event_receipts_table.c.projection
                    == watermark["projection"],
                    projection_event_receipts_table.c.stream_id
                    == watermark["stream_id"],
                    projection_event_receipts_table.c.seq == int(event["seq"]),
                )
            ).mappings().first()
            fingerprint = _fingerprint(dict(event))
            if existing is not None:
                if (
                    existing["event_id"] != str(event["event_id"])
                    or existing["fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "conflicting projection receipt during migration for "
                        f"projection={watermark['projection']}, "
                        f"stream_id={watermark['stream_id']}, seq={event['seq']}"
                    )
                continue
            connection.execute(
                insert(projection_event_receipts_table).values(
                    projection=watermark["projection"],
                    stream_id=watermark["stream_id"],
                    seq=int(event["seq"]),
                    event_id=str(event["event_id"]),
                    fingerprint=fingerprint,
                )
            )


def upgrade(connection: Connection) -> None:
    projection_event_receipts_table.create(connection, checkfirst=True)
    _backfill(connection)
