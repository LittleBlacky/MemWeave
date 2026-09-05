from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.outbox import OutboxStatus, OutboxStore
from memweave.storage.schema import outbox_consumer_receipts_table, outbox_table


def test_outbox_enqueue_is_idempotent(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "outbox.db")))
    event_id = uuid4()

    first = outbox.enqueue(event_id, "projection.vector", {"id": 1}, "event-1")
    duplicate = outbox.enqueue(event_id, "projection.vector", {"id": 1}, "event-1")

    assert duplicate == first
    assert first.status is OutboxStatus.PENDING

    with pytest.raises(ValueError, match="idempotency"):
        outbox.enqueue(event_id, "projection.vector", {"id": 2}, "event-1")


def test_outbox_retries_and_reclaims_expired_processing_item(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "outbox.db")), lease_seconds=60)
    item = outbox.enqueue(uuid4(), "projection.vector", {"id": 1}, "event-1")

    claimed = outbox.claim()
    assert claimed is not None
    assert claimed.id == item.id
    assert claimed.status is OutboxStatus.PROCESSING
    assert claimed.attempts == 1

    outbox.mark_retryable(
        claimed.id,
        "temporary failure",
        claimed.lease_token,
        available_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    retried = outbox.claim()
    assert retried is not None
    assert retried.attempts == 2

    recovered = OutboxStore(Database(str(tmp_path / "outbox.db")), lease_seconds=0)
    reclaimed = recovered.claim()
    assert reclaimed is not None
    assert reclaimed.id == item.id
    assert reclaimed.attempts == 3


def test_outbox_can_mark_applied_or_dead_letter(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "outbox.db")))
    applied = outbox.enqueue(uuid4(), "projection.a", {}, "event-a")
    claimed_applied = outbox.claim()
    assert claimed_applied is not None
    outbox.mark_applied(claimed_applied.id, claimed_applied.lease_token)
    assert outbox.get(applied.id).status is OutboxStatus.APPLIED

    dead = outbox.enqueue(uuid4(), "projection.b", {}, "event-b")
    claimed_dead = outbox.claim()
    assert claimed_dead is not None
    outbox.mark_dead_letter(
        claimed_dead.id, "permanent failure", claimed_dead.lease_token
    )
    assert outbox.get(dead.id).status is OutboxStatus.DEAD_LETTER


def test_outbox_validates_public_input_types(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "validation.db")))
    event_id = uuid4()

    with pytest.raises(TypeError, match="topic"):
        outbox.enqueue(event_id, None, {}, "key")
    with pytest.raises(TypeError, match="payload"):
        outbox.enqueue(event_id, "projection.a", [], "key")
    with pytest.raises(TypeError, match="idempotency_key"):
        outbox.enqueue(event_id, "projection.a", {}, None)
    with pytest.raises(TypeError, match="item_id"):
        outbox.get("not-a-uuid")
    with pytest.raises(TypeError, match="topic"):
        outbox.claim(topic=123)
    with pytest.raises(ValueError, match="topic"):
        outbox.claim(topic="   ")


def test_expired_lease_token_cannot_finalize_reclaimed_item(tmp_path):
    current = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    outbox = OutboxStore(
        Database(str(tmp_path / "lease-fencing.db")),
        lease_seconds=60,
        clock=lambda: current[0],
    )
    item = outbox.enqueue(uuid4(), "projection.vector", {}, "event-1")
    first = outbox.claim()
    assert first is not None
    assert first.lease_token is not None
    assert outbox.begin_consume(
        first.id, "vector-indexer", first.lease_token
    ).value == "acquired"

    current[0] += timedelta(seconds=61)
    second = outbox.claim()
    assert second is not None
    assert second.id == item.id
    assert second.lease_token != first.lease_token
    assert outbox.begin_consume(
        second.id, "vector-indexer", second.lease_token
    ).value == "acquired"

    with pytest.raises(ValueError, match="missing or not processing"):
        outbox.mark_consumed(first.id, "vector-indexer", first.lease_token)
    with pytest.raises(ValueError, match="missing or not processing"):
        outbox.mark_applied(first.id, first.lease_token)

    outbox.mark_consumed(second.id, "vector-indexer", second.lease_token)
    outbox.mark_applied(second.id, second.lease_token)
    assert outbox.get(item.id).status is OutboxStatus.APPLIED


def test_concurrent_enqueue_returns_one_idempotent_item(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "enqueue-race.db")))
    event_id = uuid4()

    def enqueue_once(_):
        return outbox.enqueue(event_id, "projection.vector", {"id": 1}, "event-1")

    with ThreadPoolExecutor(max_workers=8) as executor:
        items = list(executor.map(enqueue_once, range(16)))

    assert len({item.id for item in items}) == 1
    assert items[0].idempotency_key == "event-1"


def test_processing_item_without_lease_timestamp_is_reclaimed(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "orphaned-processing.db")))
    item = outbox.enqueue(uuid4(), "projection.vector", {}, "event-1")
    with outbox.database.begin() as connection:
        connection.execute(
            outbox_table.update()
            .where(outbox_table.c.id == str(item.id))
            .values(status=OutboxStatus.PROCESSING.value, locked_at=None)
        )

    reclaimed = outbox.claim()
    assert reclaimed is not None
    assert reclaimed.id == item.id
    assert reclaimed.lease_token is not None


def test_retry_schedule_normalizes_timezone_and_rejects_naive_datetime(tmp_path):
    current = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    outbox = OutboxStore(
        Database(str(tmp_path / "retry-timezone.db")),
        clock=lambda: current,
    )
    item = outbox.enqueue(uuid4(), "projection.vector", {}, "event-1")
    claimed = outbox.claim()
    assert claimed is not None

    retry_at = current + timedelta(seconds=30)
    outbox.mark_retryable(
        claimed.id,
        "temporary",
        claimed.lease_token,
        available_at=retry_at.astimezone(timezone(timedelta(hours=8))),
    )
    assert outbox.get(item.id).available_at == retry_at

    next_claim = outbox.claim()
    assert next_claim is None
    with pytest.raises(ValueError, match="timezone-aware"):
        outbox.mark_retryable(
            claimed.id,
            "temporary",
            claimed.lease_token,
            available_at=datetime(2026, 1, 1),
        )


def test_unknown_consumer_receipt_status_is_rejected(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "unknown-receipt-status.db")))
    item = outbox.enqueue(uuid4(), "projection.vector", {}, "event-1")
    claimed = outbox.claim()
    assert claimed is not None
    with outbox.database.begin() as connection:
        connection.execute(
            outbox_consumer_receipts_table.insert().values(
                id=str(uuid4()),
                outbox_id=str(item.id),
                consumer_id="vector-indexer",
                idempotency_key=item.idempotency_key,
                status="corrupt",
                locked_at=None,
                lease_token=None,
                consumed_at=None,
                created_at=claimed.created_at.isoformat(),
                updated_at=claimed.updated_at.isoformat(),
            )
        )

    with pytest.raises(ValueError, match="unknown consumer receipt status"):
        outbox.begin_consume(item.id, "vector-indexer", claimed.lease_token)


def test_enqueue_rejects_non_standard_json_numbers(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "strict-json.db")))
    with pytest.raises(ValueError, match="Out of range float values"):
        outbox.enqueue(uuid4(), "projection.vector", {"score": float("nan")}, "event-1")
