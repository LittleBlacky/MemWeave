from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.outbox import OutboxStatus, OutboxStore


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
    outbox.mark_applied(claimed_applied.id)
    assert outbox.get(applied.id).status is OutboxStatus.APPLIED

    dead = outbox.enqueue(uuid4(), "projection.b", {}, "event-b")
    claimed_dead = outbox.claim()
    assert claimed_dead is not None
    outbox.mark_dead_letter(claimed_dead.id, "permanent failure")
    assert outbox.get(dead.id).status is OutboxStatus.DEAD_LETTER
