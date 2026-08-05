from datetime import datetime, timedelta, timezone
from uuid import uuid4

from memweave.db import Database
from memweave.outbox import OutboxStatus, OutboxStore
from memweave.worker import LocalWorker


def test_local_worker_claims_dispatches_and_marks_task_applied(tmp_path):
    outbox = OutboxStore(Database(str(tmp_path / "worker.db")))
    item = outbox.enqueue(uuid4(), "projection.vector", {"memory_id": "m1"}, "event-1")
    handled = []
    worker = LocalWorker(
        outbox,
        {"projection.vector": lambda task: handled.append(task.payload)},
    )

    assert worker.run_once() == 1
    assert handled == [{"memory_id": "m1"}]
    assert outbox.get(item.id).status is OutboxStatus.APPLIED
    assert worker.run_once() == 0


def test_local_worker_retries_with_backoff_and_dead_letters(tmp_path):
    current = [datetime(2026, 1, 1, tzinfo=timezone.utc)]

    def clock():
        return current[0]

    outbox = OutboxStore(
        Database(str(tmp_path / "retry.db")),
        clock=clock,
    )
    item = outbox.enqueue(uuid4(), "projection.vector", {}, "event-1")
    worker = LocalWorker(
        outbox,
        {"projection.vector": lambda task: (_ for _ in ()).throw(RuntimeError("temporary"))},
        max_attempts=3,
        base_delay_seconds=10,
        clock=clock,
    )

    assert worker.run_once() == 1
    retry = outbox.get(item.id)
    assert retry.status is OutboxStatus.RETRYABLE
    assert retry.attempts == 1
    assert retry.available_at == current[0] + timedelta(seconds=10)
    assert worker.run_once() == 0

    current[0] += timedelta(seconds=10)
    assert worker.run_once() == 1
    retry = outbox.get(item.id)
    assert retry.status is OutboxStatus.RETRYABLE
    assert retry.attempts == 2
    assert retry.available_at == current[0] + timedelta(seconds=20)

    current[0] += timedelta(seconds=20)
    assert worker.run_once() == 1
    assert outbox.get(item.id).status is OutboxStatus.DEAD_LETTER
