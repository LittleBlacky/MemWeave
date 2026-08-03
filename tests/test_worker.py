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
