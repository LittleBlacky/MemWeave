from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from memweave.events import EventStore
from memweave.models import EventType
from memweave.db import Database


def test_append_allocates_strict_stream_sequences(tmp_path):
    store = EventStore(Database(str(tmp_path / "events.db")))

    first = store.append(
        "tenant:t1/session:s1",
        EventType.USER_MESSAGE,
        {"text": "hello"},
        "user:u1",
        request_id=uuid4(),
    )
    second = store.append(
        "tenant:t1/session:s1",
        EventType.MODEL_OUTPUT,
        {"text": "hi"},
        "agent:a1",
        request_id=uuid4(),
    )

    assert (first.seq, second.seq) == (1, 2)
    assert store.last_seq("tenant:t1/session:s1") == 2
    assert [event.seq for event in store.list_after("tenant:t1/session:s1", 0)] == [1, 2]


def test_duplicate_event_id_is_idempotent_and_payload_is_immutable(tmp_path):
    store = EventStore(Database(str(tmp_path / "events.db")))
    event_id = uuid4()
    request_id = uuid4()

    original = store.append(
        "session:s1",
        EventType.USER_MESSAGE,
        {"text": "original"},
        "user:u1",
        request_id=request_id,
        event_id=event_id,
    )
    duplicate = store.append(
        "session:s1",
        EventType.USER_MESSAGE,
        {"text": "original"},
        "user:u1",
        request_id=request_id,
        event_id=event_id,
    )

    assert duplicate == original
    assert store.last_seq("session:s1") == 1

    with pytest.raises(ValueError, match="immutable"):
        store.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {"text": "mutated"},
            "user:u1",
            request_id=request_id,
            event_id=event_id,
        )


def test_duplicate_event_id_rejects_a_different_occurred_at(tmp_path):
    store = EventStore(Database(str(tmp_path / "events.db")))
    event_id = uuid4()
    request_id = uuid4()
    first_occurred_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    second_occurred_at = datetime(2026, 1, 2, tzinfo=timezone.utc)

    store.append(
        "session:s1",
        EventType.USER_MESSAGE,
        {"text": "same"},
        "user:u1",
        request_id=request_id,
        event_id=event_id,
        occurred_at=first_occurred_at,
    )

    with pytest.raises(ValueError, match="immutable"):
        store.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {"text": "same"},
            "user:u1",
            request_id=request_id,
            event_id=event_id,
            occurred_at=second_occurred_at,
        )


def test_concurrent_writers_receive_unique_sequences(tmp_path):
    database = Database(str(tmp_path / "events.db"))
    store = EventStore(database)

    def append_one(index):
        return store.append(
            "session:concurrent",
            EventType.TOOL_COMPLETED,
            {"index": index},
            "agent:a1",
            request_id=uuid4(),
        ).seq

    with ThreadPoolExecutor(max_workers=8) as executor:
        sequences = list(executor.map(append_one, range(20)))

    assert sorted(sequences) == list(range(1, 21))


def test_append_persists_protocol_and_causality_metadata(tmp_path):
    store = EventStore(Database(str(tmp_path / "events.db")))
    causation_id = uuid4()
    correlation_id = uuid4()
    event = store.append(
        "session:s1",
        "code.test_failed",
        {"exit_code": 1},
        "agent:codex",
        request_id=uuid4(),
        idempotency_key="codex:req-1:test-1",
        causation_id=causation_id,
        correlation_id=correlation_id,
    )

    restored = store.list_after("session:s1", 0)[0]

    assert restored.event_type == "code.test_failed"
    assert restored.idempotency_key == "codex:req-1:test-1"
    assert restored.causation_id == causation_id
    assert restored.correlation_id == correlation_id
    assert restored.payload == {"exit_code": 1}


def test_event_store_rejects_invalid_public_arguments(tmp_path):
    store = EventStore(Database(str(tmp_path / "invalid-arguments.db")))

    with pytest.raises(TypeError, match="stream_id must be a string"):
        store.append(None, EventType.USER_MESSAGE, {}, "user:u1", request_id=uuid4())
    with pytest.raises(ValueError, match="stream_id must not be blank"):
        store.append("   ", EventType.USER_MESSAGE, {}, "user:u1", request_id=uuid4())
    with pytest.raises(TypeError, match="payload must be a dictionary"):
        store.append("session:s1", EventType.USER_MESSAGE, [], "user:u1", request_id=uuid4())

    with pytest.raises(TypeError, match="stream_id must be a string"):
        store.list_after(None, 0)
    with pytest.raises(ValueError, match="seq must not be negative"):
        store.list_after("session:s1", -1)


def test_event_store_rejects_invalid_event_type_and_idempotency_key(tmp_path):
    store = EventStore(Database(str(tmp_path / "invalid-event-fields.db")))

    with pytest.raises(TypeError, match="event_type must be a string or EventType"):
        store.append("session:s1", None, {}, "user:u1", request_id=uuid4())
    with pytest.raises(ValueError, match="event_type must not be blank"):
        store.append("session:s1", "   ", {}, "user:u1", request_id=uuid4())
    with pytest.raises(TypeError, match="idempotency_key must be a string"):
        store.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {},
            "user:u1",
            request_id=uuid4(),
            idempotency_key=123,
        )
    with pytest.raises(ValueError, match="idempotency_key must not be blank"):
        store.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {},
            "user:u1",
            request_id=uuid4(),
            idempotency_key="   ",
        )
