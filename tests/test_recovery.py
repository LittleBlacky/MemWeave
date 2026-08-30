from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent
from uuid import uuid4

import pytest

from memweave.events import EventStore
from memweave.models import Event
from memweave.storage.checkpoints import RelationalProjectionCheckpointStore
from memweave.storage.coordinator import ProjectionDispatcher
from memweave.storage.sqlite import SQLiteDatabase


class RecordingBackend:
    name = "recording"

    def __init__(self):
        self.events = []
        self._watermarks = {}

    def apply(self, event: Event) -> None:
        self.events.append(event.event_id)
        self._watermarks[event.stream_id] = event.seq

    def health(self) -> bool:
        return True

    def watermark(self, stream_id: str) -> int:
        return self._watermarks.get(stream_id, 0)


def _append_events(store, stream_id, count):
    return [
        store.append(
            stream_id,
            "code.test_passed",
            {"seq": seq},
            "agent:codex",
            request_id=uuid4(),
        )
        for seq in range(1, count + 1)
    ]


def test_projection_runtime_replays_events_after_dispatcher_restart(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "recovery.db"))
    event_store = EventStore(database)
    events = _append_events(event_store, "session:recovery", 3)

    checkpoint_store = RelationalProjectionCheckpointStore(database)
    first = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    first_backend = RecordingBackend()
    first.register_backend(first_backend)
    first.project(events[2])
    first.project(events[0])
    assert checkpoint_store.get("recording", "session:recovery") == 1

    restarted = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    restarted_backend = RecordingBackend()
    restarted.register_backend(restarted_backend)
    from memweave.storage.recovery import ProjectionRuntime

    runtime = ProjectionRuntime(restarted, event_store)

    assert runtime.recover("session:recovery") == 3
    assert restarted_backend.events == [events[1].event_id, events[2].event_id]
    assert checkpoint_store.get("recording", "session:recovery") == 3


def test_projection_runtime_starts_replay_after_slowest_projection_checkpoint(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "recovery-start.db"))
    checkpoint_store = RelationalProjectionCheckpointStore(database)
    checkpoint_store.save_max("fast", "session:start", 5)
    checkpoint_store.save_max("slow", "session:start", 3)

    class RecordingSource:
        def __init__(self):
            self.start_seq = None
            self.events = [
                Event(
                    event_id=uuid4(),
                    event_type="code.test_passed",
                    stream_id="session:start",
                    seq=seq,
                    actor="agent:codex",
                    payload={"seq": seq},
                )
                for seq in (4, 5)
            ]

        def last_seq(self, stream_id):
            return 5

        def list_after(self, stream_id, seq):
            self.start_seq = seq
            return self.events

    class NamedBackend(RecordingBackend):
        def __init__(self, name):
            super().__init__()
            self.name = name

    dispatcher = ProjectionDispatcher(checkpoint_store=checkpoint_store)
    dispatcher.register_backend(NamedBackend("fast"))
    dispatcher.register_backend(NamedBackend("slow"))
    source = RecordingSource()
    from memweave.storage.recovery import ProjectionRuntime

    assert ProjectionRuntime(dispatcher, source).recover("session:start") == 5
    assert source.start_seq == 3


def test_projection_runtime_does_not_drop_events_seen_after_recovery_target():
    replayed = [
        Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:late-replay",
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )
        for seq in (1, 2, 3)
    ]

    class SourceWithLateEvent:
        def last_seq(self, stream_id):
            return 2

        def list_after(self, stream_id, seq):
            return replayed

    backend = RecordingBackend()
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(backend)
    from memweave.storage.recovery import ProjectionRuntime

    runtime = ProjectionRuntime(dispatcher, SourceWithLateEvent())

    assert runtime.recover("session:late-replay") == 3

    assert backend.events == [event.event_id for event in replayed]


def test_projection_runtime_bounds_recovery_buffers_and_supports_clear():
    from memweave.storage.recovery import ProjectionRuntime

    def event(stream_id, seq):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id=stream_id,
            seq=seq,
            actor="agent:codex",
            payload={"seq": seq},
        )

    runtime = ProjectionRuntime(
        ProjectionDispatcher(),
        object(),
        max_buffer_events=2,
        max_buffer_events_total=3,
    )

    runtime.publish(event("session:one", 1))
    runtime.publish(event("session:one", 2))
    with pytest.raises(RuntimeError, match="recovery buffer full"):
        runtime.publish(event("session:one", 3))

    runtime.publish(event("session:two", 1))
    with pytest.raises(RuntimeError, match="total recovery buffer full"):
        runtime.publish(event("session:two", 2))

    assert runtime.clear_buffer("session:one") == 2
    runtime.publish(event("session:two", 2))


def test_projection_runtime_distinguishes_invalid_stream_id_types():
    from memweave.storage.recovery import ProjectionRuntime

    runtime = ProjectionRuntime(ProjectionDispatcher(), object())

    for method in (runtime.state, runtime.recover, runtime.clear_buffer):
        with pytest.raises(TypeError, match="stream_id must be a string"):
            method(None)
        with pytest.raises(TypeError, match="stream_id must be a string"):
            method(123)
        with pytest.raises(ValueError, match="stream_id must not be blank"):
            method("   ")


def test_projection_runtime_rejects_replay_events_from_another_stream():
    from memweave.storage.recovery import ProjectionRuntime, ProjectionRuntimeState

    wrong_stream_event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:other",
        seq=1,
        actor="agent:codex",
        payload={},
    )

    class SourceWithWrongStream:
        def last_seq(self, stream_id):
            return 1

        def list_after(self, stream_id, seq):
            return [wrong_stream_event]

    backend = RecordingBackend()
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(backend)
    runtime = ProjectionRuntime(dispatcher, SourceWithWrongStream())

    with pytest.raises(ValueError, match="replay event stream_id does not match"):
        runtime.recover("session:expected")

    assert backend.events == []
    assert runtime.state("session:expected") is ProjectionRuntimeState.FAILED


def test_projection_runtime_rejects_replay_source_that_lags_target():
    from memweave.storage.recovery import ProjectionRuntime, ProjectionRuntimeState

    class LaggingSource:
        def last_seq(self, stream_id):
            return 3

        def list_after(self, stream_id, seq):
            return []

    runtime = ProjectionRuntime(ProjectionDispatcher(), LaggingSource())

    with pytest.raises(RuntimeError, match="replay did not cover target sequence"):
        runtime.recover("session:lagging")

    assert runtime.state("session:lagging") is ProjectionRuntimeState.FAILED


def test_projection_runtime_rejects_conflicting_duplicate_sequence_in_buffer():
    from memweave.storage.recovery import ProjectionRuntime

    def event(text):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:conflict",
            seq=2,
            actor="agent:codex",
            payload={"text": text},
        )

    runtime = ProjectionRuntime(ProjectionDispatcher(), object())
    runtime.publish(event("first"))

    with pytest.raises(ValueError, match="conflicting events for stream_id=session:conflict"):
        runtime.publish(event("second"))


def test_projection_runtime_rejects_conflicting_duplicate_sequence_in_replay():
    from memweave.storage.recovery import ProjectionRuntime, ProjectionRuntimeState

    def event(text):
        return Event(
            event_id=uuid4(),
            event_type="code.test_passed",
            stream_id="session:replay-conflict",
            seq=1,
            actor="agent:codex",
            payload={"text": text},
        )

    class ConflictingSource:
        def last_seq(self, stream_id):
            return 1

        def list_after(self, stream_id, seq):
            return [event("first"), event("second")]

    backend = RecordingBackend()
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(backend)
    runtime = ProjectionRuntime(dispatcher, ConflictingSource())

    with pytest.raises(ValueError, match="conflicting events for stream_id=session:replay-conflict"):
        runtime.recover("session:replay-conflict")

    assert backend.events == []
    assert runtime.state("session:replay-conflict") is ProjectionRuntimeState.FAILED


def test_projection_runtime_buffers_live_events_until_recovery_finishes(tmp_path):
    database = SQLiteDatabase(str(tmp_path / "recovery-buffer.db"))
    event_store = EventStore(database)
    events = _append_events(event_store, "session:buffer", 2)
    live_event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:buffer",
        seq=3,
        actor="agent:codex",
        payload={"seq": 3},
    )

    class BlockingSource:
        def __init__(self):
            self.started = ThreadEvent()
            self.release = ThreadEvent()

        def list_after(self, stream_id, seq):
            self.started.set()
            self.release.wait(timeout=5)
            return event_store.list_after(stream_id, seq)

        def last_seq(self, stream_id):
            return event_store.last_seq(stream_id)

    backend = RecordingBackend()
    dispatcher = ProjectionDispatcher()
    dispatcher.register_backend(backend)
    from memweave.storage.recovery import ProjectionRuntime

    source = BlockingSource()
    runtime = ProjectionRuntime(dispatcher, source)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(runtime.recover, "session:buffer")
        assert source.started.wait(timeout=5)
        runtime.publish(live_event)
        assert backend.events == []
        source.release.set()
        assert future.result(timeout=5) == 3

    assert backend.events == [events[0].event_id, events[1].event_id, live_event.event_id]


def test_projection_runtime_enters_failed_state_when_recovery_errors():
    class BrokenSource:
        def last_seq(self, stream_id):
            return 1

        def list_after(self, stream_id, seq):
            raise RuntimeError("event source unavailable")

    from memweave.storage.recovery import ProjectionRuntime, ProjectionRuntimeState

    runtime = ProjectionRuntime(ProjectionDispatcher(), BrokenSource())

    with pytest.raises(RuntimeError, match="event source unavailable"):
        runtime.recover("session:broken")
    assert runtime.state("session:broken") is ProjectionRuntimeState.FAILED
    with pytest.raises(RuntimeError, match="recovery failed"):
        runtime.publish(
            Event(
                event_id=uuid4(),
                event_type="code.test_passed",
                stream_id="session:broken",
                seq=1,
                actor="agent:codex",
                payload={},
            )
        )


def test_projection_runtime_ignores_errors_from_other_streams_during_recovery():
    class SelectiveFailureBackend(RecordingBackend):
        def apply(self, event):
            if event.stream_id == "session:failed":
                raise RuntimeError("failed stream")
            super().apply(event)

    dispatcher = ProjectionDispatcher()
    backend = SelectiveFailureBackend()
    dispatcher.register_backend(backend)
    failed_event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:failed",
        seq=1,
        actor="agent:codex",
        payload={},
    )
    dispatcher.project(failed_event)

    healthy_event = Event(
        event_id=uuid4(),
        event_type="code.test_passed",
        stream_id="session:healthy",
        seq=1,
        actor="agent:codex",
        payload={},
    )

    class HealthySource:
        def last_seq(self, stream_id):
            return 1

        def list_after(self, stream_id, seq):
            return [healthy_event]

    from memweave.storage.recovery import ProjectionRuntime, ProjectionRuntimeState

    runtime = ProjectionRuntime(dispatcher, HealthySource())

    assert runtime.recover("session:healthy") == 1
    assert runtime.state("session:healthy") is ProjectionRuntimeState.READY
    assert backend.events == [healthy_event.event_id]
