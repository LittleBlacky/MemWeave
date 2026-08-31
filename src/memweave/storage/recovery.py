"""Lifecycle and replay coordination for checkpointed projections."""

from enum import Enum
from threading import Lock, RLock
from typing import Dict, Protocol, Tuple

from ..models import Event
from .coordinator import ProjectionDispatcher


class EventReplaySource(Protocol):
    def list_after(self, stream_id: str, seq: int) -> list[Event]:
        ...

    def last_seq(self, stream_id: str) -> int:
        ...


class ProjectionRuntimeState(str, Enum):
    RECOVERING = "recovering"
    READY = "ready"
    FAILED = "failed"


class ProjectionRuntime:
    """Gate live projection behind an explicit per-stream replay recovery."""

    def __init__(
        self,
        dispatcher: ProjectionDispatcher,
        event_source: EventReplaySource,
        max_buffer_events: int = 10_000,
        max_buffer_events_total: int = 100_000,
    ) -> None:
        self._validate_capacity(max_buffer_events, "max_buffer_events")
        self._validate_capacity(max_buffer_events_total, "max_buffer_events_total")
        self.dispatcher = dispatcher
        self.event_source = event_source
        self.max_buffer_events = max_buffer_events
        self.max_buffer_events_total = max_buffer_events_total
        self._states: Dict[str, ProjectionRuntimeState] = {}
        self._buffers: Dict[str, Dict[int, Event]] = {}
        self._buffer_count = 0
        self._buffer_count_lock = Lock()
        self._locks: Dict[str, RLock] = {}
        self._locks_guard = Lock()
        self._active_recoveries: set[str] = set()

    def state(self, stream_id: str) -> ProjectionRuntimeState:
        self._validate_stream_id(stream_id)
        with self._stream_lock(stream_id):
            return self._states.get(stream_id, ProjectionRuntimeState.RECOVERING)

    def target_seq(self, stream_id: str) -> int:
        """Return the latest authoritative event sequence for a stream."""
        self._validate_stream_id(stream_id)
        return self.event_source.last_seq(stream_id)

    def catch_up(self, stream_id: str, target_seq: int) -> int:
        """Replay the stream until the requested target is covered."""
        self._validate_stream_id(stream_id)
        if not isinstance(target_seq, int) or isinstance(target_seq, bool):
            raise TypeError("target_seq must be an integer")
        if target_seq < 0:
            raise ValueError("target_seq must not be negative")
        applied = self.recover(stream_id)
        if applied < target_seq:
            raise RuntimeError(
                f"projection catch-up stopped at {applied}, target is {target_seq}"
            )
        return applied

    def recover(self, stream_id: str) -> int:
        self._validate_stream_id(stream_id)
        lock = self._stream_lock(stream_id)
        with lock:
            if stream_id in self._active_recoveries:
                raise RuntimeError(f"recovery already in progress for {stream_id}")
            self._active_recoveries.add(stream_id)
            self._states[stream_id] = ProjectionRuntimeState.RECOVERING
            self._buffers.setdefault(stream_id, {})

        try:
            target_seq = self.event_source.last_seq(stream_id)
            start_seq = self.dispatcher.replay_from(stream_id)
            replay = self.event_source.list_after(stream_id, start_seq)
            replayed_events: Dict[int, Event] = {}
            ordered_replay = sorted(replay, key=lambda item: item.seq)
            for event in ordered_replay:
                self._validate_event_stream(event, stream_id)
                existing = replayed_events.get(event.seq)
                if existing is not None:
                    if existing != event:
                        raise ValueError(
                            "conflicting events for "
                            f"stream_id={stream_id}, seq={event.seq}"
                        )
                    continue
                replayed_events[event.seq] = event
            for event in sorted(replayed_events.values(), key=lambda item: item.seq):
                self.dispatcher.project(event)
                self._raise_on_dispatch_error(stream_id)

            with lock:
                buffer = self._buffers.get(stream_id, {})
                for event in sorted(buffer.values(), key=lambda item: item.seq):
                    self._validate_event_stream(event, stream_id)
                    replayed = replayed_events.get(event.seq)
                    if replayed is not None:
                        if replayed != event:
                            raise ValueError(
                                "conflicting events for "
                                f"stream_id={stream_id}, seq={event.seq}"
                            )
                        buffer.pop(event.seq, None)
                        with self._buffer_count_lock:
                            self._buffer_count -= 1
                        continue
                    self.dispatcher.project(event)
                    self._raise_on_dispatch_error(stream_id)
                    replayed_events[event.seq] = event
                    buffer.pop(event.seq, None)
                    with self._buffer_count_lock:
                        self._buffer_count -= 1
                if not self._covers_target_sequence(
                    start_seq, target_seq, set(replayed_events)
                ):
                    raise RuntimeError(
                        "replay did not cover target sequence "
                        f"for stream_id={stream_id}"
                    )
                self._states[stream_id] = ProjectionRuntimeState.READY
            return max(replayed_events, default=target_seq)
        except Exception:
            with lock:
                self._states[stream_id] = ProjectionRuntimeState.FAILED
            raise
        finally:
            with lock:
                self._active_recoveries.discard(stream_id)

    def publish(self, event: Event) -> Dict[str, int]:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        stream_id = event.stream_id
        lock = self._stream_lock(stream_id)
        with lock:
            state = self._states.get(stream_id, ProjectionRuntimeState.RECOVERING)
            if state is ProjectionRuntimeState.FAILED:
                raise RuntimeError(f"recovery failed for {stream_id}")
            if state is ProjectionRuntimeState.RECOVERING:
                buffer = self._buffers.setdefault(stream_id, {})
                existing = buffer.get(event.seq)
                if existing is not None:
                    if existing != event:
                        raise ValueError(
                            "conflicting events for "
                            f"stream_id={stream_id}, seq={event.seq}"
                        )
                else:
                    with self._buffer_count_lock:
                        if len(buffer) >= self.max_buffer_events:
                            raise RuntimeError(
                                "recovery buffer full for "
                                f"stream_id={stream_id}"
                            )
                        if self._buffer_count >= self.max_buffer_events_total:
                            raise RuntimeError(
                                "total recovery buffer full for "
                                f"stream_id={stream_id}"
                            )
                        buffer[event.seq] = event
                        self._buffer_count += 1
                return {}
            return self.dispatcher.project(event)

    def clear_buffer(self, stream_id: str) -> int:
        """Drop buffered live events after replaying the authoritative source."""
        self._validate_stream_id(stream_id)
        with self._stream_lock(stream_id):
            buffer = self._buffers.get(stream_id)
            if not buffer:
                return 0
            removed = len(buffer)
            buffer.clear()
            with self._buffer_count_lock:
                self._buffer_count -= removed
            return removed

    @staticmethod
    def _validate_capacity(value: int, name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{name} must be an integer")
        if value < 1:
            raise ValueError(f"{name} must be positive")

    @staticmethod
    def _validate_event_stream(event: Event, stream_id: str) -> None:
        if event.stream_id != stream_id:
            raise ValueError(
                "replay event stream_id does not match requested stream_id"
            )

    @staticmethod
    def _covers_target_sequence(
        start_seq: int, target_seq: int, observed_sequences: set[int]
    ) -> bool:
        if target_seq <= start_seq:
            return True
        expected = start_seq + 1
        for seq in sorted(observed_sequences):
            if seq < expected:
                continue
            if seq != expected:
                return False
            expected += 1
            if expected > target_seq:
                return True
        return False

    def _raise_on_dispatch_error(self, stream_id: str) -> None:
        errors = self.dispatcher.errors(stream_id)
        if errors:
            raise RuntimeError(f"projection failed during recovery for {stream_id}: {errors}")

    def _stream_lock(self, stream_id: str) -> RLock:
        with self._locks_guard:
            lock = self._locks.get(stream_id)
            if lock is None:
                lock = RLock()
                self._locks[stream_id] = lock
            return lock

    @staticmethod
    def _validate_stream_id(stream_id: str) -> None:
        if not isinstance(stream_id, str):
            raise TypeError("stream_id must be a string")
        if not stream_id.strip():
            raise ValueError("stream_id must not be blank")
