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
    ) -> None:
        self.dispatcher = dispatcher
        self.event_source = event_source
        self._states: Dict[str, ProjectionRuntimeState] = {}
        self._buffers: Dict[str, Dict[int, Event]] = {}
        self._locks: Dict[str, RLock] = {}
        self._locks_guard = Lock()
        self._active_recoveries: set[str] = set()

    def state(self, stream_id: str) -> ProjectionRuntimeState:
        self._validate_stream_id(stream_id)
        with self._stream_lock(stream_id):
            return self._states.get(stream_id, ProjectionRuntimeState.RECOVERING)

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
            replay = self.event_source.list_after(stream_id, 0)
            for event in sorted(replay, key=lambda item: item.seq):
                if event.seq <= target_seq:
                    self.dispatcher.project(event)
                    self._raise_on_dispatch_error(stream_id)

            with lock:
                buffered = sorted(
                    self._buffers.get(stream_id, {}).values(),
                    key=lambda item: item.seq,
                )
                self._buffers[stream_id] = {}
                for event in buffered:
                    self.dispatcher.project(event)
                    self._raise_on_dispatch_error(stream_id)
                self._states[stream_id] = ProjectionRuntimeState.READY
            return target_seq
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
                self._buffers.setdefault(stream_id, {}).setdefault(event.seq, event)
                return {}
            return self.dispatcher.project(event)

    def _raise_on_dispatch_error(self, stream_id: str) -> None:
        errors = self.dispatcher.errors()
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
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("stream_id must not be blank")
