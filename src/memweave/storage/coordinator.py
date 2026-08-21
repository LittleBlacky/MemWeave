"""Dispatch events to in-process projection handlers.

Durable delivery, retries, and restart recovery belong to the Outbox worker.
An optional checkpoint store records successfully applied event watermarks;
this module still only performs best-effort in-process fan-out.
"""

from threading import Lock, RLock
from typing import Dict, Tuple

from ..models import Event
from .ports import EventProjector, ProjectionCheckpointStore


class ProjectionDispatcher:
    """Best-effort in-process fan-out for event projectors."""

    def __init__(
        self,
        checkpoint_store: ProjectionCheckpointStore | None = None,
        max_pending_events: int = 10_000,
        max_pending_events_total: int = 100_000,
    ) -> None:
        if not isinstance(max_pending_events, int) or isinstance(max_pending_events, bool):
            raise TypeError("max_pending_events must be an integer")
        if max_pending_events < 1:
            raise ValueError("max_pending_events must be positive")
        if not isinstance(max_pending_events_total, int) or isinstance(
            max_pending_events_total, bool
        ):
            raise TypeError("max_pending_events_total must be an integer")
        if max_pending_events_total < 1:
            raise ValueError("max_pending_events_total must be positive")
        self._backends: Dict[str, EventProjector] = {}
        self._errors: Dict[str, Dict[str, str]] = {}
        self._errors_lock = Lock()
        self._checkpoint_store = checkpoint_store
        self.max_pending_events = max_pending_events
        self.max_pending_events_total = max_pending_events_total
        self._pending: Dict[Tuple[str, str], Dict[int, Event]] = {}
        self._pending_count = 0
        self._pending_count_lock = Lock()
        self._projection_locks: Dict[Tuple[str, str], RLock] = {}
        self._projection_locks_guard = Lock()

    def register_backend(self, backend: EventProjector) -> None:
        if not isinstance(backend, EventProjector):
            raise TypeError("backend must implement EventProjector")
        if not isinstance(backend.name, str):
            raise TypeError("backend name must be a string")
        if not backend.name.strip():
            raise ValueError("backend name must not be blank")
        if backend.name in self._backends:
            raise ValueError("backend name already registered")
        self._backends[backend.name] = backend

    def project(self, event: Event) -> Dict[str, int]:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        watermarks: Dict[str, int] = {}
        for name, backend in self._backends.items():
            with self._projection_lock(name, event.stream_id):
                try:
                    if self._checkpoint_store is not None:
                        checkpoint = self._checkpoint_store.get(name, event.stream_id)
                        if event.seq <= checkpoint:
                            watermarks[name] = checkpoint
                            self._clear_error(event.stream_id, name)
                            continue
                        pending = self._pending.setdefault((name, event.stream_id), {})
                        if event.seq not in pending:
                            is_gap_filler = event.seq == checkpoint + 1
                            with self._pending_count_lock:
                                if (
                                    not is_gap_filler
                                    and len(pending) >= self.max_pending_events
                                ):
                                    raise RuntimeError(
                                        "pending gap buffer full for "
                                        f"projection={name}, stream_id={event.stream_id}"
                                    )
                                if (
                                    not is_gap_filler
                                    and self._pending_count >= self.max_pending_events_total
                                ):
                                    raise RuntimeError(
                                        "total pending gap buffer full for "
                                        f"stream_id={event.stream_id}"
                                    )
                                pending[event.seq] = event
                                self._pending_count += 1
                        next_seq = checkpoint + 1
                        while next_seq in pending:
                            candidate = pending[next_seq]
                            # The backend watermark reflects the side effect
                            # that actually reached the projector. If the
                            # checkpoint write failed after apply(), a retry
                            # must advance the checkpoint without applying the
                            # same event a second time.
                            if backend.watermark(event.stream_id) < candidate.seq:
                                backend.apply(candidate)
                            self._checkpoint_store.save_max(
                                name, event.stream_id, candidate.seq
                            )
                            del pending[next_seq]
                            with self._pending_count_lock:
                                self._pending_count -= 1
                            checkpoint = candidate.seq
                            next_seq += 1
                        watermarks[name] = checkpoint
                        if checkpoint >= event.seq:
                            self._clear_error(event.stream_id, name)
                    else:
                        backend.apply(event)
                        watermarks[name] = backend.watermark(event.stream_id)
                        self._clear_error(event.stream_id, name)
                except Exception as exc:
                    self._record_error(event.stream_id, name, str(exc))
        return watermarks

    def _projection_lock(self, name: str, stream_id: str) -> RLock:
        key = (name, stream_id)
        with self._projection_locks_guard:
            lock = self._projection_locks.get(key)
            if lock is None:
                lock = RLock()
                self._projection_locks[key] = lock
            return lock

    def watermarks(self, stream_id: str) -> Dict[str, int]:
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        watermarks: Dict[str, int] = {}
        for name, backend in self._backends.items():
            try:
                watermarks[name] = backend.watermark(stream_id)
            except Exception as exc:
                self._record_error(stream_id, name, str(exc))
        return watermarks

    def replay_from(self, stream_id: str) -> int:
        """Return the earliest checkpoint that must be replayed for a stream.

        Recovery has to satisfy every registered projection. The slowest
        projection therefore determines the replay boundary; using the
        maximum checkpoint could permanently skip events for lagging backends.
        """
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        if self._checkpoint_store is None or not self._backends:
            return 0
        checkpoints = [
            self._checkpoint_store.get(name, stream_id)
            for name in self._backends
        ]
        return min(checkpoints, default=0)

    def clear_pending(self, stream_id: str) -> int:
        """Drop buffered gap events after the caller has replayed the source log."""
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        removed = 0
        for name in self._backends:
            with self._projection_lock(name, stream_id):
                removed_for_key = len(self._pending.pop((name, stream_id), {}))
                removed += removed_for_key
                with self._pending_count_lock:
                    self._pending_count -= removed_for_key
        return removed

    def health(self) -> Dict[str, bool]:
        statuses: Dict[str, bool] = {}
        for name, backend in self._backends.items():
            try:
                statuses[name] = bool(backend.health())
            except Exception as exc:
                statuses[name] = False
                self._record_error("__system__", name, str(exc))
        return statuses

    def errors(self, stream_id: str | None = None):
        """Return a snapshot of projection errors, optionally for one stream."""
        if stream_id is not None:
            if not isinstance(stream_id, str) or not stream_id.strip():
                raise ValueError("stream_id must not be blank")
        with self._errors_lock:
            if stream_id is not None:
                return dict(self._errors.get(stream_id, {}))
            return {
                scope: dict(errors)
                for scope, errors in self._errors.items()
            }

    def _record_error(self, scope: str, name: str, message: str) -> None:
        with self._errors_lock:
            self._errors.setdefault(scope, {})[name] = message

    def _clear_error(self, scope: str, name: str) -> None:
        with self._errors_lock:
            errors = self._errors.get(scope)
            if errors is None:
                return
            errors.pop(name, None)
            if not errors:
                self._errors.pop(scope, None)


# Compatibility name retained for callers written against the Task 2 API.
StorageCoordinator = ProjectionDispatcher
