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

    def __init__(self, checkpoint_store: ProjectionCheckpointStore | None = None) -> None:
        self._backends: Dict[str, EventProjector] = {}
        self._errors: Dict[str, str] = {}
        self._checkpoint_store = checkpoint_store
        self._pending: Dict[Tuple[str, str], Dict[int, Event]] = {}
        self._projection_locks: Dict[Tuple[str, str], RLock] = {}
        self._projection_locks_guard = Lock()

    def register_backend(self, backend: EventProjector) -> None:
        if not isinstance(backend, EventProjector):
            raise TypeError("backend must implement EventProjector")
        if not backend.name.strip():
            raise ValueError("backend name must not be blank")
        if backend.name in self._backends:
            raise ValueError("backend name already registered")
        self._backends[backend.name] = backend

    def project(self, event: Event) -> Dict[str, int]:
        watermarks: Dict[str, int] = {}
        self._errors = {}
        for name, backend in self._backends.items():
            with self._projection_lock(name, event.stream_id):
                try:
                    if self._checkpoint_store is not None:
                        checkpoint = self._checkpoint_store.get(name, event.stream_id)
                        if event.seq <= checkpoint:
                            watermarks[name] = checkpoint
                            continue
                        pending = self._pending.setdefault((name, event.stream_id), {})
                        pending.setdefault(event.seq, event)
                        next_seq = checkpoint + 1
                        while next_seq in pending:
                            candidate = pending[next_seq]
                            backend.apply(candidate)
                            self._checkpoint_store.save_max(
                                name, event.stream_id, candidate.seq
                            )
                            del pending[next_seq]
                            checkpoint = candidate.seq
                            next_seq += 1
                        watermarks[name] = checkpoint
                    else:
                        backend.apply(event)
                        watermarks[name] = backend.watermark(event.stream_id)
                except Exception as exc:
                    self._errors[name] = str(exc)
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
                self._errors[name] = str(exc)
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

    def health(self) -> Dict[str, bool]:
        statuses: Dict[str, bool] = {}
        for name, backend in self._backends.items():
            try:
                statuses[name] = bool(backend.health())
            except Exception as exc:
                statuses[name] = False
                self._errors[name] = str(exc)
        return statuses

    def errors(self) -> Dict[str, str]:
        return dict(self._errors)


# Compatibility name retained for callers written against the Task 2 API.
StorageCoordinator = ProjectionDispatcher
