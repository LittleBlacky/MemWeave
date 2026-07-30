"""Dispatch events to in-process projection handlers.

Durable delivery, retries, checkpoints, and restart recovery belong to the
Outbox worker. This module only performs best-effort in-process fan-out.
"""

from typing import Dict

from ..models import Event
from .ports import EventProjector


class ProjectionDispatcher:
    """Best-effort in-process fan-out for event projectors."""

    def __init__(self) -> None:
        self._backends: Dict[str, EventProjector] = {}
        self._errors: Dict[str, str] = {}

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
            try:
                backend.apply(event)
                watermarks[name] = backend.watermark()
            except Exception as exc:
                self._errors[name] = str(exc)
        return watermarks

    def watermarks(self) -> Dict[str, int]:
        watermarks: Dict[str, int] = {}
        for name, backend in self._backends.items():
            try:
                watermarks[name] = backend.watermark()
            except Exception as exc:
                self._errors[name] = str(exc)
        return watermarks

    def errors(self) -> Dict[str, str]:
        return dict(self._errors)


# Compatibility name retained for callers written against the Task 2 API.
StorageCoordinator = ProjectionDispatcher
