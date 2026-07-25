"""Coordinate independent projection backends without distributed transactions."""

from typing import Dict

from ..models import Event
from .ports import EventProjector


class StorageCoordinator:
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
        return {name: backend.watermark() for name, backend in self._backends.items()}

    def errors(self) -> Dict[str, str]:
        return dict(self._errors)
