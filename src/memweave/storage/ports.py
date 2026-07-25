"""Backend contracts used by the memory core."""

from typing import Any, ContextManager, Dict, Protocol, runtime_checkable

from ..models import Event, MemoryRecord


class RelationalDatabase(Protocol):
    def begin(self) -> ContextManager[Any]:
        ...

    def read(self) -> ContextManager[Any]:
        ...

    def apply_migrations(self) -> list[str]:
        ...


class EventRepository(Protocol):
    def append(self, *args: Any, **kwargs: Any) -> Event:
        ...

    def list_after(self, stream_id: str, seq: int) -> list[Event]:
        ...

    def last_seq(self, stream_id: str) -> int:
        ...


@runtime_checkable
class ProjectionBackend(Protocol):
    name: str

    def health(self) -> bool:
        ...

    def watermark(self) -> int:
        ...


@runtime_checkable
class EventProjector(ProjectionBackend, Protocol):
    def apply(self, event: Event) -> None:
        ...


class VectorIndex(ProjectionBackend, Protocol):
    def upsert(self, memory: MemoryRecord) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...


class GraphStore(ProjectionBackend, Protocol):
    def upsert(self, memory: MemoryRecord) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...


class KeywordIndex(ProjectionBackend, Protocol):
    def upsert(self, memory: MemoryRecord) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...
