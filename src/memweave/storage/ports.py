"""Backend contracts used by the memory core."""

from typing import Any, ContextManager, Dict, Protocol, runtime_checkable

from sqlalchemy.engine import Connection

from ..models import Event


class RelationalDatabase(Protocol):
    def begin(self) -> ContextManager[Connection]:
        ...

    def read(self) -> ContextManager[Connection]:
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

    def project(self, event: Event) -> None:
        ...

    def health(self) -> bool:
        ...

    def watermark(self) -> int:
        ...


class VectorIndex(ProjectionBackend, Protocol):
    def upsert(self, memory: Any) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...


class GraphStore(ProjectionBackend, Protocol):
    def upsert(self, memory: Any) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...


class KeywordIndex(ProjectionBackend, Protocol):
    def upsert(self, memory: Any) -> None:
        ...

    def delete(self, memory_id: str) -> None:
        ...
