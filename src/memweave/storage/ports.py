"""Backend contracts used by the memory core."""

from datetime import datetime
from typing import Any, ContextManager, Dict, Protocol, runtime_checkable
from uuid import UUID

from ..models import Event, EventType, MemoryRecord
from ..protocol import ProtocolVersion


class RelationalDatabase(Protocol):
    def begin(self) -> ContextManager[Any]:
        ...

    def read(self) -> ContextManager[Any]:
        ...

    def apply_migrations(self) -> list[str]:
        ...


class EventRepository(Protocol):
    def append(
        self,
        stream_id: str,
        event_type: EventType | str,
        payload: Dict[str, Any],
        actor: str,
        request_id: UUID,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
        causation_id: UUID | None = None,
        correlation_id: UUID | None = None,
        idempotency_key: str | None = None,
        protocol_version: ProtocolVersion | str = "1.0",
    ) -> Event:
        ...

    def list_after(self, stream_id: str, seq: int) -> list[Event]:
        ...

    def last_seq(self, stream_id: str) -> int:
        ...


class ProjectionCheckpointStore(Protocol):
    def get(self, projection: str, stream_id: str) -> int:
        ...

    def save_max(self, projection: str, stream_id: str, seq: int) -> int:
        ...


@runtime_checkable
class ProjectionCheckpointReceiptStore(ProjectionCheckpointStore, Protocol):
    """Checkpoint capability required for strict event identity checks."""

    def receipts_complete(
        self, projection: str, stream_id: str, through_seq: int
    ) -> bool:
        """Return whether receipts cover every sequence from 1 through N."""
        ...

    def get_receipt(
        self, projection: str, stream_id: str, seq: int
    ) -> tuple[str, str] | None:
        ...

    def save_receipt(
        self,
        projection: str,
        stream_id: str,
        seq: int,
        event_id: str,
        fingerprint: str,
    ) -> None:
        ...


@runtime_checkable
class ProjectionBackend(Protocol):
    name: str

    def health(self) -> bool:
        ...

    def watermark(self, stream_id: str) -> int:
        """Return the highest contiguous sequence applied by this backend.

        Implementations must make this value durable when they need duplicate
        suppression across process restarts. It is also used to close the
        failure window between a successful apply and checkpoint persistence.
        """
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
