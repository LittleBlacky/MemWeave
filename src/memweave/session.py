"""Synchronous session projection for current working memory."""

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import os
import time
from threading import Lock, RLock
from typing import Any, Protocol
from uuid import UUID, uuid4
from weakref import WeakValueDictionary

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError

from .errors import StaleWriteError
from .models import (
    Event,
    EventType,
    MemoryKind,
    MemoryOperation,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    OperationType,
)
from .storage.schema import session_command_leases_table, session_states_table


def _json_default(value: Any) -> str:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    raise TypeError("session projection contains a non-serializable value")


def _json_safe(value: Any) -> Any:
    """Normalize a value through the session snapshot JSON boundary."""

    return json.loads(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
    )


@dataclass
class SessionState:
    session_id: str
    last_seq: int = 0
    recent_messages: list[dict[str, Any]] = field(default_factory=list)
    active_memories: list[MemoryRecord] = field(default_factory=list)


@dataclass(frozen=True)
class SessionCommandResult:
    """The authoritative event and resulting session projection state."""

    event: Event
    state: SessionState


@dataclass(frozen=True)
class SessionLease:
    session_id: str
    owner_id: str
    fencing_token: int
    storage_session_id: str


@dataclass(frozen=True)
class SessionReadResult:
    """A session read together with the event watermark it represents."""

    state: SessionState
    requested_seq: int
    applied_seq: int
    lagging: bool
    degraded: bool = False
    error: str | None = None


class SessionProjectionBackend:
    """ProjectionBackend adapter for dispatching ordered events to SessionStore."""

    def __init__(self, session_store: "SessionStore", name: str = "session"):
        if not isinstance(session_store, SessionStore):
            raise TypeError("session_store must be a SessionStore")
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if not name.strip():
            raise ValueError("name must not be blank")
        self.session_store = session_store
        self.name = name

    def apply(self, event: Event) -> None:
        self.session_store.apply_event(event)

    def health(self) -> bool:
        return True

    def watermark(self, stream_id: str) -> int:
        session_id = self.session_store._session_id_from_stream(stream_id)
        return self.session_store.get(session_id).last_seq


class ProjectionCatchup(Protocol):
    """Minimal contract required by SessionReadBarrier."""

    def target_seq(self, stream_id: str) -> int:
        ...

    def catch_up(self, stream_id: str, target_seq: int) -> int:
        ...


class SessionReadBarrier:
    """Recover a lagging session projection before returning a read result."""

    def __init__(self, session_store: "SessionStore", runtime: ProjectionCatchup):
        if not isinstance(session_store, SessionStore):
            raise TypeError("session_store must be a SessionStore")
        if not hasattr(runtime, "catch_up") or not hasattr(runtime, "target_seq"):
            raise TypeError("runtime must provide catch_up() and target_seq()")
        self.session_store = session_store
        self.runtime = runtime

    def read(
        self,
        session_id: str,
        *,
        stream_id: str | None = None,
        target_seq: int | None = None,
    ) -> SessionReadResult:
        self.session_store._validate_session_id(session_id)
        resolved_stream = stream_id or self.session_store.stream_id_for_session(session_id)
        if self.session_store._session_id_from_stream(resolved_stream) != session_id:
            raise ValueError("stream_id does not match session_id")
        if target_seq is not None:
            if not isinstance(target_seq, int) or isinstance(target_seq, bool):
                raise TypeError("target_seq must be an integer")
            if target_seq < 0:
                raise ValueError("target_seq must not be negative")
        state = self.session_store.get(session_id)
        degraded = False
        error = None
        if target_seq is None:
            try:
                requested_seq = self.runtime.target_seq(resolved_stream)
            except Exception as exc:
                return SessionReadResult(
                    state=state,
                    requested_seq=state.last_seq,
                    applied_seq=state.last_seq,
                    lagging=False,
                    degraded=True,
                    error=str(exc),
                )
        else:
            requested_seq = target_seq
        if state.last_seq < requested_seq:
            try:
                self.runtime.catch_up(resolved_stream, requested_seq)
            except Exception as exc:
                degraded = True
                error = str(exc)
            state = self.session_store.get(session_id)
        return SessionReadResult(
            state=state,
            requested_seq=requested_seq,
            applied_seq=state.last_seq,
            lagging=state.last_seq < requested_seq,
            degraded=degraded,
            error=error,
        )


class SessionStore:
    """Durable, synchronous projection of one session's working state."""

    def __init__(self, database, recent_limit: int = 50, *, tenant_id: str | None = None):
        if not isinstance(recent_limit, int) or isinstance(recent_limit, bool):
            raise TypeError("recent_limit must be an integer")
        if recent_limit < 1:
            raise ValueError("recent_limit must be positive")
        if tenant_id is not None:
            self._validate_namespace(tenant_id, "tenant_id")
        self.database = database
        self.recent_limit = recent_limit
        self.tenant_id = tenant_id
        self._command_locks: WeakValueDictionary[str, RLock] = WeakValueDictionary()
        self._command_locks_guard = Lock()

    @contextmanager
    def command_lock(self, stream_id: str):
        """Serialize append-and-project commands for one logical session.

        The lock is owned by the SessionStore so multiple coordinators sharing
        one projection instance cannot append the next event before the prior
        event has been projected. Durable cross-process ordering remains the
        responsibility of the event/projection runtime.
        """

        session_id = self._session_id_from_stream(stream_id)
        key = self._storage_session_id(session_id)
        with self._command_locks_guard:
            lock = self._command_locks.get(key)
            if lock is None:
                lock = RLock()
                self._command_locks[key] = lock
        with lock:
            yield

    @contextmanager
    def command_lease(
        self,
        stream_id: str,
        *,
        owner_id: str,
        lease_seconds: float = 30.0,
        wait_timeout: float = 30.0,
        poll_interval: float = 0.01,
    ):
        """Acquire a database-backed lease for cross-process command ordering."""
        session_id = self._session_id_from_stream(stream_id)
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id must be a non-empty string")
        if lease_seconds <= 0 or wait_timeout < 0 or poll_interval <= 0:
            raise ValueError("lease timing values are invalid")
        deadline = time.monotonic() + wait_timeout
        lease = None
        while lease is None:
            now = time.time()
            try:
                with self.database.begin() as connection:
                    row = connection.execute(
                        select(session_command_leases_table).where(
                            session_command_leases_table.c.session_id
                            == self._storage_session_id(session_id)
                        )
                    ).mappings().first()
                    if row is None:
                        token = 1
                        connection.execute(
                            insert(session_command_leases_table).values(
                                session_id=self._storage_session_id(session_id),
                                owner_id=owner_id,
                                lease_until=now + lease_seconds,
                                fencing_token=token,
                            )
                        )
                    elif float(row["lease_until"]) <= now:
                        previous_token = int(row["fencing_token"])
                        token = previous_token + 1
                        updated = connection.execute(
                            update(session_command_leases_table)
                            .where(
                                session_command_leases_table.c.session_id
                                == self._storage_session_id(session_id),
                                session_command_leases_table.c.fencing_token
                                == previous_token,
                                session_command_leases_table.c.lease_until <= now,
                            )
                            .values(
                                owner_id=owner_id,
                                lease_until=now + lease_seconds,
                                fencing_token=token,
                            )
                        )
                        if updated.rowcount != 1:
                            token = None
                    else:
                        token = None
                    if token is not None:
                        lease = SessionLease(
                            session_id=session_id,
                            owner_id=owner_id,
                            fencing_token=token,
                            storage_session_id=self._storage_session_id(session_id),
                        )
            except IntegrityError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring session lease for {stream_id}")
            except OperationalError as exc:
                if not self._is_retryable_lease_error(exc):
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring session lease for {stream_id}") from exc
            if lease is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring session lease for {stream_id}")
                time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
        try:
            yield lease
        finally:
            with self.database.begin() as connection:
                connection.execute(
                    update(session_command_leases_table).where(
                        session_command_leases_table.c.session_id
                        == self._storage_session_id(session_id),
                        session_command_leases_table.c.owner_id == owner_id,
                        session_command_leases_table.c.fencing_token == lease.fencing_token,
                    ).values(lease_until=0.0)
                )

    def apply_event(self, event: Event, *, lease: SessionLease | None = None) -> SessionState:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        session_id = self._session_id_from_stream(event.stream_id)
        storage_session_id = self._storage_session_id(session_id)
        if lease is not None and (
            lease.session_id != session_id
            or lease.storage_session_id != storage_session_id
        ):
            raise ValueError("lease does not match event session")
        with self.database.begin() as connection:
            if lease is not None:
                self._assert_lease(connection, storage_session_id, lease)
            state = self._read(connection, storage_session_id, session_id)
            if event.seq <= state.last_seq:
                return state
            if event.seq != state.last_seq + 1:
                raise ValueError(
                    "session event sequence gap: "
                    f"expected {state.last_seq + 1}, got {event.seq}"
                )
            if event.event_type == EventType.MEMORY_COMMAND.value:
                operation = self._operation_from_event(event)
                if operation.scope is not MemoryScope.SESSION or operation.scope_id != session_id:
                    raise ValueError(
                        "memory.command operation scope does not match event session"
                    )
                self._apply_operation_to_state(
                    state,
                    operation,
                    source_seq=event.seq,
                    source_event_id=event.event_id,
                )
            if self._is_recent_event(event):
                self._append_recent_event(state, event)
            state.last_seq = event.seq
            self._write(connection, state)
            return state

    def get(self, session_id: str) -> SessionState:
        self._validate_session_id(session_id)
        storage_session_id = self._storage_session_id(session_id)
        with self.database.read() as connection:
            return self._read(connection, storage_session_id, session_id)

    def upsert_active(self, memory: MemoryRecord) -> None:
        if not isinstance(memory, MemoryRecord):
            raise TypeError("memory must be a MemoryRecord")
        if memory.scope is not MemoryScope.SESSION:
            raise ValueError("session projection requires session-scoped memory")
        if memory.scope_id.strip() == "":
            raise ValueError("memory scope_id must not be blank")
        session_id = memory.scope_id
        storage_session_id = self._storage_session_id(session_id)
        with self.database.begin() as connection:
            state = self._read(connection, storage_session_id, session_id)
            if memory.source_seq > state.last_seq:
                raise ValueError(
                    "memory source_seq must not exceed the session watermark; "
                    "apply the source event first"
                )
            if self._upsert_memory_to_state(state, memory):
                self._write(connection, state)

    def apply_operation(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_event_id: UUID | str,
    ) -> SessionState:
        """Apply one trusted explicit operation to the session projection.

        ``source_seq`` and ``source_event_id`` are supplied by the event/adapter
        boundary, never parsed from user text.  The method is intentionally
        session-scoped; durable cross-session authority is handled later.
        """
        if not isinstance(operation, MemoryOperation):
            raise TypeError("operation must be a MemoryOperation")
        if operation.scope is not MemoryScope.SESSION:
            raise ValueError("session projection requires session-scoped operation")
        if not isinstance(source_seq, int) or isinstance(source_seq, bool):
            raise TypeError("source_seq must be an integer")
        if source_seq < 1:
            raise ValueError("source_seq must be positive")
        if not isinstance(source_event_id, (UUID, str)):
            raise TypeError("source_event_id must be a UUID or string")
        source_event_id_value = str(source_event_id)
        if not source_event_id_value.strip():
            raise ValueError("source_event_id must not be blank")

        session_id = operation.scope_id
        storage_session_id = self._storage_session_id(session_id)
        with self.database.begin() as connection:
            state = self._read(connection, storage_session_id, session_id)
            if source_seq > state.last_seq:
                raise ValueError(
                    "source_seq must not exceed the session watermark; apply the source event first"
                )
            if self._apply_operation_to_state(
                state,
                operation,
                source_seq=source_seq,
                source_event_id=source_event_id_value,
            ):
                self._write(connection, state)
            return state

    @staticmethod
    def _is_recent_event(event: Event) -> bool:
        return event.event_type in {
            EventType.USER_MESSAGE.value,
            EventType.MODEL_INPUT.value,
            EventType.MODEL_OUTPUT.value,
            EventType.TOOL_CALLED.value,
            EventType.TOOL_COMPLETED.value,
            EventType.MEMORY_COMMAND.value,
        } or event.event_type.startswith("turn.")

    def _append_recent_event(self, state: SessionState, event: Event) -> None:
        payload = _json_safe(event.payload)
        state.recent_messages.append(
            {
                "event_id": str(event.event_id),
                "seq": event.seq,
                "event_type": event.event_type,
                "actor": event.actor,
                "payload": payload,
            }
        )
        state.recent_messages = state.recent_messages[-self.recent_limit :]

    @staticmethod
    def _operation_from_event(event: Event) -> MemoryOperation:
        raw_operation = event.payload.get("operation")
        if not isinstance(raw_operation, dict):
            raise ValueError("memory.command event payload requires an operation object")
        try:
            return MemoryOperation.model_validate(raw_operation)
        except Exception as exc:
            raise ValueError("memory.command event contains an invalid operation") from exc

    @staticmethod
    def _upsert_memory_to_state(state: SessionState, memory: MemoryRecord) -> bool:
        existing_index = next(
            (i for i, item in enumerate(state.active_memories) if item.key == memory.key),
            None,
        )
        if existing_index is not None:
            existing = state.active_memories[existing_index]
            if existing.source_seq > memory.source_seq:
                return False
            if existing.source_seq == memory.source_seq:
                if (
                    existing.key == memory.key
                    and existing.value == memory.value
                    and existing.kind is memory.kind
                    and existing.status is memory.status
                    and set(existing.source.event_ids).intersection(
                        memory.source.event_ids
                    )
                ):
                    return False
                raise StaleWriteError(
                    f"conflicting session write for key {memory.key!r} at source_seq {memory.source_seq}"
                )
            if memory.version <= existing.version:
                raise StaleWriteError(
                    f"session memory version must increase: existing {existing.version}, got {memory.version}"
                )
            state.active_memories[existing_index] = memory
            return True
        state.active_memories.append(memory)
        return True

    @classmethod
    def _apply_operation_to_state(
        cls,
        state: SessionState,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_event_id: UUID | str,
    ) -> bool:
        source_event_id_value = str(source_event_id)
        existing_index = cls._memory_index(state, operation)
        existing = (
            state.active_memories[existing_index]
            if existing_index is not None
            else None
        )

        if operation.operation is OperationType.FORGET:
            if existing is None or source_seq < existing.source_seq:
                return False
            if (
                operation.expected_version is not None
                and existing.version != operation.expected_version
            ):
                raise StaleWriteError(
                    f"expected session version {operation.expected_version}, got {existing.version}"
                )
            if source_seq == existing.source_seq and source_event_id_value not in existing.source.event_ids:
                raise StaleWriteError(
                    f"conflicting session forget for key {operation.key!r} at source_seq {source_seq}"
                )
            del state.active_memories[existing_index]
            return True

        if operation.operation not in (OperationType.REMEMBER, OperationType.UPDATE):
            raise ValueError(
                f"unsupported session operation: {operation.operation.value}"
            )
        if existing is not None:
            if source_seq < existing.source_seq:
                return False
            if source_seq == existing.source_seq:
                if (
                    existing.value == operation.value
                    and source_event_id_value in existing.source.event_ids
                ):
                    return False
                raise StaleWriteError(
                    f"conflicting session write for key {operation.key!r} at source_seq {source_seq}"
                )

        if operation.operation is OperationType.UPDATE:
            if existing is None:
                actual = existing.version if existing is not None else None
                raise StaleWriteError(
                    f"expected session version {operation.expected_version}, got {actual}"
                )
            if (
                operation.expected_version is not None
                and existing.version != operation.expected_version
            ):
                raise StaleWriteError(
                    f"expected session version {operation.expected_version}, got {existing.version}"
                )
            version = existing.version + 1
            created_at = existing.created_at
        else:
            version = (existing.version + 1) if existing is not None else 1
            created_at = existing.created_at if existing is not None else None

        source = operation.source or MemorySource(
            type="explicit", event_ids=[source_event_id_value]
        )
        if source_event_id_value not in source.event_ids:
            source = MemorySource(
                type=source.type,
                event_ids=[*source.event_ids, source_event_id_value],
                extractor=source.extractor,
            )
        record = MemoryRecord(
            kind=operation.kind or MemoryKind.WORKING,
            scope=MemoryScope.SESSION,
            scope_id=operation.scope_id,
            key=operation.key,
            value=operation.value,
            status=MemoryStatus.SESSION_ONLY,
            confidence=1.0,
            source=source,
            source_seq=source_seq,
            version=version,
            **({"created_at": created_at} if created_at is not None else {}),
        )
        return cls._upsert_memory_to_state(state, record)

    @staticmethod
    def _memory_index(
        state: SessionState, operation: MemoryOperation
    ) -> int | None:
        if operation.key is not None:
            return next(
                (i for i, item in enumerate(state.active_memories) if item.key == operation.key),
                None,
            )
        if operation.memory_id is not None:
            return next(
                (i for i, item in enumerate(state.active_memories) if item.id == operation.memory_id),
                None,
            )
        return None

    @staticmethod
    def _read(
        connection, storage_session_id: str, logical_session_id: str
    ) -> SessionState:
        row = connection.execute(
            select(session_states_table).where(
                session_states_table.c.session_id == storage_session_id
            )
        ).mappings().first()
        if row is None:
            return SessionState(session_id=logical_session_id)
        return SessionState(
            session_id=logical_session_id,
            last_seq=int(row["last_seq"]),
            recent_messages=json.loads(row["recent_messages_json"]),
            active_memories=[MemoryRecord.model_validate(item) for item in json.loads(row["active_memories_json"])],
        )

    def _write(self, connection, state: SessionState) -> None:
        storage_session_id = self._storage_session_id(state.session_id)
        values = {
            "last_seq": state.last_seq,
            "recent_messages_json": json.dumps(state.recent_messages, sort_keys=True),
            "active_memories_json": json.dumps(
                [item.model_dump(mode="json") for item in state.active_memories],
                sort_keys=True,
            ),
        }
        exists = connection.execute(
            select(session_states_table.c.session_id).where(
                session_states_table.c.session_id == storage_session_id
            )
        ).scalar_one_or_none()
        if exists is None:
            connection.execute(
                session_states_table.insert().values(
                    session_id=storage_session_id, **values
                )
            )
        else:
            connection.execute(
                update(session_states_table)
                .where(session_states_table.c.session_id == storage_session_id)
                .values(**values)
            )

    @staticmethod
    def _validate_session_id(session_id: str) -> None:
        if not isinstance(session_id, str):
            raise TypeError("session_id must be a string")
        if not session_id.strip():
            raise ValueError("session_id must not be blank")
        if "/" in session_id:
            raise ValueError("session_id must not contain '/'")

    def _session_id_from_stream(self, stream_id: str) -> str:
        if not isinstance(stream_id, str):
            raise TypeError("stream_id must be a string")
        if not stream_id.strip():
            raise ValueError("stream_id must not be blank")
        marker = "session:"
        if self.tenant_id is not None:
            segments = stream_id.split("/")
            if (
                len(segments) < 2
                or segments[0] != f"tenant:{self.tenant_id}"
                or not segments[-1].startswith(marker)
            ):
                raise ValueError("stream_id does not match configured tenant")
            if any(not segment or ":" not in segment for segment in segments[1:-1]):
                raise ValueError("stream_id contains an invalid scope segment")
            session_id = segments[-1][len(marker) :]
        elif stream_id.startswith("tenant:"):
            raise ValueError("tenant stream_id requires a tenant-scoped SessionStore")
        elif stream_id.startswith(marker):
            session_id = stream_id[len(marker) :]
        else:
            raise ValueError("unscoped stream_id must use session:<id>")
        self._validate_session_id(session_id)
        return session_id

    def stream_id_for_session(self, session_id: str) -> str:
        self._validate_session_id(session_id)
        if self.tenant_id is None:
            return f"session:{session_id}"
        return f"tenant:{self.tenant_id}/session:{session_id}"

    def _storage_session_id(self, session_id: str) -> str:
        self._validate_session_id(session_id)
        if self.tenant_id is None:
            return session_id
        return f"{self.tenant_id}:session:{session_id}"

    @staticmethod
    def _validate_namespace(value: str, name: str) -> None:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value.strip() or ":" in value:
            raise ValueError(f"{name} must be non-empty and must not contain ':'")

    @staticmethod
    def _assert_lease(connection, storage_session_id: str, lease: SessionLease) -> None:
        row = connection.execute(
            select(session_command_leases_table).where(
                session_command_leases_table.c.session_id == storage_session_id
            ).with_for_update()
        ).mappings().first()
        if (
            lease.storage_session_id != storage_session_id
            or row is None
            or row["owner_id"] != lease.owner_id
            or int(row["fencing_token"]) != lease.fencing_token
            or float(row["lease_until"]) <= time.time()
        ):
            raise RuntimeError("session command lease is no longer valid")

    @staticmethod
    def _is_retryable_lease_error(error: OperationalError) -> bool:
        message = str(error).lower()
        return any(
            marker in message
            for marker in (
                "database is locked",
                "deadlock",
                "serialization failure",
                "could not serialize",
            )
        )

class SessionCommandCoordinator:
    """Append explicit commands as events, then synchronously project them."""

    def __init__(self, event_store, session_store: SessionStore):
        if not hasattr(event_store, "append"):
            raise TypeError("event_store must provide append()")
        if not isinstance(session_store, SessionStore):
            raise TypeError("session_store must be a SessionStore")
        self.event_store = event_store
        self.session_store = session_store
        self.owner_id = f"pid:{os.getpid()}:{uuid4()}"

    def append_explicit(
        self,
        operation: MemoryOperation,
        *,
        stream_id: str,
        actor: str,
        request_id: UUID,
        event_id: UUID | None = None,
        causation_id: UUID | None = None,
        correlation_id: UUID | None = None,
        idempotency_key: str | None = None,
        protocol_version: str = "1.0",
    ) -> SessionCommandResult:
        if not isinstance(operation, MemoryOperation):
            raise TypeError("operation must be a MemoryOperation")
        if operation.scope is not MemoryScope.SESSION:
            raise ValueError("explicit session command requires session scope")
        event_session_id = self.session_store._session_id_from_stream(stream_id)
        if operation.scope_id != event_session_id:
            raise ValueError("operation scope_id does not match stream session")

        with self.session_store.command_lock(stream_id):
            with self.session_store.command_lease(
                stream_id, owner_id=self.owner_id
            ) as lease:
                event = self.event_store.append(
                    stream_id=stream_id,
                    event_type=EventType.MEMORY_COMMAND,
                    payload={"operation": operation.model_dump(mode="json")},
                    actor=actor,
                    request_id=request_id,
                    event_id=event_id,
                    causation_id=causation_id,
                    correlation_id=correlation_id,
                    idempotency_key=idempotency_key,
                    protocol_version=protocol_version,
                )
                state = self.session_store.apply_event(event, lease=lease)
                return SessionCommandResult(event=event, state=state)
