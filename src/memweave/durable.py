"""Versioned durable memory authority with tombstone masking."""

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from .clock import utc_now
from .errors import StaleWriteError
from .models import (
    MemoryKind,
    MemoryOperation,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    OperationType,
)
from .storage.ports import RelationalDatabase
from .storage.schema import durable_memories_table


def _json_default(value: Any) -> str:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    raise TypeError("durable memory value contains a non-serializable value")


class DurableMemoryStore:
    """Relational authority for long-lived memory versions and tombstones."""

    def __init__(self, database: RelationalDatabase):
        self.database = database

    def create(self, record: MemoryRecord) -> MemoryRecord:
        self._validate_record(record)
        with self._transaction() as connection:
            source_match = self._find_source_match(
                connection, record.scope, record.scope_id, record.key, record.source.event_ids
            )
            if source_match is not None:
                if self._same_record_content(source_match, record):
                    return source_match
                raise StaleWriteError(
                    "source event already applied with different memory content"
                )
            latest = self._latest_for_key(
                connection, record.scope, record.scope_id, record.key
            )
            if latest is not None:
                if self._same_record(latest, record):
                    return latest
                if latest.id != record.id:
                    raise StaleWriteError(
                        "memory_id must remain stable across versions of a memory key"
                    )
                self._assert_newer(record.source_seq, latest.source_seq)
                if record.version <= latest.version:
                    raise StaleWriteError(
                        f"memory version must increase: existing {latest.version}, "
                        f"got {record.version}"
                    )
                if not self._supersede_latest(connection, latest):
                    raise StaleWriteError(
                        "memory version changed concurrently; retry the create"
                    )
            self._insert_record(connection, record)
            return record

    def update(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int | None = None,
        source_event_id: UUID | str | None = None,
    ) -> MemoryRecord:
        self._validate_operation(operation, OperationType.UPDATE)
        with self._transaction() as connection:
            if source_event_id is not None:
                source_match = self._find_source_match(
                    connection,
                    operation.scope,
                    operation.scope_id,
                    operation.key,
                    [str(source_event_id)],
                )
                if source_match is not None:
                    expected_kind = operation.kind or source_match.kind
                    if (
                        source_match.status is MemoryStatus.ACTIVE
                        and source_match.value == operation.value
                        and source_match.kind is expected_kind
                    ):
                        return source_match
                    raise StaleWriteError(
                        "source event already applied with different update"
                    )
            latest = self._latest_for_key(
                connection, operation.scope, operation.scope_id, operation.key
            )
            if latest is None or latest.status is not MemoryStatus.ACTIVE:
                raise StaleWriteError(
                    f"cannot update inactive memory key {operation.key!r}"
                )
            if (
                operation.expected_version is not None
                and latest.version != operation.expected_version
            ):
                raise StaleWriteError(
                    f"expected memory version {operation.expected_version}, "
                    f"got {latest.version}"
                )
            resolved_source_seq = self._resolve_source_seq(
                source_seq, latest.source_seq
            )
            self._assert_newer(resolved_source_seq, latest.source_seq)
            record = self._updated_record(
                latest,
                operation,
                source_seq=resolved_source_seq,
                source_event_id=source_event_id,
            )
            if not self._supersede_latest(connection, latest):
                raise StaleWriteError(
                    "memory version changed concurrently; retry the update"
                )
            self._insert_record(connection, record)
            return record

    def forget(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int | None = None,
        source_event_id: UUID | str | None = None,
    ) -> MemoryRecord | None:
        self._validate_operation(operation, OperationType.FORGET)
        with self._transaction() as connection:
            if source_event_id is not None:
                source_match = self._find_source_match(
                    connection,
                    operation.scope,
                    operation.scope_id,
                    operation.key,
                    [str(source_event_id)],
                )
                if source_match is not None:
                    if source_match.status is MemoryStatus.RETRACTED:
                        return source_match
                    raise StaleWriteError(
                        "source event already applied to a different memory operation"
                    )
            latest = self._find_forget_target(connection, operation)
            if latest is None:
                return None
            resolved_source_seq = self._resolve_source_seq(
                source_seq, latest.source_seq
            )
            if latest.status is MemoryStatus.RETRACTED:
                if resolved_source_seq <= latest.source_seq:
                    return latest
                return latest
            if (
                operation.expected_version is not None
                and latest.version != operation.expected_version
            ):
                raise StaleWriteError(
                    f"expected memory version {operation.expected_version}, "
                    f"got {latest.version}"
                )
            self._assert_newer(resolved_source_seq, latest.source_seq)
            tombstone = self._tombstone_record(
                latest,
                source_seq=resolved_source_seq,
                source_event_id=source_event_id,
                source=operation.source,
            )
            if not self._supersede_latest(connection, latest):
                raise StaleWriteError(
                    "memory version changed concurrently; retry the forget"
                )
            self._insert_record(connection, tombstone)
            return tombstone

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        """Translate unique-version races into the public stale-write error."""
        try:
            with self.database.begin() as connection:
                yield connection
        except IntegrityError as exc:
            raise StaleWriteError(
                "durable memory version changed concurrently; retry the operation"
            ) from exc

    def get_active(
        self, scope: MemoryScope, scope_id: str, key: str
    ) -> MemoryRecord | None:
        self._validate_scope_args(scope, scope_id, key)
        with self.database.read() as connection:
            latest = self._latest_for_key(connection, scope, scope_id, key)
        if latest is None or latest.status is not MemoryStatus.ACTIVE:
            return None
        return latest

    def list_versions(
        self, scope: MemoryScope, scope_id: str, key: str
    ) -> list[MemoryRecord]:
        self._validate_scope_args(scope, scope_id, key)
        with self.database.read() as connection:
            rows = connection.execute(
                select(durable_memories_table)
                .where(
                    durable_memories_table.c.scope == scope.value,
                    durable_memories_table.c.scope_id == scope_id,
                    durable_memories_table.c.key == key,
                )
                .order_by(durable_memories_table.c.version)
            ).mappings().all()
        return [self._row_to_record(row) for row in rows]

    @staticmethod
    def _validate_record(record: MemoryRecord) -> None:
        if not isinstance(record, MemoryRecord):
            raise TypeError("record must be a MemoryRecord")
        if record.status is not MemoryStatus.ACTIVE:
            raise ValueError(
                "durable authority create requires an active record; "
                "candidate and lifecycle transition states must use their "
                "dedicated workflow"
            )

    @classmethod
    def _validate_operation(
        cls, operation: MemoryOperation, expected: OperationType
    ) -> None:
        if not isinstance(operation, MemoryOperation):
            raise TypeError("operation must be a MemoryOperation")
        if operation.operation is not expected:
            raise ValueError(f"durable store requires {expected.value} operation")
        cls._validate_scope_args(operation.scope, operation.scope_id, operation.key)

    @staticmethod
    def _validate_scope_args(scope, scope_id: str, key: str | None) -> None:
        if not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if not isinstance(scope_id, str) or not scope_id.strip():
            raise ValueError("scope_id must be a non-empty string")
        if key is not None and (not isinstance(key, str) or not key.strip()):
            raise ValueError("key must be a non-empty string")

    @staticmethod
    def _resolve_source_seq(source_seq: int | None, current: int) -> int:
        if source_seq is None:
            return current + 1
        if not isinstance(source_seq, int) or isinstance(source_seq, bool):
            raise TypeError("source_seq must be an integer")
        if source_seq < 1:
            raise ValueError("source_seq must be positive")
        return source_seq

    @staticmethod
    def _assert_newer(source_seq: int, current: int) -> None:
        if source_seq <= current:
            raise StaleWriteError(
                f"memory source_seq must increase: existing {current}, got {source_seq}"
            )

    @staticmethod
    def _latest_for_key(connection, scope, scope_id: str, key: str):
        row = connection.execute(
            select(durable_memories_table)
            .where(
                durable_memories_table.c.scope == scope.value,
                durable_memories_table.c.scope_id == scope_id,
                durable_memories_table.c.key == key,
            )
            .order_by(durable_memories_table.c.version.desc())
            .limit(1)
        ).mappings().first()
        return None if row is None else DurableMemoryStore._row_to_record(row)

    @staticmethod
    def _find_source_match(connection, scope, scope_id: str, key: str, event_ids):
        """Find a prior version carrying one of the source evidence IDs.

        Source evidence is stored as structured JSON rather than a separate
        idempotency table.  Reading the small per-key history keeps the
        authority portable across relational backends and makes replay
        semantics independent of the caller's in-memory record instance.
        """
        wanted = {str(event_id) for event_id in event_ids}
        if not wanted:
            return None
        rows = connection.execute(
            select(durable_memories_table)
            .where(
                durable_memories_table.c.scope == scope.value,
                durable_memories_table.c.scope_id == scope_id,
                durable_memories_table.c.key == key,
            )
            .order_by(durable_memories_table.c.version.desc())
        ).mappings().all()
        for row in rows:
            source = json.loads(row["source_json"])
            if wanted.intersection(str(item) for item in source.get("event_ids", [])):
                return DurableMemoryStore._row_to_record(row)
        return None

    @staticmethod
    def _find_forget_target(connection, operation: MemoryOperation):
        by_key = None
        by_id = None
        if operation.key is not None:
            by_key = DurableMemoryStore._latest_for_key(
                connection, operation.scope, operation.scope_id, operation.key
            )
        if operation.memory_id is not None:
            row = connection.execute(
                select(durable_memories_table)
                .where(
                    durable_memories_table.c.scope == operation.scope.value,
                    durable_memories_table.c.scope_id == operation.scope_id,
                    durable_memories_table.c.memory_id == str(operation.memory_id),
                )
                .order_by(durable_memories_table.c.version.desc())
                .limit(1)
            ).mappings().first()
            by_id = None if row is None else DurableMemoryStore._row_to_record(row)
        if by_key is not None and by_id is not None and (
            by_key.key != by_id.key or by_key.id != by_id.id
        ):
            raise ValueError("memory key and memory_id identify different memories")
        if operation.key is not None and by_key is None and operation.memory_id is not None:
            raise ValueError("memory key and memory_id identify different memories")
        return by_key or by_id

    @staticmethod
    def _same_record(left: MemoryRecord, right: MemoryRecord) -> bool:
        return left.model_dump(mode="json") == right.model_dump(mode="json")

    @staticmethod
    def _same_record_content(left: MemoryRecord, right: MemoryRecord) -> bool:
        """Compare replayable content while ignoring write timestamps/evidence order."""
        return (
            left.id == right.id
            and left.kind is right.kind
            and left.scope is right.scope
            and left.scope_id == right.scope_id
            and left.key == right.key
            and left.value == right.value
            and left.status is right.status
            and left.confidence == right.confidence
            and left.source_seq == right.source_seq
            and left.version == right.version
        )

    @staticmethod
    def _supersede_latest(connection, latest: MemoryRecord) -> bool:
        if latest.status in {MemoryStatus.SUPERSEDED, MemoryStatus.RETRACTED}:
            return True
        result = connection.execute(
            update(durable_memories_table)
            .where(
                durable_memories_table.c.scope == latest.scope.value,
                durable_memories_table.c.scope_id == latest.scope_id,
                durable_memories_table.c.key == latest.key,
                durable_memories_table.c.version == latest.version,
                durable_memories_table.c.status == MemoryStatus.ACTIVE.value,
            )
            .values(status=MemoryStatus.SUPERSEDED.value)
        )
        return result.rowcount == 1

    @staticmethod
    def _insert_record(connection, record: MemoryRecord) -> None:
        connection.execute(
            insert(durable_memories_table).values(
                memory_id=str(record.id),
                scope=record.scope.value,
                scope_id=record.scope_id,
                key=record.key,
                version=record.version,
                kind=record.kind.value,
                value_json=json.dumps(
                    record.value, sort_keys=True, separators=(",", ":"), default=_json_default
                ),
                status=record.status.value,
                confidence=record.confidence,
                source_json=json.dumps(
                    record.source.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
                ),
                source_seq=record.source_seq,
                created_at=record.created_at.isoformat(),
                updated_at=record.updated_at.isoformat(),
            )
        )

    @staticmethod
    def _updated_record(
        latest: MemoryRecord,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_event_id: UUID | str | None,
    ) -> MemoryRecord:
        source = DurableMemoryStore._operation_source(
            operation, source_seq=source_seq, source_event_id=source_event_id
        )
        return MemoryRecord(
            id=latest.id,
            kind=operation.kind or latest.kind,
            scope=latest.scope,
            scope_id=latest.scope_id,
            key=latest.key,
            value=operation.value,
            status=MemoryStatus.ACTIVE,
            confidence=latest.confidence,
            source=source,
            source_seq=source_seq,
            version=latest.version + 1,
            created_at=latest.created_at,
            updated_at=utc_now(),
        )

    @staticmethod
    def _tombstone_record(
        latest: MemoryRecord,
        *,
        source_seq: int,
        source_event_id: UUID | str | None,
        source: MemorySource | None,
    ) -> MemoryRecord:
        resolved_source = source or MemorySource(
            type="explicit",
            event_ids=[
                str(
                    source_event_id
                    or uuid5(
                        NAMESPACE_URL,
                        f"memweave:durable-forget:{latest.id}:{source_seq}",
                    )
                )
            ],
        )
        if source_event_id is not None and str(source_event_id) not in resolved_source.event_ids:
            resolved_source = MemorySource(
                type=resolved_source.type,
                event_ids=[*resolved_source.event_ids, str(source_event_id)],
                extractor=resolved_source.extractor,
            )
        return MemoryRecord(
            id=latest.id,
            kind=latest.kind,
            scope=latest.scope,
            scope_id=latest.scope_id,
            key=latest.key,
            value=None,
            status=MemoryStatus.RETRACTED,
            confidence=latest.confidence,
            source=resolved_source,
            source_seq=source_seq,
            version=latest.version + 1,
            created_at=latest.created_at,
            updated_at=utc_now(),
        )

    @staticmethod
    def _operation_source(
        operation: MemoryOperation, *, source_seq: int, source_event_id: UUID | str | None
    ) -> MemorySource:
        source = operation.source or MemorySource(
            type="explicit",
            event_ids=[
                str(
                    source_event_id
                    or uuid5(
                        NAMESPACE_URL,
                        f"memweave:durable-update:{operation.scope.value}:"
                        f"{operation.scope_id}:{operation.key}:{source_seq}",
                    )
                )
            ],
        )
        if source_event_id is not None and str(source_event_id) not in source.event_ids:
            source = MemorySource(
                type=source.type,
                event_ids=[*source.event_ids, str(source_event_id)],
                extractor=source.extractor,
            )
        return source

    @staticmethod
    def _row_to_record(row) -> MemoryRecord:
        return MemoryRecord(
            id=UUID(row["memory_id"]),
            kind=MemoryKind(row["kind"]),
            scope=MemoryScope(row["scope"]),
            scope_id=row["scope_id"],
            key=row["key"],
            value=json.loads(row["value_json"]),
            status=MemoryStatus(row["status"]),
            confidence=float(row["confidence"]),
            source=MemorySource.model_validate(json.loads(row["source_json"])),
            source_seq=int(row["source_seq"]),
            version=int(row["version"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
