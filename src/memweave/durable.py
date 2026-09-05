"""Versioned durable memory authority with tombstone masking."""

import json
import math
from contextlib import contextmanager
from datetime import datetime
from hashlib import sha256
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
from .storage.schema import (
    durable_memories_table,
    durable_memory_identities_table,
    durable_memory_writes_table,
)


def _validate_json_value(value: Any, path: str = "value") -> None:
    """Require values that round-trip identically through JSON storage."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must contain only finite numbers")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} object keys must be strings")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise TypeError(f"{path} must contain only JSON-native values")


def _dump_json_value(value: Any) -> str:
    _validate_json_value(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class DurableMemoryStore:
    """Relational authority for long-lived memory versions and tombstones."""

    def __init__(self, database: RelationalDatabase):
        self.database = database

    def create(
        self,
        record: MemoryRecord,
        *,
        source_event_id: UUID | str | None = None,
        source_stream_id: str | None = None,
    ) -> MemoryRecord:
        self._validate_record(record)
        source_event_id = self._validate_source_event_id(source_event_id)
        source_stream_id = self._resolve_source_stream_id(
            record.source, source_stream_id
        )
        if source_stream_id is None:
            raise ValueError("durable write requires source_stream_id")
        source = record.source
        source_updates = {}
        if source_stream_id != source.stream_id:
            source_updates["stream_id"] = source_stream_id
        if source_updates:
            record = record.model_copy(
                update={"source": source.model_copy(update=source_updates)}
            )
        write_fingerprint = self._create_fingerprint(record)
        with self._transaction() as connection:
            source_match = None
            if source_event_id is not None:
                source_match = self._find_write_match(
                    connection,
                    record.scope,
                    record.scope_id,
                    record.key,
                    source_stream_id,
                    source_event_id,
                    operation_type="create",
                    request_fingerprint=write_fingerprint,
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
            if latest is None:
                if record.version != 1:
                    raise StaleWriteError(
                        f"first memory version must be 1, got {record.version}"
                    )
            else:
                if self._same_record(latest, record):
                    if source_event_id is not None:
                        raise StaleWriteError(
                            "memory version already exists without this write identity"
                        )
                    return latest
                if latest.id != record.id:
                    raise StaleWriteError(
                        "memory_id must remain stable across versions of a memory key"
                    )
                self._assert_newer(
                    record.source_seq,
                    latest.source_seq,
                    source_stream_id=record.source.stream_id,
                    current_stream_id=latest.source.stream_id,
                )
                if record.version != latest.version + 1:
                    raise StaleWriteError(
                        f"memory version must be contiguous: expected "
                        f"{latest.version + 1}, got {record.version}"
                    )
                if not self._supersede_latest(connection, latest):
                    raise StaleWriteError(
                        "memory version changed concurrently; retry the create"
                    )
            if source_event_id is None:
                self._insert_record(connection, record)
            else:
                self._insert_record(
                    connection,
                    record,
                    write_stream_id=source_stream_id,
                    write_event_id=source_event_id,
                    operation_type="create",
                    request_fingerprint=write_fingerprint,
                )
            return record

    def update(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int | None = None,
        source_event_id: UUID | str | None = None,
        source_stream_id: str | None = None,
    ) -> MemoryRecord:
        self._validate_operation(operation, OperationType.UPDATE)
        source_seq = self._validate_source_seq(source_seq)
        source_event_id = self._validate_source_event_id(source_event_id)
        source_stream_id = self._resolve_source_stream_id(
            operation.source, source_stream_id
        )
        if source_stream_id is None:
            raise ValueError("durable write requires source_stream_id")
        write_fingerprint = self._operation_fingerprint(
            operation, source_seq=source_seq, source_stream_id=source_stream_id
        )
        with self._transaction() as connection:
            if source_event_id is not None:
                source_match = self._find_write_match(
                    connection,
                    operation.scope,
                    operation.scope_id,
                    operation.key,
                    source_stream_id,
                    source_event_id,
                    operation_type=OperationType.UPDATE.value,
                    request_fingerprint=write_fingerprint,
                )
                if source_match is not None:
                    expected_kind = operation.kind or source_match.kind
                    if (
                        source_match.version > 1
                        and operation.expected_version == source_match.version - 1
                        and source_seq == source_match.source_seq
                        and source_stream_id == source_match.source.stream_id
                        and source_match.status
                        in {MemoryStatus.ACTIVE, MemoryStatus.SUPERSEDED}
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
            self._assert_newer(
                source_seq,
                latest.source_seq,
                source_stream_id=source_stream_id,
                current_stream_id=latest.source.stream_id,
            )
            record = self._updated_record(
                latest,
                operation,
                source_seq=source_seq,
                source_stream_id=source_stream_id,
            )
            if not self._supersede_latest(connection, latest):
                raise StaleWriteError(
                    "memory version changed concurrently; retry the update"
                )
            if source_event_id is None:
                self._insert_record(connection, record)
            else:
                self._insert_record(
                    connection,
                    record,
                    write_stream_id=source_stream_id,
                    write_event_id=source_event_id,
                    operation_type=OperationType.UPDATE.value,
                    request_fingerprint=write_fingerprint,
                )
            return record

    def forget(
        self,
        operation: MemoryOperation,
        *,
        source_seq: int | None = None,
        source_event_id: UUID | str | None = None,
        source_stream_id: str | None = None,
    ) -> MemoryRecord:
        self._validate_operation(operation, OperationType.FORGET)
        source_seq = self._validate_source_seq(source_seq)
        source_event_id = self._validate_source_event_id(source_event_id)
        source_stream_id = self._resolve_source_stream_id(
            operation.source, source_stream_id
        )
        if source_stream_id is None:
            raise ValueError("durable write requires source_stream_id")
        write_fingerprint = self._operation_fingerprint(
            operation, source_seq=source_seq, source_stream_id=source_stream_id
        )
        with self._transaction() as connection:
            if source_event_id is not None:
                source_match = self._find_write_match(
                    connection,
                    operation.scope,
                    operation.scope_id,
                    operation.key,
                    source_stream_id,
                    source_event_id,
                    operation_type=OperationType.FORGET.value,
                    request_fingerprint=write_fingerprint,
                )
                if source_match is not None:
                    if (
                        operation.memory_id is not None
                        and source_match.id != operation.memory_id
                    ):
                        raise StaleWriteError(
                            "source event already applied to a different memory operation"
                        )
                    if source_match.status is MemoryStatus.RETRACTED:
                        if (
                            operation.expected_version == source_match.version - 1
                            and source_seq == source_match.source_seq
                            and source_stream_id == source_match.source.stream_id
                        ):
                            return source_match
                        raise StaleWriteError(
                            "source event already applied with different delete"
                        )
                    raise StaleWriteError(
                        "source event already applied to a different memory operation"
                    )
            latest = self._find_forget_target(connection, operation)
            if latest is None:
                raise StaleWriteError(
                    "cannot forget a missing durable memory target"
                )
            if latest.status is MemoryStatus.RETRACTED:
                if operation.expected_version != latest.version - 1:
                    raise StaleWriteError(
                        f"expected memory version {operation.expected_version}, "
                        f"got {latest.version - 1} for the deleted version"
                    )
                expected_source = self._tombstone_record(
                    latest,
                    source_seq=source_seq,
                    source=operation.source,
                    source_stream_id=source_stream_id,
                ).source
                if (
                    source_seq == latest.source_seq
                    and self._canonical_source(expected_source)
                    == self._canonical_source(latest.source)
                ):
                    return latest
                raise StaleWriteError(
                    "memory delete was already applied with different source metadata"
                )
            if (
                operation.expected_version is not None
                and latest.version != operation.expected_version
            ):
                raise StaleWriteError(
                    f"expected memory version {operation.expected_version}, "
                    f"got {latest.version}"
                )
            self._assert_newer(
                source_seq,
                latest.source_seq,
                source_stream_id=source_stream_id,
                current_stream_id=latest.source.stream_id,
            )
            tombstone = self._tombstone_record(
                latest,
                source_seq=source_seq,
                source=operation.source,
                source_stream_id=source_stream_id,
            )
            if not self._supersede_latest(connection, latest):
                raise StaleWriteError(
                    "memory version changed concurrently; retry the forget"
                )
            if source_event_id is None:
                self._insert_record(connection, tombstone)
            else:
                self._insert_record(
                    connection,
                    tombstone,
                    write_stream_id=source_stream_id,
                    write_event_id=source_event_id,
                    operation_type=OperationType.FORGET.value,
                    request_fingerprint=write_fingerprint,
                )
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
        _validate_json_value(record.value)

    @classmethod
    def _validate_operation(
        cls, operation: MemoryOperation, expected: OperationType
    ) -> None:
        if not isinstance(operation, MemoryOperation):
            raise TypeError("operation must be a MemoryOperation")
        if operation.operation is not expected:
            raise ValueError(f"durable store requires {expected.value} operation")
        if expected in (OperationType.UPDATE, OperationType.FORGET) and operation.expected_version is None:
            raise ValueError(
                f"durable {expected.value} requires expected_version"
            )
        if expected is OperationType.UPDATE:
            _validate_json_value(operation.value)
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
    def _validate_source_seq(source_seq: int | None) -> int:
        if source_seq is None:
            raise ValueError("durable write requires source_seq")
        if not isinstance(source_seq, int) or isinstance(source_seq, bool):
            raise TypeError("source_seq must be an integer")
        if source_seq < 1:
            raise ValueError("source_seq must be positive")
        return source_seq

    @staticmethod
    def _validate_source_event_id(source_event_id: UUID | str | None) -> str | None:
        if source_event_id is None:
            return None
        if isinstance(source_event_id, UUID):
            return str(source_event_id)
        if not isinstance(source_event_id, str) or not source_event_id.strip():
            raise ValueError("source_event_id must be a non-empty string or UUID")
        return source_event_id

    @staticmethod
    def _validate_source_stream_id(source_stream_id: str | None) -> str | None:
        if source_stream_id is None:
            return None
        if not isinstance(source_stream_id, str) or not source_stream_id.strip():
            raise ValueError("source_stream_id must be a non-empty string")
        return source_stream_id

    @classmethod
    def _resolve_source_stream_id(
        cls, source: MemorySource | None, source_stream_id: str | None
    ) -> str | None:
        source_stream_id = cls._validate_source_stream_id(source_stream_id)
        if source is None:
            return source_stream_id
        source_stream_id_from_source = cls._validate_source_stream_id(
            source.stream_id
        )
        if (
            source_stream_id is not None
            and source_stream_id_from_source is not None
            and source_stream_id != source_stream_id_from_source
        ):
            raise ValueError("source_stream_id conflicts with operation source")
        return source_stream_id or source_stream_id_from_source

    @staticmethod
    def _assert_newer(
        source_seq: int,
        current: int,
        *,
        source_stream_id: str | None,
        current_stream_id: str | None,
    ) -> None:
        if source_stream_id == current_stream_id and source_seq <= current:
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
    def _find_write_match(
        connection,
        scope,
        scope_id: str,
        key: str | None,
        write_stream_id: str,
        write_event_id: str,
        *,
        operation_type: str,
        request_fingerprint: str,
    ) -> MemoryRecord | None:
        """Find the version bound to an explicit write identity.

        ``MemorySource.event_ids`` are evidence and may be reused by later
        versions.  Only this registry is authoritative for write replay.
        """
        row = connection.execute(
            select(durable_memory_writes_table).where(
                durable_memory_writes_table.c.write_stream_id == write_stream_id,
                durable_memory_writes_table.c.write_event_id == str(write_event_id),
            )
        ).mappings().first()
        if row is None:
            other_stream = connection.execute(
                select(durable_memory_writes_table.c.write_stream_id).where(
                    durable_memory_writes_table.c.write_event_id == str(write_event_id)
                )
            ).first()
            if other_stream is not None:
                raise StaleWriteError(
                    "source event identity was replayed from a different stream"
                )
            return None
        if (
            row["scope"] != scope.value
            or row["scope_id"] != scope_id
            or (key is not None and row["key"] != key)
        ):
            raise StaleWriteError(
                "write event identity is bound to a different memory"
            )
        if row["operation_type"] is None or row["request_fingerprint"] is None:
            raise StaleWriteError(
                "source event identity predates request fingerprints and cannot be replayed"
            )
        if (
            row["operation_type"] != operation_type
            or row["request_fingerprint"] != request_fingerprint
        ):
            raise StaleWriteError(
                "source event already applied with different request parameters"
            )
        record_row = connection.execute(
            select(durable_memories_table).where(
                durable_memories_table.c.scope == row["scope"],
                durable_memories_table.c.scope_id == row["scope_id"],
                durable_memories_table.c.key == row["key"],
                durable_memories_table.c.version == row["version"],
            )
        ).mappings().first()
        if record_row is None:
            raise StaleWriteError(
                "write event identity points to a missing memory version"
            )
        return DurableMemoryStore._row_to_record(record_row)

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
    def _canonical_source(source: MemorySource | None) -> dict[str, Any] | None:
        if source is None:
            return None
        value = source.model_dump(mode="json")
        value["event_ids"] = sorted(set(value["event_ids"]))
        return value

    @classmethod
    def _create_fingerprint(cls, record: MemoryRecord) -> str:
        payload = record.model_dump(
            mode="json", exclude={"created_at", "updated_at"}
        )
        payload["source"] = cls._canonical_source(record.source)
        return cls._request_fingerprint("create", payload)

    @classmethod
    def _operation_fingerprint(
        cls,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_stream_id: str,
    ) -> str:
        payload = operation.model_dump(mode="json")
        source = operation.source
        if source is not None and source.stream_id != source_stream_id:
            source = source.model_copy(update={"stream_id": source_stream_id})
        payload["source"] = cls._canonical_source(source)
        payload["source_seq"] = source_seq
        payload["source_stream_id"] = source_stream_id
        return cls._request_fingerprint(operation.operation.value, payload)

    @staticmethod
    def _request_fingerprint(operation_type: str, payload: dict[str, Any]) -> str:
        canonical = json.dumps(
            {"operation_type": operation_type, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return sha256(canonical.encode("utf-8")).hexdigest()

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
            and (
                left.status is right.status
                or (
                    left.status is MemoryStatus.SUPERSEDED
                    and right.status is MemoryStatus.ACTIVE
                )
            )
            and left.confidence == right.confidence
            and left.source.stream_id == right.source.stream_id
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
    def _insert_record(
        connection,
        record: MemoryRecord,
        *,
        write_stream_id: str | None = None,
        write_event_id: str | None = None,
        operation_type: str | None = None,
        request_fingerprint: str | None = None,
    ) -> None:
        identity_by_id = connection.execute(
            select(durable_memory_identities_table).where(
                durable_memory_identities_table.c.scope == record.scope.value,
                durable_memory_identities_table.c.scope_id == record.scope_id,
                durable_memory_identities_table.c.memory_id == str(record.id),
            )
        ).mappings().first()
        identity_by_key = connection.execute(
            select(durable_memory_identities_table).where(
                durable_memory_identities_table.c.scope == record.scope.value,
                durable_memory_identities_table.c.scope_id == record.scope_id,
                durable_memory_identities_table.c.key == record.key,
            )
        ).mappings().first()
        if identity_by_id is not None and identity_by_id["key"] != record.key:
            raise StaleWriteError(
                "memory_id is already bound to a different memory key"
            )
        if identity_by_key is not None and identity_by_key["memory_id"] != str(record.id):
            raise StaleWriteError(
                "memory key is already bound to a different memory_id"
            )
        if identity_by_id is None and identity_by_key is None:
            connection.execute(
                insert(durable_memory_identities_table).values(
                    scope=record.scope.value,
                    scope_id=record.scope_id,
                    memory_id=str(record.id),
                    key=record.key,
                )
            )
        connection.execute(
            insert(durable_memories_table).values(
                memory_id=str(record.id),
                scope=record.scope.value,
                scope_id=record.scope_id,
                key=record.key,
                version=record.version,
                kind=record.kind.value,
                value_json=_dump_json_value(record.value),
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
        write_identity = (
            write_stream_id,
            write_event_id,
            operation_type,
            request_fingerprint,
        )
        if any(value is not None for value in write_identity):
            if any(value is None for value in write_identity):
                raise ValueError(
                    "write identity requires stream, event, operation, and fingerprint"
                )
            connection.execute(
                insert(durable_memory_writes_table).values(
                    scope=record.scope.value,
                    scope_id=record.scope_id,
                    key=record.key,
                    version=record.version,
                    write_stream_id=write_stream_id,
                    write_event_id=str(write_event_id),
                    operation_type=operation_type,
                    request_fingerprint=request_fingerprint,
                )
            )

    @staticmethod
    def _updated_record(
        latest: MemoryRecord,
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_stream_id: str | None,
    ) -> MemoryRecord:
        source = DurableMemoryStore._operation_source(
            operation,
            source_seq=source_seq,
            source_stream_id=source_stream_id,
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
        source: MemorySource | None,
        source_stream_id: str | None,
    ) -> MemoryRecord:
        resolved_source = source or MemorySource(
            type="explicit",
            event_ids=[
                str(
                    uuid5(
                        NAMESPACE_URL,
                        f"memweave:durable-forget:{latest.id}:{source_seq}",
                    )
                )
            ],
            stream_id=source_stream_id,
        )
        if (
            source_stream_id is not None
            and resolved_source.stream_id != source_stream_id
        ):
            resolved_source = resolved_source.model_copy(
                update={"stream_id": source_stream_id}
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
        operation: MemoryOperation,
        *,
        source_seq: int,
        source_stream_id: str | None,
    ) -> MemorySource:
        source = operation.source or MemorySource(
            type="explicit",
            event_ids=[
                str(
                    uuid5(
                        NAMESPACE_URL,
                        f"memweave:durable-update:{operation.scope.value}:"
                        f"{operation.scope_id}:{operation.key}:{source_seq}",
                    )
                )
            ],
            stream_id=source_stream_id,
        )
        if (
            source_stream_id is not None
            and source.stream_id != source_stream_id
        ):
            source = source.model_copy(update={"stream_id": source_stream_id})
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
