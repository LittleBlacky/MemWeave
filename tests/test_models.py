from uuid import uuid4

import pytest
from pydantic import ValidationError

from memweave.models import (
    AuthContext,
    ConsistencyMode,
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


def test_memory_record_requires_scope_and_source_sequence():
    record = MemoryRecord(
        id=uuid4(),
        kind=MemoryKind.FACT,
        scope=MemoryScope.PROJECT,
        scope_id="project-1",
        key="database.engine",
        value="PostgreSQL",
        status=MemoryStatus.ACTIVE,
        confidence=0.96,
        source=MemorySource(type="user_conversation", event_ids=["evt-1"]),
        source_seq=7,
        version=1,
    )

    assert record.scope_id == "project-1"
    assert record.source_seq == 7


def test_memory_record_rejects_confidence_outside_zero_to_one():
    with pytest.raises(ValidationError):
        MemoryRecord(
            id=uuid4(),
            kind=MemoryKind.FACT,
            scope=MemoryScope.PROJECT,
            scope_id="project-1",
            key="database.engine",
            value="PostgreSQL",
            status=MemoryStatus.ACTIVE,
            confidence=1.1,
            source=MemorySource(type="user_conversation", event_ids=["evt-1"]),
            source_seq=7,
            version=1,
        )


def test_update_requires_expected_version():
    with pytest.raises(ValidationError):
        MemoryOperation(
            operation=OperationType.UPDATE,
            kind=MemoryKind.FACT,
            scope=MemoryScope.PROJECT,
            scope_id="project-1",
            key="database.engine",
            value="PostgreSQL",
        )


def test_forget_requires_memory_id_or_key():
    with pytest.raises(ValidationError):
        MemoryOperation(
            operation=OperationType.FORGET,
            kind=MemoryKind.FACT,
            scope=MemoryScope.PROJECT,
            scope_id="project-1",
        )


def test_event_carries_protocol_metadata_and_payload():
    event = Event(
        event_id=uuid4(),
        event_type=EventType.USER_MESSAGE,
        stream_id="session:1",
        seq=1,
        actor="user:user-1",
        payload={"text": "remember PostgreSQL"},
    )

    assert event.stream_id == "session:1"
    assert event.payload["text"] == "remember PostgreSQL"


def test_event_accepts_extension_event_type():
    event = Event(
        event_id=uuid4(),
        event_type="code.test_failed",
        stream_id="session:1",
        seq=2,
        actor="agent:codex",
        payload={"exit_code": 1},
    )

    assert event.event_type == "code.test_failed"


def test_consistency_modes_are_stable_protocol_values():
    assert ConsistencyMode.EVENTUAL.value == "eventual"
    assert ConsistencyMode.SESSION_CONSISTENT.value == "session_consistent"
    assert ConsistencyMode.DURABLE_CONSISTENT.value == "durable_consistent"


def test_auth_context_requires_tenant_and_user_identity():
    context = AuthContext(tenant_id="tenant-1", user_id="user-1", agent_id="agent-1")

    assert context.tenant_id == "tenant-1"
