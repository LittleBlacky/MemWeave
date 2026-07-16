from uuid import uuid4

import pytest
from pydantic import ValidationError

from memweave.protocol import (
    CapabilitySet,
    ContextEnvelope,
    ProtocolEvent,
    ProtocolVersion,
    RequestEnvelope,
    TurnInput,
    TurnOutcome,
)


def test_protocol_version_serializes_as_major_minor():
    version = ProtocolVersion(major=1, minor=0)

    assert version.model_dump() == {"major": 1, "minor": 0}


def test_request_envelope_requires_request_session_and_idempotency_key():
    envelope = RequestEnvelope(
        protocol_version=ProtocolVersion(major=1, minor=0),
        request_id=uuid4(),
        session_id="session-1",
        idempotency_key="agent-1:request-1",
        payload={"text": "hello"},
    )

    assert envelope.session_id == "session-1"

    with pytest.raises(ValidationError):
        RequestEnvelope(
            protocol_version=ProtocolVersion(major=1, minor=0),
            request_id=uuid4(),
            session_id="session-1",
            idempotency_key=" ",
            payload={"text": "hello"},
        )


def test_capability_set_round_trips_all_adapter_capabilities():
    capabilities = CapabilitySet(
        before_turn=True,
        after_turn=True,
        context_provider=True,
        model_proxy=False,
        native_tools=True,
        tool_events=True,
        episode_events=False,
    )

    restored = CapabilitySet.model_validate(capabilities.model_dump())

    assert restored.context_provider is True
    assert restored.model_proxy is False


def test_turn_protocol_models_share_request_identity():
    request_id = uuid4()
    turn = TurnInput(
        protocol_version=ProtocolVersion(major=1, minor=0),
        request_id=request_id,
        session_id="session-1",
        user_message="继续使用 PostgreSQL",
        task_goal="完成数据库迁移",
    )
    event = ProtocolEvent(
        protocol_version=ProtocolVersion(major=1, minor=0),
        request_id=request_id,
        session_id="session-1",
        event_type="user.message",
        idempotency_key="agent-1:req-1:user-1",
        payload={"text": turn.user_message},
    )
    context = ContextEnvelope(
        protocol_version=ProtocolVersion(major=1, minor=0),
        request_id=request_id,
        session_id="session-1",
        items=[],
        token_budget=800,
    )
    outcome = TurnOutcome(
        protocol_version=ProtocolVersion(major=1, minor=0),
        request_id=request_id,
        session_id="session-1",
        status="completed",
    )

    assert {turn.request_id, event.request_id, context.request_id, outcome.request_id} == {request_id}
