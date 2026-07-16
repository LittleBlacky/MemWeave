"""Versioned, framework-neutral messages exchanged with Agent adapters."""

from typing import Any, Dict, Generic, List, Optional, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProtocolVersion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    major: int = Field(ge=1)
    minor: int = Field(ge=0)


PayloadT = TypeVar("PayloadT")


class RequestEnvelope(BaseModel, Generic[PayloadT]):
    model_config = ConfigDict(extra="forbid")

    protocol_version: ProtocolVersion
    request_id: UUID = Field(default_factory=uuid4)
    session_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    payload: PayloadT
    causation_id: Optional[UUID] = None

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key(cls, value: str) -> str:
        if not value.strip() or any(char.isspace() for char in value):
            raise ValueError("idempotency_key must be non-blank and contain no whitespace")
        return value


class CapabilitySet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    before_turn: bool = False
    after_turn: bool = False
    context_provider: bool = False
    model_proxy: bool = False
    native_tools: bool = False
    tool_events: bool = False
    episode_events: bool = False


class TurnInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol_version: ProtocolVersion
    request_id: UUID
    session_id: str = Field(min_length=1)
    user_message: str = Field(min_length=1)
    task_goal: Optional[str] = None
    context_summary: Optional[str] = None


class ContextEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol_version: ProtocolVersion
    request_id: UUID
    session_id: str = Field(min_length=1)
    items: List[Dict[str, Any]] = Field(default_factory=list)
    token_budget: int = Field(ge=0)
    degraded: bool = False


class ProtocolEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol_version: ProtocolVersion
    request_id: UUID
    session_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)
    causation_id: Optional[UUID] = None

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key(cls, value: str) -> str:
        if not value.strip() or any(char.isspace() for char in value):
            raise ValueError("idempotency_key must be non-blank and contain no whitespace")
        return value


class TurnOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol_version: ProtocolVersion
    request_id: UUID
    session_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    error: Optional[str] = None
