"""Domain models shared by the memory core and all adapters."""

from enum import Enum
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .clock import utc_now


class MemoryKind(str, Enum):
    WORKING = "working"
    PROFILE = "profile"
    FACT = "fact"
    DECISION = "decision"
    EXPERIENCE = "experience"
    PROCEDURE = "procedure"


class MemoryScope(str, Enum):
    SESSION = "session"
    USER = "user"
    PROJECT = "project"
    TEAM = "team"
    TENANT = "tenant"
    GLOBAL = "global"


class MemoryStatus(str, Enum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    RETRACTED = "retracted"
    EXPIRED = "expired"
    SESSION_ONLY = "session_only"
    NEEDS_CONFIRMATION = "needs_confirmation"


class EventType(str, Enum):
    TURN_STARTED = "turn.started"
    USER_MESSAGE = "user.message"
    MODEL_INPUT = "model.input"
    MODEL_OUTPUT = "model.output"
    TOOL_CALLED = "tool.called"
    TOOL_COMPLETED = "tool.completed"
    TURN_COMPLETED = "turn.completed"
    EPISODE_COMPLETED = "episode.completed"
    MEMORY_COMMAND = "memory.command"


class OperationType(str, Enum):
    REMEMBER = "remember"
    UPDATE = "update"
    FORGET = "forget"
    CONFIRM = "confirm"


class ConsistencyMode(str, Enum):
    EVENTUAL = "eventual"
    SESSION_CONSISTENT = "session_consistent"
    DURABLE_CONSISTENT = "durable_consistent"


class MemorySource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    event_ids: List[str] = Field(min_length=1)
    extractor: Optional[str] = None


class AuthContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    project_id: Optional[str] = None
    roles: Set[str] = Field(default_factory=set)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    stream_id: str = Field(min_length=1)
    seq: int = Field(ge=1)
    actor: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)
    schema_version: int = Field(default=1, ge=1)
    protocol_version: str = "1.0"
    request_id: UUID = Field(default_factory=uuid4)
    idempotency_key: Optional[str] = None
    occurred_at: datetime = Field(default_factory=utc_now)
    ingested_at: datetime = Field(default_factory=utc_now)
    causation_id: Optional[UUID] = None
    correlation_id: Optional[UUID] = None


class MemoryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    kind: MemoryKind
    scope: MemoryScope
    scope_id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    value: Any
    status: MemoryStatus
    confidence: float = Field(ge=0.0, le=1.0)
    source: MemorySource
    source_seq: int = Field(ge=1)
    version: int = Field(ge=1)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class MemoryOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: OperationType
    kind: Optional[MemoryKind] = None
    scope: MemoryScope
    scope_id: str = Field(min_length=1)
    key: Optional[str] = Field(default=None, min_length=1)
    value: Any = None
    memory_id: Optional[UUID] = None
    expected_version: Optional[int] = Field(default=None, ge=1)
    source: Optional[MemorySource] = None

    @model_validator(mode="after")
    def validate_operation_requirements(self) -> "MemoryOperation":
        if self.operation is OperationType.FORGET and self.memory_id is None and self.key is None:
            raise ValueError("FORGET requires memory_id or key")
        if self.operation in (OperationType.REMEMBER, OperationType.UPDATE) and self.key is None:
            raise ValueError("remember/update requires key")
        return self


class RecallRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    visible_scopes: List[str] = Field(default_factory=list)
    kinds: List[MemoryKind] = Field(default_factory=list)
    top_k: int = Field(default=5, ge=1)
    max_tokens: int = Field(default=1000, ge=1)
    consistency: ConsistencyMode = ConsistencyMode.SESSION_CONSISTENT


class RecallResult(BaseModel):
    items: List[MemoryRecord] = Field(default_factory=list)
    watermarks: Dict[str, int] = Field(default_factory=dict)
    consistency: ConsistencyMode = ConsistencyMode.SESSION_CONSISTENT
    degraded: bool = False


class Watermarks(BaseModel):
    session: int = Field(default=0, ge=0)
    durable: int = Field(default=0, ge=0)
    index: int = Field(default=0, ge=0)
