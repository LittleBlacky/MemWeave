# Agent Memory Protocol Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a transport-neutral Memory Core plus reference L1 Middleware and L3 Tools/MCP adapters that can add reliable memory to an existing Agent without owning its runtime loop.

**Architecture:** The Core is an event-sourced Python library behind explicit ports for events, session projection, durable memory, recall, policy, and jobs. A versioned Memory Protocol translates host-specific lifecycle signals into Core requests. L1 automates recall and synchronous explicit writes through hooks; L3 exposes the same operations as governed tools. L2 API Proxy is specified but deferred.

**Tech Stack:** Python 3.11+, Pydantic v2, SQLite WAL, FastAPI, pytest, anyio, and standard-library asyncio/threading for the local worker.

**Spec:** docs/superpowers/specs/2026-08-29-memory-system-design.md

## Global Constraints

- The immutable event log is the source of truth; session, durable, and search data are projections.
- Current-session reads use `session_consistent` by default; cross-session processing may be eventually consistent.
- Every protocol request and event carries `protocol_version`, `request_id`, session identity, causation and idempotency information.
- Tenant, user, agent, project and role identity are injected by the trusted adapter/service boundary; client payloads cannot widen scope.
- Explicit remember/update/forget/confirm operations are synchronous and visible before the adapter acknowledges the turn.
- Ordinary extraction and indexing are asynchronous, retryable, idempotent, and recoverable from the event log.
- Memory data cannot override system instructions, tool permissions, or adapter capabilities.
- Phase 1 implements text and structured values, deterministic recall, L1 and L3; vector, graph, LLM extraction, L2 proxy and automatic policy evolution remain later work.

---

## File and Module Map

```text
pyproject.toml
src/agent_memory/
  __init__.py              # public exports and protocol version
  models.py                # domain and protocol Pydantic models
  errors.py                # typed protocol/domain errors
  clock.py                 # injectable UTC clock
  db.py                    # SQLite schema and transactions
  events.py                # append-only event store
  session.py               # session projection and working memory
  durable.py               # durable memory authority and CAS resolver
  outbox.py                # retryable asynchronous jobs
  recall.py                # deterministic session-first recall
  policy.py                # explicit command parser and policy hooks
  kernel.py                # transport-neutral MemoryKernel facade
  protocol.py              # protocol envelopes, capabilities, and validation
  adapters/
    __init__.py
    base.py                # AgentAdapter protocol and lifecycle types
    l1_middleware.py       # reference before/context/after-turn adapter
    l3_tools.py            # governed memory tool definitions and dispatcher
  http_api.py              # FastAPI adapter for Core and protocol endpoints
  worker.py                # local outbox worker
tests/
  conftest.py
  test_models.py
  test_protocol.py
  test_events.py
  test_session_consistency.py
  test_durable_versions.py
  test_outbox.py
  test_recall.py
  test_l1_adapter.py
  test_l3_tools.py
  test_http_api.py
  test_phase1_integration.py
```

## Task 1: Project Skeleton, Domain Models, and Protocol Envelopes

**Files:**
- Create: `pyproject.toml`
- Create: `src/agent_memory/__init__.py`
- Create: `src/agent_memory/models.py`
- Create: `src/agent_memory/errors.py`
- Create: `src/agent_memory/clock.py`
- Create: `src/agent_memory/protocol.py`
- Test: `tests/conftest.py`, `tests/test_models.py`, `tests/test_protocol.py`

**Interfaces:**
- `AuthContext`, `Event`, `MemoryRecord`, `MemorySource`, `MemoryOperation`, `RecallRequest`, `RecallResult`, `Watermarks`.
- `ProtocolVersion(major: int, minor: int)`, `RequestEnvelope[T](protocol_version, request_id, session_id, payload, causation_id, idempotency_key)`.
- `CapabilitySet` fields: `before_turn`, `after_turn`, `context_provider`, `model_proxy`, `native_tools`, `tool_events`, `episode_events`.
- `TurnInput`, `ContextEnvelope`, `ProtocolEvent`, `TurnOutcome`.
- Enums `MemoryKind`, `MemoryScope`, `MemoryStatus`, `EventType`, `OperationType`, `ConsistencyMode`.

- [ ] **Step 1: Write failing tests** for required scope/source sequence, confidence bounds, protocol version presence, idempotency key format, and capability serialization.
- [ ] **Step 2: Run `python -m pytest tests/test_models.py tests/test_protocol.py -q`** and verify collection fails because modules do not exist.
- [ ] **Step 3: Implement Pydantic v2 models** with strict validation; reject UPDATE without `expected_version`, FORGET without target, and envelopes missing trusted session/request metadata.
- [ ] **Step 4: Run the focused tests** and verify all pass.
- [ ] **Step 5: Commit** with `feat: define memory protocol and domain models`.

## Task 2: SQLite Authority and Append-Only Event Store

**Files:**
- Create: `src/agent_memory/db.py`
- Create: `src/agent_memory/events.py`
- Test: `tests/test_events.py`

**Interfaces:**
- `Database(path: str)` and `Database.transaction()`.
- `EventStore.append(stream_id, event_type, payload, actor, request_id, event_id=None, occurred_at=None, causation_id=None, correlation_id=None) -> Event`.
- `EventStore.list_after(stream_id, seq) -> list[Event]`; `EventStore.last_seq(stream_id) -> int`.

- [ ] **Step 1: Write failing tests** for sequence allocation, duplicate event idempotency, concurrent writers, protocol metadata persistence, and immutable payload retrieval.
- [ ] **Step 2: Run `python -m pytest tests/test_events.py -q`** and verify failure.
- [ ] **Step 3: Implement SQLite WAL schema** for events, stream heads, projection watermarks, and request idempotency; allocate stream sequence under a write transaction and reject payload mutation.
- [ ] **Step 4: Run the event tests** including a two-thread append stress case.
- [ ] **Step 5: Commit** with `feat: add transactional event authority`.

## Task 3: Session Projection and Explicit Command Policy

**Files:**
- Create: `src/agent_memory/session.py`
- Create: `src/agent_memory/policy.py`
- Test: `tests/test_session_consistency.py`

**Interfaces:**
- `SessionStore.apply_event(event: Event) -> SessionState`; `get(session_id) -> SessionState`; `upsert_active(memory) -> None`.
- `ExplicitOperationParser.parse(text, context: ParseContext) -> list[MemoryOperation]`.
- `ParseContext` contains server-injected tenant/user/session/project and current source sequence.

- [ ] **Step 1: Write failing tests** for Chinese/English remember, update, forget, ambiguous text returning no command, and N unrelated turns preserving the latest working value.
- [ ] **Step 2: Run the session test file** and verify failure.
- [ ] **Step 3: Implement synchronous projection updates** for turn events and explicit commands; insert outbox records in the same transaction. Parser must never accept caller-supplied identity or final source sequence.
- [ ] **Step 4: Run tests** for restart persistence, stale command rejection, and immediate visibility.
- [ ] **Step 5: Commit** with `feat: add session projection and explicit memory policy`.

## Task 4: Durable Memory Authority, Versions, and Tombstones

**Files:**
- Create: `src/agent_memory/durable.py`
- Test: `tests/test_durable_versions.py`

**Interfaces:**
- `DurableMemoryStore.create(record)`, `update(operation)`, `forget(operation)`, `get_active(scope, scope_id, key)`, `list_versions(scope, scope_id, key)`.

- [ ] **Step 1: Write failing tests** for compare-and-swap versions, monotonic `source_seq`, same-value deduplication, idempotent forget, and tombstone masking.
- [ ] **Step 2: Run `python -m pytest tests/test_durable_versions.py -q`** and verify failure.
- [ ] **Step 3: Implement authoritative memories and tombstones**; preserve superseded versions and source evidence, expose only active non-retracted records, and reject stale writes.
- [ ] **Step 4: Run durable tests** including database reopen and out-of-order async updates.
- [ ] **Step 5: Commit** with `feat: add versioned durable memory authority`.

## Task 5: Outbox and Retryable Worker

**Files:**
- Create: `src/agent_memory/outbox.py`
- Create: `src/agent_memory/worker.py`
- Test: `tests/test_outbox.py`

**Interfaces:**
- `Outbox.enqueue(event_id, topic, payload, idempotency_key) -> OutboxItem`.
- `claim(limit, lease_seconds)`, `mark_applied`, `mark_retryable`, `get`.
- `LocalWorker.run_once() -> int`.

- [ ] **Step 1: Write failing tests** for retry, lease expiry, duplicate delivery, five-attempt dead-letter, and replay after restart.
- [ ] **Step 2: Run the outbox tests** and verify failure.
- [ ] **Step 3: Implement state transitions** `pending → processing → applied|retryable|dead_letter`, exponential backoff capped at five minutes, and handler idempotency checks.
- [ ] **Step 4: Run the outbox suite** and verify deterministic retries using an injected clock.
- [ ] **Step 5: Commit** with `feat: add retryable outbox worker`.

## Task 6: Recall Service with Budget and Consistency Controls

**Files:**
- Create: `src/agent_memory/recall.py`
- Test: `tests/test_recall.py`

**Interfaces:**
- `RecallRequest(query, session_id, visible_scopes, kinds, top_k, max_tokens, consistency)`.
- `RecallService.recall(request) -> RecallResult` and replaceable `RecallProvider.search(request)`.

- [ ] **Step 1: Write failing tests** for session-over-durable precedence, scope isolation, tombstone filtering, stale-version exclusion, token budget, and durable-unavailable fallback.
- [ ] **Step 2: Run the recall tests** and verify failure.
- [ ] **Step 3: Implement deterministic exact/keyword recall** with session-first merge, source-sequence ordering, permission filtering, deduplication, top-k and token limits; mark injected values as memory data.
- [ ] **Step 4: Run focused recall tests** and verify all constraints.
- [ ] **Step 5: Commit** with `feat: add bounded session-first recall`.

## Task 7: MemoryKernel and L1 Middleware Adapter

**Files:**
- Create: `src/agent_memory/kernel.py`
- Create: `src/agent_memory/adapters/base.py`
- Create: `src/agent_memory/adapters/l1_middleware.py`
- Test: `tests/test_l1_adapter.py`

**Interfaces:**
- `MemoryKernel.append_event(...)`, `recall(request)`, `remember(operation)`, `forget(operation)`, `session_state(session_id)`.
- `AgentAdapter.capabilities() -> CapabilitySet`.
- `L1Middleware.start_turn(TurnInput) -> TurnHandle`; `provide_context(handle) -> ContextEnvelope`; `record_event(handle, ProtocolEvent)`; `finish_turn(handle, TurnOutcome)`.

- [ ] **Step 1: Write failing tests** proving before-turn recall is automatic, explicit remember/update/forget is synchronous, after-turn emits events and enqueues extraction, and adapter reports `session_consistent` plus degradation flags.
- [ ] **Step 2: Run `python -m pytest tests/test_l1_adapter.py -q`** and verify failure.
- [ ] **Step 3: Implement Kernel orchestration and L1 hooks**; short-circuit explicit commands before model execution, inject bounded context envelopes, propagate request/idempotency metadata, and never let memory text become instructions.
- [ ] **Step 4: Run adapter tests** with a fake host Agent and verify the N-turn race scenario returns the latest session projection.
- [ ] **Step 5: Commit** with `feat: add l1 middleware adapter`.

## Task 8: L3 Tools/MCP Adapter and HTTP Surface

**Files:**
- Create: `src/agent_memory/adapters/l3_tools.py`
- Create: `src/agent_memory/http_api.py`
- Create: `src/agent_memory/adapters/__init__.py`
- Test: `tests/test_l3_tools.py`, `tests/test_http_api.py`

**Interfaces:**
- `ToolDispatcher.list_tools() -> list[ToolSpec]`.
- `ToolDispatcher.invoke(name, arguments, auth: AuthContext, request_id) -> ToolResult`.
- Tools: `memory.search`, `memory.get`, `memory.remember`, `memory.update`, `memory.forget`.
- HTTP routes: `POST /v1/protocol/turns`, `POST /v1/recall`, `POST /v1/memories`, `DELETE /v1/memories/{memory_id}`.

- [ ] **Step 1: Write failing tests** for generated schemas, server-injected identity, permission rejection, tool budgets, idempotency, and HTTP protocol-version negotiation.
- [ ] **Step 2: Run the L3 and HTTP tests** and verify failure.
- [ ] **Step 3: Implement governed dispatch** using Kernel methods; reject unknown fields and client scope overrides, enforce per-request top-k/token/tool-call limits, and return watermarks/consistency/degraded metadata.
- [ ] **Step 4: Run focused tests** and verify tool and HTTP behavior.
- [ ] **Step 5: Commit** with `feat: add governed l3 tools and protocol http api`.

## Task 9: Integration, Recovery, and Documentation

**Files:**
- Create: `tests/test_phase1_integration.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing end-to-end tests** covering L1 automatic recall, L3 explicit search, N delayed outbox turns, duplicate/乱序 delivery, worker restart, tombstone filtering, and two-tenant isolation.
- [ ] **Step 2: Run `python -m pytest tests/test_phase1_integration.py -q`** and verify only unimplemented behavior fails.
- [ ] **Step 3: Implement missing integration glue** without bypassing Core interfaces; use fake clock and explicit worker ticks, never sleeps longer than 100 ms.
- [ ] **Step 4: Run `python -m pytest -q`, `python -m compileall src`, and `git diff --check`.**
- [ ] **Step 5: Document** Core/Protocol/Adapter boundaries, L1/L3 examples, consistency modes, worker startup, and L2 deferral in `README.md`.
- [ ] **Step 6: Commit** with `test: verify phase one protocol and memory recovery`.

## Plan Self-Review

Spec coverage: Tasks 1–2 cover protocol metadata and event sourcing; Tasks 3–6 cover session, durable state, lifecycle, outbox, recall, scope, and budgets; Tasks 7–8 cover the required L1 and L3 adapter contracts and HTTP/tool governance; Task 9 covers phase-one acceptance and documentation. L2, vector, graph, extraction, experience synthesis, and policy evolution are explicitly deferred as required by the spec.

Placeholder scan: No TODO/TBD or unspecified test steps are used. Every public type and method referenced by a later task is defined in an earlier task.

Type consistency: Adapters depend only on `AgentAdapter`, `MemoryKernel`, and protocol models; HTTP and tools never access SQLite tables directly. Identity is always supplied by `AuthContext` from the service boundary.
