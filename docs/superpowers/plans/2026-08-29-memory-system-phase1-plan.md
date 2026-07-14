# Agent Memory System Phase 1 Implementation Plan

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

Goal: Build the first reliable, single-machine memory kernel with session-consistent reads, synchronous explicit memory operations, durable versioned facts, and a transport-neutral SDK.

Architecture: Implement an event-sourced core in Python. SQLite is the local authority for immutable events, session projection, durable memories, and the outbox. Phase 1 uses deterministic exact/keyword recall behind a replaceable provider interface; vector, graph, LLM extraction, and MCP adapters are later phases.

Tech Stack: Python 3.11+, Pydantic v2, SQLite in WAL mode, FastAPI, pytest, anyio, and standard-library asyncio/threading for the local worker.

Spec: docs/superpowers/specs/2026-08-29-memory-system-design.md

## Global Constraints

- Current-session reads are session_consistent; cross-session durable processing is eventually consistent.
- The immutable event log is the source of truth; session, durable, and search data are projections.
- Every event has a stream-local strictly increasing sequence number and a globally unique event ID.
- Every memory record carries kind, scope, status, source evidence, source sequence, and version.
- Explicit remember/update/forget operations are visible before the request returns.
- Vector and graph indexes are derived data and are never the only source of truth.
- Tenant, user, agent, and project identity are injected by the service boundary.
- Memory content cannot override system instructions or tool permissions.
- All writes are idempotent; stale source versions cannot overwrite newer versions.
- Phase 1 supports text and structured values; files and multimodal objects are references only.

---

## File and Module Map

Create focused modules with these responsibilities:

~~~text
pyproject.toml
src/agent_memory/
  __init__.py
  models.py              # domain enums and Pydantic models
  errors.py              # typed domain errors
  clock.py               # injectable UTC clock
  db.py                  # SQLite connection, schema, transactions
  events.py              # append-only event store and sequencing
  session.py             # session projection and active working memory
  durable.py             # authoritative memory table and resolver
  outbox.py              # outbox records and retry states
  recall.py              # exact/keyword recall and merge rules
  policy.py              # explicit-operation parser and write policy
  kernel.py              # transport-neutral MemoryKernel facade
  http_api.py            # FastAPI v1 adapter
  worker.py              # local outbox worker
tests/
  conftest.py
  test_models.py
  test_events.py
  test_session_consistency.py
  test_durable_versions.py
  test_outbox.py
  test_recall.py
  test_http_api.py
  test_phase1_integration.py
~~~

Future vector, graph, extractor, source-adapter, and MCP modules must depend on interfaces in these modules, never on SQLite tables directly.

## Task 1: Project Skeleton and Domain Models

Files:
- Create: pyproject.toml
- Create: src/agent_memory/__init__.py
- Create: src/agent_memory/models.py
- Create: src/agent_memory/errors.py
- Create: src/agent_memory/clock.py
- Test: tests/test_models.py

Interfaces:
- Event, MemoryRecord, MemorySource, MemoryOperation, RecallRequest, RecallResult, Watermarks.
- AuthContext with tenant_id, user_id, agent_id, project_id, and roles.
- Enums MemoryKind, MemoryScope, MemoryStatus, EventType, OperationType, ConsistencyMode.
- utc_now() and Clock.now() -> datetime.
- Create tests/conftest.py with make_record, make_update, and make_request helpers plus database, kernel, and FastAPI client fixtures. Helpers must use explicit defaults for scope, session, source sequence, and version.

- [ ] Step 1: Write failing model tests.

~~~python
def test_memory_record_requires_scope_and_source_seq():
    record = MemoryRecord(
        id=uuid4(), kind=MemoryKind.FACT, scope=MemoryScope.PROJECT,
        scope_id="p1", key="database.engine", value="PostgreSQL",
        status=MemoryStatus.ACTIVE, confidence=0.96,
        source=MemorySource(type="user_conversation", event_ids=["evt-1"]),
        source_seq=7, version=1,
    )
    assert record.scope_id == "p1"
    assert record.source_seq == 7
~~~

- [ ] Step 2: Run python -m pytest tests/test_models.py -q. Expected: FAIL because the package and models do not exist.
- [ ] Step 3: Implement Pydantic validation. Reject confidence outside 0..1, missing scope, UPDATE without expected_version, and FORGET without a target key or memory ID.
- [ ] Step 4: Run the focused tests. Expected: PASS, including invalid input cases.
- [ ] Step 5: Commit with message feat: define memory kernel domain models.

## Task 2: SQLite Authority and Append-Only Event Store

Files:
- Create: src/agent_memory/db.py
- Create: src/agent_memory/events.py
- Test: tests/test_events.py

Interfaces:
- Database(path: str) -> Database
- Database.transaction() -> context manager[sqlite3.Connection]
- EventStore.append(stream_id: str, event_type: EventType, payload: dict[str, Any], actor: str, event_id: UUID | None = None, occurred_at: datetime | None = None, causation_id: UUID | None = None, correlation_id: UUID | None = None) -> Event
- EventStore.list_after(stream_id, seq) -> list[Event]
- EventStore.last_seq(stream_id) -> int

- [ ] Step 1: Write tests proving stream sequences are 1, 2, duplicate event IDs are idempotent, and two concurrent writers never receive the same sequence.
- [ ] Step 2: Run python -m pytest tests/test_events.py -q. Expected: FAIL because schema and EventStore are missing.
- [ ] Step 3: Implement events, stream_heads, and projection_watermarks tables. Enable WAL, foreign keys, busy timeout, unique event_id, and unique stream_id plus seq. Allocate sequence under the write transaction. Store occurred_at, ingested_at, schema_version, causation_id, and correlation_id.
- [ ] Step 4: Run the event suite. Expected: PASS, including concurrent append and duplicate delivery.
- [ ] Step 5: Commit with message feat: add transactional append-only event store.

## Task 3: Session Projection and Synchronous Explicit Operations

Files:
- Create: src/agent_memory/session.py
- Create: src/agent_memory/policy.py
- Test: tests/test_session_consistency.py

Interfaces:
- SessionStore.apply_event(event: Event) -> SessionState
- SessionStore.get(session_id: str) -> SessionState
- SessionStore.upsert_active(memory: MemoryRecord) -> None
- ExplicitOperationParser.parse(text: str, context: ParseContext) -> list[MemoryOperation]
- ParseContext contains tenant_id, user_id, session_id, project_id, and current_seq; parser never receives authority to choose these identities.

- [ ] Step 1: Write tests for remember MySQL, update PostgreSQL, and immediate visibility after reopening the database.
- [ ] Step 2: Run python -m pytest tests/test_session_consistency.py -q. Expected: FAIL because SessionStore and parser are missing.
- [ ] Step 3: Implement recent messages, summary, active memories, and last_seq. Add rule parsing for explicit Chinese and English remember, update, and forget markers. Ambiguous text returns no operation. Apply event, session projection, explicit operation, and outbox insertion in one local transaction.
- [ ] Step 4: Run tests for sequential changes, stale operations, ambiguous text fallback, and N unrelated turns. Expected: PASS.
- [ ] Step 5: Commit with message feat: add session projection and explicit memory operations.

## Task 4: Durable Memory Table, Resolver, and Tombstones

Files:
- Create: src/agent_memory/durable.py
- Test: tests/test_durable_versions.py

Interfaces:
- DurableMemoryStore.create(record: MemoryRecord) -> MemoryRecord
- DurableMemoryStore.update(operation: MemoryOperation) -> MemoryRecord
- DurableMemoryStore.forget(operation: MemoryOperation) -> Tombstone
- DurableMemoryStore.get_active(scope, scope_id, key) -> MemoryRecord | None
- DurableMemoryStore.list_versions(scope, scope_id, key) -> list[MemoryRecord]

- [ ] Step 1: Write tests proving a stale expected_version raises StaleWrite, old values become superseded, and forget creates a tombstone.
- [ ] Step 2: Run python -m pytest tests/test_durable_versions.py -q. Expected: FAIL because durable tables are missing.
- [ ] Step 3: Create memories and tombstones tables. Keep superseded versions for audit; expose only active records. Require compare-and-swap on version and monotonic source_seq for the same key. Make forget idempotent and mask all derived reads.
- [ ] Step 4: Run tests for same-value deduplication, stale writes, tombstone filtering, and restart persistence. Expected: PASS.
- [ ] Step 5: Commit with message feat: add versioned durable memory authority.

## Task 5: Outbox and Retryable Local Worker

Files:
- Create: src/agent_memory/outbox.py
- Create: src/agent_memory/worker.py
- Test: tests/test_outbox.py

Interfaces:
- Outbox.enqueue(event_id, topic, payload) -> OutboxItem
- Outbox.claim(limit, lease_seconds) -> list[OutboxItem]
- Outbox.get(item_id: UUID) -> OutboxItem
- Outbox.mark_applied(item_id) -> None
- Outbox.mark_retryable(item_id, error, next_attempt_at) -> None
- LocalWorker.run_once() -> int

- [ ] Step 1: Write tests for retryable failure, lease expiry, duplicate delivery, and dead-letter after five attempts.
- [ ] Step 2: Run python -m pytest tests/test_outbox.py -q. Expected: FAIL because outbox state is missing.
- [ ] Step 3: Implement pending, processing, applied, retryable, and dead_letter states. Claim with a lease and attempt count. Use exponential backoff capped at five minutes. Require idempotency checks in every handler.
- [ ] Step 4: Run the outbox suite. Expected: PASS, including restart recovery.
- [ ] Step 5: Commit with message feat: add retryable outbox worker.

## Task 6: Recall Planner, Scope Merge, and Token Budget

Files:
- Create: src/agent_memory/recall.py
- Test: tests/test_recall.py

Interfaces:
- RecallRequest(query, session_id, visible_scopes, kinds, top_k, max_tokens, consistency)
- RecallProvider.search(request) -> list[MemoryRecord]
- RecallService.recall(request) -> RecallResult
- The request test helper returns a RecallRequest with session_id s1, visible project p1, top_k 5, max_tokens 1000, and session_consistent unless overridden.

- [ ] Step 1: Write tests proving session PostgreSQL overrides durable MySQL, scope filtering works, and max_tokens is respected.
- [ ] Step 2: Run python -m pytest tests/test_recall.py -q. Expected: FAIL because recall is missing.
- [ ] Step 3: Implement deterministic phase-1 recall: session active memories plus durable active records, normalized key/keyword matching, scope policy, tombstone filtering, source-sequence merge, deduplication, top_k, and token budget. Do not call an LLM or vector index in phase 1. Mark returned values as memory data.
- [ ] Step 4: Run the recall suite. Expected: PASS, including durable-store-unavailable fallback to session state.
- [ ] Step 5: Commit with message feat: add deterministic session-first recall.

## Task 7: MemoryKernel Facade and HTTP v1 API

Files:
- Create: src/agent_memory/kernel.py
- Create: src/agent_memory/http_api.py
- Test: tests/test_http_api.py

Interfaces:
- MemoryKernel(db: Database, auth: AuthContext, clock: Clock) -> MemoryKernel
- MemoryKernel.append_event(stream_id: str, event_type: EventType, payload: dict[str, Any], occurred_at: datetime | None = None, causation_id: UUID | None = None, correlation_id: UUID | None = None) -> Event
- MemoryKernel.recall(request: RecallRequest) -> RecallResult
- MemoryKernel.remember(operation: MemoryOperation) -> MemoryRecord
- MemoryKernel.forget(operation: MemoryOperation) -> Tombstone
- MemoryKernel.session_state(session_id: str) -> SessionState
- POST /v1/events, POST /v1/recall, POST /v1/memories, DELETE /v1/memories/{memory_id}, GET /v1/sessions/{session_id}

- [ ] Step 1: Write API contract tests for remember, recall, forget, session state, missing auth, and cross-scope rejection.
- [ ] Step 2: Run python -m pytest tests/test_http_api.py -q. Expected: FAIL because the kernel and routes are missing.
- [ ] Step 3: Implement MemoryKernel transaction orchestration and FastAPI dependency injection for AuthContext. Construct the kernel with the server-derived AuthContext, ignore client identity fields, enforce scope permissions, and return request_id, watermarks, consistency, and degraded status.
- [ ] Step 4: Run the API suite. Expected: PASS, with no raw memory values in logs.
- [ ] Step 5: Commit with message feat: expose memory kernel sdk and http v1 api.

## Task 8: End-to-End Consistency and Failure Tests

Files:
- Create: tests/test_phase1_integration.py
- Modify: README.md

Interfaces:
- Tests use only MemoryKernel and the v1 API; they do not inspect SQLite tables directly.

- [ ] Step 1: Write scenarios for remember MySQL then update PostgreSQL, N delayed outbox turns, duplicate delivery, worker crash/restart, forget filtering, and two-tenant isolation.
- Use public APIs only; assert values and statuses rather than inspecting SQLite implementation tables.
- Example test shape:

~~~python
def test_delayed_outbox_does_not_hide_latest_session_value(kernel):
    kernel.remember(make_update("project.database", "MySQL", seq=1))
    kernel.remember(make_update("project.database", "PostgreSQL", seq=2))
    for _ in range(20):
        kernel.append_event("session:s1", EventType.USER_MESSAGE, {"text": "unrelated"}, "user:u1")
    result = kernel.recall(make_request("database"))
    assert result.items[0].value == "PostgreSQL"
~~~

- [ ] Step 2: Run python -m pytest tests/test_phase1_integration.py -q. Expected: FAIL only for behavior not yet implemented.
- [ ] Step 3: Inject a fake Clock and explicit worker ticks; avoid sleeps longer than 100 ms. Assert session and durable watermarks.
- [ ] Step 4: Run python -m pytest -q. Expected: PASS.
- [ ] Step 5: Run python -m compileall src. Expected: exit code 0.
- [ ] Step 6: Run git diff --check. Expected: no whitespace errors.
- [ ] Step 7: Document SQLite initialization, WAL mode, worker startup, API examples, consistency modes, and future adapter interfaces in README.md.
- [ ] Step 8: Commit with message test: verify phase one memory consistency and recovery.

## Plan Self-Review

Spec coverage: Tasks 1–4 cover object, event, lifecycle, scope, and version models. Task 5 covers Outbox, retries, and idempotency. Task 6 covers deterministic recall, scope merge, budgets, and degraded reads. Task 7 covers SDK/HTTP boundaries and injected identity. Task 8 covers phase-one acceptance scenarios. Vector, graph, LLM extraction, experience synthesis, and self-evolution are intentionally deferred to later phases as required by the spec.

Placeholder scan: The plan contains no TODO, TBD, implement-later, ellipsis, or unspecified test step. Every helper referenced by a test is defined in Task 1 or the task that owns it.

Type consistency: All public methods use models from Task 1. MemoryKernel is the only facade consumed by HTTP and integration tests; storage and worker details remain behind the interfaces named in each task.
