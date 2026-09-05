# Agent Memory Network Phase 2 Implementation Plan

> Phase 2 builds automated semantic memory on top of the reliable Phase 1 substrate. It does not
> implement Skill execution or unrestricted Agent self-modification.

## Goal

Turn ordinary Agent events into governed, evidence-backed memory candidates and make those memories
discoverable through semantic and hybrid retrieval without weakening Phase 1 consistency, scope, version,
permission, audit, or deletion guarantees.

## Boundary

Phase 2 includes:

- candidate extraction for facts, preferences, constraints, and decisions;
- candidate persistence, idempotency, evidence, and lifecycle;
- normalization, resolver decisions, conflict handling, and promotion policy;
- relation metadata needed for evidence and basic semantic association;
- vector/keyword hybrid retrieval with authoritative recheck;
- user confirmation, sensitive-data governance, and evaluation;
- L2 proxy and additional host adapters where they only translate trusted events.

Phase 2 excludes:

- Episode-to-Experience synthesis as a general capability;
- Skill or Workflow execution and automatic publication;
- multi-hop graph reasoning as a production dependency;
- automatic changes to prompts, tools, models, or production code;
- unrestricted online policy self-modification.

## Non-Negotiable Invariants

1. Extractors produce `MemoryCandidate` objects only; they never write active durable memory directly.
2. Every candidate carries `evidence_event_ids`, `source_stream_id`, `source_seq`, extractor identity/version,
   confidence, scope hint, sensitivity, and an idempotency fingerprint.
3. Candidate scope is a hint. The trusted adapter/policy computes the effective scope.
4. Active durable writes use the Phase 1 source metadata, expected version, CAS, tombstone, and write identity.
5. A late or lower-authority candidate cannot silently replace a newer or canonical memory.
6. Vector and keyword indexes are discovery projections only; every result is rechecked against authority,
   scope, version, status, and deletion state before injection.
7. Prediction-like output from an extractor remains a candidate or discarded result and cannot become an
   active fact without the normal evidence and policy path.

## Proposed Modules

```text
src/memweave/
  extraction.py          # extractor protocol and provider adapters
  candidates.py          # candidate model, lifecycle, and store
  resolver.py            # normalize, compare, conflict, and promotion decisions
  relations.py           # evidence and typed relation ports
  indexing.py             # index job payloads and authoritative recheck helpers
  evaluation.py          # extraction/recall evaluation records and metrics
```

Concrete vendor adapters remain optional packages. The core depends only on ports.

## Work Breakdown

### P2-T1: Candidate model and durable lifecycle

Define `MemoryCandidate`, `CandidateStatus`, `ExtractionRun`, and `CandidateStore`. Persist candidate
payloads and fingerprints so retries and duplicate extraction runs are safe. Add tests for serialization,
evidence requirements, status transitions, deduplication, and restart recovery.

### P2-T2: Extraction runtime

Define a replaceable `MemoryExtractor` provider contract and a rules baseline. Add an LLM provider adapter
behind the same contract, batch events by stream/turn, record extractor version and latency, and enqueue
retryable extraction jobs through Outbox. Extraction remains asynchronous; explicit commands remain
synchronous and take precedence over implicit candidates.

### P2-T3: Normalization, Resolver, and promotion

Normalize keys, structured values, scope hints, aliases, and source metadata. Compare candidates with
authoritative memory and return `CREATE`, `UPDATE`, `IGNORE`, `CONFLICT`, `NEEDS_CONFIRMATION`, or
`DISCARD`. Use canonical-source priority, effective time, evidence quality, expected version, and CAS.
Never raise confidence merely because a memory was repeatedly recalled.

### P2-T4: Governance and confirmation

Implement sensitive-data classification, TTL requirements, user confirmation, audit records, and policy
overrides that cannot disable permission, deletion, or provenance checks. Add tests for ambiguous intent,
cross-scope attempts, stale candidates, sensitive values, and explicit user corrections.

### P2-T5: Relations and semantic indexing

Add typed relation ports for `derived_from`, `supports`, `contradicts`, `applies_to`, `validated_by`, and
`used_in`. Add vector/keyword index ports and idempotent projection jobs. Index payloads must reference
stable memory IDs and versions; deletes and tombstones must propagate before stale results are injectable.

### P2-T6: Hybrid recall

Extend recall with exact, keyword, and vector providers, then fuse and rerank results. Always recheck
authoritative status, scope, version, tombstone, and token budget. Return reasons, source evidence,
watermarks, consistency, and degraded metadata.

### P2-T7: Adapter and evaluation surface

Add L2 proxy and additional host adapters without assuming unsupported lifecycle hooks. Record extraction,
recall, confirmation, correction, and task outcome metrics. Build a fixed evaluation set covering extraction
precision/recall, conflict detection, stale-read rate, scope leakage, and useful recall.

## Acceptance Criteria

1. Natural language produces structured candidates with stable provenance and no direct active writes.
2. Duplicate extraction and worker retries do not create duplicate active memories.
3. Out-of-order candidates cannot overwrite newer or canonical values.
4. Every active automatic memory is traceable to source events and an extraction run.
5. Sensitive or ambiguous candidates are rejected or await confirmation.
6. Index failure or unavailability falls back to authoritative/session recall.
7. Tombstones immediately mask stale vector/keyword results.
8. Hybrid recall respects scope, permissions, top-k, token, timeout, and consistency limits.
9. Relation and index projections can be rebuilt from authoritative data.
10. Evaluation reports extraction quality, recall quality, latency, and false-memory rate.

## Deferred to Phase 3+

Episode boundaries, Experience synthesis, Skill/Workflow registry and execution, multi-hop graph reasoning,
user-intent prediction, and policy/ability evolution require separate plans and evidence semantics.
