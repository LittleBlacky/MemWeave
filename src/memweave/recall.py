"""Deterministic, bounded recall from authoritative projections."""

import json
import re
from typing import Protocol

from .models import (
    ConsistencyMode,
    MemoryRecord,
    MemoryScope,
    MemoryStatus,
    RecallRequest,
    RecallResult,
)


class RecallProvider(Protocol):
    def search(self, request: RecallRequest) -> list[MemoryRecord]:
        ...


_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
_SCOPE_ORDER = {
    MemoryScope.SESSION: 0,
    MemoryScope.PROJECT: 1,
    MemoryScope.USER: 2,
    MemoryScope.TEAM: 3,
    MemoryScope.TENANT: 4,
    MemoryScope.GLOBAL: 5,
}


class RecallService:
    """Merge session-first and durable records with hard read boundaries."""

    def __init__(self, session_store, durable_store, *, provider: RecallProvider | None = None):
        if not hasattr(session_store, "get"):
            raise TypeError("session_store must provide get()")
        if durable_store is not None and not hasattr(durable_store, "list_active"):
            raise TypeError("durable_store must provide list_active()")
        self.session_store = session_store
        self.durable_store = durable_store
        self.provider = provider

    def recall(self, request: RecallRequest) -> RecallResult:
        if not isinstance(request, RecallRequest):
            raise TypeError("request must be a RecallRequest")
        visible = self._visible_scopes(request)
        degraded = False
        candidates: list[MemoryRecord] = []
        durable_watermark = 0

        session_scope = f"{MemoryScope.SESSION.value}:{request.session_id}"
        session_watermark = 0
        if session_scope in visible:
            state = self.session_store.get(request.session_id, stream_id=session_scope)
            session_watermark = state.last_seq
            candidates.extend(state.active_memories)

        if self.provider is not None:
            try:
                candidates.extend(self.provider.search(request))
            except Exception:
                degraded = True

        if self.durable_store is not None:
            for scope_ref in visible:
                scope, scope_id = self._parse_scope(scope_ref)
                if scope is MemoryScope.SESSION:
                    continue
                try:
                    records = self.durable_store.list_active(scope, scope_id)
                    candidates.extend(records)
                    if records:
                        durable_watermark = max(
                            durable_watermark,
                            max(record.source_seq for record in records),
                        )
                except Exception:
                    degraded = True

        items = self._rank_and_merge(candidates, request)
        return RecallResult(
            items=items,
            watermarks={"session": session_watermark, "durable": durable_watermark},
            consistency=request.consistency,
            degraded=degraded,
        )

    @staticmethod
    def _parse_scope(value: str) -> tuple[MemoryScope, str]:
        if not isinstance(value, str) or ":" not in value:
            raise ValueError("visible scope must use '<scope>:<scope_id>'")
        scope_name, scope_id = value.split(":", 1)
        scope = MemoryScope(scope_name)
        if not scope_id.strip():
            raise ValueError("visible scope id must not be blank")
        return scope, scope_id

    @classmethod
    def _visible_scopes(cls, request: RecallRequest) -> set[str]:
        values = set()
        for value in request.visible_scopes:
            cls._parse_scope(value)
            values.add(value)
        return values

    @classmethod
    def _rank_and_merge(cls, candidates: list[MemoryRecord], request: RecallRequest) -> list[MemoryRecord]:
        terms = {term.lower() for term in _TOKEN_RE.findall(request.query)}
        kinds = set(request.kinds)
        visible = set(request.visible_scopes)
        best: dict[str, MemoryRecord] = {}
        for item in candidates:
            if item.status not in (MemoryStatus.ACTIVE, MemoryStatus.SESSION_ONLY):
                continue
            scope_ref = f"{item.scope.value}:{item.scope_id}"
            if scope_ref not in visible:
                continue
            if kinds and item.kind not in kinds:
                continue
            if cls._score(item, terms) == 0:
                continue
            # A key resolves to one visible value. Scope precedence determines
            # which candidate wins when the same key exists at several scopes.
            identity = item.key
            previous = best.get(identity)
            if previous is None or cls._newer(item, previous):
                best[identity] = item
        ranked = sorted(
            best.values(),
            key=lambda item: (
                -cls._score(item, terms),
                _SCOPE_ORDER[item.scope],
                -item.source_seq,
                item.key,
            ),
        )
        result: list[MemoryRecord] = []
        used_tokens = 0
        for item in ranked:
            if len(result) >= request.top_k:
                break
            item_tokens = cls._estimate_tokens(item)
            if used_tokens + item_tokens > request.max_tokens:
                continue
            result.append(item)
            used_tokens += item_tokens
        return result

    @staticmethod
    def _newer(left: MemoryRecord, right: MemoryRecord) -> bool:
        if _SCOPE_ORDER[left.scope] != _SCOPE_ORDER[right.scope]:
            return _SCOPE_ORDER[left.scope] < _SCOPE_ORDER[right.scope]
        return (left.source_seq, left.version, left.updated_at) > (right.source_seq, right.version, right.updated_at)

    @staticmethod
    def _score(item: MemoryRecord, terms: set[str]) -> int:
        haystack = f"{item.key} {json.dumps(item.value, ensure_ascii=False, default=str)}".lower()
        return sum(1 for term in terms if term in haystack)

    @staticmethod
    def _estimate_tokens(item: MemoryRecord) -> int:
        text = f"{item.key} {json.dumps(item.value, ensure_ascii=False, default=str)}"
        return max(1, len(_TOKEN_RE.findall(text)))


__all__ = ["RecallProvider", "RecallService"]
