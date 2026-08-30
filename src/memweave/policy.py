"""Deterministic parser for explicit memory commands."""

import re
from dataclasses import dataclass

from .models import MemoryScope, MemoryOperation, OperationType


@dataclass(frozen=True)
class ParseContext:
    tenant_id: str
    user_id: str
    session_id: str
    project_id: str | None
    current_seq: int


class ExplicitOperationParser:
    _ASSIGNMENT = re.compile(r"^\s*(?P<key>[\w][\w.:-]*)\s*=\s*(?P<value>.+?)\s*$")
    _REMEMBER = re.compile(r"^\s*(?:记住|remember)\s+(?P<body>.+?)\s*$", re.I)
    _UPDATE = re.compile(r"^\s*(?:更新|修改|改成|update)\s+(?P<body>.+?)\s*$", re.I)
    _FORGET = re.compile(r"^\s*(?:忘记|删除|forget|delete)\s+(?P<key>[\w][\w.:-]*)\s*$", re.I)

    def parse(self, text: str, context: ParseContext) -> list[MemoryOperation]:
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        if not isinstance(context, ParseContext):
            raise TypeError("context must be a ParseContext")
        match = self._FORGET.match(text)
        if match:
            return [
                MemoryOperation(
                    operation=OperationType.FORGET,
                    scope=MemoryScope.SESSION,
                    scope_id=context.session_id,
                    key=match.group("key"),
                )
            ]
        for operation, pattern in (
            (OperationType.REMEMBER, self._REMEMBER),
            (OperationType.UPDATE, self._UPDATE),
        ):
            match = pattern.match(text)
            if not match:
                continue
            assignment = self._ASSIGNMENT.match(match.group("body"))
            if not assignment:
                return []
            expected_version = 1 if operation is OperationType.UPDATE else None
            return [
                MemoryOperation(
                    operation=operation,
                    scope=MemoryScope.SESSION,
                    scope_id=context.session_id,
                    key=assignment.group("key"),
                    value=assignment.group("value"),
                    expected_version=expected_version,
                )
            ]
        return []
