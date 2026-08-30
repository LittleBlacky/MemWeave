"""Extensible, deterministic parser for explicit memory commands."""

import re
from dataclasses import dataclass

from .models import MemoryOperation, MemoryScope, OperationType


@dataclass(frozen=True)
class ParseContext:
    tenant_id: str
    user_id: str
    session_id: str
    project_id: str | None
    current_seq: int


@dataclass(frozen=True)
class CommandSpec:
    operation: OperationType
    aliases: tuple[str, ...]
    grammar: str


@dataclass(frozen=True)
class ParserRule:
    spec: CommandSpec
    pattern: re.Pattern[str]

    @classmethod
    def compile(cls, spec: CommandSpec) -> "ParserRule":
        aliases = "|".join(
            re.escape(alias) for alias in sorted(spec.aliases, key=len, reverse=True)
        )
        if spec.grammar == "assignment":
            pattern = re.compile(rf"^\s*(?:{aliases})\s+(?P<body>.+?)\s*$", re.I)
        elif spec.grammar == "key":
            pattern = re.compile(
                rf"^\s*(?:{aliases})\s+(?P<key>[\w][\w.:-]*)\s*$", re.I
            )
        else:
            raise ValueError(f"unsupported command grammar: {spec.grammar}")
        return cls(spec=spec, pattern=pattern)


class ExplicitOperationParser:
    _ASSIGNMENT = re.compile(r"^\s*(?P<key>[\w][\w.:-]*)\s*=\s*(?P<value>.+?)\s*$")

    def __init__(self, rules: tuple[CommandSpec, ...] | None = None):
        self._rules: list[ParserRule] = []
        defaults = rules if rules is not None else (
            CommandSpec(OperationType.REMEMBER, ("记住", "remember"), "assignment"),
            CommandSpec(OperationType.UPDATE, ("更新", "修改", "改成", "update"), "assignment"),
            CommandSpec(OperationType.FORGET, ("忘记", "删除", "forget", "delete"), "key"),
        )
        for spec in defaults:
            self.register(spec)

    @property
    def rules(self) -> tuple[ParserRule, ...]:
        return tuple(self._rules)

    def register(self, spec: CommandSpec) -> None:
        if not isinstance(spec, CommandSpec):
            raise TypeError("spec must be a CommandSpec")
        if not isinstance(spec.operation, OperationType):
            raise TypeError("command operation must be an OperationType")
        if not spec.aliases or any(
            not isinstance(alias, str) or not alias.strip() for alias in spec.aliases
        ):
            raise ValueError("command aliases must be non-empty strings")
        if spec.grammar not in {"assignment", "key"}:
            raise ValueError(f"unsupported command grammar: {spec.grammar}")
        known = {alias.casefold() for rule in self._rules for alias in rule.spec.aliases}
        if any(alias.casefold() in known for alias in spec.aliases):
            raise ValueError("command alias already registered")
        self._rules.append(ParserRule.compile(spec))

    def parse(self, text: str, context: ParseContext) -> list[MemoryOperation]:
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        if not isinstance(context, ParseContext):
            raise TypeError("context must be a ParseContext")
        for rule in self._rules:
            match = rule.pattern.match(text)
            if not match:
                continue
            if rule.spec.grammar == "key":
                return [MemoryOperation(
                    operation=rule.spec.operation,
                    scope=MemoryScope.SESSION,
                    scope_id=context.session_id,
                    key=match.group("key"),
                )]
            assignment = self._ASSIGNMENT.match(match.group("body"))
            if not assignment:
                return []
            expected_version = 1 if rule.spec.operation is OperationType.UPDATE else None
            return [MemoryOperation(
                operation=rule.spec.operation,
                scope=MemoryScope.SESSION,
                scope_id=context.session_id,
                key=assignment.group("key"),
                value=assignment.group("value"),
                expected_version=expected_version,
            )]
        return []
