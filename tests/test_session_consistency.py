from uuid import uuid4

from memweave.db import Database
from memweave.models import (
    Event,
    EventType,
    MemoryKind,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    MemoryStatus,
    OperationType,
)
from memweave.policy import CommandSpec, ExplicitOperationParser, ParseContext
from memweave.session import SessionStore


def make_event(seq, event_type=EventType.USER_MESSAGE, payload=None):
    return Event(
        event_id=uuid4(),
        event_type=event_type,
        stream_id="session:s1",
        seq=seq,
        actor="user:u1",
        payload=payload or {"text": f"message-{seq}"},
    )


def make_memory(value, source_seq):
    return MemoryRecord(
        kind=MemoryKind.WORKING,
        scope=MemoryScope.SESSION,
        scope_id="s1",
        key="database.engine",
        value=value,
        status=MemoryStatus.ACTIVE,
        confidence=1.0,
        source=MemorySource(type="explicit", event_ids=[f"evt-{source_seq}"]),
        source_seq=source_seq,
        version=source_seq,
    )


def test_session_projection_is_visible_after_database_reopen(tmp_path):
    path = str(tmp_path / "memory.db")
    first = SessionStore(Database(path))

    state = first.apply_event(make_event(1, payload={"text": "remember MySQL"}))

    assert state.last_seq == 1
    reopened = SessionStore(Database(path)).get("s1")
    assert reopened.recent_messages[0]["payload"]["text"] == "remember MySQL"


def test_latest_session_memory_wins_over_older_out_of_order_event(tmp_path):
    store = SessionStore(Database(str(tmp_path / "memory.db")))

    store.apply_event(make_event(2))
    store.upsert_active(make_memory("PostgreSQL", source_seq=2))
    store.apply_event(make_event(1))
    store.upsert_active(make_memory("MySQL", source_seq=1))

    state = store.get("s1")
    assert state.last_seq == 2
    assert state.active_memories[0].value == "PostgreSQL"


def test_explicit_parser_recognizes_chinese_and_english_commands():
    context = ParseContext(
        tenant_id="t1",
        user_id="u1",
        session_id="s1",
        project_id="p1",
        current_seq=3,
    )

    remember = ExplicitOperationParser().parse("记住 database.engine = MySQL", context)
    update = ExplicitOperationParser().parse(
        "update database.engine = PostgreSQL", context
    )
    forget = ExplicitOperationParser().parse("忘记 database.engine", context)

    assert remember[0].operation is OperationType.REMEMBER
    assert remember[0].value == "MySQL"
    assert update[0].operation is OperationType.UPDATE
    assert update[0].expected_version == 1
    assert forget[0].operation is OperationType.FORGET
    assert forget[0].key == "database.engine"


def test_ambiguous_text_does_not_create_memory_operation():
    context = ParseContext(
        tenant_id="t1", user_id="u1", session_id="s1", project_id="p1", current_seq=1
    )

    operations = ExplicitOperationParser().parse("我们也许可以使用 PostgreSQL", context)

    assert operations == []


def test_explicit_parser_supports_registered_command_spec():
    context = ParseContext(
        tenant_id="t1", user_id="u1", session_id="s1", project_id="p1", current_seq=1
    )
    parser = ExplicitOperationParser()
    parser.register(
        CommandSpec(
            operation=OperationType.REMEMBER,
            aliases=("保存",),
            grammar="assignment",
        )
    )

    operations = parser.parse("保存 database.engine = SQLite", context)

    assert operations[0].operation is OperationType.REMEMBER
    assert operations[0].key == "database.engine"
    assert operations[0].value == "SQLite"
