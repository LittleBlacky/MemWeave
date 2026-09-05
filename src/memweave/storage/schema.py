"""SQLAlchemy Core table metadata for the relational authority."""

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Text, UniqueConstraint


metadata = MetaData()

events_table = Table(
    "events",
    metadata,
    Column("event_id", String(36), primary_key=True),
    Column("stream_id", String(255), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("event_type", String(255), nullable=False),
    Column("actor", String(255), nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("protocol_version", String(32), nullable=False),
    Column("request_id", String(36), nullable=False),
    Column("idempotency_key", String(512)),
    Column("occurred_at", String(64), nullable=False),
    Column("ingested_at", String(64), nullable=False),
    Column("causation_id", String(36)),
    Column("correlation_id", String(36)),
    UniqueConstraint("stream_id", "seq", name="uq_events_stream_seq"),
    UniqueConstraint(
        "stream_id",
        "idempotency_key",
        name="uq_events_stream_idempotency",
    ),
)

stream_heads_table = Table(
    "stream_heads",
    metadata,
    Column("stream_id", String(255), primary_key=True),
    Column("last_seq", Integer, nullable=False),
)

projection_watermarks_table = Table(
    "projection_watermarks",
    metadata,
    Column("projection", String(255), nullable=False),
    Column("stream_id", String(255), nullable=False),
    Column("last_seq", Integer, nullable=False),
    UniqueConstraint("projection", "stream_id", name="pk_projection_watermarks"),
)

projection_event_receipts_table = Table(
    "projection_event_receipts",
    metadata,
    Column("projection", String(255), nullable=False),
    Column("stream_id", String(255), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("event_id", String(36), nullable=False),
    Column("fingerprint", String(64), nullable=False),
    UniqueConstraint(
        "projection",
        "stream_id",
        "seq",
        name="pk_projection_event_receipts",
    ),
)

session_states_table = Table(
    "session_states",
    metadata,
    Column("session_id", String(255), primary_key=True),
    Column("stream_id", String(512)),
    Column("last_seq", Integer, nullable=False),
    Column("recent_messages_json", Text, nullable=False),
    Column("active_memories_json", Text, nullable=False),
)

session_event_receipts_table = Table(
    "session_event_receipts",
    metadata,
    Column("session_id", String(255), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("event_id", String(36), nullable=False),
    Column("fingerprint", String(64), nullable=False),
    UniqueConstraint("session_id", "seq", name="pk_session_event_receipts"),
)

session_command_leases_table = Table(
    "session_command_leases",
    metadata,
    Column("session_id", String(255), primary_key=True),
    Column("stream_id", String(512)),
    Column("owner_id", String(255), nullable=False),
    Column("lease_until", Float, nullable=False),
    Column("fencing_token", Integer, nullable=False),
)

durable_memories_table = Table(
    "durable_memories",
    metadata,
    Column("memory_id", String(36), nullable=False),
    Column("scope", String(32), nullable=False),
    Column("scope_id", String(255), nullable=False),
    Column("key", String(512), nullable=False),
    Column("version", Integer, nullable=False),
    Column("kind", String(64), nullable=False),
    Column("value_json", Text, nullable=False),
    Column("status", String(32), nullable=False),
    Column("confidence", Float, nullable=False),
    Column("source_json", Text, nullable=False),
    Column("source_seq", Integer, nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint(
        "scope",
        "scope_id",
        "key",
        "version",
        name="uq_durable_memory_scope_key_version",
    ),
)

durable_memory_identities_table = Table(
    "durable_memory_identities",
    metadata,
    Column("scope", String(32), nullable=False),
    Column("scope_id", String(255), nullable=False),
    Column("memory_id", String(36), nullable=False),
    Column("key", String(512), nullable=False),
    UniqueConstraint(
        "scope",
        "scope_id",
        "memory_id",
        name="pk_durable_memory_identity",
    ),
    UniqueConstraint(
        "scope",
        "scope_id",
        "key",
        name="uq_durable_memory_identity_key",
    ),
)

schema_migrations_table = Table(
    "schema_migrations",
    metadata,
    Column("version", String(255), primary_key=True),
    Column("applied_at", String(64), nullable=False),
)

outbox_table = Table(
    "outbox",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("event_id", String(36), nullable=False),
    Column("topic", String(255), nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("idempotency_key", String(512), nullable=False, unique=True),
    Column("status", String(32), nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("available_at", String(64), nullable=False),
    Column("locked_at", String(64)),
    Column("last_error", Text),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

outbox_consumer_receipts_table = Table(
    "outbox_consumer_receipts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("outbox_id", String(36), nullable=False),
    Column("consumer_id", String(255), nullable=False),
    Column("idempotency_key", String(512), nullable=False),
    Column("status", String(32), nullable=False),
    Column("locked_at", String(64)),
    Column("consumed_at", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint(
        "consumer_id",
        "idempotency_key",
        name="uq_outbox_consumer_receipt_key",
    ),
)
