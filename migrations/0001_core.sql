CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    protocol_version TEXT NOT NULL,
    request_id TEXT NOT NULL,
    idempotency_key TEXT,
    occurred_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    causation_id TEXT,
    correlation_id TEXT,
    UNIQUE(stream_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream_idempotency
    ON events(stream_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS stream_heads (
    stream_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_watermarks (
    projection TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    last_seq INTEGER NOT NULL,
    PRIMARY KEY(projection, stream_id)
);
