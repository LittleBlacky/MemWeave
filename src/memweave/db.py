"""SQLite authority setup and transaction helpers."""

import sqlite3
from contextlib import contextmanager
from typing import Iterator


SCHEMA = """
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
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        connection = self._connect()
        try:
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()
