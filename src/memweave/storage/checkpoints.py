"""Relational persistence for projection processing watermarks."""

import time

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError

from .ports import ProjectionCheckpointStore
from .schema import projection_watermarks_table


class RelationalProjectionCheckpointStore(ProjectionCheckpointStore):
    def __init__(self, database, max_retries: int = 8):
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")
        self.database = database
        self.max_retries = max_retries

    def get(self, projection: str, stream_id: str) -> int:
        self._validate_text(projection, "projection")
        self._validate_text(stream_id, "stream_id")
        with self.database.read() as connection:
            value = connection.execute(
                select(projection_watermarks_table.c.last_seq).where(
                    projection_watermarks_table.c.projection == projection,
                    projection_watermarks_table.c.stream_id == stream_id,
                )
            ).scalar_one_or_none()
        return int(value or 0)

    def save_max(self, projection: str, stream_id: str, seq: int) -> int:
        self._validate_text(projection, "projection")
        self._validate_text(stream_id, "stream_id")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise TypeError("seq must be an integer")
        if seq < 0:
            raise ValueError("seq must not be negative")
        for attempt in range(self.max_retries + 1):
            try:
                return self._save_max_once(projection, stream_id, seq)
            except (IntegrityError, OperationalError) as exc:
                if attempt >= self.max_retries or not self._is_retryable(exc):
                    raise
                time.sleep(0.005 * (2**attempt))

        raise AssertionError("unreachable")

    @staticmethod
    def _validate_text(value: str, name: str) -> None:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value.strip():
            raise ValueError(f"{name} must not be blank")

    def _save_max_once(self, projection: str, stream_id: str, seq: int) -> int:
        with self.database.begin() as connection:
            updated = connection.execute(
                update(projection_watermarks_table)
                .where(
                    projection_watermarks_table.c.projection == projection,
                    projection_watermarks_table.c.stream_id == stream_id,
                    projection_watermarks_table.c.last_seq < seq,
                )
                .values(last_seq=seq)
            )
            if updated.rowcount == 1:
                return seq

            current = connection.execute(
                select(projection_watermarks_table.c.last_seq).where(
                    projection_watermarks_table.c.projection == projection,
                    projection_watermarks_table.c.stream_id == stream_id,
                )
            ).scalar_one_or_none()
            if current is not None:
                return int(current)

            connection.execute(
                insert(projection_watermarks_table).values(
                    projection=projection,
                    stream_id=stream_id,
                    last_seq=seq,
                )
            )
            return seq

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        if isinstance(error, IntegrityError):
            return True
        if isinstance(error, OperationalError):
            message = str(error).lower()
            return any(
                marker in message
                for marker in (
                    "database is locked",
                    "deadlock",
                    "serialization failure",
                    "could not serialize",
                )
            )
        return False
