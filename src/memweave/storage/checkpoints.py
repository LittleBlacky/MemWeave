"""Relational persistence for projection processing watermarks."""

from sqlalchemy import insert, select, update

from .ports import ProjectionCheckpointStore
from .schema import projection_watermarks_table


class RelationalProjectionCheckpointStore(ProjectionCheckpointStore):
    def __init__(self, database):
        self.database = database

    def get(self, projection: str, stream_id: str) -> int:
        with self.database.read() as connection:
            value = connection.execute(
                select(projection_watermarks_table.c.last_seq).where(
                    projection_watermarks_table.c.projection == projection,
                    projection_watermarks_table.c.stream_id == stream_id,
                )
            ).scalar_one_or_none()
        return int(value or 0)

    def save_max(self, projection: str, stream_id: str, seq: int) -> int:
        if seq < 0:
            raise ValueError("seq must not be negative")
        with self.database.begin() as connection:
            current = connection.execute(
                select(projection_watermarks_table.c.last_seq).where(
                    projection_watermarks_table.c.projection == projection,
                    projection_watermarks_table.c.stream_id == stream_id,
                )
            ).scalar_one_or_none()
            if current is None:
                connection.execute(
                    insert(projection_watermarks_table).values(
                        projection=projection,
                        stream_id=stream_id,
                        last_seq=seq,
                    )
                )
                return seq
            if seq > current:
                connection.execute(
                    update(projection_watermarks_table)
                    .where(
                        projection_watermarks_table.c.projection == projection,
                        projection_watermarks_table.c.stream_id == stream_id,
                    )
                    .values(last_seq=seq)
                )
                return seq
            return int(current)
