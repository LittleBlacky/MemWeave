"""Injectable UTC clocks used by projections and deterministic tests."""

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Clock:
    def now(self) -> datetime:
        return utc_now()
