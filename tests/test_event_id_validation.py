from uuid import uuid4

import pytest

from memweave.db import Database
from memweave.events import EventStore
from memweave.models import EventType


def test_append_rejects_non_uuid_event_id_before_persisting(tmp_path):
    store = EventStore(Database(str(tmp_path / "event-id-validation.db")))

    with pytest.raises(TypeError, match="event_id must be a UUID"):
        store.append(
            "session:s1",
            EventType.USER_MESSAGE,
            {},
            "user:u1",
            request_id=uuid4(),
            event_id=str(uuid4()),
        )

    assert store.last_seq("session:s1") == 0
