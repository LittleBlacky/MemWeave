"""Stable identities for events applied by generic projections."""

import hashlib
import json

from ..models import Event


def event_fingerprint(event: Event) -> str:
    """Return the immutable identity used for projection duplicate checks."""

    values = event.model_dump(mode="json", exclude={"ingested_at"})
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
