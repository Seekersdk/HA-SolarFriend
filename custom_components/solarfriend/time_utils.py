"""Shared datetime normalization helpers."""
from __future__ import annotations

from datetime import datetime, timezone

from homeassistant.util import dt as ha_dt


def normalize_local_datetime(value: datetime) -> datetime:
    """Return a timezone-aware local datetime for safe comparisons."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return ha_dt.as_local(value)
