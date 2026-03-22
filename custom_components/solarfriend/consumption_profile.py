"""SolarFriend consumption profile — learns hourly load patterns over time."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

STORAGE_KEY = "solarfriend_profile"
STORAGE_VERSION = 1
DEFAULT_WATT = 500.0

# Confidence thresholds (days)
_CONFIDENCE_LOW = 3
_CONFIDENCE_MEDIUM = 7
_CONFIDENCE_HIGH = 14


def _empty_profile() -> dict[int, dict[str, float]]:
    """Return a fresh 24-hour profile with zero samples."""
    return {hour: {"samples": 0, "avg_watt": 0.0} for hour in range(24)}


class ConsumptionProfile:
    """Tracks and predicts hourly household power consumption."""

    def __init__(self) -> None:
        self._profiles: dict[str, dict[int, dict[str, float]]] = {
            "weekday": _empty_profile(),
            "weekend": _empty_profile(),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _store(self, hass: HomeAssistant) -> Store:
        return Store(hass, STORAGE_VERSION, STORAGE_KEY)

    async def async_load(self, hass: HomeAssistant) -> None:
        """Load profiles from HA storage. Keeps defaults if no data found."""
        data: dict[str, Any] | None = await self._store(hass).async_load()
        if not data:
            _LOGGER.debug("ConsumptionProfile: no stored data, starting fresh")
            return

        for profile_key in ("weekday", "weekend"):
            stored = data.get(profile_key, {})
            for hour in range(24):
                entry = stored.get(str(hour))
                if entry:
                    self._profiles[profile_key][hour]["samples"] = float(entry.get("samples", 0))
                    self._profiles[profile_key][hour]["avg_watt"] = float(entry.get("avg_watt", 0.0))

        _LOGGER.debug(
            "ConsumptionProfile loaded — days_collected=%d confidence=%s",
            self.days_collected,
            self.confidence,
        )

    async def async_save(self, hass: HomeAssistant) -> None:
        """Persist profiles to HA storage."""
        # Store hour keys as strings (JSON requirement)
        serialisable = {
            profile_key: {
                str(hour): slot
                for hour, slot in profile.items()
            }
            for profile_key, profile in self._profiles.items()
        }
        await self._store(hass).async_save(serialisable)

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    async def async_update(self, hass: HomeAssistant, load_watt: float) -> None:
        """Record a load sample for the current hour and day-type, then save."""
        now = datetime.now()
        hour = now.hour
        profile_key = "weekend" if now.weekday() >= 5 else "weekday"

        slot = self._profiles[profile_key][hour]
        samples = slot["samples"]
        old_avg = slot["avg_watt"]

        slot["avg_watt"] = (old_avg * samples + load_watt) / (samples + 1)
        slot["samples"] = samples + 1

        _LOGGER.debug(
            "ConsumptionProfile update: %s hour=%02d watt=%.1f → avg=%.1f (n=%d)",
            profile_key, hour, load_watt, slot["avg_watt"], slot["samples"],
        )

        await self.async_save(hass)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        """Return predicted load for a given hour and day-type.

        Falls back to DEFAULT_WATT if fewer than 3 samples have been collected.
        """
        profile_key = "weekend" if is_weekend else "weekday"
        slot = self._profiles[profile_key][hour]
        if slot["samples"] < 3:
            return DEFAULT_WATT
        return slot["avg_watt"]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def days_collected(self) -> int:
        """Estimate collected days from the minimum sample count across all hours.

        Each hour accumulates ~4 samples per day (called every 15 min),
        so days ≈ min_samples / 4.
        """
        all_samples = [
            slot["samples"]
            for profile in self._profiles.values()
            for slot in profile.values()
        ]
        min_samples = min(all_samples) if all_samples else 0
        return int(min_samples // 4)

    @property
    def confidence(self) -> str:
        days = self.days_collected
        if days < _CONFIDENCE_LOW:
            return "LEARNING"
        if days < _CONFIDENCE_MEDIUM:
            return "LOW"
        if days < _CONFIDENCE_HIGH:
            return "MEDIUM"
        return "HIGH"
