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


def _percentile_filter(values: list[float], percentile: float = 85) -> list[float]:
    """Return values strictly below the given percentile threshold.

    Removes EV/battery-charging spikes regardless of their absolute level.
    Falls back to the original list when fewer than 4 values are present or
    when the filter would eliminate everything.
    """
    if len(values) < 4:
        return values
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * percentile / 100)
    threshold = sorted_vals[min(idx, len(sorted_vals) - 1)]
    filtered = [v for v in values if v < threshold]
    return filtered if filtered else values


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

    @staticmethod
    def _clean_load_w(
        load_w: float,
        ev_power_w: float = 0.0,
        battery_power_w: float = 0.0,
    ) -> float | None:
        """Return net household consumption by subtracting EV and grid charging.

        battery_power_w sign convention: positive = discharging, negative = charging.
        Only grid-charging (negative battery_power_w) is subtracted; discharging
        simply means the battery is covering house load and is not an extra load.

        Returns None when the result still exceeds 10 kW — something unknown
        is drawing power and the sample should be skipped.
        """
        battery_grid_charge_w = max(0.0, -battery_power_w)
        household_w = load_w - ev_power_w - battery_grid_charge_w
        if household_w < 0:
            household_w = 0.0
        if household_w > 10_000:
            return None
        return household_w

    async def async_update(
        self,
        hass: HomeAssistant,
        load_watt: float,
        ev_power_w: float = 0.0,
        battery_power_w: float = 0.0,
    ) -> None:
        """Record a cleaned load sample for the current hour and day-type, then save."""
        clean = self._clean_load_w(load_watt, ev_power_w, battery_power_w)
        if clean is None:
            _LOGGER.debug(
                "ConsumptionProfile: skip %.0fW "
                "(household efter fradrag stadig > 10kW)",
                load_watt,
            )
            return

        _LOGGER.debug(
            "ConsumptionProfile update: load=%.0fW ev=%.0fW bat=%.0fW → household=%.0fW",
            load_watt, ev_power_w, battery_power_w, clean,
        )

        now = datetime.now()
        hour = now.hour
        profile_key = "weekend" if now.weekday() >= 5 else "weekday"

        slot = self._profiles[profile_key][hour]
        samples = slot["samples"]
        old_avg = slot["avg_watt"]

        slot["avg_watt"] = (old_avg * samples + clean) / (samples + 1)
        slot["samples"] = samples + 1

        _LOGGER.debug(
            "ConsumptionProfile: %s hour=%02d → avg=%.1f (n=%d)",
            profile_key, hour, slot["avg_watt"], slot["samples"],
        )

        await self.async_save(hass)

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    async def bootstrap_from_history(
        self, hass: HomeAssistant, entity_id: str, days: int = 14
    ) -> int:
        """Build a starter profile from HA recorder history.

        Groups measurements into (weekday/weekend, hour) buckets, filters
        out EV/battery-charging spikes via the 85th-percentile cutoff, then
        writes the per-bucket average with samples=1 so live data takes over
        gradually via the rolling average.

        Returns the number of hour-buckets written (0 on failure or skip).
        """
        if self.days_collected >= 3:
            return 0

        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.history import get_significant_states
        import homeassistant.util.dt as ha_dt
        from datetime import timedelta
        from collections import defaultdict

        end_time = ha_dt.now()
        start_time = end_time - timedelta(days=days)

        try:
            recorder = get_instance(hass)
            states = await recorder.async_add_executor_job(
                get_significant_states,
                hass, start_time, end_time, [entity_id],
            )
        except Exception as exc:
            _LOGGER.warning("Bootstrap: historik fejl: %s", exc)
            return 0

        entity_states = states.get(entity_id, [])
        if not entity_states:
            _LOGGER.warning("Bootstrap: ingen historik for %s", entity_id)
            return 0

        buckets: dict[tuple[str, int], list[float]] = defaultdict(list)
        for state in entity_states:
            if state.state in ("unknown", "unavailable"):
                continue
            try:
                watt = float(state.state)
                if watt <= 0:
                    continue
                dt = ha_dt.as_local(state.last_changed)
                day_type = "weekday" if dt.weekday() < 5 else "weekend"
                buckets[(day_type, dt.hour)].append(watt)
            except (ValueError, TypeError):
                continue

        if not buckets:
            return 0

        bootstrapped = 0
        for (day_type, hour), values in buckets.items():
            filtered = _percentile_filter(values, percentile=85)
            avg_watt = sum(filtered) / len(filtered)
            slot = self._profiles[day_type][hour]
            if slot["samples"] < 5:
                slot["avg_watt"] = round(avg_watt, 1)
                slot["samples"] = 1
                bootstrapped += 1
                _LOGGER.debug(
                    "Bootstrap %s h%02d: %d målinger → %d efter filter → %.0fW",
                    day_type, hour, len(values), len(filtered), avg_watt,
                )

        if bootstrapped > 0:
            await self.async_save(hass)
            _LOGGER.info(
                "Bootstrap: %d buckets fra %d dages historik "
                "(EV-toppe filtreret via 85. percentil)",
                bootstrapped, days,
            )

        return bootstrapped

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
