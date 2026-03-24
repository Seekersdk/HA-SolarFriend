"""SolarFriend consumption profile - learns hourly load patterns over time."""
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
    """Return values strictly below the given percentile threshold."""
    if len(values) < 4:
        return values
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * percentile / 100)
    threshold = sorted_vals[min(idx, len(sorted_vals) - 1)]
    filtered = [v for v in values if v < threshold]
    return filtered if filtered else values


def _empty_profile() -> dict[int, dict[str, float]]:
    """Return a fresh 24-hour profile with zero samples."""
    return {hour: {"samples": 0.0, "avg_watt": 0.0} for hour in range(24)}


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
            "ConsumptionProfile loaded - days_collected=%d confidence=%s",
            self.days_collected,
            self.confidence,
        )

    async def async_save(self, hass: HomeAssistant) -> None:
        """Persist profiles to HA storage."""
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
        """Return net household consumption by subtracting EV and grid charging."""
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
                "ConsumptionProfile: skip %.0fW (household after subtraction still > 10kW)",
                load_watt,
            )
            return

        _LOGGER.debug(
            "ConsumptionProfile update: load=%.0fW ev=%.0fW bat=%.0fW -> household=%.0fW",
            load_watt,
            ev_power_w,
            battery_power_w,
            clean,
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
            "ConsumptionProfile: %s hour=%02d -> avg=%.1f (n=%d)",
            profile_key,
            hour,
            slot["avg_watt"],
            slot["samples"],
        )

        await self.async_save(hass)

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    async def bootstrap_from_history(
        self,
        hass: HomeAssistant,
        entity_id: str,
        days: int = 14,
        *,
        force: bool = False,
    ) -> int:
        """Build or refresh the profile from HA recorder history."""
        days = max(1, min(int(days), 14))

        if not force and self.days_collected >= 3:
            return 0

        from collections import defaultdict
        from datetime import timedelta

        import homeassistant.util.dt as ha_dt
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.history import get_significant_states

        end_time = ha_dt.now()
        start_time = end_time - timedelta(days=days)

        try:
            recorder = get_instance(hass)
            states = await recorder.async_add_executor_job(
                get_significant_states,
                hass,
                start_time,
                end_time,
                [entity_id],
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
            if force or slot["samples"] < 5:
                slot["avg_watt"] = round(avg_watt, 1)
                # Make bootstrapped buckets immediately usable after restart.
                slot["samples"] = max(float(slot["samples"]), 3.0)
                bootstrapped += 1
                _LOGGER.debug(
                    "Bootstrap %s h%02d: %d readings -> %d after filter -> %.0fW",
                    day_type,
                    hour,
                    len(values),
                    len(filtered),
                    avg_watt,
                )

        if bootstrapped > 0:
            await self.async_save(hass)
            _LOGGER.info(
                "Bootstrap: %d buckets from %d days of history%s",
                bootstrapped,
                days,
                " (forced)" if force else "",
            )

        return bootstrapped

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        """Return predicted load for a given hour and day-type."""
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
        """Estimate learning maturity without letting sparse buckets reset progress."""
        day_estimates: list[int] = []
        for profile in self._profiles.values():
            populated = [slot["samples"] for slot in profile.values() if slot["samples"] > 0]
            if len(populated) < 6:
                day_estimates.append(0)
                continue
            populated.sort()
            median_samples = populated[len(populated) // 2]
            day_estimates.append(int(median_samples // 4))
        return max(day_estimates, default=0)

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
