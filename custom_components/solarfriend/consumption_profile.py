"""SolarFriend consumption profile - learns hourly load patterns over time."""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as ha_dt

_LOGGER = logging.getLogger(__name__)

STORAGE_KEY = "solarfriend_profile"
STORAGE_VERSION = 1
DEFAULT_WATT = 500.0

# Confidence thresholds (days)
_CONFIDENCE_LOW = 3
_CONFIDENCE_MEDIUM = 7
_CONFIDENCE_HIGH = 14
_SEED_MATCH_ABS_W = 50.0
_SEED_MATCH_REL = 0.05
_SEED_HISTORY_WEIGHT = 3.0
_SEED_MIN_DIRECT_SAMPLES = 3.0
_PROFILE_FALLBACK_MIN_SAMPLES = 3.0
_PROFILE_FALLBACK_MIN_DAYS = 2


def _to_float(value: Any) -> float | None:
    """Return float(value) or None on invalid input."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sensor_seed_mode(unit: str | None, state_class: str | None) -> str:
    """Classify the source sensor for seeding."""
    unit_norm = (unit or "").lower()
    state_class_norm = (state_class or "").lower()

    if unit_norm in {"w", "kw"}:
        return "power"
    if unit_norm in {"wh", "kwh"} and state_class_norm in {"total", "total_increasing"}:
        return "energy"
    if unit_norm in {"wh", "kwh"}:
        return "energy"
    return "power"


def _power_to_watt(value: float, unit: str | None) -> float:
    """Normalize a power value to watts."""
    return value * 1000.0 if (unit or "").lower() == "kw" else value


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

    @staticmethod
    def _bucket_key(dt: datetime) -> tuple[str, int]:
        """Map a timestamp to profile bucket."""
        return ("weekday" if dt.weekday() < 5 else "weekend", dt.hour)

    def _profile_days_estimate(self, profile_key: str) -> int:
        """Estimate maturity for a single day-type profile."""
        profile = self._profiles[profile_key]
        populated = [float(slot["samples"]) for slot in profile.values() if slot["samples"] > 0]
        if len(populated) < 6:
            return 0
        populated.sort()
        median_samples = populated[len(populated) // 2]
        return int(median_samples // 4)

    def _resolve_slot(self, hour: int, is_weekend: bool) -> tuple[str, dict[str, float]]:
        """Return the best available slot, falling back to the opposite day type if needed."""
        profile_key = "weekend" if is_weekend else "weekday"
        slot = self._profiles[profile_key][hour]
        if (
            self._profile_days_estimate(profile_key) >= _PROFILE_FALLBACK_MIN_DAYS
            and slot["samples"] >= _PROFILE_FALLBACK_MIN_SAMPLES
        ):
            return profile_key, slot

        fallback_key = "weekday" if is_weekend else "weekend"
        fallback_slot = self._profiles[fallback_key][hour]
        if (
            self._profile_days_estimate(fallback_key) >= _PROFILE_FALLBACK_MIN_DAYS
            and fallback_slot["samples"] >= _PROFILE_FALLBACK_MIN_SAMPLES
        ):
            return fallback_key, fallback_slot

        return profile_key, slot

    @staticmethod
    def _distribute_energy_to_buckets(
        start: datetime,
        end: datetime,
        energy_kwh: float,
        buckets: dict[tuple[str, int], dict[str, float]],
    ) -> None:
        """Distribute interval energy proportionally across hour buckets."""
        if end <= start or energy_kwh <= 0:
            return

        total_seconds = (end - start).total_seconds()
        cursor = start
        while cursor < end:
            next_hour = (cursor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            segment_end = min(end, next_hour)
            segment_seconds = (segment_end - cursor).total_seconds()
            fraction = segment_seconds / total_seconds if total_seconds > 0 else 0.0
            bucket = buckets[ConsumptionProfile._bucket_key(cursor)]
            bucket["energy_kwh"] += energy_kwh * fraction
            bucket["duration_h"] += segment_seconds / 3600.0
            cursor = segment_end

    def _seed_bucket(
        self,
        *,
        day_type: str,
        hour: int,
        seeded_avg_watt: float,
        force: bool,
        history_weight: float = _SEED_HISTORY_WEIGHT,
    ) -> bool:
        """Apply seeded data while prioritising existing live samples."""
        slot = self._profiles[day_type][hour]
        current_samples = float(slot["samples"])
        current_avg = float(slot["avg_watt"])

        if current_samples <= 0:
            slot["avg_watt"] = round(seeded_avg_watt, 1)
            slot["samples"] = max(_SEED_MIN_DIRECT_SAMPLES, history_weight)
            return True

        difference = abs(current_avg - seeded_avg_watt)
        if difference <= max(_SEED_MATCH_ABS_W, abs(current_avg) * _SEED_MATCH_REL):
            return False

        if not force and current_samples >= 5:
            return False

        blend_weight = min(history_weight, max(1.0, current_samples / 4.0))
        blended_avg = (
            (current_avg * current_samples) + (seeded_avg_watt * blend_weight)
        ) / (current_samples + blend_weight)
        slot["avg_watt"] = round(blended_avg, 1)
        # Keep live maturity intact; seeded history should not masquerade as live samples.
        slot["samples"] = current_samples
        return True

    def _seed_from_power_history(
        self,
        states: list[Any],
        end_time: datetime,
        *,
        force: bool,
        unit: str | None,
    ) -> int:
        """Seed from power measurements by integrating W over time."""
        bucket_energy: dict[tuple[str, int], dict[str, float]] = defaultdict(
            lambda: {"energy_kwh": 0.0, "duration_h": 0.0}
        )

        valid_points: list[tuple[datetime, float]] = []
        for state in states:
            if state.state in ("unknown", "unavailable"):
                continue
            raw_value = _to_float(state.state)
            watt = _power_to_watt(raw_value, unit) if raw_value is not None else None
            if watt is None or watt <= 0:
                continue
            valid_points.append((state.last_changed, watt))

        if not valid_points:
            return 0
        if len({point[0] for point in valid_points}) < 2:
            return self._seed_from_point_history(states, force=force, unit=unit)

        valid_points.sort(key=lambda item: item[0])
        for index, (start_dt, watt) in enumerate(valid_points):
            if index + 1 < len(valid_points):
                end_dt = valid_points[index + 1][0]
            else:
                next_hour = start_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                end_dt = min(end_time, next_hour)
            if end_dt <= start_dt:
                continue
            energy_kwh = (watt * (end_dt - start_dt).total_seconds()) / 3_600_000.0
            self._distribute_energy_to_buckets(start_dt, end_dt, energy_kwh, bucket_energy)

        bootstrapped = 0
        for (day_type, hour), bucket in bucket_energy.items():
            duration_h = bucket["duration_h"]
            if duration_h <= 0:
                continue
            avg_watt = (bucket["energy_kwh"] / duration_h) * 1000.0
            if self._seed_bucket(day_type=day_type, hour=hour, seeded_avg_watt=avg_watt, force=force):
                bootstrapped += 1
        return bootstrapped

    def _seed_from_energy_history(
        self,
        states: list[Any],
        end_time: datetime,
        *,
        force: bool,
        unit: str | None,
    ) -> int:
        """Seed from cumulative energy history by distributing deltas over time."""
        unit_norm = (unit or "").lower()
        factor_to_kwh = 1.0 if unit_norm == "kwh" else 0.001
        bucket_energy: dict[tuple[str, int], dict[str, float]] = defaultdict(
            lambda: {"energy_kwh": 0.0, "duration_h": 0.0}
        )

        valid_points: list[tuple[datetime, float]] = []
        for state in states:
            if state.state in ("unknown", "unavailable"):
                continue
            value = _to_float(state.state)
            if value is None:
                continue
            valid_points.append((state.last_changed, value * factor_to_kwh))

        if len(valid_points) < 2:
            return 0

        valid_points.sort(key=lambda item: item[0])
        for index in range(len(valid_points) - 1):
            start_dt, start_kwh = valid_points[index]
            end_dt, end_kwh = valid_points[index + 1]
            if end_dt <= start_dt:
                continue
            delta_kwh = end_kwh - start_kwh
            if delta_kwh <= 0:
                continue
            self._distribute_energy_to_buckets(start_dt, end_dt, delta_kwh, bucket_energy)

        bootstrapped = 0
        for (day_type, hour), bucket in bucket_energy.items():
            duration_h = bucket["duration_h"]
            if duration_h <= 0:
                continue
            avg_watt = (bucket["energy_kwh"] / duration_h) * 1000.0
            if self._seed_bucket(day_type=day_type, hour=hour, seeded_avg_watt=avg_watt, force=force):
                bootstrapped += 1
        return bootstrapped

    def _seed_from_point_history(
        self,
        states: list[Any],
        *,
        force: bool,
        unit: str | None = None,
    ) -> int:
        """Fallback seed from point-in-time averages."""
        buckets: dict[tuple[str, int], list[float]] = defaultdict(list)
        for state in states:
            if state.state in ("unknown", "unavailable"):
                continue
            raw_value = _to_float(state.state)
            watt = _power_to_watt(raw_value, unit) if raw_value is not None else None
            if watt is None or watt <= 0:
                continue
            buckets[self._bucket_key(state.last_changed)].append(watt)

        bootstrapped = 0
        for (day_type, hour), values in buckets.items():
            filtered = _percentile_filter(values, percentile=85)
            avg_watt = sum(filtered) / len(filtered)
            if self._seed_bucket(day_type=day_type, hour=hour, seeded_avg_watt=avg_watt, force=force):
                bootstrapped += 1
        return bootstrapped

    async def async_load(self, hass: HomeAssistant) -> None:
        """Load profiles from HA storage. Keeps defaults if no data found."""
        try:
            data: dict[str, Any] | None = await self._store(hass).async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "ConsumptionProfile storage load failed for %s; starting fresh: %s",
                STORAGE_KEY,
                exc,
            )
            return
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
        """Return household consumption with EV load removed."""
        household_w = load_w - ev_power_w
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

        now = ha_dt.now()
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

        import homeassistant.util.dt as ha_dt
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.history import get_significant_states

        state_obj = hass.states.get(entity_id) if getattr(hass, "states", None) else None
        unit = state_obj.attributes.get("unit_of_measurement") if state_obj is not None else None
        state_class = state_obj.attributes.get("state_class") if state_obj is not None else None
        seed_mode = _sensor_seed_mode(unit, state_class)

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

        localized_states = []
        for state in entity_states:
            dt = ha_dt.as_local(state.last_changed)
            localized_states.append(type("HistoryPoint", (), {"state": state.state, "last_changed": dt}))

        if seed_mode == "energy":
            bootstrapped = self._seed_from_energy_history(
                localized_states,
                end_time=ha_dt.as_local(end_time),
                force=force,
                unit=unit,
            )
        elif seed_mode == "power":
            bootstrapped = self._seed_from_power_history(
                localized_states,
                end_time=ha_dt.as_local(end_time),
                force=force,
                unit=unit,
            )
        else:
            bootstrapped = self._seed_from_point_history(localized_states, force=force, unit=unit)

        if bootstrapped > 0:
            await self.async_save(hass)
            _LOGGER.info(
                "Bootstrap: %d buckets from %d days of %s history%s",
                bootstrapped,
                days,
                seed_mode,
                " (forced)" if force else "",
            )

        return bootstrapped

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        """Return predicted load for a given hour and day-type."""
        _, slot = self._resolve_slot(hour, is_weekend)
        if slot["samples"] < 3:
            return DEFAULT_WATT
        return slot["avg_watt"]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def days_collected(self) -> int:
        """Estimate learning maturity without letting sparse buckets reset progress."""
        return max((self._profile_days_estimate(profile_key) for profile_key in self._profiles), default=0)

    def build_debug_snapshot(self) -> dict[str, Any]:
        """Expose raw learning metrics behind days_collected for diagnostics."""
        snapshot: dict[str, Any] = {}
        for profile_key, profile in self._profiles.items():
            populated = [float(slot["samples"]) for slot in profile.values() if slot["samples"] > 0]
            populated.sort()
            median_samples = populated[len(populated) // 2] if populated else 0.0
            snapshot[profile_key] = {
                "populated_hours": len(populated),
                "median_samples": median_samples,
                "days_estimate": self._profile_days_estimate(profile_key),
                "fallback_hours": [
                    hour
                    for hour in range(24)
                    if self._resolve_slot(hour, profile_key == "weekend")[0] != profile_key
                ],
                "samples_per_hour": {
                    str(hour): float(profile[hour]["samples"])
                    for hour in range(24)
                    if profile[hour]["samples"] > 0
                },
            }
        return snapshot

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
