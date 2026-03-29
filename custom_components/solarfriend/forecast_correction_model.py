"""Passive forecast correction model with season/elevation/azimuth buckets."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as ha_dt

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 2
STORAGE_KEY = "solarfriend_forecast_correction"
_MIN_VALID_KWH = 0.15
_MIN_EARLY_SAMPLES = 5
_MIN_CONFIDENT_SAMPLES = 10
_MAX_FACTOR_DELTA = 0.35
_MIN_FACTOR = 0.5
_MAX_FACTOR = 1.5
_SUN_BUFFER = timedelta(minutes=30)
_MAX_GEOMETRY_BUCKETS = 10_000
_MAX_TEMPERATURE_BUCKETS = 128
_MIN_TEMP_FACTOR = 0.8
_MAX_TEMP_FACTOR = 1.2


@dataclass
class GeometryBucket:
    """Learned correction state for one season/elevation/azimuth bucket."""

    factor: float = 1.0
    samples: int = 0
    avg_abs_error_kwh: float = 0.0


@dataclass
class TemperatureBucket:
    """Secondary residual correction for one season/temperature bucket."""

    factor: float = 1.0
    samples: int = 0
    avg_abs_error_kwh: float = 0.0


@dataclass
class CorrectionModelSnapshot:
    """Compact diagnostics snapshot for sensor exposure."""

    state: str = "inactive"
    current_season: int = 0
    active_buckets: int = 0
    confident_buckets: int = 0
    average_factor_this_season: float = 1.0
    today_geometry_factors: dict[str, dict[str, float]] = field(default_factory=dict)
    current_total_factor: float = 1.0
    current_geometry_factor: float = 1.0
    current_geometry_samples: int = 0
    current_geometry_key: str = ""
    current_temperature_factor: float = 1.0
    current_temperature_samples: int = 0
    current_temperature_key: str = ""
    raw_vs_corrected_delta_today: float = 0.0
    last_environment: dict[str, Any] = field(default_factory=dict)


class ForecastCorrectionModel:
    """Build a passive season/elevation/azimuth correction model."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._hass = hass
        self._legacy_entry_id = entry_id
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._geometry_buckets: dict[str, GeometryBucket] = {}
        self._temperature_buckets: dict[str, TemperatureBucket] = {}
        self._today_date: str = ""
        self._today_actual_kwh_by_hour: dict[int, float] = {}
        self._today_raw_forecast_kwh_by_hour: dict[int, float] = {}
        self._today_context_by_hour: dict[int, dict[str, Any]] = {}
        self._finalized_hours: set[int] = set()
        self._today_sunrise: datetime | None = None
        self._today_sunset: datetime | None = None

    async def async_load(self) -> None:
        """Load persisted buckets and current-day partial data."""
        data = await self._async_safe_load(self._store, STORAGE_KEY)
        if not data and self._legacy_entry_id:
            legacy_store = Store(
                self._hass,
                STORAGE_VERSION,
                f"{STORAGE_KEY}_{self._legacy_entry_id}",
            )
            data = await self._async_safe_load(
                legacy_store,
                f"{STORAGE_KEY}_{self._legacy_entry_id}",
            )
            if data:
                _LOGGER.info(
                    "ForecastCorrectionModel migrated legacy storage for entry_id=%s to stable key",
                    self._legacy_entry_id,
                )
                await self._store.async_save(data)
        if not data:
            return

        raw_geometry = data.get("geometry_buckets", {}) or {}
        raw_context = data.get("context_buckets", {}) or {}
        if raw_geometry:
            self._geometry_buckets = {
                str(key): GeometryBucket(
                    factor=float(bucket.get("factor", 1.0)),
                    samples=int(bucket.get("samples", 0)),
                    avg_abs_error_kwh=float(bucket.get("avg_abs_error_kwh", 0.0)),
                )
                for key, bucket in raw_geometry.items()
            }
        raw_temperature = data.get("temperature_buckets", {}) or {}
        if raw_temperature:
            self._temperature_buckets = {
                str(key): TemperatureBucket(
                    factor=float(bucket.get("factor", 1.0)),
                    samples=int(bucket.get("samples", 0)),
                    avg_abs_error_kwh=float(bucket.get("avg_abs_error_kwh", 0.0)),
                )
                for key, bucket in raw_temperature.items()
            }
        elif raw_context:
            self._geometry_buckets = self._migrate_legacy_context_buckets(raw_context)
            if self._geometry_buckets:
                _LOGGER.info(
                    "ForecastCorrectionModel: migrated %d legacy context buckets to season/elevation/azimuth",
                    len(self._geometry_buckets),
                )
        elif data.get("buckets"):
            _LOGGER.info(
                "ForecastCorrectionModel: ignoring legacy month/hour buckets after geometry migration"
            )

        self._today_date = str(data.get("today_date", ""))
        self._today_actual_kwh_by_hour = {
            int(hour): float(value)
            for hour, value in (data.get("today_actual_kwh_by_hour", {}) or {}).items()
        }
        self._today_raw_forecast_kwh_by_hour = {
            int(hour): float(value)
            for hour, value in (data.get("today_raw_forecast_kwh_by_hour", {}) or {}).items()
        }
        self._today_context_by_hour = {
            int(hour): dict(value)
            for hour, value in (data.get("today_context_by_hour", {}) or {}).items()
        }
        self._finalized_hours = {
            int(hour)
            for hour in (data.get("finalized_hours", []) or [])
        }
        self._today_sunrise = self._parse_datetime(data.get("today_sunrise"))
        self._today_sunset = self._parse_datetime(data.get("today_sunset"))

    @staticmethod
    def _migrate_legacy_context_buckets(raw_context: dict[str, Any]) -> dict[str, GeometryBucket]:
        aggregated: dict[str, GeometryBucket] = {}
        for key, bucket in raw_context.items():
            geometry_key = ForecastCorrectionModel._geometry_key_from_legacy_key(key)
            if not geometry_key:
                continue
            migrated_bucket = GeometryBucket(
                factor=float(bucket.get("factor", 1.0)),
                samples=int(bucket.get("samples", 0)),
                avg_abs_error_kwh=float(bucket.get("avg_abs_error_kwh", 0.0)),
            )
            if migrated_bucket.samples <= 0:
                continue
            existing = aggregated.get(geometry_key)
            if existing is None:
                aggregated[geometry_key] = migrated_bucket
                continue
            total_samples = existing.samples + migrated_bucket.samples
            existing.factor = (
                existing.factor * existing.samples
                + migrated_bucket.factor * migrated_bucket.samples
            ) / total_samples
            existing.avg_abs_error_kwh = (
                existing.avg_abs_error_kwh * existing.samples
                + migrated_bucket.avg_abs_error_kwh * migrated_bucket.samples
            ) / total_samples
            existing.samples = total_samples
        return aggregated

    @staticmethod
    def _geometry_key_from_legacy_key(key: Any) -> str:
        parts = str(key).split("|")
        season = next((part[1:] for part in parts if part.startswith("s")), None)
        elevation = next((part[1:] for part in parts if part.startswith("e")), None)
        azimuth = next((part[1:] for part in parts if part.startswith("a")), None)
        if None in (season, elevation, azimuth):
            return ""
        return f"s{season}|e{elevation}|a{azimuth}"

    async def _async_safe_load(
        self,
        store: Store,
        storage_key: str,
    ) -> dict[str, Any] | None:
        """Load persisted state without aborting startup on corruption."""
        try:
            data = await store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "ForecastCorrectionModel storage load failed for %s; starting fresh: %s",
                storage_key,
                exc,
            )
            return None
        return data if isinstance(data, dict) else None

    async def async_save(self) -> None:
        """Persist geometry buckets and current-day partial state."""
        await self._store.async_save(
            {
                "geometry_buckets": {
                    key: {
                        "factor": round(bucket.factor, 4),
                        "samples": bucket.samples,
                        "avg_abs_error_kwh": round(bucket.avg_abs_error_kwh, 4),
                    }
                    for key, bucket in self._geometry_buckets.items()
                },
                "buckets": {},
                "context_buckets": {},
                "temperature_buckets": {
                    key: {
                        "factor": round(bucket.factor, 4),
                        "samples": bucket.samples,
                        "avg_abs_error_kwh": round(bucket.avg_abs_error_kwh, 4),
                    }
                    for key, bucket in self._temperature_buckets.items()
                },
                "today_date": self._today_date,
                "today_actual_kwh_by_hour": {
                    str(hour): round(value, 6)
                    for hour, value in self._today_actual_kwh_by_hour.items()
                },
                "today_raw_forecast_kwh_by_hour": {
                    str(hour): round(value, 6)
                    for hour, value in self._today_raw_forecast_kwh_by_hour.items()
                },
                "today_context_by_hour": {
                    str(hour): context
                    for hour, context in self._today_context_by_hour.items()
                },
                "finalized_hours": sorted(self._finalized_hours),
                "today_sunrise": self._today_sunrise.isoformat() if self._today_sunrise else None,
                "today_sunset": self._today_sunset.isoformat() if self._today_sunset else None,
            }
        )

    def update(
        self,
        *,
        now: datetime,
        pv_power_w: float,
        dt_seconds: float,
        hourly_forecast: list[dict[str, Any]] | None,
        sunrise: datetime | None,
        sunset: datetime | None,
        weather_snapshot: dict[str, Any] | None,
        solar_elevation: float | None,
        solar_azimuth: float | None,
    ) -> None:
        """Update current-day actuals and finalize completed buckets when possible."""
        self._rollover_if_needed(now.date(), sunrise, sunset)

        self._today_date = now.date().isoformat()
        self._today_sunrise = sunrise
        self._today_sunset = sunset
        self._update_forecast_map(now.date(), hourly_forecast)

        if dt_seconds > 0 and pv_power_w > 0:
            self._today_actual_kwh_by_hour[now.hour] = self._today_actual_kwh_by_hour.get(now.hour, 0.0) + (
                pv_power_w * dt_seconds / 3_600_000
            )
        context = self._build_hour_context(
            now=now,
            weather_snapshot=weather_snapshot or {},
            solar_elevation=solar_elevation,
            solar_azimuth=solar_azimuth,
        )
        if context:
            self._today_context_by_hour[now.hour] = context

        self._finalize_completed_hours(now, sunrise, sunset)

    def get_corrected_hourly_forecast(
        self,
        *,
        now: datetime,
        hourly_forecast: list[dict[str, Any]] | None,
        latitude: float | None = None,
        longitude: float | None = None,
        hourly_weather_forecast: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Return hourly forecast slots with Track-1 geometry correction applied."""
        del now
        if not hourly_forecast:
            return []
        active = any(bucket.samples >= _MIN_EARLY_SAMPLES for bucket in self._geometry_buckets.values())
        if not active:
            return list(hourly_forecast)

        result: list[dict[str, Any]] = []
        for slot in hourly_forecast:
            ps = slot.get("period_start")
            if ps is None:
                result.append(slot)
                continue
            try:
                if isinstance(ps, str):
                    ps = datetime.fromisoformat(ps)
                local_ps = ha_dt.as_local(ps) if ps.tzinfo is not None else ps
            except (TypeError, ValueError, AttributeError):
                result.append(slot)
                continue

            geometry_key = self._geometry_key_for_forecast_slot(
                slot_time=local_ps,
                latitude=latitude,
                longitude=longitude,
            )
            temperature_key = self._temperature_key_for_forecast_slot(
                slot_time=local_ps,
                hourly_weather_forecast=hourly_weather_forecast,
            )
            factor = self._combined_factor(
                geometry_bucket=self._geometry_buckets.get(geometry_key),
                temperature_bucket=self._temperature_buckets.get(temperature_key),
            )
            raw_kwh = float(slot.get("pv_estimate_kwh", 0.0))
            result.append({**slot, "pv_estimate_kwh": round(raw_kwh * factor, 4)})
        return result

    def build_snapshot(
        self,
        *,
        now: datetime,
        hourly_forecast: list[dict[str, Any]] | None,
        current_environment: dict[str, Any] | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        hourly_weather_forecast: list[dict[str, Any]] | None = None,
    ) -> CorrectionModelSnapshot:
        """Build compact diagnostics data for the current season/geometry model."""
        season = self._season_bucket(now.month) or 0
        active = [
            bucket for key, bucket in self._geometry_buckets.items()
            if key.startswith(f"s{season}|") and bucket.samples >= _MIN_EARLY_SAMPLES
        ]
        confident = [
            bucket for key, bucket in self._geometry_buckets.items()
            if key.startswith(f"s{season}|") and bucket.samples >= _MIN_CONFIDENT_SAMPLES
        ]
        current_geometry_key = self._geometry_key_from_snapshot(current_environment or {})
        current_geometry_bucket = self._geometry_buckets.get(current_geometry_key)
        current_temperature_key = self._temperature_key_from_snapshot(current_environment or {})
        current_temperature_bucket = self._temperature_buckets.get(current_temperature_key)
        current_geometry_factor = self._effective_factor(current_geometry_bucket)
        current_temperature_factor = self._effective_temperature_factor(current_temperature_bucket)

        today_geometry_factors: dict[str, dict[str, float]] = {}
        raw_total = 0.0
        corrected_total = 0.0
        if hourly_forecast:
            for start, raw_kwh in self._iter_forecast_entries(hourly_forecast):
                if start.date() != now.date() or raw_kwh <= 0:
                    continue
                geometry_key = self._geometry_key_for_forecast_slot(
                    slot_time=start,
                    latitude=latitude,
                    longitude=longitude,
                )
                geometry_bucket = self._geometry_buckets.get(geometry_key)
                temperature_key = self._temperature_key_for_forecast_slot(
                    slot_time=start,
                    hourly_weather_forecast=hourly_weather_forecast,
                )
                temperature_bucket = self._temperature_buckets.get(temperature_key)
                geometry_factor = self._effective_factor(geometry_bucket)
                temperature_factor = self._effective_temperature_factor(temperature_bucket)
                factor = geometry_factor * temperature_factor
                label = f"{start.hour:02d}:00"
                today_geometry_factors[label] = {
                    "factor": round(factor, 4),
                    "samples": float(geometry_bucket.samples if geometry_bucket is not None else 0),
                    "season_bucket": self._extract_bucket_value(geometry_key, "s"),
                    "solar_elevation_bucket": self._extract_bucket_value(geometry_key, "e"),
                    "solar_azimuth_bucket": self._extract_bucket_value(geometry_key, "a"),
                    "temperature_bucket_c": self._extract_bucket_value(temperature_key, "t"),
                    "temperature_factor": round(temperature_factor, 4),
                    "temperature_samples": float(temperature_bucket.samples if temperature_bucket is not None else 0),
                }
                raw_total += raw_kwh
                corrected_total += raw_kwh * factor

        if confident:
            state = "ready"
        elif active:
            state = "learning"
        else:
            state = "inactive"

        avg_factor = (
            sum(self._effective_factor(bucket) for bucket in active) / len(active)
            if active
            else 1.0
        )

        return CorrectionModelSnapshot(
            state=state,
            current_season=season,
            active_buckets=len(active),
            confident_buckets=len(confident),
            average_factor_this_season=round(avg_factor, 4),
            today_geometry_factors=today_geometry_factors,
            current_total_factor=round(current_geometry_factor * current_temperature_factor, 4),
            current_geometry_factor=round(current_geometry_factor, 4),
            current_geometry_samples=current_geometry_bucket.samples if current_geometry_bucket else 0,
            current_geometry_key=current_geometry_key,
            current_temperature_factor=round(current_temperature_factor, 4),
            current_temperature_samples=current_temperature_bucket.samples if current_temperature_bucket else 0,
            current_temperature_key=current_temperature_key,
            raw_vs_corrected_delta_today=round(corrected_total - raw_total, 4),
            last_environment=dict(current_environment or self._today_context_by_hour.get(now.hour, {})),
        )

    def _rollover_if_needed(
        self,
        today: date,
        sunrise: datetime | None,
        sunset: datetime | None,
    ) -> None:
        if not self._today_date:
            self._today_date = today.isoformat()
            return
        if self._today_date == today.isoformat():
            return

        previous_date = date.fromisoformat(self._today_date)
        for hour in range(24):
            if hour in self._finalized_hours:
                continue
            self._finalize_hour(previous_date, hour, self._today_sunrise, self._today_sunset)

        self._today_date = today.isoformat()
        self._today_actual_kwh_by_hour = {}
        self._today_raw_forecast_kwh_by_hour = {}
        self._today_context_by_hour = {}
        self._finalized_hours = set()
        self._today_sunrise = sunrise
        self._today_sunset = sunset

    def _update_forecast_map(self, current_date: date, hourly_forecast: list[dict[str, Any]] | None) -> None:
        if not hourly_forecast:
            return
        fresh_forecast_by_hour: dict[int, float] = {}
        for raw_start, kwh in self._iter_forecast_entries(hourly_forecast):
            if raw_start.date() != current_date:
                continue
            fresh_forecast_by_hour[raw_start.hour] = fresh_forecast_by_hour.get(raw_start.hour, 0.0) + kwh
        self._today_raw_forecast_kwh_by_hour = fresh_forecast_by_hour

    def _finalize_completed_hours(
        self,
        now: datetime,
        sunrise: datetime | None,
        sunset: datetime | None,
    ) -> None:
        if not self._today_date:
            return
        current_date = date.fromisoformat(self._today_date)
        for hour in sorted(self._today_raw_forecast_kwh_by_hour):
            if hour >= now.hour or hour in self._finalized_hours:
                continue
            self._finalize_hour(current_date, hour, sunrise, sunset)

    def _finalize_hour(
        self,
        current_date: date,
        hour: int,
        sunrise: datetime | None,
        sunset: datetime | None,
    ) -> None:
        actual_kwh = self._today_actual_kwh_by_hour.get(hour, 0.0)
        raw_forecast_kwh = self._today_raw_forecast_kwh_by_hour.get(hour, 0.0)
        self._finalized_hours.add(hour)

        if not self._hour_is_eligible(current_date, hour, actual_kwh, raw_forecast_kwh, sunrise, sunset):
            return

        geometry_key = self._context_key_for_hour(hour)
        if not geometry_key:
            return

        bucket = self._geometry_buckets.get(geometry_key)
        observed_factor = max(_MIN_FACTOR, min(_MAX_FACTOR, actual_kwh / max(raw_forecast_kwh, 0.05)))
        if bucket is None:
            if len(self._geometry_buckets) >= _MAX_GEOMETRY_BUCKETS:
                _LOGGER.warning(
                    "ForecastCorrectionModel: geometry bucket limit reached (%d) - skipping %s",
                    _MAX_GEOMETRY_BUCKETS,
                    geometry_key,
                )
                return
            self._geometry_buckets[geometry_key] = GeometryBucket(
                factor=observed_factor,
                samples=1,
                avg_abs_error_kwh=abs(actual_kwh - raw_forecast_kwh),
            )
            return

        alpha = 1.0 / min(bucket.samples + 1, 30)
        bucket.factor = max(
            _MIN_FACTOR,
            min(
                _MAX_FACTOR,
                bucket.factor
                + max(-_MAX_FACTOR_DELTA, min(_MAX_FACTOR_DELTA, observed_factor - bucket.factor)) * alpha,
            ),
        )
        bucket.avg_abs_error_kwh = (
            (bucket.avg_abs_error_kwh * bucket.samples) + abs(actual_kwh - raw_forecast_kwh)
        ) / (bucket.samples + 1)
        bucket.samples += 1

        temperature_key = self._temperature_key_for_hour(hour)
        if not temperature_key:
            return
        residual_factor = observed_factor / max(self._effective_factor(bucket), 0.01)
        residual_factor = max(_MIN_TEMP_FACTOR, min(_MAX_TEMP_FACTOR, residual_factor))
        temperature_bucket = self._temperature_buckets.get(temperature_key)
        if temperature_bucket is None:
            if len(self._temperature_buckets) >= _MAX_TEMPERATURE_BUCKETS:
                _LOGGER.warning(
                    "ForecastCorrectionModel: temperature bucket limit reached (%d) - skipping %s",
                    _MAX_TEMPERATURE_BUCKETS,
                    temperature_key,
                )
                return
            self._temperature_buckets[temperature_key] = TemperatureBucket(
                factor=residual_factor,
                samples=1,
                avg_abs_error_kwh=abs(actual_kwh - raw_forecast_kwh),
            )
            return

        alpha = 1.0 / min(temperature_bucket.samples + 1, 30)
        temperature_bucket.factor = max(
            _MIN_TEMP_FACTOR,
            min(
                _MAX_TEMP_FACTOR,
                temperature_bucket.factor
                + max(
                    -_MAX_FACTOR_DELTA,
                    min(_MAX_FACTOR_DELTA, residual_factor - temperature_bucket.factor),
                )
                * alpha,
            ),
        )
        temperature_bucket.avg_abs_error_kwh = (
            (temperature_bucket.avg_abs_error_kwh * temperature_bucket.samples)
            + abs(actual_kwh - raw_forecast_kwh)
        ) / (temperature_bucket.samples + 1)
        temperature_bucket.samples += 1

    @staticmethod
    def _iter_forecast_entries(hourly_forecast: list[dict[str, Any]]) -> list[tuple[datetime, float]]:
        entries: list[tuple[datetime, float]] = []
        for slot in hourly_forecast:
            raw_start = slot.get("period_start")
            if raw_start is None:
                continue
            try:
                start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                if start.tzinfo is not None:
                    start = ha_dt.as_local(start)
                entries.append((start, float(slot.get("pv_estimate_kwh", 0.0))))
            except (TypeError, ValueError):
                continue
        return entries

    @staticmethod
    def _hour_is_eligible(
        current_date: date,
        hour: int,
        actual_kwh: float,
        raw_forecast_kwh: float,
        sunrise: datetime | None,
        sunset: datetime | None,
    ) -> bool:
        if max(actual_kwh, raw_forecast_kwh) < _MIN_VALID_KWH:
            return False

        if sunrise is None or sunset is None:
            return True

        default_tz = getattr(ha_dt, "DEFAULT_TIME_ZONE", None) or getattr(sunrise, "tzinfo", None) or getattr(sunset, "tzinfo", None)
        midpoint = datetime.combine(current_date, datetime.min.time()).replace(
            hour=hour, minute=30, tzinfo=default_tz
        )
        return (ha_dt.as_local(sunrise) + _SUN_BUFFER) <= midpoint <= (ha_dt.as_local(sunset) - _SUN_BUFFER)

    @staticmethod
    def _effective_factor(bucket: GeometryBucket | None) -> float:
        if bucket is None or bucket.samples < _MIN_EARLY_SAMPLES:
            return 1.0
        confidence = min(
            1.0,
            max(
                0.0,
                (bucket.samples - _MIN_EARLY_SAMPLES + 1)
                / (_MIN_CONFIDENT_SAMPLES - _MIN_EARLY_SAMPLES + 1),
            ),
        )
        return 1.0 + (bucket.factor - 1.0) * confidence

    @staticmethod
    def _effective_temperature_factor(bucket: TemperatureBucket | None) -> float:
        if bucket is None or bucket.samples < _MIN_EARLY_SAMPLES:
            return 1.0
        confidence = min(
            1.0,
            max(
                0.0,
                (bucket.samples - _MIN_EARLY_SAMPLES + 1)
                / (_MIN_CONFIDENT_SAMPLES - _MIN_EARLY_SAMPLES + 1),
            ),
        )
        return 1.0 + (bucket.factor - 1.0) * confidence

    def _combined_factor(
        self,
        *,
        geometry_bucket: GeometryBucket | None,
        temperature_bucket: TemperatureBucket | None,
    ) -> float:
        return self._effective_factor(geometry_bucket) * self._effective_temperature_factor(temperature_bucket)

    def _context_key_for_hour(self, hour: int) -> str:
        return self._geometry_key_from_snapshot(self._today_context_by_hour.get(hour, {}))

    def _temperature_key_for_hour(self, hour: int) -> str:
        return self._temperature_key_from_snapshot(self._today_context_by_hour.get(hour, {}))

    def _geometry_key_from_snapshot(self, snapshot: dict[str, Any]) -> str:
        season = snapshot.get("season_bucket")
        elevation = snapshot.get("solar_elevation_bucket")
        azimuth = snapshot.get("solar_azimuth_bucket")
        if season is None:
            season = self._season_bucket(snapshot.get("month"))
        if elevation is None:
            elevation = self._elevation_bucket(snapshot.get("solar_elevation"))
        if azimuth is None:
            azimuth = self._azimuth_bucket(snapshot.get("solar_azimuth"))
        if None in (season, elevation, azimuth):
            return ""
        return f"s{int(season)}|e{int(elevation)}|a{int(azimuth)}"

    def _temperature_key_from_snapshot(self, snapshot: dict[str, Any]) -> str:
        season = snapshot.get("season_bucket")
        temperature = snapshot.get("temperature_bucket_c")
        if season is None:
            season = self._season_bucket(snapshot.get("month"))
        if temperature is None:
            temperature = self._temperature_bucket(snapshot.get("temperature_c"))
        if None in (season, temperature):
            return ""
        return f"s{int(season)}|t{int(temperature)}"

    def _geometry_key_for_forecast_slot(
        self,
        *,
        slot_time: datetime,
        latitude: float | None,
        longitude: float | None,
    ) -> str:
        if latitude is None or longitude is None:
            return self._geometry_key_from_snapshot(self._today_context_by_hour.get(slot_time.hour, {}))
        solar_elevation, solar_azimuth = self._solar_position(
            when=slot_time,
            latitude=latitude,
            longitude=longitude,
        )
        return self._geometry_key_from_snapshot(
            {
                "month": slot_time.month,
                "solar_elevation": solar_elevation,
                "solar_azimuth": solar_azimuth,
            }
        )

    def _temperature_key_for_forecast_slot(
        self,
        *,
        slot_time: datetime,
        hourly_weather_forecast: list[dict[str, Any]] | None,
    ) -> str:
        temperature_c = self._temperature_for_slot(
            slot_time=slot_time,
            hourly_weather_forecast=hourly_weather_forecast,
        )
        return self._temperature_key_from_snapshot(
            {
                "month": slot_time.month,
                "temperature_c": temperature_c,
            }
        )

    @staticmethod
    def _solar_position(*, when: datetime, latitude: float, longitude: float) -> tuple[float | None, float | None]:
        try:
            from astral import LocationInfo
            from astral.sun import azimuth as calc_az, elevation as calc_elev
        except ImportError:
            return (None, None)
        observer = LocationInfo(latitude=latitude, longitude=longitude).observer
        local_when = ha_dt.as_local(when) if when.tzinfo is not None else when
        return (
            calc_elev(observer, dateandtime=local_when),
            calc_az(observer, dateandtime=local_when),
        )

    @staticmethod
    def _extract_bucket_value(key: str, prefix: str) -> int | None:
        for part in key.split("|"):
            if part.startswith(prefix):
                try:
                    return int(part[1:])
                except ValueError:
                    return None
        return None

    @staticmethod
    def _temperature_for_slot(
        *,
        slot_time: datetime,
        hourly_weather_forecast: list[dict[str, Any]] | None,
    ) -> float | None:
        if not hourly_weather_forecast:
            return None
        local_slot = ha_dt.as_local(slot_time) if slot_time.tzinfo is not None else slot_time
        for entry in hourly_weather_forecast:
            raw_start = entry.get("datetime")
            if raw_start is None:
                continue
            try:
                start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                start = ha_dt.as_local(start) if start.tzinfo is not None else start
            except (TypeError, ValueError):
                continue
            if start <= local_slot < start + timedelta(hours=1):
                try:
                    return float(entry.get("temperature"))
                except (TypeError, ValueError):
                    return None
        return None

    def _build_hour_context(
        self,
        *,
        now: datetime,
        weather_snapshot: dict[str, Any],
        solar_elevation: float | None,
        solar_azimuth: float | None,
    ) -> dict[str, Any]:
        return {
            "month": now.month,
            "season_bucket": self._season_bucket(now.month),
            "hour": now.hour,
            "condition": weather_snapshot.get("condition"),
            "cloud_coverage_pct": weather_snapshot.get("cloud_coverage_pct"),
            "temperature_c": weather_snapshot.get("temperature_c"),
            "precipitation_mm": weather_snapshot.get("precipitation_mm"),
            "wind_speed_mps": weather_snapshot.get("wind_speed_mps"),
            "wind_bearing_deg": weather_snapshot.get("wind_bearing_deg"),
            "humidity_pct": weather_snapshot.get("humidity_pct"),
            "is_daylight": weather_snapshot.get("is_daylight"),
            "solar_elevation": solar_elevation,
            "solar_elevation_bucket": self._elevation_bucket(solar_elevation),
            "solar_azimuth": solar_azimuth,
            "solar_azimuth_bucket": self._azimuth_bucket(solar_azimuth),
        }

    @staticmethod
    def _season_bucket(month: Any) -> int | None:
        """Return season index: 0=winter, 1=spring, 2=summer, 3=autumn."""
        try:
            m = int(month)
        except (TypeError, ValueError):
            return None
        return {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}[m]

    @staticmethod
    def _elevation_bucket(value: Any) -> int | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        clamped = max(-10.0, min(90.0, numeric))
        return int((clamped // 10) * 10)

    @staticmethod
    def _azimuth_bucket(value: Any) -> int | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        normalized = numeric % 360.0
        return int((normalized // 30) * 30)

    @staticmethod
    def _temperature_bucket(value: Any) -> int | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return int((numeric // 5) * 5)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
