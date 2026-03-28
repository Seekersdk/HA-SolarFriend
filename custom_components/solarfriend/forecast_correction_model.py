"""Passive forecast correction model with hour and environment-aware buckets."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

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


@dataclass
class HourBucket:
    """Learned correction state for one month/hour bucket."""

    factor: float = 1.0
    samples: int = 0
    avg_abs_error_kwh: float = 0.0


@dataclass
class ContextBucket:
    """Learned correction state for one month/environment bucket."""

    factor: float = 1.0
    samples: int = 0
    avg_abs_error_kwh: float = 0.0


@dataclass
class CorrectionModelSnapshot:
    """Compact diagnostics snapshot for sensor exposure."""

    state: str = "inactive"
    current_month: int = 0
    active_buckets: int = 0
    confident_buckets: int = 0
    average_factor_this_month: float = 1.0
    today_hourly_factors: dict[str, dict[str, float]] = field(default_factory=dict)
    today_contextual_factors: dict[str, dict[str, float]] = field(default_factory=dict)
    current_hour_factor: float = 1.0
    current_hour_samples: int = 0
    active_context_buckets: int = 0
    confident_context_buckets: int = 0
    current_context_factor: float = 1.0
    current_context_samples: int = 0
    current_context_key: str = ""
    raw_vs_corrected_delta_today: float = 0.0
    last_environment: dict[str, Any] = field(default_factory=dict)


class ForecastCorrectionModel:
    """Build a passive month/hour correction model without applying it live yet."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._hass = hass
        self._legacy_entry_id = entry_id
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._buckets: dict[int, dict[int, HourBucket]] = {
            month: {hour: HourBucket() for hour in range(24)}
            for month in range(1, 13)
        }
        self._context_buckets: dict[str, ContextBucket] = {}
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

        buckets = data.get("buckets", {})
        for month_str, hour_map in buckets.items():
            try:
                month = int(month_str)
            except (TypeError, ValueError):
                continue
            if month not in self._buckets:
                continue
            for hour_str, bucket in hour_map.items():
                try:
                    hour = int(hour_str)
                except (TypeError, ValueError):
                    continue
                if hour not in self._buckets[month]:
                    continue
                self._buckets[month][hour] = HourBucket(
                    factor=float(bucket.get("factor", 1.0)),
                    samples=int(bucket.get("samples", 0)),
                    avg_abs_error_kwh=float(bucket.get("avg_abs_error_kwh", 0.0)),
                )
        self._context_buckets = {
            str(key): ContextBucket(
                factor=float(bucket.get("factor", 1.0)),
                samples=int(bucket.get("samples", 0)),
                avg_abs_error_kwh=float(bucket.get("avg_abs_error_kwh", 0.0)),
            )
            for key, bucket in (data.get("context_buckets", {}) or {}).items()
        }

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
        """Persist buckets and current-day partial state."""
        await self._store.async_save(
            {
                "buckets": {
                    str(month): {
                        str(hour): {
                            "factor": round(bucket.factor, 4),
                            "samples": bucket.samples,
                            "avg_abs_error_kwh": round(bucket.avg_abs_error_kwh, 4),
                        }
                        for hour, bucket in hours.items()
                    }
                    for month, hours in self._buckets.items()
                },
                "context_buckets": {
                    key: {
                        "factor": round(bucket.factor, 4),
                        "samples": bucket.samples,
                        "avg_abs_error_kwh": round(bucket.avg_abs_error_kwh, 4),
                    }
                    for key, bucket in self._context_buckets.items()
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

    def build_snapshot(
        self,
        *,
        now: datetime,
        hourly_forecast: list[dict[str, Any]] | None,
        current_environment: dict[str, Any] | None = None,
    ) -> CorrectionModelSnapshot:
        """Build compact diagnostics data for the current month/day."""
        month = now.month
        buckets = self._buckets[month]
        active = [bucket for bucket in buckets.values() if bucket.samples >= _MIN_EARLY_SAMPLES]
        confident = [bucket for bucket in buckets.values() if bucket.samples >= _MIN_CONFIDENT_SAMPLES]
        current_bucket = buckets[now.hour]
        active_context = [
            bucket for bucket in self._context_buckets.values() if bucket.samples >= _MIN_EARLY_SAMPLES
        ]
        confident_context = [
            bucket for bucket in self._context_buckets.values() if bucket.samples >= _MIN_CONFIDENT_SAMPLES
        ]
        current_context_key = self._context_key_from_snapshot(current_environment or {})
        current_context_bucket = self._context_buckets.get(current_context_key, ContextBucket())

        today_forecast = self._forecast_by_hour_for_date(now.date(), hourly_forecast)
        today_hourly_factors: dict[str, dict[str, float]] = {}
        today_contextual_factors: dict[str, dict[str, float]] = {}
        raw_total = 0.0
        corrected_total = 0.0
        for hour, raw_kwh in sorted(today_forecast.items()):
            if raw_kwh <= 0:
                continue
            bucket = buckets[hour]
            hour_factor = self._effective_factor(bucket)
            context_snapshot = self._today_context_by_hour.get(hour, {})
            context_key = self._context_key_from_snapshot(context_snapshot)
            context_bucket = self._context_buckets.get(context_key)
            context_factor = (
                self._effective_context_factor(context_bucket)
                if context_bucket is not None
                else 1.0
            )
            factor = context_factor if context_bucket is not None else hour_factor
            today_hourly_factors[f"{hour:02d}:00"] = {
                "factor": round(hour_factor, 4),
                "samples": float(bucket.samples),
            }
            if context_snapshot:
                today_contextual_factors[f"{hour:02d}:00"] = {
                    "factor": round(factor, 4),
                    "samples": float(context_bucket.samples if context_bucket is not None else 0),
                    "solar_elevation_bucket": context_snapshot.get("solar_elevation_bucket"),
                    "solar_azimuth_bucket": context_snapshot.get("solar_azimuth_bucket"),
                    "cloud_coverage_bucket": context_snapshot.get("cloud_coverage_bucket"),
                    "temperature_bucket_c": context_snapshot.get("temperature_bucket_c"),
                }
            raw_total += raw_kwh
            corrected_total += raw_kwh * factor

        if confident_context:
            state = "ready"
        elif active_context:
            state = "learning"
        elif confident:
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
            current_month=month,
            active_buckets=len(active),
            confident_buckets=len(confident),
            average_factor_this_month=round(avg_factor, 4),
            today_hourly_factors=today_hourly_factors,
            today_contextual_factors=today_contextual_factors,
            current_hour_factor=round(self._effective_factor(current_bucket), 4),
            current_hour_samples=current_bucket.samples,
            active_context_buckets=len(active_context),
            confident_context_buckets=len(confident_context),
            current_context_factor=round(self._effective_context_factor(current_context_bucket), 4),
            current_context_samples=current_context_bucket.samples,
            current_context_key=current_context_key,
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

        month = current_date.month
        bucket = self._buckets[month][hour]
        observed_factor = max(_MIN_FACTOR, min(_MAX_FACTOR, actual_kwh / max(raw_forecast_kwh, 0.05)))
        if bucket.samples == 0:
            bucket.factor = observed_factor
            bucket.samples = 1
            bucket.avg_abs_error_kwh = abs(actual_kwh - raw_forecast_kwh)
        else:
            alpha = 1.0 / min(bucket.samples + 1, 12)
            bucket.factor = max(
                _MIN_FACTOR,
                min(
                    _MAX_FACTOR,
                    bucket.factor + max(-_MAX_FACTOR_DELTA, min(_MAX_FACTOR_DELTA, observed_factor - bucket.factor)) * alpha,
                ),
            )
            bucket.avg_abs_error_kwh = (
                (bucket.avg_abs_error_kwh * bucket.samples) + abs(actual_kwh - raw_forecast_kwh)
            ) / (bucket.samples + 1)
            bucket.samples += 1

        context_key = self._context_key_for_hour(hour)
        if not context_key:
            return
        context_bucket = self._context_buckets.get(context_key)
        if context_bucket is None:
            self._context_buckets[context_key] = ContextBucket(
                factor=observed_factor,
                samples=1,
                avg_abs_error_kwh=abs(actual_kwh - raw_forecast_kwh),
            )
            return
        alpha = 1.0 / min(context_bucket.samples + 1, 12)
        context_bucket.factor = max(
            _MIN_FACTOR,
            min(
                _MAX_FACTOR,
                context_bucket.factor
                + max(-_MAX_FACTOR_DELTA, min(_MAX_FACTOR_DELTA, observed_factor - context_bucket.factor)) * alpha,
            ),
        )
        context_bucket.avg_abs_error_kwh = (
            (context_bucket.avg_abs_error_kwh * context_bucket.samples) + abs(actual_kwh - raw_forecast_kwh)
        ) / (context_bucket.samples + 1)
        context_bucket.samples += 1

    @staticmethod
    def _iter_forecast_entries(hourly_forecast: list[dict[str, Any]]) -> list[tuple[datetime, float]]:
        entries: list[tuple[datetime, float]] = []
        for slot in hourly_forecast:
            raw_start = slot.get("period_start")
            if raw_start is None:
                continue
            try:
                start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                entries.append((start, float(slot.get("pv_estimate_kwh", 0.0))))
            except (TypeError, ValueError):
                continue
        return entries

    def _forecast_by_hour_for_date(
        self,
        current_date: date,
        hourly_forecast: list[dict[str, Any]] | None,
    ) -> dict[int, float]:
        forecast_by_hour: dict[int, float] = {}
        if not hourly_forecast:
            return forecast_by_hour
        for start, kwh in self._iter_forecast_entries(hourly_forecast):
            if start.date() != current_date:
                continue
            forecast_by_hour[start.hour] = forecast_by_hour.get(start.hour, 0.0) + kwh
        return forecast_by_hour

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

        midpoint = datetime.combine(current_date, datetime.min.time()).replace(hour=hour, minute=30)
        if sunrise.tzinfo is not None and midpoint.tzinfo is None:
            midpoint = midpoint.replace(tzinfo=sunrise.tzinfo)
        if sunset.tzinfo is not None and midpoint.tzinfo is None:
            midpoint = midpoint.replace(tzinfo=sunset.tzinfo)
        return (sunrise + _SUN_BUFFER) <= midpoint <= (sunset - _SUN_BUFFER)

    @staticmethod
    def _effective_factor(bucket: HourBucket) -> float:
        if bucket.samples < _MIN_EARLY_SAMPLES:
            return 1.0
        confidence = min(1.0, max(0.0, (bucket.samples - _MIN_EARLY_SAMPLES + 1) / (_MIN_CONFIDENT_SAMPLES - _MIN_EARLY_SAMPLES + 1)))
        return 1.0 + (bucket.factor - 1.0) * confidence

    @staticmethod
    def _effective_context_factor(bucket: ContextBucket) -> float:
        if bucket.samples < _MIN_EARLY_SAMPLES:
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

    def _context_key_for_hour(self, hour: int) -> str:
        return self._context_key_from_snapshot(self._today_context_by_hour.get(hour, {}))

    def _context_key_from_snapshot(self, snapshot: dict[str, Any]) -> str:
        month = snapshot.get("month")
        elevation = snapshot.get("solar_elevation_bucket")
        azimuth = snapshot.get("solar_azimuth_bucket")
        cloud = snapshot.get("cloud_coverage_bucket")
        if elevation is None:
            elevation = self._elevation_bucket(snapshot.get("solar_elevation"))
        if azimuth is None:
            azimuth = self._azimuth_bucket(snapshot.get("solar_azimuth"))
        if cloud is None:
            cloud = self._cloud_bucket(snapshot.get("cloud_coverage_pct"))
        if None in (month, elevation, azimuth, cloud):
            return ""
        return f"m{int(month)}|e{int(elevation)}|a{int(azimuth)}|c{int(cloud)}"

    def _build_hour_context(
        self,
        *,
        now: datetime,
        weather_snapshot: dict[str, Any],
        solar_elevation: float | None,
        solar_azimuth: float | None,
    ) -> dict[str, Any]:
        cloud_coverage = weather_snapshot.get("cloud_coverage_pct")
        temperature_c = weather_snapshot.get("temperature_c")
        return {
            "month": now.month,
            "hour": now.hour,
            "condition": weather_snapshot.get("condition"),
            "cloud_coverage_pct": cloud_coverage,
            "cloud_coverage_bucket": self._cloud_bucket(cloud_coverage),
            "temperature_c": temperature_c,
            "temperature_bucket_c": self._temperature_bucket(temperature_c),
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
    def _cloud_bucket(value: Any) -> int | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        clamped = max(0.0, min(100.0, numeric))
        return int((clamped // 20) * 20)

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
