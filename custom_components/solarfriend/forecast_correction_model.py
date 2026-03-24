"""Passive month/hour forecast correction model."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

STORAGE_VERSION = 1
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
class CorrectionModelSnapshot:
    """Compact diagnostics snapshot for sensor exposure."""

    state: str = "inactive"
    current_month: int = 0
    active_buckets: int = 0
    confident_buckets: int = 0
    average_factor_this_month: float = 1.0
    today_hourly_factors: dict[str, dict[str, float]] = field(default_factory=dict)
    current_hour_factor: float = 1.0
    current_hour_samples: int = 0
    raw_vs_corrected_delta_today: float = 0.0


class ForecastCorrectionModel:
    """Build a passive month/hour correction model without applying it live yet."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._store = Store(hass, STORAGE_VERSION, f"solarfriend_forecast_correction_{entry_id}")
        self._buckets: dict[int, dict[int, HourBucket]] = {
            month: {hour: HourBucket() for hour in range(24)}
            for month in range(1, 13)
        }
        self._today_date: str = ""
        self._today_actual_kwh_by_hour: dict[int, float] = {}
        self._today_raw_forecast_kwh_by_hour: dict[int, float] = {}
        self._finalized_hours: set[int] = set()

    async def async_load(self) -> None:
        """Load persisted buckets and current-day partial data."""
        data = await self._store.async_load()
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

        self._today_date = str(data.get("today_date", ""))
        self._today_actual_kwh_by_hour = {
            int(hour): float(value)
            for hour, value in (data.get("today_actual_kwh_by_hour", {}) or {}).items()
        }
        self._today_raw_forecast_kwh_by_hour = {
            int(hour): float(value)
            for hour, value in (data.get("today_raw_forecast_kwh_by_hour", {}) or {}).items()
        }
        self._finalized_hours = {
            int(hour)
            for hour in (data.get("finalized_hours", []) or [])
        }

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
                "today_date": self._today_date,
                "today_actual_kwh_by_hour": {
                    str(hour): round(value, 6)
                    for hour, value in self._today_actual_kwh_by_hour.items()
                },
                "today_raw_forecast_kwh_by_hour": {
                    str(hour): round(value, 6)
                    for hour, value in self._today_raw_forecast_kwh_by_hour.items()
                },
                "finalized_hours": sorted(self._finalized_hours),
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
    ) -> None:
        """Update current-day actuals and finalize completed buckets when possible."""
        self._rollover_if_needed(now.date(), sunrise, sunset)

        self._today_date = now.date().isoformat()
        self._update_forecast_map(now.date(), hourly_forecast)

        if dt_seconds > 0 and pv_power_w > 0:
            self._today_actual_kwh_by_hour[now.hour] = self._today_actual_kwh_by_hour.get(now.hour, 0.0) + (
                pv_power_w * dt_seconds / 3_600_000
            )

        self._finalize_completed_hours(now, sunrise, sunset)

    def build_snapshot(
        self,
        *,
        now: datetime,
        hourly_forecast: list[dict[str, Any]] | None,
    ) -> CorrectionModelSnapshot:
        """Build compact diagnostics data for the current month/day."""
        month = now.month
        buckets = self._buckets[month]
        active = [bucket for bucket in buckets.values() if bucket.samples >= _MIN_EARLY_SAMPLES]
        confident = [bucket for bucket in buckets.values() if bucket.samples >= _MIN_CONFIDENT_SAMPLES]
        current_bucket = buckets[now.hour]

        today_forecast = self._forecast_by_hour_for_date(now.date(), hourly_forecast)
        today_hourly_factors: dict[str, dict[str, float]] = {}
        raw_total = 0.0
        corrected_total = 0.0
        for hour, raw_kwh in sorted(today_forecast.items()):
            if raw_kwh <= 0:
                continue
            bucket = buckets[hour]
            factor = self._effective_factor(bucket)
            today_hourly_factors[f"{hour:02d}:00"] = {
                "factor": round(factor, 4),
                "samples": float(bucket.samples),
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
            current_month=month,
            active_buckets=len(active),
            confident_buckets=len(confident),
            average_factor_this_month=round(avg_factor, 4),
            today_hourly_factors=today_hourly_factors,
            current_hour_factor=round(self._effective_factor(current_bucket), 4),
            current_hour_samples=current_bucket.samples,
            raw_vs_corrected_delta_today=round(corrected_total - raw_total, 4),
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
            self._finalize_hour(previous_date, hour, sunrise, sunset)

        self._today_date = today.isoformat()
        self._today_actual_kwh_by_hour = {}
        self._today_raw_forecast_kwh_by_hour = {}
        self._finalized_hours = set()

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
            return

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
        return (sunrise + _SUN_BUFFER) <= midpoint <= (sunset - _SUN_BUFFER)

    @staticmethod
    def _effective_factor(bucket: HourBucket) -> float:
        if bucket.samples < _MIN_EARLY_SAMPLES:
            return 1.0
        confidence = min(1.0, max(0.0, (bucket.samples - _MIN_EARLY_SAMPLES + 1) / (_MIN_CONFIDENT_SAMPLES - _MIN_EARLY_SAMPLES + 1)))
        return 1.0 + (bucket.factor - 1.0) * confidence
