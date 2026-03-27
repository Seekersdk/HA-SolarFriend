"""Advanced hourly consumption model with passive weather tracking.

AI bot guide:
- This module is intentionally side-car only. It must not change the default
  `ConsumptionProfile` behavior unless explicitly wired in later.
- The model stores one finalized record per day/hour and keeps at most 365 days.
- Weather is tracked per finalized hour but is not yet used aggressively in the
  prediction. V1 focuses on clean data collection and dashboard visibility.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
import logging


_LOGGER = logging.getLogger(__name__)


STORAGE_KEY = "solarfriend_advanced_consumption_model"
STORAGE_VERSION = 1
MAX_HISTORY_DAYS = 365


@dataclass
class AdvancedConsumptionRecord:
    """One finalized hourly record."""

    day: str
    hour: int
    timestamp: str
    load_w: float
    predicted_w: float | None
    error_w: float | None
    weekday: int
    is_weekend: bool
    month: int
    season: str
    condition: str | None = None
    cloud_coverage_pct: float | None = None
    temperature_c: float | None = None
    precipitation_mm: float | None = None
    wind_speed_mps: float | None = None
    wind_bearing_deg: float | None = None
    is_daylight: bool | None = None
    is_heating_season: bool | None = None


@dataclass
class AdvancedConsumptionSnapshot:
    """UI-facing snapshot for sensors and dashboards."""

    state: str = "disabled"
    records_count: int = 0
    tracked_days: int = 0
    current_hour_prediction_w: float | None = None
    current_hour_partial_actual_w: float | None = None
    last_hour_actual_w: float | None = None
    last_hour_prediction_w: float | None = None
    last_hour_error_w: float | None = None
    today_mae_w: float | None = None
    rolling_7d_mae_w: float | None = None
    today_hourly_actual: list[float | None] = field(default_factory=list)
    today_hourly_prediction: list[float | None] = field(default_factory=list)
    recent_daily_totals: list[dict[str, Any]] = field(default_factory=list)
    last_weather_snapshot: dict[str, Any] = field(default_factory=dict)


class AdvancedConsumptionModel:
    """Track hourly non-EV consumption together with contextual weather."""

    def __init__(self) -> None:
        self._records: dict[str, dict[str, dict[str, Any]]] = {}
        self._current_hour_key: str | None = None
        self._current_hour_sum_w: float = 0.0
        self._current_hour_samples: int = 0
        self._current_hour_weather: dict[str, Any] = {}

    def _store(self, hass: HomeAssistant) -> Store:
        return Store(hass, STORAGE_VERSION, STORAGE_KEY)

    async def async_load(self, hass: HomeAssistant) -> None:
        data = await self._async_safe_load(self._store(hass))
        if not data:
            return
        self._records = dict(data.get("records", {}))
        self._current_hour_key = data.get("current_hour_key")
        self._current_hour_sum_w = float(data.get("current_hour_sum_w", 0.0))
        self._current_hour_samples = int(data.get("current_hour_samples", 0))
        self._current_hour_weather = dict(data.get("current_hour_weather", {}))

    async def _async_safe_load(self, store: Store) -> dict[str, Any] | None:
        """Load persisted state without aborting startup on corrupted storage."""
        try:
            data = await store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "AdvancedConsumptionModel storage load failed for %s; starting fresh: %s",
                STORAGE_KEY,
                exc,
            )
            return None
        return data if isinstance(data, dict) else None

    async def async_save(self, hass: HomeAssistant) -> None:
        await self._store(hass).async_save(
            {
                "records": self._records,
                "current_hour_key": self._current_hour_key,
                "current_hour_sum_w": self._current_hour_sum_w,
                "current_hour_samples": self._current_hour_samples,
                "current_hour_weather": self._current_hour_weather,
            }
        )

    @staticmethod
    def _season_for_month(month: int) -> str:
        if month in (12, 1, 2):
            return "winter"
        if month in (3, 4, 5):
            return "spring"
        if month in (6, 7, 8):
            return "summer"
        return "autumn"

    def _record_iter(self) -> list[AdvancedConsumptionRecord]:
        records: list[AdvancedConsumptionRecord] = []
        for day in self._records.values():
            for raw in day.values():
                records.append(AdvancedConsumptionRecord(**raw))
        records.sort(key=lambda record: record.timestamp)
        return records

    def _predict_for_hour(self, dt: datetime) -> float | None:
        """Simple hierarchical baseline prediction from historical hourly records."""
        hour = dt.hour
        is_weekend = dt.weekday() >= 5
        month = dt.month
        season = self._season_for_month(month)
        records = [record for record in self._record_iter() if record.hour == hour]
        if not records:
            return None

        exact = [r.load_w for r in records if r.month == month and r.is_weekend == is_weekend]
        if exact:
            return round(sum(exact) / len(exact), 1)

        seasonal = [r.load_w for r in records if r.season == season and r.is_weekend == is_weekend]
        if seasonal:
            return round(sum(seasonal) / len(seasonal), 1)

        weekend_match = [r.load_w for r in records if r.is_weekend == is_weekend]
        if weekend_match:
            return round(sum(weekend_match) / len(weekend_match), 1)

        return round(sum(r.load_w for r in records) / len(records), 1)

    def _finalize_previous_hour(self, hour_start: datetime) -> None:
        if self._current_hour_samples <= 0:
            return

        avg_load_w = round(self._current_hour_sum_w / self._current_hour_samples, 1)
        prediction_w = self._predict_for_hour(hour_start)
        error_w = round(avg_load_w - prediction_w, 1) if prediction_w is not None else None
        day_key = hour_start.date().isoformat()
        day_records = self._records.setdefault(day_key, {})

        record = AdvancedConsumptionRecord(
            day=day_key,
            hour=hour_start.hour,
            timestamp=hour_start.isoformat(),
            load_w=avg_load_w,
            predicted_w=prediction_w,
            error_w=error_w,
            weekday=hour_start.weekday(),
            is_weekend=hour_start.weekday() >= 5,
            month=hour_start.month,
            season=self._season_for_month(hour_start.month),
            condition=self._current_hour_weather.get("condition"),
            cloud_coverage_pct=self._current_hour_weather.get("cloud_coverage_pct"),
            temperature_c=self._current_hour_weather.get("temperature_c"),
            precipitation_mm=self._current_hour_weather.get("precipitation_mm"),
            wind_speed_mps=self._current_hour_weather.get("wind_speed_mps"),
            wind_bearing_deg=self._current_hour_weather.get("wind_bearing_deg"),
            is_daylight=self._current_hour_weather.get("is_daylight"),
            is_heating_season=self._current_hour_weather.get("is_heating_season"),
        )
        day_records[str(hour_start.hour)] = asdict(record)
        self._prune(day_key)

    def _prune(self, newest_day_key: str) -> None:
        newest_day = datetime.fromisoformat(newest_day_key).date()
        keep_from = newest_day - timedelta(days=MAX_HISTORY_DAYS - 1)
        self._records = {
            day_key: value
            for day_key, value in self._records.items()
            if datetime.fromisoformat(day_key).date() >= keep_from
        }

    def update(self, *, now: datetime, load_w: float, weather_snapshot: dict[str, Any] | None) -> None:
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_key = hour_start.isoformat()

        if self._current_hour_key is None:
            self._current_hour_key = hour_key

        if hour_key != self._current_hour_key:
            previous_hour_start = datetime.fromisoformat(self._current_hour_key)
            self._finalize_previous_hour(previous_hour_start)
            self._current_hour_key = hour_key
            self._current_hour_sum_w = 0.0
            self._current_hour_samples = 0
            self._current_hour_weather = {}

        self._current_hour_sum_w += float(load_w)
        self._current_hour_samples += 1
        if weather_snapshot:
            self._current_hour_weather = dict(weather_snapshot)

    def build_snapshot(self, *, now: datetime, enabled: bool) -> AdvancedConsumptionSnapshot:
        records = self._record_iter()
        today_key = now.date().isoformat()
        today_records_raw = self._records.get(today_key, {})
        today_hourly_actual: list[float | None] = [None] * 24
        today_hourly_prediction: list[float | None] = [None] * 24

        for hour_str, raw in today_records_raw.items():
            hour = int(hour_str)
            today_hourly_actual[hour] = float(raw.get("load_w", 0.0))
            predicted = raw.get("predicted_w")
            today_hourly_prediction[hour] = float(predicted) if predicted is not None else None

        if self._current_hour_key == now.replace(minute=0, second=0, microsecond=0).isoformat():
            current_hour = now.hour
            if self._current_hour_samples > 0:
                today_hourly_actual[current_hour] = round(
                    self._current_hour_sum_w / self._current_hour_samples, 1
                )
            today_hourly_prediction[current_hour] = self._predict_for_hour(now)

        records_count = len(records)
        last_record = records[-1] if records else None

        today_errors = [abs(r.error_w) for r in records if r.day == today_key and r.error_w is not None]
        last_7_days = {
            (now - timedelta(days=offset)).date().isoformat()
            for offset in range(7)
        }
        rolling_7d_errors = [abs(r.error_w) for r in records if r.day in last_7_days and r.error_w is not None]

        recent_daily_totals: list[dict[str, Any]] = []
        for day_key in sorted(self._records.keys())[-14:]:
            day = self._records[day_key]
            total_w = sum(float(raw.get("load_w", 0.0)) for raw in day.values())
            recent_daily_totals.append(
                {"day": day_key, "total_kwh_equivalent": round(total_w / 1000.0, 3)}
            )

        if not enabled:
            state = "disabled"
        elif records_count < 24:
            state = "learning"
        else:
            state = "ready"

        return AdvancedConsumptionSnapshot(
            state=state,
            records_count=records_count,
            tracked_days=len(self._records),
            current_hour_prediction_w=self._predict_for_hour(now),
            current_hour_partial_actual_w=(
                round(self._current_hour_sum_w / self._current_hour_samples, 1)
                if self._current_hour_samples > 0
                else None
            ),
            last_hour_actual_w=last_record.load_w if last_record else None,
            last_hour_prediction_w=last_record.predicted_w if last_record else None,
            last_hour_error_w=last_record.error_w if last_record else None,
            today_mae_w=round(sum(today_errors) / len(today_errors), 1) if today_errors else None,
            rolling_7d_mae_w=(
                round(sum(rolling_7d_errors) / len(rolling_7d_errors), 1)
                if rolling_7d_errors
                else None
            ),
            today_hourly_actual=today_hourly_actual,
            today_hourly_prediction=today_hourly_prediction,
            recent_daily_totals=recent_daily_totals,
            last_weather_snapshot=dict(self._current_hour_weather),
        )
