"""SolarFriend ForecastTracker — tracks forecast accuracy over time."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .forecast_adapter import ForecastData, get_forecast_for_period
from .time_utils import normalize_local_datetime

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
STORAGE_KEY = "solarfriend_forecast_tracker"
_MIN_VALID_DAY_KWH = 2.0
_ROLLING_DAYS = 14
_MAX_HISTORY = 30


@dataclass
class ForecastDayRecord:
    date: str
    actual_kwh: float
    forecast_kwh: float
    valid: bool


@dataclass
class ForecastMetrics:
    today_actual_kwh: float = 0.0
    today_predicted_kwh: float = 0.0
    today_error_kwh: float = 0.0
    today_accuracy_pct: float = 0.0
    yesterday_actual_kwh: float = 0.0
    yesterday_predicted_kwh: float = 0.0
    yesterday_error_kwh: float = 0.0
    yesterday_accuracy_pct: float = 0.0
    bias_factor_14d: float = 1.0
    mae_14d_kwh: float = 0.0
    mape_14d_pct: float = 0.0
    accuracy_14d_pct: float = 0.0
    valid_days_14d: int = 0
    correction_valid: bool = False
    history_14d: list[dict[str, Any]] = field(default_factory=list)


class ForecastTracker:
    """Persists actual vs forecast production history and derives quality metrics."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._hass = hass
        self._legacy_entry_id = entry_id
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self.today_date: str = ""
        self.today_actual_kwh: float = 0.0
        self.today_forecast_baseline_kwh: float = 0.0
        self.history: list[ForecastDayRecord] = []

    async def async_load(self) -> None:
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
                    "ForecastTracker migrated legacy storage for entry_id=%s to stable key",
                    self._legacy_entry_id,
                )
                await self._store.async_save(data)
        if not data:
            return
        self.today_date = str(data.get("today_date", ""))
        self.today_actual_kwh = float(data.get("today_actual_kwh", 0.0))
        self.today_forecast_baseline_kwh = float(data.get("today_forecast_baseline_kwh", 0.0))
        self.history = [
            ForecastDayRecord(
                date=str(entry.get("date", "")),
                actual_kwh=float(entry.get("actual_kwh", 0.0)),
                forecast_kwh=float(entry.get("forecast_kwh", 0.0)),
                valid=bool(entry.get("valid", False)),
            )
            for entry in data.get("history", [])
            if entry.get("date")
        ]

    async def _async_safe_load(
        self,
        store: Store,
        storage_key: str,
    ) -> dict[str, Any] | None:
        """Load persisted state without aborting integration startup on corruption."""
        try:
            data = await store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "ForecastTracker storage load failed for %s; starting fresh: %s",
                storage_key,
                exc,
            )
            return None
        return data if isinstance(data, dict) else None

    async def async_save(self) -> None:
        await self._store.async_save(
            {
                "today_date": self.today_date,
                "today_actual_kwh": round(self.today_actual_kwh, 6),
                "today_forecast_baseline_kwh": round(self.today_forecast_baseline_kwh, 6),
                "history": [
                    {
                        "date": item.date,
                        "actual_kwh": round(item.actual_kwh, 6),
                        "forecast_kwh": round(item.forecast_kwh, 6),
                        "valid": item.valid,
                    }
                    for item in self.history[-_MAX_HISTORY:]
                ],
            }
        )

    def update(
        self,
        *,
        now: datetime,
        pv_power_w: float,
        dt_seconds: float,
        forecast_total_today_kwh: float | None,
    ) -> None:
        today = now.date().isoformat()
        self._rollover_if_needed(today)

        if forecast_total_today_kwh is not None and self.today_forecast_baseline_kwh <= 0:
            self.today_forecast_baseline_kwh = max(0.0, float(forecast_total_today_kwh))

        if dt_seconds > 0 and pv_power_w > 0:
            self.today_actual_kwh += pv_power_w * dt_seconds / 3_600_000

    def build_metrics(self, now: datetime, forecast_data: ForecastData | None) -> ForecastMetrics:
        self._rollover_if_needed(now.date().isoformat())

        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        predicted_today = 0.0
        if forecast_data is not None:
            predicted_today = self._forecast_until_now(
                forecast_data.hourly_forecast,
                start_of_day,
                now,
            )

        today_error = self.today_actual_kwh - predicted_today
        today_accuracy = self._accuracy_pct(self.today_actual_kwh, predicted_today)

        yesterday = self._get_record_for_date((now.date()).toordinal() - 1)
        valid_records = [item for item in self.history if item.valid][- _ROLLING_DAYS :]

        if valid_records:
            forecast_sum = sum(item.forecast_kwh for item in valid_records)
            actual_sum = sum(item.actual_kwh for item in valid_records)
            errors = [abs(item.actual_kwh - item.forecast_kwh) for item in valid_records]
            pct_errors = [
                (abs(item.actual_kwh - item.forecast_kwh) / max(item.forecast_kwh, 0.1)) * 100.0
                for item in valid_records
            ]
            bias_factor = actual_sum / max(forecast_sum, 0.1)
            mae_14d = sum(errors) / len(errors)
            mape_14d = sum(pct_errors) / len(pct_errors)
            accuracy_14d = max(0.0, 100.0 - mape_14d)
        else:
            bias_factor = 1.0
            mae_14d = 0.0
            mape_14d = 0.0
            accuracy_14d = 0.0

        correction_valid = len(valid_records) >= 10 and 0.75 <= bias_factor <= 1.25

        return ForecastMetrics(
            today_actual_kwh=round(self.today_actual_kwh, 3),
            today_predicted_kwh=round(predicted_today, 3),
            today_error_kwh=round(today_error, 3),
            today_accuracy_pct=round(today_accuracy, 1),
            yesterday_actual_kwh=round(yesterday.actual_kwh, 3) if yesterday else 0.0,
            yesterday_predicted_kwh=round(yesterday.forecast_kwh, 3) if yesterday else 0.0,
            yesterday_error_kwh=round(
                yesterday.actual_kwh - yesterday.forecast_kwh, 3
            )
            if yesterday
            else 0.0,
            yesterday_accuracy_pct=round(
                self._accuracy_pct(yesterday.actual_kwh, yesterday.forecast_kwh), 1
            )
            if yesterday
            else 0.0,
            bias_factor_14d=round(bias_factor, 4),
            mae_14d_kwh=round(mae_14d, 3),
            mape_14d_pct=round(mape_14d, 1),
            accuracy_14d_pct=round(accuracy_14d, 1),
            valid_days_14d=len(valid_records),
            correction_valid=correction_valid,
            history_14d=[
                {
                    "date": item.date,
                    "actual_kwh": round(item.actual_kwh, 3),
                    "forecast_kwh": round(item.forecast_kwh, 3),
                    "error_kwh": round(item.actual_kwh - item.forecast_kwh, 3),
                    "accuracy_pct": round(self._accuracy_pct(item.actual_kwh, item.forecast_kwh), 1),
                    "valid": item.valid,
                }
                for item in valid_records
            ],
        )

    def _rollover_if_needed(self, today: str) -> None:
        if not self.today_date:
            self.today_date = today
            return
        if self.today_date == today:
            return

        self._upsert_record(
            ForecastDayRecord(
                date=self.today_date,
                actual_kwh=self.today_actual_kwh,
                forecast_kwh=self.today_forecast_baseline_kwh,
                valid=max(self.today_actual_kwh, self.today_forecast_baseline_kwh) >= _MIN_VALID_DAY_KWH,
            )
        )
        self.today_date = today
        self.today_actual_kwh = 0.0
        self.today_forecast_baseline_kwh = 0.0

    def _upsert_record(self, record: ForecastDayRecord) -> None:
        self.history = [item for item in self.history if item.date != record.date]
        self.history.append(record)
        self.history.sort(key=lambda item: item.date)
        self.history = self.history[-_MAX_HISTORY:]

    def _get_record_for_date(self, ordinal: int) -> ForecastDayRecord | None:
        date_str = datetime.combine(datetime.fromordinal(ordinal).date(), time.min).date().isoformat()
        for item in reversed(self.history):
            if item.date == date_str:
                return item
        return None

    @staticmethod
    def _accuracy_pct(actual_kwh: float, forecast_kwh: float) -> float:
        denom = max(actual_kwh, forecast_kwh, 0.1)
        return max(0.0, 100.0 - (abs(actual_kwh - forecast_kwh) / denom * 100.0))

    @staticmethod
    def _forecast_until_now(
        hourly_forecast: list[dict[str, Any]],
        start_dt: datetime,
        now: datetime,
    ) -> float:
        """Return forecast energy from day start until now, prorating the active slot."""
        start_dt = normalize_local_datetime(start_dt)
        now = normalize_local_datetime(now)
        entries: list[tuple[datetime, float]] = []
        for entry in hourly_forecast:
            raw_start = entry.get("period_start")
            if raw_start is None:
                continue
            if isinstance(raw_start, str):
                try:
                    slot_start = datetime.fromisoformat(raw_start)
                except (ValueError, TypeError):
                    continue
            elif isinstance(raw_start, datetime):
                slot_start = raw_start
            else:
                continue
            slot_start = normalize_local_datetime(slot_start)
            entries.append((slot_start, float(entry.get("pv_estimate_kwh", 0.0))))

        entries.sort(key=lambda item: item[0])
        total = 0.0
        for idx, (slot_start, slot_kwh) in enumerate(entries):
            next_start = entries[idx + 1][0] if idx + 1 < len(entries) else slot_start + timedelta(hours=1)
            slot_end = next_start if next_start > slot_start else slot_start + timedelta(hours=1)

            overlap_start = max(start_dt, slot_start)
            overlap_end = min(now, slot_end)
            if overlap_end <= overlap_start:
                continue

            slot_seconds = max((slot_end - slot_start).total_seconds(), 1.0)
            overlap_seconds = (overlap_end - overlap_start).total_seconds()
            total += slot_kwh * (overlap_seconds / slot_seconds)

        return round(total, 4)
