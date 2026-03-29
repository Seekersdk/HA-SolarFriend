"""Compact long-term evaluation logging for forecast model comparison."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as ha_dt

STORAGE_VERSION = 1
STORAGE_KEY = "solarfriend_model_evaluation_state"


@dataclass
class EvaluationSummary:
    """Compact monthly summary derived from the JSONL evaluation log."""

    period_month: str = ""
    rows: int = 0
    best_model: str = "n/a"
    mae_by_model: dict[str, float] = field(default_factory=dict)
    mape_by_model: dict[str, float] = field(default_factory=dict)
    bias_by_model: dict[str, float] = field(default_factory=dict)


class ModelEvaluationLogger:
    """Append one compact JSONL row per finalized forecast slot."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        entry_id: str,
        log_path: str | Path,
    ) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._log_path = Path(log_path)
        self._store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY}.{entry_id}")
        self._lock = asyncio.Lock()
        self._last_logged_slot: str = ""
        self._summary_cache_key: str = ""
        self._summary_cache: EvaluationSummary = EvaluationSummary()

    async def async_load(self) -> None:
        """Load the last logged slot marker."""
        try:
            data = await self._store.async_load()
        except Exception:
            return
        if isinstance(data, dict):
            self._last_logged_slot = str(data.get("last_logged_slot", ""))

    async def async_save(self) -> None:
        """Persist the last logged slot marker."""
        await self._store.async_save({"last_logged_slot": self._last_logged_slot})

    async def build_summary(self, *, now: datetime) -> EvaluationSummary:
        """Return cached summary for the current month."""
        month_key = _normalize_local(now).strftime("%Y-%m")
        if month_key == self._summary_cache_key:
            return self._summary_cache

        def _read() -> EvaluationSummary:
            return summarize_evaluation_log(self._log_path, month_key=month_key)

        async with self._lock:
            summary = await asyncio.to_thread(_read)
            self._summary_cache_key = month_key
            self._summary_cache = summary
            return summary

    async def append_slot(
        self,
        *,
        slot_start: datetime,
        slot_minutes: int,
        actual_kwh: float,
        solcast_kwh: float,
        empirical_kwh: float | None,
        solar_elevation: float | None,
        solar_azimuth: float | None,
        cloud_coverage_pct: float | None,
        temperature_c: float | None,
        track2_rows: dict[str, dict[str, float | None]],
    ) -> None:
        """Append one deduplicated slot row to the evaluation log."""
        slot_key = _slot_key(slot_start)
        if self._last_logged_slot and slot_key <= self._last_logged_slot:
            return

        payload: dict[str, Any] = {
            "schema_version": 1,
            "entry_id": self._entry_id,
            "logged_at": ha_dt.now().isoformat(),
            "period_start": slot_key,
            "period_minutes": slot_minutes,
            "actual_kwh": round(actual_kwh, 4),
            "solcast_kwh": round(solcast_kwh, 4),
            "empirisk_kwh": round(empirical_kwh, 4) if empirical_kwh is not None else None,
            "solar_elevation": round(solar_elevation, 2) if solar_elevation is not None else None,
            "solar_azimuth": round(solar_azimuth, 2) if solar_azimuth is not None else None,
            "cloud_coverage_pct": (
                round(cloud_coverage_pct, 1) if cloud_coverage_pct is not None else None
            ),
            "temperature_c": round(temperature_c, 1) if temperature_c is not None else None,
        }
        for key, row in sorted(track2_rows.items()):
            payload[f"beregnet_{key}_kwh"] = (
                round(row.get("kwh", 0.0), 4) if row.get("kwh") is not None else None
            )
            payload[f"beregnet_{key}_confidence"] = (
                round(row.get("confidence", 0.0), 4)
                if row.get("confidence") is not None
                else None
            )

        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"

        def _write() -> None:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)

        async with self._lock:
            await asyncio.to_thread(_write)
            self._last_logged_slot = slot_key
            await self.async_save()


def lookup_forecast_kwh(
    hourly_forecast: list[dict[str, Any]] | None,
    slot_start: datetime,
    *,
    slot_delta: timedelta | None = None,
) -> float | None:
    """Return kWh for the forecast slot covering ``slot_start``."""
    if not hourly_forecast:
        return None
    local_slot = _normalize_local(slot_start).replace(second=0, microsecond=0, tzinfo=None)
    delta = slot_delta or _infer_slot_delta(hourly_forecast)
    for entry in hourly_forecast:
        raw_start = entry.get("period_start")
        if raw_start is None:
            continue
        try:
            start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
        except (TypeError, ValueError):
            continue
        start = _normalize_local(start).replace(second=0, microsecond=0, tzinfo=None)
        if start <= local_slot < start + delta:
            try:
                return float(entry.get("pv_estimate_kwh", 0.0))
            except (TypeError, ValueError):
                return None
    return None


def lookup_weather_value(
    hourly_weather_forecast: list[dict[str, Any]] | None,
    slot_start: datetime,
    key: str,
) -> float | None:
    """Return one numeric weather value for the slot covering ``slot_start``."""
    if not hourly_weather_forecast:
        return None
    local_slot = _normalize_local(slot_start)
    for entry in hourly_weather_forecast:
        raw_dt = entry.get("datetime")
        if raw_dt is None:
            continue
        try:
            start = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
        except (TypeError, ValueError):
            continue
        start = _normalize_local(start)
        if start <= local_slot < start + timedelta(hours=1):
            try:
                value = entry.get(key)
                return float(value) if value is not None else None
            except (TypeError, ValueError):
                return None
    return None


def _infer_slot_delta(hourly_forecast: list[dict[str, Any]]) -> timedelta:
    starts: list[datetime] = []
    for entry in hourly_forecast:
        raw_start = entry.get("period_start")
        if raw_start is None:
            continue
        try:
            start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
        except (TypeError, ValueError):
            continue
        starts.append(_normalize_local(start).replace(second=0, microsecond=0, tzinfo=None))
    starts.sort()
    for idx in range(1, len(starts)):
        delta = starts[idx] - starts[idx - 1]
        if delta.total_seconds() > 0:
            return delta
    return timedelta(hours=1)


def _normalize_local(value: datetime) -> datetime:
    return ha_dt.as_local(value) if value.tzinfo is not None else value


def _slot_key(slot_start: datetime) -> str:
    return _normalize_local(slot_start).replace(second=0, microsecond=0).isoformat()


def summarize_evaluation_log(log_path: str | Path, *, month_key: str) -> EvaluationSummary:
    """Summarize one month of evaluation rows from the JSONL log."""
    path = Path(log_path)
    if not path.exists():
        return EvaluationSummary(period_month=month_key)

    error_totals: dict[str, float] = {}
    pct_error_totals: dict[str, float] = {}
    bias_totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    rows = 0

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            period_start = str(row.get("period_start", ""))
            if not period_start.startswith(month_key):
                continue
            try:
                actual_kwh = float(row.get("actual_kwh"))
            except (TypeError, ValueError):
                continue
            if actual_kwh <= 0:
                continue
            rows += 1
            for model_key in (
                "solcast",
                "empirisk",
                "beregnet_fast",
                "beregnet_medium",
                "beregnet_fine",
            ):
                field_name = f"{model_key}_kwh" if model_key != "solcast" else "solcast_kwh"
                predicted = row.get(field_name)
                if predicted is None:
                    continue
                try:
                    predicted_kwh = float(predicted)
                except (TypeError, ValueError):
                    continue
                abs_error = abs(predicted_kwh - actual_kwh)
                pct_error = abs_error / actual_kwh * 100.0
                bias = predicted_kwh - actual_kwh
                error_totals[model_key] = error_totals.get(model_key, 0.0) + abs_error
                pct_error_totals[model_key] = pct_error_totals.get(model_key, 0.0) + pct_error
                bias_totals[model_key] = bias_totals.get(model_key, 0.0) + bias
                counts[model_key] = counts.get(model_key, 0) + 1

    mae_by_model = {
        key: round(error_totals[key] / counts[key], 4)
        for key in counts
        if counts[key] > 0
    }
    mape_by_model = {
        key: round(pct_error_totals[key] / counts[key], 2)
        for key in counts
        if counts[key] > 0
    }
    bias_by_model = {
        key: round(bias_totals[key] / counts[key], 4)
        for key in counts
        if counts[key] > 0
    }
    best_model = min(mae_by_model, key=mae_by_model.get) if mae_by_model else "n/a"
    return EvaluationSummary(
        period_month=month_key,
        rows=rows,
        best_model=best_model,
        mae_by_model=mae_by_model,
        mape_by_model=mape_by_model,
        bias_by_model=bias_by_model,
    )
