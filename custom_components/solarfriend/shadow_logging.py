"""Structured shadow logging helpers for replay and evaluation."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any


class ShadowLogger:
    """Build and write structured shadow-log payloads."""

    def __init__(
        self,
        *,
        entry: Any,
        profile: Any,
        log_path: str | Path,
        enabled: bool = True,
    ) -> None:
        self._entry = entry
        self._profile = profile
        self._log_path = Path(log_path)
        self.enabled = enabled
        self._lock = asyncio.Lock()

    @staticmethod
    def json_safe(value: Any) -> Any:
        """Convert nested values to JSON-safe primitives."""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): ShadowLogger.json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [ShadowLogger.json_safe(item) for item in value]
        return value

    def build_horizon(self, data: Any, now: datetime, normalize_local_datetime: Any) -> list[dict[str, Any]]:
        """Build a replayable horizon of price, load and raw/corrected forecast inputs."""
        if data.price_data is None:
            return []

        raw_prices = data.price_data.to_legacy_raw_prices()
        price_by_start: dict[datetime, float] = {}
        for entry in raw_prices:
            raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_dt is None or raw_price is None:
                continue
            try:
                dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
                local_dt = normalize_local_datetime(dt).replace(minute=0, second=0, microsecond=0)
                if local_dt >= now.replace(minute=0, second=0, microsecond=0):
                    price_by_start[local_dt] = float(raw_price)
            except (TypeError, ValueError):
                continue

        raw_forecast_by_start: dict[datetime, float] = {}
        if data.forecast_data is not None:
            for slot in data.forecast_data.hourly_forecast:
                raw_start = slot.get("period_start")
                if raw_start is None:
                    continue
                try:
                    dt = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                    local_dt = normalize_local_datetime(dt).replace(minute=0, second=0, microsecond=0)
                    raw_forecast_by_start[local_dt] = raw_forecast_by_start.get(local_dt, 0.0) + float(
                        slot.get("pv_estimate_kwh", 0.0)
                    )
                except (TypeError, ValueError):
                    continue

        correction_factor = (
            data.forecast_bias_factor_14d
            if data.forecast_correction_valid and data.forecast_bias_factor_14d > 0
            else 1.0
        )

        horizon: list[dict[str, Any]] = []
        for start, price in sorted(price_by_start.items(), key=lambda item: item[0]):
            load_w = float(self._profile.get_predicted_watt(start.hour, start.weekday() >= 5))
            raw_pv_kwh = raw_forecast_by_start.get(start, 0.0)
            corrected_pv_kwh = raw_pv_kwh * correction_factor
            horizon.append(
                {
                    "start": start.isoformat(),
                    "price_dkk": round(price, 4),
                    "forecast_load_w": round(load_w, 1),
                    "forecast_load_kwh": round(load_w / 1000.0, 4),
                    "raw_pv_kwh": round(raw_pv_kwh, 4),
                    "corrected_pv_kwh": round(corrected_pv_kwh, 4),
                    "raw_net_load_kwh": round(max(0.0, (load_w / 1000.0) - raw_pv_kwh), 4),
                    "corrected_net_load_kwh": round(max(0.0, (load_w / 1000.0) - corrected_pv_kwh), 4),
                }
            )
        return horizon

    def build_payload(
        self,
        data: Any,
        now: datetime,
        *,
        optimizer_ran: bool,
        normalize_local_datetime: Any,
    ) -> dict[str, Any]:
        """Build a structured shadow-log payload for replay and evaluation."""
        optimize_result = data.optimize_result
        return {
            "schema_version": 1,
            "timestamp": now.isoformat(),
            "entry_id": self._entry.entry_id,
            "optimizer_ran": optimizer_ran,
            "current_actuals": {
                "pv_power_w": round(data.pv_power, 1),
                "load_power_w": round(data.load_power, 1),
                "grid_power_w": round(data.grid_power, 1),
                "battery_power_w": round(data.battery_power, 1),
                "battery_soc_pct": round(data.battery_soc, 2),
                "price_dkk": round(data.price, 4),
            },
            "battery_context": {
                "battery_capacity_kwh": float(self._entry.data.get("battery_capacity_kwh", 10.0)),
                "battery_min_soc": float(self._entry.data.get("battery_min_soc", 10.0)),
                "battery_max_soc": float(self._entry.data.get("battery_max_soc", 100.0)),
                "charge_rate_kw": float(self._entry.data.get("charge_rate_kw", 6.0)),
                "battery_cost_per_kwh": float(self._entry.data.get("battery_cost_per_kwh", 0.0)),
                "battery_weighted_cost": round(data.battery_weighted_cost, 4),
                "battery_solar_fraction": round(data.battery_solar_fraction, 4),
                "battery_solar_kwh": round(data.battery_solar_kwh, 4),
                "battery_grid_kwh": round(data.battery_grid_kwh, 4),
            },
            "learning_model": {
                "profile_confidence": data.profile_confidence,
                "profile_days_collected": data.profile_days_collected,
                "consumption_profile_day_type": data.consumption_profile_day_type,
                "consumption_profile_chart_w": [round(v, 1) for v in data.consumption_profile_chart],
            },
            "forecast_quality": {
                "forecast_type": data.forecast_data.forecast_type if data.forecast_data else None,
                "forecast_confidence": data.forecast_data.confidence if data.forecast_data else None,
                "forecast_correction_valid": data.forecast_correction_valid,
                "forecast_bias_factor_14d": data.forecast_bias_factor_14d,
                "forecast_mae_14d_kwh": data.forecast_mae_14d_kwh,
                "forecast_mape_14d_pct": data.forecast_mape_14d_pct,
                "forecast_accuracy_14d_pct": data.forecast_accuracy_14d_pct,
                "forecast_valid_days_14d": data.forecast_valid_days_14d,
                "forecast_actual_today_so_far_kwh": data.forecast_actual_today_so_far_kwh,
                "forecast_predicted_today_so_far_kwh": data.forecast_predicted_today_so_far_kwh,
                "forecast_error_today_so_far_kwh": data.forecast_error_today_so_far_kwh,
                "forecast_accuracy_today_so_far_pct": data.forecast_accuracy_today_so_far_pct,
                "forecast_actual_yesterday_kwh": data.forecast_actual_yesterday_kwh,
                "forecast_predicted_yesterday_kwh": data.forecast_predicted_yesterday_kwh,
                "forecast_error_yesterday_kwh": data.forecast_error_yesterday_kwh,
                "forecast_accuracy_yesterday_pct": data.forecast_accuracy_yesterday_pct,
                "forecast_history_14d": self.json_safe(data.forecast_history_14d),
            },
            "forecast_snapshot": {
                "total_today_kwh": data.forecast_data.total_today_kwh if data.forecast_data else None,
                "total_tomorrow_kwh": data.forecast_data.total_tomorrow_kwh if data.forecast_data else None,
                "remaining_today_kwh": data.forecast_data.remaining_today_kwh if data.forecast_data else None,
                "power_now_w": data.forecast_data.power_now_w if data.forecast_data else None,
                "power_next_hour_w": data.forecast_data.power_next_hour_w if data.forecast_data else None,
                "solar_next_2h_kwh": data.solar_next_2h,
                "solar_until_sunset_kwh": data.solar_until_sunset,
                "raw_hourly_forecast": self.json_safe(data.forecast_data.hourly_forecast if data.forecast_data else []),
                "corrected_hourly_forecast": self.json_safe(
                    [
                        {
                            **slot,
                            "pv_estimate_kwh": round(
                                float(slot.get("pv_estimate_kwh", 0.0))
                                * (
                                    data.forecast_bias_factor_14d
                                    if data.forecast_correction_valid and data.forecast_bias_factor_14d > 0
                                    else 1.0
                                ),
                                4,
                            ),
                        }
                        for slot in (data.forecast_data.hourly_forecast if data.forecast_data else [])
                    ]
                ),
            },
            "optimizer_inputs": {
                "price_horizon": self.build_horizon(data, now, normalize_local_datetime),
                "raw_prices": self.json_safe(data.price_data.to_legacy_raw_prices() if data.price_data else []),
            },
            "optimizer_output": {
                "strategy": optimize_result.strategy if optimize_result else None,
                "reason": optimize_result.reason if optimize_result else None,
                "target_soc": optimize_result.target_soc if optimize_result else None,
                "charge_now": optimize_result.charge_now if optimize_result else None,
                "cheapest_charge_hour": optimize_result.cheapest_charge_hour if optimize_result else None,
                "night_charge_kwh": optimize_result.night_charge_kwh if optimize_result else None,
                "morning_need_kwh": optimize_result.morning_need_kwh if optimize_result else None,
                "day_deficit_kwh": optimize_result.day_deficit_kwh if optimize_result else None,
                "peak_need_kwh": optimize_result.peak_need_kwh if optimize_result else None,
                "expected_saving_dkk": optimize_result.expected_saving_dkk if optimize_result else None,
                "best_discharge_hours": optimize_result.best_discharge_hours if optimize_result else [],
                "allowed_discharge_slots": (
                    optimize_result.allowed_discharge_slots if optimize_result else []
                ),
                "battery_plan": self.json_safe(data.battery_plan),
                "forecast_soc_chart": self.json_safe(data.forecast_soc_chart),
            },
        }

    async def append(self, payload: dict[str, Any]) -> None:
        """Append a JSONL shadow-log row."""
        if not self.enabled:
            return

        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"

        def _write() -> None:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)

        async with self._lock:
            await asyncio.to_thread(_write)
