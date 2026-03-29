"""SolarFriend ForecastAdapter — normalises solar forecast data from different HA integrations."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from homeassistant.util import dt as ha_dt

from .time_utils import normalize_local_datetime

_LOGGER = logging.getLogger(__name__)

# Solcast PV Forecast integration — standard entity IDs
_SOLCAST_TODAY    = "sensor.solcast_pv_forecast_forecast_today"
_SOLCAST_TOMORROW = "sensor.solcast_pv_forecast_forecast_tomorrow"
_SOLCAST_POWER_NOW = "sensor.solcast_pv_forecast_power_now"
_SOLCAST_PEAK_TIME = "sensor.solcast_pv_forecast_peak_time_today"
_SOLCAST_PEAK_WATT = "sensor.solcast_pv_forecast_peak_forecast_today"

_MAX_SLOT_KWH = 100.0  # 200 kW continuous — far beyond any residential installation


@dataclass
class ForecastData:
    """Normalised solar forecast — one object regardless of upstream integration."""

    total_today_kwh: float = 0.0
    total_tomorrow_kwh: float = 0.0
    remaining_today_kwh: float = 0.0
    power_now_w: float = 0.0
    power_next_hour_w: float = 0.0
    peak_time_today: datetime | None = None
    peak_power_today_w: float = 0.0
    hourly_forecast: list[dict[str, Any]] = field(default_factory=list)
    confidence: float = 0.5          # 0.0–1.0; Solcast ≈ 0.8, Forecast.Solar = 0.5
    forecast_type: str = "forecast_solar"


# ---------------------------------------------------------------------------
# Standalone helper (also used by battery_optimizer.py)
# ---------------------------------------------------------------------------

def get_forecast_for_period(
    hourly_forecast: list[dict[str, Any]],
    from_dt: datetime,
    to_dt: datetime,
) -> float:
    """Sum pv_estimate_kwh for all entries whose period_start falls in [from_dt, to_dt).

    Handles both timezone-aware and naive datetimes — all are normalised to
    timezone-aware (local) before comparison.
    """
    from_dt = normalize_local_datetime(from_dt)
    to_dt = normalize_local_datetime(to_dt)

    total = 0.0
    for entry in hourly_forecast:
        ps = entry.get("period_start")
        if ps is None:
            continue
        if isinstance(ps, str):
            try:
                ps = datetime.fromisoformat(ps)
            except (ValueError, TypeError):
                continue
        elif not isinstance(ps, datetime):
            continue
        ps = normalize_local_datetime(ps)
        if from_dt <= ps < to_dt:
            total += float(entry.get("pv_estimate_kwh", 0.0))
    return round(total, 4)


# ---------------------------------------------------------------------------
# ForecastAdapter
# ---------------------------------------------------------------------------

class ForecastAdapter:
    """Fetch and normalise solar forecast data from a HA state machine."""

    @staticmethod
    async def from_hass(
        hass: Any,
        forecast_type: str,
        forecast_sensor_entity: str | None,
    ) -> ForecastData | None:
        """Return ForecastData or None if the required sensors are unavailable."""
        if forecast_type == "solcast":
            return ForecastAdapter._from_solcast(hass)
        return ForecastAdapter._from_forecast_solar(hass, forecast_sensor_entity)

    # ------------------------------------------------------------------
    # Solcast PV Forecast
    # ------------------------------------------------------------------

    @staticmethod
    def _from_solcast(hass: Any) -> ForecastData | None:
        """Read forecast data from the Solcast PV Forecast integration."""

        def _safe_float(entity_id: str) -> float | None:
            state = hass.states.get(entity_id)
            if state is None or state.state in ("unavailable", "unknown", ""):
                return None
            try:
                return float(state.state)
            except (ValueError, TypeError):
                return None

        total_today = _safe_float(_SOLCAST_TODAY)
        if total_today is None:
            _LOGGER.debug("ForecastAdapter: %s unavailable — skipping Solcast", _SOLCAST_TODAY)
            return None

        total_tomorrow = _safe_float(_SOLCAST_TOMORROW) or 0.0
        power_now_w   = _safe_float(_SOLCAST_POWER_NOW) or 0.0   # sensor already in W
        peak_power_w  = _safe_float(_SOLCAST_PEAK_WATT) or 0.0

        peak_time: datetime | None = None
        peak_state = hass.states.get(_SOLCAST_PEAK_TIME)
        if peak_state and peak_state.state not in ("unavailable", "unknown", ""):
            try:
                peak_time = ha_dt.as_local(datetime.fromisoformat(peak_state.state))
            except (ValueError, TypeError):
                pass

        # Build hourly_forecast from detailedForecast attributes on today + tomorrow
        now = ha_dt.now()
        hourly_forecast: list[dict[str, Any]] = []

        for sensor_id in (_SOLCAST_TODAY, _SOLCAST_TOMORROW):
            state = hass.states.get(sensor_id)
            if state is None:
                continue
            detailed = state.attributes.get("detailedForecast")
            if not isinstance(detailed, list):
                continue
            for entry in detailed:
                ps_raw = entry.get("period_start")
                if ps_raw is None:
                    continue
                try:
                    ps = ps_raw if isinstance(ps_raw, datetime) else datetime.fromisoformat(str(ps_raw))
                    ps = normalize_local_datetime(ps)
                except (ValueError, TypeError):
                    continue

                # Derive slot duration from period_end if available; fall back to 30 min.
                interval_hours = 0.5
                pe_raw = entry.get("period_end")
                if pe_raw is not None:
                    try:
                        pe = pe_raw if isinstance(pe_raw, datetime) else datetime.fromisoformat(str(pe_raw))
                        pe = normalize_local_datetime(pe)
                        derived = (pe - ps).total_seconds() / 3600
                        if derived > 0:
                            interval_hours = derived
                    except (ValueError, TypeError):
                        pass

                kw    = float(entry.get("pv_estimate",   0.0))
                kw10  = float(entry.get("pv_estimate10", 0.0))
                kw90  = float(entry.get("pv_estimate90", 0.0))

                def _clamp_kwh(kw_val: float) -> float:
                    kwh = kw_val * interval_hours
                    if kwh < 0:
                        return 0.0
                    if kwh > _MAX_SLOT_KWH:
                        _LOGGER.warning(
                            "ForecastAdapter: slot forecast %.1f kWh afskåret til %.1f kWh",
                            kwh,
                            _MAX_SLOT_KWH,
                        )
                        return _MAX_SLOT_KWH
                    return kwh

                hourly_forecast.append({
                    "period_start":      ps,
                    "pv_estimate_kwh":   round(_clamp_kwh(kw),   4),
                    "pv_estimate10_kwh": round(_clamp_kwh(kw10), 4),
                    "pv_estimate90_kwh": round(_clamp_kwh(kw90), 4),
                })

        hourly_forecast.sort(key=lambda e: e["period_start"])

        # Remaining today = sum of future 30-min slots within today
        today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        remaining_today = get_forecast_for_period(hourly_forecast, now, today_end)

        # Power next hour = first slot that starts ≥ next whole hour
        next_hour_start = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        power_next_w = 0.0
        for entry in hourly_forecast:
            ps = entry["period_start"]
            if isinstance(ps, datetime) and ps >= next_hour_start:
                power_next_w = entry["pv_estimate_kwh"] * 2000  # kWh/30min → W
                break

        # Confidence from sensor attributes (optional)
        confidence = 0.8
        today_state = hass.states.get(_SOLCAST_TODAY)
        if today_state:
            raw_conf = today_state.attributes.get("confidence")
            if raw_conf is not None:
                try:
                    v = float(raw_conf)
                    confidence = v / 100 if v > 1 else v
                except (ValueError, TypeError):
                    pass

        return ForecastData(
            total_today_kwh=total_today,
            total_tomorrow_kwh=total_tomorrow,
            remaining_today_kwh=remaining_today,
            power_now_w=power_now_w,
            power_next_hour_w=power_next_w,
            peak_time_today=peak_time,
            peak_power_today_w=peak_power_w,
            hourly_forecast=hourly_forecast,
            confidence=confidence,
            forecast_type="solcast",
        )

    # ------------------------------------------------------------------
    # Forecast.Solar / generic energy sensor
    # ------------------------------------------------------------------

    @staticmethod
    def _from_forecast_solar(
        hass: Any,
        forecast_sensor_entity: str | None,
    ) -> ForecastData | None:
        """Read forecast data from a generic Forecast.Solar-style sensor."""
        if not forecast_sensor_entity:
            return None

        state = hass.states.get(forecast_sensor_entity)
        if state is None or state.state in ("unavailable", "unknown", ""):
            _LOGGER.debug("ForecastAdapter: %s unavailable", forecast_sensor_entity)
            return None

        try:
            total_today = float(state.state)
        except (ValueError, TypeError):
            return None

        total_tomorrow = 0.0
        tmr_state = hass.states.get("sensor.energy_production_tomorrow")
        if tmr_state and tmr_state.state not in ("unavailable", "unknown", ""):
            try:
                total_tomorrow = float(tmr_state.state)
            except (ValueError, TypeError):
                pass

        # Build a minimal hourly_forecast from current-hour / next-hour sensors
        now = ha_dt.now()
        this_hour = now.replace(minute=0, second=0, microsecond=0)
        hourly_forecast: list[dict[str, Any]] = []

        for offset, sensor_id in enumerate(
            ("sensor.energy_current_hour", "sensor.energy_next_hour")
        ):
            s = hass.states.get(sensor_id)
            if s and s.state not in ("unavailable", "unknown", ""):
                try:
                    kwh = float(s.state)
                    hourly_forecast.append({
                        "period_start":      this_hour + timedelta(hours=offset),
                        "pv_estimate_kwh":   round(kwh, 4),
                        "pv_estimate10_kwh": round(kwh * 0.7, 4),
                        "pv_estimate90_kwh": round(kwh * 1.3, 4),
                    })
                except (ValueError, TypeError):
                    pass

        # Remaining today — try sensor attribute first, fall back to total
        try:
            remaining_today = float(state.attributes.get("remaining", total_today))
        except (ValueError, TypeError):
            remaining_today = total_today

        power_now_w  = hourly_forecast[0]["pv_estimate_kwh"] * 2000 if hourly_forecast else 0.0
        power_next_w = hourly_forecast[1]["pv_estimate_kwh"] * 2000 if len(hourly_forecast) > 1 else 0.0

        return ForecastData(
            total_today_kwh=total_today,
            total_tomorrow_kwh=total_tomorrow,
            remaining_today_kwh=remaining_today,
            power_now_w=power_now_w,
            power_next_hour_w=power_next_w,
            peak_time_today=None,
            peak_power_today_w=0.0,
            hourly_forecast=hourly_forecast,
            confidence=0.5,
            forecast_type="forecast_solar",
        )

    # ------------------------------------------------------------------
    # Class-level helper (delegates to module function)
    # ------------------------------------------------------------------

    @staticmethod
    def get_forecast_for_period(
        forecast: ForecastData | list[dict[str, Any]],
        from_dt: datetime,
        to_dt: datetime,
    ) -> float:
        """Sum pv_estimate_kwh for entries within [from_dt, to_dt)."""
        lst = forecast.hourly_forecast if isinstance(forecast, ForecastData) else forecast
        return get_forecast_for_period(lst, from_dt, to_dt)
