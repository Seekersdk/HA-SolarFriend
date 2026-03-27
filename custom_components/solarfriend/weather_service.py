"""Weather forecast fetch/cache and Solar Only profile selection."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.util import dt as ha_dt

from .weather_profile import (
    DEFAULT_PROFILE,
    SolarOnlyWeatherProfile,
    select_hourly_weather_profile,
)

_LOGGER = logging.getLogger(__name__)


def _wind_speed_to_mps(value: Any, unit: str | None) -> float | None:
    """Normalize provider wind speed to m/s for downstream model consumers."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    normalized_unit = (unit or "").strip().lower()
    if normalized_unit in {"km/h", "kmh", "kph"}:
        return numeric / 3.6
    if normalized_unit in {"m/s", "mps", "ms"}:
        return numeric
    if normalized_unit in {"mph"}:
        return numeric * 0.44704
    return numeric


class WeatherProfileService:
    """Fetch and cache hourly weather forecast for Solar Only profiling."""

    def __init__(
        self,
        hass: Any,
        *,
        weather_entity: str | None,
        cache_ttl: timedelta = timedelta(minutes=15),
    ) -> None:
        self._hass = hass
        self._weather_entity = weather_entity
        self._cache_ttl = cache_ttl
        self._hourly_forecast: list[dict[str, Any]] = []
        self._fetched_at: datetime | None = None

    def update_weather_entity(self, weather_entity: str | None) -> None:
        """Refresh configured weather entity and clear stale cache if changed."""
        if self._weather_entity != weather_entity:
            self._weather_entity = weather_entity
            self._hourly_forecast = []
            self._fetched_at = None

    async def async_fetch_hourly_forecast(self) -> list[dict[str, Any]]:
        """Fetch and cache hourly weather forecast."""
        if not self._weather_entity:
            return []

        now = ha_dt.now()
        if (
            self._fetched_at is not None
            and self._hourly_forecast
            and (now - self._fetched_at) < self._cache_ttl
        ):
            return self._hourly_forecast

        try:
            result = await self._hass.services.async_call(
                "weather",
                "get_forecasts",
                {"type": "hourly", "entity_id": self._weather_entity},
                blocking=True,
                return_response=True,
            )
            forecast = (
                result.get(self._weather_entity, {}).get("forecast", [])
                if isinstance(result, dict)
                else []
            )
            self._hourly_forecast = forecast if isinstance(forecast, list) else []
            self._fetched_at = now
        except Exception as exc:  # noqa: BLE001
            _LOGGER.debug("Kunne ikke hente weather hourly forecast: %s", exc)
            if self._fetched_at is None:
                self._hourly_forecast = []
        return self._hourly_forecast

    async def async_get_current_profile(self, now: datetime) -> SolarOnlyWeatherProfile:
        """Return the active Solar Only weather profile for the current hour."""
        hourly_forecast = await self.async_fetch_hourly_forecast()
        if not hourly_forecast:
            return DEFAULT_PROFILE
        return select_hourly_weather_profile(hourly_forecast=hourly_forecast, now=now)

    async def async_get_current_hour_snapshot(self, now: datetime) -> dict[str, Any]:
        """Return a compact weather snapshot for the current hour."""
        hourly_forecast = await self.async_fetch_hourly_forecast()
        if not hourly_forecast:
            return {}
        now_local = ha_dt.as_local(now) if now.tzinfo is not None else ha_dt.as_local(now.replace(tzinfo=ha_dt.UTC))

        weather_state = self._hass.states.get(self._weather_entity) if self._weather_entity else None
        wind_speed_unit = None
        if weather_state is not None:
            wind_speed_unit = weather_state.attributes.get("wind_speed_unit")

        for entry in hourly_forecast:
            raw_start = entry.get("datetime")
            if raw_start is None:
                continue
            try:
                start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                start = ha_dt.as_local(start) if start.tzinfo is not None else ha_dt.as_local(start.replace(tzinfo=ha_dt.UTC))
            except (ValueError, TypeError):
                continue
            if start <= now_local < start + timedelta(hours=1):
                month = now_local.month
                return {
                    "condition": entry.get("condition"),
                    "cloud_coverage_pct": entry.get("cloud_coverage"),
                    "temperature_c": entry.get("temperature"),
                    "precipitation_mm": entry.get("precipitation"),
                    "wind_speed_mps": _wind_speed_to_mps(entry.get("wind_speed"), wind_speed_unit),
                    "wind_bearing_deg": entry.get("wind_bearing"),
                    "humidity_pct": entry.get("humidity"),
                    "is_daylight": bool(6 <= now_local.hour < 22),
                    "is_heating_season": bool(month in (10, 11, 12, 1, 2, 3, 4)),
                }
        return {}
