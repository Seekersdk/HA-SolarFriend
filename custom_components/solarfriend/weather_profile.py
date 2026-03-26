"""Weather-based Solar Only profiles for EV charging."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .time_utils import normalize_local_datetime


@dataclass(frozen=True)
class SolarOnlyWeatherProfile:
    """Runtime thresholds for Solar Only EV charging."""

    key: str
    label: str
    start_surplus_w: float
    stop_surplus_w: float
    start_hold_seconds: int
    stop_hold_seconds: int
    grid_buffer_w: float


CLEAR_PROFILE = SolarOnlyWeatherProfile(
    key="clear",
    label="Skyfrit",
    start_surplus_w=1600.0,
    stop_surplus_w=1410.0,
    start_hold_seconds=300,
    stop_hold_seconds=300,
    grid_buffer_w=0.0,
)

PARTLY_CLOUDY_PROFILE = SolarOnlyWeatherProfile(
    key="partly_cloudy",
    label="Delvist skyet",
    start_surplus_w=2000.0,
    stop_surplus_w=1200.0,
    start_hold_seconds=300,
    stop_hold_seconds=600,
    grid_buffer_w=300.0,
)

CLOUDY_PROFILE = SolarOnlyWeatherProfile(
    key="cloudy",
    label="Overskyet",
    start_surplus_w=2000.0,
    stop_surplus_w=1000.0,
    start_hold_seconds=300,
    stop_hold_seconds=600,
    grid_buffer_w=500.0,
)

DEFAULT_PROFILE = PARTLY_CLOUDY_PROFILE

_CLEAR_CONDITIONS = {"sunny", "clear-night", "clear"}
_PARTLY_CLOUDY_CONDITIONS = {"partlycloudy"}
_CLOUDY_CONDITIONS = {
    "cloudy",
    "fog",
    "rainy",
    "pouring",
    "snowy",
    "snowy-rainy",
    "hail",
    "lightning",
    "lightning-rainy",
    "exceptional",
}


def classify_weather_profile(
    *,
    condition: str | None,
    cloud_coverage: float | None,
) -> SolarOnlyWeatherProfile:
    """Map a weather forecast condition to a Solar Only charging profile."""
    normalized_condition = (condition or "").lower()
    normalized_cloud_coverage = float(cloud_coverage) if cloud_coverage is not None else None

    if normalized_cloud_coverage is not None:
        if normalized_cloud_coverage < 15.0:
            return CLEAR_PROFILE
        if normalized_cloud_coverage >= 70.0:
            return CLOUDY_PROFILE

    if normalized_condition in _CLEAR_CONDITIONS:
        return CLEAR_PROFILE
    if normalized_condition in _CLOUDY_CONDITIONS:
        return CLOUDY_PROFILE
    if normalized_condition in _PARTLY_CLOUDY_CONDITIONS:
        return PARTLY_CLOUDY_PROFILE

    if normalized_cloud_coverage is None:
        return DEFAULT_PROFILE
    return PARTLY_CLOUDY_PROFILE


def select_hourly_weather_profile(
    *,
    hourly_forecast: list[dict[str, Any]],
    now: datetime,
) -> SolarOnlyWeatherProfile:
    """Return the profile for the current local hour from weather forecast data."""
    normalized_now = normalize_local_datetime(now)
    for entry in hourly_forecast:
        raw_start = entry.get("datetime")
        if raw_start is None:
            continue
        try:
            start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
            start = normalize_local_datetime(start)
        except ValueError:
            continue
        if start <= normalized_now < start + timedelta(hours=1):
            return classify_weather_profile(
                condition=entry.get("condition"),
                cloud_coverage=entry.get("cloud_coverage"),
            )

    if hourly_forecast:
        first = hourly_forecast[0]
        return classify_weather_profile(
            condition=first.get("condition"),
            cloud_coverage=first.get("cloud_coverage"),
        )

    return DEFAULT_PROFILE
