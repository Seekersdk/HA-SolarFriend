"""Unit tests for passive forecast correction model."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import types
from datetime import datetime


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    _mock("homeassistant.core", HomeAssistant=type("HomeAssistant", (), {}))
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))
    _mock("homeassistant.helpers")
    _mock("homeassistant.const", Platform=type("Platform", (), {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select"}))


class _FakeStore:
    def __init__(self):
        self._data: dict | None = None

    async def async_load(self):
        return self._data

    async def async_save(self, data: dict):
        import copy

        self._data = copy.deepcopy(data)


_store = _FakeStore()
_mock("homeassistant.helpers.storage", Store=lambda hass, version, key: _store)

module_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "custom_components",
    "solarfriend",
    "forecast_correction_model.py",
)
spec = importlib.util.spec_from_file_location("test_forecast_correction_model_module", module_path)
assert spec is not None and spec.loader is not None
_fcm = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = _fcm
spec.loader.exec_module(_fcm)
_fcm.Store = lambda hass, version, key: _store
ForecastCorrectionModel = _fcm.ForecastCorrectionModel


def run(coro):
    return asyncio.run(coro)


def _forecast_slot(dt: datetime, kwh: float) -> dict:
    return {"period_start": dt.isoformat(), "pv_estimate_kwh": kwh}


def _make_model() -> ForecastCorrectionModel:
    return ForecastCorrectionModel(hass=object(), entry_id="test")


def _weather(**overrides) -> dict:
    snapshot = {
        "condition": "sunny",
        "cloud_coverage_pct": 42.0,
        "temperature_c": 18.0,
        "precipitation_mm": 0.0,
        "wind_speed_mps": 3.0,
        "wind_bearing_deg": 180.0,
        "humidity_pct": 55.0,
        "is_daylight": True,
    }
    snapshot.update(overrides)
    return snapshot


def test_model_persists_buckets_and_partial_day_state():
    model = _make_model()
    model._buckets[3][10].factor = 0.91
    model._buckets[3][10].samples = 7
    model._today_date = "2026-03-25"
    model._today_actual_kwh_by_hour = {10: 0.8}
    model._today_raw_forecast_kwh_by_hour = {10: 1.0}
    model._finalized_hours = {9}
    model._today_sunrise = datetime(2026, 3, 25, 7, 0)
    model._today_sunset = datetime(2026, 3, 25, 18, 0)

    run(model.async_save())

    loaded = _make_model()
    run(loaded.async_load())

    assert loaded._buckets[3][10].factor == 0.91
    assert loaded._buckets[3][10].samples == 7
    assert loaded._today_date == "2026-03-25"
    assert loaded._today_actual_kwh_by_hour[10] == 0.8
    assert loaded._today_raw_forecast_kwh_by_hour[10] == 1.0
    assert 9 in loaded._finalized_hours
    assert loaded._today_sunrise == datetime(2026, 3, 25, 7, 0)
    assert loaded._today_sunset == datetime(2026, 3, 25, 18, 0)


def test_model_finalizes_hour_and_learns_factor_in_daylight():
    model = _make_model()
    sunrise = datetime(2026, 3, 25, 8, 0)
    sunset = datetime(2026, 3, 25, 18, 0)

    model.update(
        now=datetime(2026, 3, 25, 10, 30),
        pv_power_w=1000.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(),
        solar_elevation=34.0,
        solar_azimuth=145.0,
    )
    model.update(
        now=datetime(2026, 3, 25, 11, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(),
        solar_elevation=28.0,
        solar_azimuth=160.0,
    )

    bucket = model._buckets[3][10]
    assert bucket.samples == 1
    assert round(bucket.factor, 3) == 1.0


def test_model_ignores_night_hours_even_if_energy_exists():
    model = _make_model()
    sunrise = datetime(2026, 3, 25, 8, 0)
    sunset = datetime(2026, 3, 25, 18, 0)

    model.update(
        now=datetime(2026, 3, 25, 2, 30),
        pv_power_w=1000.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 2, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(),
        solar_elevation=-12.0,
        solar_azimuth=20.0,
    )
    model.update(
        now=datetime(2026, 3, 25, 3, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 2, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(),
        solar_elevation=-10.0,
        solar_azimuth=25.0,
    )

    assert model._buckets[3][2].samples == 0


def test_snapshot_reports_learning_state_after_five_buckets():
    model = _make_model()
    bucket = model._buckets[3][10]
    bucket.factor = 0.8
    bucket.samples = 5

    snapshot = model.build_snapshot(
        now=datetime(2026, 3, 25, 10, 15),
        hourly_forecast=[
            _forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0),
            _forecast_slot(datetime(2026, 3, 25, 11, 0), 0.5),
        ],
        current_environment={
            "month": 3,
            "solar_elevation_bucket": 30,
            "solar_azimuth_bucket": 120,
            "cloud_coverage_bucket": 40,
        },
    )

    assert snapshot.state == "learning"
    assert snapshot.current_month == 3
    assert snapshot.active_buckets == 1
    assert snapshot.confident_buckets == 0
    assert snapshot.current_hour_samples == 5
    assert snapshot.current_hour_factor < 1.0
    assert "10:00" in snapshot.today_hourly_factors


def test_rollover_finalizes_previous_day_with_previous_day_sun_window():
    model = _make_model()
    previous_sunrise = datetime(2026, 3, 25, 8, 0)
    previous_sunset = datetime(2026, 3, 25, 18, 0)
    new_sunrise = datetime(2026, 3, 26, 11, 0)
    new_sunset = datetime(2026, 3, 26, 12, 0)

    model.update(
        now=datetime(2026, 3, 25, 10, 30),
        pv_power_w=1000.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=previous_sunrise,
        sunset=previous_sunset,
        weather_snapshot=_weather(),
        solar_elevation=32.0,
        solar_azimuth=150.0,
    )

    model.update(
        now=datetime(2026, 3, 26, 0, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[],
        sunrise=new_sunrise,
        sunset=new_sunset,
        weather_snapshot=_weather(),
        solar_elevation=-18.0,
        solar_azimuth=300.0,
    )

    bucket = model._buckets[3][10]
    assert bucket.samples == 1
    assert round(bucket.factor, 3) == 1.0


def test_model_learns_environment_context_bucket_and_exposes_snapshot():
    model = _make_model()
    sunrise = datetime(2026, 3, 25, 8, 0)
    sunset = datetime(2026, 3, 25, 18, 0)

    model.update(
        now=datetime(2026, 3, 25, 10, 30),
        pv_power_w=800.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(cloud_coverage_pct=45.0, temperature_c=21.0),
        solar_elevation=33.0,
        solar_azimuth=141.0,
    )
    model.update(
        now=datetime(2026, 3, 25, 11, 1),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(cloud_coverage_pct=50.0, temperature_c=20.0),
        solar_elevation=28.0,
        solar_azimuth=165.0,
    )

    snapshot = model.build_snapshot(
        now=datetime(2026, 3, 25, 10, 45),
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        current_environment={
            "month": 3,
            "solar_elevation_bucket": 30,
            "solar_azimuth_bucket": 120,
            "cloud_coverage_bucket": 40,
        },
    )

    assert model._context_buckets["m3|e30|a120|c40"].samples == 1
    assert snapshot.current_context_key == "m3|e30|a120|c40"
    assert snapshot.last_environment["solar_elevation_bucket"] == 30
