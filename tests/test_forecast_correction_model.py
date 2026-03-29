"""Unit tests for season/elevation/azimuth forecast correction model."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import types
from datetime import datetime, timezone


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
    _mock(
        "homeassistant.const",
        Platform=type(
            "Platform",
            (),
            {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select"},
        ),
    )
    _mock("homeassistant.util")
    _mock(
        "homeassistant.util.dt",
        as_local=lambda dt: dt,
        now=datetime.now,
        UTC=timezone.utc,
        DEFAULT_TIME_ZONE=timezone.utc,
    )


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
GeometryBucket = _fcm.GeometryBucket
TemperatureBucket = _fcm.TemperatureBucket


def run(coro):
    return asyncio.run(coro)


def _forecast_slot(dt: datetime, kwh: float) -> dict:
    return {"period_start": dt.isoformat(), "pv_estimate_kwh": kwh}


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


def _make_model() -> ForecastCorrectionModel:
    _store._data = None
    return ForecastCorrectionModel(hass=object(), entry_id="test")


def test_model_persists_geometry_temperature_and_partial_day_state():
    model = _make_model()
    model._geometry_buckets["s1|e30|a120"] = GeometryBucket(factor=0.91, samples=7, avg_abs_error_kwh=0.08)
    model._temperature_buckets["s1|t20"] = TemperatureBucket(factor=1.04, samples=6, avg_abs_error_kwh=0.05)
    model._today_date = "2026-03-25"
    model._today_actual_kwh_by_hour = {10: 0.8}
    model._today_raw_forecast_kwh_by_hour = {10: 1.0}
    model._today_context_by_hour = {10: {"season_bucket": 1, "solar_elevation_bucket": 30, "solar_azimuth_bucket": 120}}
    model._finalized_hours = {9}
    model._today_sunrise = datetime(2026, 3, 25, 7, 0, tzinfo=timezone.utc)
    model._today_sunset = datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc)

    run(model.async_save())

    loaded = ForecastCorrectionModel(hass=object(), entry_id="test")
    run(loaded.async_load())

    assert loaded._geometry_buckets["s1|e30|a120"].factor == 0.91
    assert loaded._geometry_buckets["s1|e30|a120"].samples == 7
    assert loaded._temperature_buckets["s1|t20"].factor == 1.04
    assert loaded._temperature_buckets["s1|t20"].samples == 6
    assert loaded._today_date == "2026-03-25"
    assert loaded._today_actual_kwh_by_hour[10] == 0.8
    assert loaded._today_raw_forecast_kwh_by_hour[10] == 1.0
    assert loaded._today_context_by_hour[10]["solar_elevation_bucket"] == 30
    assert 9 in loaded._finalized_hours


def test_model_finalizes_hour_and_learns_geometry_bucket_in_daylight():
    model = _make_model()
    sunrise = datetime(2026, 3, 25, 8, 0, tzinfo=timezone.utc)
    sunset = datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc)

    model.update(
        now=datetime(2026, 3, 25, 10, 30),
        pv_power_w=1000.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
        weather_snapshot=_weather(temperature_c=20.0),
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
        weather_snapshot=_weather(temperature_c=20.0),
        solar_elevation=28.0,
        solar_azimuth=160.0,
    )

    geometry_bucket = model._geometry_buckets["s1|e30|a120"]
    assert geometry_bucket.samples == 1
    assert round(geometry_bucket.factor, 3) == 1.0
    assert model._temperature_buckets == {}


def test_model_ignores_night_hours_even_if_energy_exists():
    model = _make_model()
    sunrise = datetime(2026, 3, 25, 8, 0, tzinfo=timezone.utc)
    sunset = datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc)

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

    assert model._geometry_buckets == {}
    assert model._temperature_buckets == {}


def test_snapshot_reports_learning_state_after_five_geometry_samples():
    model = _make_model()
    model._geometry_buckets["s1|e30|a120"] = GeometryBucket(factor=0.8, samples=5, avg_abs_error_kwh=0.1)
    model._temperature_buckets["s1|t20"] = TemperatureBucket(factor=1.0, samples=5, avg_abs_error_kwh=0.1)

    snapshot = model.build_snapshot(
        now=datetime(2026, 3, 25, 10, 15),
        hourly_forecast=[
            _forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0),
            _forecast_slot(datetime(2026, 3, 25, 11, 0), 0.5),
        ],
        current_environment={
            "month": 3,
            "season_bucket": 1,
            "solar_elevation_bucket": 30,
            "solar_azimuth_bucket": 120,
            "temperature_bucket_c": 20,
        },
    )

    assert snapshot.state == "learning"
    assert snapshot.current_season == 1
    assert snapshot.active_buckets == 1
    assert snapshot.confident_buckets == 0
    assert snapshot.current_geometry_samples == 5
    assert snapshot.current_geometry_factor < 1.0
    assert snapshot.current_temperature_samples == 5
    assert "10:00" in snapshot.today_geometry_factors


def test_rollover_finalizes_previous_day_with_previous_day_sun_window():
    model = _make_model()
    previous_sunrise = datetime(2026, 3, 25, 8, 0, tzinfo=timezone.utc)
    previous_sunset = datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc)
    new_sunrise = datetime(2026, 3, 26, 11, 0, tzinfo=timezone.utc)
    new_sunset = datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)

    model.update(
        now=datetime(2026, 3, 25, 10, 30),
        pv_power_w=1000.0,
        dt_seconds=3600.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=previous_sunrise,
        sunset=previous_sunset,
        weather_snapshot=_weather(temperature_c=15.0),
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
        weather_snapshot=_weather(temperature_c=5.0),
        solar_elevation=-18.0,
        solar_azimuth=300.0,
    )

    bucket = model._geometry_buckets["s1|e30|a150"]
    assert bucket.samples == 1
    assert round(bucket.factor, 3) == 1.0


def test_corrected_forecast_combines_geometry_and_temperature_factors():
    model = _make_model()
    model._geometry_buckets["s1|e30|a120"] = GeometryBucket(factor=0.8, samples=8, avg_abs_error_kwh=0.1)
    model._temperature_buckets["s1|t20"] = TemperatureBucket(factor=0.9, samples=8, avg_abs_error_kwh=0.1)
    model._today_context_by_hour = {
        10: {
            "season_bucket": 1,
            "solar_elevation_bucket": 30,
            "solar_azimuth_bucket": 120,
        }
    }

    corrected = model.get_corrected_hourly_forecast(
        now=datetime(2026, 3, 25, 10, 15),
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        hourly_weather_forecast=[
            {"datetime": datetime(2026, 3, 25, 10, 0).isoformat(), "temperature": 20.0},
        ],
    )

    assert len(corrected) == 1
    assert round(corrected[0]["pv_estimate_kwh"], 4) < 1.0
    assert round(corrected[0]["pv_estimate_kwh"], 4) == round(0.8667 * 0.9333, 4)


def test_build_snapshot_exposes_temperature_diagnostics():
    model = _make_model()
    model._geometry_buckets["s1|e30|a120"] = GeometryBucket(factor=0.8, samples=8, avg_abs_error_kwh=0.1)
    model._temperature_buckets["s1|t20"] = TemperatureBucket(factor=0.9, samples=8, avg_abs_error_kwh=0.1)

    snapshot = model.build_snapshot(
        now=datetime(2026, 3, 25, 10, 15),
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        current_environment={
            "month": 3,
            "season_bucket": 1,
            "solar_elevation_bucket": 30,
            "solar_azimuth_bucket": 120,
            "temperature_bucket_c": 20,
        },
        hourly_weather_forecast=[
            {"datetime": datetime(2026, 3, 25, 10, 0).isoformat(), "temperature": 20.0},
        ],
    )

    assert snapshot.current_geometry_key == "s1|e30|a120"
    assert snapshot.current_temperature_key == "s1|t20"
    assert snapshot.current_temperature_samples == 8
    assert snapshot.today_geometry_factors["10:00"]["temperature_bucket_c"] == 20
    assert snapshot.today_geometry_factors["10:00"]["temperature_samples"] == 8
