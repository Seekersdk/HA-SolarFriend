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
    )
    model.update(
        now=datetime(2026, 3, 25, 11, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 10, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
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
    )
    model.update(
        now=datetime(2026, 3, 25, 3, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[_forecast_slot(datetime(2026, 3, 25, 2, 0), 1.0)],
        sunrise=sunrise,
        sunset=sunset,
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
    )

    model.update(
        now=datetime(2026, 3, 26, 0, 5),
        pv_power_w=0.0,
        dt_seconds=0.0,
        hourly_forecast=[],
        sunrise=new_sunrise,
        sunset=new_sunset,
    )

    bucket = model._buckets[3][10]
    assert bucket.samples == 1
    assert round(bucket.factor, 3) == 1.0
