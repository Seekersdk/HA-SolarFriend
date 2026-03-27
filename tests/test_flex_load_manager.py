"""Unit tests for flex-load reservations."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import types
from datetime import datetime, timedelta


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    _mock("homeassistant.helpers")
    _mock("homeassistant.core", HomeAssistant=type("HomeAssistant", (), {}), Event=type("Event", (), {}), callback=lambda f: f)
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))
    _mock(
        "homeassistant.const",
        Platform=type("Platform", (), {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select", "BUTTON": "button"}),
    )
    _mock("homeassistant.helpers.entity_registry", async_get=lambda hass: type("Registry", (), {"entities": {}, "async_remove": lambda self, eid: None})())
    _mock("homeassistant.util")
    _mock("homeassistant.util.dt", as_local=lambda dt: dt)


class _FakeStore:
    def __init__(self) -> None:
        self._data: dict | None = None

    async def async_load(self):
        return self._data

    async def async_save(self, data: dict):
        import copy

        self._data = copy.deepcopy(data)


_store = _FakeStore()
storage_mod = types.ModuleType("homeassistant.helpers.storage")
storage_mod.Store = lambda hass, version, key: _store
sys.modules["homeassistant.helpers.storage"] = storage_mod


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_module_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "custom_components",
    "solarfriend",
    "flex_load_manager.py",
)
_spec = importlib.util.spec_from_file_location("solarfriend_flex_load_manager_test", _module_path)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _module
_spec.loader.exec_module(_module)
FlexLoadReservationManager = _module.FlexLoadReservationManager


class _Profile:
    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        return 500.0


def run(coro):
    return asyncio.run(coro)


def _forecast_for(base: datetime) -> list[dict]:
    return [
        {"period_start": base.replace(hour=9), "pv_estimate_kwh": 3.0},
        {"period_start": base.replace(hour=10), "pv_estimate_kwh": 3.2},
        {"period_start": base.replace(hour=11), "pv_estimate_kwh": 2.8},
    ]


def _prices_for(base: datetime) -> list[dict]:
    return [
        {"start": base.replace(hour=8), "price": 2.0},
        {"start": base.replace(hour=9), "price": 1.5},
        {"start": base.replace(hour=10), "price": 0.8},
        {"start": base.replace(hour=11), "price": 0.9},
        {"start": base.replace(hour=12), "price": 1.1},
    ]


def test_book_flex_load_prefers_earliest_solar_slot():
    manager = FlexLoadReservationManager(object(), "entry-1")
    now = datetime(2026, 3, 27, 20, 15, 0)
    tomorrow = now + timedelta(days=1)

    response = manager.upsert(
        now=now,
        job_id="dishwasher",
        name="Dishwasher",
        energy_wh=2000.0,
        power_w=800.0,
        duration_minutes=150,
        earliest_start=tomorrow.replace(hour=8, minute=0),
        deadline=tomorrow.replace(hour=14, minute=0),
        preferred_source="solar",
        min_solar_w=1500.0,
        max_grid_w=300.0,
        allow_battery=False,
        hourly_forecast=_forecast_for(tomorrow),
        raw_prices=_prices_for(tomorrow),
        consumption_profile=_Profile(),
    )

    assert response["operation"] == "created"
    assert response["job_id"] == "dishwasher"
    assert response["start_time"].startswith("2026-03-28T09:00:00")
    assert response["expected_solar_kwh"] > 0
    assert response["expected_grid_kwh"] <= 0.75


def test_book_flex_load_same_job_id_updates_existing_reservation():
    manager = FlexLoadReservationManager(object(), "entry-1")
    now = datetime(2026, 3, 27, 20, 15, 0)
    tomorrow = now + timedelta(days=1)

    manager.upsert(
        now=now,
        job_id="dishwasher",
        name="Dishwasher",
        energy_wh=2000.0,
        power_w=800.0,
        duration_minutes=150,
        earliest_start=tomorrow.replace(hour=8, minute=0),
        deadline=tomorrow.replace(hour=14, minute=0),
        preferred_source="solar",
        min_solar_w=1500.0,
        max_grid_w=300.0,
        allow_battery=False,
        hourly_forecast=_forecast_for(tomorrow),
        raw_prices=_prices_for(tomorrow),
        consumption_profile=_Profile(),
    )

    updated = manager.upsert(
        now=now,
        job_id="dishwasher",
        name="Dishwasher",
        energy_wh=2400.0,
        power_w=960.0,
        duration_minutes=150,
        earliest_start=tomorrow.replace(hour=10, minute=0),
        deadline=tomorrow.replace(hour=16, minute=0),
        preferred_source="cheap",
        min_solar_w=None,
        max_grid_w=None,
        allow_battery=False,
        hourly_forecast=_forecast_for(tomorrow),
        raw_prices=_prices_for(tomorrow),
        consumption_profile=_Profile(),
    )

    assert updated["operation"] == "updated"
    assert len(manager.reservations) == 1
    assert manager.reservations["dishwasher"].preferred_source == "cheap"

    reserved = manager.reserved_solar_kwh_by_hour(now)
    assert reserved


def test_book_flex_load_cheap_rejects_slots_that_need_grid_without_prices():
    manager = FlexLoadReservationManager(object(), "entry-1")
    now = datetime(2026, 3, 27, 20, 15, 0)
    tomorrow = now + timedelta(days=1)

    response = manager.upsert(
        now=now,
        job_id="washing_machine",
        name="Washing Machine",
        energy_wh=2000.0,
        power_w=1000.0,
        duration_minutes=120,
        earliest_start=tomorrow.replace(hour=8, minute=0),
        deadline=tomorrow.replace(hour=12, minute=0),
        preferred_source="cheap",
        min_solar_w=None,
        max_grid_w=None,
        allow_battery=False,
        hourly_forecast=[],
        raw_prices=[
            {"start": tomorrow.replace(hour=10), "price": 0.8},
            {"start": tomorrow.replace(hour=11), "price": 0.9},
        ],
        consumption_profile=_Profile(),
    )

    assert response["start_time"].startswith("2026-03-28T10:00:00")


def test_book_flex_load_cheap_raises_when_all_grid_slots_have_missing_prices():
    manager = FlexLoadReservationManager(object(), "entry-1")
    now = datetime(2026, 3, 27, 20, 15, 0)
    tomorrow = now + timedelta(days=1)

    try:
        manager.upsert(
            now=now,
            job_id="washing_machine",
            name="Washing Machine",
            energy_wh=2000.0,
            power_w=1000.0,
            duration_minutes=120,
            earliest_start=tomorrow.replace(hour=8, minute=0),
            deadline=tomorrow.replace(hour=12, minute=0),
            preferred_source="cheap",
            min_solar_w=None,
            max_grid_w=None,
            allow_battery=False,
            hourly_forecast=[],
            raw_prices=[{"start": tomorrow.replace(hour=10), "price": 0.8}],
            consumption_profile=_Profile(),
        )
        assert False, "Expected ValueError when no full priced grid slot exists"
    except ValueError as exc:
        assert str(exc) == "No valid flex-load slot found"


def test_book_flex_load_respects_half_hour_reservations_within_same_hour():
    manager = FlexLoadReservationManager(object(), "entry-1")
    now = datetime(2026, 3, 27, 20, 15, 0)
    tomorrow = now + timedelta(days=1)
    forecast = [
        {"period_start": tomorrow.replace(hour=9), "pv_estimate_kwh": 2.0},
        {"period_start": tomorrow.replace(hour=10), "pv_estimate_kwh": 2.0},
    ]
    prices = [
        {"start": tomorrow.replace(hour=9), "price": 1.0},
        {"start": tomorrow.replace(hour=10), "price": 1.0},
    ]

    first = manager.upsert(
        now=now,
        job_id="job_1",
        name="Job 1",
        energy_wh=500.0,
        power_w=1000.0,
        duration_minutes=30,
        earliest_start=tomorrow.replace(hour=9, minute=0),
        deadline=tomorrow.replace(hour=11, minute=0),
        preferred_source="solar",
        min_solar_w=900.0,
        max_grid_w=0.0,
        allow_battery=False,
        hourly_forecast=forecast,
        raw_prices=prices,
        consumption_profile=_Profile(),
    )

    second = manager.upsert(
        now=now,
        job_id="job_2",
        name="Job 2",
        energy_wh=500.0,
        power_w=1000.0,
        duration_minutes=30,
        earliest_start=tomorrow.replace(hour=9, minute=0),
        deadline=tomorrow.replace(hour=11, minute=0),
        preferred_source="solar",
        min_solar_w=900.0,
        max_grid_w=0.0,
        allow_battery=False,
        hourly_forecast=forecast,
        raw_prices=prices,
        consumption_profile=_Profile(),
    )

    assert first["start_time"].startswith("2026-03-28T09:00:00")
    assert second["start_time"].startswith("2026-03-28T09:30:00")
