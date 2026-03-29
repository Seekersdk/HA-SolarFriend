"""Unit tests for BatteryTracker persistence and midnight reset."""
from __future__ import annotations

import asyncio
import os
import sys
import types
from datetime import date, datetime, timedelta

import pytest


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    _mock(
        "homeassistant.core",
        HomeAssistant=type("HomeAssistant", (), {}),
        Event=type("Event", (), {}),
        callback=lambda func: func,
    )
    _mock("homeassistant.helpers")
    _mock("homeassistant.helpers.event", async_track_state_change_event=lambda *a, **kw: None)
    _duc = type(
        "DataUpdateCoordinator",
        (),
        {"__class_getitem__": classmethod(lambda cls, item: cls)},
    )
    _ce = type(
        "CoordinatorEntity",
        (),
        {"__class_getitem__": classmethod(lambda cls, item: cls)},
    )
    _mock(
        "homeassistant.helpers.update_coordinator",
        DataUpdateCoordinator=_duc,
        UpdateFailed=Exception,
        CoordinatorEntity=_ce,
    )
    _mock("homeassistant.helpers.device_registry", DeviceInfo=dict)
    _mock(
        "homeassistant.helpers.entity_registry",
        async_get=lambda hass: type(
            "Registry",
            (),
            {"entities": {}, "async_remove": lambda self, eid: None},
        )(),
    )
    _mock(
        "homeassistant.helpers.entity_platform",
        AddEntitiesCallback=type("AddEntitiesCallback", (), {}),
    )
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))
    _mock(
        "homeassistant.const",
        Platform=type(
            "Platform",
            (),
            {
                "SENSOR": "sensor",
                "NUMBER": "number",
                "SWITCH": "switch",
                "SELECT": "select",
            },
        ),
        CONF_NAME="name",
        UnitOfEnergy=type("UnitOfEnergy", (), {"KILO_WATT_HOUR": "kWh", "WATT_HOUR": "Wh"}),
        UnitOfPower=type("UnitOfPower", (), {"WATT": "W"}),
        PERCENTAGE="%",
    )
    _mock("homeassistant.components")
    _mock(
        "homeassistant.components.sensor",
        SensorEntity=type("SensorEntity", (), {}),
        SensorEntityDescription=type("SensorEntityDescription", (), {"__init__": lambda self, **kw: None}),
        SensorDeviceClass=type(
            "SensorDeviceClass",
            (),
            {"ENERGY": "energy", "POWER": "power", "BATTERY": "battery"},
        ),
        SensorStateClass=type(
            "SensorStateClass",
            (),
            {"MEASUREMENT": "measurement", "TOTAL": "total", "TOTAL_INCREASING": "total_increasing"},
        ),
    )
    _mock("homeassistant.util")
    _mock("homeassistant.util.dt", now=datetime.now, as_local=lambda dt: dt, UTC=None)


class _FakeStore:
    def __init__(self):
        self._data: dict | None = None

    async def async_load(self):
        return self._data

    async def async_save(self, data: dict):
        import copy

        self._data = copy.deepcopy(data)


def _make_store_module():
    stores: dict[str, _FakeStore] = {}

    def _factory(hass, version, key):
        return stores.setdefault(key, _FakeStore())

    mod = types.ModuleType("homeassistant.helpers.storage")
    mod.Store = _factory
    sys.modules["homeassistant.helpers.storage"] = mod
    return stores


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_stores = _make_store_module()

import custom_components.solarfriend.battery_tracker as _bt_module  # noqa: E402

_bt_module.Store = lambda hass, version, key: _stores.setdefault(key, _FakeStore())
from custom_components.solarfriend.battery_tracker import BatteryTracker  # noqa: E402


def run(coro):
    return asyncio.run(coro)


def _clear_stores() -> None:
    _stores.clear()


def _make_tracker(battery_cost: float = 0.25) -> BatteryTracker:
    return BatteryTracker(hass=object(), entry_id="test_entry", battery_cost_per_kwh=battery_cost)


def pytest_approx(value, rel=1e-4):
    return value


def test_battery_tracker_all_4_fields_persist():
    _clear_stores()
    tracker = _make_tracker()
    tracker.today_solar_direct_saved_dkk = 3.50
    tracker.today_optimizer_saved_dkk = 1.20
    tracker.total_solar_direct_saved_dkk = 42.00
    tracker.total_optimizer_saved_dkk = 18.75
    tracker._last_reset_date = "2026-03-22"

    run(tracker.async_save())

    restored = _make_tracker()
    run(restored.async_load())

    assert restored.today_solar_direct_saved_dkk == pytest_approx(3.50)
    assert restored.today_optimizer_saved_dkk == pytest_approx(1.20)
    assert restored.total_solar_direct_saved_dkk == pytest_approx(42.00)
    assert restored.total_optimizer_saved_dkk == pytest_approx(18.75)
    assert restored._last_reset_date == "2026-03-22"


def test_battery_sell_fields_persist():
    _clear_stores()
    tracker = _make_tracker()
    tracker.today_battery_sell_kwh = 3.25
    tracker.today_battery_sell_saved_dkk = 4.75
    tracker.total_battery_sell_saved_dkk = 21.5
    tracker._last_reset_date = "2026-03-22"

    run(tracker.async_save())

    restored = _make_tracker()
    run(restored.async_load())

    assert restored.today_battery_sell_kwh == pytest_approx(3.25)
    assert restored.today_battery_sell_saved_dkk == pytest_approx(4.75)
    assert restored.total_battery_sell_saved_dkk == pytest_approx(21.5)


def test_async_save_writes_backup_copy():
    _clear_stores()
    tracker = _make_tracker()
    tracker.total_optimizer_saved_dkk = 12.5

    run(tracker.async_save())

    assert _stores[_bt_module.STORAGE_KEY]._data["total_optimizer_saved_dkk"] == pytest_approx(12.5)
    assert _stores[_bt_module.BACKUP_STORAGE_KEY]._data["total_optimizer_saved_dkk"] == pytest_approx(12.5)


def test_async_load_recovers_from_backup_when_primary_is_empty():
    _clear_stores()
    _stores[_bt_module.BACKUP_STORAGE_KEY] = _FakeStore()
    _stores[_bt_module.BACKUP_STORAGE_KEY]._data = {
        "solar_kwh": 1.0,
        "grid_kwh": 0.0,
        "grid_avg_cost": 0.0,
        "today_solar_direct_kwh": 0.0,
        "today_solar_direct_saved_dkk": 0.0,
        "today_optimizer_saved_dkk": 1.2,
        "total_solar_direct_saved_dkk": 10.0,
        "total_optimizer_saved_dkk": 4.5,
        "today_battery_sell_kwh": 0.0,
        "today_battery_sell_saved_dkk": 0.0,
        "total_battery_sell_saved_dkk": 2.0,
        "last_reset_date": "2026-03-28",
    }

    tracker = _make_tracker()
    run(tracker.async_load())

    assert tracker.total_optimizer_saved_dkk == pytest_approx(4.5)
    assert _stores[_bt_module.STORAGE_KEY]._data["total_optimizer_saved_dkk"] == pytest_approx(4.5)


def test_async_load_recovers_from_backup_when_primary_load_raises():
    _clear_stores()

    class _BrokenStore(_FakeStore):
        async def async_load(self):
            raise ValueError("corrupt json")

    _stores[_bt_module.STORAGE_KEY] = _BrokenStore()
    _stores[_bt_module.BACKUP_STORAGE_KEY] = _FakeStore()
    _stores[_bt_module.BACKUP_STORAGE_KEY]._data = {
        "solar_kwh": 0.5,
        "grid_kwh": 0.0,
        "grid_avg_cost": 0.0,
        "today_solar_direct_kwh": 0.0,
        "today_solar_direct_saved_dkk": 0.0,
        "today_optimizer_saved_dkk": 0.3,
        "total_solar_direct_saved_dkk": 1.0,
        "total_optimizer_saved_dkk": 8.75,
        "today_battery_sell_kwh": 0.0,
        "today_battery_sell_saved_dkk": 0.0,
        "total_battery_sell_saved_dkk": 0.0,
        "last_reset_date": "2026-03-28",
    }

    tracker = _make_tracker()
    run(tracker.async_load())

    assert tracker.total_optimizer_saved_dkk == pytest_approx(8.75)


def test_midnight_reset_accumulates_to_total():
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    tracker = _make_tracker()
    tracker.today_solar_direct_saved_dkk = 5.0
    tracker.today_optimizer_saved_dkk = 2.5
    tracker.total_solar_direct_saved_dkk = 10.0
    tracker.total_optimizer_saved_dkk = 4.0
    tracker._last_reset_date = yesterday

    tracker._check_midnight_reset()

    assert tracker.total_solar_direct_saved_dkk == 15.0
    assert tracker.total_optimizer_saved_dkk == 6.5
    assert tracker.today_solar_direct_saved_dkk == 0.0
    assert tracker.today_optimizer_saved_dkk == 0.0
    assert tracker._last_reset_date == date.today().isoformat()


def test_no_reset_same_day():
    today = date.today().isoformat()

    tracker = _make_tracker()
    tracker.today_solar_direct_saved_dkk = 5.0
    tracker.today_optimizer_saved_dkk = 2.5
    tracker.total_solar_direct_saved_dkk = 10.0
    tracker.total_optimizer_saved_dkk = 4.0
    tracker._last_reset_date = today

    tracker._check_midnight_reset()

    assert tracker.today_solar_direct_saved_dkk == 5.0
    assert tracker.today_optimizer_saved_dkk == 2.5
    assert tracker.total_solar_direct_saved_dkk == 10.0
    assert tracker.total_optimizer_saved_dkk == 4.0


def test_no_reset_on_first_startup():
    tracker = _make_tracker()
    tracker.today_solar_direct_saved_dkk = 3.0
    tracker.total_solar_direct_saved_dkk = 0.0
    tracker._last_reset_date = ""

    tracker._check_midnight_reset()

    assert tracker.total_solar_direct_saved_dkk == 0.0
    assert tracker.today_solar_direct_saved_dkk == 3.0
    assert tracker._last_reset_date == date.today().isoformat()


def test_live_total_properties_include_today_values():
    tracker = _make_tracker()
    tracker.today_solar_direct_saved_dkk = 3.0
    tracker.today_optimizer_saved_dkk = 1.5
    tracker.total_solar_direct_saved_dkk = 10.0
    tracker.total_optimizer_saved_dkk = 4.0

    assert tracker.live_total_solar_saved_dkk == 13.0
    assert tracker.live_total_optimizer_saved_dkk == 5.5


def test_live_total_battery_sell_property_includes_today_values():
    tracker = _make_tracker()
    tracker.today_battery_sell_saved_dkk = 2.0
    tracker.total_battery_sell_saved_dkk = 8.5

    assert tracker.live_total_battery_sell_saved_dkk == 10.5


def test_update_savings_reports_changes():
    tracker = _make_tracker()
    tracker._last_reset_date = date.today().isoformat()

    changed = tracker.update_savings(
        pv_w=2000.0,
        load_w=1000.0,
        battery_w=500.0,
        price_dkk=1.0,
        dt_seconds=3600.0,
    )

    assert changed is True
    assert tracker.today_solar_direct_saved_dkk == 1.0


def test_update_battery_sell_savings_accumulates_value():
    tracker = _make_tracker()
    tracker._last_reset_date = date.today().isoformat()

    changed = tracker.update_battery_sell_savings(
        battery_w=2000.0,
        sell_price_dkk=1.5,
        dt_seconds=3600.0,
    )

    assert changed is True
    assert tracker.today_battery_sell_kwh == 2.0
    assert tracker.today_battery_sell_saved_dkk == 3.0


def test_weighted_cost_for_solar_energy_includes_full_battery_cost():
    tracker = _make_tracker(battery_cost=0.204)
    tracker.on_solar_charge(2.0)

    assert tracker.weighted_cost == pytest.approx(0.204, rel=1e-6)


def test_weighted_cost_for_grid_energy_includes_grid_price_plus_full_battery_cost():
    tracker = _make_tracker(battery_cost=0.204)
    tracker.on_grid_charge(2.0, grid_price=0.50)

    assert tracker.grid_avg_cost == pytest.approx(0.704, rel=1e-6)
    assert tracker.weighted_cost == pytest.approx(0.704, rel=1e-6)
