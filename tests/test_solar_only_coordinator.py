"""Coordinator-level tests for Solar Only weather hysteresis."""
from __future__ import annotations

import asyncio
import os
import sys
import types
from datetime import datetime, timedelta


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    _mock(
        "homeassistant.core",
        HomeAssistant=type("HomeAssistant", (), {}),
        Event=type("Event", (), {}),
        callback=lambda f: f,
    )
    _mock("homeassistant.helpers")
    _mock("homeassistant.helpers.storage", Store=type("Store", (), {}))
    _mock("homeassistant.helpers.event", async_track_state_change_event=lambda *a, **kw: None)
    _DUC = type("DataUpdateCoordinator", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
    _CE = type("CoordinatorEntity", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
    _mock(
        "homeassistant.helpers.update_coordinator",
        DataUpdateCoordinator=_DUC,
        UpdateFailed=Exception,
        CoordinatorEntity=_CE,
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
    _mock("homeassistant.helpers.entity_platform", AddEntitiesCallback=type("AddEntitiesCallback", (), {}))
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))
    _mock(
        "homeassistant.const",
        Platform=type(
            "Platform",
            (),
            {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select"},
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


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from custom_components.solarfriend.coordinator import SolarFriendCoordinator  # noqa: E402
from custom_components.solarfriend.ev_optimizer import EVContext, EVOptimizeResult, EVOptimizer  # noqa: E402
from custom_components.solarfriend.weather_profile import CLOUDY_PROFILE, PARTLY_CLOUDY_PROFILE  # noqa: E402
import custom_components.solarfriend.coordinator as coordinator_mod  # noqa: E402


def _make_coordinator() -> SolarFriendCoordinator:
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator._ev_optimizer = EVOptimizer()
    coordinator._ev_solar_start_candidate_since = None
    coordinator._ev_solar_stop_candidate_since = None
    coordinator._weather_hourly_forecast = []
    coordinator._weather_forecast_fetched_at = None
    coordinator._entry = types.SimpleNamespace(data={"weather_entity": "weather.forecast_hjem"})
    coordinator.hass = types.SimpleNamespace(
        services=types.SimpleNamespace(async_call=None),
    )
    return coordinator


def _make_ctx(**overrides) -> EVContext:
    defaults = dict(
        pv_power_w=4000.0,
        load_power_w=1000.0,
        grid_power_w=0.0,
        battery_charging_w=0.0,
        battery_soc=50.0,
        battery_capacity_kwh=10.0,
        battery_min_soc=10.0,
        charger_status="connected",
        currently_charging=False,
        vehicle_soc=50.0,
        vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0,
        departure=datetime(2026, 3, 27, 7, 0),
        current_price=1.0,
        raw_prices=[],
        max_charge_kw=11.0,
        now=datetime(2026, 3, 26, 12, 0, 0),
    )
    defaults.update(overrides)
    return EVContext(**defaults)


def _make_result(**overrides) -> EVOptimizeResult:
    defaults = dict(
        should_charge=True,
        target_w=3760.0,
        phases=1,
        target_amps=16.0,
        reason="Sol-overskud",
        surplus_w=2000.0,
        charger_status="connected",
    )
    defaults.update(overrides)
    return EVOptimizeResult(**defaults)


def test_solar_only_start_hysteresis_requires_full_hold_time():
    coordinator = _make_coordinator()
    start_time = datetime(2026, 3, 26, 12, 0, 0)

    blocked = coordinator._apply_solar_only_hysteresis(
        ctx=_make_ctx(now=start_time),
        result=_make_result(should_charge=True, surplus_w=2100.0),
        profile=PARTLY_CLOUDY_PROFILE,
        actual_charging=False,
    )

    assert blocked.should_charge is False
    assert blocked.target_w == 0.0
    assert "Start hysterese" in blocked.reason

    allowed = coordinator._apply_solar_only_hysteresis(
        ctx=_make_ctx(now=start_time + timedelta(minutes=5)),
        result=_make_result(should_charge=True, surplus_w=2100.0),
        profile=PARTLY_CLOUDY_PROFILE,
        actual_charging=False,
    )

    assert allowed.should_charge is True
    assert "Delvist skyet:" in allowed.reason
    assert "Start hysterese" not in allowed.reason


def test_solar_only_stop_hysteresis_holds_charging_with_buffer():
    coordinator = _make_coordinator()
    stop_time = datetime(2026, 3, 26, 12, 0, 0)

    held = coordinator._apply_solar_only_hysteresis(
        ctx=_make_ctx(now=stop_time, currently_charging=True),
        result=_make_result(
            should_charge=False,
            target_w=0.0,
            phases=0,
            target_amps=0.0,
            reason="For lidt sol-overskud",
            surplus_w=1100.0,
        ),
        profile=PARTLY_CLOUDY_PROFILE,
        actual_charging=True,
    )

    assert held.should_charge is True
    assert held.target_w >= 1410.0
    assert "Stop hysterese" in held.reason

    stopped = coordinator._apply_solar_only_hysteresis(
        ctx=_make_ctx(now=stop_time + timedelta(minutes=10), currently_charging=True),
        result=_make_result(
            should_charge=False,
            target_w=0.0,
            phases=0,
            target_amps=0.0,
            reason="For lidt sol-overskud",
            surplus_w=1100.0,
        ),
        profile=PARTLY_CLOUDY_PROFILE,
        actual_charging=True,
    )

    assert stopped.should_charge is False
    assert "Delvist skyet:" in stopped.reason
    assert "Stop hysterese" not in stopped.reason


def test_fetch_weather_profile_uses_service_response_and_cache():
    coordinator = _make_coordinator()
    service_calls: list[tuple[str, str, dict]] = []
    now = datetime(2026, 3, 26, 12, 15, 0)

    async def _async_call(domain, service, data, blocking=True, return_response=False):
        service_calls.append((domain, service, data))
        return {
            "weather.forecast_hjem": {
                "forecast": [
                    {
                        "datetime": "2026-03-26T12:00:00",
                        "condition": "partlycloudy",
                        "cloud_coverage": 82.0,
                    },
                    {
                        "datetime": "2026-03-26T13:00:00",
                        "condition": "sunny",
                        "cloud_coverage": 5.0,
                    },
                ]
            }
        }

    coordinator.hass.services.async_call = _async_call
    original_now = coordinator_mod.ha_dt.now
    coordinator_mod.ha_dt.now = lambda: now
    try:
        profile = asyncio.run(coordinator._get_current_solar_only_profile(now))
        cached = asyncio.run(coordinator._fetch_weather_hourly_forecast())
    finally:
        coordinator_mod.ha_dt.now = original_now

    assert profile == CLOUDY_PROFILE
    assert len(cached) == 2
    assert len(service_calls) == 1
