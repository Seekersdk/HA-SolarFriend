"""Unit tests for EaseeController power/status normalization."""
from __future__ import annotations

import asyncio
import os
import sys
import types
from datetime import datetime


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
        callback=lambda f: f,
    )
    _mock("homeassistant.helpers")
    _mock("homeassistant.helpers.storage", Store=type("Store", (), {}))
    _mock("homeassistant.helpers.event", async_track_state_change_event=lambda *a, **kw: None)
    _duc = type("DataUpdateCoordinator", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
    _ce = type("CoordinatorEntity", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
    _mock(
        "homeassistant.helpers.update_coordinator",
        DataUpdateCoordinator=_duc,
        UpdateFailed=Exception,
        CoordinatorEntity=_ce,
    )
    _mock(
        "homeassistant.const",
        Platform=type("Platform", (), {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select"}),
        CONF_NAME="name",
        UnitOfEnergy=type("UnitOfEnergy", (), {"KILO_WATT_HOUR": "kWh", "WATT_HOUR": "Wh"}),
        UnitOfPower=type("UnitOfPower", (), {"WATT": "W"}),
        PERCENTAGE="%",
    )
    _mock(
        "homeassistant.helpers.entity_registry",
        async_get=lambda hass: type(
            "Registry",
            (),
            {"async_get": lambda self, eid: types.SimpleNamespace(device_id="device-1")},
        )(),
    )
    _mock("homeassistant.helpers.device_registry", DeviceInfo=dict)
    _mock("homeassistant.helpers.entity_platform", AddEntitiesCallback=type("AddEntitiesCallback", (), {}))
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))
    _mock("homeassistant.components")
    _mock(
        "homeassistant.components.sensor",
        SensorEntity=type("SensorEntity", (), {}),
        SensorEntityDescription=type("SensorEntityDescription", (), {"__init__": lambda self, **kw: None}),
        SensorDeviceClass=type("SensorDeviceClass", (), {"ENERGY": "energy", "POWER": "power", "BATTERY": "battery"}),
        SensorStateClass=type("SensorStateClass", (), {"MEASUREMENT": "measurement", "TOTAL": "total", "TOTAL_INCREASING": "total_increasing"}),
    )
    _mock("homeassistant.util")
    _mock("homeassistant.util.dt", now=lambda: datetime.now(), as_local=lambda dt: dt, UTC=None)

if "homeassistant.exceptions" not in sys.modules:
    _mock("homeassistant.exceptions", ServiceNotFound=Exception)


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from custom_components.solarfriend.easee_controller import EaseeController  # noqa: E402


class _StateStore:
    def __init__(self, mapping: dict[str, object]) -> None:
        self._mapping = mapping

    def get(self, entity_id: str):
        return self._mapping.get(entity_id)


def _make_state(state: str, *, unit: str = "") -> object:
    return types.SimpleNamespace(state=state, attributes={"unit_of_measurement": unit})


def _make_hass(state_map: dict[str, object]) -> object:
    services = types.SimpleNamespace(async_call=lambda *a, **kw: None)
    return types.SimpleNamespace(states=_StateStore(state_map), services=services)


def _make_entry() -> object:
    return types.SimpleNamespace(
        data={
            "ev_charger_status_entity": "sensor.easee_status",
            "ev_charger_power_entity": "sensor.easee_power",
            "ev_charger_id": "EHUT8C3W",
        }
    )


def test_get_power_w_converts_kw_to_w() -> None:
    hass = _make_hass(
        {
            "sensor.easee_status": _make_state("charging"),
            "sensor.easee_power": _make_state("2.804", unit="kW"),
        }
    )
    controller = EaseeController(hass, _make_entry())

    power_w = asyncio.run(controller.get_power_w())

    assert power_w == 2804.0


def test_get_power_w_keeps_w_unchanged() -> None:
    hass = _make_hass(
        {
            "sensor.easee_status": _make_state("charging"),
            "sensor.easee_power": _make_state("1410", unit="W"),
        }
    )
    controller = EaseeController(hass, _make_entry())

    power_w = asyncio.run(controller.get_power_w())

    assert power_w == 1410.0
