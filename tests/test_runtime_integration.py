"""Higher-level integration-style tests around coordinator/config-entry runtime flow.

These tests sit one level above the pure policy/unit tests:
- They drive the real coordinator methods with a fake Home Assistant object.
- They validate config-entry setup/unload behavior and runtime setting refreshes.
- They keep the HA boundary mocked, but preserve the integration's own wiring.
"""
from __future__ import annotations

import asyncio
import importlib
import os
import sys
import types
from datetime import datetime
from datetime import timedelta
from datetime import timezone


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    class _RestoreEntity:
        async def async_added_to_hass(self) -> None:
            return None

        async def async_get_last_state(self):
            return None

    _mock(
        "homeassistant.core",
        HomeAssistant=type("HomeAssistant", (), {}),
        Event=type("Event", (), {}),
        callback=lambda f: f,
        SupportsResponse=type("SupportsResponse", (), {"NONE": "none", "ONLY": "only"}),
    )
    _mock("homeassistant.helpers")
    _mock("homeassistant.helpers.storage", Store=type("Store", (), {}))
    _mock("homeassistant.helpers.event", async_track_state_change_event=lambda *a, **kw: None)
    _duc = type(
        "DataUpdateCoordinator",
        (),
        {
            "__class_getitem__": classmethod(lambda cls, item: cls),
            "__init__": lambda self, hass, logger, name=None, update_interval=None: (
                setattr(self, "hass", hass),
                setattr(self, "logger", logger),
                setattr(self, "name", name),
                setattr(self, "update_interval", update_interval),
                setattr(self, "data", None),
            ),
            "async_config_entry_first_refresh": lambda self: None,
            "async_request_refresh": lambda self: None,
        },
    )
    _ce = type("CoordinatorEntity", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
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
            {
                "entities": {},
                "async_remove": lambda self, eid: None,
            },
        )(),
    )
    _mock("homeassistant.helpers.entity_platform", AddEntitiesCallback=type("AddEntitiesCallback", (), {}))
    _mock(
        "homeassistant.helpers.restore_state",
        RestoreEntity=_RestoreEntity,
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
                "BUTTON": "button",
            },
        ),
        CONF_NAME="name",
        UnitOfEnergy=type("UnitOfEnergy", (), {"KILO_WATT_HOUR": "kWh", "WATT_HOUR": "Wh"}),
        UnitOfPower=type("UnitOfPower", (), {"WATT": "W", "KILO_WATT": "kW"}),
        PERCENTAGE="%",
    )
    _mock("homeassistant.components")
    _mock("homeassistant.util")
    _mock(
        "homeassistant.util.dt",
        now=lambda: datetime(2026, 3, 27, 12, 0, 0),
        as_local=lambda dt: dt,
        parse_datetime=lambda value: datetime.fromisoformat(str(value)),
        UTC=None,
    )

if "homeassistant.components" not in sys.modules:
    _mock("homeassistant.components")

if "homeassistant.core" not in sys.modules:
    _mock(
        "homeassistant.core",
        HomeAssistant=type("HomeAssistant", (), {}),
        Event=type("Event", (), {}),
        callback=lambda f: f,
    )
else:
    core_mod = sys.modules["homeassistant.core"]
    if not hasattr(core_mod, "HomeAssistant"):
        setattr(core_mod, "HomeAssistant", type("HomeAssistant", (), {}))
    if not hasattr(core_mod, "Event"):
        setattr(core_mod, "Event", type("Event", (), {}))
    if not hasattr(core_mod, "callback"):
        setattr(core_mod, "callback", lambda f: f)
    if not hasattr(core_mod, "SupportsResponse"):
        setattr(core_mod, "SupportsResponse", type("SupportsResponse", (), {"NONE": "none", "ONLY": "only"}))

if "homeassistant.config_entries" not in sys.modules:
    _mock("homeassistant.config_entries", ConfigEntry=type("ConfigEntry", (), {}))

if "homeassistant.const" not in sys.modules:
    _mock(
        "homeassistant.const",
        Platform=type(
            "Platform",
            (),
            {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select", "BUTTON": "button"},
        ),
        CONF_NAME="name",
        UnitOfEnergy=type("UnitOfEnergy", (), {"KILO_WATT_HOUR": "kWh", "WATT_HOUR": "Wh"}),
        UnitOfPower=type("UnitOfPower", (), {"WATT": "W", "KILO_WATT": "kW"}),
        PERCENTAGE="%",
    )

if "homeassistant.components.sensor" not in sys.modules:
    _mock(
        "homeassistant.components.sensor",
        SensorEntity=type("SensorEntity", (), {}),
        SensorEntityDescription=type("SensorEntityDescription", (), {"__init__": lambda self, **kw: None}),
        SensorDeviceClass=type("SensorDeviceClass", (), {"ENERGY": "energy", "POWER": "power", "BATTERY": "battery"}),
        SensorStateClass=type(
            "SensorStateClass",
            (),
            {"MEASUREMENT": "measurement", "TOTAL": "total", "TOTAL_INCREASING": "total_increasing"},
        ),
    )

if "homeassistant.components.switch" not in sys.modules:
    _mock("homeassistant.components.switch", SwitchEntity=type("SwitchEntity", (), {}))

if "homeassistant.components.select" not in sys.modules:
    _mock("homeassistant.components.select", SelectEntity=type("SelectEntity", (), {}))

if "homeassistant.helpers.event" not in sys.modules:
    _mock("homeassistant.helpers.event", async_track_state_change_event=lambda *a, **kw: None)

if "homeassistant.helpers.update_coordinator" not in sys.modules:
    _duc = type(
        "DataUpdateCoordinator",
        (),
        {
            "__class_getitem__": classmethod(lambda cls, item: cls),
            "__init__": lambda self, hass, logger, name=None, update_interval=None: (
                setattr(self, "hass", hass),
                setattr(self, "logger", logger),
                setattr(self, "name", name),
                setattr(self, "update_interval", update_interval),
                setattr(self, "data", None),
            ),
        },
    )
    _ce = type("CoordinatorEntity", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
    _mock(
        "homeassistant.helpers.update_coordinator",
        DataUpdateCoordinator=_duc,
        UpdateFailed=Exception,
        CoordinatorEntity=_ce,
    )

if "homeassistant.helpers.restore_state" not in sys.modules:
    class _RestoreEntityFallback:
        async def async_added_to_hass(self) -> None:
            return None

        async def async_get_last_state(self):
            return None

    _mock("homeassistant.helpers.restore_state", RestoreEntity=_RestoreEntityFallback)

if "homeassistant.helpers.entity_registry" not in sys.modules:
    _mock(
        "homeassistant.helpers.entity_registry",
        async_get=lambda hass: type("Registry", (), {"entities": {}, "async_remove": lambda self, eid: None})(),
    )

if "homeassistant.helpers.device_registry" not in sys.modules:
    _mock("homeassistant.helpers.device_registry", DeviceInfo=dict)

if "homeassistant.helpers.entity_platform" not in sys.modules:
    _mock("homeassistant.helpers.entity_platform", AddEntitiesCallback=type("AddEntitiesCallback", (), {}))

if "homeassistant.util" not in sys.modules:
    _mock("homeassistant.util")

if "homeassistant.util.dt" not in sys.modules:
    _mock(
        "homeassistant.util.dt",
        now=lambda: datetime(2026, 3, 27, 12, 0, 0),
        as_local=lambda dt: dt,
        parse_datetime=lambda value: datetime.fromisoformat(str(value)),
        UTC=None,
    )
else:
    dt_mod = sys.modules["homeassistant.util.dt"]
    if not hasattr(dt_mod, "now"):
        setattr(dt_mod, "now", lambda: datetime(2026, 3, 27, 12, 0, 0))
    if not hasattr(dt_mod, "as_local"):
        setattr(dt_mod, "as_local", lambda dt: dt)
    if not hasattr(dt_mod, "parse_datetime"):
        setattr(dt_mod, "parse_datetime", lambda value: datetime.fromisoformat(str(value)))
    if not hasattr(dt_mod, "UTC"):
        setattr(dt_mod, "UTC", None)


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

init_mod = importlib.import_module("custom_components.solarfriend")  # noqa: E402
from custom_components.solarfriend.battery_tracker import BatteryTracker  # noqa: E402
from custom_components.solarfriend.advanced_consumption_model import AdvancedConsumptionModel  # noqa: E402
from custom_components.solarfriend.coordinator import SolarFriendCoordinator  # noqa: E402
from custom_components.solarfriend.coordinator_policy import DEFAULT_COORDINATOR_POLICY  # noqa: E402
from custom_components.solarfriend.forecast_correction_model import ForecastCorrectionModel  # noqa: E402
from custom_components.solarfriend.price_adapter import PriceData  # noqa: E402
from custom_components.solarfriend.solar_installation_profile import SolarInstallationProfile  # noqa: E402
from custom_components.solarfriend.select import (  # noqa: E402
    SolarFriendEVDepartureSelect,
    SolarFriendEVModeSelect,
)
from custom_components.solarfriend.state_reader import SolarFriendStateReader  # noqa: E402
from custom_components.solarfriend.switch import (  # noqa: E402
    SolarFriendAdvancedConsumptionModelSwitch,
    SolarFriendEVSwitch,
    SolarFriendEVSolarOnlyGridBufferSwitch,
)
from custom_components.solarfriend.tracker_runtime import TrackerRuntime  # noqa: E402
from custom_components.solarfriend.weather_service import WeatherProfileService  # noqa: E402


class _FakeState:
    def __init__(self, state: str, attributes: dict | None = None) -> None:
        self.state = state
        self.attributes = attributes or {}


async def _async_record_reason(target: list[str], reason: str) -> None:
    target.append(reason)


class _FakeStates:
    def __init__(self, mapping: dict[str, _FakeState] | None = None) -> None:
        self._mapping = mapping or {}

    def get(self, entity_id: str):
        return self._mapping.get(entity_id)


class _FakeServices:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []
        self._registry: dict[tuple[str, str], object] = {}

    def has_service(self, domain: str, service: str) -> bool:
        return (domain, service) in self._registry

    def async_register(self, domain: str, service: str, handler, **kwargs) -> None:
        self._registry[(domain, service)] = handler

    def async_remove(self, domain: str, service: str) -> None:
        self._registry.pop((domain, service), None)

    async def async_call(self, domain: str, service: str, data: dict, **kwargs):
        self.calls.append((domain, service, data))
        return None


class _FakeConfigEntries:
    def __init__(self) -> None:
        self.updated: list[tuple[object, dict]] = []
        self.forwarded: list[tuple[object, list[str]]] = []
        self.unloaded: list[tuple[object, list[str]]] = []

    def async_update_entry(self, entry, *, data=None) -> None:
        entry.data = data or entry.data
        self.updated.append((entry, dict(entry.data)))

    async def async_forward_entry_setups(self, entry, platforms) -> None:
        self.forwarded.append((entry, list(platforms)))

    async def async_unload_platforms(self, entry, platforms) -> bool:
        self.unloaded.append((entry, list(platforms)))
        return True


class _FakeHass:
    def __init__(self, *, states: dict[str, _FakeState] | None = None) -> None:
        self.states = _FakeStates(states)
        self.services = _FakeServices()
        self.config_entries = _FakeConfigEntries()
        self.data: dict = {}
        self.created_tasks: list = []
        self.config = types.SimpleNamespace(
            path=lambda *parts: os.path.join("config", *parts),
            latitude=55.0,
            longitude=12.0,
        )

    def async_create_task(self, coro):
        self.created_tasks.append(coro)
        coro.close()
        return None


class _FakeServicesNoResponseKwarg(_FakeServices):
    def async_register(self, domain: str, service: str, handler, **kwargs) -> None:
        if "supports_response" in kwargs:
            raise TypeError("async_register() got an unexpected keyword argument 'supports_response'")
        super().async_register(domain, service, handler, **kwargs)


class _FakeForecastData:
    def __init__(self, total_today_kwh: float = 12.5) -> None:
        self.total_today_kwh = total_today_kwh
        self.hourly_forecast: list[dict] = []


def _make_entry(**overrides):
    data = {
        "name": "SolarFriend",
        "pv_power_sensor": "sensor.pv_power",
        "pv2_power_sensor": "",
        "grid_power_sensor": "sensor.grid_power",
        "battery_soc_sensor": "sensor.battery_soc",
        "battery_power_sensor": "sensor.battery_power",
        "load_power_sensor": "sensor.total_load",
        "buy_price_sensor": "sensor.buy_price",
        "sell_price_sensor": "sensor.sell_price",
        "forecast_sensor": "sensor.forecast",
        "forecast_type": "forecast_solar",
        "ev_charging_enabled": True,
        "ev_solar_only_grid_buffer_enabled": True,
        "battery_sell_enabled": True,
        "battery_capacity_kwh": 10.0,
        "battery_min_soc": 10.0,
    }
    data.update(overrides)
    return types.SimpleNamespace(entry_id="entry-1", data=data)


def _prime_runtime_test_coordinator(coordinator) -> None:
    """Populate coordinator runtime fields that __init__ normally sets."""
    coordinator._prev_update_time = None
    coordinator._prev_battery_power = 0.0
    coordinator._startup_at = datetime(2026, 3, 27, 11, 0, 0)
    coordinator._startup_price_recovery_optimize_done = False


def test_async_update_data_reads_states_and_publishes_cleaned_ev_load():
    hass = _FakeHass(
        states={
            "sensor.pv_power": _FakeState("6000"),
            "sensor.grid_power": _FakeState("-500"),
            "sensor.battery_soc": _FakeState("82"),
            "sensor.battery_power": _FakeState("0"),
            "sensor.total_load": _FakeState("5000"),
            "sun.sun": _FakeState("above_horizon", {"next_setting": "2026-03-27T18:30:00"}),
        }
    )
    entry = _make_entry()
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator.hass = hass
    coordinator._entry = entry
    coordinator._policy = DEFAULT_COORDINATOR_POLICY
    coordinator.data = None
    coordinator._state_reader = SolarFriendStateReader(hass, entry)
    coordinator._price_runtime = types.SimpleNamespace(
        resolve_snapshot=lambda now, cache_kind, fresh_snapshot, normalize: fresh_snapshot,
        update_history=lambda price: None,
        record_night_price=lambda hour, price: None,
        price_average=lambda: 1.0,
        battery_strategy=lambda solar_surplus, price, avg_price: "IDLE",
        min_night_price=lambda: 0.5,
        price_level=lambda price, avg_price: "NORMAL",
    )
    coordinator._profile = types.SimpleNamespace(
        confidence="READY",
        days_collected=7,
        _profiles={
            "weekday": [{"avg_watt": 800.0, "samples": 5} for _ in range(24)],
            "weekend": [{"avg_watt": 900.0, "samples": 5} for _ in range(24)],
        },
        build_debug_snapshot=lambda: {"status": "ok"},
    )
    coordinator._tracker = None
    coordinator._forecast_tracker = None
    coordinator._forecast_correction_model = None
    coordinator._solar_installation_profile = None
    coordinator._optimizer = types.SimpleNamespace(get_last_plan=lambda: [])
    coordinator._last_optimize_dt = datetime(2026, 3, 27, 11, 58, 0)
    coordinator._last_plan_optimize_result = None
    coordinator._shadow_log_enabled = False
    coordinator._shadow_logger = types.SimpleNamespace(build_payload=lambda *a, **kw: {})
    coordinator.advanced_consumption_model_enabled = True
    coordinator._advanced_consumption_model = AdvancedConsumptionModel()
    coordinator._weather_service = types.SimpleNamespace(
        async_get_current_hour_snapshot=lambda now: asyncio.sleep(
            0,
            result={
                "condition": "sunny",
                "cloud_coverage_pct": 10.0,
                "temperature_c": 12.0,
                "precipitation_mm": 0.0,
                "wind_speed_mps": 3.0,
                "wind_bearing_deg": 180.0,
                "is_daylight": True,
                "is_heating_season": True,
            },
        ),
        async_fetch_hourly_forecast=lambda: asyncio.sleep(0, result=[]),
    )
    coordinator._ev_enabled = True
    coordinator._ev_charger = types.SimpleNamespace(
        get_status=lambda: asyncio.sleep(0, result="charging"),
        get_power_w=lambda: asyncio.sleep(0, result=2200.0),
    )
    coordinator._update_ev = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_forecast_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._maybe_update_profile = lambda *a, **kw: asyncio.sleep(0)
    coordinator._append_shadow_log = lambda payload: asyncio.sleep(0)
    coordinator._build_shadow_payload = lambda *a, **kw: {}
    coordinator._should_trigger_plan_deviation_replan = lambda **kwargs: False
    coordinator._build_battery_plan = lambda data, now: []
    coordinator._trigger_optimize = lambda *a, **kw: asyncio.sleep(0)
    coordinator._clean_live_house_load = SolarFriendCoordinator._clean_live_house_load.__get__(coordinator)
    _prime_runtime_test_coordinator(coordinator)

    from custom_components.solarfriend import coordinator as coordinator_mod

    original_price_adapter = coordinator_mod.PriceAdapter.from_hass
    original_forecast_adapter = coordinator_mod.ForecastAdapter.from_hass
    coordinator_mod.PriceAdapter.from_hass = staticmethod(
        lambda hass_obj, entity_id: PriceData(current_price=1.25, source_entity=entity_id)
    )

    async def _fake_forecast_from_hass(**kwargs):
        return _FakeForecastData()

    coordinator_mod.ForecastAdapter.from_hass = staticmethod(_fake_forecast_from_hass)
    try:
        data = asyncio.run(coordinator._async_update_data())
    finally:
        coordinator_mod.PriceAdapter.from_hass = original_price_adapter
        coordinator_mod.ForecastAdapter.from_hass = original_forecast_adapter

    assert data.pv_power == 6000.0
    assert data.ev_charging_power == 2200.0
    assert data.load_power == 2800.0
    assert data.solar_surplus == 3200.0
    assert data.price == 1.25
    assert data.sell_price == 1.25
    assert data.forecast == 12.5
    assert data.profile_confidence == "READY"
    expected_day_type = (
        "weekend" if coordinator_mod.ha_dt.now().weekday() >= 5 else "weekday"
    )
    assert data.consumption_profile_day_type == expected_day_type
    assert data.advanced_consumption_model_enabled is True
    assert data.advanced_consumption_model_state == "learning"
    assert data.advanced_consumption_model_last_weather["condition"] == "sunny"


def test_async_update_data_triggers_optimize_when_forecast_recovers_during_startup():
    hass = _FakeHass(
        states={
            "sensor.pv_power": _FakeState("6000"),
            "sensor.grid_power": _FakeState("-500"),
            "sensor.battery_soc": _FakeState("82"),
            "sensor.battery_power": _FakeState("0"),
            "sensor.total_load": _FakeState("5000"),
            "sun.sun": _FakeState("above_horizon", {"next_setting": "2026-03-27T18:30:00"}),
        }
    )
    entry = _make_entry(ev_charging_enabled=False)
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator.hass = hass
    coordinator._entry = entry
    coordinator._policy = DEFAULT_COORDINATOR_POLICY
    coordinator.data = importlib.import_module("custom_components.solarfriend.coordinator_models").SolarFriendData()
    coordinator.data.price_data = PriceData(current_price=1.25, source_entity="sensor.buy_price")
    coordinator.data.forecast_data = None
    coordinator._state_reader = SolarFriendStateReader(hass, entry)
    coordinator._price_runtime = types.SimpleNamespace(
        resolve_snapshot=lambda now, cache_kind, fresh_snapshot, normalize: fresh_snapshot,
        update_history=lambda price: None,
        record_night_price=lambda hour, price: None,
        price_average=lambda: 1.0,
        battery_strategy=lambda solar_surplus, price, avg_price: "IDLE",
        min_night_price=lambda: 0.5,
        price_level=lambda price, avg_price: "NORMAL",
    )
    coordinator._profile = types.SimpleNamespace(
        confidence="READY",
        days_collected=7,
        _profiles={
            "weekday": [{"avg_watt": 800.0, "samples": 5} for _ in range(24)],
            "weekend": [{"avg_watt": 900.0, "samples": 5} for _ in range(24)],
        },
        build_debug_snapshot=lambda: {"status": "ok"},
    )
    coordinator._tracker = None
    coordinator._forecast_tracker = None
    coordinator._forecast_correction_model = None
    coordinator._solar_installation_profile = None
    coordinator._solar_installation_profiles = {}
    coordinator._optimizer = types.SimpleNamespace(get_last_plan=lambda: [])
    coordinator._last_optimize_dt = datetime(2026, 3, 27, 11, 59, 30)
    coordinator._last_plan_optimize_result = None
    coordinator._shadow_log_enabled = False
    coordinator._shadow_logger = types.SimpleNamespace(build_payload=lambda *a, **kw: {})
    coordinator.advanced_consumption_model_enabled = False
    coordinator._advanced_consumption_model = AdvancedConsumptionModel()
    coordinator._weather_service = types.SimpleNamespace(
        async_get_current_hour_snapshot=lambda now: asyncio.sleep(
            0,
            result={
                "condition": "sunny",
                "cloud_coverage_pct": 10.0,
                "temperature_c": 12.0,
                "precipitation_mm": 0.0,
                "wind_speed_mps": 3.0,
                "wind_bearing_deg": 180.0,
                "is_daylight": True,
                "is_heating_season": True,
            },
        ),
        async_fetch_hourly_forecast=lambda: asyncio.sleep(0, result=[]),
    )
    coordinator._ev_enabled = False
    coordinator._update_ev = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_forecast_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._maybe_update_profile = lambda *a, **kw: asyncio.sleep(0)
    coordinator._append_shadow_log = lambda payload: asyncio.sleep(0)
    coordinator._build_shadow_payload = lambda *a, **kw: {}
    coordinator._should_trigger_plan_deviation_replan = lambda **kwargs: False
    coordinator._build_battery_plan = lambda data, now: []
    triggered_reasons: list[str] = []

    async def _fake_trigger_optimize(reason: str = "event", **kwargs):
        triggered_reasons.append(reason)

    coordinator._trigger_optimize = _fake_trigger_optimize
    coordinator._clean_live_house_load = SolarFriendCoordinator._clean_live_house_load.__get__(coordinator)
    _prime_runtime_test_coordinator(coordinator)
    coordinator._startup_at = datetime(2026, 3, 27, 11, 59, 0)

    from custom_components.solarfriend import coordinator as coordinator_mod

    original_price_adapter = coordinator_mod.PriceAdapter.from_hass
    original_forecast_adapter = coordinator_mod.ForecastAdapter.from_hass
    original_now = coordinator_mod.ha_dt.now
    coordinator_mod.ha_dt.now = lambda: datetime(2026, 3, 27, 12, 0, 0)
    coordinator_mod.PriceAdapter.from_hass = staticmethod(
        lambda hass_obj, entity_id: PriceData(current_price=1.25, source_entity=entity_id)
    )

    async def _fake_forecast_from_hass(**kwargs):
        return _FakeForecastData()

    coordinator_mod.ForecastAdapter.from_hass = staticmethod(_fake_forecast_from_hass)
    try:
        asyncio.run(coordinator._async_update_data())
    finally:
        coordinator_mod.ha_dt.now = original_now
        coordinator_mod.PriceAdapter.from_hass = original_price_adapter
        coordinator_mod.ForecastAdapter.from_hass = original_forecast_adapter

    assert triggered_reasons == ["startup-inputs-recovered"]


def test_async_update_data_wires_solar_profile_snapshot_fields():
    hass = _FakeHass(
        states={
            "sensor.pv_power": _FakeState("6000"),
            "sensor.grid_power": _FakeState("-500"),
            "sensor.battery_soc": _FakeState("82"),
            "sensor.battery_power": _FakeState("0"),
            "sensor.total_load": _FakeState("5000"),
            "sun.sun": _FakeState(
                "above_horizon",
                {
                    "elevation": 35.0,
                    "azimuth": 150.0,
                    "next_rising": "2026-03-28T06:12:00+00:00",
                    "next_setting": "2026-03-27T18:44:00+00:00",
                },
            ),
        }
    )
    hass.config.latitude = 55.0
    hass.config.longitude = 12.0
    entry = _make_entry()
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator.hass = hass
    coordinator._entry = entry
    coordinator._policy = DEFAULT_COORDINATOR_POLICY
    coordinator.data = None
    coordinator._state_reader = SolarFriendStateReader(hass, entry)
    coordinator._price_runtime = types.SimpleNamespace(
        resolve_snapshot=lambda now, cache_kind, fresh_snapshot, normalize: fresh_snapshot,
        update_history=lambda price: None,
        record_night_price=lambda hour, price: None,
        price_average=lambda: 1.0,
        battery_strategy=lambda solar_surplus, price, avg_price: "IDLE",
        min_night_price=lambda: 0.5,
        price_level=lambda price, avg_price: "NORMAL",
    )
    coordinator._profile = types.SimpleNamespace(
        confidence="READY",
        days_collected=7,
        _profiles={
            "weekday": [{"avg_watt": 800.0, "samples": 5} for _ in range(24)],
            "weekend": [{"avg_watt": 900.0, "samples": 5} for _ in range(24)],
        },
        build_debug_snapshot=lambda: {"status": "ok"},
    )
    coordinator._tracker = None
    coordinator._forecast_tracker = None
    class _DummyStore:
        def __init__(self, *args, **kwargs):
            self._data = None

        async def async_load(self):
            return self._data

        async def async_save(self, data):
            self._data = data

    from custom_components.solarfriend import forecast_correction_model as fcm_mod
    from custom_components.solarfriend import solar_installation_profile as sip_mod

    original_fcm_store = fcm_mod.Store
    original_sip_store = sip_mod.Store
    fcm_mod.Store = lambda *args, **kwargs: _DummyStore()
    sip_mod.Store = lambda *args, **kwargs: _DummyStore()

    coordinator._forecast_correction_model = ForecastCorrectionModel(hass, entry.entry_id)
    coordinator._forecast_correction_model._geometry_buckets["s1|e30|a150"] = types.SimpleNamespace(
        factor=0.8,
        samples=8,
        avg_abs_error_kwh=0.1,
    )
    coordinator._forecast_correction_model._temperature_buckets["s1|t10"] = types.SimpleNamespace(
        factor=0.95,
        samples=8,
        avg_abs_error_kwh=0.1,
    )
    coordinator._solar_installation_profile = SolarInstallationProfile(hass, entry.entry_id)
    for elevation_bucket in (20, 30, 40, 50):
        for azimuth_bucket in range(0, 360, 30):
            coordinator._solar_installation_profile._cells[(elevation_bucket, azimuth_bucket)] = types.SimpleNamespace(
                factor=0.9,
                samples=20,
                avg_abs_error_kwh=0.1,
            )
    coordinator._optimizer = types.SimpleNamespace(get_last_plan=lambda: [])
    coordinator._last_optimize_dt = datetime(2026, 3, 27, 11, 58, 0)
    coordinator._last_plan_optimize_result = None
    coordinator._shadow_log_enabled = False
    coordinator._shadow_logger = types.SimpleNamespace(build_payload=lambda *a, **kw: {})
    coordinator.advanced_consumption_model_enabled = True
    coordinator._advanced_consumption_model = AdvancedConsumptionModel()
    comparison_now = sys.modules["homeassistant.util.dt"].now().replace(minute=0, second=0, microsecond=0)
    next_hour = comparison_now + timedelta(hours=1)
    coordinator._weather_service = types.SimpleNamespace(
        async_get_current_hour_snapshot=lambda now: asyncio.sleep(
            0,
            result={
                "condition": "sunny",
                "cloud_coverage_pct": 5.0,
                "temperature_c": 12.0,
                "precipitation_mm": 0.0,
                "wind_speed_mps": 3.0,
                "wind_bearing_deg": 180.0,
                "humidity_pct": 55.0,
                "is_daylight": True,
                "is_heating_season": True,
            },
        ),
        async_fetch_hourly_forecast=lambda: asyncio.sleep(
            0,
            result=[
                {"datetime": comparison_now.isoformat(), "temperature": 12.0},
                {"datetime": next_hour.isoformat(), "temperature": 12.0},
            ],
        ),
    )
    coordinator._ev_enabled = False
    coordinator._update_ev = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_forecast_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._maybe_update_profile = lambda *a, **kw: asyncio.sleep(0)
    coordinator._append_shadow_log = lambda payload: asyncio.sleep(0)
    coordinator._build_shadow_payload = lambda *a, **kw: {}
    coordinator._should_trigger_plan_deviation_replan = lambda **kwargs: False
    coordinator._build_battery_plan = lambda data, now: []
    coordinator._trigger_optimize = lambda *a, **kw: asyncio.sleep(0)
    coordinator._clean_live_house_load = SolarFriendCoordinator._clean_live_house_load.__get__(coordinator)
    _prime_runtime_test_coordinator(coordinator)
    coordinator._prev_update_time = datetime(2026, 3, 27, 11, 59, 30)

    from custom_components.solarfriend import coordinator as coordinator_mod

    original_price_adapter = coordinator_mod.PriceAdapter.from_hass
    original_forecast_adapter = coordinator_mod.ForecastAdapter.from_hass
    original_solar_position = ForecastCorrectionModel._solar_position
    forecast_now = comparison_now

    coordinator_mod.PriceAdapter.from_hass = staticmethod(
        lambda hass_obj, entity_id: PriceData(current_price=1.25, source_entity=entity_id)
    )

    async def _fake_forecast_from_hass(**kwargs):
        forecast = _FakeForecastData()
        forecast.hourly_forecast = [
            {"period_start": forecast_now, "pv_estimate_kwh": 1.0},
            {"period_start": forecast_now.replace(hour=(forecast_now.hour + 1) % 24), "pv_estimate_kwh": 0.8},
        ]
        return forecast

    coordinator_mod.ForecastAdapter.from_hass = staticmethod(_fake_forecast_from_hass)
    ForecastCorrectionModel._solar_position = staticmethod(lambda **kwargs: (35.0, 150.0))
    try:
        data = asyncio.run(coordinator._async_update_data())
    finally:
        fcm_mod.Store = original_fcm_store
        sip_mod.Store = original_sip_store
        coordinator_mod.PriceAdapter.from_hass = original_price_adapter
        coordinator_mod.ForecastAdapter.from_hass = original_forecast_adapter
        ForecastCorrectionModel._solar_position = original_solar_position

    assert data.solar_profile_state == "ready"
    assert data.solar_profile_populated_cells >= 48
    assert data.solar_profile_confident_cells >= 48
    assert data.solar_profile_response_surface
    assert isinstance(data.solar_profile_comparison_today, list)
    assert isinstance(data.solar_profile_comparison_tomorrow, list)


def test_async_on_runtime_setting_changed_rebuilds_runtime_and_reoptimizes():
    from custom_components.solarfriend import coordinator as coordinator_mod

    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator.hass = _FakeHass()
    coordinator._entry = _make_entry()
    coordinator._tracker = object()
    coordinator._profile = object()
    coordinator._optimizer = "old-optimizer"
    coordinator._inverter = "old-inverter"
    coordinator._state_reader = "old-reader"
    coordinator._weather_service = "old-weather"
    refresh_calls: list[str] = []
    optimize_calls: list[tuple[str, bool, bool]] = []

    async def _fake_refresh():
        refresh_calls.append("refresh")

    async def _fake_trigger_optimize(*, reason: str, notify: bool, force: bool):
        optimize_calls.append((reason, notify, force))

    coordinator.async_request_refresh = _fake_refresh
    coordinator._trigger_optimize = _fake_trigger_optimize

    original_build_runtime_components = coordinator_mod.build_runtime_components
    coordinator_mod.build_runtime_components = lambda hass, config_entry, battery_tracker, consumption_profile: types.SimpleNamespace(
        optimizer="new-optimizer",
        inverter="new-inverter",
        state_reader="new-reader",
        weather_service="new-weather",
    )
    try:
        asyncio.run(coordinator.async_on_runtime_setting_changed(reason="number-updated"))
    finally:
        coordinator_mod.build_runtime_components = original_build_runtime_components

    assert coordinator._optimizer == "new-optimizer"
    assert coordinator._inverter == "new-inverter"
    assert coordinator._state_reader == "new-reader"
    assert coordinator._weather_service == "new-weather"
    assert refresh_calls == ["refresh"]
    assert optimize_calls == [("number-updated", True, True)]


def test_price_state_change_forces_immediate_reoptimize():
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator._entry = _make_entry()
    coordinator._last_optimize_soc = None

    optimize_calls: list[tuple[str, bool, bool]] = []

    async def _fake_trigger_optimize(reason="event", notify=False, force=False):
        optimize_calls.append((reason, notify, force))

    coordinator._trigger_optimize = _fake_trigger_optimize
    coordinator.hass = types.SimpleNamespace(
        async_create_task=lambda coro: asyncio.run(coro),
        states=types.SimpleNamespace(get=lambda entity_id: None),
    )

    event = types.SimpleNamespace(
        data={
            "entity_id": "sensor.buy_price",
            "new_state": _FakeState("1.25"),
        }
    )

    coordinator._async_on_relevant_state_change(event)

    assert optimize_calls == [("price_updated", True, True)]


def test_ev_mode_select_triggers_immediate_runtime_refresh():
    entry = _make_entry(ev_charging_enabled=True)
    coordinator = types.SimpleNamespace(
        config_entry=entry,
        _entry=entry,
        ev_charge_mode="solar_only",
        async_on_runtime_setting_changed=lambda *, reason: _async_record_reason(refresh_reasons, reason),
    )
    refresh_reasons: list[str] = []
    entity = SolarFriendEVModeSelect(coordinator)
    entity.async_write_ha_state = lambda: None

    asyncio.run(entity.async_select_option("hybrid"))

    assert coordinator.ev_charge_mode == "hybrid"
    assert entity.current_option == "hybrid"
    assert refresh_reasons == ["select-ev_charge_mode-updated"]


def test_ev_departure_select_only_refreshes_for_planned_modes():
    refresh_reasons: list[str] = []
    entry = _make_entry(ev_charging_enabled=True)
    coordinator = types.SimpleNamespace(
        config_entry=entry,
        _entry=entry,
        ev_charge_mode="solar_only",
        ev_departure_time=None,
        async_on_runtime_setting_changed=lambda *, reason: _async_record_reason(refresh_reasons, reason),
    )
    entity = SolarFriendEVDepartureSelect(coordinator)
    entity.async_write_ha_state = lambda: None

    asyncio.run(entity.async_select_option("08:00"))
    assert refresh_reasons == []

    coordinator.ev_charge_mode = "grid_schedule"
    asyncio.run(entity.async_select_option("08:30"))
    assert refresh_reasons == ["select-ev_departure_time-updated"]


def test_ev_switch_triggers_immediate_runtime_refresh():
    refresh_reasons: list[str] = []
    entry = _make_entry(ev_charging_enabled=True)
    coordinator = types.SimpleNamespace(
        config_entry=entry,
        _entry=entry,
        ev_charging_allowed=True,
        async_on_runtime_setting_changed=lambda *, reason: _async_record_reason(refresh_reasons, reason),
    )
    entity = SolarFriendEVSwitch(coordinator)
    entity.async_write_ha_state = lambda: None

    asyncio.run(entity.async_turn_off())
    asyncio.run(entity.async_turn_on())

    assert refresh_reasons == [
        "switch-ev_charging_allowed-updated",
        "switch-ev_charging_allowed-updated",
    ]


def test_tracker_runtime_skips_load_based_savings_when_load_is_untrustworthy():
    runtime = TrackerRuntime(DEFAULT_COORDINATOR_POLICY, config_entry=_make_entry())
    runtime.state.battery_prev_update_time = datetime(2026, 3, 27, 12, 0, 0)

    tracker = types.SimpleNamespace(
        on_solar_charge=lambda *a, **kw: None,
        on_grid_charge=lambda *a, **kw: None,
        on_discharge=lambda *a, **kw: None,
        on_soc_correction=lambda *a, **kw: None,
        update_savings=lambda **kw: (_ for _ in ()).throw(AssertionError("update_savings must not run")),
        async_save=lambda: asyncio.sleep(0),
    )

    asyncio.run(
        runtime.update_battery_tracker(
            tracker=tracker,
            now=datetime(2026, 3, 27, 12, 5, 0),
            pv_power=4000.0,
            battery_power=1200.0,
            load_power=2500.0,
            battery_soc=80.0,
            current_price=1.2,
            sell_price=1.0,
            previous_soc=79.0,
            load_is_trustworthy=False,
        )
    )


def test_tracker_runtime_tracks_battery_sell_value_from_discharge_power():
    runtime = TrackerRuntime(DEFAULT_COORDINATOR_POLICY, config_entry=_make_entry())
    runtime.state.battery_prev_update_time = datetime(2026, 3, 27, 12, 0, 0)

    sell_calls: list[tuple[float, float, float]] = []
    tracker = types.SimpleNamespace(
        on_solar_charge=lambda *a, **kw: None,
        on_grid_charge=lambda *a, **kw: None,
        on_discharge=lambda *a, **kw: None,
        on_soc_correction=lambda *a, **kw: None,
        update_savings=lambda **kw: False,
        update_battery_sell_savings=lambda **kw: sell_calls.append(
            (kw["battery_w"], kw["sell_price_dkk"], kw["dt_seconds"])
        ) or True,
        async_save=lambda: asyncio.sleep(0),
    )

    asyncio.run(
        runtime.update_battery_tracker(
            tracker=tracker,
            now=datetime(2026, 3, 27, 12, 5, 0),
            pv_power=1000.0,
            battery_power=1500.0,
            load_power=0.0,
            battery_soc=80.0,
            current_price=1.2,
            sell_price=0.9,
            previous_soc=79.0,
            load_is_trustworthy=False,
            active_strategy="SELL_BATTERY",
        )
    )

    assert sell_calls == [(1500.0, 0.9, 300.0)]


def test_async_update_data_skips_load_learning_while_sell_battery_active():
    coordinator = SolarFriendCoordinator.__new__(SolarFriendCoordinator)
    coordinator.hass = _FakeHass(
        states={
            "sensor.pv_power": _FakeState("5000"),
            "sensor.grid_power": _FakeState("-2000"),
            "sensor.battery_soc": _FakeState("85"),
            "sensor.battery_power": _FakeState("1500"),
            "sensor.total_load": _FakeState("2200"),
            "sensor.buy_price": _FakeState("1.25"),
            "sensor.sell_price": _FakeState("1.25"),
            "sun.sun": _FakeState(
                "above_horizon",
                {
                    "elevation": 35.0,
                    "azimuth": 150.0,
                    "next_rising": "2026-03-28T06:12:00+00:00",
                    "next_setting": "2026-03-27T18:44:00+00:00",
                },
            ),
        }
    )
    coordinator._entry = _make_entry()
    coordinator._policy = DEFAULT_COORDINATOR_POLICY
    coordinator._state_reader = SolarFriendStateReader(coordinator.hass, coordinator._entry)
    coordinator._weather_service = types.SimpleNamespace(
        async_get_current_hour_snapshot=lambda now: asyncio.sleep(
            0,
            result={
                "condition": "sunny",
                "cloud_coverage_pct": 5.0,
                "temperature_c": 14.0,
                "precipitation_mm": 0.0,
                "wind_speed_mps": 2.0,
                "wind_bearing_deg": 190.0,
                "humidity_pct": 55.0,
                "is_daylight": True,
                "is_heating_season": True,
            },
        ),
        async_fetch_hourly_forecast=lambda: asyncio.sleep(0, result=[]),
    )
    coordinator._profile = types.SimpleNamespace(
        confidence="READY",
        days_collected=14,
        build_debug_snapshot=lambda: {},
        _profiles={
            "weekday": [{"avg_watt": 800.0, "samples": 10} for _ in range(24)],
            "weekend": [{"avg_watt": 800.0, "samples": 10} for _ in range(24)],
        },
    )
    coordinator._tracker = None
    coordinator._forecast_tracker = None
    coordinator._forecast_correction_model = None
    coordinator._solar_installation_profile = None
    coordinator._optimizer = types.SimpleNamespace(get_last_plan=lambda: [])
    coordinator._last_optimize_dt = datetime(2026, 3, 27, 11, 58, 0)
    coordinator._last_plan_optimize_result = None
    coordinator._shadow_log_enabled = False
    coordinator._shadow_logger = types.SimpleNamespace(build_payload=lambda *a, **kw: {})
    coordinator.advanced_consumption_model_enabled = True
    coordinator._advanced_consumption_model = AdvancedConsumptionModel()
    coordinator._ev_enabled = False
    coordinator._update_ev = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_tracker = lambda **kwargs: asyncio.sleep(0)
    coordinator._update_forecast_tracker = lambda **kwargs: asyncio.sleep(0)
    profile_calls: list[tuple] = []

    async def _fake_maybe_update_profile(*args, **kwargs):
        profile_calls.append((args, kwargs))

    coordinator._maybe_update_profile = _fake_maybe_update_profile
    coordinator._append_shadow_log = lambda payload: asyncio.sleep(0)
    coordinator._build_shadow_payload = lambda *a, **kw: {}
    coordinator._should_trigger_plan_deviation_replan = lambda **kwargs: False
    coordinator._build_battery_plan = lambda data, now: []
    coordinator._trigger_optimize = lambda *a, **kw: asyncio.sleep(0)
    coordinator._clean_live_house_load = SolarFriendCoordinator._clean_live_house_load.__get__(coordinator)
    _prime_runtime_test_coordinator(coordinator)

    from custom_components.solarfriend import coordinator as coordinator_mod

    original_price_adapter = coordinator_mod.PriceAdapter.from_hass
    original_forecast_adapter = coordinator_mod.ForecastAdapter.from_hass
    coordinator_mod.PriceAdapter.from_hass = staticmethod(
        lambda hass_obj, entity_id: PriceData(current_price=1.25, source_entity=entity_id)
    )

    async def _fake_forecast_from_hass(**kwargs):
        return _FakeForecastData()

    coordinator_mod.ForecastAdapter.from_hass = staticmethod(_fake_forecast_from_hass)
    try:
        coordinator.data = coordinator_mod.SolarFriendData()
        coordinator.data.optimize_result = types.SimpleNamespace(strategy="SELL_BATTERY")
        coordinator.data.ev_charging_power = 0.0
        coordinator.data.solar_until_sunset = 0.0
        data = asyncio.run(coordinator._async_update_data())
    finally:
        coordinator_mod.PriceAdapter.from_hass = original_price_adapter
        coordinator_mod.ForecastAdapter.from_hass = original_forecast_adapter

    assert profile_calls == []
    assert data.advanced_consumption_model_records == 0


def test_async_setup_entry_registers_service_and_stores_coordinator():
    hass = _FakeHass()
    entry = _make_entry()
    startup_calls: list[str] = []
    trigger_calls: list[str] = []

    class _FakeCoordinator:
        def __init__(self, hass_obj, entry_obj) -> None:
            self.hass = hass_obj
            self.entry = entry_obj

        async def async_startup(self):
            startup_calls.append("startup")

        async def async_config_entry_first_refresh(self):
            startup_calls.append("first_refresh")

        async def _trigger_optimize(self, reason="event", notify=False, force=False):
            trigger_calls.append(reason)

    original_cleanup = init_mod._cleanup_orphaned_ev_entities
    original_coordinator = init_mod.SolarFriendCoordinator
    init_mod._cleanup_orphaned_ev_entities = lambda hass_obj, entry_obj: asyncio.sleep(0)
    init_mod.SolarFriendCoordinator = _FakeCoordinator
    try:
        result = asyncio.run(init_mod.async_setup_entry(hass, entry))
    finally:
        init_mod._cleanup_orphaned_ev_entities = original_cleanup
        init_mod.SolarFriendCoordinator = original_coordinator

    assert result is True
    assert startup_calls == ["startup", "first_refresh"]
    assert entry.entry_id in hass.data[init_mod.DOMAIN]
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_POPULATE_LOAD_MODEL) is True
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_BOOK_FLEX_LOAD) is True
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_CANCEL_FLEX_LOAD) is True
    assert len(hass.config_entries.forwarded) == 1
    assert len(hass.created_tasks) == 1


def test_async_setup_entry_falls_back_when_service_registry_rejects_supports_response():
    hass = _FakeHass()
    hass.services = _FakeServicesNoResponseKwarg()
    entry = _make_entry()

    class _FakeCoordinator:
        def __init__(self, hass_obj, entry_obj) -> None:
            self.hass = hass_obj
            self.entry = entry_obj

        async def async_startup(self):
            return None

        async def async_config_entry_first_refresh(self):
            return None

        async def _trigger_optimize(self, reason="event", notify=False, force=False):
            return None

    original_cleanup = init_mod._cleanup_orphaned_ev_entities
    original_coordinator = init_mod.SolarFriendCoordinator
    init_mod._cleanup_orphaned_ev_entities = lambda hass_obj, entry_obj: asyncio.sleep(0)
    init_mod.SolarFriendCoordinator = _FakeCoordinator
    try:
        result = asyncio.run(init_mod.async_setup_entry(hass, entry))
    finally:
        init_mod._cleanup_orphaned_ev_entities = original_cleanup
        init_mod.SolarFriendCoordinator = original_coordinator

    assert result is True
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_POPULATE_LOAD_MODEL) is True
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_BOOK_FLEX_LOAD) is True
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_CANCEL_FLEX_LOAD) is True


def test_weather_snapshot_converts_kmh_to_mps():
    class _ForecastServices:
        async def async_call(self, domain: str, service: str, data: dict, **kwargs):
            assert domain == "weather"
            assert service == "get_forecasts"
            return {
                "weather.forecast_hjem": {
                    "forecast": [
                        {
                            "datetime": "2026-03-27T12:00:00+01:00",
                            "condition": "sunny",
                            "cloud_coverage": 12.0,
                            "temperature": 14.0,
                            "precipitation": 0.0,
                            "wind_speed": 18.0,
                            "wind_bearing": 225.0,
                        }
                    ]
                }
            }

    hass = types.SimpleNamespace(
        states=_FakeStates(
            {
                "weather.forecast_hjem": _FakeState(
                    "sunny",
                    {"wind_speed_unit": "km/h"},
                )
            }
        ),
        services=_ForecastServices(),
    )
    service = WeatherProfileService(hass, weather_entity="weather.forecast_hjem")
    from custom_components.solarfriend import weather_service as weather_service_mod

    original_as_local = weather_service_mod.ha_dt.as_local
    original_utc = weather_service_mod.ha_dt.UTC
    weather_service_mod.ha_dt.as_local = lambda dt: dt
    weather_service_mod.ha_dt.UTC = timezone.utc
    try:
        snapshot = asyncio.run(
            service.async_get_current_hour_snapshot(datetime(2026, 3, 27, 11, 30, 0, tzinfo=timezone.utc))
        )
    finally:
        weather_service_mod.ha_dt.as_local = original_as_local
        weather_service_mod.ha_dt.UTC = original_utc

    assert snapshot["condition"] == "sunny"
    assert snapshot["wind_speed_mps"] == 5.0


def test_battery_tracker_storage_load_failure_does_not_abort_startup():
    tracker = BatteryTracker.__new__(BatteryTracker)
    tracker._hass = _FakeHass()
    tracker._legacy_entry_id = ""
    tracker._battery_cost_per_kwh = 0.2
    tracker.solar_kwh = 0.0
    tracker.grid_kwh = 0.0
    tracker.grid_avg_cost = 0.0
    tracker.today_solar_direct_kwh = 0.0
    tracker.today_solar_direct_saved_dkk = 0.0
    tracker.today_optimizer_saved_dkk = 0.0
    tracker.total_solar_direct_saved_dkk = 0.0
    tracker.total_optimizer_saved_dkk = 0.0
    tracker.today_battery_sell_kwh = 0.0
    tracker.today_battery_sell_saved_dkk = 0.0
    tracker.total_battery_sell_saved_dkk = 0.0
    tracker._last_reset_date = ""

    class _BrokenStore:
        async def async_load(self):
            raise ValueError("corrupt json")

        async def async_save(self, data):
            return None

    class _EmptyStore:
        async def async_load(self):
            return None

        async def async_save(self, data):
            return None

    tracker._store = _BrokenStore()
    tracker._backup_store = _EmptyStore()

    asyncio.run(tracker.async_load())

    assert tracker.solar_kwh == 0.0
    assert tracker.grid_kwh == 0.0


def test_forecast_correction_storage_load_failure_does_not_abort_startup():
    model = ForecastCorrectionModel.__new__(ForecastCorrectionModel)
    model._hass = _FakeHass()
    model._legacy_entry_id = ""
    model._buckets = {month: {hour: types.SimpleNamespace(factor=1.0, samples=0, avg_abs_error_kwh=0.0) for hour in range(24)} for month in range(1, 13)}
    model._context_buckets = {}
    model._today_date = ""
    model._today_actual_kwh_by_hour = {}
    model._today_raw_forecast_kwh_by_hour = {}
    model._today_context_by_hour = {}
    model._finalized_hours = set()
    model._today_sunrise = None
    model._today_sunset = None

    class _BrokenStore:
        async def async_load(self):
            raise ValueError("corrupt json")

        async def async_save(self, data):
            return None

    model._store = _BrokenStore()

    asyncio.run(model.async_load())

    assert model._today_date == ""
    assert model._context_buckets == {}


def test_forecast_tracker_saves_every_minute_while_pv_is_active():
    runtime = TrackerRuntime(DEFAULT_COORDINATOR_POLICY, config_entry=_make_entry())
    save_calls: list[datetime] = []

    class _FakeForecastTracker:
        def update(self, **kwargs) -> None:
            return None

        async def async_save(self) -> None:
            save_calls.append(current_now)

    tracker = _FakeForecastTracker()
    first_now = datetime(2026, 3, 27, 12, 0, 0)
    second_now = datetime(2026, 3, 27, 12, 0, 45)
    third_now = datetime(2026, 3, 27, 12, 1, 5)

    global current_now
    current_now = first_now
    asyncio.run(
        runtime.update_forecast_tracker(
            forecast_tracker=tracker,
            now=first_now,
            pv_power=3500.0,
            forecast_total_today_kwh=25.0,
        )
    )
    current_now = second_now
    asyncio.run(
        runtime.update_forecast_tracker(
            forecast_tracker=tracker,
            now=second_now,
            pv_power=3200.0,
            forecast_total_today_kwh=25.0,
        )
    )
    current_now = third_now
    asyncio.run(
        runtime.update_forecast_tracker(
            forecast_tracker=tracker,
            now=third_now,
            pv_power=3000.0,
            forecast_total_today_kwh=25.0,
        )
    )

    assert save_calls == [first_now, third_now]


def test_forecast_tracker_keeps_its_own_delta_after_battery_tracker_update():
    runtime = TrackerRuntime(DEFAULT_COORDINATOR_POLICY, config_entry=_make_entry())
    observed_dt_seconds: list[float] = []

    class _FakeBatteryTracker:
        def on_solar_charge(self, kwh: float) -> None:
            return None

        def on_grid_charge(self, kwh: float, grid_price: float) -> None:
            return None

        def on_discharge(self, kwh: float) -> None:
            return None

        def on_soc_correction(self, actual_soc: float, capacity_kwh: float, min_soc: float) -> None:
            return None

        def update_savings(self, **kwargs) -> bool:
            return False

        def update_battery_sell_savings(self, **kwargs) -> bool:
            return False

        async def async_save(self) -> None:
            return None

    class _FakeForecastTracker:
        def update(self, **kwargs) -> None:
            observed_dt_seconds.append(kwargs["dt_seconds"])

        async def async_save(self) -> None:
            return None

    first_now = datetime(2026, 3, 27, 12, 0, 0)
    second_now = datetime(2026, 3, 27, 12, 0, 30)

    asyncio.run(
        runtime.update_battery_tracker(
            tracker=_FakeBatteryTracker(),
            now=first_now,
            pv_power=2500.0,
            battery_power=0.0,
            load_power=500.0,
            battery_soc=80.0,
            current_price=1.5,
            sell_price=1.0,
            previous_soc=80.0,
        )
    )
    asyncio.run(
        runtime.update_forecast_tracker(
            forecast_tracker=_FakeForecastTracker(),
            now=first_now,
            pv_power=2500.0,
            forecast_total_today_kwh=20.0,
        )
    )
    asyncio.run(
        runtime.update_battery_tracker(
            tracker=_FakeBatteryTracker(),
            now=second_now,
            pv_power=2600.0,
            battery_power=0.0,
            load_power=500.0,
            battery_soc=80.0,
            current_price=1.5,
            sell_price=1.0,
            previous_soc=80.0,
        )
    )
    asyncio.run(
        runtime.update_forecast_tracker(
            forecast_tracker=_FakeForecastTracker(),
            now=second_now,
            pv_power=2600.0,
            forecast_total_today_kwh=20.0,
        )
    )

    assert observed_dt_seconds == [0.0, 30.0]


def test_async_unload_entry_persists_unregisters_and_removes_service():
    hass = _FakeHass()
    entry = _make_entry()
    unregister_calls: list[str] = []
    persist_calls: list[str] = []

    class _FakeCoordinator:
        def unregister_listeners(self):
            unregister_calls.append("unregister")

        async def async_persist_state(self):
            persist_calls.append("persist")

    hass.data[init_mod.DOMAIN] = {entry.entry_id: _FakeCoordinator()}
    hass.services.async_register(init_mod.DOMAIN, init_mod.SERVICE_POPULATE_LOAD_MODEL, lambda call: None)
    hass.services.async_register(init_mod.DOMAIN, init_mod.SERVICE_BOOK_FLEX_LOAD, lambda call: None)
    hass.services.async_register(init_mod.DOMAIN, init_mod.SERVICE_CANCEL_FLEX_LOAD, lambda call: None)

    result = asyncio.run(init_mod.async_unload_entry(hass, entry))

    assert result is True
    assert unregister_calls == ["unregister"]
    assert persist_calls == ["persist"]
    assert entry.entry_id not in hass.data[init_mod.DOMAIN]
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_POPULATE_LOAD_MODEL) is False
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_BOOK_FLEX_LOAD) is False
    assert hass.services.has_service(init_mod.DOMAIN, init_mod.SERVICE_CANCEL_FLEX_LOAD) is False


def test_book_flex_load_service_handler_delegates_to_coordinator():
    hass = _FakeHass()
    coordinator_calls: list[dict] = []

    class _FakeCoordinator:
        async def async_book_flex_load(self, **kwargs):
            coordinator_calls.append(kwargs)
            return {"job_id": kwargs["job_id"], "status": "booked"}

    hass.data[init_mod.DOMAIN] = {"entry-1": _FakeCoordinator()}
    call = types.SimpleNamespace(
        data={
            "entry_id": "entry-1",
            "job_id": "dishwasher",
            "name": "Dishwasher",
            "duration_minutes": 150,
            "deadline": "2026-03-28T06:00:00",
            "earliest_start": "2026-03-27T22:00:00",
            "preferred_source": "solar",
            "energy_wh": 2000,
            "min_solar_w": 1500,
            "max_grid_w": 300,
            "allow_battery": False,
        }
    )

    response = asyncio.run(init_mod._async_handle_book_flex_load(hass, call))

    assert response == {"job_id": "dishwasher", "status": "booked"}
    assert coordinator_calls[0]["job_id"] == "dishwasher"
    assert coordinator_calls[0]["preferred_source"] == "solar"


def test_ev_grid_buffer_switch_restore_updates_config_quietly():
    hass = _FakeHass()
    entry = _make_entry(ev_solar_only_grid_buffer_enabled=True)
    runtime_reasons: list[str] = []
    coordinator = types.SimpleNamespace(ev_solar_only_grid_buffer_enabled=True)

    async def _runtime_changed(*, reason: str):
        runtime_reasons.append(reason)

    coordinator.async_on_runtime_setting_changed = _runtime_changed
    switch = SolarFriendEVSolarOnlyGridBufferSwitch(coordinator, entry)
    switch.hass = hass
    switch.async_write_ha_state = lambda: None

    async def _fake_last_state():
        return types.SimpleNamespace(state="off")

    switch.async_get_last_state = _fake_last_state

    asyncio.run(switch.async_added_to_hass())

    assert coordinator.ev_solar_only_grid_buffer_enabled is False
    assert entry.data["ev_solar_only_grid_buffer_enabled"] is False
    assert runtime_reasons == []
    assert len(hass.config_entries.updated) == 1


def test_advanced_consumption_model_switch_triggers_runtime_refresh_on_update():
    hass = _FakeHass()
    entry = _make_entry(advanced_consumption_model_enabled=False)
    runtime_reasons: list[str] = []
    coordinator = types.SimpleNamespace(advanced_consumption_model_enabled=False)

    async def _runtime_changed(*, reason: str):
        runtime_reasons.append(reason)

    coordinator.async_on_runtime_setting_changed = _runtime_changed
    switch = SolarFriendAdvancedConsumptionModelSwitch(coordinator, entry)
    switch.hass = hass
    switch.async_write_ha_state = lambda: None

    asyncio.run(switch.async_turn_on())

    assert coordinator.advanced_consumption_model_enabled is True
    assert entry.data["advanced_consumption_model_enabled"] is True
    assert runtime_reasons == ["switch-advanced_consumption_model_enabled-updated"]
