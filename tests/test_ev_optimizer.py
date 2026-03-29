"""Unit tests for EVOptimizer — no Home Assistant dependencies."""
from __future__ import annotations

import sys
import os
import types
from datetime import datetime, timedelta, timezone

import pytest

# ---------------------------------------------------------------------------
# Mock the homeassistant package before any SolarFriend import
# ---------------------------------------------------------------------------

def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_mock("homeassistant")
_mock("homeassistant.core",
      HomeAssistant=type("HomeAssistant", (), {}),
      Event=type("Event", (), {}),
      callback=lambda f: f)
_mock("homeassistant.helpers")
_mock("homeassistant.helpers.storage",
      Store=type("Store", (), {}))
_mock("homeassistant.helpers.event",
      async_track_state_change_event=lambda *a, **kw: None)
_DUC = type("DataUpdateCoordinator", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
_CE  = type("CoordinatorEntity", (), {"__class_getitem__": classmethod(lambda cls, item: cls)})
_mock("homeassistant.helpers.update_coordinator",
      DataUpdateCoordinator=_DUC,
      UpdateFailed=Exception,
      CoordinatorEntity=_CE)
_mock("homeassistant.helpers.device_registry",
      DeviceInfo=dict)
_mock("homeassistant.helpers.entity_registry",
      async_get=lambda hass: type("Registry", (), {"entities": {}, "async_remove": lambda self, eid: None})())
_mock("homeassistant.helpers.entity_platform",
      AddEntitiesCallback=type("AddEntitiesCallback", (), {}))
_mock("homeassistant.config_entries",
      ConfigEntry=type("ConfigEntry", (), {}))
_mock("homeassistant.const",
      Platform=type("Platform", (), {"SENSOR": "sensor", "NUMBER": "number", "SWITCH": "switch", "SELECT": "select"}),
      CONF_NAME="name",
      UnitOfEnergy=type("UnitOfEnergy", (), {"KILO_WATT_HOUR": "kWh", "WATT_HOUR": "Wh"}),
      UnitOfPower=type("UnitOfPower", (), {"WATT": "W"}),
      PERCENTAGE="%")
_mock("homeassistant.components")
_mock("homeassistant.components.sensor",
      SensorEntity=type("SensorEntity", (), {}),
      SensorEntityDescription=type("SensorEntityDescription", (), {"__init__": lambda self, **kw: None}),
      SensorDeviceClass=type("SensorDeviceClass", (), {"ENERGY": "energy", "POWER": "power", "BATTERY": "battery"}),
      SensorStateClass=type("SensorStateClass", (), {"MEASUREMENT": "measurement", "TOTAL": "total", "TOTAL_INCREASING": "total_increasing"}))
_mock("homeassistant.util")
_mock("homeassistant.util.dt",
      now=datetime.now,
      as_local=lambda dt: dt,
      UTC=None)

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from custom_components.solarfriend.ev_optimizer import (
    EVContext,
    EVHybridSlot,
    EVOptimizer,
    EVOptimizeResult,
    MIN_CHARGE_AMPS,
    MAX_CHARGE_AMPS,
    VOLTAGE,
    MIN_SURPLUS_W,
    MIN_1PHASE_W,
    MIN_3PHASE_W,
    STOP_THRESHOLD_W,
    _parse_prices,
    _find_cheapest_charge_hours,
    _battery_needs_priority,
    _surplus_w,
    _needed_kwh,
    _needed_charge_hours,
    _should_prioritize_ev_solar,
)
from custom_components.solarfriend.weather_profile import (
    CLEAR_PROFILE,
    CLOUDY_PROFILE,
    PARTLY_CLOUDY_PROFILE,
    classify_weather_profile,
    select_hourly_weather_profile,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 23, 22, 0)
_DEP = datetime(2026, 3, 24, 7, 0)


def make_ctx(**overrides) -> EVContext:
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
        departure=_DEP,
        current_price=1.0,
        raw_prices=[],
        max_charge_kw=7.4,
        now=_NOW,
    )
    defaults.update(overrides)
    return EVContext(**defaults)


def make_prices(prices: list[float], start: datetime | None = None) -> list[dict]:
    base = start or _NOW
    return [
        {"hour": (base + timedelta(hours=i)).isoformat(), "price": p}
        for i, p in enumerate(prices)
    ]


def opt(mode: str = "solar_only", **ctx_overrides) -> EVOptimizeResult:
    return EVOptimizer().optimize(make_ctx(**ctx_overrides), mode=mode)


def build_plan(mode: str = "hybrid", **ctx_overrides) -> list[dict]:
    return EVOptimizer().build_plan(make_ctx(**ctx_overrides), mode=mode)


def calc(surplus: float, max_w: float) -> tuple[bool, int, float, float]:
    """Shortcut to call _calc_phase_and_amps directly."""
    return EVOptimizer()._calc_phase_and_amps(surplus, max_w)


# ---------------------------------------------------------------------------
# _calc_phase_and_amps tests
# ---------------------------------------------------------------------------

def test_phase_calc_below_minimum():
    should_charge, phases, amps, w = calc(surplus=1000, max_w=22000)
    assert should_charge is False
    assert phases == 0
    assert amps == 0.0
    assert w == 0.0


def test_phase_calc_1phase():
    should_charge, phases, amps, w = calc(surplus=2000, max_w=22000)
    assert should_charge is True
    assert phases == 1
    assert amps == pytest.approx(2000 / VOLTAGE, abs=0.2)
    assert w == pytest.approx(amps * VOLTAGE, abs=10)


def test_phase_calc_3phase():
    should_charge, phases, amps, w = calc(surplus=5000, max_w=22000)
    assert should_charge is True
    assert phases == 3
    assert amps == pytest.approx(5000 / 3 / VOLTAGE, abs=0.2)
    assert w == pytest.approx(amps * 3 * VOLTAGE, abs=10)


def test_phase_calc_capped_by_max_charge_w():
    # surplus=15000W but user max=7400W → capped to 7400W (1-fase range)
    should_charge, phases, amps, w = calc(surplus=15000, max_w=7400)
    assert should_charge is True
    assert w == pytest.approx(7400, abs=100)


def test_phase_calc_boundary_1phase_to_3phase():
    # Just below 3-phase threshold → 1-fase
    should_charge, phases, amps, w = calc(surplus=MIN_3PHASE_W - 1, max_w=22000)
    assert phases == 1

    # At 3-phase threshold → 3-fase
    should_charge2, phases2, amps2, w2 = calc(surplus=MIN_3PHASE_W, max_w=22000)
    assert phases2 == 3


def test_phase_calc_max_amps_capped_at_16():
    # Very high surplus → amps never exceed MAX_AMPS
    _, _, amps, _ = calc(surplus=50000, max_w=50000)
    assert amps <= MAX_CHARGE_AMPS


# ---------------------------------------------------------------------------
# Helper-function unit tests
# ---------------------------------------------------------------------------

def test_parse_prices_valid():
    raw = [
        {"hour": "2026-03-23T22:00:00", "price": 1.5},
        {"hour": "2026-03-23T23:00:00", "price": 0.8},
    ]
    parsed = _parse_prices(raw)
    assert len(parsed) == 2
    assert parsed[0][1] == 1.5
    assert parsed[1][1] == 0.8


def test_parse_prices_ignores_bad_entries():
    raw = [
        {"hour": "not-a-date", "price": 1.0},
        {"hour": "2026-03-23T22:00:00", "price": "bad"},
        {"hour": "2026-03-23T23:00:00", "price": 2.0},
    ]
    parsed = _parse_prices(raw)
    assert len(parsed) == 1
    assert parsed[0][1] == 2.0


def test_battery_needs_priority_below_min():
    assert _battery_needs_priority(make_ctx(battery_soc=5.0, battery_min_soc=10.0)) is True


def test_battery_needs_priority_above_min():
    assert _battery_needs_priority(make_ctx(battery_soc=15.0, battery_min_soc=10.0)) is False


def test_surplus_w_no_battery():
    ctx = make_ctx(pv_power_w=4000.0, load_power_w=1000.0, battery_charging_w=0.0)
    assert _surplus_w(ctx) == 3000.0


def test_surplus_w_battery_charging():
    ctx = make_ctx(pv_power_w=4000.0, load_power_w=1000.0, battery_charging_w=-2000.0)
    assert _surplus_w(ctx) == 1000.0


def test_surplus_w_battery_discharging():
    # Batteri aflader 2000W → trækkes fra surplus (vi må ikke lade EV på husbatteri)
    ctx = make_ctx(pv_power_w=4000.0, load_power_w=1000.0, battery_charging_w=2000.0)
    assert _surplus_w(ctx) == 1000.0


def test_surplus_excludes_battery_discharge():
    # sol=3000, load=1000, batteri aflader 500W → surplus = 3000-1000-500 = 1500W
    ctx = make_ctx(pv_power_w=3000.0, load_power_w=1000.0, battery_charging_w=500.0)
    assert _surplus_w(ctx) == pytest.approx(1500.0, abs=1)


def test_surplus_excludes_battery_charging():
    # Battery charging consumes solar unless an active EV solar slot can reclaim it.
    ctx = make_ctx(pv_power_w=3000.0, load_power_w=1000.0, battery_charging_w=-500.0)
    assert _surplus_w(ctx) == pytest.approx(1500.0, abs=1)


def test_surplus_reclaims_battery_charging_only_in_active_ev_slot():
    ctx = make_ctx(
        pv_power_w=3000.0,
        load_power_w=1000.0,
        battery_charging_w=-500.0,
        allow_battery_charge_reclaim=True,
        grid_power_w=0.0,
    )
    assert _surplus_w(ctx) == pytest.approx(2000.0, abs=1)


def test_needed_kwh_calculation():
    ctx = make_ctx(vehicle_soc=50.0, vehicle_target_soc=80.0, vehicle_capacity_kwh=63.0)
    assert abs(_needed_kwh(ctx) - 18.9) < 0.01


def test_needed_charge_hours_rounds_up():
    ctx = make_ctx(vehicle_soc=50.0, vehicle_target_soc=80.0,
                   vehicle_capacity_kwh=63.0, max_charge_kw=7.4)
    assert _needed_charge_hours(ctx) == 3


def test_find_cheapest_hours_picks_cheapest():
    prices = make_prices([3.0, 1.0, 2.0, 0.5, 4.0])
    cheapest = _find_cheapest_charge_hours(prices, _NOW, _DEP, n_hours=2)
    hours_str = {h.strftime("%H") for h in cheapest}
    assert "01" in hours_str
    assert "23" in hours_str


def test_find_cheapest_hours_empty_when_no_prices():
    assert _find_cheapest_charge_hours([], _NOW, _DEP, n_hours=2) == set()


def test_find_cheapest_hours_empty_when_n_zero():
    assert _find_cheapest_charge_hours(make_prices([1.0, 2.0]), _NOW, _DEP, n_hours=0) == set()


def test_ev_solar_priority_when_ev_alt_is_more_expensive():
    assert _should_prioritize_ev_solar(
        ev_alt_grid_price=0.80,
        battery_future_value=0.40,
        recoverable_battery_kwh=3.0,
        reserved_ev_solar_kwh=2.0,
    ) is True


def test_ev_solar_priority_denied_when_battery_has_higher_value():
    assert _should_prioritize_ev_solar(
        ev_alt_grid_price=0.30,
        battery_future_value=1.00,
        recoverable_battery_kwh=4.0,
        reserved_ev_solar_kwh=2.0,
    ) is False


def test_ev_solar_priority_denied_when_battery_cannot_recover():
    assert _should_prioritize_ev_solar(
        ev_alt_grid_price=1.20,
        battery_future_value=0.50,
        recoverable_battery_kwh=1.0,
        reserved_ev_solar_kwh=2.0,
    ) is False


def test_ev_solar_priority_defaults_true_without_comparison_prices():
    assert _should_prioritize_ev_solar(
        ev_alt_grid_price=None,
        battery_future_value=None,
        recoverable_battery_kwh=2.0,
        reserved_ev_solar_kwh=1.0,
    ) is True


# ---------------------------------------------------------------------------
# solar_only mode tests
# ---------------------------------------------------------------------------

def test_solar_only_disconnected():
    result = opt(charger_status="disconnected")
    assert result.should_charge is False
    assert "tilsluttet" in result.reason


def test_solar_only_vehicle_full():
    result = opt(vehicle_soc=80.0, vehicle_target_soc=80.0)
    assert result.should_charge is False
    assert "fuld" in result.reason


def test_solar_only_insufficient_surplus():
    # pv=2000, load=1500 → surplus=500W < MIN_1PHASE_W
    result = opt(pv_power_w=2000, load_power_w=1500)
    assert result.should_charge is False
    assert result.surplus_w == 500.0


def test_solar_only_1phase_charging():
    # surplus=2000W → 1-fase
    result = opt(pv_power_w=3000, load_power_w=1000)
    assert result.should_charge is True
    assert result.phases == 1
    assert result.target_w == pytest.approx(2000 / VOLTAGE * VOLTAGE, abs=10)


def test_solar_only_3phase_charging():
    # surplus=5000W ≥ MIN_3PHASE_W (4230W) → 3-fase
    result = opt(pv_power_w=6000, load_power_w=1000)
    assert result.should_charge is True
    assert result.phases == 3
    assert result.target_amps == pytest.approx(5000 / 3 / VOLTAGE, abs=0.2)


def test_solar_only_capped_by_max_charge_kw():
    # Large surplus but max_charge_kw=3.7 → capped
    result = opt(pv_power_w=12000, load_power_w=1000, max_charge_kw=3.7)
    assert result.should_charge is True
    assert result.target_w <= 3700 + 50   # allow rounding


def test_solar_only_battery_priority():
    # Solar-only EV must leave room for ongoing battery charging by default.
    result = opt(pv_power_w=4000, load_power_w=1000, battery_charging_w=-2000)
    assert result.should_charge is False
    assert result.surplus_w == 1000.0


def test_solar_only_hysteresis_stop():
    # currently_charging=True, surplus=1300W
    # STOP_THRESHOLD_W = 1210W → 1300 > 1210 → continue (at minimum 1-fase)
    result = opt(pv_power_w=2300, load_power_w=1000, currently_charging=True)
    assert result.surplus_w == 1300.0
    assert result.should_charge is True
    assert result.phases == 1
    assert result.target_amps == pytest.approx(MIN_CHARGE_AMPS, abs=0.1)


def test_solar_only_hysteresis_start():
    # currently_charging=False, surplus=1300W < MIN_1PHASE_W (1410W) → don't start
    result = opt(pv_power_w=2300, load_power_w=1000, currently_charging=False)
    assert result.surplus_w == 1300.0
    assert result.should_charge is False


def test_solar_only_uses_profile_start_threshold():
    result = opt(
        pv_power_w=2700.0,
        load_power_w=1000.0,
        solar_only_start_threshold_w=2000.0,
        solar_only_stop_threshold_w=1200.0,
        solar_only_grid_buffer_w=300.0,
        solar_only_profile_name="partly_cloudy",
    )
    assert result.should_charge is False
    assert result.reason == "For lidt sol-overskud (1700W < 2000W)"


def test_solar_only_uses_profile_stop_threshold_when_already_charging():
    result = opt(
        pv_power_w=2300.0,
        load_power_w=1000.0,
        currently_charging=True,
        solar_only_start_threshold_w=2000.0,
        solar_only_stop_threshold_w=1200.0,
        solar_only_grid_buffer_w=300.0,
        solar_only_profile_name="partly_cloudy",
    )
    assert result.should_charge is True
    assert result.solar_only_profile_name == "partly_cloudy"


def test_solar_only_adds_grid_buffer_after_start_condition():
    result = opt(
        pv_power_w=3000.0,
        load_power_w=1000.0,
        solar_only_start_threshold_w=2000.0,
        solar_only_stop_threshold_w=1200.0,
        solar_only_grid_buffer_w=300.0,
        solar_only_profile_name="partly_cloudy",
    )
    assert result.should_charge is True
    assert result.target_w > 2000.0


def test_solar_only_starts_exactly_at_minimum_threshold():
    result = opt(
        pv_power_w=MIN_1PHASE_W + 1000.0,
        load_power_w=1000.0,
        currently_charging=False,
    )
    assert result.surplus_w == pytest.approx(MIN_1PHASE_W, abs=1)
    assert result.should_charge is True
    assert result.phases == 1
    assert result.target_amps == pytest.approx(MIN_CHARGE_AMPS, abs=0.1)


def test_solar_only_continues_exactly_at_stop_threshold():
    result = opt(
        pv_power_w=STOP_THRESHOLD_W + 1000.0,
        load_power_w=1000.0,
        currently_charging=True,
    )
    assert result.surplus_w == pytest.approx(STOP_THRESHOLD_W, abs=1)
    assert result.should_charge is True
    assert result.phases == 1
    assert result.target_amps == pytest.approx(MIN_CHARGE_AMPS, abs=0.1)


def test_solar_only_switches_to_three_phase_at_exact_boundary():
    result = opt(
        pv_power_w=MIN_3PHASE_W + 1000.0,
        load_power_w=1000.0,
        currently_charging=False,
    )
    assert result.surplus_w == pytest.approx(MIN_3PHASE_W, abs=1)
    assert result.should_charge is True
    assert result.phases == 3
    assert result.target_amps == pytest.approx(MIN_CHARGE_AMPS, abs=0.1)


def test_solar_only_does_not_charge_when_max_power_cap_is_below_minimum():
    result = opt(
        pv_power_w=10000.0,
        load_power_w=1000.0,
        max_charge_kw=1.0,
    )
    assert result.surplus_w == pytest.approx(9000.0, abs=1)
    assert result.should_charge is False
    assert "under minimum" in result.reason


def test_solar_only_surplus_can_be_negative():
    result = opt(
        pv_power_w=500.0,
        load_power_w=2000.0,
        battery_charging_w=-1000.0,
    )
    assert result.surplus_w == pytest.approx(-2500.0, abs=1)
    assert result.should_charge is False


def test_solar_only_surplus_treats_charge_and_discharge_differently():
    charging = opt(
        pv_power_w=5000.0,
        load_power_w=1000.0,
        battery_charging_w=-1500.0,
    )
    discharging = opt(
        pv_power_w=5000.0,
        load_power_w=1000.0,
        battery_charging_w=1500.0,
    )
    assert charging.surplus_w == pytest.approx(2500.0, abs=1)
    assert discharging.surplus_w == pytest.approx(2500.0, abs=1)
    assert charging.target_w == pytest.approx(discharging.target_w, abs=1)


def test_solar_only_uses_surplus_strength_for_one_phase_amps():
    result = opt(
        pv_power_w=3500.0,
        load_power_w=1000.0,
        currently_charging=False,
    )
    assert result.should_charge is True
    assert result.phases == 1
    assert result.target_amps == pytest.approx(2500.0 / VOLTAGE, abs=0.2)


def test_solar_only_uses_surplus_strength_for_three_phase_amps():
    result = opt(
        pv_power_w=8500.0,
        load_power_w=1000.0,
        currently_charging=False,
    )
    assert result.should_charge is True
    assert result.phases == 3
    assert result.target_amps == pytest.approx(7500.0 / 3 / VOLTAGE, abs=0.2)


# ---------------------------------------------------------------------------
# hybrid mode tests
# ---------------------------------------------------------------------------

def test_hybrid_uses_solar_when_available():
    result = opt(mode="hybrid", pv_power_w=6000, load_power_w=1000)
    assert result.should_charge is True
    assert "Sol-overskud" in result.reason


def test_hybrid_disconnected():
    result = opt(mode="hybrid", charger_status="disconnected",
                 raw_prices=make_prices([0.5, 0.4, 0.3]))
    assert result.should_charge is False


def test_hybrid_vehicle_full():
    result = opt(mode="hybrid", vehicle_soc=80.0, vehicle_target_soc=80.0,
                 raw_prices=make_prices([0.5, 0.4, 0.3]))
    assert result.should_charge is False


def test_hybrid_battery_needs_priority():
    result = opt(
        mode="hybrid",
        pv_power_w=500, load_power_w=1000,
        battery_soc=5.0, battery_min_soc=10.0,
        raw_prices=make_prices([0.1, 0.2, 0.3]),
    )
    assert result.should_charge is False
    assert "Batteri prioritet" in result.reason


def test_hybrid_charges_during_cheap_hour():
    prices = make_prices([0.1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2])
    result = opt(
        mode="hybrid",
        pv_power_w=500, load_power_w=1000,
        current_price=0.1, raw_prices=prices,
        vehicle_soc=50.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True
    assert "netladning" in result.reason


def test_hybrid_skips_expensive_hour():
    prices = make_prices([5.0, 4.0, 0.1, 0.2, 0.3, 3.0, 2.0, 1.0, 0.5])
    result = opt(
        mode="hybrid",
        pv_power_w=500, load_power_w=1000,
        current_price=5.0, raw_prices=prices,
        vehicle_soc=78.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is False
    assert "Ikke billigste time" in result.reason


def test_hybrid_no_price_data():
    result = opt(mode="hybrid", pv_power_w=500, load_power_w=1000, raw_prices=[])
    assert result.should_charge is False
    assert "prisdata" in result.reason


def test_hybrid_solar_takes_priority_over_grid():
    prices = make_prices([0.1, 0.2, 0.3, 0.4])
    result = opt(mode="hybrid", pv_power_w=6000, load_power_w=1000,
                 current_price=0.1, raw_prices=prices)
    assert result.should_charge is True
    assert "Sol-overskud" in result.reason


def test_hybrid_charges_at_max_power():
    prices = make_prices([0.1] + [1.0] * 8)
    result = opt(
        mode="hybrid",
        pv_power_w=500, load_power_w=1000,
        current_price=0.1, raw_prices=prices,
        max_charge_kw=7.4,
        vehicle_soc=70.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True
    assert result.target_w > 0


# ---------------------------------------------------------------------------
# grid_schedule mode tests
# ---------------------------------------------------------------------------

def test_grid_schedule_disconnected():
    result = opt(mode="grid_schedule", charger_status="disconnected",
                 raw_prices=make_prices([0.1, 0.2, 0.3]))
    assert result.should_charge is False


def test_grid_schedule_vehicle_full():
    result = opt(mode="grid_schedule", vehicle_soc=80.0, vehicle_target_soc=80.0,
                 raw_prices=make_prices([0.1, 0.2, 0.3]))
    assert result.should_charge is False


def test_grid_schedule_battery_priority():
    result = opt(
        mode="grid_schedule",
        battery_soc=5.0, battery_min_soc=10.0,
        raw_prices=make_prices([0.1, 0.2, 0.3]),
    )
    assert result.should_charge is False
    assert "Batteri prioritet" in result.reason


def test_grid_schedule_charges_during_cheapest_hour():
    prices = make_prices([0.1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2])
    result = opt(
        mode="grid_schedule",
        current_price=0.1, raw_prices=prices,
        vehicle_soc=50.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True
    assert "Planlagt netladning" in result.reason


def test_grid_schedule_skips_non_cheapest_hour():
    prices = make_prices([5.0, 4.0, 0.1, 3.0, 2.5, 2.0, 1.5, 1.0, 0.5])
    result = opt(
        mode="grid_schedule",
        current_price=5.0, raw_prices=prices,
        vehicle_soc=79.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is False
    assert "Ikke billigste time" in result.reason


def test_grid_schedule_no_price_data():
    result = opt(mode="grid_schedule", raw_prices=[])
    assert result.should_charge is False
    assert "prisdata" in result.reason


def test_grid_schedule_charges_at_max_power():
    prices = make_prices([0.1] + [2.0] * 8)
    result = opt(
        mode="grid_schedule",
        current_price=0.1, raw_prices=prices,
        vehicle_soc=70.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True
    assert result.phases in (1, 3)


def test_grid_schedule_n_hours_calculation():
    # need ~18.9 kWh @ 7.4 kW → 3 cheapest hours; 22:00 is one of them
    prices = make_prices([0.1, 0.2, 0.3, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
    result = opt(
        mode="grid_schedule",
        current_price=0.1, raw_prices=prices,
        vehicle_soc=50.0, vehicle_capacity_kwh=63.0,
        vehicle_target_soc=80.0, max_charge_kw=7.4,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True


def test_grid_schedule_no_prices_before_departure():
    after_dep = _DEP + timedelta(hours=2)
    prices = make_prices([0.1, 0.2, 0.3], start=after_dep)
    result = opt(mode="grid_schedule", raw_prices=prices, now=_NOW, departure=_DEP)
    assert result.should_charge is False


def test_grid_schedule_plan_prefers_cheapest_slots_by_capacity():
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.60, solar_surplus_w=6000.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=2), duration_h=1.0, price_dkk=0.20, solar_surplus_w=3000.0),
    ]
    plan = build_plan(
        mode="grid_schedule",
        vehicle_soc=50.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["solar_w"] == pytest.approx(0.0, abs=1)
    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(7400.0, abs=1)
    assert plan[2]["grid_w"] == pytest.approx(600.0, abs=1)


def test_grid_schedule_plan_uses_partial_current_hour_capacity():
    now = datetime(2026, 3, 23, 22, 30)
    departure = datetime(2026, 3, 23, 23, 30)
    prices = make_prices([0.10, 0.50], start=datetime(2026, 3, 23, 22, 0))
    plan = build_plan(
        mode="grid_schedule",
        now=now,
        departure=departure,
        current_price=0.10,
        raw_prices=prices,
        vehicle_soc=50.0,
        vehicle_target_soc=100.0,
        vehicle_capacity_kwh=10.0,
        max_charge_kw=7.4,
    )

    assert plan[0]["duration_h"] == pytest.approx(0.5, abs=0.001)
    assert plan[0]["grid_w"] == pytest.approx(7400.0, abs=1)
    assert plan[1]["duration_h"] == pytest.approx(0.5, abs=0.001)
    assert plan[1]["grid_w"] == pytest.approx(2600.0, abs=1)


def test_grid_schedule_plan_caps_last_slot_to_remaining_need():
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.20, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="grid_schedule",
        vehicle_soc=50.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["grid_w"] == pytest.approx(7400.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(600.0, abs=1)
    assert plan[1]["total_w"] == pytest.approx(600.0, abs=1)


def test_grid_schedule_plan_skips_infinite_price_slots():
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=float("inf"), solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="grid_schedule",
        vehicle_soc=70.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(4000.0, abs=1)


def test_grid_schedule_live_decision_matches_current_plan_slot():
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.50, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="grid_schedule",
        vehicle_soc=50.0,
        vehicle_target_soc=80.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )
    result = opt(
        mode="grid_schedule",
        vehicle_soc=50.0,
        vehicle_target_soc=80.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
        current_price=0.50,
    )

    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert result.should_charge is False
    assert "billigste" in result.reason


def test_grid_schedule_live_decision_uses_partial_power_when_last_slot_is_small():
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.20, solar_surplus_w=0.0),
    ]
    result = opt(
        mode="grid_schedule",
        vehicle_soc=70.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
        current_price=0.10,
    )

    assert result.should_charge is True
    assert result.target_w == pytest.approx(3760.0, abs=25)


# ---------------------------------------------------------------------------
# Unknown mode fallback
# ---------------------------------------------------------------------------

def test_unknown_mode_falls_back_to_solar_only():
    result = opt(mode="banana", pv_power_w=6000, load_power_w=1000)
    assert result.should_charge is True
    assert "Sol-overskud" in result.reason


# ---------------------------------------------------------------------------
# Emergency charging (minimum range feature)
# ---------------------------------------------------------------------------

def test_emergency_disabled_when_min_range_zero():
    """min_range_km=0 means feature is off — no emergency even with low range."""
    result = opt(
        pv_power_w=500, load_power_w=1000,  # no solar surplus
        driving_range_km=50.0,
        min_range_km=0.0,
        vehicle_efficiency_km_per_kwh=6.0,
    )
    assert result.is_emergency is False


def test_emergency_disabled_when_no_range_sensor():
    """driving_range_km=None means sensor not configured — no emergency."""
    result = opt(
        pv_power_w=500, load_power_w=1000,
        driving_range_km=None,
        min_range_km=100.0,
        vehicle_efficiency_km_per_kwh=6.0,
    )
    assert result.is_emergency is False


def test_emergency_not_triggered_above_minimum():
    """Range above minimum — normal optimizer runs."""
    result = opt(
        pv_power_w=500, load_power_w=1000,  # no solar surplus
        driving_range_km=150.0,
        min_range_km=100.0,
        vehicle_efficiency_km_per_kwh=6.0,
    )
    assert result.is_emergency is False
    assert result.should_charge is False  # no surplus, so solar_only won't charge


def test_emergency_does_not_override_solar_only():
    """solar_only ignores emergency charging and still requires solar surplus."""
    result = opt(
        pv_power_w=0, load_power_w=1000,  # no solar surplus at all
        driving_range_km=80.0,
        min_range_km=100.0,
        vehicle_efficiency_km_per_kwh=6.0,
        vehicle_capacity_kwh=63.0,
        max_charge_kw=7.4,
    )
    assert result.is_emergency is False
    assert result.should_charge is False
    assert "For lidt sol-overskud" in result.reason


def test_weather_profile_classification_prefers_clear_conditions():
    assert classify_weather_profile(condition="sunny", cloud_coverage=5.0) == CLEAR_PROFILE


def test_weather_profile_classification_prefers_cloud_coverage_when_high():
    assert classify_weather_profile(condition="partlycloudy", cloud_coverage=90.0) == CLOUDY_PROFILE


def test_weather_profile_classification_uses_partly_cloudy_mid_range():
    assert classify_weather_profile(condition="partlycloudy", cloud_coverage=40.0) == PARTLY_CLOUDY_PROFILE


def test_weather_profile_hour_selection_handles_aware_forecast_and_naive_now():
    profile = select_hourly_weather_profile(
        hourly_forecast=[
            {
                "datetime": "2026-03-26T12:00:00+01:00",
                "condition": "partlycloudy",
                "cloud_coverage": 80.0,
            }
        ],
        now=datetime(2026, 3, 26, 11, 15, 0),
    )

    assert profile == CLOUDY_PROFILE


def test_weather_profile_hour_selection_handles_naive_forecast_and_aware_now():
    profile = select_hourly_weather_profile(
        hourly_forecast=[
            {
                "datetime": "2026-03-26T12:00:00",
                "condition": "sunny",
                "cloud_coverage": 5.0,
            }
        ],
        now=datetime(2026, 3, 26, 12, 15, 0, tzinfo=timezone.utc),
    )

    assert profile == CLEAR_PROFILE


def test_emergency_overrides_grid_schedule_expensive_hour():
    """Emergency charging ignores price schedule."""
    prices = make_prices([5.0, 4.0, 0.1, 0.2])  # current hour is expensive
    result = opt(
        mode="grid_schedule",
        pv_power_w=0, load_power_w=1000,
        current_price=5.0, raw_prices=prices,
        driving_range_km=50.0,
        min_range_km=100.0,
        vehicle_efficiency_km_per_kwh=6.0,
        vehicle_capacity_kwh=63.0,
        max_charge_kw=7.4,
    )
    assert result.is_emergency is True
    assert result.should_charge is True


def test_emergency_target_soc_calculation():
    """solar_only keeps the normal target SOC even when range is below minimum."""
    result = opt(
        driving_range_km=50.0,
        min_range_km=90.0,
        vehicle_efficiency_km_per_kwh=6.0,
        vehicle_capacity_kwh=63.0,
        max_charge_kw=7.4,
    )
    assert result.is_emergency is False
    assert result.vehicle_target_soc == pytest.approx(80.0, abs=0.1)


def test_emergency_charging_at_max_power_in_grid_schedule():
    """Emergency charging still uses max charger power outside solar_only."""
    result = opt(
        mode="grid_schedule",
        pv_power_w=0, load_power_w=500,
        driving_range_km=10.0,
        min_range_km=50.0,
        vehicle_efficiency_km_per_kwh=6.0,
        vehicle_capacity_kwh=63.0,
        max_charge_kw=7.4,
    )
    assert result.is_emergency is True
    assert result.phases == 3  # 7400W > 4230W → 3-phase
    assert result.target_w > 0


# ---------------------------------------------------------------------------
# Tests — hybrid adaptive re-evaluation
# ---------------------------------------------------------------------------

def test_deadline_charging_activates():
    """When missing kWh ≥ 90 % of max possible, charge at full speed immediately."""
    # missing = 10 kWh, max_charge = 11 kW, hours = 0.95 → max_possible = 10.45 kWh
    # 10 >= 10.45 * 0.9 = 9.41 → deadline triggers
    now = datetime(2026, 3, 24, 14, 3)
    departure = datetime(2026, 3, 24, 15, 0)   # 0.95 h from now
    result = opt(
        mode="hybrid",
        pv_power_w=0, load_power_w=500,
        vehicle_soc=30.0, vehicle_target_soc=40.0, vehicle_capacity_kwh=100.0,
        max_charge_kw=11.0,
        raw_prices=make_prices([1.5] * 4, start=now),
        current_price=1.5, current_price_dkk=1.5,
        now=now, departure=departure,
    )
    assert result.should_charge is True
    assert "Deadline" in result.reason or "⏰" in result.reason


def test_behind_schedule_sol_can_recover():
    """Deficit > 0.5 kWh but solar forecast covers 1.2× deficit → solar_only path."""
    # expected_soc=60, actual=50, capacity=100 → deficit=10 kWh
    # sol_remaining=12.5 kWh >= 10 * 1.2 = 12 → solar can recover
    result = opt(
        mode="hybrid",
        pv_power_w=0, load_power_w=500,   # no current surplus (solar_only won't charge)
        vehicle_soc=50.0, vehicle_target_soc=80.0, vehicle_capacity_kwh=100.0,
        max_charge_kw=7.4,
        ev_plan_expected_soc_now=60.0,
        solar_forecast_to_departure_kwh=12.5,
        current_price_dkk=1.0,
        raw_prices=[],
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is False   # no surplus right now
    assert "sol indhenter" in result.reason or "☀️" in result.reason


def test_behind_schedule_wait_for_cheaper():
    """Deficit > 0.5 kWh, sol not enough, but cheaper price coming → wait (solar_only)."""
    # expected=60, actual=58, capacity=100 → deficit=2 kWh > 0.5
    # sol=1 kWh < 2*1.2=2.4 → sol not enough
    # current=1.5, cheapest_future=0.3 < 1.5*0.7=1.05 → wait
    future_prices = make_prices([1.5, 1.5, 0.3, 1.4, 1.3], start=_NOW)
    result = opt(
        mode="hybrid",
        pv_power_w=0, load_power_w=500,
        vehicle_soc=58.0, vehicle_target_soc=80.0, vehicle_capacity_kwh=100.0,
        max_charge_kw=7.4,
        ev_plan_expected_soc_now=60.0,
        solar_forecast_to_departure_kwh=1.0,
        current_price=1.5, current_price_dkk=1.5,
        raw_prices=future_prices,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is False
    assert "venter" in result.reason or "⏳" in result.reason


def test_behind_schedule_supplement_grid_now():
    """Deficit > 0.5 kWh, sol not enough, no cheaper future prices → grid supplement."""
    # expected=70, actual=55, capacity=100 → deficit=15 kWh
    # grid_boost_w = 15 / 9h * 1000 = 1667 W > MIN_SURPLUS_W → charges
    # sol=0.5 kWh < 15*1.2=18 → sol not enough
    # current=0.5 kr, all future prices 0.5 → cheapest_future=0.5 >= 0.5*0.7=0.35 → no wait
    prices = make_prices([0.5, 0.5, 0.5, 0.5, 0.6], start=_NOW)
    result = opt(
        mode="hybrid",
        pv_power_w=0, load_power_w=500,
        vehicle_soc=55.0, vehicle_target_soc=80.0, vehicle_capacity_kwh=100.0,
        max_charge_kw=7.4,
        ev_plan_expected_soc_now=70.0,
        solar_forecast_to_departure_kwh=0.5,
        current_price=0.5, current_price_dkk=0.5,
        raw_prices=prices,
        now=_NOW, departure=_DEP,
    )
    assert result.should_charge is True
    assert "supplerer" in result.reason or "⚡" in result.reason


def test_on_schedule_normal_hybrid():
    """No deficit → adaptive checks skipped, normal hybrid logic runs."""
    # vehicle needs 7 kWh → ceil(7/7.4)=1 hour needed
    # cheapest 1 hour = 23:00 (0.1 kr) — current hour 22:00 (2.0 kr) not in cheapest
    # → normal hybrid: not cheapest hour → no grid charge
    prices = make_prices([2.0, 0.1, 2.0, 2.0, 2.0], start=_NOW)
    result = opt(
        mode="hybrid",
        pv_power_w=0, load_power_w=500,
        vehicle_soc=73.0, vehicle_target_soc=80.0, vehicle_capacity_kwh=100.0,
        max_charge_kw=7.4,
        ev_plan_expected_soc_now=73.0,   # on schedule — no deficit
        solar_forecast_to_departure_kwh=0.0,
        current_price=2.0, current_price_dkk=2.0,
        raw_prices=prices,
        now=_NOW, departure=_DEP,
    )
    # 1 cheap hour needed: 23:00 (0.1 kr). Current hour 22:00 (2.0 kr) → not cheapest
    assert result.should_charge is False
    assert "billigste" in result.reason


def test_hybrid_plan_uses_slot_solar_before_grid():
    """Hybrid plan should subtract slot-level solar before allocating grid capacity."""
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.20, solar_surplus_w=4000.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="hybrid",
        vehicle_soc=50.0,
        vehicle_target_soc=100.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=11.0,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["solar_w"] == pytest.approx(4000.0, abs=1)
    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(6000.0, abs=1)


def test_hybrid_plan_caps_grid_headroom_when_solar_present():
    """If solar already fills part of EV power, grid may only use the remaining headroom."""
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.05, solar_surplus_w=4000.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.40, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="hybrid",
        vehicle_soc=50.0,
        vehicle_target_soc=100.0,
        vehicle_capacity_kwh=30.0,
        max_charge_kw=11.0,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["solar_w"] == pytest.approx(4000.0, abs=1)
    assert plan[0]["grid_w"] == pytest.approx(7000.0, abs=1)
    assert plan[0]["total_w"] == pytest.approx(11000.0, abs=1)


def test_hybrid_plan_prefers_cheapest_slots_by_capacity_not_order():
    """Grid should be allocated to the cheapest slots until the needed capacity is covered."""
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.50, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=2), duration_h=1.0, price_dkk=0.20, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="hybrid",
        vehicle_soc=50.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(7400.0, abs=1)
    assert plan[2]["grid_w"] == pytest.approx(600.0, abs=1)


def test_hybrid_plan_respects_battery_reserved_solar_via_slot_input():
    """Reserved battery charging should reduce the solar surplus exposed to EV planning."""
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=0.10, solar_surplus_w=1000.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.20, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="hybrid",
        vehicle_soc=60.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=11.0,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["solar_w"] == pytest.approx(0.0, abs=1)
    assert plan[0]["grid_w"] == pytest.approx(6000.0, abs=1)


def test_hybrid_plan_skips_infinite_price_slots_when_no_price_known():
    """Slots without price data should not be treated as cheap grid slots."""
    slots = [
        EVHybridSlot(start=_NOW, duration_h=1.0, price_dkk=float("inf"), solar_surplus_w=0.0),
        EVHybridSlot(start=_NOW + timedelta(hours=1), duration_h=1.0, price_dkk=0.10, solar_surplus_w=0.0),
    ]
    plan = build_plan(
        mode="hybrid",
        vehicle_soc=70.0,
        vehicle_target_soc=90.0,
        vehicle_capacity_kwh=20.0,
        max_charge_kw=7.4,
        raw_prices=[],
        hybrid_slots=slots,
    )

    assert plan[0]["grid_w"] == pytest.approx(0.0, abs=1)
    assert plan[1]["grid_w"] == pytest.approx(4000.0, abs=1)
