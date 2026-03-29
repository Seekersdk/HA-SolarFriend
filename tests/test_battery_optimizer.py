"""Unit tests for BatteryOptimizer — no Home Assistant dependencies."""
from __future__ import annotations

from functools import lru_cache
import sys
import types
from datetime import datetime, timedelta

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

# ha_dt.now() / as_local() used in forecast_adapter at module import level
_ha_dt = _mock("homeassistant.util.dt",
               now=datetime.now,
               as_local=lambda dt: dt,
               UTC=None)

# ---------------------------------------------------------------------------
# Import the module under test (relative imports resolved via sys.path)
# ---------------------------------------------------------------------------

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from custom_components.solarfriend.battery_optimizer import (  # noqa: E402
    BatteryOptimizer,
    OptimizeResult,
)

# ---------------------------------------------------------------------------
# Test fixtures and helpers
# ---------------------------------------------------------------------------

SUNRISE = datetime(2026, 3, 22, 6, 30, 0)
SUNSET  = datetime(2026, 3, 22, 19, 0, 0)


class _MockEntry:
    def __init__(self, data: dict) -> None:
        self.data = data


class _MockTracker:
    def __init__(self, weighted_cost: float = 0.0, solar_fraction: float = 0.5) -> None:
        self.weighted_cost = weighted_cost
        self.solar_fraction = solar_fraction


class _MockProfile:
    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        return 500.0  # constant 500 W for all hours


class _ScheduledProfile:
    def __init__(self, default_watt: float = 500.0, overrides: dict[int, float] | None = None) -> None:
        self._default_watt = default_watt
        self._overrides = overrides or {}

    def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
        return self._overrides.get(hour, self._default_watt)


def make_optimizer(
    weighted_cost: float = 0.0,
    extra_config: dict | None = None,
    profile: object | None = None,
) -> BatteryOptimizer:
    """Return a BatteryOptimizer with standard test configuration."""
    config = {
        "battery_capacity_kwh": 10.0,
        "battery_min_soc":      10.0,
        "battery_max_soc":      90.0,
        "battery_cost_per_kwh": 0.25,
        "min_charge_saving":    0.20,
        "charge_rate_kw":       3.6,
    }
    if extra_config:
        config.update(extra_config)
    return BatteryOptimizer(_MockEntry(config), _MockTracker(weighted_cost), profile or _MockProfile())


def make_prices(cheap_hour: int = 2, expensive_hour: int = 18) -> list[dict]:
    """Return 24 hourly price slots with realistic day/night spread."""
    result = []
    for h in range(24):
        if h == cheap_hour:
            p = 0.20
        elif h == expensive_hour:
            p = 1.80
        elif 0 <= h < 6:
            p = 0.40
        elif 6 <= h < 14:
            p = 0.70
        elif 17 <= h < 21:
            p = 1.40
        else:
            p = 0.60
        result.append({"hour": h, "price": p})
    return result


def make_forecast(peak_hour: int = 12, peak_kw: float = 4.5) -> list[dict]:
    """Return 48 half-hour forecast slots with a bell-curve solar profile (06:00–19:00)."""
    slots = []
    base = datetime(2026, 3, 22, 0, 0, 0)
    for h in range(24):
        for m in (0, 30):
            t = h + m / 60.0
            if 6.0 <= t <= 19.0:
                dist = abs(t - peak_hour)
                kw = max(0.0, peak_kw * (1.0 - (dist / 7.0) ** 2))
            else:
                kw = 0.0
            slots.append({
                "period_start":      base.replace(hour=h, minute=m),
                "pv_estimate_kwh":   round(kw * 0.5, 4),   # kW × 0.5 h = kWh
                "pv_estimate10_kwh": round(kw * 0.5 * 0.7, 4),
                "pv_estimate90_kwh": round(kw * 0.5 * 1.3, 4),
            })
    return slots


def make_flat_forecast(slots_kwh: float, start_hour: int, n_slots: int) -> list[dict]:
    """Return a minimal forecast: n_slots × 30 min each with slots_kwh, rest zero."""
    base = datetime(2026, 3, 22, 0, 0, 0)
    result = []
    for h in range(24):
        for m in (0, 30):
            slot_index = (h * 2 + m // 30) - start_hour * 2
            kwh = slots_kwh if 0 <= slot_index < n_slots else 0.0
            result.append({
                "period_start":      base.replace(hour=h, minute=m),
                "pv_estimate_kwh":   round(kwh, 4),
                "pv_estimate10_kwh": round(kwh * 0.7, 4),
                "pv_estimate90_kwh": round(kwh * 1.3, 4),
            })
    return result


def run(now: datetime, soc: float, pv_w: float = 0.0, load_w: float = 500.0,
        prices: list | None = None, forecast: list | None = None,
        weighted_cost: float = 0.0,
        sell_prices: list | None = None,
        reserved_solar_kwh: dict | None = None,
        profile: object | None = None) -> OptimizeResult:
    """Convenience wrapper around BatteryOptimizer.optimize()."""
    opt = make_optimizer(weighted_cost=weighted_cost, profile=profile)
    return opt.optimize(
        now=now,
        pv_power=pv_w,
        load_power=load_w,
        current_soc=soc,
        raw_prices=prices if prices is not None else make_prices(),
        forecast_today_kwh=5.0,
        forecast_tomorrow_kwh=5.0,
        sunrise_time=SUNRISE,
        sunset_time=SUNSET,
        is_weekend=False,
        hourly_forecast=forecast,
        reserved_solar_kwh=reserved_solar_kwh,
        raw_sell_prices=sell_prices,
    )


def run_with_plan(
    now: datetime,
    soc: float,
    pv_w: float = 0.0,
    load_w: float = 500.0,
    prices: list | None = None,
    forecast: list | None = None,
    weighted_cost: float = 0.0,
    sell_prices: list | None = None,
    extra_config: dict | None = None,
    reserved_solar_kwh: dict | None = None,
    profile: object | None = None,
) -> tuple[OptimizeResult, list[dict]]:
    """Run optimizer and also return the internal horizon plan."""
    opt = make_optimizer(weighted_cost=weighted_cost, extra_config=extra_config, profile=profile)
    result = opt.optimize(
        now=now,
        pv_power=pv_w,
        load_power=load_w,
        current_soc=soc,
        raw_prices=prices if prices is not None else make_prices(),
        forecast_today_kwh=5.0,
        forecast_tomorrow_kwh=5.0,
        sunrise_time=SUNRISE,
        sunset_time=SUNSET,
        is_weekend=False,
        hourly_forecast=forecast,
        reserved_solar_kwh=reserved_solar_kwh,
        raw_sell_prices=sell_prices,
    )
    return result, opt.get_last_plan()


def _explicit_prices(start: datetime, prices: list[float]) -> list[dict]:
    """Build explicit hourly price slots starting from start."""
    return [
        {"hour": (start + timedelta(hours=offset)).isoformat(), "price": price}
        for offset, price in enumerate(prices)
    ]


def _explicit_forecast(start: datetime, hourly_kwh: dict[datetime, float]) -> list[dict]:
    """Build half-hour forecast slots from explicit hourly totals."""
    slots: list[dict] = []
    for hour_start, kwh in sorted(hourly_kwh.items()):
        half_kwh = kwh / 2.0
        for minute in (0, 30):
            slot_start = hour_start.replace(minute=minute, second=0, microsecond=0)
            slots.append({
                "period_start": slot_start,
                "pv_estimate_kwh": round(half_kwh, 4),
                "pv_estimate10_kwh": round(half_kwh * 0.7, 4),
                "pv_estimate90_kwh": round(half_kwh * 1.3, 4),
            })
    return slots


def _plan_cost(plan: list[dict], battery_cost_per_kwh: float) -> float:
    """Return the planner's total economic cost under the same assumptions as the optimizer."""
    total = 0.0
    for slot in plan:
        grid_import_kwh = float(slot["grid_import_w"]) / 1000.0
        grid_charge_kwh = float(slot["grid_charge_w"]) / 1000.0
        total += grid_import_kwh * float(slot["price_dkk"])
        total += grid_charge_kwh * battery_cost_per_kwh
    return round(total, 6)


def _bruteforce_optimal_cost(
    prices: list[float],
    *,
    initial_stored_kwh: float,
    load_kwh: float,
    charge_rate_kwh: float,
    usable_capacity_kwh: float,
    battery_cost_per_kwh: float,
) -> float:
    """Brute-force the optimal plan cost for a tiny horizon with discrete 0.5 kWh actions."""
    quantum = 0.5

    @lru_cache(maxsize=None)
    def solve(index: int, stored_units: int) -> float:
        if index >= len(prices):
            return 0.0

        stored_kwh = stored_units * quantum
        best = float("inf")
        max_charge_units = int(min(charge_rate_kwh, usable_capacity_kwh - stored_kwh) / quantum + 1e-9)
        max_discharge_units = int(min(charge_rate_kwh, load_kwh, stored_kwh) / quantum + 1e-9)

        for discharge_units in range(max_discharge_units + 1):
            for charge_units in range(max_charge_units + 1):
                if discharge_units and charge_units:
                    continue
                discharge_kwh = discharge_units * quantum
                charge_kwh = charge_units * quantum
                next_stored_kwh = stored_kwh - discharge_kwh + charge_kwh
                grid_import_kwh = max(0.0, load_kwh - discharge_kwh) + charge_kwh
                step_cost = grid_import_kwh * prices[index] + charge_kwh * battery_cost_per_kwh
                total_cost = step_cost + solve(index + 1, int(round(next_stored_kwh / quantum)))
                best = min(best, total_cost)

        return best

    return round(solve(0, int(round(initial_stored_kwh / quantum))), 6)


def _assert_plan_invariants(
    plan: list[dict],
    *,
    min_soc: float,
    max_soc: float,
    battery_capacity_kwh: float,
) -> None:
    """Validate core physical invariants for each plan slot."""
    prev_soc = None
    for slot in plan:
        soc_start = float(slot["soc_start"])
        soc_end = float(slot["soc"])
        solar_charge_kwh = float(slot["solar_charge_w"]) / 1000.0
        grid_charge_kwh = float(slot["grid_charge_w"]) / 1000.0
        discharge_kwh = float(slot["discharge_w"]) / 1000.0
        grid_import_kwh = float(slot["grid_import_w"]) / 1000.0

        assert min_soc - 1e-6 <= soc_start <= max_soc + 1e-6
        assert min_soc - 1e-6 <= soc_end <= max_soc + 1e-6
        assert not (grid_charge_kwh > 0 and discharge_kwh > 0)
        assert grid_import_kwh >= 0.0

        expected_delta_soc = ((solar_charge_kwh + grid_charge_kwh - discharge_kwh) / battery_capacity_kwh) * 100.0
        assert abs((soc_end - soc_start) - expected_delta_soc) <= 0.11

        if prev_soc is not None:
            assert abs(prev_soc - soc_start) <= 0.11
        prev_soc = soc_end


REFERENCE_CASES = [
    {
        "name": "flat_prices_no_arbitrage",
        "prices": [0.50, 0.50, 0.50, 0.50],
        "soc": 25.0,
    },
    {
        "name": "single_late_peak",
        "prices": [0.30, 0.20, 0.20, 1.20],
        "soc": 25.0,
    },
    {
        "name": "free_slot_before_peak",
        "prices": [0.60, 0.00, 0.25, 1.30],
        "soc": 25.0,
    },
    {
        "name": "reserve_for_higher_peak",
        "prices": [0.60, 1.10, 0.05, 1.40],
        "soc": 25.0,
    },
    {
        "name": "double_peak_with_mid_cheap",
        "prices": [0.80, 0.15, 1.00, 0.10],
        "soc": 25.0,
    },
    {
        "name": "cheap_start_then_expensive_tail",
        "prices": [0.05, 0.25, 1.00, 1.10],
        "soc": 0.0,
    },
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_idle_when_battery_full_and_no_sol():
    """High SOC, no solar, weighted_cost above any daytime price → IDLE."""
    # weighted_cost = 0.80 is above all prices in make_prices() during the day
    # → TRIN 3 fires: grid price (0.70) <= weighted_cost (0.80) → IDLE
    now = datetime(2026, 3, 22, 12, 0, 0)
    result = run(now, soc=85.0, weighted_cost=0.80)
    assert result.strategy == "IDLE", f"Expected IDLE, got {result.strategy}: {result.reason}"


def test_reserved_ev_solar_is_removed_from_battery_forecast():
    """Battery plan should not count solar that is explicitly reserved for EV before departure."""
    now = datetime(2026, 3, 22, 10, 0, 0)
    forecast = make_flat_forecast(slots_kwh=0.5, start_hour=11, n_slots=2)  # 1.0 kWh in 11:00 hour

    _, normal_plan = run_with_plan(now, soc=20.0, forecast=forecast)
    _, reserved_plan = run_with_plan(
        now,
        soc=20.0,
        forecast=forecast,
        reserved_solar_kwh={datetime(2026, 3, 22, 11, 0, 0): 1.0},
    )

    normal_slot = next(slot for slot in normal_plan if slot["hour_str"] == "11:00")
    reserved_slot = next(slot for slot in reserved_plan if slot["hour_str"] == "11:00")

    assert normal_slot["forecast_solar_w"] == 1000
    assert reserved_slot["forecast_solar_w"] == 0


def test_charge_night_when_low_soc():
    """Low SOC at night + cheap price + profitable peak next day → CHARGE_NIGHT."""
    now = datetime(2026, 3, 22, 2, 0, 0)   # 02:00 nat
    result = run(now, soc=15.0)
    assert result.strategy == "CHARGE_NIGHT", \
        f"Expected CHARGE_NIGHT, got {result.strategy}: {result.reason}"
    assert result.target_soc is not None and result.target_soc > 10.0, \
        f"target_soc should be above min_soc, got {result.target_soc}"
    assert result.cheapest_charge_hour is not None, \
        "cheapest_charge_hour should be set"


def test_save_solar_when_sol_incoming():
    """solar_next_2h = 1.5 kWh (> 1.0 threshold) during the day → SAVE_SOLAR.

    SOC is kept near min_soc so SELL_BATTERY is not triggered
    (sellable_kwh would be ≤ 0).
    """
    now = datetime(2026, 3, 22, 11, 0, 0)
    # 4 slots × 0.375 kWh = 1.5 kWh in next 2 hours
    forecast = make_flat_forecast(slots_kwh=0.375, start_hour=11, n_slots=4)
    result = run(now, soc=11.0, forecast=forecast)   # soc barely above min_soc
    assert result.strategy == "SAVE_SOLAR", \
        f"Expected SAVE_SOLAR, got {result.strategy}: {result.reason}"


def test_no_midday_sell_battery_without_next_day_prices():
    """After noon, missing post-midnight prices must block battery export planning.

    now=12:00 — solar has been up since 07:30 (SUNRISE+1h), so load_until_solar=0.
    Without prices beyond midnight, export should stay disabled even if the current slot is expensive.
    """
    now = datetime(2026, 3, 22, 12, 0, 0)
    prices = make_prices(cheap_hour=2, expensive_hour=12)
    result = run(now, soc=80.0, prices=prices, forecast=None)
    assert result.strategy != "SELL_BATTERY", \
        f"Expected no SELL_BATTERY, got {result.strategy}: {result.reason}"


def test_sell_battery_is_blocked_when_battery_is_more_valuable_later_before_recharge():
    """Do not sell now when the same battery energy avoids more expensive imports before recharge."""
    now = datetime(2026, 3, 22, 0, 0, 0)
    prices = _explicit_prices(
        now,
        [
            0.52,  # sell now
            0.90,  # hold battery for this expensive night slot
            0.85,  # and this one
            0.10,  # cheap grid recharge opens here
            0.10,
            0.10,
        ],
    )
    opt = make_optimizer(weighted_cost=0.0)
    result = opt.optimize(
        now=now,
        pv_power=0.0,
        load_power=500.0,
        current_soc=30.0,
        raw_prices=prices,
        raw_sell_prices=[{**slot, "price": 0.52} for slot in prices],
        forecast_today_kwh=0.0,
        forecast_tomorrow_kwh=0.0,
        sunrise_time=SUNRISE,
        sunset_time=SUNSET,
        is_weekend=False,
        hourly_forecast=None,
    )

    assert result.strategy != "SELL_BATTERY", result.reason


def test_sell_battery_sells_only_energy_not_needed_before_next_recharge():
    """With export blocked, current-slot self-use can still choose ordinary USE_BATTERY."""
    now = datetime(2026, 3, 22, 12, 0, 0)
    prices = _explicit_prices(
        now,
        [
            0.52,  # sell now
            0.25,
            0.25,
            0.25,
            0.20,  # cheap recharge later
            0.20,
        ],
    )
    opt = make_optimizer(weighted_cost=0.0)
    result = opt.optimize(
        now=now,
        pv_power=0.0,
        load_power=500.0,
        current_soc=80.0,
        raw_prices=prices,
        raw_sell_prices=[{**slot, "price": 0.52} for slot in prices],
        forecast_today_kwh=0.0,
        forecast_tomorrow_kwh=0.0,
        sunrise_time=SUNRISE,
        sunset_time=SUNSET,
        is_weekend=False,
        hourly_forecast=None,
    )

    assert result.strategy == "USE_BATTERY", result.reason
    assert result.expected_saving_dkk > 0.0
    plan = opt.get_last_plan()
    assert float(plan[0]["battery_export_w"]) == 0
    assert float(plan[0]["sell_price_dkk"]) == 0.52


def test_sell_battery_requires_minimum_spread_over_weighted_cost():
    """Realtime export should be blocked when sell spread is below min_charge_saving."""
    now = datetime(2026, 3, 22, 0, 0, 0)
    prices = _explicit_prices(now, [0.52, 0.10, 0.10, 0.10])
    opt = make_optimizer(weighted_cost=0.35, profile=_ScheduledProfile(default_watt=0.0))
    result = opt.optimize(
        now=now,
        pv_power=0.0,
        load_power=0.0,
        current_soc=60.0,
        raw_prices=prices,
        raw_sell_prices=[{**slot, "price": 0.52} for slot in prices],
        forecast_today_kwh=0.0,
        forecast_tomorrow_kwh=0.0,
        sunrise_time=SUNRISE,
        sunset_time=SUNSET,
        is_weekend=False,
        hourly_forecast=None,
    )

    plan = opt.get_last_plan()
    assert result.strategy != "SELL_BATTERY", result.reason
    assert float(plan[0]["battery_export_w"]) == 0.0


def test_sell_battery_does_not_steal_upcoming_solar_export_value():
    """Do not export battery now if the next solar surplus can be sold at a better price directly."""
    now = datetime(2026, 3, 22, 15, 0, 0)
    prices = _explicit_prices(now, [0.20, 0.20, 0.20, 0.20])
    sell_prices = _explicit_prices(now, [0.235, 0.436, 0.10, 0.10])
    forecast = _explicit_forecast(
        now,
        {
            datetime(2026, 3, 22, 16, 0, 0): 2.5,
        },
    )
    profile = _ScheduledProfile(default_watt=500.0)

    result, plan = run_with_plan(
        now,
        soc=90.0,
        prices=prices,
        sell_prices=sell_prices,
        forecast=forecast,
        profile=profile,
    )

    assert float(plan[0]["battery_export_w"]) == 0.0
    assert result.strategy != "SELL_BATTERY", result.reason


def test_use_battery_only_requires_price_above_weighted_cost():
    """Realtime discharge should trigger as soon as current price beats weighted cost."""
    now = datetime(2026, 3, 22, 12, 0, 0)
    prices = make_prices(cheap_hour=2, expensive_hour=12)
    result = run(now, soc=80.0, prices=prices, forecast=None, weighted_cost=1.65)

    assert result.strategy == "USE_BATTERY", result.reason


def test_plan_never_exports_battery_in_same_slot_as_solar_charge():
    """Battery export must be blocked in any slot that also charges from solar."""
    now = datetime(2026, 3, 22, 15, 0, 0)
    prices = _explicit_prices(now, [0.20, 0.20, 0.20, 0.20])
    sell_prices = _explicit_prices(now, [0.40, 0.60, 0.20, 0.20])
    forecast = _explicit_forecast(
        now,
        {
            datetime(2026, 3, 22, 15, 0, 0): 2.0,
            datetime(2026, 3, 22, 16, 0, 0): 2.0,
        },
    )
    profile = _ScheduledProfile(default_watt=500.0)

    _, plan = run_with_plan(
        now,
        soc=80.0,
        prices=prices,
        sell_prices=sell_prices,
        forecast=forecast,
        profile=profile,
    )

    for slot in plan:
        if float(slot["solar_charge_w"]) > 0:
            assert float(slot["battery_export_w"]) == 0.0, slot


def test_idle_when_saving_too_small():
    """Night + SOC barely low + spread between cheap and peak too small → IDLE."""
    # Flat prices: no price spread means no profitable charging
    flat_prices = [{"hour": h, "price": 0.50} for h in range(24)]
    now = datetime(2026, 3, 22, 2, 0, 0)
    result = run(now, soc=15.0, prices=flat_prices)
    assert result.strategy == "IDLE", \
        f"Expected IDLE with flat prices, got {result.strategy}: {result.reason}"


def test_charge_night_picks_cheapest_hour():
    """Cheapest charge hour in prices is 02:00 → optimizer picks it."""
    now = datetime(2026, 3, 22, 22, 0, 0)
    prices = make_prices(cheap_hour=2, expensive_hour=18)
    result = run(now, soc=15.0, prices=prices)
    assert result.strategy == "CHARGE_NIGHT"
    assert result.cheapest_charge_hour == "02:00", \
        f"Expected cheapest at 02:00, got {result.cheapest_charge_hour}"


def test_target_soc_covers_morning_need():
    """target_soc should cover at least min_soc + (morning_need / capacity × 100)."""
    now = datetime(2026, 3, 22, 22, 0, 0)
    prices = make_prices(cheap_hour=2, expensive_hour=18)
    result = run(now, soc=5.0, prices=prices)
    assert result.strategy == "CHARGE_NIGHT"
    # morning_need_kwh exposed on result
    min_expected_soc = 10.0 + (result.morning_need_kwh / 10.0) * 100.0
    assert result.target_soc is not None
    assert result.target_soc >= min_expected_soc - 1.0, (   # -1% tolerance
        f"target_soc={result.target_soc:.1f}% < expected≥{min_expected_soc:.1f}%"
    )


def test_no_crash_when_no_forecast():
    """hourly_forecast=None must not raise — returns a valid OptimizeResult."""
    now = datetime(2026, 3, 22, 14, 0, 0)
    result = run(now, soc=50.0, forecast=None)
    assert isinstance(result, OptimizeResult)
    assert result.strategy in {
        "IDLE", "SAVE_SOLAR", "USE_BATTERY", "SELL_BATTERY",
        "CHARGE_GRID", "CHARGE_NIGHT",
    }


def test_no_crash_when_no_prices():
    """raw_prices=[] must not raise — returns IDLE."""
    now = datetime(2026, 3, 22, 14, 0, 0)
    opt = make_optimizer()
    result = opt.optimize(
        now=now, pv_power=0.0, load_power=500.0, current_soc=50.0,
        raw_prices=[],
        forecast_today_kwh=5.0, forecast_tomorrow_kwh=5.0,
        sunrise_time=SUNRISE, sunset_time=SUNSET,
        is_weekend=False,
    )
    assert isinstance(result, OptimizeResult)
    assert result.strategy == "IDLE", \
        f"Expected IDLE with no prices, got {result.strategy}"


# ---------------------------------------------------------------------------
# Anti-eksport tests (negativ/nul spotpris)
# ---------------------------------------------------------------------------

def _make_prices_with_hour(hour: int, price: float) -> list[dict]:
    """Return 24 price slots where `hour` has `price`, rest default positive."""
    result = []
    for h in range(24):
        p = price if h == hour else 0.60
        result.append({"hour": h, "price": p})
    return result


def test_negative_price_anti_export():
    """Negativ spotpris i nuværende time → NEGATIVE_IMPORT med solar_sell=False."""
    now = datetime(2026, 3, 22, 14, 0, 0)  # midt på dagen (dagtid)
    prices = _make_prices_with_hour(14, -0.05)
    result = run(now, soc=50.0, prices=prices)
    assert result.strategy == "NEGATIVE_IMPORT", \
        f"Expected NEGATIVE_IMPORT ved negativ pris, got {result.strategy}: {result.reason}"
    assert result.solar_sell is False, \
        "solar_sell skal være False ved NEGATIVE_IMPORT"
    assert result.charge_now is False, \
        "charge_now skal være False ved NEGATIVE_IMPORT"


def test_zero_price_anti_export():
    """Nul spotpris (0.0) i nuværende time → ANTI_EXPORT."""
    now = datetime(2026, 3, 22, 10, 0, 0)
    prices = _make_prices_with_hour(10, 0.0)
    result = run(now, soc=50.0, prices=prices)
    assert result.strategy == "ANTI_EXPORT", \
        f"Expected ANTI_EXPORT ved nul pris, got {result.strategy}: {result.reason}"
    assert result.solar_sell is False


def test_positive_price_no_anti_export():
    """Positiv spotpris → ANTI_EXPORT må ikke vælges."""
    now = datetime(2026, 3, 22, 14, 0, 0)
    prices = _make_prices_with_hour(14, 0.50)
    result = run(now, soc=50.0, prices=prices)
    assert result.strategy != "ANTI_EXPORT", \
        f"ANTI_EXPORT må ikke vælges ved positiv pris, got {result.strategy}"
    assert result.solar_sell is True, \
        "solar_sell skal være True ved normal strategi"


def test_horizon_uses_battery_before_free_night_charge():
    """Battery should be discharged in the best evening slot before a free charge window."""
    now = datetime(2026, 3, 22, 18, 0, 0)
    prices = []
    start = now.replace(minute=0, second=0, microsecond=0)
    for offset in range(0, 27):
        slot = start + timedelta(hours=offset)
        price = 0.35
        if slot == datetime(2026, 3, 22, 18, 0, 0):
            price = 1.40
        elif slot == datetime(2026, 3, 23, 2, 0, 0) or slot == datetime(2026, 3, 23, 3, 0, 0):
            price = 0.00
        elif slot == datetime(2026, 3, 23, 18, 0, 0):
            price = 0.90
        elif 6 <= slot.hour <= 11:
            price = 0.10
        prices.append({"hour": slot.isoformat(), "price": price})

    result, plan = run_with_plan(now, soc=15.0, prices=prices, forecast=None)

    assert result.strategy in {"SELL_BATTERY", "CHARGE_NIGHT", "USE_BATTERY"}
    plan_by_hour = {slot["hour_str"]: slot for slot in plan}
    assert plan_by_hour["18:00"]["discharge_w"] > 0
    assert plan_by_hour["02:00"]["grid_charge_w"] > 0 or plan_by_hour["03:00"]["grid_charge_w"] > 0


def test_no_sell_battery_after_noon_without_prices_beyond_midnight():
    """After noon, battery export must stay off until tomorrow prices are known."""
    now = datetime(2026, 3, 22, 19, 0, 0)
    prices = _explicit_prices(now, [1.20, 1.10, 1.00, 0.90, 0.80])
    sell_prices = _explicit_prices(now, [0.80, 0.75, 0.70, 0.65, 0.60])

    result, plan = run_with_plan(
        now,
        soc=90.0,
        prices=prices,
        sell_prices=sell_prices,
        forecast=None,
        profile=_ScheduledProfile(default_watt=0.0),
    )

    assert result.strategy != "SELL_BATTERY", result.reason
    assert all(float(slot["battery_export_w"]) == 0.0 for slot in plan), plan


def test_horizon_skips_battery_use_during_cheap_morning():
    """Cheap morning slots should be served from grid when better discharge value exists later."""
    now = datetime(2026, 3, 22, 23, 0, 0)
    prices = []
    for h in range(24):
        price = 0.35
        if h in (2, 3):
            price = 0.00
        elif 6 <= h <= 11:
            price = 0.10
        elif h == 18:
            price = 0.70
        prices.append({"hour": h, "price": price})

    result, plan = run_with_plan(now, soc=15.0, prices=prices, forecast=None)

    assert result.strategy in {"IDLE", "CHARGE_NIGHT", "SAVE_SOLAR"}
    plan_by_hour = {slot["hour_str"]: slot for slot in plan}
    assert plan_by_hour["06:00"]["discharge_w"] == 0
    assert plan_by_hour["18:00"]["discharge_w"] > 0


def test_horizon_limits_grid_charge_when_solar_will_cover_need():
    """Strong daytime solar surplus should reduce or remove planned night charging."""
    now = datetime(2026, 3, 22, 23, 0, 0)
    prices = []
    start = now.replace(minute=0, second=0, microsecond=0)
    for offset in range(0, 24):
        slot = start + timedelta(hours=offset)
        price = 0.35
        if slot == datetime(2026, 3, 23, 2, 0, 0):
            price = 0.20
        elif datetime(2026, 3, 23, 17, 0, 0) <= slot <= datetime(2026, 3, 23, 21, 0, 0):
            price = 1.80
        prices.append({"hour": slot.isoformat(), "price": price})

    forecast = []
    for hour in range(10, 16):
        forecast.append({
            "period_start": datetime(2026, 3, 23, hour, 0, 0),
            "pv_estimate_kwh": 1.0,
            "pv_estimate10_kwh": 0.7,
            "pv_estimate90_kwh": 1.3,
        })

    result, plan = run_with_plan(now, soc=15.0, prices=prices, forecast=forecast)
    _, plan_without_forecast = run_with_plan(now, soc=15.0, prices=prices, forecast=None)

    planned_grid_charge_kwh = sum(slot["grid_charge_w"] for slot in plan) / 1000.0
    baseline_grid_charge_kwh = sum(slot["grid_charge_w"] for slot in plan_without_forecast) / 1000.0
    assert planned_grid_charge_kwh < baseline_grid_charge_kwh, (
        f"Expected solar forecast to reduce grid charge, got forecast={planned_grid_charge_kwh:.2f} "
        f"vs baseline={baseline_grid_charge_kwh:.2f} kWh"
    )
    assert result.strategy in {"IDLE", "SAVE_SOLAR", "CHARGE_NIGHT"}


def test_horizon_does_not_fill_battery_for_small_future_need():
    """Planner may charge extra for arbitrage, but should not fill the battery unnecessarily."""
    now = datetime(2026, 3, 22, 22, 0, 0)
    prices = []
    for h in range(24):
        price = 0.25
        if h == 2:
            price = 0.05
        elif h == 18:
            price = 0.60
        prices.append({"hour": h, "price": price})

    result, plan = run_with_plan(
        now,
        soc=20.0,
        prices=prices,
        forecast=None,
        extra_config={"cheap_grid_threshold": 0.10},
    )

    planned_grid_charge_kwh = sum(slot["grid_charge_w"] for slot in plan) / 1000.0
    assert planned_grid_charge_kwh <= 3.0, f"Expected bounded charge need, got {planned_grid_charge_kwh:.2f} kWh"
    assert result.target_soc is None or result.target_soc < 80.0


def test_horizon_prefers_later_higher_value_slot_over_current_slot():
    """Battery should be reserved when a later slot has higher value than the current one."""
    now = datetime(2026, 3, 22, 17, 0, 0)
    prices = []
    for h in range(24):
        price = 0.30
        if h == 17:
            price = 0.65
        elif h == 18:
            price = 0.95
        prices.append({"hour": h, "price": price})

    result, plan = run_with_plan(now, soc=15.0, prices=prices, forecast=None)

    plan_by_hour = {slot["hour_str"]: slot for slot in plan}
    assert plan_by_hour["17:00"]["discharge_w"] == 0
    assert plan_by_hour["18:00"]["discharge_w"] > 0
    assert result.strategy in {"IDLE", "SAVE_SOLAR", "CHARGE_GRID", "CHARGE_NIGHT", "SELL_BATTERY"}


def test_plan_invariants_hold_for_horizon_output():
    """The generated horizon plan should obey SOC and energy-balance constraints."""
    now = datetime(2026, 3, 22, 18, 0, 0)
    prices = _explicit_prices(now, [1.40, 0.60, 0.00, 0.00, 0.10, 0.10, 0.70, 0.90])
    forecast = [
        {"period_start": datetime(2026, 3, 23, 10, 0, 0), "pv_estimate_kwh": 0.8, "pv_estimate10_kwh": 0.56, "pv_estimate90_kwh": 1.04},
        {"period_start": datetime(2026, 3, 23, 11, 0, 0), "pv_estimate_kwh": 0.8, "pv_estimate10_kwh": 0.56, "pv_estimate90_kwh": 1.04},
    ]

    _, plan = run_with_plan(now, soc=45.0, prices=prices, forecast=forecast)

    _assert_plan_invariants(plan, min_soc=10.0, max_soc=90.0, battery_capacity_kwh=10.0)


def test_bruteforce_reference_matches_optimizer_for_small_horizon():
    """For a tiny horizon, optimizer cost should match the brute-force optimum."""
    now = datetime(2026, 3, 22, 18, 0, 0)
    price_series = [0.90, 0.10, 1.30, 0.20]
    prices = _explicit_prices(now, price_series)
    extra_config = {
        "battery_capacity_kwh": 2.0,
        "battery_min_soc": 0.0,
        "battery_max_soc": 100.0,
        "charge_rate_kw": 0.5,
        "battery_cost_per_kwh": 0.10,
        "min_charge_saving": 0.0,
        "cheap_grid_threshold": 0.15,
    }

    _, plan = run_with_plan(now, soc=25.0, prices=prices, forecast=None, extra_config=extra_config)

    optimizer_cost = _plan_cost(plan, battery_cost_per_kwh=0.10)
    brute_force_cost = _bruteforce_optimal_cost(
        price_series,
        initial_stored_kwh=0.5,
        load_kwh=0.5,
        charge_rate_kwh=0.5,
        usable_capacity_kwh=2.0,
        battery_cost_per_kwh=0.10,
    )

    assert optimizer_cost == brute_force_cost, (
        f"Expected optimizer cost {optimizer_cost:.3f} to match brute-force optimum {brute_force_cost:.3f}"
    )


def test_bruteforce_reference_matches_optimizer_when_reserving_for_later_peak():
    """Small-horizon optimizer should reserve energy for the highest-value future slot."""
    now = datetime(2026, 3, 22, 17, 0, 0)
    price_series = [0.60, 1.10, 0.05, 1.40]
    prices = _explicit_prices(now, price_series)
    extra_config = {
        "battery_capacity_kwh": 2.0,
        "battery_min_soc": 0.0,
        "battery_max_soc": 100.0,
        "charge_rate_kw": 0.5,
        "battery_cost_per_kwh": 0.10,
        "min_charge_saving": 0.0,
        "cheap_grid_threshold": 0.10,
    }

    _, plan = run_with_plan(now, soc=25.0, prices=prices, forecast=None, extra_config=extra_config)

    optimizer_cost = _plan_cost(plan, battery_cost_per_kwh=0.10)
    brute_force_cost = _bruteforce_optimal_cost(
        price_series,
        initial_stored_kwh=0.5,
        load_kwh=0.5,
        charge_rate_kwh=0.5,
        usable_capacity_kwh=2.0,
        battery_cost_per_kwh=0.10,
    )

    assert optimizer_cost == brute_force_cost, (
        f"Expected optimizer cost {optimizer_cost:.3f} to match brute-force optimum {brute_force_cost:.3f}"
    )


@pytest.mark.parametrize("case", REFERENCE_CASES, ids=[case["name"] for case in REFERENCE_CASES])
def test_bruteforce_reference_matrix_matches_optimizer(case: dict) -> None:
    """Optimizer should match brute-force optimum across a small matrix of short horizons."""
    now = datetime(2026, 3, 22, 17, 0, 0)
    prices = _explicit_prices(now, case["prices"])
    extra_config = {
        "battery_capacity_kwh": 2.0,
        "battery_min_soc": 0.0,
        "battery_max_soc": 100.0,
        "charge_rate_kw": 0.5,
        "battery_cost_per_kwh": 0.10,
        "min_charge_saving": 0.0,
        "cheap_grid_threshold": 0.10,
    }

    _, plan = run_with_plan(
        now,
        soc=case["soc"],
        prices=prices,
        forecast=None,
        extra_config=extra_config,
    )

    optimizer_cost = _plan_cost(plan, battery_cost_per_kwh=0.10)
    brute_force_cost = _bruteforce_optimal_cost(
        case["prices"],
        initial_stored_kwh=(case["soc"] / 100.0) * 2.0,
        load_kwh=0.5,
        charge_rate_kwh=0.5,
        usable_capacity_kwh=2.0,
        battery_cost_per_kwh=0.10,
    )

    assert optimizer_cost == brute_force_cost, (
        f"Case {case['name']} expected optimizer cost {optimizer_cost:.3f} "
        f"to match brute-force optimum {brute_force_cost:.3f}"
    )


def test_recalculation_changes_plan_when_evening_discharge_never_happened():
    """Simulate a stale evening plan versus a fresh morning replan after missed discharge."""
    profile = _ScheduledProfile(default_watt=500.0, overrides={6: 2000.0, 7: 2000.0})

    evening_now = datetime(2026, 3, 24, 17, 0, 0)
    evening_prices = _explicit_prices(
        evening_now,
        [
            1.40, 1.45, 1.35, 0.45, 0.35, 0.30, 0.28, 0.26, 0.90, 0.95,
            0.25, 0.20, 0.18, 0.22, 0.30, 0.40, 0.55, 0.70, 0.65, 0.50,
        ],
    )
    forecast = _explicit_forecast(
        evening_now,
        {
            datetime(2026, 3, 25, 8, 0): 1.5,
            datetime(2026, 3, 25, 9, 0): 2.0,
            datetime(2026, 3, 25, 10, 0): 2.5,
            datetime(2026, 3, 25, 11, 0): 2.2,
            datetime(2026, 3, 25, 12, 0): 1.8,
        },
    )

    _, evening_plan = run_with_plan(
        now=evening_now,
        soc=36.0,
        prices=evening_prices,
        forecast=forecast,
        profile=profile,
    )

    evening_discharge_hours = {
        datetime.fromisoformat(slot["hour"]).hour
        for slot in evening_plan
        if float(slot["discharge_w"]) > 0 and datetime.fromisoformat(slot["hour"]).date() == evening_now.date()
    }
    stale_morning_discharge_hours = {
        datetime.fromisoformat(slot["hour"]).hour
        for slot in evening_plan
        if float(slot["discharge_w"]) > 0 and datetime.fromisoformat(slot["hour"]).date() == datetime(2026, 3, 25).date()
    }

    assert evening_discharge_hours & {17, 18, 19}
    assert not (stale_morning_discharge_hours & {6, 7})

    morning_now = datetime(2026, 3, 25, 5, 0, 0)
    morning_prices = [
        slot for slot in evening_prices
        if datetime.fromisoformat(str(slot["hour"])) >= morning_now
    ]
    _, morning_replan = run_with_plan(
        now=morning_now,
        soc=36.0,
        prices=morning_prices,
        forecast=forecast,
        profile=profile,
    )

    morning_discharge_hours_after_replan = {
        datetime.fromisoformat(slot["hour"]).hour
        for slot in morning_replan
        if float(slot["discharge_w"]) > 0
    }

    assert morning_discharge_hours_after_replan
    assert morning_discharge_hours_after_replan != stale_morning_discharge_hours
    assert morning_discharge_hours_after_replan & {9, 10, 11, 12}
    assert morning_replan != evening_plan
