"""Unit tests for ConsumptionProfile.bootstrap_from_history + _percentile_filter."""
from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime

# ---------------------------------------------------------------------------
# Mock the homeassistant package before any SolarFriend import
# ---------------------------------------------------------------------------

def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
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
    _mock("homeassistant.helpers.device_registry", DeviceInfo=dict)
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
    _mock("homeassistant.components.button",
          ButtonEntity=type("ButtonEntity", (), {}))
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
# Import modules under test
# ---------------------------------------------------------------------------

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from custom_components.solarfriend.consumption_profile import (  # noqa: E402
    ConsumptionProfile,
    _percentile_filter,
)
from custom_components.solarfriend.forecast_adapter import get_forecast_for_period  # noqa: E402
from custom_components.solarfriend.price_adapter import PriceAdapter  # noqa: E402
from custom_components.solarfriend.snapshot_builder import SnapshotBuilder  # noqa: E402

# _clean_load_w is a static method — grab it for convenience
_clean_load_w = ConsumptionProfile._clean_load_w


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(coro):
    return asyncio.run(coro)


def _make_state(state_value: str, hour: int, weekday: int = 0) -> object:
    """Return a minimal mock state object (weekday 0 = Monday = hverdag)."""
    dt = datetime(2026, 3, 16 + weekday, hour, 15, 0)  # 2026-03-16 is Monday
    s = types.SimpleNamespace()
    s.state = state_value
    s.last_changed = dt
    return s


def _make_hass_with_recorder(entity_id: str, states: list) -> tuple:
    """Return (hass_mock, recorder_mock) wired so bootstrap can call them."""
    def _get_significant_states(hass, start, end, entity_ids):
        return {entity_id: states}

    recorder_mock = types.SimpleNamespace()

    async def _executor_job(fn, *args):
        return fn(*args)

    recorder_mock.async_add_executor_job = _executor_job

    recorder_mod = types.ModuleType("homeassistant.components.recorder")
    recorder_mod.get_instance = lambda hass: recorder_mock
    sys.modules["homeassistant.components.recorder"] = recorder_mod

    history_mod = types.ModuleType("homeassistant.components.recorder.history")
    history_mod.get_significant_states = _get_significant_states
    sys.modules["homeassistant.components.recorder.history"] = history_mod

    state_lookup = {
        entity_id: types.SimpleNamespace(attributes={}),
    }
    hass = types.SimpleNamespace(
        states=types.SimpleNamespace(get=lambda eid: state_lookup.get(eid))
    )
    return hass, recorder_mock


def _set_sensor_attrs(hass, entity_id: str, **attrs) -> None:
    hass.states.get(entity_id).attributes.update(attrs)


def _make_hass_with_failing_recorder() -> object:
    """Wire recorder so get_instance raises RuntimeError."""
    def _raise(hass):
        raise RuntimeError("no recorder")

    recorder_mod = types.ModuleType("homeassistant.components.recorder")
    recorder_mod.get_instance = _raise
    sys.modules["homeassistant.components.recorder"] = recorder_mod

    history_mod = types.ModuleType("homeassistant.components.recorder.history")
    history_mod.get_significant_states = lambda *a, **kw: {}  # never reached
    sys.modules["homeassistant.components.recorder.history"] = history_mod

    return object()


def _profile_with_saved_noop() -> ConsumptionProfile:
    """ConsumptionProfile whose async_save is a no-op (no real HA storage)."""
    profile = ConsumptionProfile()

    async def _noop_save(hass):
        pass

    profile.async_save = _noop_save  # type: ignore[method-assign]
    return profile


def _should_run_bootstrap(entry_data: dict, days_collected: int) -> bool:
    """Simulate the coordinator's bootstrap guard condition."""
    bootstrap_done = entry_data.get("bootstrap_done", False)
    load_entity = entry_data.get("load_power_sensor", "")
    return not bootstrap_done and bool(load_entity) and days_collected < 3


# ---------------------------------------------------------------------------
# Tests — _percentile_filter
# ---------------------------------------------------------------------------

def test_percentile_filter_removes_ev_spikes():
    """85th-percentile filter removes EV-charging spikes, avg drops below 1000 W."""
    values = [850, 900, 800, 9500, 850, 8800, 950, 875]
    filtered = _percentile_filter(values, percentile=85)
    # EV spikes (8800, 9500) must not appear in filtered result
    assert all(v < 8800 for v in filtered), f"EV spike still present: {filtered}"
    avg = sum(filtered) / len(filtered)
    assert avg < 1000, f"Expected avg < 1000 W after filtering, got {avg:.1f} W"


def test_percentile_filter_few_values_unchanged():
    """Fewer than 4 values are returned unmodified."""
    values = [850.0, 900.0]
    assert _percentile_filter(values) == values


def test_percentile_filter_fallback_when_all_filtered():
    """If the threshold is at the minimum value, fall back to the original list."""
    # All values identical → threshold = same value → strict < keeps nothing → fallback
    values = [1000.0, 1000.0, 1000.0, 1000.0]
    result = _percentile_filter(values, percentile=85)
    assert result == values  # fallback: return original


# ---------------------------------------------------------------------------
# Tests — bootstrap_from_history (profile level)
# ---------------------------------------------------------------------------

def test_bootstrap_averages_household_only():
    """Mix of household and EV measurements → avg reflects household level only.

    Realistic ratio: ~80 % household, ~20 % EV — matches real 14-day data
    where EV charges for a couple of hours per day at most.
    """
    household = [850, 900, 800, 950, 875, 830, 890, 860]   # 8 readings ~870 W
    ev_spikes  = [9500, 8800]                               # 2 EV readings
    states = [
        _make_state(str(w), hour=8, weekday=0)
        for w in household + ev_spikes
    ]
    hass, _ = _make_hass_with_recorder("sensor.load", states)
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 1
    avg = profile._profiles["weekday"][8]["avg_watt"]
    assert avg < 2000, f"Expected household-level avg (<2000 W), got {avg:.1f} W"


def test_bootstrap_ignores_invalid_states():
    """unknown, unavailable, negative, zero ignored — only valid states count."""
    states = [
        _make_state("unknown", hour=10, weekday=0),
        _make_state("unavailable", hour=10, weekday=0),
        _make_state("-500", hour=10, weekday=0),
        _make_state("0", hour=10, weekday=0),
        _make_state("not_a_number", hour=10, weekday=0),
        _make_state("1200", hour=10, weekday=0),
    ]
    hass, _ = _make_hass_with_recorder("sensor.load", states)
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 1
    assert profile._profiles["weekday"][10]["avg_watt"] == 1200.0
    assert profile._profiles["weekday"][10]["samples"] == 3


def test_bootstrap_no_overwrite_if_enough_live_samples():
    """Buckets with >= 5 live samples are not overwritten."""
    profile = _profile_with_saved_noop()
    profile._profiles["weekday"][8]["samples"] = 6
    profile._profiles["weekday"][8]["avg_watt"] = 800.0

    states = [_make_state("3000", hour=8, weekday=0)]
    hass, _ = _make_hass_with_recorder("sensor.load", states)

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 0
    assert profile._profiles["weekday"][8]["avg_watt"] == 800.0


def test_bootstrap_returns_zero_on_recorder_failure():
    """Recorder exception → returns 0, profile unchanged."""
    hass = _make_hass_with_failing_recorder()
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 0
    assert profile._profiles["weekday"][8]["samples"] == 0


def test_bootstrap_returns_zero_on_empty_history():
    """Recorder returns empty list → returns 0."""
    hass, _ = _make_hass_with_recorder("sensor.load", [])
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 0


def test_bootstrap_skips_if_enough_live_data():
    """days_collected >= 3 → bootstrap returns 0 immediately."""
    profile = _profile_with_saved_noop()
    # Fill all slots with 12 samples → days_collected = 3
    for pk in ("weekday", "weekend"):
        for h in range(24):
            profile._profiles[pk][h]["samples"] = 12

    hass, _ = _make_hass_with_recorder("sensor.load", [_make_state("1000", hour=8)])
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14))

    assert entries == 0, "Should skip when days_collected >= 3"


def test_bootstrap_force_blends_with_mature_live_bucket():
    """force=True should reseed a mature bucket without overriding live-learned data."""
    profile = _profile_with_saved_noop()
    for pk in ("weekday", "weekend"):
        for h in range(24):
            profile._profiles[pk][h]["samples"] = 12
            profile._profiles[pk][h]["avg_watt"] = 800.0

    states = [_make_state("1500", hour=8, weekday=0)]
    hass, _ = _make_hass_with_recorder("sensor.load", states)

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=20, force=True))

    assert entries == 1
    assert profile._profiles["weekday"][8]["avg_watt"] == 940.0
    assert profile._profiles["weekday"][8]["samples"] == 12


def test_days_collected_ignores_sparse_buckets():
    """Sparse weekday/weekend gaps should not reset the whole model to zero days."""
    profile = _profile_with_saved_noop()
    for hour in range(6):
        profile._profiles["weekday"][hour]["samples"] = 12
        profile._profiles["weekday"][hour]["avg_watt"] = 900.0

    assert profile.days_collected == 3


def test_get_predicted_watt_falls_back_to_weekday_when_weekend_is_sparse():
    """Weekend predictions should use weekday data when weekend profile is still immature."""
    profile = ConsumptionProfile()
    for hour in range(24):
        profile._profiles["weekday"][hour]["samples"] = 8
        profile._profiles["weekday"][hour]["avg_watt"] = 900.0 + hour
    profile._profiles["weekday"][20]["avg_watt"] = 1450.0
    profile._profiles["weekend"][20]["samples"] = 1
    profile._profiles["weekend"][20]["avg_watt"] = 0.0

    assert profile.get_predicted_watt(20, is_weekend=True) == 1450.0


def test_get_predicted_watt_keeps_weekend_when_weekend_profile_is_mature():
    """Weekend predictions should stay on weekend once the weekend profile has enough days."""
    profile = ConsumptionProfile()
    for hour in range(24):
        profile._profiles["weekday"][hour]["samples"] = 8
        profile._profiles["weekday"][hour]["avg_watt"] = 900.0 + hour
        profile._profiles["weekend"][hour]["samples"] = 8
        profile._profiles["weekend"][hour]["avg_watt"] = 1200.0 + hour

    assert profile.get_predicted_watt(20, is_weekend=True) == 1220.0


def test_debug_snapshot_reports_fallback_hours():
    """Diagnostics should expose which hours are using fallback day-type data."""
    profile = ConsumptionProfile()
    for hour in range(24):
        profile._profiles["weekday"][hour]["samples"] = 8
        profile._profiles["weekday"][hour]["avg_watt"] = 800.0 + hour
    profile._profiles["weekday"][0]["samples"] = 8
    profile._profiles["weekday"][0]["avg_watt"] = 900.0
    profile._profiles["weekend"][0]["samples"] = 1
    profile._profiles["weekend"][0]["avg_watt"] = 0.0

    snapshot = profile.build_debug_snapshot()

    assert 0 in snapshot["weekend"]["fallback_hours"]
    assert snapshot["weekend"]["days_estimate"] == 0
    assert snapshot["weekday"]["days_estimate"] == 2


def test_days_collected_remains_max_of_profile_maturity():
    """Overall maturity may be high even when weekend still falls back to weekday."""
    profile = ConsumptionProfile()
    for hour in range(24):
        profile._profiles["weekday"][hour]["samples"] = 8
        profile._profiles["weekday"][hour]["avg_watt"] = 900.0 + hour
    profile._profiles["weekend"][20]["samples"] = 3

    assert profile.days_collected == 2
    assert profile.get_predicted_watt(20, is_weekend=True) == 920.0


def test_consumption_profile_chart_uses_fallback_prediction_logic():
    """Published chart should use the same fallback path as optimizer predictions."""
    profile = ConsumptionProfile()
    for hour in range(24):
        profile._profiles["weekday"][hour]["samples"] = 8
        profile._profiles["weekday"][hour]["avg_watt"] = 1000.0 + hour
    profile._profiles["weekend"][20]["samples"] = 1
    profile._profiles["weekend"][20]["avg_watt"] = 0.0

    builder = SnapshotBuilder()
    data = types.SimpleNamespace(consumption_profile_chart=[], consumption_profile_day_type="")

    builder.apply_consumption_profile_chart(
        data=data,
        now=datetime(2026, 3, 28, 12, 0, 0),  # Saturday
        profile=profile,
    )

    assert data.consumption_profile_day_type == "weekend"
    assert data.consumption_profile_chart[20] == 1020.0


def test_bootstrap_power_history_integrates_time_weighted_load():
    """Power history should be integrated over time, not averaged as raw points."""
    states = [
        _make_state("1000", hour=8, weekday=0),
        _make_state("2000", hour=8, weekday=0),
    ]
    states[0].last_changed = datetime(2026, 3, 16, 8, 0, 0)
    states[1].last_changed = datetime(2026, 3, 16, 8, 30, 0)
    hass, _ = _make_hass_with_recorder("sensor.load", states)
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14, force=True))

    assert entries == 1
    assert profile._profiles["weekday"][8]["avg_watt"] == 1500.0


def test_bootstrap_energy_history_uses_deltas():
    """Energy sensors should seed from deltas between cumulative readings."""
    states = [
        _make_state("10.0", hour=8, weekday=0),
        _make_state("11.0", hour=9, weekday=0),
        _make_state("12.0", hour=10, weekday=0),
    ]
    states[0].last_changed = datetime(2026, 3, 16, 8, 0, 0)
    states[1].last_changed = datetime(2026, 3, 16, 9, 0, 0)
    states[2].last_changed = datetime(2026, 3, 16, 10, 0, 0)
    hass, _ = _make_hass_with_recorder("sensor.energy_load", states)
    _set_sensor_attrs(hass, "sensor.energy_load", unit_of_measurement="kWh", state_class="total_increasing")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.energy_load", days=14, force=True))

    assert entries == 2
    assert profile._profiles["weekday"][8]["avg_watt"] == 1000.0
    assert profile._profiles["weekday"][9]["avg_watt"] == 1000.0


def test_bootstrap_kw_power_history_normalizes_to_watt():
    """Power sensors reported in kW should still seed watt-based buckets correctly."""
    states = [
        _make_state("1.0", hour=8, weekday=0),
        _make_state("1.5", hour=9, weekday=0),
    ]
    states[0].last_changed = datetime(2026, 3, 16, 8, 0, 0)
    states[1].last_changed = datetime(2026, 3, 16, 9, 0, 0)
    hass, _ = _make_hass_with_recorder("sensor.load_kw", states)
    _set_sensor_attrs(hass, "sensor.load_kw", unit_of_measurement="kW", state_class="measurement")
    profile = _profile_with_saved_noop()

    entries = run(profile.bootstrap_from_history(hass, "sensor.load_kw", days=14, force=True))

    assert entries == 2
    assert profile._profiles["weekday"][8]["avg_watt"] == 1000.0
    assert profile._profiles["weekday"][9]["avg_watt"] == 1500.0


def test_bootstrap_skip_when_history_matches_live_profile():
    """Matching history should not perturb an existing live bucket."""
    states = [_make_state("920", hour=8, weekday=0)]
    hass, _ = _make_hass_with_recorder("sensor.load", states)
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()
    profile._profiles["weekday"][8]["samples"] = 12
    profile._profiles["weekday"][8]["avg_watt"] = 900.0

    entries = run(profile.bootstrap_from_history(hass, "sensor.load", days=14, force=True))

    assert entries == 0
    assert profile._profiles["weekday"][8]["avg_watt"] == 900.0


# ---------------------------------------------------------------------------
# Tests — coordinator bootstrap trigger conditions
# ---------------------------------------------------------------------------

def test_bootstrap_skips_if_flag_set():
    """bootstrap_done=True → condition is False regardless of days_collected."""
    entry_data = {"bootstrap_done": True, "load_power_sensor": "sensor.load"}
    assert not _should_run_bootstrap(entry_data, days_collected=0)


def test_bootstrap_runs_on_fresh_install():
    """No flag, no live data, load entity configured → condition is True."""
    entry_data = {"load_power_sensor": "sensor.load"}
    assert _should_run_bootstrap(entry_data, days_collected=0)


def test_bootstrap_saves_flag_on_success():
    """Successful bootstrap → config entry gets bootstrap_done=True."""
    states = [_make_state("1500", hour=9, weekday=0)]
    hass, _ = _make_hass_with_recorder("sensor.load", states)
    _set_sensor_attrs(hass, "sensor.load", unit_of_measurement="W", state_class="measurement")
    profile = _profile_with_saved_noop()

    entry_data: dict = {"load_power_sensor": "sensor.load"}
    updated_data: dict | None = None

    async def _run():
        nonlocal updated_data
        if _should_run_bootstrap(entry_data, profile.days_collected):
            entries = await profile.bootstrap_from_history(hass, entry_data["load_power_sensor"])
            if entries > 0:
                updated_data = {**entry_data, "bootstrap_done": True}

    run(_run())
    assert updated_data is not None, "Config entry should have been updated"
    assert updated_data.get("bootstrap_done") is True


def test_bootstrap_no_flag_on_failure():
    """Recorder failure → entries=0 → bootstrap_done NOT set in config entry."""
    hass = _make_hass_with_failing_recorder()
    profile = _profile_with_saved_noop()

    entry_data: dict = {"load_power_sensor": "sensor.load"}
    updated_data: dict | None = None

    async def _run():
        nonlocal updated_data
        if _should_run_bootstrap(entry_data, profile.days_collected):
            try:
                entries = await profile.bootstrap_from_history(hass, entry_data["load_power_sensor"])
                if entries > 0:
                    updated_data = {**entry_data, "bootstrap_done": True}
            except Exception:
                pass

    run(_run())
    assert updated_data is None, "bootstrap_done must NOT be set when bootstrap fails"


# ---------------------------------------------------------------------------
# Tests — _clean_load_w
# ---------------------------------------------------------------------------

def test_clean_load_ignores_battery_charging():
    """Battery charging is left inside load; only EV is cleaned out."""
    result = _clean_load_w(5500.0, ev_power_w=0.0, battery_power_w=-3200.0)
    assert result == 5500.0


def test_clean_load_subtracts_ev():
    """EV charging power is subtracted from load."""
    result = _clean_load_w(13000.0, ev_power_w=11000.0, battery_power_w=0.0)
    assert result == 2000.0


def test_clean_load_subtracts_only_ev_even_when_battery_charging():
    """Battery charging no longer changes the learned household load."""
    result = _clean_load_w(14000.0, ev_power_w=11000.0, battery_power_w=-1500.0)
    assert result == 3000.0


def test_clean_load_battery_discharging_ignored():
    """Positive battery_power (discharging) is NOT subtracted — it covers house load."""
    result = _clean_load_w(2000.0, ev_power_w=0.0, battery_power_w=1000.0)
    assert result == 2000.0


def test_clean_load_result_clamped_to_zero():
    """Result is clamped to 0 when subtraction would go negative."""
    result = _clean_load_w(500.0, ev_power_w=1000.0, battery_power_w=0.0)
    assert result == 0.0


def test_clean_load_still_too_high_returns_none():
    """Returns None when household load after subtraction still exceeds 10 kW."""
    result = _clean_load_w(15000.0, ev_power_w=0.0, battery_power_w=0.0)
    assert result is None


def test_get_forecast_for_period_handles_naive_range_and_aware_slots():
    total = get_forecast_for_period(
        [
            {"period_start": "2026-03-26T10:00:00+00:00", "pv_estimate_kwh": 0.7},
            {"period_start": "2026-03-26T11:00:00+00:00", "pv_estimate_kwh": 0.9},
        ],
        datetime(2026, 3, 26, 10, 0, 0),
        datetime(2026, 3, 26, 11, 0, 0),
    )
    assert total == 0.7


def test_get_forecast_for_period_handles_aware_range_and_naive_slots():
    total = get_forecast_for_period(
        [
            {"period_start": datetime(2026, 3, 26, 10, 0, 0), "pv_estimate_kwh": 0.4},
            {"period_start": datetime(2026, 3, 26, 11, 0, 0), "pv_estimate_kwh": 0.6},
        ],
        datetime.fromisoformat("2026-03-26T10:00:00+00:00"),
        datetime.fromisoformat("2026-03-26T12:00:00+00:00"),
    )
    assert total == 1.0


def test_price_adapter_accepts_raw_today_when_raw_tomorrow_is_missing():
    """A valid raw_today list must build a horizon even when raw_tomorrow is None."""
    import custom_components.solarfriend.price_adapter as price_adapter_module

    original_now = price_adapter_module.ha_dt.now
    price_adapter_module.ha_dt.now = lambda: datetime(2026, 3, 28, 0, 15, 0)
    try:
        state = types.SimpleNamespace(
            state="1.267",
            attributes={
                "raw_today": [
                    {"hour": "2026-03-28T00:00:00", "price": 1.267},
                    {"hour": "2026-03-28T01:00:00", "price": 1.238},
                ],
                "raw_tomorrow": None,
            },
        )
        hass = types.SimpleNamespace(states=types.SimpleNamespace(get=lambda entity_id: state))

        snapshot = PriceAdapter.from_hass(hass, "sensor.price")

        assert snapshot is not None
        assert snapshot.current_price == 1.267
        assert len(snapshot.points) == 2
        assert snapshot.to_legacy_raw_prices()[0]["price"] == 1.267
    finally:
        price_adapter_module.ha_dt.now = original_now
