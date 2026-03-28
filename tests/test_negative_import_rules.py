import importlib.util
import pathlib
import sys
import types
import unittest
from datetime import datetime, timezone


ROOT = pathlib.Path(__file__).resolve().parents[1] / "custom_components" / "solarfriend"


def _install_homeassistant_stub() -> None:
    ha_mod = types.ModuleType("homeassistant")
    ha_util = types.ModuleType("homeassistant.util")
    ha_dt = types.ModuleType("homeassistant.util.dt")
    ha_dt.UTC = timezone.utc
    ha_dt.now = lambda: datetime.now(timezone.utc)
    ha_dt.as_local = lambda value: value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    ha_util.dt = ha_dt
    ha_mod.util = ha_util
    sys.modules["homeassistant"] = ha_mod
    sys.modules["homeassistant.util"] = ha_util
    sys.modules["homeassistant.util.dt"] = ha_dt


def _install_package_stub() -> None:
    pkg_custom = types.ModuleType("custom_components")
    pkg_custom.__path__ = [str(ROOT.parent)]
    sys.modules.setdefault("custom_components", pkg_custom)
    pkg_sf = types.ModuleType("custom_components.solarfriend")
    pkg_sf.__path__ = [str(ROOT)]
    sys.modules.setdefault("custom_components.solarfriend", pkg_sf)


def _load_module(name: str):
    spec = importlib.util.spec_from_file_location(
        f"custom_components.solarfriend.{name}",
        ROOT / f"{name}.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_install_homeassistant_stub()
_install_package_stub()
_load_module("forecast_adapter")
_load_module("price_adapter")
battery_optimizer = _load_module("battery_optimizer")
deye_controller = _load_module("deye_controller")


class _MockEntry:
    def __init__(self, data):
        self.data = data


class _MockTracker:
    weighted_cost = 0.20
    solar_fraction = 0.5


class _MockProfile:
    def get_predicted_watt(self, hour, is_weekend):
        return 500.0


class NegativeImportRuleTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "battery_capacity_kwh": 10.0,
            "battery_min_soc": 10,
            "battery_max_soc": 100,
            "charge_rate_kw": 6.0,
            "battery_cost_per_kwh": 0.20,
            "min_charge_saving": 0.10,
        }
        self.sunrise = datetime(2026, 3, 22, 6, 30, 0)
        self.sunset = datetime(2026, 3, 22, 19, 0, 0)
        self.optimizer = battery_optimizer.BatteryOptimizer(
            _MockEntry(self.config),
            _MockTracker(),
            _MockProfile(),
        )

    def test_negative_import_wins_when_buy_price_is_negative(self):
        buy_prices = [
            {"hour": h, "price": (-0.25 if h == 12 else (1.38 if 7 <= h < 22 else 0.38))}
            for h in range(24)
        ]
        sell_prices = [
            {"hour": h, "price": (-0.10 if h == 12 else 0.05)}
            for h in range(24)
        ]
        result = self.optimizer.optimize(
            now=datetime(2026, 3, 22, 12, 0, 0),
            pv_power=5000,
            load_power=800,
            current_soc=60,
            raw_prices=buy_prices,
            raw_sell_prices=sell_prices,
            forecast_today_kwh=15.0,
            forecast_tomorrow_kwh=15.0,
            sunrise_time=self.sunrise,
            sunset_time=self.sunset,
            is_weekend=False,
        )
        self.assertEqual("NEGATIVE_IMPORT", result.strategy)
        self.assertFalse(result.solar_sell)

    def test_anti_export_still_applies_when_only_sell_price_is_negative(self):
        buy_prices = [
            {"hour": h, "price": (1.20 if 7 <= h < 22 else 0.38)}
            for h in range(24)
        ]
        sell_prices = [
            {"hour": h, "price": (-0.10 if h == 12 else 0.05)}
            for h in range(24)
        ]
        result = self.optimizer.optimize(
            now=datetime(2026, 3, 22, 12, 0, 0),
            pv_power=5000,
            load_power=800,
            current_soc=60,
            raw_prices=buy_prices,
            raw_sell_prices=sell_prices,
            forecast_today_kwh=15.0,
            forecast_tomorrow_kwh=15.0,
            sunrise_time=self.sunrise,
            sunset_time=self.sunset,
            is_weekend=False,
        )
        self.assertEqual("ANTI_EXPORT", result.strategy)
        self.assertFalse(result.solar_sell)

    def test_deye_negative_import_maps_to_zero_export_to_load(self):
        config = {
            "deye_grid_charge_switch": "switch.grid_charge",
            "deye_time_of_use_switch": "switch.tou",
            "deye_time_point_1_enable": "switch.tp1_enable",
            "deye_time_point_1_start": "number.tp1_start",
            "deye_time_point_1_capacity": "number.tp1_capacity",
            "deye_grid_charge_current": "number.charge_current",
            "deye_max_battery_discharge_current": "number.max_discharge_current",
            "deye_default_battery_discharge_current": 80.0,
            "deye_energy_priority": "select.energy_priority",
            "deye_limit_control_mode": "select.limit_control_mode",
        }
        controller = deye_controller.DeyeController(hass=types.SimpleNamespace(), config_entry=_MockEntry(config))
        result = battery_optimizer.OptimizeResult(
            strategy="NEGATIVE_IMPORT",
            reason="test",
            target_soc=None,
            charge_now=False,
            cheapest_charge_hour=None,
            night_charge_kwh=0.0,
            morning_need_kwh=0.0,
            day_deficit_kwh=0.0,
            peak_need_kwh=0.0,
            expected_saving_dkk=0.0,
            weighted_battery_cost=0.0,
            solar_fraction=0.0,
            best_discharge_hours=[],
            solar_sell=False,
        )
        expected = controller._expected_state_for(result)
        self.assertEqual("Zero export to load", expected["select.limit_control_mode"])
        self.assertEqual(0.0, expected["number.max_discharge_current"])


if __name__ == "__main__":
    unittest.main()
