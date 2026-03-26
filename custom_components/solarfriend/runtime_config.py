"""Runtime component construction and optimizer setting refresh helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .battery_optimizer import BatteryOptimizer, LOW_GRID_HOLD_PRICE
from .inverter_controller import InverterController
from .state_reader import SolarFriendStateReader
from .weather_service import WeatherProfileService


@dataclass
class RuntimeComponents:
    """Coordinator runtime collaborators that depend on config entry values."""

    optimizer: BatteryOptimizer
    inverter: InverterController
    state_reader: SolarFriendStateReader
    weather_service: WeatherProfileService


def build_runtime_components(
    hass: Any,
    config_entry: Any,
    *,
    battery_tracker: Any,
    consumption_profile: Any,
) -> RuntimeComponents:
    """Build the runtime collaborators that mirror config-entry state."""
    return RuntimeComponents(
        optimizer=BatteryOptimizer(
            config_entry=config_entry,
            battery_tracker=battery_tracker,
            consumption_profile=consumption_profile,
        ),
        inverter=InverterController.from_config(hass, config_entry),
        state_reader=SolarFriendStateReader(hass, config_entry),
        weather_service=WeatherProfileService(
            hass,
            weather_entity=config_entry.data.get("weather_entity"),
        ),
    )


def refresh_optimizer_runtime_settings(optimizer: BatteryOptimizer, config_entry: Any) -> None:
    """Update runtime-adjustable optimizer values from the latest config entry."""
    cfg = config_entry.data
    optimizer.charge_rate_kw = float(cfg.get("charge_rate_kw", 6.0))
    optimizer.battery_min_soc = float(cfg.get("battery_min_soc", 10.0))
    optimizer.battery_max_soc = float(cfg.get("battery_max_soc", 100.0))
    optimizer.min_charge_saving = float(cfg.get("min_charge_saving", 0.10))
    optimizer.cheap_grid_threshold = float(cfg.get("cheap_grid_threshold", LOW_GRID_HOLD_PRICE))
