"""Helpers for reading and normalizing Home Assistant sensor state."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StateReadResult:
    """Structured result for core sensor reads."""

    readings: dict[str, float] = field(default_factory=dict)
    unavailable: list[str] = field(default_factory=list)


class SolarFriendStateReader:
    """Read configured HA entities and normalize house-load semantics."""

    def __init__(self, hass: Any, config_entry: Any) -> None:
        self._hass = hass
        self._entry = config_entry

    def update_config_entry(self, config_entry: Any) -> None:
        """Refresh config entry reference after runtime config updates."""
        self._entry = config_entry

    def read_float_state(self, entity_id: str) -> tuple[float | None, bool]:
        """Return (float_value, is_available) for an entity."""
        state = self._hass.states.get(entity_id)
        if state is None or state.state in ("unavailable", "unknown", ""):
            return None, False
        try:
            return float(state.state), True
        except (ValueError, TypeError):
            return None, False

    def read_core_sensors(self) -> StateReadResult:
        """Read the coordinator core sensor set from the configured entities."""
        sensor_map: dict[str, str] = {
            "pv_power": self._entry.data.get("pv_power_sensor", ""),
            "pv2_power": self._entry.data.get("pv2_power_sensor", ""),
            "grid_power": self._entry.data.get("grid_power_sensor", ""),
            "battery_soc": self._entry.data.get("battery_soc_sensor", ""),
            "battery_power": self._entry.data.get("battery_power_sensor", ""),
            "load_power": self._entry.data.get("load_power_sensor", ""),
        }

        result = StateReadResult()
        for field_name, entity_id in sensor_map.items():
            if not entity_id:
                result.unavailable.append(field_name)
                continue
            value, available = self.read_float_state(entity_id)
            if available and value is not None:
                result.readings[field_name] = value
            else:
                result.unavailable.append(field_name)
        return result

    def load_sensor_is_total_load(self) -> bool:
        """Return True when the configured load sensor is a whole-site total load."""
        entity_id = str(self._entry.data.get("load_power_sensor", "")).lower()
        return any(token in entity_id for token in ("load_totalpower", "totalpower", "total_load"))

    def clean_live_house_load(
        self,
        total_load_w: float,
        *,
        ev_power_w: float = 0.0,
    ) -> float:
        """Return house-only load when the configured load sensor is total site load."""
        if not self.load_sensor_is_total_load():
            return max(0.0, total_load_w)
        return max(0.0, total_load_w - ev_power_w)
