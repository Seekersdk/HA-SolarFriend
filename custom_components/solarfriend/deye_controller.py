"""DeyeController - translates OptimizeResult strategies into Deye HA service calls."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from homeassistant.util import dt as ha_dt

from .inverter_controller import InverterController

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

    from .battery_optimizer import OptimizeResult

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CommandSignature:
    """Minimal signature of the desired inverter command state."""

    strategy: str
    solar_sell: bool
    cheapest_charge_hour: str | None
    target_soc: float | None
    charge_now: bool


class DeyeController(InverterController):
    """Translate an OptimizeResult strategy into Deye inverter commands via HA services."""

    _REQUIRED_ENTITY_FIELDS = (
        "_grid_charge",
        "_time_of_use",
        "_tp1_enable",
        "_tp1_start",
        "_tp1_capacity",
        "_charge_current",
        "_energy_priority",
        "_limit_control_mode",
    )

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        self.hass = hass
        data = config_entry.data

        self._grid_charge = data.get("deye_grid_charge_switch")
        self._time_of_use = data.get("deye_time_of_use_switch")
        self._tp1_enable = data.get("deye_time_point_1_enable")
        self._tp1_start = data.get("deye_time_point_1_start")
        self._tp1_capacity = data.get("deye_time_point_1_capacity")
        self._charge_current = data.get("deye_grid_charge_current")
        self._energy_priority = data.get("deye_energy_priority")
        self._limit_control_mode = data.get("deye_limit_control_mode")
        self._solar_sell = data.get("solar_sell_entity", "")
        self._battery_min_soc = float(data.get("battery_min_soc", 10.0))

        self._last_signature: _CommandSignature | None = None

    @property
    def is_configured(self) -> bool:
        """True if all core Deye write-entities are configured."""
        return all(getattr(self, field) for field in self._REQUIRED_ENTITY_FIELDS)

    @staticmethod
    def _signature_for(result: OptimizeResult) -> _CommandSignature:
        return _CommandSignature(
            strategy=result.strategy,
            solar_sell=result.solar_sell,
            cheapest_charge_hour=result.cheapest_charge_hour,
            target_soc=result.target_soc,
            charge_now=result.charge_now,
        )

    async def apply(self, result: OptimizeResult) -> None:
        """Apply strategy to Deye - skips only when desired command state is unchanged."""
        signature = self._signature_for(result)
        if signature == self._last_signature:
            return

        _LOGGER.info(
            "DeyeController [%s -> %s]: %s",
            self._last_signature.strategy if self._last_signature else None,
            result.strategy,
            result.reason,
        )

        if result.strategy == "ANTI_EXPORT":
            await self._set_solar_sell(False)
            await self._apply_idle()
        elif result.strategy == "CHARGE_NIGHT":
            await self._set_solar_sell(True)
            await self._apply_charge_night(result)
        elif result.strategy == "USE_BATTERY":
            await self._set_solar_sell(True)
            await self._apply_use_battery()
        elif result.strategy == "SELL_BATTERY":
            await self._set_solar_sell(True)
            await self._apply_sell_battery()
        elif result.strategy == "SAVE_SOLAR":
            await self._set_solar_sell(True)
            await self._apply_idle()
        elif result.strategy == "CHARGE_GRID":
            await self._set_solar_sell(True)
            await self._apply_charge_grid(result)
        elif result.strategy == "IDLE":
            await self._set_solar_sell(True)
            await self._apply_idle()

        self._last_signature = signature

    async def _apply_charge_night(self, result: OptimizeResult) -> None:
        """Charge from grid during cheapest night hours using Time-of-Use schedule."""
        raw = result.cheapest_charge_hour
        if raw and isinstance(raw, str) and ":" in raw:
            parts = raw.split(":")
            try:
                tp_start = int(parts[0]) * 100 + int(parts[1])
            except (ValueError, IndexError):
                tp_start = 0
        else:
            tp_start = 0

        target_soc = int(result.target_soc) if result.target_soc else 80

        await self._set_switch(self._grid_charge, True)
        await self._set_switch(self._time_of_use, True)
        await self._set_switch(self._tp1_enable, True)
        await self._set_number(self._tp1_start, tp_start)
        await self._set_number(self._tp1_capacity, target_soc)
        await self._set_number(self._charge_current, 25)
        await self._set_select(self._energy_priority, "Battery first")
        await self._set_select(self._limit_control_mode, "Zero export to CT")

        _LOGGER.info(
            "DeyeController: CHARGE_NIGHT - tp1_start=%s target_soc=%s%%",
            tp_start,
            target_soc,
        )

    async def _apply_use_battery(self) -> None:
        """Discharge battery to supply house load via a TOU window."""
        await self._set_switch(self._time_of_use, True)
        await self._set_switch(self._tp1_enable, True)
        await self._set_number(self._tp1_start, self._current_hhmm())
        await self._set_number(self._tp1_capacity, int(self._battery_min_soc))
        await self._set_select(self._energy_priority, "Load first")
        await self._set_select(self._limit_control_mode, "Zero export to CT")
        await self._set_switch(self._grid_charge, False)

    async def _apply_sell_battery(self) -> None:
        """Export battery energy to grid - solar will recharge afterwards."""
        await self._set_switch(self._time_of_use, True)
        await self._set_switch(self._tp1_enable, True)
        await self._set_number(self._tp1_start, self._current_hhmm())
        await self._set_number(self._tp1_capacity, int(self._battery_min_soc))
        await self._set_select(self._energy_priority, "Load first")
        await self._set_select(self._limit_control_mode, "Selling first")
        await self._set_switch(self._grid_charge, False)
        _LOGGER.info(
            "DeyeController: SELL_BATTERY - Selling first aktiveret via limit_control_mode"
        )

    async def _apply_charge_grid(self, result: OptimizeResult) -> None:
        """Charge now via an immediate TOU window."""
        await self._set_switch(self._grid_charge, True)
        await self._set_switch(self._time_of_use, True)
        await self._set_switch(self._tp1_enable, True)
        await self._set_number(self._tp1_start, self._current_hhmm())
        await self._set_number(
            self._tp1_capacity,
            int(result.target_soc) if result.target_soc else 80,
        )
        await self._set_number(self._charge_current, 25)
        await self._set_select(self._energy_priority, "Battery first")
        await self._set_select(self._limit_control_mode, "Zero export to CT")

    async def _apply_idle(self) -> None:
        """Neutral state - let Deye manage itself."""
        await self._set_switch(self._grid_charge, False)
        await self._set_switch(self._time_of_use, False)
        await self._set_switch(self._tp1_enable, False)
        await self._set_select(self._energy_priority, "Load first")
        await self._set_select(self._limit_control_mode, "Zero export to CT")

    async def _set_solar_sell(self, enabled: bool) -> None:
        """Enable or disable solar sell to grid. No-op if entity not configured."""
        if not self._solar_sell:
            return
        await self._set_switch(self._solar_sell, enabled)
        _LOGGER.info(
            "DeyeController: solar_sell -> %s (%s)",
            "ON" if enabled else "OFF",
            self._solar_sell,
        )

    async def _set_switch(self, entity_id: str | None, state: bool) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "switch",
            "turn_on" if state else "turn_off",
            {"entity_id": entity_id},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s -> %s", entity_id, "ON" if state else "OFF")

    async def _set_number(self, entity_id: str | None, value: float) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "number",
            "set_value",
            {"entity_id": entity_id, "value": value},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s -> %s", entity_id, value)

    async def _set_select(self, entity_id: str | None, option: str) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "select",
            "select_option",
            {"entity_id": entity_id, "option": option},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s -> %s", entity_id, option)

    @staticmethod
    def _current_hhmm() -> int:
        now = ha_dt.now()
        return now.hour * 100 + now.minute
