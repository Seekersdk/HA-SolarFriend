"""DeyeController — translates OptimizeResult strategies into Deye HA service calls."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .inverter_controller import InverterController

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.config_entries import ConfigEntry
    from .battery_optimizer import OptimizeResult

_LOGGER = logging.getLogger(__name__)


class DeyeController(InverterController):
    """Translate an OptimizeResult strategy into Deye inverter commands via HA services."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        self.hass = hass
        data = config_entry.data

        self._grid_charge     = data.get("deye_grid_charge_switch")
        self._time_of_use     = data.get("deye_time_of_use_switch")
        self._tp1_enable      = data.get("deye_time_point_1_enable")
        self._tp1_start       = data.get("deye_time_point_1_start")
        self._tp1_capacity    = data.get("deye_time_point_1_capacity")
        self._charge_current  = data.get("deye_grid_charge_current")
        self._energy_priority = data.get("deye_energy_priority")

        self._last_strategy: str | None = None

    @property
    def is_configured(self) -> bool:
        """True if at least grid_charge and energy_priority are configured."""
        return bool(self._grid_charge and self._energy_priority)

    async def apply(self, result: OptimizeResult) -> None:
        """Apply strategy to Deye — skips if strategy unchanged since last call."""
        if result.strategy == self._last_strategy:
            return

        _LOGGER.info(
            "DeyeController [%s → %s]: %s",
            self._last_strategy, result.strategy, result.reason,
        )

        if result.strategy == "CHARGE_NIGHT":
            await self._apply_charge_night(result)
        elif result.strategy == "USE_BATTERY":
            await self._apply_use_battery()
        elif result.strategy == "SELL_BATTERY":
            await self._apply_sell_battery()
        elif result.strategy == "SAVE_SOLAR":
            await self._apply_save_solar()
        elif result.strategy == "CHARGE_GRID":
            await self._apply_charge_grid(result)
        elif result.strategy == "IDLE":
            await self._apply_idle()

        self._last_strategy = result.strategy

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

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

        _LOGGER.info(
            "DeyeController: CHARGE_NIGHT — tp1_start=%s target_soc=%s%%",
            tp_start, target_soc,
        )

    async def _apply_use_battery(self) -> None:
        """Discharge battery to supply house load."""
        await self._set_select(self._energy_priority, "Load first")
        await self._set_switch(self._grid_charge, False)
        await self._set_switch(self._time_of_use, False)

    async def _apply_sell_battery(self) -> None:
        """Export battery energy to grid — solar will recharge afterwards."""
        await self._set_select(self._energy_priority, "Grid first")
        await self._set_switch(self._grid_charge, False)
        await self._set_switch(self._time_of_use, False)
        _LOGGER.info(
            "DeyeController: SELL_BATTERY — Grid first aktiveret, batteri aflader til net"
        )

    async def _apply_save_solar(self) -> None:
        """Store solar energy in battery rather than exporting."""
        await self._set_select(self._energy_priority, "Battery first")
        await self._set_switch(self._grid_charge, False)
        await self._set_switch(self._time_of_use, False)

    async def _apply_charge_grid(self, result: OptimizeResult) -> None:
        """Charge now — price is at floor (no time-of-use scheduling needed)."""
        await self._set_switch(self._grid_charge, True)
        await self._set_number(self._charge_current, 25)
        await self._set_select(self._energy_priority, "Battery first")

    async def _apply_idle(self) -> None:
        """Neutral state — let Deye manage itself."""
        await self._set_switch(self._grid_charge, False)
        await self._set_switch(self._time_of_use, False)
        await self._set_select(self._energy_priority, "Load first")

    # ------------------------------------------------------------------
    # HA service helpers
    # ------------------------------------------------------------------

    async def _set_switch(self, entity_id: str | None, state: bool) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "switch", "turn_on" if state else "turn_off",
            {"entity_id": entity_id},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s → %s", entity_id, "ON" if state else "OFF")

    async def _set_number(self, entity_id: str | None, value: float) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "number", "set_value",
            {"entity_id": entity_id, "value": value},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s → %s", entity_id, value)

    async def _set_select(self, entity_id: str | None, option: str) -> None:
        if not entity_id:
            return
        await self.hass.services.async_call(
            "select", "select_option",
            {"entity_id": entity_id, "option": option},
            blocking=True,
        )
        _LOGGER.debug("Deye: %s → %s", entity_id, option)
