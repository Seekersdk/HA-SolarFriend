"""SolarFriend select platform — EV charge mode and departure time selectors."""
from __future__ import annotations

import logging
from datetime import time

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator, ev_device_info

_LOGGER = logging.getLogger(__name__)

EV_CHARGE_MODES = ["solar_only", "hybrid", "grid_schedule"]

# 48 options: 00:00, 00:30, 01:00, … 23:30
DEPARTURE_OPTIONS: list[str] = [
    f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)
]
_DEFAULT_DEPARTURE = "07:30"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]

    if entry.data.get("ev_charging_enabled", False):
        async_add_entities([
            SolarFriendEVModeSelect(coordinator),
            SolarFriendEVDepartureSelect(coordinator),
        ])


class SolarFriendEVModeSelect(RestoreEntity, SelectEntity):
    """Select entity for choosing EV charge mode."""

    _attr_has_entity_name = True
    _attr_name = "Ladetilstand"
    _attr_icon = "mdi:solar-power"
    _attr_options = EV_CHARGE_MODES

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_mode"
        _LOGGER.debug("EV mode select unique_id: %s", self._attr_unique_id)
        self._current_option: str = coordinator.ev_charge_mode

    @property
    def device_info(self):
        return ev_device_info(self._coordinator)

    @property
    def current_option(self) -> str:
        return self._current_option

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state in EV_CHARGE_MODES:
            self._current_option = last_state.state
        self._coordinator.ev_charge_mode = self._current_option
        _LOGGER.debug("EV ladetilstand gendannet: %s", self._current_option)

    async def async_select_option(self, option: str) -> None:
        if option not in EV_CHARGE_MODES:
            _LOGGER.warning("Ukendt EV ladetilstand: %s", option)
            return
        self._current_option = option
        self._coordinator.ev_charge_mode = option
        self.async_write_ha_state()
        _LOGGER.info("EV ladetilstand ændret til: %s", option)


class SolarFriendEVDepartureSelect(RestoreEntity, SelectEntity):
    """Dropdown med afgangstider i 30-minutters intervaller (00:00 – 23:30)."""

    _attr_has_entity_name = True
    _attr_name = "Afgangstid"
    _attr_icon = "mdi:car-clock"
    _attr_options = DEPARTURE_OPTIONS

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_departure"
        _LOGGER.debug("EV departure select unique_id: %s", self._attr_unique_id)
        self._attr_device_info = ev_device_info(coordinator)
        self._current_option: str = _DEFAULT_DEPARTURE

    @property
    def current_option(self) -> str:
        return self._current_option

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state in DEPARTURE_OPTIONS:
            self._current_option = last_state.state
        self._coordinator.ev_departure_time = self._parse(self._current_option)
        _LOGGER.debug("EV afgangstid gendannet: %s", self._current_option)

    async def async_select_option(self, option: str) -> None:
        self._current_option = option
        self._coordinator.ev_departure_time = self._parse(option)
        self.async_write_ha_state()
        _LOGGER.info("EV afgangstid sat til %s", option)

    @staticmethod
    def _parse(value: str) -> time:
        h, m = value.split(":")
        return time(int(h), int(m))
