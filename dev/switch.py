"""SolarFriend switch platform — manual EV charging control."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator, ev_device_info

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]

    if entry.data.get("ev_charging_enabled", False):
        async_add_entities([SolarFriendEVSwitch(coordinator)])


class SolarFriendEVSwitch(RestoreEntity, SwitchEntity):
    """
    Manuel EV-ladning aktiv/inaktiv switch.

    Når OFF: optimizer tillader aldrig EV-ladning uanset solforhold eller priser.
    Default: ON.
    """

    _attr_has_entity_name = True
    _attr_name = "EV Ladning"
    _attr_icon = "mdi:ev-station"

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        self._coordinator = coordinator
        self._is_on: bool = True
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_switch"
        _LOGGER.debug("EV switch unique_id: %s", self._attr_unique_id)

    @property
    def device_info(self):
        return ev_device_info(self._coordinator)

    @property
    def is_on(self) -> bool:
        return self._is_on

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state in ("on", "off"):
            self._is_on = last_state.state == "on"
        # Synkroniser til coordinator
        self._coordinator.ev_charging_allowed = self._is_on
        _LOGGER.debug("EV switch restored: %s", "ON" if self._is_on else "OFF")

    async def async_turn_on(self, **kwargs: Any) -> None:
        self._is_on = True
        self._coordinator.ev_charging_allowed = True
        self.async_write_ha_state()
        _LOGGER.info("EV ladning aktiveret")

    async def async_turn_off(self, **kwargs: Any) -> None:
        self._is_on = False
        self._coordinator.ev_charging_allowed = False
        self.async_write_ha_state()
        _LOGGER.info("EV ladning deaktiveret")
