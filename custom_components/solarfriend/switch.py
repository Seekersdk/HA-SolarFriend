"""SolarFriend switch platform — manual EV charging and debug controls."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.helpers.device_registry import DeviceInfo
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

    entities: list[SwitchEntity] = [SolarFriendShadowLogSwitch(coordinator, entry)]
    if entry.data.get("ev_charging_enabled", False):
        entities.append(SolarFriendEVSwitch(coordinator))
    async_add_entities(entities)


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


class SolarFriendShadowLogSwitch(RestoreEntity, SwitchEntity):
    """Persistent switch for enabling shadow logging."""

    _attr_has_entity_name = True
    _attr_name = "Shadow Log"
    _attr_icon = "mdi:file-chart"

    def __init__(self, coordinator: SolarFriendCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._is_on = bool(entry.data.get("shadow_log_enabled", False))
        self._attr_unique_id = f"{entry.entry_id}_shadow_log"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.data.get("name", "SolarFriend"),
            manufacturer="SolarFriend",
            model="Solar Energy Manager",
        )

    @property
    def is_on(self) -> bool:
        return self._is_on

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state in ("on", "off"):
            self._is_on = last_state.state == "on"
        self._coordinator._shadow_log_enabled = self._is_on
        await self._persist_state()

    async def _persist_state(self) -> None:
        new_data = {**self._entry.data, "shadow_log_enabled": self._is_on}
        self.hass.config_entries.async_update_entry(self._entry, data=new_data)

    async def async_turn_on(self, **kwargs: Any) -> None:
        self._is_on = True
        self._coordinator._shadow_log_enabled = True
        await self._persist_state()
        self.async_write_ha_state()
        _LOGGER.info("Shadow log aktiveret")

    async def async_turn_off(self, **kwargs: Any) -> None:
        self._is_on = False
        self._coordinator._shadow_log_enabled = False
        await self._persist_state()
        self.async_write_ha_state()
        _LOGGER.info("Shadow log deaktiveret")
