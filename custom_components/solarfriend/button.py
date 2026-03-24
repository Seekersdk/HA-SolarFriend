"""SolarFriend button platform."""
from __future__ import annotations

import logging

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up SolarFriend buttons."""
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([SolarFriendPopulateLoadModelButton(coordinator, entry)])


class SolarFriendPopulateLoadModelButton(ButtonEntity):
    """Manual trigger for replaying load history into the model."""

    _attr_has_entity_name = True
    _attr_name = "Populate Load Model"
    _attr_icon = "mdi:database-arrow-down"

    def __init__(self, coordinator: SolarFriendCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_populate_load_model"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.data.get("name", "SolarFriend"),
            manufacturer="SolarFriend",
            model="Solar Energy Manager",
        )

    async def async_press(self) -> None:
        """Populate the learned load model from up to 14 days of history."""
        entries = await self._coordinator.async_force_populate_load_model(days=14)
        await self._coordinator.async_request_refresh()
        _LOGGER.info("Populate Load Model button pressed: updated %d buckets", entries)
