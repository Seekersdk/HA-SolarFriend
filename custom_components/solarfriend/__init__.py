"""SolarFriend — Home Assistant integration for Deye solar inverters via ESPHome/MQTT."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR, Platform.NUMBER]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Create coordinator, do first refresh, forward to platforms."""
    coordinator = SolarFriendCoordinator(hass, entry)
    await coordinator.async_startup()
    await coordinator.async_config_entry_first_refresh()
    # Kick off an initial optimizer run now that we have real data
    hass.async_create_task(coordinator._trigger_optimize("startup"))

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload platforms and remove coordinator."""
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN].get(entry.entry_id)
    if coordinator is not None:
        coordinator.unregister_listeners()

    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unloaded
