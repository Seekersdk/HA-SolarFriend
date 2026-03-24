"""SolarFriend — Home Assistant integration for Deye solar inverters via ESPHome/MQTT."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN, SERVICE_POPULATE_LOAD_MODEL
from .coordinator import SolarFriendCoordinator

_LOGGER = logging.getLogger(__name__)

BUTTON_PLATFORM = getattr(Platform, "BUTTON", "button")
PLATFORMS = [Platform.SENSOR, Platform.NUMBER, Platform.SWITCH, Platform.SELECT, BUTTON_PLATFORM]


async def _async_handle_populate_load_model(hass: HomeAssistant, call: Any) -> None:
    """Populate one or all load models from recorder history."""
    days = max(1, min(int(call.data.get("days", 14)), 14))
    entry_id = call.data.get("entry_id")
    coordinators: dict[str, SolarFriendCoordinator] = hass.data.get(DOMAIN, {})

    targets = (
        [coordinators[entry_id]]
        if entry_id and entry_id in coordinators
        else list(coordinators.values())
    )

    for coordinator in targets:
        entries = await coordinator.async_force_populate_load_model(days)
        if entries > 0:
            await coordinator.async_request_refresh()


async def _cleanup_orphaned_ev_entities(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Fjern legacy-entiteter for denne config entry uden at røre andre entries."""
    registry = er.async_get(hass)
    to_remove = [
        entity.entity_id
        for entity in registry.entities.values()
        if entity.platform == "solarfriend"
        and entity.config_entry_id == entry.entry_id
        and not entity.unique_id.startswith(entry.entry_id)
    ]
    for entity_id in to_remove:
        _LOGGER.info("Fjerner orphaned entitet: %s", entity_id)
        registry.async_remove(entity_id)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Create coordinator, do first refresh, forward to platforms."""
    await _cleanup_orphaned_ev_entities(hass, entry)
    coordinator = SolarFriendCoordinator(hass, entry)
    await coordinator.async_startup()
    await coordinator.async_config_entry_first_refresh()
    # Kick off an initial optimizer run now that we have real data
    hass.async_create_task(coordinator._trigger_optimize("startup"))

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    if not hass.services.has_service(DOMAIN, SERVICE_POPULATE_LOAD_MODEL):
        async def _handle_populate_load_model(call: Any) -> None:
            await _async_handle_populate_load_model(hass, call)

        hass.services.async_register(
            DOMAIN,
            SERVICE_POPULATE_LOAD_MODEL,
            _handle_populate_load_model,
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload platforms and remove coordinator."""
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN].get(entry.entry_id)
    if coordinator is not None:
        coordinator.unregister_listeners()
        await coordinator.async_persist_state()

    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        hass.data[DOMAIN].pop(entry.entry_id)
        if not hass.data[DOMAIN] and hass.services.has_service(DOMAIN, SERVICE_POPULATE_LOAD_MODEL):
            hass.services.async_remove(DOMAIN, SERVICE_POPULATE_LOAD_MODEL)
    return unloaded
