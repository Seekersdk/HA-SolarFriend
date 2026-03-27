"""SolarFriend — Home Assistant integration for Deye solar inverters via ESPHome/MQTT."""
from __future__ import annotations

import inspect
import logging
from datetime import datetime
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as ha_dt

from .const import (
    CONF_BUY_PRICE_SENSOR,
    CONF_SELL_PRICE_SENSOR,
    DOMAIN,
    SERVICE_BOOK_FLEX_LOAD,
    SERVICE_CANCEL_FLEX_LOAD,
    SERVICE_POPULATE_LOAD_MODEL,
)
from .coordinator import SolarFriendCoordinator

try:
    from homeassistant.core import SupportsResponse
except ImportError:  # pragma: no cover - test shim
    class SupportsResponse:  # type: ignore[override]
        """Fallback enum for lightweight test environments."""

        NONE = "none"
        ONLY = "only"

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


def _resolve_target_coordinator(hass: HomeAssistant, entry_id: str | None) -> SolarFriendCoordinator:
    """Resolve the targeted SolarFriend coordinator for a service call."""
    coordinators: dict[str, SolarFriendCoordinator] = hass.data.get(DOMAIN, {})
    if entry_id:
        if entry_id not in coordinators:
            raise ValueError(f"Unknown SolarFriend entry_id: {entry_id}")
        return coordinators[entry_id]
    if len(coordinators) == 1:
        return next(iter(coordinators.values()))
    raise ValueError("entry_id is required when multiple SolarFriend entries exist")


async def _async_handle_book_flex_load(hass: HomeAssistant, call: Any) -> dict[str, Any]:
    """Book or replace a flexible load slot and return the computed reservation."""
    coordinator = _resolve_target_coordinator(hass, call.data.get("entry_id"))
    parse_datetime = getattr(ha_dt, "parse_datetime", None)
    deadline = (
        parse_datetime(str(call.data["deadline"]))
        if parse_datetime is not None
        else datetime.fromisoformat(str(call.data["deadline"]))
    )
    earliest_raw = call.data.get("earliest_start")
    earliest_start = (
        (
            parse_datetime(str(earliest_raw))
            if parse_datetime is not None
            else datetime.fromisoformat(str(earliest_raw))
        )
        if earliest_raw
        else None
    )
    if deadline is None:
        raise ValueError("deadline must be a valid ISO datetime")
    return await coordinator.async_book_flex_load(
        job_id=str(call.data["job_id"]),
        name=str(call.data.get("name") or call.data["job_id"]),
        duration_minutes=int(call.data["duration_minutes"]),
        deadline=deadline,
        earliest_start=earliest_start,
        preferred_source=str(call.data.get("preferred_source", "cheap")),
        energy_wh=float(call.data.get("energy_wh", 0.0) or 0.0),
        power_w=float(call.data.get("power_w", 0.0) or 0.0),
        min_solar_w=(
            float(call.data["min_solar_w"])
            if call.data.get("min_solar_w") is not None
            else None
        ),
        max_grid_w=(
            float(call.data["max_grid_w"])
            if call.data.get("max_grid_w") is not None
            else None
        ),
        allow_battery=bool(call.data.get("allow_battery", False)),
    )


async def _async_handle_cancel_flex_load(hass: HomeAssistant, call: Any) -> dict[str, Any]:
    """Cancel an existing flex-load reservation."""
    coordinator = _resolve_target_coordinator(hass, call.data.get("entry_id"))
    return await coordinator.async_cancel_flex_load(str(call.data["job_id"]))


def _async_register_service(
    hass: HomeAssistant,
    service: str,
    handler: Any,
    *,
    supports_response: Any | None = None,
) -> None:
    """Register a SolarFriend service with HA-version-tolerant response support.

    Some HA environments or test doubles may not accept the `supports_response`
    kwarg yet. In that case we fall back to plain registration so the
    integration still starts instead of failing setup.
    """
    if supports_response is None:
        hass.services.async_register(DOMAIN, service, handler)
        return

    try:
        signature = inspect.signature(hass.services.async_register)
        supports_kwarg = "supports_response" in signature.parameters
    except (TypeError, ValueError):
        supports_kwarg = True

    if supports_kwarg:
        try:
            hass.services.async_register(
                DOMAIN,
                service,
                handler,
                supports_response=supports_response,
            )
            return
        except TypeError:
            _LOGGER.warning(
                "HA service registry rejected supports_response for %s; "
                "registering without response support",
                service,
            )

    hass.services.async_register(DOMAIN, service, handler)


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
    legacy_price_sensor = entry.data.get("price_sensor")
    if legacy_price_sensor and (
        CONF_BUY_PRICE_SENSOR not in entry.data or CONF_SELL_PRICE_SENSOR not in entry.data
    ):
        new_data = dict(entry.data)
        new_data.setdefault(CONF_BUY_PRICE_SENSOR, legacy_price_sensor)
        new_data.setdefault(CONF_SELL_PRICE_SENSOR, legacy_price_sensor)
        hass.config_entries.async_update_entry(entry, data=new_data)

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

        _async_register_service(
            hass,
            SERVICE_POPULATE_LOAD_MODEL,
            _handle_populate_load_model,
        )
    if not hass.services.has_service(DOMAIN, SERVICE_BOOK_FLEX_LOAD):
        async def _handle_book_flex_load(call: Any) -> dict[str, Any]:
            return await _async_handle_book_flex_load(hass, call)

        _async_register_service(
            hass,
            SERVICE_BOOK_FLEX_LOAD,
            _handle_book_flex_load,
            supports_response=SupportsResponse.ONLY,
        )
    if not hass.services.has_service(DOMAIN, SERVICE_CANCEL_FLEX_LOAD):
        async def _handle_cancel_flex_load(call: Any) -> dict[str, Any]:
            return await _async_handle_cancel_flex_load(hass, call)

        _async_register_service(
            hass,
            SERVICE_CANCEL_FLEX_LOAD,
            _handle_cancel_flex_load,
            supports_response=SupportsResponse.ONLY,
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
        if not hass.data[DOMAIN]:
            for service in (
                SERVICE_POPULATE_LOAD_MODEL,
                SERVICE_BOOK_FLEX_LOAD,
                SERVICE_CANCEL_FLEX_LOAD,
            ):
                if hass.services.has_service(DOMAIN, service):
                    hass.services.async_remove(DOMAIN, service)
    return unloaded
