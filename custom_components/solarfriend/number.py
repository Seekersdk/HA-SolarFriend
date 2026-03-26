"""SolarFriend number platform — user-adjustable parameters."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from homeassistant.components.number import (
    NumberDeviceClass,
    NumberEntity,
    NumberEntityDescription,
    NumberMode,
    RestoreNumber,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, UnitOfPower
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator, ev_device_info

_LOGGER = logging.getLogger(__name__)

UNIT_DKK_KWH = "kr/kWh"


@dataclass(frozen=True)
class SolarFriendNumberDescription(NumberEntityDescription):
    """Extends NumberEntityDescription with config-entry key and step."""
    config_key: str = ""


NUMBER_DESCRIPTIONS: tuple[SolarFriendNumberDescription, ...] = (
    SolarFriendNumberDescription(
        key="charge_rate_kw",
        name="Charge Rate",
        config_key="charge_rate_kw",
        native_min_value=0.5,
        native_max_value=20.0,
        native_step=0.5,
        native_unit_of_measurement=UnitOfPower.KILO_WATT,
        device_class=NumberDeviceClass.POWER,
        mode=NumberMode.BOX,
        icon="mdi:lightning-bolt",
    ),
    SolarFriendNumberDescription(
        key="battery_min_soc",
        name="Battery Min SOC",
        config_key="battery_min_soc",
        native_min_value=0,
        native_max_value=50,
        native_step=5,
        native_unit_of_measurement=PERCENTAGE,
        mode=NumberMode.SLIDER,
        icon="mdi:battery-low",
    ),
    SolarFriendNumberDescription(
        key="battery_max_soc",
        name="Battery Max SOC",
        config_key="battery_max_soc",
        native_min_value=50,
        native_max_value=100,
        native_step=5,
        native_unit_of_measurement=PERCENTAGE,
        mode=NumberMode.SLIDER,
        icon="mdi:battery-high",
    ),
    SolarFriendNumberDescription(
        key="min_charge_saving",
        name="Min Charge Saving",
        config_key="min_charge_saving",
        native_min_value=0.05,
        native_max_value=1.00,
        native_step=0.05,
        native_unit_of_measurement=UNIT_DKK_KWH,
        mode=NumberMode.SLIDER,
        icon="mdi:cash-check",
    ),
    SolarFriendNumberDescription(
        key="cheap_grid_threshold",
        name="Cheap Grid Threshold",
        config_key="cheap_grid_threshold",
        native_min_value=0.0,
        native_max_value=1.00,
        native_step=0.05,
        native_unit_of_measurement=UNIT_DKK_KWH,
        mode=NumberMode.SLIDER,
        icon="mdi:transmission-tower-import",
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]

    entities: list = [
        SolarFriendNumber(coordinator, entry, description)
        for description in NUMBER_DESCRIPTIONS
    ]

    if entry.data.get("ev_charging_enabled", False):
        entities.append(SolarFriendEVTargetSOCNumber(coordinator))
        entities.append(SolarFriendEVMinRangeNumber(coordinator))

    async_add_entities(entities)


class SolarFriendNumber(RestoreEntity, NumberEntity):
    """A user-adjustable SolarFriend parameter stored in the config entry."""

    entity_description: SolarFriendNumberDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SolarFriendCoordinator,
        entry: ConfigEntry,
        description: SolarFriendNumberDescription,
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.data.get("name", "SolarFriend"),
            manufacturer="SolarFriend",
            model="Solar Energy Manager",
        )
        # Initialise from config entry; restored state takes precedence in async_added_to_hass
        defaults = {
            "charge_rate_kw":    6.0,
            "battery_min_soc":  10.0,
            "battery_max_soc":  90.0,
            "min_charge_saving": 0.20,
            "cheap_grid_threshold": 0.10,
        }
        fallback = defaults.get(description.config_key, description.native_min_value)
        self._attr_native_value = float(entry.data.get(description.config_key, fallback))

    async def async_added_to_hass(self) -> None:
        """Restore last known value if HA was restarted."""
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state not in ("unknown", "unavailable"):
            try:
                restored_value = float(last_state.state)
            except (ValueError, TypeError):
                return

            self._attr_native_value = restored_value
            await self._async_sync_config_value(restored_value, reason="restored")
            self.async_write_ha_state()

    async def async_set_native_value(self, value: float) -> None:
        """Persist new value to config entry and trigger coordinator refresh."""
        self._attr_native_value = value
        await self._async_sync_config_value(value, reason="updated")
        self.async_write_ha_state()
        _LOGGER.debug(
            "SolarFriend number %s updated to %s",
            self.entity_description.config_key,
            value,
        )

    async def _async_sync_config_value(self, value: float, *, reason: str) -> None:
        """Keep config entry and optimizer runtime in sync with the exposed number."""
        key = self.entity_description.config_key
        try:
            current_value = float(self._entry.data.get(key, value))
        except (ValueError, TypeError):
            current_value = value

        changed = current_value != float(value)
        if changed:
            new_data = {**self._entry.data, key: value}
            self.hass.config_entries.async_update_entry(self._entry, data=new_data)

        if changed or reason == "updated":
            await self._coordinator.async_on_runtime_setting_changed(
                reason=f"number-{key}-{reason}"
            )




class SolarFriendEVTargetSOCNumber(RestoreNumber):
    """Mål-SOC for EV-oplader (50–100 %, standard 80 %)."""

    _attr_has_entity_name = True
    _attr_name = "Mål-SOC"
    _attr_icon = "mdi:battery-charging-80"
    _attr_native_min_value = 50
    _attr_native_max_value = 100
    _attr_native_step = 5
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_mode = NumberMode.SLIDER

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_target_soc"
        self._attr_device_info = ev_device_info(coordinator)
        self._attr_native_value = 80.0

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last = await self.async_get_last_number_data()
        if last and last.native_value is not None:
            self._attr_native_value = float(last.native_value)
        self._coordinator.ev_target_soc_override = self._attr_native_value

    async def async_set_native_value(self, value: float) -> None:
        self._attr_native_value = value
        self._coordinator.ev_target_soc_override = value
        self.async_write_ha_state()
        _LOGGER.debug("EV mål-SOC sat til %.0f%%", value)


class SolarFriendEVMinRangeNumber(RestoreNumber):
    """Minimum rækkevidde — tving nødopladning hvis under dette (0 = deaktiveret)."""

    _attr_has_entity_name = True
    _attr_name = "Min Rækkevidde"
    _attr_icon = "mdi:map-marker-distance"
    _attr_native_min_value = 0
    _attr_native_max_value = 500
    _attr_native_step = 10
    _attr_native_unit_of_measurement = "km"
    _attr_mode = NumberMode.SLIDER

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_min_range"
        self._attr_device_info = ev_device_info(coordinator)
        self._attr_native_value = 0.0

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last = await self.async_get_last_number_data()
        if last and last.native_value is not None:
            self._attr_native_value = float(last.native_value)
        self._coordinator.ev_min_range_km = self._attr_native_value

    async def async_set_native_value(self, value: float) -> None:
        self._attr_native_value = value
        self._coordinator.ev_min_range_km = value
        self.async_write_ha_state()
        _LOGGER.debug("EV min rækkevidde sat til %.0f km", value)
