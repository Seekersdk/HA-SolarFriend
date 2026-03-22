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
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME, PERCENTAGE, UnitOfPower
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator

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
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]

    async_add_entities(
        SolarFriendNumber(coordinator, entry, description)
        for description in NUMBER_DESCRIPTIONS
    )


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
            name=entry.data.get(CONF_NAME, "SolarFriend"),
            manufacturer="SolarFriend",
            model="Solar Energy Manager",
        )
        # Initialise from config entry; restored state takes precedence in async_added_to_hass
        defaults = {
            "charge_rate_kw":    6.0,
            "battery_min_soc":  10.0,
            "battery_max_soc":  90.0,
            "min_charge_saving": 0.20,
        }
        fallback = defaults.get(description.config_key, description.native_min_value)
        self._attr_native_value = float(entry.data.get(description.config_key, fallback))

    async def async_added_to_hass(self) -> None:
        """Restore last known value if HA was restarted."""
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None and last_state.state not in ("unknown", "unavailable"):
            try:
                self._attr_native_value = float(last_state.state)
            except (ValueError, TypeError):
                pass

    async def async_set_native_value(self, value: float) -> None:
        """Persist new value to config entry and trigger coordinator refresh."""
        self._attr_native_value = value

        # Merge updated key into config entry data
        new_data = {**self._entry.data, self.entity_description.config_key: value}
        self.hass.config_entries.async_update_entry(self._entry, data=new_data)

        self.async_write_ha_state()
        await self._coordinator.async_request_refresh()

        _LOGGER.debug(
            "SolarFriend number %s updated to %s",
            self.entity_description.config_key,
            value,
        )
