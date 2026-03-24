"""SolarFriend sensor platform."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME, UnitOfEnergy, UnitOfPower, PERCENTAGE
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator, SolarFriendData, ev_device_info

_LOGGER = logging.getLogger(__name__)

UNIT_DKK_KWH = "DKK/kWh"
UNIT_KR_KWH = "kr/kWh"
UNIT_KR = "kr"


@dataclass(frozen=True)
class SolarFriendSensorDescription(SensorEntityDescription):
    """Extends SensorEntityDescription with a value accessor and optional attributes."""

    value_fn: Callable[[SolarFriendData, dict[str, Any]], float | str | None] = (
        lambda d, c: None
    )
    extra_attrs_fn: Callable[[SolarFriendData, dict[str, Any]], dict[str, Any] | None] = (
        lambda d, c: None
    )


# ---------------------------------------------------------------------------
# Attribute helpers
# ---------------------------------------------------------------------------

def _forecast_soc_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Compute hourly_soc + hourly_power attributes for forecast_soc_chart."""
    hourly_soc = d.forecast_soc_chart
    capacity_kwh = float(cfg.get("battery_capacity_kwh", 10.0))
    hourly_power = []
    for i, soc in enumerate(hourly_soc):
        if soc is None or i == 0:
            hourly_power.append(None)
            continue
        prev = hourly_soc[i - 1]
        if prev is None:
            hourly_power.append(None)
            continue
        delta_kwh = (soc - prev) / 100.0 * capacity_kwh
        hourly_power.append(round(delta_kwh * 1000))
    return {"hourly_soc": hourly_soc, "hourly_power": hourly_power}


# ---------------------------------------------------------------------------
# Sensor catalogue
# ---------------------------------------------------------------------------

SENSOR_DESCRIPTIONS: tuple[SolarFriendSensorDescription, ...] = (
    # --- Pass-through ---
    SolarFriendSensorDescription(
        key="pv_power",
        name="PV Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:solar-power",
        value_fn=lambda d, _: d.pv_power,
    ),
    SolarFriendSensorDescription(
        key="grid_power",
        name="Grid Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:transmission-tower",
        value_fn=lambda d, _: d.grid_power,
    ),
    SolarFriendSensorDescription(
        key="load_power",
        name="Load Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:home-lightning-bolt",
        value_fn=lambda d, _: d.load_power,
    ),
    SolarFriendSensorDescription(
        key="battery_soc",
        name="Battery SOC",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d, _: d.battery_soc,
    ),
    SolarFriendSensorDescription(
        key="battery_power",
        name="Battery Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-arrow-up-down",
        value_fn=lambda d, _: d.battery_power,
    ),
    SolarFriendSensorDescription(
        key="current_price",
        name="Current Price",
        native_unit_of_measurement=UNIT_DKK_KWH,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:currency-usd",
        value_fn=lambda d, _: d.price,
    ),
    SolarFriendSensorDescription(
        key="forecast_today",
        name="Forecast Today",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:sun-clock",
        value_fn=lambda d, _: d.forecast,
    ),
    # --- Calculated ---
    SolarFriendSensorDescription(
        key="battery_energy_kwh",
        name="Battery Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:battery",
        value_fn=lambda d, cfg: round(
            d.battery_soc / 100 * cfg.get("battery_capacity_kwh", 0), 2
        ),
    ),
    SolarFriendSensorDescription(
        key="battery_usable_kwh",
        name="Battery Usable Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:battery-arrow-down",
        value_fn=lambda d, cfg: round(
            max(0.0, (d.battery_soc - cfg.get("battery_min_soc", 0)) / 100)
            * cfg.get("battery_capacity_kwh", 0),
            2,
        ),
    ),
    # --- Battery economics ---
    SolarFriendSensorDescription(
        key="battery_cost_per_kwh",
        name="Battery Cost per kWh",
        native_unit_of_measurement=UNIT_KR_KWH,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:battery-heart",
        value_fn=lambda d, _: d.battery_cost_per_kwh,
    ),
    SolarFriendSensorDescription(
        key="charge_threshold",
        name="Charge Threshold",
        native_unit_of_measurement=UNIT_KR_KWH,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:cash-check",
        value_fn=lambda d, _: d.charge_threshold,
    ),
    # --- Consumption profile ---
    SolarFriendSensorDescription(
        key="profile_confidence",
        name="Profile Confidence",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:brain",
        value_fn=lambda d, _: d.profile_confidence,
    ),
    SolarFriendSensorDescription(
        key="profile_days_collected",
        name="Profile Days Collected",
        native_unit_of_measurement="days",
        device_class=None,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:calendar-check",
        value_fn=lambda d, _: d.profile_days_collected,
    ),
    # --- Battery tracker ---
    SolarFriendSensorDescription(
        key="battery_solar_kwh",
        name="Battery Solar Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:solar-power",
        value_fn=lambda d, _: round(d.battery_solar_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_grid_kwh",
        name="Battery Grid Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:transmission-tower",
        value_fn=lambda d, _: round(d.battery_grid_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_weighted_cost",
        name="Battery Weighted Cost",
        native_unit_of_measurement=UNIT_KR_KWH,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:cash",
        value_fn=lambda d, _: round(d.battery_weighted_cost, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_solar_fraction",
        name="Battery Solar Fraction",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:percent",
        value_fn=lambda d, _: round(d.battery_solar_fraction * 100, 1),
    ),
    # --- Savings ---
    SolarFriendSensorDescription(
        key="today_solar_saving",
        name="Sparet på sol i dag",
        native_unit_of_measurement=UNIT_KR,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:solar-power",
        value_fn=lambda d, _: round(d.today_solar_direct_saved_dkk, 2),
    ),
    SolarFriendSensorDescription(
        key="today_optimizer_saving",
        name="Sparet via optimering i dag",
        native_unit_of_measurement=UNIT_KR,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:brain",
        value_fn=lambda d, _: round(d.today_optimizer_saved_dkk, 2),
    ),
    SolarFriendSensorDescription(
        key="total_solar_saving",
        name="Total sparet på sol",
        native_unit_of_measurement=UNIT_KR,
        device_class=None,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:solar-power",
        value_fn=lambda d, _: round(d.total_solar_direct_saved_dkk, 2),
    ),
    SolarFriendSensorDescription(
        key="total_optimizer_saving",
        name="Total sparet via optimering",
        native_unit_of_measurement=UNIT_KR,
        device_class=None,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:brain",
        value_fn=lambda d, _: round(d.total_optimizer_saved_dkk, 2),
    ),
    # --- Optimizer: strategy (with attributes) ---
    SolarFriendSensorDescription(
        key="optimizer_strategy",
        name="Optimizer Strategy",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:brain",
        value_fn=lambda d, _: (
            "Anti-eksport (negativ pris)"
            if d.optimize_result and d.optimize_result.strategy == "ANTI_EXPORT"
            else (d.optimize_result.strategy if d.optimize_result else "IDLE")
        ),
        extra_attrs_fn=lambda d, _: (
            {
                "reason": d.optimize_result.reason,
                "charge_now": d.optimize_result.charge_now,
                "target_soc": d.optimize_result.target_soc,
                "best_discharge_hours": d.optimize_result.best_discharge_hours,
            }
            if d.optimize_result
            else None
        ),
    ),
    # --- Optimizer: numeric outputs ---
    SolarFriendSensorDescription(
        key="optimizer_expected_saving",
        name="Optimizer Expected Saving",
        native_unit_of_measurement=UNIT_KR,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:piggy-bank",
        value_fn=lambda d, _: (
            round(d.optimize_result.expected_saving_dkk, 2)
            if d.optimize_result
            else 0.0
        ),
    ),
    SolarFriendSensorDescription(
        key="optimizer_morning_need",
        name="Optimizer Morning Need",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:weather-sunset-up",
        value_fn=lambda d, _: (
            round(d.optimize_result.morning_need_kwh, 2)
            if d.optimize_result
            else 0.0
        ),
    ),
    SolarFriendSensorDescription(
        key="optimizer_night_charge_target",
        name="Optimizer Night Charge Target",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:battery-charging",
        value_fn=lambda d, _: (
            round(d.optimize_result.night_charge_kwh, 2)
            if d.optimize_result
            else 0.0
        ),
    ),
    SolarFriendSensorDescription(
        key="optimizer_cheapest_charge_hour",
        name="Optimizer Cheapest Charge Hour",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:clock-outline",
        value_fn=lambda d, _: (
            d.optimize_result.cheapest_charge_hour or "N/A"
            if d.optimize_result
            else "N/A"
        ),
    ),
    SolarFriendSensorDescription(
        key="optimizer_target_soc",
        name="Optimizer Target SOC",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-arrow-up",
        value_fn=lambda d, _: (
            d.optimize_result.target_soc or 0
            if d.optimize_result
            else 0
        ),
    ),
    # --- Forecast ---
    SolarFriendSensorDescription(
        key="forecast_remaining_today",
        name="Forecast Remaining Today",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:solar-power",
        value_fn=lambda d, _: (
            round(d.forecast_data.remaining_today_kwh, 3)
            if d.forecast_data
            else None
        ),
    ),
    SolarFriendSensorDescription(
        key="forecast_power_now",
        name="Forecast Power Now",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:white-balance-sunny",
        value_fn=lambda d, _: (
            round(d.forecast_data.power_now_w, 1)
            if d.forecast_data
            else None
        ),
    ),
    SolarFriendSensorDescription(
        key="forecast_peak_today",
        name="Forecast Peak Today",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=None,
        icon="mdi:solar-power-variant",
        value_fn=lambda d, _: (
            round(d.forecast_data.peak_power_today_w, 1)
            if d.forecast_data
            else None
        ),
    ),
    SolarFriendSensorDescription(
        key="forecast_confidence",
        name="Forecast Confidence",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:check-circle",
        value_fn=lambda d, _: (
            round(d.forecast_data.confidence * 100, 1)
            if d.forecast_data
            else None
        ),
    ),
    SolarFriendSensorDescription(
        key="solar_next_2h",
        name="Solar Next 2 Hours",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:weather-sunny-alert",
        value_fn=lambda d, _: round(d.solar_next_2h, 3),
    ),
    SolarFriendSensorDescription(
        key="solar_until_sunset",
        name="Solar Until Sunset",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:weather-sunset",
        value_fn=lambda d, _: round(d.solar_until_sunset, 3),
    ),
    # --- Forecast SOC chart ---
    # apexcharts-card data_generator example:
    #
    #   type: custom:apexcharts-card
    #   entities:
    #     - entity: sensor.solarfriend_forecast_soc_chart
    #       data_generator: |
    #         const profile = entity.attributes.hourly_soc || [];
    #         const today = new Date();
    #         today.setHours(0, 0, 0, 0);
    #         return profile
    #           .map((soc, hour) => soc !== null ? [
    #             today.getTime() + hour * 3600000, soc
    #           ] : null)
    #           .filter(v => v !== null);
    SolarFriendSensorDescription(
        key="forecast_soc_chart",
        name="Forecast SOC Chart",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-clock",
        value_fn=lambda d, _: (
            next(
                (v for v in reversed(d.forecast_soc_chart) if v is not None),
                None,
            )
            if d.forecast_soc_chart
            else None
        ),
        extra_attrs_fn=lambda d, cfg: _forecast_soc_attrs(d, cfg),
    ),
    # --- Consumption profile chart ---
    # apexcharts-card data_generator example:
    #
    #   type: custom:apexcharts-card
    #   entities:
    #     - entity: sensor.solarfriend_consumption_profile_chart
    #       data_generator: |
    #         const profile = entity.attributes.hourly_profile || [];
    #         const today = new Date();
    #         today.setHours(0, 0, 0, 0);
    #         return profile.map((watt, hour) => [
    #           today.getTime() + hour * 3600000,
    #           watt
    #         ]);
    SolarFriendSensorDescription(
        key="consumption_profile_chart",
        name="Consumption Profile Chart",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:chart-bell-curve",
        value_fn=lambda d, _: (
            round(sum(d.consumption_profile_chart), 1)
            if d.consumption_profile_chart
            else 0.0
        ),
        extra_attrs_fn=lambda d, _: {
            "hourly_profile": d.consumption_profile_chart,
            "day_type": d.consumption_profile_day_type,
            "confidence": d.profile_confidence,
            "days_collected": d.profile_days_collected,
        },
    ),
)


# ---------------------------------------------------------------------------
# Platform setup
# ---------------------------------------------------------------------------

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SolarFriendCoordinator = hass.data[DOMAIN][entry.entry_id]

    entities: list = [
        SolarFriendSensor(coordinator, entry, description)
        for description in SENSOR_DESCRIPTIONS
    ]

    if entry.data.get("ev_charging_enabled", False):
        entities.extend([
            SolarFriendEVSensor(
                coordinator, "ev_charging_power", "Ladning",
                UnitOfPower.WATT, SensorDeviceClass.POWER,
                SensorStateClass.MEASUREMENT, "mdi:ev-station",
                lambda d: d.ev_charging_power,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_vehicle_soc", "Bil SOC",
                PERCENTAGE, SensorDeviceClass.BATTERY,
                SensorStateClass.MEASUREMENT, "mdi:battery-electric-vehicle",
                lambda d: d.ev_vehicle_soc,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_surplus_w", "Sol-overskud",
                UnitOfPower.WATT, SensorDeviceClass.POWER,
                SensorStateClass.MEASUREMENT, "mdi:solar-power",
                lambda d: d.ev_surplus_w,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_strategy_reason", "Strategi",
                None, None, None, "mdi:brain",
                lambda d: d.ev_strategy_reason,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_vehicle_soc_kwh", "Bil SOC kWh",
                UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY,
                None, "mdi:battery-electric-vehicle",
                lambda d: d.ev_vehicle_soc_kwh,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_needed_kwh", "Manglende kWh",
                UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY,
                None, "mdi:battery-arrow-up",
                lambda d: d.ev_needed_kwh,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_hours_to_departure", "Timer til afgang",
                "h", None, SensorStateClass.MEASUREMENT, "mdi:car-clock",
                lambda d: d.ev_hours_to_departure,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_charge_mode", "Lademodus",
                None, None, None, "mdi:ev-station",
                lambda d: d.ev_charge_mode,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_target_soc", "Target SOC",
                PERCENTAGE, SensorDeviceClass.BATTERY,
                SensorStateClass.MEASUREMENT, "mdi:battery-charging-80",
                lambda d: d.ev_target_soc,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_target_w", "Target Effekt",
                UnitOfPower.WATT, SensorDeviceClass.POWER,
                SensorStateClass.MEASUREMENT, "mdi:lightning-bolt",
                lambda d: d.ev_target_w,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_phases", "Antal Faser",
                None, None, None, "mdi:numeric",
                lambda d: d.ev_phases,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_min_soc_fra_km", "Min SOC fra km",
                PERCENTAGE, SensorDeviceClass.BATTERY,
                SensorStateClass.MEASUREMENT, "mdi:map-marker-distance",
                lambda d: d.ev_min_soc_from_range,
            ),
            SolarFriendEVSensor(
                coordinator, "ev_nodopladning", "Nødopladning aktiv",
                None, None, None, "mdi:alert-circle",
                lambda d: "Ja" if d.ev_emergency_charging else "Nej",
            ),
            SolarFriendEVSensor(
                coordinator, "ev_charger_status", "Lader status",
                None, None, None, "mdi:ev-station",
                lambda d: d.ev_charger_status,
            ),
            SolarFriendEVPlanSensor(coordinator),
        ])

    async_add_entities(entities)


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------

class SolarFriendSensor(CoordinatorEntity[SolarFriendCoordinator], SensorEntity):
    """A single SolarFriend sensor backed by the coordinator."""

    entity_description: SolarFriendSensorDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SolarFriendCoordinator,
        entry: ConfigEntry,
        description: SolarFriendSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.data.get(CONF_NAME, "SolarFriend"),
            manufacturer="SolarFriend",
            model="Solar Energy Manager",
        )

    @property
    def native_value(self) -> float | str | None:
        if self.coordinator.data is None:
            return None
        return self.entity_description.value_fn(
            self.coordinator.data, self._entry.data
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.coordinator.data is None:
            return None
        return self.entity_description.extra_attrs_fn(self.coordinator.data, self._entry.data)

    @property
    def available(self) -> bool:
        if not super().available or self.coordinator.data is None:
            return False
        # Mark unavailable if this sensor's source was missing last cycle
        key_map = {
            "pv_power": "pv_power",
            "grid_power": "grid_power",
            "load_power": "load_power",
            "battery_soc": "battery_soc",
            "battery_energy_kwh": "battery_soc",
            "battery_usable_kwh": "battery_soc",
            "current_price": "price",
            "forecast_today": "forecast",
        }
        source = key_map.get(self.entity_description.key)
        if source and source in self.coordinator.data.unavailable:
            return False
        return True


class SolarFriendEVSensor(CoordinatorEntity[SolarFriendCoordinator], SensorEntity):
    """EV charging sensor — only created when ev_charging_enabled."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SolarFriendCoordinator,
        key: str,
        name: str,
        unit: str | None,
        device_class: SensorDeviceClass | None,
        state_class: SensorStateClass | None,
        icon: str,
        value_fn: Callable[[SolarFriendData], Any],
    ) -> None:
        super().__init__(coordinator)
        self._key = key
        self._value_fn = value_fn
        self._attr_name = name
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_{key}"
        self._attr_native_unit_of_measurement = unit
        self._attr_device_class = device_class
        self._attr_state_class = state_class
        self._attr_icon = icon

    @property
    def device_info(self) -> DeviceInfo:
        return ev_device_info(self.coordinator)

    @property
    def native_value(self) -> float | str | None:
        if self.coordinator.data is None:
            return None
        try:
            return self._value_fn(self.coordinator.data)
        except Exception:  # noqa: BLE001
            return None


class SolarFriendEVPlanSensor(CoordinatorEntity[SolarFriendCoordinator], SensorEntity):
    """EV charging plan chart sensor — shows expected SOC at departure with hourly breakdown.

    apexcharts-card data_generator example:

      type: custom:apexcharts-card
      entities:
        - entity: sensor.solarfriend_ev_plan
          data_generator: |
            const plan = entity.attributes.hourly_plan || [];
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            return plan.map(h => {
              const [hh] = h.hour.split(':').map(Number);
              return [today.getTime() + hh * 3600000, h.soc];
            });
    """

    _attr_has_entity_name = True
    _attr_name = "EV Plan"
    _attr_icon = "mdi:chart-timeline-variant"
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator: SolarFriendCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_ev_plan"

    @property
    def device_info(self) -> DeviceInfo:
        return ev_device_info(self.coordinator)

    @property
    def native_value(self) -> float | None:
        """Expected SOC at departure (last entry in plan)."""
        if self.coordinator.data is None:
            return None
        plan = self.coordinator.data.ev_plan
        if not plan:
            return None
        return plan[-1]["soc"]

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        if self.coordinator.data is None:
            return {}
        data = self.coordinator.data
        dep = self.coordinator.ev_next_departure
        return {
            "hourly_plan": data.ev_plan,
            "departure": dep.strftime("%H:%M"),
            "target_soc": data.ev_target_soc,
            "current_soc": data.ev_vehicle_soc,
            "charge_mode": data.ev_charge_mode,
        }
