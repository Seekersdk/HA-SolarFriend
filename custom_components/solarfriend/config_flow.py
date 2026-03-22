"""Config flow for SolarFriend."""
from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import DOMAIN

# Config entry keys
CONF_PV_POWER_SENSOR = "pv_power_sensor"
CONF_GRID_POWER_SENSOR = "grid_power_sensor"
CONF_BATTERY_SOC_SENSOR = "battery_soc_sensor"
CONF_BATTERY_POWER_SENSOR = "battery_power_sensor"
CONF_LOAD_POWER_SENSOR = "load_power_sensor"
CONF_PRICE_SENSOR = "price_sensor"
CONF_FORECAST_SENSOR = "forecast_sensor"
CONF_BATTERY_CAPACITY = "battery_capacity_kwh"
CONF_BATTERY_MIN_SOC = "battery_min_soc"
CONF_BATTERY_MAX_SOC = "battery_max_soc"
CONF_USABLE_CAPACITY = "usable_capacity_kwh"
CONF_BATTERY_PRICE = "battery_price_dkk"
CONF_BATTERY_CYCLES = "battery_cycles"
CONF_MIN_CHARGE_SAVING = "min_charge_saving"
CONF_BATTERY_COST_PER_KWH = "battery_cost_per_kwh"
CONF_FORECAST_TYPE = "forecast_type"
CONF_INVERTER_TYPE = "inverter_type"

# Deye control entity keys
CONF_DEYE_GRID_CHARGE_SWITCH   = "deye_grid_charge_switch"
CONF_DEYE_TIME_OF_USE_SWITCH   = "deye_time_of_use_switch"
CONF_DEYE_TIME_POINT_1_ENABLE  = "deye_time_point_1_enable"
CONF_DEYE_TIME_POINT_1_START   = "deye_time_point_1_start"
CONF_DEYE_TIME_POINT_1_CAPACITY = "deye_time_point_1_capacity"
CONF_DEYE_GRID_CHARGE_CURRENT  = "deye_grid_charge_current"
CONF_DEYE_ENERGY_PRIORITY      = "deye_energy_priority"

DEFAULT_NAME = "SolarFriend"
FORECAST_DEFAULT = "sensor.energy_production_today"
SOLCAST_SENSOR = "sensor.solcast_pv_forecast_forecast_today"

# klatremis/esphome-for-deye entity_id suffixes — work for any device_type prefix
# (e.g. sun12k, deye12, myinverter, ...)
_KLATREMIS_SENSOR_SUFFIXES: dict[str, list[str]] = {
    CONF_PV_POWER_SENSOR:      ["_pv1_power", "_pv2_power"],
    CONF_GRID_POWER_SENSOR:    ["_total_grid_power"],
    CONF_BATTERY_SOC_SENSOR:   ["_battery_capacity"],
    CONF_BATTERY_POWER_SENSOR: ["_battery_output_power"],
    CONF_LOAD_POWER_SENSOR:    ["_load_totalpower"],
}

_KLATREMIS_CONTROL_SUFFIXES: dict[str, tuple[str, list[str]]] = {
    CONF_DEYE_GRID_CHARGE_SWITCH:    ("switch", ["_grid_charge"]),
    CONF_DEYE_TIME_OF_USE_SWITCH:    ("switch", ["_time_of_use"]),
    CONF_DEYE_TIME_POINT_1_ENABLE:   ("switch", ["_time_point_1-6_charge_enable"]),
    CONF_DEYE_TIME_POINT_1_START:    ("number", ["_time_point_1-6_start"]),
    CONF_DEYE_TIME_POINT_1_CAPACITY: ("number", ["_time_point_1-6_capacity"]),
    CONF_DEYE_GRID_CHARGE_CURRENT:   ("number", ["_maximum_battery_grid_charge_current", "_grid_charge_current"]),
    CONF_DEYE_ENERGY_PRIORITY:       ("select", ["_energy_priority"]),
}

# (device_class, name keywords) used as broad fallback for unknown integrations
_SENSOR_PATTERNS: dict[str, tuple[str, list[str]]] = {
    CONF_PV_POWER_SENSOR:      ("power",   ["pv", "solar", "panel", "photovoltaic"]),
    CONF_GRID_POWER_SENSOR:    ("power",   ["grid", "net", "mains"]),
    CONF_BATTERY_POWER_SENSOR: ("power",   ["battery", "bat", "batt"]),
    CONF_BATTERY_SOC_SENSOR:   ("battery", ["soc", "battery", "bat", "charge", "capacity"]),
    CONF_LOAD_POWER_SENSOR:    ("power",   ["load", "consumption", "forbrug", "house", "home", "totalpower"]),
}


def _guess_deye_sensors(hass: HomeAssistant) -> dict[str, str | None]:
    """Return {conf_key: entity_id} using a three-pass heuristic.

    Pass 1 — klatremis suffix match: sensor entity_id ends with a known
              klatremis suffix (works for any device_type prefix).
    Pass 2 — device-registry lookup: find entities on an ESPHome/Deye
              device, then match by device_class + name keywords.
    Pass 3 — broad keyword fallback for unknown integrations.
    """
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    all_sensors = [e for e in ent_reg.entities.values() if e.domain == "sensor"]
    result: dict[str, str | None] = {k: None for k in _KLATREMIS_SENSOR_SUFFIXES}

    # ------------------------------------------------------------------ #
    # Pass 1 — klatremis entity_id suffix                                #
    # ------------------------------------------------------------------ #
    for conf_key, suffixes in _KLATREMIS_SENSOR_SUFFIXES.items():
        for suffix in suffixes:
            match = next(
                (e.entity_id for e in all_sensors if e.entity_id.endswith(suffix)),
                None,
            )
            if match:
                result[conf_key] = match
                break

    if all(result.values()):
        return result

    # ------------------------------------------------------------------ #
    # Pass 2 — ESPHome / Deye device in device registry                  #
    # ------------------------------------------------------------------ #
    esphome_device_ids: set[str] = {
        dev.id
        for dev in dev_reg.devices.values()
        if any(
            kw in (part or "").lower()
            for part in (dev.manufacturer, dev.model, dev.name, str(dev.entry_type))
            for kw in ("deye", "esphome", "sun12k")
        )
    }

    device_candidates = [
        e for e in all_sensors if e.device_id in esphome_device_ids
    ]
    result = _match_by_pattern(hass, device_candidates, result)

    if all(result.values()):
        return result

    # ------------------------------------------------------------------ #
    # Pass 3 — broad keyword fallback                                     #
    # ------------------------------------------------------------------ #
    broad_candidates = [
        e for e in all_sensors
        if any(kw in e.entity_id for kw in ("deye", "sun12k", "esphome"))
    ]
    result = _match_by_pattern(hass, broad_candidates, result)
    return result


def _match_by_pattern(
    hass: HomeAssistant,
    candidates: list[er.RegistryEntry],
    current: dict[str, str | None],
) -> dict[str, str | None]:
    """Fill None slots in `current` by matching candidates on device_class + keywords."""
    result = dict(current)

    for conf_key, (expected_dc, keywords) in _SENSOR_PATTERNS.items():
        if result.get(conf_key):
            continue  # already matched in a prior pass

        for entry in candidates:
            state = hass.states.get(entry.entity_id)
            if state is None:
                continue
            dc = (
                state.attributes.get("device_class")
                or entry.device_class
                or entry.original_device_class
                or ""
            )
            if dc != expected_dc:
                continue
            name_haystack = (entry.entity_id + (entry.name or "") + (entry.original_name or "")).lower()
            if any(kw in name_haystack for kw in keywords):
                result[conf_key] = entry.entity_id
                break

    return result


def _get_sensors_by_device_class(
    hass: HomeAssistant, *device_classes: str
) -> dict[str, str]:
    """Return {entity_id: friendly_name} for sensors matching any of the given device classes.

    Falls back to unit_of_measurement when device_class is not set, so ESPHome
    sensors without explicit device_class are still included.
    """
    _POWER_UNITS = {"W", "kW", "VA", "kVA"}
    _BATTERY_UNITS = {"%"}

    registry = er.async_get(hass)
    result: dict[str, str] = {}
    for entity in registry.entities.values():
        if entity.domain != "sensor":
            continue
        state = hass.states.get(entity.entity_id)
        if state is None:
            continue
        dc = state.attributes.get("device_class") or entity.device_class or entity.original_device_class
        unit = state.attributes.get("unit_of_measurement", "")

        matched = dc in device_classes
        if not matched:
            if "power" in device_classes and unit in _POWER_UNITS:
                matched = True
            elif "battery" in device_classes and unit in _BATTERY_UNITS:
                matched = True

        if matched:
            name = state.attributes.get("friendly_name") or entity.entity_id
            result[entity.entity_id] = f"{name} ({entity.entity_id})"
    return dict(sorted(result.items(), key=lambda x: x[1]))


def _get_forecast_sensors(hass: HomeAssistant) -> dict[str, str]:
    """Return sensors with device_class energy or 'production' in their entity_id/name."""
    registry = er.async_get(hass)
    result: dict[str, str] = {}
    for entity in registry.entities.values():
        if entity.domain != "sensor":
            continue
        state = hass.states.get(entity.entity_id)
        if state is None:
            continue
        dc = state.attributes.get("device_class") or entity.device_class or entity.original_device_class
        name = state.attributes.get("friendly_name") or entity.entity_id
        if dc == "energy" or "production" in entity.entity_id or "production" in name.lower():
            result[entity.entity_id] = f"{name} ({entity.entity_id})"
    return dict(sorted(result.items(), key=lambda x: x[1]))


def _get_entities_by_domain(hass: HomeAssistant, *domains: str) -> dict[str, str]:
    """Return {entity_id: 'friendly_name (entity_id)'} for all entities in given domains.

    Combines entity registry + live state machine so template entities
    (which may not yet be in the registry) are also included.
    """
    registry = er.async_get(hass)
    result: dict[str, str] = {}

    # From entity registry
    for entity in registry.entities.values():
        if entity.domain not in domains:
            continue
        state = hass.states.get(entity.entity_id)
        name = (
            (state.attributes.get("friendly_name") if state else None)
            or entity.name
            or entity.original_name
            or entity.entity_id
        )
        result[entity.entity_id] = f"{name} ({entity.entity_id})"

    # From state machine (catches template entities not yet in registry)
    for state in hass.states.async_all():
        domain = state.entity_id.split(".")[0]
        if domain not in domains or state.entity_id in result:
            continue
        name = state.attributes.get("friendly_name") or state.entity_id
        result[state.entity_id] = f"{name} ({state.entity_id})"

    return dict(sorted(result.items(), key=lambda x: x[1]))


def _guess_deye_control_entities(hass: HomeAssistant) -> dict[str, str | None]:
    """Return {conf_key: entity_id} for Deye write-entities (switch/number/select).

    Uses klatremis entity_id suffixes — works with any {device_type} prefix.
    """
    registry = er.async_get(hass)
    by_domain: dict[str, list[str]] = {}
    for e in registry.entities.values():
        by_domain.setdefault(e.domain, []).append(e.entity_id)

    result: dict[str, str | None] = {}
    for conf_key, (domain, suffixes) in _KLATREMIS_CONTROL_SUFFIXES.items():
        candidates = by_domain.get(domain, [])
        result[conf_key] = next(
            (eid for suffix in suffixes for eid in candidates if eid.endswith(suffix)),
            None,
        )
    return result


class SolarFriendConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the SolarFriend config flow."""

    VERSION = 1

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Step 1 — Name
    # ------------------------------------------------------------------

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors: dict[str, str] = {}

        if user_input is not None:
            self._data[CONF_NAME] = user_input[CONF_NAME]
            return await self.async_step_inverter_type()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {vol.Required(CONF_NAME, default=DEFAULT_NAME): cv.string}
            ),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 1b — Inverter type
    # ------------------------------------------------------------------

    async def async_step_inverter_type(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._data[CONF_INVERTER_TYPE] = user_input[CONF_INVERTER_TYPE]
            return await self.async_step_power_sensors()

        schema = vol.Schema(
            {
                vol.Required(CONF_INVERTER_TYPE, default="deye_klatremis"): SelectSelector(
                    SelectSelectorConfig(
                        options=[
                            {"value": "deye_klatremis", "label": "Deye via ESPHome (klatremis) — anbefalet"},
                        ],
                        mode=SelectSelectorMode.LIST,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="inverter_type",
            data_schema=schema,
        )

    # ------------------------------------------------------------------
    # Step 2 — Power & battery sensors
    # ------------------------------------------------------------------

    async def async_step_power_sensors(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        power_sensors = await self.hass.async_add_executor_job(
            _get_sensors_by_device_class, self.hass, "power", "battery"
        )
        guesses = _guess_deye_sensors(self.hass)

        if user_input is not None:
            self._data.update(user_input)
            return await self.async_step_battery_config()

        def _req(conf_key: str) -> vol.Required:
            guess = guesses.get(conf_key)
            return vol.Required(conf_key, default=guess) if guess in power_sensors else vol.Required(conf_key)

        schema = vol.Schema(
            {
                _req(CONF_PV_POWER_SENSOR):      vol.In(power_sensors),
                _req(CONF_GRID_POWER_SENSOR):    vol.In(power_sensors),
                _req(CONF_BATTERY_SOC_SENSOR):   vol.In(power_sensors),
                _req(CONF_BATTERY_POWER_SENSOR): vol.In(power_sensors),
                _req(CONF_LOAD_POWER_SENSOR):    vol.In(power_sensors),
            }
        )

        matched = sum(1 for v in guesses.values() if v in power_sensors)
        description_placeholders = {"matched": str(matched), "total": str(len(_KLATREMIS_SENSOR_SUFFIXES))}

        return self.async_show_form(
            step_id="power_sensors",
            data_schema=schema,
            errors=errors,
            description_placeholders=description_placeholders,
        )

    # ------------------------------------------------------------------
    # Step 3 — Battery configuration
    # ------------------------------------------------------------------

    async def async_step_battery_config(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            min_soc = user_input[CONF_BATTERY_MIN_SOC]
            max_soc = user_input[CONF_BATTERY_MAX_SOC]
            if min_soc >= max_soc:
                errors["battery_min_soc"] = "min_soc_above_max"
            else:
                capacity = user_input[CONF_BATTERY_CAPACITY]
                user_input[CONF_USABLE_CAPACITY] = round(
                    capacity * (max_soc - min_soc) / 100, 3
                )
                self._data.update(user_input)
                return await self.async_step_battery_economics()

        schema = vol.Schema(
            {
                vol.Required(CONF_BATTERY_CAPACITY): NumberSelector(
                    NumberSelectorConfig(min=1, max=100, step=0.5, unit_of_measurement="kWh", mode=NumberSelectorMode.BOX)
                ),
                vol.Required(CONF_BATTERY_MIN_SOC, default=10): NumberSelector(
                    NumberSelectorConfig(min=0, max=50, step=5, unit_of_measurement="%", mode=NumberSelectorMode.SLIDER)
                ),
                vol.Required(CONF_BATTERY_MAX_SOC, default=90): NumberSelector(
                    NumberSelectorConfig(min=50, max=100, step=5, unit_of_measurement="%", mode=NumberSelectorMode.SLIDER)
                ),
            }
        )

        return self.async_show_form(
            step_id="battery_config",
            data_schema=schema,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 4 — Battery economics
    # ------------------------------------------------------------------

    async def async_step_battery_economics(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            capacity = self._data.get(CONF_BATTERY_CAPACITY, 1.0)
            cycles = user_input[CONF_BATTERY_CYCLES]
            price_dkk = user_input[CONF_BATTERY_PRICE]
            denominator = cycles * capacity
            user_input[CONF_BATTERY_COST_PER_KWH] = round(price_dkk / denominator, 4) if denominator else 0.0
            self._data.update(user_input)
            return await self.async_step_price_sensor()

        schema = vol.Schema(
            {
                vol.Required(CONF_BATTERY_PRICE, default=15000): NumberSelector(
                    NumberSelectorConfig(min=1000, max=200000, step=500, unit_of_measurement="kr", mode=NumberSelectorMode.BOX)
                ),
                vol.Required(CONF_BATTERY_CYCLES, default=4000): NumberSelector(
                    NumberSelectorConfig(min=1000, max=10000, step=100, mode=NumberSelectorMode.BOX)
                ),
                vol.Required(CONF_MIN_CHARGE_SAVING, default=0.20): NumberSelector(
                    NumberSelectorConfig(min=0.05, max=1.00, step=0.05, unit_of_measurement="kr/kWh", mode=NumberSelectorMode.SLIDER)
                ),
            }
        )

        return self.async_show_form(
            step_id="battery_economics",
            data_schema=schema,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 5 — Price sensor
    # ------------------------------------------------------------------

    async def async_step_price_sensor(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        price_sensors = await self.hass.async_add_executor_job(
            _get_sensors_by_device_class, self.hass, "monetary"
        )

        if user_input is not None:
            self._data.update(user_input)
            return await self.async_step_forecast_type()

        schema = vol.Schema(
            {
                vol.Required(CONF_PRICE_SENSOR): vol.In(price_sensors),
            }
        )

        return self.async_show_form(
            step_id="price_sensor",
            data_schema=schema,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 6 — Forecast integration type
    # ------------------------------------------------------------------

    async def async_step_forecast_type(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._data[CONF_FORECAST_TYPE] = user_input[CONF_FORECAST_TYPE]
            if user_input[CONF_FORECAST_TYPE] == "solcast":
                # Solcast uses fixed entity IDs — no sensor selection needed
                self._data[CONF_FORECAST_SENSOR] = SOLCAST_SENSOR
                return await self.async_step_deye_control()
            return await self.async_step_forecast_sensor()

        # Auto-detect Solcast
        solcast_present = self.hass.states.get(SOLCAST_SENSOR) is not None
        default_type = "solcast" if solcast_present else "forecast_solar"

        schema = vol.Schema(
            {
                vol.Required(CONF_FORECAST_TYPE, default=default_type): SelectSelector(
                    SelectSelectorConfig(
                        options=[
                            {"value": "solcast",        "label": "Solcast PV Forecast (anbefalet)"},
                            {"value": "forecast_solar", "label": "Forecast.Solar / anden sensor"},
                        ],
                        mode=SelectSelectorMode.LIST,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="forecast_type",
            data_schema=schema,
        )

    # ------------------------------------------------------------------
    # Step 7 — Forecast sensor (only shown when forecast_solar is chosen)
    # ------------------------------------------------------------------

    async def async_step_forecast_sensor(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        forecast_sensors = await self.hass.async_add_executor_job(
            _get_forecast_sensors, self.hass
        )

        default_forecast = (
            FORECAST_DEFAULT if FORECAST_DEFAULT in forecast_sensors else next(iter(forecast_sensors), None)
        )

        if user_input is not None:
            self._data.update(user_input)
            return await self.async_step_deye_control()

        schema = vol.Schema(
            {
                vol.Required(CONF_FORECAST_SENSOR, default=default_forecast): vol.In(
                    forecast_sensors
                ),
            }
        )

        return self.async_show_form(
            step_id="forecast_sensor",
            data_schema=schema,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 8 — Deye control entities
    # ------------------------------------------------------------------

    async def async_step_deye_control(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._data.update({k: v for k, v in user_input.items() if v})
            return self.async_create_entry(
                title=self._data[CONF_NAME],
                data=self._data,
            )

        switch_entities = await self.hass.async_add_executor_job(
            _get_entities_by_domain, self.hass, "switch"
        )
        number_entities = await self.hass.async_add_executor_job(
            _get_entities_by_domain, self.hass, "number"
        )
        select_entities = await self.hass.async_add_executor_job(
            _get_entities_by_domain, self.hass, "select"
        )
        guesses = _guess_deye_control_entities(self.hass)

        def _opt_switch(conf_key: str) -> vol.Optional:
            guess = guesses.get(conf_key)
            return vol.Optional(conf_key, default=guess) if guess in switch_entities else vol.Optional(conf_key)

        def _opt_number(conf_key: str) -> vol.Optional:
            guess = guesses.get(conf_key)
            return vol.Optional(conf_key, default=guess) if guess in number_entities else vol.Optional(conf_key)

        # Filter selects to only energy_priority candidates; fall back to all if none
        priority_entities = {
            eid: label for eid, label in select_entities.items()
            if eid.endswith("_energy_priority")
        } or select_entities

        def _opt_select(conf_key: str) -> vol.Optional:
            guess = guesses.get(conf_key)
            return vol.Optional(conf_key, default=guess) if guess in priority_entities else vol.Optional(conf_key)

        schema = vol.Schema(
            {
                _opt_switch(CONF_DEYE_GRID_CHARGE_SWITCH):    vol.In(switch_entities),
                _opt_switch(CONF_DEYE_TIME_OF_USE_SWITCH):    vol.In(switch_entities),
                _opt_switch(CONF_DEYE_TIME_POINT_1_ENABLE):   vol.In(switch_entities),
                _opt_number(CONF_DEYE_TIME_POINT_1_START):    vol.In(number_entities),
                _opt_number(CONF_DEYE_TIME_POINT_1_CAPACITY): vol.In(number_entities),
                _opt_number(CONF_DEYE_GRID_CHARGE_CURRENT):   vol.In(number_entities),
                _opt_select(CONF_DEYE_ENERGY_PRIORITY):       vol.In(priority_entities),
            }
        )

        detected = sum(1 for v in guesses.values() if v)

        return self.async_show_form(
            step_id="deye_control",
            data_schema=schema,
            description_placeholders={"detected": str(detected)},
        )
