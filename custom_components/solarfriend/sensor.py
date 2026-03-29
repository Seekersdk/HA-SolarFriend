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
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import SolarFriendCoordinator
from .coordinator_models import SolarFriendData, ev_device_info

_LOGGER = logging.getLogger(__name__)

UNIT_DKK_KWH = "DKK/kWh"
UNIT_KR_KWH = "kr/kWh"
UNIT_KR = "kr"

_UNRECORDED_ATTRIBUTE_KEYS: dict[str, frozenset[str]] = {
    "solar_installation_profile": frozenset(
        {
            "response_surface",
            "variants",
            "comparison_today",
            "comparison_tomorrow",
        }
    ),
    "solar_installation_profile_fast": frozenset({"response_surface"}),
    "solar_installation_profile_medium": frozenset({"response_surface"}),
    "solar_installation_profile_fine": frozenset({"response_surface"}),
}


@dataclass(frozen=True)
class SolarFriendSensorDescription(SensorEntityDescription):
    """Extends SensorEntityDescription with a value accessor and optional attributes."""

    entity_registry_visible_default: bool = True
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


def _rounded_or_none(value: float | None, digits: int) -> float | None:
    """Round numeric values but preserve None for temporarily missing data."""
    if value is None:
        return None
    return round(value, digits)


def _battery_plan_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose battery plan series for charts."""
    plan = d.battery_plan or []
    plan_result = _plan_metrics_result(d)
    allowed_slots = plan_result.allowed_discharge_slots if plan_result else []
    allowed_by_hour = {slot["hour"]: slot for slot in allowed_slots}
    return {
        "hourly_plan": plan,
        "hourly_soc": [slot["soc"] for slot in plan],
        "hourly_solar_charge_w": [slot["solar_charge_w"] for slot in plan],
        "hourly_grid_charge_w": [slot["grid_charge_w"] for slot in plan],
        "hourly_discharge_w": [slot["discharge_w"] for slot in plan],
        "hourly_discharge_to_load_w": [slot.get("discharge_to_load_w", slot["discharge_w"]) for slot in plan],
        "hourly_battery_export_w": [slot.get("battery_export_w", 0.0) for slot in plan],
        "hourly_grid_import_w": [slot["grid_import_w"] for slot in plan],
        "hourly_price": [slot["price_dkk"] for slot in plan],
        "hourly_sell_price": [slot.get("sell_price_dkk", slot["price_dkk"]) for slot in plan],
        "allowed_discharge_slots": allowed_slots,
        "allowed_discharge_hours": [slot["hour_str"] for slot in allowed_slots],
        "hourly_allowed_discharge_w": [
            allowed_by_hour.get(slot["hour"], {}).get("baseline_discharge_w", 0)
            for slot in plan
        ],
    }


def _plan_metrics_result(d: "SolarFriendData"):
    """Return the optimizer result that matches the current battery plan snapshot."""
    return d.plan_optimize_result if d.battery_plan and d.plan_optimize_result else d.optimize_result


def _forecast_accuracy_14d_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose rolling forecast quality details."""
    return {
        "bias_factor_14d": d.forecast_bias_factor_14d,
        "mae_14d_kwh": d.forecast_mae_14d_kwh,
        "mape_14d_pct": d.forecast_mape_14d_pct,
        "valid_days_14d": d.forecast_valid_days_14d,
        "correction_valid": d.forecast_correction_valid,
        "history_14d": d.forecast_history_14d,
    }


def _forecast_correction_model_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose passive season/elevation/azimuth forecast correction diagnostics."""
    return {
        "current_season": d.forecast_correction_current_season,
        "active_buckets": d.forecast_correction_active_buckets,
        "confident_buckets": d.forecast_correction_confident_buckets,
        "average_factor_this_season": d.forecast_correction_average_factor_this_season,
        "today_geometry_factors": d.forecast_correction_today_geometry_factors,
        "current_total_factor": d.forecast_correction_current_total_factor,
        "current_geometry_factor": d.forecast_correction_current_geometry_factor,
        "current_geometry_samples": d.forecast_correction_current_geometry_samples,
        "current_geometry_key": d.forecast_correction_current_geometry_key,
        "current_temperature_factor": d.forecast_correction_current_temperature_factor,
        "current_temperature_samples": d.forecast_correction_current_temperature_samples,
        "current_temperature_key": d.forecast_correction_current_temperature_key,
        "raw_vs_corrected_delta_today": d.forecast_correction_raw_vs_corrected_delta_today,
        "last_environment": d.forecast_correction_last_environment,
    }


def _solar_installation_profile_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose solar installation profile diagnostics and comparison data for graphs."""
    return {
        "populated_cells": d.solar_profile_populated_cells,
        "confident_cells": d.solar_profile_confident_cells,
        "astronomical_coverage_pct": d.solar_profile_astronomical_coverage_pct,
        "annual_paths_total": d.solar_profile_annual_paths_total,
        "annual_paths_covered": d.solar_profile_annual_paths_covered,
        "annual_paths_missing": d.solar_profile_annual_paths_missing,
        "clear_sky_observations": d.solar_profile_clear_sky_observations,
        "estimated_hours_to_ready": d.solar_profile_estimated_hours_to_ready,
        "response_surface": d.solar_profile_response_surface,
        "variants": d.solar_profile_variants,
        "comparison_today": d.solar_profile_comparison_today,
        "comparison_tomorrow": d.solar_profile_comparison_tomorrow,
    }


def _solar_profile_variant(d: "SolarFriendData", key: str) -> dict[str, Any]:
    return d.solar_profile_variants.get(key, {})


def _solar_installation_profile_variant_attrs(
    d: "SolarFriendData", _cfg: dict, key: str
) -> dict[str, Any]:
    variant = _solar_profile_variant(d, key)
    return {
        "resolution_key": key,
        "resolution_label": variant.get("resolution_label", key.title()),
        "populated_cells": variant.get("populated_cells", 0),
        "confident_cells": variant.get("confident_cells", 0),
        "astronomical_coverage_pct": variant.get("astronomical_coverage_pct", 0.0),
        "annual_paths_total": variant.get("annual_paths_total", 0),
        "annual_paths_covered": variant.get("annual_paths_covered", 0),
        "annual_paths_missing": variant.get("annual_paths_missing", 0),
        "clear_sky_observations": variant.get("clear_sky_observations", 0),
        "estimated_hours_to_ready": variant.get("estimated_hours_to_ready", 0.0),
        "response_surface": variant.get("response_surface", {}),
    }


def _advanced_consumption_chart_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose advanced model chart series and weather snapshot for dashboards."""
    return {
        "hourly_actual": d.advanced_consumption_model_today_hourly_actual,
        "hourly_prediction": d.advanced_consumption_model_today_hourly_prediction,
        "records_count": d.advanced_consumption_model_records,
        "tracked_days": d.advanced_consumption_model_tracked_days,
        "today_mae_w": d.advanced_consumption_model_today_mae_w,
        "rolling_7d_mae_w": d.advanced_consumption_model_7d_mae_w,
        "recent_daily_totals": d.advanced_consumption_model_recent_daily_totals,
        "last_weather_snapshot": d.advanced_consumption_model_last_weather,
    }


def _model_evaluation_summary_attrs(d: "SolarFriendData", _cfg: dict) -> dict:
    """Expose compact current-month evaluation summary."""
    return {
        "period_month": d.model_evaluation_period_month,
        "rows": d.model_evaluation_rows,
        "best_model": d.model_evaluation_best_model,
        "mae_by_model": d.model_evaluation_mae_by_model,
        "mape_by_model": d.model_evaluation_mape_by_model,
        "bias_by_model": d.model_evaluation_bias_by_model,
    }


def _flex_load_attrs(d: "SolarFriendData", cfg: dict) -> dict:
    """Expose flex-load reservation details for dashboards and automations."""
    return {
        "next_name": d.flex_load_next_name,
        "next_start": d.flex_load_next_start,
        "next_end": d.flex_load_next_end,
        "next_power_w": d.flex_load_next_power_w,
        "reserved_solar_today_kwh": d.flex_load_reserved_solar_today_kwh,
        "reserved_solar_tomorrow_kwh": d.flex_load_reserved_solar_tomorrow_kwh,
        "reservations": d.flex_load_reservations,
    }


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
        state_class=None,
        icon="mdi:sun-clock",
        value_fn=lambda d, _: d.forecast,
    ),
    # --- Calculated ---
    SolarFriendSensorDescription(
        key="battery_energy_kwh",
        name="Battery Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery",
        value_fn=lambda d, cfg: None
        if d.battery_soc is None
        else round(d.battery_soc / 100 * cfg.get("battery_capacity_kwh", 0), 2),
    ),
    SolarFriendSensorDescription(
        key="battery_usable_kwh",
        name="Battery Usable Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-arrow-down",
        value_fn=lambda d, cfg: None
        if d.battery_soc is None
        else round(
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
        state_class=None,
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
        value_fn=lambda d, _: _rounded_or_none(d.battery_solar_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_grid_kwh",
        name="Battery Grid Energy",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:transmission-tower",
        value_fn=lambda d, _: _rounded_or_none(d.battery_grid_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_weighted_cost",
        name="Battery Weighted Cost",
        native_unit_of_measurement=UNIT_KR_KWH,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:cash",
        value_fn=lambda d, _: _rounded_or_none(d.battery_weighted_cost, 3),
    ),
    SolarFriendSensorDescription(
        key="battery_solar_fraction",
        name="Battery Solar Fraction",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:percent",
        value_fn=lambda d, _: None
        if d.battery_solar_fraction is None
        else round(d.battery_solar_fraction * 100, 1),
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
        key="today_battery_sell_export",
        name="Batteri solgt i dag",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:battery-arrow-up",
        value_fn=lambda d, _: round(d.today_battery_sell_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="today_battery_sell_saving",
        name="Batterisalg i dag",
        native_unit_of_measurement=UNIT_KR,
        device_class=SensorDeviceClass.MONETARY,
        state_class=None,
        icon="mdi:cash-fast",
        value_fn=lambda d, _: round(d.today_battery_sell_saved_dkk, 2),
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
        key="total_battery_sell_saving",
        name="Total batterisalg",
        native_unit_of_measurement=UNIT_KR,
        device_class=None,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:cash-multiple",
        value_fn=lambda d, _: round(d.total_battery_sell_saved_dkk, 2),
    ),
    SolarFriendSensorDescription(
        key="flex_load_reservations",
        name="Flex Load Reservations",
        native_unit_of_measurement="jobs",
        device_class=None,
        state_class=None,
        icon="mdi:calendar-clock",
        value_fn=lambda d, _: d.flex_load_reservations_count,
        extra_attrs_fn=lambda d, cfg: _flex_load_attrs(d, cfg),
    ),
    SolarFriendSensorDescription(
        key="flex_load_next_name",
        name="Flex Load Next Name",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:playlist-play",
        value_fn=lambda d, _: d.flex_load_next_name or "Ingen booking",
    ),
    SolarFriendSensorDescription(
        key="flex_load_next_start",
        name="Flex Load Next Start",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:clock-start",
        value_fn=lambda d, _: d.flex_load_next_start or "Ingen booking",
    ),
    SolarFriendSensorDescription(
        key="flex_load_next_power",
        name="Flex Load Next Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:flash-outline",
        value_fn=lambda d, _: round(d.flex_load_next_power_w, 1),
    ),
    SolarFriendSensorDescription(
        key="flex_load_reserved_solar_today",
        name="Flex Load Reserved Solar Today",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:solar-power-variant",
        value_fn=lambda d, _: round(d.flex_load_reserved_solar_today_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="flex_load_reserved_solar_tomorrow",
        name="Flex Load Reserved Solar Tomorrow",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL,
        icon="mdi:weather-sunny-alert",
        value_fn=lambda d, _: round(d.flex_load_reserved_solar_tomorrow_kwh, 3),
    ),
    SolarFriendSensorDescription(
        key="optimizer_strategy",
        name="Optimizer Strategy",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:brain",
        value_fn=lambda d, _: (
            "Negativ import (køb alt fra nettet)"
            if _plan_metrics_result(d) and _plan_metrics_result(d).strategy == "NEGATIVE_IMPORT"
            else (
                "Anti-eksport (negativ pris)"
                if _plan_metrics_result(d) and _plan_metrics_result(d).strategy == "ANTI_EXPORT"
            else (_plan_metrics_result(d).strategy if _plan_metrics_result(d) else "IDLE")
            )
        ),
        extra_attrs_fn=lambda d, _: (
            {
                "reason": _plan_metrics_result(d).reason,
                "charge_now": _plan_metrics_result(d).charge_now,
                "target_soc": _plan_metrics_result(d).target_soc,
                "best_discharge_hours": _plan_metrics_result(d).best_discharge_hours,
                "allowed_discharge_slots": _plan_metrics_result(d).allowed_discharge_slots,
                "allowed_discharge_hours": [
                    slot["hour_str"] for slot in _plan_metrics_result(d).allowed_discharge_slots
                ],
            }
            if _plan_metrics_result(d)
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
            round(_plan_metrics_result(d).expected_saving_dkk, 2)
            if _plan_metrics_result(d)
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
            round(_plan_metrics_result(d).morning_need_kwh, 2)
            if _plan_metrics_result(d)
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
            round(_plan_metrics_result(d).night_charge_kwh, 2)
            if _plan_metrics_result(d)
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
            _plan_metrics_result(d).cheapest_charge_hour or "N/A"
            if _plan_metrics_result(d)
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
            _plan_metrics_result(d).target_soc or 0
            if _plan_metrics_result(d)
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
        state_class=None,
        icon="mdi:weather-sunny-alert",
        value_fn=lambda d, _: round(d.solar_next_2h, 3),
    ),
    SolarFriendSensorDescription(
        key="solar_until_sunset",
        name="Solar Until Sunset",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:weather-sunset",
        value_fn=lambda d, _: round(d.solar_until_sunset, 3),
    ),
    SolarFriendSensorDescription(
        key="forecast_actual_today_so_far",
        name="Forecast Actual Today So Far",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:solar-power",
        value_fn=lambda d, _: d.forecast_actual_today_so_far_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_predicted_today_so_far",
        name="Forecast Predicted Today So Far",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:chart-line",
        value_fn=lambda d, _: d.forecast_predicted_today_so_far_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_error_today_so_far",
        name="Forecast Error Today So Far",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:chart-bell-curve-cumulative",
        value_fn=lambda d, _: d.forecast_error_today_so_far_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_accuracy_today_so_far",
        name="Forecast Accuracy Today So Far",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:target",
        value_fn=lambda d, _: d.forecast_accuracy_today_so_far_pct,
    ),
    SolarFriendSensorDescription(
        key="forecast_actual_yesterday",
        name="Forecast Actual Yesterday",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:solar-power-variant",
        value_fn=lambda d, _: d.forecast_actual_yesterday_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_predicted_yesterday",
        name="Forecast Predicted Yesterday",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:chart-line",
        value_fn=lambda d, _: d.forecast_predicted_yesterday_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_error_yesterday",
        name="Forecast Error Yesterday",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:chart-bell-curve-cumulative",
        value_fn=lambda d, _: d.forecast_error_yesterday_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_accuracy_yesterday",
        name="Forecast Accuracy Yesterday",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:target",
        value_fn=lambda d, _: d.forecast_accuracy_yesterday_pct,
    ),
    SolarFriendSensorDescription(
        key="forecast_bias_factor_14d",
        name="Forecast Bias Factor 14D",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        icon="mdi:tune-variant",
        value_fn=lambda d, _: d.forecast_bias_factor_14d,
    ),
    SolarFriendSensorDescription(
        key="forecast_mae_14d",
        name="Forecast MAE 14D",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=None,
        icon="mdi:chart-timeline-variant",
        value_fn=lambda d, _: d.forecast_mae_14d_kwh,
    ),
    SolarFriendSensorDescription(
        key="forecast_mape_14d",
        name="Forecast MAPE 14D",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:percent",
        value_fn=lambda d, _: d.forecast_mape_14d_pct,
    ),
    SolarFriendSensorDescription(
        key="forecast_accuracy_14d",
        name="Forecast Accuracy 14D",
        native_unit_of_measurement=PERCENTAGE,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:calendar-check",
        value_fn=lambda d, _: d.forecast_accuracy_14d_pct,
        extra_attrs_fn=lambda d, cfg: _forecast_accuracy_14d_attrs(d, cfg),
    ),
    SolarFriendSensorDescription(
        key="solar_installation_profile",
        name="Solar Installation Profile",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:solar-panel",
        value_fn=lambda d, _: d.solar_profile_state,
        extra_attrs_fn=lambda d, cfg: _solar_installation_profile_attrs(d, cfg),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_hours_to_ready",
        name="Solar Profile Hours To Ready",
        native_unit_of_measurement="h",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:timer-sand",
        value_fn=lambda d, _: round(d.solar_profile_estimated_hours_to_ready, 1),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_missing_annual_paths",
        name="Solar Profile Missing Annual Paths",
        native_unit_of_measurement="paths",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:vector-polyline-minus",
        value_fn=lambda d, _: d.solar_profile_annual_paths_missing,
    ),
    SolarFriendSensorDescription(
        key="solar_installation_profile_fast",
        name="Solar Installation Profile Fast",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:solar-panel-large",
        value_fn=lambda d, _: _solar_profile_variant(d, "fast").get("state", "inactive"),
        extra_attrs_fn=lambda d, cfg: _solar_installation_profile_variant_attrs(d, cfg, "fast"),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_hours_to_ready_fast",
        name="Solar Profile Hours To Ready Fast",
        native_unit_of_measurement="h",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:timer-sand",
        value_fn=lambda d, _: round(float(_solar_profile_variant(d, "fast").get("estimated_hours_to_ready", 0.0)), 1),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_missing_annual_paths_fast",
        name="Solar Profile Missing Annual Paths Fast",
        native_unit_of_measurement="paths",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:vector-polyline-minus",
        value_fn=lambda d, _: int(_solar_profile_variant(d, "fast").get("annual_paths_missing", 0)),
    ),
    SolarFriendSensorDescription(
        key="solar_installation_profile_medium",
        name="Solar Installation Profile Medium",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:solar-panel-large",
        value_fn=lambda d, _: _solar_profile_variant(d, "medium").get("state", d.solar_profile_state),
        extra_attrs_fn=lambda d, cfg: _solar_installation_profile_variant_attrs(d, cfg, "medium"),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_hours_to_ready_medium",
        name="Solar Profile Hours To Ready Medium",
        native_unit_of_measurement="h",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:timer-sand",
        value_fn=lambda d, _: round(float(_solar_profile_variant(d, "medium").get("estimated_hours_to_ready", d.solar_profile_estimated_hours_to_ready)), 1),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_missing_annual_paths_medium",
        name="Solar Profile Missing Annual Paths Medium",
        native_unit_of_measurement="paths",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:vector-polyline-minus",
        value_fn=lambda d, _: int(_solar_profile_variant(d, "medium").get("annual_paths_missing", d.solar_profile_annual_paths_missing)),
    ),
    SolarFriendSensorDescription(
        key="solar_installation_profile_fine",
        name="Solar Installation Profile Fine",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:solar-panel-large",
        value_fn=lambda d, _: _solar_profile_variant(d, "fine").get("state", "inactive"),
        extra_attrs_fn=lambda d, cfg: _solar_installation_profile_variant_attrs(d, cfg, "fine"),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_hours_to_ready_fine",
        name="Solar Profile Hours To Ready Fine",
        native_unit_of_measurement="h",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:timer-sand",
        value_fn=lambda d, _: round(float(_solar_profile_variant(d, "fine").get("estimated_hours_to_ready", 0.0)), 1),
    ),
    SolarFriendSensorDescription(
        key="solar_profile_missing_annual_paths_fine",
        name="Solar Profile Missing Annual Paths Fine",
        native_unit_of_measurement="paths",
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:vector-polyline-minus",
        value_fn=lambda d, _: int(_solar_profile_variant(d, "fine").get("annual_paths_missing", 0)),
    ),
    SolarFriendSensorDescription(
        key="forecast_correction_model",
        name="Forecast Correction Model",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:tune-variant",
        value_fn=lambda d, _: d.forecast_correction_model_state,
        extra_attrs_fn=lambda d, cfg: _forecast_correction_model_attrs(d, cfg),
    ),
    SolarFriendSensorDescription(
        key="forecast_correction_total_factor",
        name="Forecast Correction Total Factor",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:multiplication",
        value_fn=lambda d, _: round(d.forecast_correction_current_total_factor, 4),
    ),
    SolarFriendSensorDescription(
        key="forecast_correction_geometry_factor",
        name="Forecast Correction Geometry Factor",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:solar-power-variant",
        value_fn=lambda d, _: round(d.forecast_correction_current_geometry_factor, 4),
    ),
    SolarFriendSensorDescription(
        key="forecast_correction_temperature_factor",
        name="Forecast Correction Temperature Factor",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:thermometer",
        value_fn=lambda d, _: round(d.forecast_correction_current_temperature_factor, 4),
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
    SolarFriendSensorDescription(
        key="battery_plan",
        name="Battery Plan",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-clock-outline",
        value_fn=lambda d, _: (
            d.battery_plan[-1]["soc"]
            if d.battery_plan
            else None
        ),
        extra_attrs_fn=lambda d, cfg: _battery_plan_attrs(d, cfg),
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
            "weekday_populated_hours": d.consumption_profile_debug.get("weekday", {}).get("populated_hours", 0),
            "weekend_populated_hours": d.consumption_profile_debug.get("weekend", {}).get("populated_hours", 0),
            "weekday_median_samples": d.consumption_profile_debug.get("weekday", {}).get("median_samples", 0.0),
            "weekend_median_samples": d.consumption_profile_debug.get("weekend", {}).get("median_samples", 0.0),
            "weekday_days_estimate": d.consumption_profile_debug.get("weekday", {}).get("days_estimate", 0),
            "weekend_days_estimate": d.consumption_profile_debug.get("weekend", {}).get("days_estimate", 0),
            "weekday_samples_per_hour": d.consumption_profile_debug.get("weekday", {}).get("samples_per_hour", {}),
            "weekend_samples_per_hour": d.consumption_profile_debug.get("weekend", {}).get("samples_per_hour", {}),
        },
    ),
    # --- Advanced consumption model ---
    SolarFriendSensorDescription(
        key="advanced_consumption_model_state",
        name="Advanced Consumption Model State",
        native_unit_of_measurement=None,
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:chart-timeline-variant",
        value_fn=lambda d, _: d.advanced_consumption_model_state,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_records",
        name="Advanced Consumption Model Records",
        native_unit_of_measurement="records",
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:database",
        value_fn=lambda d, _: d.advanced_consumption_model_records,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_tracked_days",
        name="Advanced Consumption Model Tracked Days",
        native_unit_of_measurement="days",
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:calendar-range",
        value_fn=lambda d, _: d.advanced_consumption_model_tracked_days,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_current_hour_prediction",
        name="Advanced Consumption Model Current Hour Prediction",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:chart-bell-curve-cumulative",
        value_fn=lambda d, _: d.advanced_consumption_model_current_hour_prediction_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_current_hour_actual",
        name="Advanced Consumption Model Current Hour Actual",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:gauge",
        value_fn=lambda d, _: d.advanced_consumption_model_current_hour_actual_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_last_hour_actual",
        name="Advanced Consumption Model Last Hour Actual",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:clock-check-outline",
        value_fn=lambda d, _: d.advanced_consumption_model_last_hour_actual_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_last_hour_prediction",
        name="Advanced Consumption Model Last Hour Prediction",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:clock-outline",
        value_fn=lambda d, _: d.advanced_consumption_model_last_hour_prediction_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_last_hour_error",
        name="Advanced Consumption Model Last Hour Error",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:chart-line-variant",
        value_fn=lambda d, _: d.advanced_consumption_model_last_hour_error_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_today_mae",
        name="Advanced Consumption Model Today MAE",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:target",
        value_fn=lambda d, _: d.advanced_consumption_model_today_mae_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_7d_mae",
        name="Advanced Consumption Model 7D MAE",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:calendar-week",
        value_fn=lambda d, _: d.advanced_consumption_model_7d_mae_w,
    ),
    SolarFriendSensorDescription(
        key="advanced_consumption_model_chart",
        name="Advanced Consumption Model Chart",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:chart-box-outline",
        value_fn=lambda d, _: (
            d.advanced_consumption_model_current_hour_actual_w
            if d.advanced_consumption_model_current_hour_actual_w is not None
            else d.advanced_consumption_model_current_hour_prediction_w
        ),
        extra_attrs_fn=lambda d, cfg: _advanced_consumption_chart_attrs(d, cfg),
    ),
    SolarFriendSensorDescription(
        key="model_evaluation_summary",
        name="Model Evaluation Summary",
        native_unit_of_measurement="rows",
        device_class=None,
        state_class=None,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_visible_default=False,
        icon="mdi:chart-line",
        value_fn=lambda d, _: d.model_evaluation_rows,
        extra_attrs_fn=lambda d, cfg: _model_evaluation_summary_attrs(d, cfg),
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
                lambda d: max(0.0, d.ev_surplus_w),
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
                lambda d: round(d.ev_min_soc_from_range, 1),
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
        self._attr_entity_registry_visible_default = (
            description.entity_registry_visible_default
        )
        self._unrecorded_attributes = _UNRECORDED_ATTRIBUTE_KEYS.get(
            description.key,
            frozenset(),
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
        if key == "ev_min_soc_fra_km":
            self._attr_suggested_display_precision = 1

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
            "departure": dep.strftime("%H:%M") if dep is not None else None,
            "target_soc": data.ev_target_soc,
            "current_soc": data.ev_vehicle_soc,
            "charge_mode": data.ev_charge_mode,
        }
