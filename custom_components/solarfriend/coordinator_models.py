"""Shared coordinator-facing models.

AI bot guide:
- `SolarFriendData` is the UI-facing snapshot published by the coordinator.
- `ev_device_info()` is shared by EV entities to keep their device registry stable.
- Keep this module free of runtime side effects so sensors/entities can import it safely.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from homeassistant.helpers.device_registry import DeviceInfo

from .const import DOMAIN


@dataclass
class SolarFriendData:
    """Coordinator snapshot consumed by entities and runtime helpers."""

    pv_power: float = 0.0
    grid_power: float = 0.0
    battery_soc: float = 0.0
    battery_power: float = 0.0
    load_power: float = 0.0
    price: float = 0.0
    sell_price: float = 0.0
    forecast: float = 0.0
    price_data: Any | None = None
    sell_price_data: Any | None = None
    solar_surplus: float = 0.0
    battery_strategy: str = "IDLE"
    price_level: str = "NORMAL"
    battery_cost_per_kwh: float = 0.0
    charge_threshold: float | None = None
    profile_confidence: str = "LEARNING"
    profile_days_collected: int = 0
    consumption_profile_debug: dict[str, Any] = field(default_factory=dict)
    optimize_result: Any | None = None
    plan_optimize_result: Any | None = None
    forecast_data: Any | None = None
    battery_solar_kwh: float = 0.0
    battery_grid_kwh: float = 0.0
    battery_weighted_cost: float = 0.0
    battery_solar_fraction: float = 0.0
    today_solar_direct_kwh: float = 0.0
    today_solar_direct_saved_dkk: float = 0.0
    today_optimizer_saved_dkk: float = 0.0
    today_battery_sell_kwh: float = 0.0
    today_battery_sell_saved_dkk: float = 0.0
    total_solar_direct_saved_dkk: float = 0.0
    total_optimizer_saved_dkk: float = 0.0
    total_battery_sell_saved_dkk: float = 0.0
    flex_load_reservations_count: int = 0
    flex_load_next_name: str = ""
    flex_load_next_start: str = ""
    flex_load_next_end: str = ""
    flex_load_next_power_w: float = 0.0
    flex_load_reserved_solar_today_kwh: float = 0.0
    flex_load_reserved_solar_tomorrow_kwh: float = 0.0
    flex_load_reservations: list[dict[str, Any]] = field(default_factory=list)
    solar_next_2h: float = 0.0
    solar_until_sunset: float = 0.0
    consumption_profile_chart: list[float] = field(default_factory=list)
    consumption_profile_day_type: str = "weekday"
    advanced_consumption_model_state: str = "disabled"
    advanced_consumption_model_enabled: bool = False
    advanced_consumption_model_records: int = 0
    advanced_consumption_model_tracked_days: int = 0
    advanced_consumption_model_current_hour_prediction_w: float | None = None
    advanced_consumption_model_current_hour_actual_w: float | None = None
    advanced_consumption_model_last_hour_actual_w: float | None = None
    advanced_consumption_model_last_hour_prediction_w: float | None = None
    advanced_consumption_model_last_hour_error_w: float | None = None
    advanced_consumption_model_today_mae_w: float | None = None
    advanced_consumption_model_7d_mae_w: float | None = None
    advanced_consumption_model_today_hourly_actual: list[Any] = field(default_factory=list)
    advanced_consumption_model_today_hourly_prediction: list[Any] = field(default_factory=list)
    advanced_consumption_model_recent_daily_totals: list[dict[str, Any]] = field(default_factory=list)
    advanced_consumption_model_last_weather: dict[str, Any] = field(default_factory=dict)
    forecast_soc_chart: list[Any] = field(default_factory=list)
    battery_plan: list[dict[str, Any]] = field(default_factory=list)
    forecast_actual_today_so_far_kwh: float = 0.0
    forecast_predicted_today_so_far_kwh: float = 0.0
    forecast_error_today_so_far_kwh: float = 0.0
    forecast_accuracy_today_so_far_pct: float = 0.0
    forecast_actual_yesterday_kwh: float = 0.0
    forecast_predicted_yesterday_kwh: float = 0.0
    forecast_error_yesterday_kwh: float = 0.0
    forecast_accuracy_yesterday_pct: float = 0.0
    forecast_bias_factor_14d: float = 1.0
    forecast_mae_14d_kwh: float = 0.0
    forecast_mape_14d_pct: float = 0.0
    forecast_accuracy_14d_pct: float = 0.0
    forecast_valid_days_14d: int = 0
    forecast_correction_valid: bool = False
    forecast_history_14d: list[dict[str, Any]] = field(default_factory=list)
    forecast_correction_model_state: str = "inactive"
    forecast_correction_current_season: int = 0
    forecast_correction_active_buckets: int = 0
    forecast_correction_confident_buckets: int = 0
    forecast_correction_average_factor_this_season: float = 1.0
    forecast_correction_today_geometry_factors: dict[str, Any] = field(default_factory=dict)
    forecast_correction_current_total_factor: float = 1.0
    forecast_correction_current_geometry_factor: float = 1.0
    forecast_correction_current_geometry_samples: int = 0
    forecast_correction_current_geometry_key: str = ""
    forecast_correction_current_temperature_factor: float = 1.0
    forecast_correction_current_temperature_samples: int = 0
    forecast_correction_current_temperature_key: str = ""
    forecast_correction_raw_vs_corrected_delta_today: float = 0.0
    forecast_correction_last_environment: dict[str, Any] = field(default_factory=dict)
    unavailable: list[str] = field(default_factory=list)
    ev_charging_enabled: bool = False
    ev_charging_power: float = 0.0
    ev_vehicle_soc: float = 0.0
    ev_target_soc: float = 80.0
    ev_surplus_w: float = 0.0
    ev_strategy_reason: str = ""
    ev_charger_status: str = "disconnected"
    ev_target_w: float = 0.0
    ev_phases: int = 0
    ev_vehicle_soc_kwh: float = 0.0
    ev_needed_kwh: float = 0.0
    ev_hours_to_departure: float = 0.0
    ev_charge_mode: str = ""
    ev_min_range_km: float = 0.0
    ev_emergency_charging: bool = False
    ev_min_soc_from_range: float = 0.0
    ev_plan: list[Any] = field(default_factory=list)

    # Solar installation profile — Track 2 (response surface + annual projection)
    solar_profile_state: str = "inactive"
    solar_profile_populated_cells: int = 0
    solar_profile_confident_cells: int = 0
    solar_profile_astronomical_coverage_pct: float = 0.0
    solar_profile_annual_paths_total: int = 0
    solar_profile_annual_paths_covered: int = 0
    solar_profile_annual_paths_missing: int = 0
    solar_profile_clear_sky_observations: int = 0
    solar_profile_estimated_hours_to_ready: float = 0.0
    solar_profile_response_surface: dict[str, float] = field(default_factory=dict)
    solar_profile_comparison_today: list[dict[str, Any]] = field(default_factory=list)
    solar_profile_comparison_tomorrow: list[dict[str, Any]] = field(default_factory=list)


def ev_device_info(coordinator: Any) -> DeviceInfo:
    """Shared EV DeviceInfo used by all EV entities."""
    entry = coordinator._entry
    charger_type_name = {"easee": "Easee", "manual": "Manuel ladeboks"}.get(
        entry.data.get("ev_charger_type", "manual"),
        entry.data.get("ev_charger_type", "manual"),
    )
    vehicle_type_name = {
        "kia_hyundai": "Kia / Hyundai",
        "manual": "Manuel bil",
        "none": "Ingen bil",
    }.get(entry.data.get("vehicle_type", "none"), entry.data.get("vehicle_type", "none"))
    return DeviceInfo(
        identifiers={(DOMAIN, f"{entry.entry_id}_ev")},
        name="SolarFriend EV",
        manufacturer=charger_type_name,
        model=vehicle_type_name,
        via_device=(DOMAIN, entry.entry_id),
    )
