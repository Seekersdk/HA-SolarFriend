"""Coordinator snapshot/publication helpers.

AI bot guide:
- Keep UI-facing snapshot population here instead of expanding `coordinator.py`.
- This module should only transform already-computed runtime/model state into
  `SolarFriendData` fields. It should not own device service calls.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .forecast_adapter import get_forecast_for_period


def _merge_solar_profile_comparison(
    rows_by_variant: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Merge per-resolution Track-2 comparison rows into one graph-friendly series."""
    merged: dict[str, dict[str, Any]] = {}
    for variant_key, rows in rows_by_variant.items():
        for row in rows:
            period_start = row.get("period_start")
            if not period_start:
                continue
            base = merged.setdefault(
                period_start,
                {
                    "period_start": period_start,
                    "solcast_kwh": row.get("solcast_kwh"),
                    "faktisk_kwh": row.get("faktisk_kwh"),
                    "empirisk_kwh": row.get("empirisk_kwh"),
                    "elevation": row.get("elevation"),
                },
            )
            if base.get("solcast_kwh") is None:
                base["solcast_kwh"] = row.get("solcast_kwh")
            if base.get("faktisk_kwh") is None:
                base["faktisk_kwh"] = row.get("faktisk_kwh")
            if base.get("empirisk_kwh") is None:
                base["empirisk_kwh"] = row.get("empirisk_kwh")
            if base.get("elevation") is None:
                base["elevation"] = row.get("elevation")

            key_name = f"beregnet_{variant_key}_kwh"
            conf_name = f"beregnet_{variant_key}_confidence"
            base[key_name] = row.get("beregnet_kwh")
            base[conf_name] = row.get("beregnet_confidence")
            if variant_key == "medium":
                base["beregnet_kwh"] = row.get("beregnet_kwh")
                base["beregnet_confidence"] = row.get("beregnet_confidence")

    return [merged[key] for key in sorted(merged)]


class SnapshotBuilder:
    """Populate snapshot fields from trackers/models/profile state."""

    def apply_battery_tracker(self, *, data: Any, tracker: Any | None) -> None:
        """Project battery tracker state into the published snapshot."""
        if tracker is None:
            return
        data.battery_solar_kwh = tracker.solar_kwh
        data.battery_grid_kwh = tracker.grid_kwh
        data.battery_weighted_cost = tracker.weighted_cost
        data.battery_solar_fraction = tracker.solar_fraction
        data.today_solar_direct_kwh = tracker.today_solar_direct_kwh
        data.today_solar_direct_saved_dkk = tracker.today_solar_direct_saved_dkk
        data.today_optimizer_saved_dkk = tracker.today_optimizer_saved_dkk
        data.today_battery_sell_kwh = tracker.today_battery_sell_kwh
        data.today_battery_sell_saved_dkk = tracker.today_battery_sell_saved_dkk
        data.total_solar_direct_saved_dkk = tracker.live_total_solar_saved_dkk
        data.total_optimizer_saved_dkk = tracker.live_total_optimizer_saved_dkk
        data.total_battery_sell_saved_dkk = tracker.live_total_battery_sell_saved_dkk

    def apply_flex_loads(self, *, data: Any, now: datetime, manager: Any | None) -> None:
        """Project stored flex-load reservations into the published snapshot."""
        if manager is None:
            return
        snapshot = manager.build_snapshot(now)
        data.flex_load_reservations_count = snapshot.reservations_count
        data.flex_load_next_name = snapshot.next_name
        data.flex_load_next_start = snapshot.next_start
        data.flex_load_next_end = snapshot.next_end
        data.flex_load_next_power_w = snapshot.next_power_w
        data.flex_load_reserved_solar_today_kwh = snapshot.reserved_solar_today_kwh
        data.flex_load_reserved_solar_tomorrow_kwh = snapshot.reserved_solar_tomorrow_kwh
        data.flex_load_reservations = snapshot.reservations

    def apply_forecast_tracker(self, *, data: Any, now: datetime, forecast_tracker: Any | None) -> None:
        """Project forecast tracker metrics into the published snapshot."""
        if forecast_tracker is None:
            return
        metrics = forecast_tracker.build_metrics(now, data.forecast_data)
        data.forecast_actual_today_so_far_kwh = metrics.today_actual_kwh
        data.forecast_predicted_today_so_far_kwh = metrics.today_predicted_kwh
        data.forecast_error_today_so_far_kwh = metrics.today_error_kwh
        data.forecast_accuracy_today_so_far_pct = metrics.today_accuracy_pct
        data.forecast_actual_yesterday_kwh = metrics.yesterday_actual_kwh
        data.forecast_predicted_yesterday_kwh = metrics.yesterday_predicted_kwh
        data.forecast_error_yesterday_kwh = metrics.yesterday_error_kwh
        data.forecast_accuracy_yesterday_pct = metrics.yesterday_accuracy_pct
        data.forecast_bias_factor_14d = metrics.bias_factor_14d
        data.forecast_mae_14d_kwh = metrics.mae_14d_kwh
        data.forecast_mape_14d_pct = metrics.mape_14d_pct
        data.forecast_accuracy_14d_pct = metrics.accuracy_14d_pct
        data.forecast_valid_days_14d = metrics.valid_days_14d
        data.forecast_correction_valid = metrics.correction_valid
        data.forecast_history_14d = metrics.history_14d

    def apply_forecast_correction(
        self,
        *,
        data: Any,
        now: datetime,
        correction_model: Any | None,
        weather_snapshot: dict[str, Any],
        solar_elevation: float | None,
        solar_azimuth: float | None,
        latitude: float | None = None,
        longitude: float | None = None,
        hourly_weather_forecast: list[dict[str, Any]] | None = None,
    ) -> None:
        """Project passive forecast-correction diagnostics into the snapshot."""
        if correction_model is None:
            return
        correction_snapshot = correction_model.build_snapshot(
            now=now,
            hourly_forecast=data.forecast_data.hourly_forecast if data.forecast_data else [],
            current_environment={
                **weather_snapshot,
                "month": now.month,
                "solar_elevation": solar_elevation,
                "solar_azimuth": solar_azimuth,
            },
            latitude=latitude,
            longitude=longitude,
            hourly_weather_forecast=hourly_weather_forecast,
        )
        data.forecast_correction_model_state = correction_snapshot.state
        data.forecast_correction_current_season = correction_snapshot.current_season
        data.forecast_correction_active_buckets = correction_snapshot.active_buckets
        data.forecast_correction_confident_buckets = correction_snapshot.confident_buckets
        data.forecast_correction_average_factor_this_season = (
            correction_snapshot.average_factor_this_season
        )
        data.forecast_correction_today_geometry_factors = correction_snapshot.today_geometry_factors
        data.forecast_correction_current_total_factor = correction_snapshot.current_total_factor
        data.forecast_correction_current_geometry_factor = correction_snapshot.current_geometry_factor
        data.forecast_correction_current_geometry_samples = correction_snapshot.current_geometry_samples
        data.forecast_correction_current_geometry_key = correction_snapshot.current_geometry_key
        data.forecast_correction_current_temperature_factor = (
            correction_snapshot.current_temperature_factor
        )
        data.forecast_correction_current_temperature_samples = (
            correction_snapshot.current_temperature_samples
        )
        data.forecast_correction_current_temperature_key = correction_snapshot.current_temperature_key
        data.forecast_correction_raw_vs_corrected_delta_today = (
            correction_snapshot.raw_vs_corrected_delta_today
        )
        data.forecast_correction_last_environment = correction_snapshot.last_environment

    def apply_consumption_profile_chart(self, *, data: Any, now: datetime, profile: Any) -> None:
        """Build the 24h consumption profile chart and metadata."""
        is_weekend = now.weekday() >= 5
        profile_key = "weekend" if is_weekend else "weekday"
        hourly: list[float] = []
        for hour in range(24):
            if hasattr(profile, "get_predicted_watt"):
                hourly.append(round(float(profile.get_predicted_watt(hour, is_weekend)), 1))
                continue
            slot = profile._profiles[profile_key][hour]
            avg = round(slot["avg_watt"], 1) if slot["samples"] >= 3 else 0.0
            hourly.append(avg)
        data.consumption_profile_chart = hourly
        data.consumption_profile_day_type = profile_key

    def apply_forecast_soc_chart(self, *, data: Any, now: datetime, capacity_kwh: float, min_soc: float) -> None:
        """Build the simple forward SOC curve from hourly forecast vs profile load."""
        current_soc = 35.0 if data.battery_soc is None else data.battery_soc
        max_soc = 100.0
        current_hour = now.hour

        solcast_hourly: dict[int, float] = {}
        if data.forecast_data and data.forecast_data.hourly_forecast:
            for slot in data.forecast_data.hourly_forecast:
                hour = slot["period_start"].hour
                solcast_hourly[hour] = solcast_hourly.get(hour, 0.0) + slot.get("pv_estimate_kwh", 0.0) * 1000

        forecast_soc: list[Any] = []
        soc = current_soc
        for hour in range(24):
            if hour < current_hour:
                forecast_soc.append(None)
                continue
            pv_w = solcast_hourly.get(hour, 0.0)
            pv_kwh = pv_w / 1000.0
            load_w = data.consumption_profile_chart[hour] if data.consumption_profile_chart else 850.0
            load_kwh = load_w / 1000.0
            net_kwh = pv_kwh - load_kwh
            delta_soc = (net_kwh / capacity_kwh) * 100.0
            soc = max(min_soc, min(max_soc, soc + delta_soc))
            forecast_soc.append(round(soc, 1))
        data.forecast_soc_chart = forecast_soc

    def apply_solar_lookahead(self, *, data: Any, now: datetime, sunset_dt: datetime | None) -> None:
        """Populate next-2h and until-sunset forecast-derived solar summaries."""
        if data.forecast_data is None or not data.forecast_data.hourly_forecast:
            return
        data.solar_next_2h = get_forecast_for_period(
            data.forecast_data.hourly_forecast, now, now + timedelta(hours=2)
        )
        if sunset_dt is not None:
            data.solar_until_sunset = get_forecast_for_period(
                data.forecast_data.hourly_forecast, now, sunset_dt
            )

    def apply_advanced_consumption(
        self,
        *,
        data: Any,
        now: datetime,
        model: Any,
        enabled: bool,
        load_learning_allowed: bool,
        weather_snapshot: dict[str, Any],
    ) -> None:
        """Update and publish the passive advanced consumption model."""
        if enabled and load_learning_allowed:
            model.update(
                now=now,
                load_w=data.load_power,
                weather_snapshot=weather_snapshot,
            )
        snapshot = model.build_snapshot(now=now, enabled=enabled)
        data.advanced_consumption_model_enabled = enabled
        data.advanced_consumption_model_state = snapshot.state
        data.advanced_consumption_model_records = snapshot.records_count
        data.advanced_consumption_model_tracked_days = snapshot.tracked_days
        data.advanced_consumption_model_current_hour_prediction_w = snapshot.current_hour_prediction_w
        data.advanced_consumption_model_current_hour_actual_w = snapshot.current_hour_partial_actual_w
        data.advanced_consumption_model_last_hour_actual_w = snapshot.last_hour_actual_w
        data.advanced_consumption_model_last_hour_prediction_w = snapshot.last_hour_prediction_w
        data.advanced_consumption_model_last_hour_error_w = snapshot.last_hour_error_w
        data.advanced_consumption_model_today_mae_w = snapshot.today_mae_w
        data.advanced_consumption_model_7d_mae_w = snapshot.rolling_7d_mae_w
        data.advanced_consumption_model_today_hourly_actual = snapshot.today_hourly_actual
        data.advanced_consumption_model_today_hourly_prediction = snapshot.today_hourly_prediction
        data.advanced_consumption_model_recent_daily_totals = snapshot.recent_daily_totals
        data.advanced_consumption_model_last_weather = snapshot.last_weather_snapshot

    def apply_solar_installation_profiles(
        self,
        *,
        data: Any,
        now: datetime,
        profiles: dict[str, Any] | None,
        latitude: float,
        longitude: float,
        raw_hourly_forecast: list[dict],
        empirical_hourly_forecast: list[dict] | None = None,
    ) -> None:
        """Project one or more Track-2 profiles into the snapshot."""
        if not profiles:
            return
        snapshots: dict[str, Any] = {}
        for key, profile in profiles.items():
            if profile is None:
                continue
            snapshots[key] = profile.build_snapshot(
                now=now,
                latitude=latitude,
                longitude=longitude,
                raw_hourly_forecast=raw_hourly_forecast,
                empirical_hourly_forecast=empirical_hourly_forecast or [],
            )
        if not snapshots:
            return

        medium_snapshot = snapshots.get("medium") or next(iter(snapshots.values()))
        data.solar_profile_state = medium_snapshot.state
        data.solar_profile_populated_cells = medium_snapshot.populated_cells
        data.solar_profile_confident_cells = medium_snapshot.confident_cells
        data.solar_profile_astronomical_coverage_pct = medium_snapshot.astronomical_coverage_pct
        data.solar_profile_annual_paths_total = medium_snapshot.annual_paths_total
        data.solar_profile_annual_paths_covered = medium_snapshot.annual_paths_covered
        data.solar_profile_annual_paths_missing = medium_snapshot.annual_paths_missing
        data.solar_profile_clear_sky_observations = medium_snapshot.clear_sky_observations
        data.solar_profile_estimated_hours_to_ready = medium_snapshot.estimated_hours_to_ready
        data.solar_profile_response_surface = medium_snapshot.response_surface
        data.solar_profile_variants = {
            key: {
                "resolution_key": snapshot.resolution_key,
                "resolution_label": snapshot.resolution_label,
                "state": snapshot.state,
                "populated_cells": snapshot.populated_cells,
                "confident_cells": snapshot.confident_cells,
                "astronomical_coverage_pct": snapshot.astronomical_coverage_pct,
                "annual_paths_total": snapshot.annual_paths_total,
                "annual_paths_covered": snapshot.annual_paths_covered,
                "annual_paths_missing": snapshot.annual_paths_missing,
                "clear_sky_observations": snapshot.clear_sky_observations,
                "estimated_hours_to_ready": snapshot.estimated_hours_to_ready,
                "response_surface": snapshot.response_surface,
            }
            for key, snapshot in snapshots.items()
        }
        data.solar_profile_comparison_today = _merge_solar_profile_comparison(
            {key: snapshot.comparison_today for key, snapshot in snapshots.items()}
        )
        data.solar_profile_comparison_tomorrow = _merge_solar_profile_comparison(
            {key: snapshot.comparison_tomorrow for key, snapshot in snapshots.items()}
        )

    def apply_profile_debug(self, *, data: Any, profile: Any) -> None:
        """Project profile confidence/debug fields into the snapshot."""
        data.profile_confidence = profile.confidence
        data.profile_days_collected = profile.days_collected
        data.consumption_profile_debug = profile.build_debug_snapshot()

    def apply_model_evaluation_summary(self, *, data: Any, summary: Any | None) -> None:
        """Project compact model-evaluation summary fields into the snapshot."""
        if summary is None:
            return
        data.model_evaluation_period_month = summary.period_month
        data.model_evaluation_rows = summary.rows
        data.model_evaluation_best_model = summary.best_model
        data.model_evaluation_mae_by_model = dict(summary.mae_by_model)
        data.model_evaluation_mape_by_model = dict(summary.mape_by_model)
        data.model_evaluation_bias_by_model = dict(summary.bias_by_model)
