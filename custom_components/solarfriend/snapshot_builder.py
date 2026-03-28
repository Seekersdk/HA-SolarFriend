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
        )
        data.forecast_correction_model_state = correction_snapshot.state
        data.forecast_correction_current_month = correction_snapshot.current_month
        data.forecast_correction_active_buckets = correction_snapshot.active_buckets
        data.forecast_correction_confident_buckets = correction_snapshot.confident_buckets
        data.forecast_correction_average_factor_this_month = correction_snapshot.average_factor_this_month
        data.forecast_correction_today_hourly_factors = correction_snapshot.today_hourly_factors
        data.forecast_correction_today_contextual_factors = correction_snapshot.today_contextual_factors
        data.forecast_correction_current_hour_factor = correction_snapshot.current_hour_factor
        data.forecast_correction_current_hour_samples = correction_snapshot.current_hour_samples
        data.forecast_correction_active_context_buckets = correction_snapshot.active_context_buckets
        data.forecast_correction_confident_context_buckets = correction_snapshot.confident_context_buckets
        data.forecast_correction_current_context_factor = correction_snapshot.current_context_factor
        data.forecast_correction_current_context_samples = correction_snapshot.current_context_samples
        data.forecast_correction_current_context_key = correction_snapshot.current_context_key
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
        current_soc = data.battery_soc or 35.0
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

    def apply_profile_debug(self, *, data: Any, profile: Any) -> None:
        """Project profile confidence/debug fields into the snapshot."""
        data.profile_confidence = profile.confidence
        data.profile_days_collected = profile.days_collected
        data.consumption_profile_debug = profile.build_debug_snapshot()
