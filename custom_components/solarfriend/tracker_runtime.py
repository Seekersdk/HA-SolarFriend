"""Battery and forecast tracker runtime helpers."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .coordinator_policy import CoordinatorPolicy

_LOGGER = logging.getLogger(__name__)


@dataclass
class TrackerRuntimeState:
    """Mutable tracker-side timing and warning state."""

    battery_prev_update_time: datetime | None = None
    forecast_prev_update_time: datetime | None = None
    last_tracker_save: datetime | None = None
    last_forecast_tracker_save: datetime | None = None
    last_soc_correction: datetime | None = None
    battery_sign_warned: bool = False
    last_plan_deviation_key: str | None = None
    unauthorized_battery_grid_charge_started_at: datetime | None = None
    unauthorized_battery_grid_charge_reported: bool = False
    ev_battery_grid_conflict_started_at: datetime | None = None
    ev_battery_grid_conflict_reported: bool = False


class TrackerRuntime:
    """Own tracker timing, battery sign warning and plan-deviation heuristics."""

    def __init__(self, policy: CoordinatorPolicy, *, config_entry: Any) -> None:
        self._policy = policy
        self._config_entry = config_entry
        self._state = TrackerRuntimeState()

    @property
    def state(self) -> TrackerRuntimeState:
        """Expose runtime state for debugging/tests if needed."""
        return self._state

    async def update_battery_tracker(
        self,
        *,
        tracker: Any,
        now: datetime,
        pv_power: float,
        battery_power: float,
        load_power: float,
        battery_soc: float,
        current_price: float,
        sell_price: float,
        previous_soc: float | None,
        load_is_trustworthy: bool = True,
        active_strategy: str | None = None,
    ) -> None:
        """Feed BatteryTracker with this tick's charge/discharge delta."""
        if self._state.battery_prev_update_time is None:
            dt_hours = 0.0
        else:
            dt_hours = (now - self._state.battery_prev_update_time).total_seconds() / 3600

        if dt_hours > 0:
            surplus_w = pv_power - load_power
            if battery_power < -self._policy.battery_noise_w:
                charge_kwh = abs(battery_power) / 1000 * dt_hours
                if surplus_w > self._policy.battery_noise_w:
                    tracker.on_solar_charge(charge_kwh)
                else:
                    tracker.on_grid_charge(charge_kwh, current_price)
            elif battery_power > self._policy.battery_noise_w:
                discharge_kwh = battery_power / 1000 * dt_hours
                tracker.on_discharge(discharge_kwh)

            # Deye hides/distorts house-load telemetry while exporting battery.
            # Skip load-based savings accounting in those periods so we do not
            # teach trackers from synthetic "grid side" measurements.
            if load_is_trustworthy:
                savings_changed = tracker.update_savings(
                    pv_w=pv_power,
                    load_w=load_power,
                    battery_w=battery_power,
                    price_dkk=current_price,
                    dt_seconds=dt_hours * 3600,
                )
            else:
                savings_changed = False
        else:
            savings_changed = False

        if (
            self._state.last_soc_correction is None
            or (now - self._state.last_soc_correction) >= timedelta(minutes=5)
        ):
            cfg = self._config_entry.data
            tracker.on_soc_correction(
                actual_soc=battery_soc,
                capacity_kwh=float(cfg.get("battery_capacity_kwh", 0.0)),
                min_soc=float(cfg.get("battery_min_soc", 0.0)),
            )
            self._state.last_soc_correction = now

        if (
            not self._state.battery_sign_warned
            and previous_soc is not None
            and previous_soc > 0
            and battery_soc - previous_soc > 2.0
            and battery_power > self._policy.battery_noise_w
        ):
            _LOGGER.warning(
                "battery_power ser ud til at have OMVENDT FORTEGN! "
                "SOC steg %.1f%% → %.1f%% mens battery_power=%.0fW (positiv). "
                "Forventet konvention: negativ = lader, positiv = aflader. "
                "Tjek din battery_power_sensor konfiguration.",
                previous_soc,
                battery_soc,
                battery_power,
            )
            self._state.battery_sign_warned = True

        if active_strategy == "SELL_BATTERY" and dt_hours > 0:
            sell_changed = tracker.update_battery_sell_savings(
                battery_w=max(0.0, battery_power),
                sell_price_dkk=sell_price,
                dt_seconds=dt_hours * 3600,
            )
            savings_changed = savings_changed or sell_changed

        save_interval = timedelta(minutes=1) if savings_changed else timedelta(minutes=15)
        if (
            self._state.last_tracker_save is None
            or (now - self._state.last_tracker_save) >= save_interval
        ):
            await tracker.async_save()
            self._state.last_tracker_save = now

        self._state.battery_prev_update_time = now

    async def update_forecast_tracker(
        self,
        *,
        forecast_tracker: Any,
        now: datetime,
        pv_power: float,
        forecast_total_today_kwh: float | None,
    ) -> None:
        """Track actual PV generation and forecast quality over time."""
        if self._state.forecast_prev_update_time is None:
            dt_seconds = 0.0
        else:
            dt_seconds = (now - self._state.forecast_prev_update_time).total_seconds()

        forecast_tracker.update(
            now=now,
            pv_power_w=pv_power,
            dt_seconds=dt_seconds,
            forecast_total_today_kwh=forecast_total_today_kwh,
        )

        # Persist more aggressively while PV is active so reloads do not rewind
        # the "actual today so far" counter by up to 15 minutes.
        save_interval = (
            timedelta(minutes=1)
            if pv_power > self._policy.battery_noise_w
            else timedelta(minutes=15)
        )
        if (
            self._state.last_forecast_tracker_save is None
            or (now - self._state.last_forecast_tracker_save) >= save_interval
        ):
            await forecast_tracker.async_save()
            self._state.last_forecast_tracker_save = now
        self._state.forecast_prev_update_time = now

    def should_trigger_plan_deviation_replan(
        self,
        *,
        optimizer: Any,
        now: datetime,
        battery_power: float,
        normalize_local_datetime: Any,
    ) -> bool:
        """Detect a clear mismatch between planned and actual battery behavior."""
        plan = optimizer.get_last_plan()
        if not plan:
            self._state.last_plan_deviation_key = None
            return False

        current_hour = now.replace(minute=0, second=0, microsecond=0)
        current_slot = next(
            (
                slot
                for slot in plan
                if normalize_local_datetime(datetime.fromisoformat(slot["hour"])).replace(
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                == current_hour
            ),
            None,
        )
        if current_slot is None:
            self._state.last_plan_deviation_key = None
            return False

        planned_discharge_w = float(current_slot.get("discharge_w", 0.0))
        planned_charge_w = float(current_slot.get("grid_charge_w", 0.0)) + float(
            current_slot.get("solar_charge_w", 0.0)
        )
        actual_discharge_w = max(0.0, battery_power)
        actual_charge_w = max(0.0, -battery_power)

        deviation_kind: str | None = None
        if (
            planned_discharge_w >= self._policy.plan_deviation_min_w
            and actual_discharge_w
            < max(
                self._policy.plan_deviation_min_w,
                planned_discharge_w * self._policy.plan_deviation_fraction,
            )
        ):
            deviation_kind = "missed_discharge"
        elif (
            planned_charge_w >= self._policy.plan_deviation_min_w
            and actual_charge_w
            < max(
                self._policy.plan_deviation_min_w,
                planned_charge_w * self._policy.plan_deviation_fraction,
            )
        ):
            deviation_kind = "missed_charge"

        if deviation_kind is None:
            self._state.last_plan_deviation_key = None
            return False

        deviation_key = f"{current_slot['hour']}|{deviation_kind}"
        if deviation_key == self._state.last_plan_deviation_key:
            return False

        self._state.last_plan_deviation_key = deviation_key
        _LOGGER.info(
            "Battery plan deviation detected: %s planned=%.0fW actual_battery=%.0fW slot=%s",
            deviation_kind,
            planned_discharge_w if deviation_kind == "missed_discharge" else planned_charge_w,
            battery_power,
            current_slot.get("hour_str", current_slot.get("hour", "?")),
        )
        return True

    def detect_unexpected_grid_events(
        self,
        *,
        now: datetime,
        strategy: str | None,
        grid_power: float,
        battery_power: float,
        pv_power: float,
        load_power: float,
        ev_charge_mode: str,
        ev_charging_power: float,
    ) -> list[str]:
        """Return newly-triggered unexpected grid/battery events."""
        events: list[str] = []
        authorized_grid_strategies = {"CHARGE_GRID", "CHARGE_NIGHT", "NEGATIVE_IMPORT"}
        strategy_name = (strategy or "").upper()
        grid_importing = grid_power >= self._policy.unexpected_grid_import_w
        battery_charging = battery_power <= -self._policy.unexpected_battery_charge_w
        unauthorized_battery_grid_charge = (
            grid_importing
            and battery_charging
            and strategy_name not in authorized_grid_strategies
        )
        if self._advance_timer(
            active=unauthorized_battery_grid_charge,
            now=now,
            started_attr="unauthorized_battery_grid_charge_started_at",
            reported_attr="unauthorized_battery_grid_charge_reported",
            duration=self._policy.unexpected_battery_grid_charge_duration,
        ):
            events.append("unauthorized_battery_grid_charge")

        total_demand_w = load_power + ev_charging_power + abs(min(0.0, battery_power))
        solar_shortfall = pv_power + self._policy.unexpected_grid_solar_margin_w < total_demand_w
        ev_battery_grid_conflict = (
            ev_charge_mode == "solar_only"
            and ev_charging_power >= self._policy.ev_active_charge_w
            and battery_charging
            and grid_importing
            and strategy_name not in authorized_grid_strategies
            and solar_shortfall
        )
        if self._advance_timer(
            active=ev_battery_grid_conflict,
            now=now,
            started_attr="ev_battery_grid_conflict_started_at",
            reported_attr="ev_battery_grid_conflict_reported",
            duration=self._policy.unexpected_ev_grid_conflict_duration,
        ):
            events.append("ev_battery_grid_conflict")

        return events

    def _advance_timer(
        self,
        *,
        active: bool,
        now: datetime,
        started_attr: str,
        reported_attr: str,
        duration: timedelta,
    ) -> bool:
        """Track a timed condition and return True on the first threshold crossing."""
        if not active:
            setattr(self._state, started_attr, None)
            setattr(self._state, reported_attr, False)
            return False

        started_at = getattr(self._state, started_attr)
        if started_at is None:
            setattr(self._state, started_attr, now)
            setattr(self._state, reported_attr, False)
            return False

        if getattr(self._state, reported_attr):
            return False

        if (now - started_at) < duration:
            return False

        setattr(self._state, reported_attr, True)
        return True
