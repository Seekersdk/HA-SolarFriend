"""Coordinator-facing EV orchestration helper.

AI bot guide:
- Keep charger/session orchestration here instead of growing `coordinator.py`.
- This module owns EV context assembly, Solar Only profile lookup, runtime gating,
  optional inverter battery-hold wiring, and EV plan publication.
- It intentionally mutates the shared `SolarFriendData` snapshot passed in.
"""
from __future__ import annotations

import logging
import math
from dataclasses import replace
from datetime import datetime
from typing import Any

from homeassistant.util import dt as ha_dt

from .coordinator_policy import CoordinatorPolicy
from .ev_optimizer import EVContext

_LOGGER = logging.getLogger(__name__)


class EVRuntimeService:
    """Run EV optimizer/runtime flow outside the coordinator."""

    def __init__(self, *, policy: CoordinatorPolicy, weather_service: Any) -> None:
        self._policy = policy
        self._weather_service = weather_service

    async def get_current_solar_only_profile(self, now: datetime):
        """Return the active Solar Only weather profile for the current hour."""
        return await self._weather_service.async_get_current_profile(now)

    def requires_battery_hold(
        self,
        *,
        data: Any,
        ev_result: Any,
        ev_charge_mode: str,
    ) -> bool:
        """Return True when EV charging should hold battery SOC via TOU."""
        if not ev_result.should_charge:
            return False
        if ev_charge_mode not in {"hybrid", "grid_schedule"}:
            return False

        surplus_w = max(0.0, float(ev_result.surplus_w))
        needs_grid_support = (
            float(ev_result.target_w) > surplus_w + self._policy.ev_grid_priority_margin_w
        )
        battery_to_ev = (
            data.battery_power > data.load_power + self._policy.ev_battery_protection_margin_w
        )
        return needs_grid_support or battery_to_ev

    def compute_ev_plan(
        self,
        *,
        data: Any,
        ev_planning: Any,
        ev_runtime: Any,
        ev_charge_mode: str,
        ev_min_range_km: float,
        now: datetime,
        departure: datetime,
    ) -> list[dict[str, Any]]:
        """Build EV plan from the active slot-based EV optimizer."""
        return ev_planning.compute_ev_plan(
            data=data,
            ev_charge_mode=ev_charge_mode,
            ev_currently_charging=ev_runtime.currently_charging if ev_runtime else False,
            ev_min_range_km=ev_min_range_km,
            now=now,
            departure=departure,
        )

    async def update(
        self,
        *,
        data: Any,
        entry: Any,
        ev_runtime: Any,
        ev_optimizer: Any,
        ev_charger: Any,
        vehicle: Any,
        ev_planning: Any,
        inverter: Any,
        optimize_result: Any,
        vehicle_battery_kwh: float,
        ev_charge_mode: str,
        ev_target_soc_override: float | None,
        ev_charging_allowed: bool,
        ev_min_range_km: float,
        ev_solar_only_grid_buffer_enabled: bool,
        ev_active_solar_slot: bool,
        ev_next_departure: datetime,
        get_raw_prices: Any,
        forecast_kwh_between: Any,
    ) -> bool:
        """Run EV optimizer and act on the result. Returns actual charging state."""
        charger_status = await ev_charger.get_status()
        charger_power = await ev_charger.get_power_w()
        vehicle_soc = vehicle.get_soc()
        vehicle_target_soc = (
            ev_target_soc_override
            if ev_target_soc_override is not None
            else vehicle.get_target_soc()
        )

        ev_runtime.sync_startup(charger_status)

        if not ev_charging_allowed:
            charger_status = "disconnected"
            _LOGGER.debug("EV: ladning deaktiveret af manuel switch")

        vehicle_efficiency = float(
            entry.data.get("vehicle_efficiency_km_per_kwh", 6.0)
        )
        driving_range_km = vehicle.get_driving_range()
        min_range_km = ev_min_range_km

        now = ha_dt.now()
        departure = ev_next_departure
        solar_to_departure = forecast_kwh_between(now, departure)
        _LOGGER.debug(
            "EV solar forecast til afgang %s: %.2f kWh",
            departure.strftime("%H:%M"),
            solar_to_departure,
        )
        solar_only_profile = await self.get_current_solar_only_profile(now)
        if not ev_solar_only_grid_buffer_enabled:
            solar_only_profile = replace(solar_only_profile, grid_buffer_w=0.0)

        now_hour = now.replace(minute=0, second=0, microsecond=0)
        expected_soc = data.ev_vehicle_soc or 0.0
        for slot in data.ev_plan:
            try:
                slot_dt = datetime.fromisoformat(slot["hour"])
                if abs((slot_dt - now_hour).total_seconds()) < 3600:
                    expected_soc = slot["soc"]
                    break
            except (KeyError, ValueError, TypeError):
                pass

        hybrid_slots = ev_planning.build_ev_hybrid_slots(
            data=data,
            now=now,
            departure=departure,
        )
        actual_charging = ev_runtime.set_currently_charging_from_actual(
            charger_status=charger_status,
            charger_power=charger_power,
        )
        ctx = EVContext(
            pv_power_w=data.pv_power,
            load_power_w=data.load_power,
            grid_power_w=data.grid_power,
            battery_charging_w=data.battery_power,
            battery_soc=data.battery_soc,
            battery_capacity_kwh=float(
                entry.data.get("battery_capacity_kwh", 10.0)
            ),
            battery_min_soc=float(
                entry.data.get("battery_min_soc", 10.0)
            ),
            charger_status=charger_status,
            currently_charging=actual_charging,
            vehicle_soc=vehicle_soc,
            vehicle_capacity_kwh=vehicle_battery_kwh,
            vehicle_target_soc=vehicle_target_soc,
            departure=departure,
            current_price=data.price,
            raw_prices=get_raw_prices(),
            max_charge_kw=float(entry.data.get("ev_max_charge_kw", 7.4)),
            driving_range_km=driving_range_km,
            min_range_km=min_range_km,
            vehicle_efficiency_km_per_kwh=vehicle_efficiency,
            now=now,
            solar_forecast_to_departure_kwh=solar_to_departure,
            ev_plan_expected_soc_now=expected_soc,
            current_price_dkk=data.price,
            hybrid_slots=hybrid_slots,
            allow_battery_charge_reclaim=ev_active_solar_slot,
            solar_only_profile_name=solar_only_profile.key,
            solar_only_start_threshold_w=solar_only_profile.start_surplus_w,
            solar_only_stop_threshold_w=solar_only_profile.stop_surplus_w,
            solar_only_grid_buffer_w=solar_only_profile.grid_buffer_w,
        )

        ev_result = ev_optimizer.optimize(ctx, mode=ev_charge_mode)
        if ev_charge_mode == "solar_only":
            ev_result = ev_runtime.apply_solar_only_hysteresis(
                ctx=ctx,
                result=ev_result,
                profile=solar_only_profile,
                actual_charging=actual_charging,
            )

        data.ev_charging_enabled = True
        data.ev_charging_power = charger_power
        data.ev_vehicle_soc = vehicle_soc
        data.ev_target_soc = vehicle_target_soc
        data.ev_surplus_w = ev_result.surplus_w
        data.ev_strategy_reason = ev_result.reason
        data.ev_charger_status = charger_status
        data.ev_target_w = ev_result.target_w if ev_result.should_charge else 0.0
        data.ev_phases = ev_result.phases if ev_result.should_charge else 0
        data.ev_vehicle_soc_kwh = round(
            vehicle_soc / 100 * vehicle_battery_kwh, 2
        )
        data.ev_needed_kwh = round(
            max(0.0, (vehicle_target_soc - vehicle_soc) / 100 * vehicle_battery_kwh), 2
        )
        data.ev_hours_to_departure = round(
            (departure - ha_dt.now()).total_seconds() / 3600, 1
        )
        data.ev_charge_mode = ev_charge_mode
        data.ev_min_range_km = min_range_km
        data.ev_emergency_charging = ev_result.is_emergency
        if min_range_km > 0 and vehicle_efficiency > 0 and vehicle_battery_kwh > 0:
            data.ev_min_soc_from_range = min(
                100.0,
                min_range_km / vehicle_efficiency / vehicle_battery_kwh * 100,
            )
        else:
            data.ev_min_soc_from_range = 0.0

        if (
            inverter is not None
            and inverter.is_configured
            and optimize_result is not None
            and data.battery_soc is not None
            and self.requires_battery_hold(
                data=data,
                ev_result=ev_result,
                ev_charge_mode=ev_charge_mode,
            )
        ):
            await inverter.apply(
                replace(
                    optimize_result,
                    strategy="EV_HOLD_BATTERY",
                    charge_now=False,
                    target_soc=math.ceil(data.battery_soc),
                    reason=(
                        "EV holder batteri-SOC via midlertidig TOU. "
                        f"{optimize_result.reason}"
                    ),
                )
            )

        await ev_runtime.async_apply_charge_decision(
            ev_result=ev_result,
            now=ha_dt.now(),
        )
        data.ev_plan = self.compute_ev_plan(
            data=data,
            ev_planning=ev_planning,
            ev_runtime=ev_runtime,
            ev_charge_mode=ev_charge_mode,
            ev_min_range_km=ev_min_range_km,
            now=ha_dt.now(),
            departure=ev_next_departure,
        )
        return ev_runtime.currently_charging
