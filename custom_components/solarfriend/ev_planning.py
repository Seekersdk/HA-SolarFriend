"""Helpers for EV planning and EV-vs-battery priority decisions."""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Any, Callable

from .ev_optimizer import EVContext, EVHybridSlot, EVOptimizer, _should_prioritize_ev_solar

_LOGGER = logging.getLogger(__name__)


class EVPlanningHelper:
    """Build EV slots, EV plans and EV solar reservations outside the coordinator."""

    def __init__(
        self,
        *,
        entry: Any,
        ev_optimizer: EVOptimizer,
        vehicle: Any,
        vehicle_battery_kwh: float,
        ev_min_range_km: float,
        get_raw_prices: Callable[[], list[dict[str, Any]]],
        forecast_kwh_between: Callable[[datetime, datetime], float],
        normalize_local_datetime: Callable[[datetime], datetime],
    ) -> None:
        self._entry = entry
        self._ev_optimizer = ev_optimizer
        self._vehicle = vehicle
        self._vehicle_battery_kwh = vehicle_battery_kwh
        self._ev_min_range_km = ev_min_range_km
        self._get_raw_prices = get_raw_prices
        self._forecast_kwh_between = forecast_kwh_between
        self._normalize_local_datetime = normalize_local_datetime

    def build_ev_hybrid_slots(
        self,
        *,
        data: Any,
        now: datetime,
        departure: datetime,
        include_battery_reserved: bool = True,
    ) -> list[EVHybridSlot]:
        """Build EV planning slots from forecast, load profile and battery plan."""
        solar_by_start: dict[datetime, float] = {}
        if data.forecast_data and data.forecast_data.hourly_forecast:
            for slot in data.forecast_data.hourly_forecast:
                raw_start = slot.get("period_start")
                if raw_start is None:
                    continue
                try:
                    dt = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                    local_start = self._normalize_local_datetime(dt).replace(
                        minute=0, second=0, microsecond=0
                    )
                    solar_by_start[local_start] = solar_by_start.get(local_start, 0.0) + float(
                        slot.get("pv_estimate_kwh", 0.0)
                    ) * 1000.0
                except (TypeError, ValueError):
                    continue

        battery_reserved_by_start: dict[datetime, float] = {}
        if include_battery_reserved:
            for slot in data.battery_plan or []:
                try:
                    dt = datetime.fromisoformat(str(slot["hour"]))
                    local_start = self._normalize_local_datetime(dt).replace(
                        minute=0, second=0, microsecond=0
                    )
                    reserved_w = float(slot.get("solar_charge_w", 0.0)) + float(slot.get("grid_charge_w", 0.0))
                    battery_reserved_by_start[local_start] = max(
                        battery_reserved_by_start.get(local_start, 0.0),
                        reserved_w,
                    )
                except (KeyError, TypeError, ValueError):
                    continue

        price_by_start: dict[datetime, float] = {}
        for entry in self._get_raw_prices():
            raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_dt is None or raw_price is None:
                continue
            try:
                dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
                local_start = self._normalize_local_datetime(dt).replace(
                    minute=0, second=0, microsecond=0
                )
                price_by_start[local_start] = float(raw_price)
            except (TypeError, ValueError):
                continue

        now = self._normalize_local_datetime(now)
        departure = self._normalize_local_datetime(departure)
        slots: list[EVHybridSlot] = []
        slot_start = now.replace(minute=0, second=0, microsecond=0)
        while slot_start < departure:
            slot_end = slot_start + timedelta(hours=1)
            effective_start = max(slot_start, now)
            effective_end = min(slot_end, departure)
            duration_h = (effective_end - effective_start).total_seconds() / 3600.0
            if duration_h > 0:
                load_w = 850.0
                chart = data.consumption_profile_chart
                if chart and len(chart) > slot_start.hour:
                    try:
                        load_w = float(chart[slot_start.hour])
                    except (TypeError, ValueError):
                        load_w = 850.0
                pv_w = solar_by_start.get(slot_start, 0.0)
                battery_reserved_w = battery_reserved_by_start.get(slot_start, 0.0)
                solar_surplus_w = max(0.0, pv_w - load_w - battery_reserved_w)
                slots.append(
                    EVHybridSlot(
                        start=slot_start,
                        duration_h=duration_h,
                        price_dkk=float(price_by_start.get(slot_start, float("inf"))),
                        solar_surplus_w=solar_surplus_w,
                    )
                )
            slot_start = slot_end

        return slots

    def compute_ev_plan(
        self,
        *,
        data: Any,
        ev_charge_mode: str,
        ev_currently_charging: bool,
        ev_min_range_km: float,
        now: datetime,
        departure: datetime,
    ) -> list[dict[str, Any]]:
        """Build EV plan from the active slot-based EV optimizer."""
        current_soc = data.ev_vehicle_soc or 0.0
        target_soc = data.ev_target_soc or 80.0
        capacity_kwh = max(0.1, self._vehicle_battery_kwh)
        ctx = EVContext(
            pv_power_w=data.pv_power,
            load_power_w=data.load_power,
            grid_power_w=data.grid_power,
            battery_charging_w=data.battery_power,
            battery_soc=data.battery_soc,
            battery_capacity_kwh=float(self._entry.data.get("battery_capacity_kwh", 10.0)),
            battery_min_soc=float(self._entry.data.get("battery_min_soc", 10.0)),
            charger_status=data.ev_charger_status or "connected",
            currently_charging=ev_currently_charging,
            vehicle_soc=current_soc,
            vehicle_capacity_kwh=capacity_kwh,
            vehicle_target_soc=target_soc,
            departure=departure,
            current_price=data.price,
            raw_prices=self._get_raw_prices(),
            max_charge_kw=float(self._entry.data.get("ev_max_charge_kw", 7.4)),
            driving_range_km=self._vehicle.get_driving_range(),
            min_range_km=ev_min_range_km,
            vehicle_efficiency_km_per_kwh=float(self._entry.data.get("vehicle_efficiency_km_per_kwh", 6.0)),
            now=now,
            solar_forecast_to_departure_kwh=self._forecast_kwh_between(now, departure),
            ev_plan_expected_soc_now=current_soc,
            current_price_dkk=data.price,
            hybrid_slots=self.build_ev_hybrid_slots(data=data, now=now, departure=departure),
            allow_battery_charge_reclaim=False,
        )
        raw_plan = self._ev_optimizer.build_plan(ctx, mode=ev_charge_mode)
        plan: list[dict[str, Any]] = []
        soc = current_soc
        for slot in raw_plan:
            charge_kwh = float(slot["total_w"]) / 1000.0 * float(slot["duration_h"])
            soc = min(target_soc, soc + (charge_kwh / capacity_kwh) * 100.0)
            plan.append(
                {
                    "hour": slot["start"].isoformat(),
                    "soc": round(soc, 1),
                    "solar_w": round(float(slot["solar_w"])),
                    "grid_w": round(float(slot["grid_w"])),
                    "total_w": round(float(slot["total_w"])),
                    "price_dkk": round(float(slot["price_dkk"]), 4)
                    if not math.isinf(float(slot["price_dkk"]))
                    else 9999.0,
                }
            )
        return plan

    def build_ev_battery_priority_reservations(
        self,
        *,
        ev_enabled: bool,
        ev_charging_allowed: bool,
        data: Any,
        ev_charge_mode: str,
        ev_currently_charging: bool,
        ev_min_range_km: float,
        vehicle_target_soc_override: float | None,
        now: datetime,
        departure: datetime,
        ev_next_departure: datetime,
    ) -> dict[datetime, float]:
        """Reserve EV solar before departure when EV should win over the house battery."""
        if not ev_enabled or not ev_charging_allowed:
            return {}

        vehicle_soc = self._vehicle.get_soc()
        vehicle_target_soc = (
            vehicle_target_soc_override
            if vehicle_target_soc_override is not None
            else self._vehicle.get_target_soc()
        )
        if vehicle_target_soc <= vehicle_soc + 0.1:
            return {}

        charger_status = data.ev_charger_status or "connected"
        if charger_status == "disconnected":
            return {}

        # Evaluate EV solar priority against raw forecast/load before the battery plan
        # consumes the same solar.
        raw_slots = self.build_ev_hybrid_slots(
            data=data,
            now=now,
            departure=departure,
            include_battery_reserved=False,
        )
        if not raw_slots:
            return {}

        vehicle_efficiency = float(self._entry.data.get("vehicle_efficiency_km_per_kwh", 6.0))
        ev_ctx = EVContext(
            pv_power_w=data.pv_power,
            load_power_w=data.load_power,
            grid_power_w=data.grid_power,
            battery_charging_w=data.battery_power,
            battery_soc=data.battery_soc,
            battery_capacity_kwh=float(self._entry.data.get("battery_capacity_kwh", 10.0)),
            battery_min_soc=float(self._entry.data.get("battery_min_soc", 10.0)),
            charger_status=charger_status,
            currently_charging=ev_currently_charging,
            vehicle_soc=vehicle_soc,
            vehicle_capacity_kwh=self._vehicle_battery_kwh,
            vehicle_target_soc=vehicle_target_soc,
            departure=departure,
            current_price=data.price,
            raw_prices=self._get_raw_prices(),
            max_charge_kw=float(self._entry.data.get("ev_max_charge_kw", 7.4)),
            driving_range_km=self._vehicle.get_driving_range(),
            min_range_km=ev_min_range_km,
            vehicle_efficiency_km_per_kwh=vehicle_efficiency,
            now=now,
            solar_forecast_to_departure_kwh=self._forecast_kwh_between(now, departure),
            ev_plan_expected_soc_now=vehicle_soc,
            current_price_dkk=data.price,
            hybrid_slots=raw_slots,
            allow_battery_charge_reclaim=False,
        )
        ev_plan = self._ev_optimizer.build_plan(ev_ctx, mode=ev_charge_mode)
        raw_solar_by_start: dict[datetime, float] = {}
        for slot in raw_slots:
            raw_solar_by_start[slot.start] = max(
                0.0,
                float(slot.solar_surplus_w) / 1000.0 * float(slot.duration_h),
            )

        ev_solar_by_start: dict[datetime, float] = {}
        for slot in ev_plan:
            ev_solar_by_start[slot["start"]] = max(
                0.0,
                float(slot["solar_w"]) / 1000.0 * float(slot["duration_h"]),
            )

        reservations: dict[datetime, float] = {}
        reserved_total_kwh = 0.0

        if ev_charge_mode == "solar_only":
            battery_max_soc = float(self._entry.data.get("battery_max_soc", 100.0))
            current_battery_soc = data.battery_soc if data.battery_soc is not None else battery_max_soc
            battery_needed_kwh = max(
                0.0,
                (battery_max_soc - current_battery_soc) / 100.0
                * float(self._entry.data.get("battery_capacity_kwh", 10.0)),
            )

            ordered_starts = sorted(raw_solar_by_start)
            future_raw_after_start: dict[datetime, float] = {}
            future_sum = 0.0
            for start in reversed(ordered_starts):
                future_raw_after_start[start] = future_sum
                future_sum += raw_solar_by_start.get(start, 0.0)

            for start in ordered_starts:
                raw_kwh = raw_solar_by_start.get(start, 0.0)
                ev_kwh = ev_solar_by_start.get(start, 0.0)
                if raw_kwh <= 0 or ev_kwh <= 0:
                    continue
                future_raw_kwh = future_raw_after_start.get(start, 0.0)
                must_leave_for_battery = max(0.0, battery_needed_kwh - future_raw_kwh)
                reserveable_kwh = min(ev_kwh, max(0.0, raw_kwh - must_leave_for_battery))
                if reserveable_kwh <= 0:
                    continue
                reservations[start] = reserveable_kwh
                reserved_total_kwh += reserveable_kwh
        else:
            for slot in ev_plan:
                solar_w = float(slot["solar_w"])
                duration_h = float(slot["duration_h"])
                if solar_w <= 0 or duration_h <= 0:
                    continue
                start = slot["start"]
                reserved_kwh = solar_w / 1000.0 * duration_h
                reservations[start] = reservations.get(start, 0.0) + reserved_kwh
                reserved_total_kwh += reserved_kwh

        if reserved_total_kwh <= 0:
            return {}

        ev_alt_grid_price = None
        ev_grid_prices = [
            float(slot["price_dkk"])
            for slot in ev_plan
            if float(slot["grid_w"]) > 0 and math.isfinite(float(slot["price_dkk"]))
        ]
        if ev_grid_prices:
            ev_alt_grid_price = min(ev_grid_prices)

        battery_future_value = None
        future_battery_prices = []
        for entry in self._get_raw_prices():
            raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_dt is None or raw_price is None:
                continue
            try:
                dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
                local_start = self._normalize_local_datetime(dt).replace(
                    minute=0, second=0, microsecond=0
                )
                if local_start >= departure:
                    future_battery_prices.append(float(raw_price))
            except (TypeError, ValueError):
                continue
        if future_battery_prices:
            battery_future_value = max(future_battery_prices)

        recoverable_solar_kwh = 0.0
        recovery_end = ev_next_departure + timedelta(hours=10)
        for slot in self.build_ev_hybrid_slots(
            data=data,
            now=departure,
            departure=recovery_end,
            include_battery_reserved=False,
        ):
            if slot.start < departure:
                continue
            recoverable_solar_kwh += max(0.0, slot.solar_surplus_w) / 1000.0 * float(slot.duration_h)

        if ev_charge_mode != "solar_only":
            if not _should_prioritize_ev_solar(
                ev_alt_grid_price=ev_alt_grid_price,
                battery_future_value=battery_future_value,
                recoverable_battery_kwh=recoverable_solar_kwh,
                reserved_ev_solar_kwh=reserved_total_kwh,
            ):
                return {}

        _LOGGER.debug(
            "EV priority active: reserving %.2f kWh solar for EV before %s; EV alt %.2f kr/kWh, battery alt %.2f kr/kWh, recoverable afterwards %.2f kWh",
            reserved_total_kwh,
            departure.strftime("%H:%M"),
            ev_alt_grid_price if ev_alt_grid_price is not None else -1.0,
            battery_future_value if battery_future_value is not None else -1.0,
            recoverable_solar_kwh,
        )
        return reservations
