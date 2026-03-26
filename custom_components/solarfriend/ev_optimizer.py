"""EV charging optimizer for solar_only, hybrid, and grid_schedule modes."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_LOGGER = logging.getLogger(__name__)

VOLTAGE = 235.0
MIN_AMPS = 6.0
MAX_AMPS = 16.0
MIN_1PHASE_W = MIN_AMPS * VOLTAGE
MIN_3PHASE_W = MIN_AMPS * 3 * VOLTAGE

# Backwards-compat aliases used by tests
MIN_CHARGE_AMPS = MIN_AMPS
MAX_CHARGE_AMPS = MAX_AMPS
MIN_SURPLUS_W = MIN_1PHASE_W
SURPLUS_HYSTERESIS_W = 200.0
STOP_THRESHOLD_W = MIN_1PHASE_W - SURPLUS_HYSTERESIS_W


@dataclass(frozen=True)
class EVHybridSlot:
    """Slot input used by hybrid/grid planning."""

    start: datetime
    duration_h: float
    price_dkk: float
    solar_surplus_w: float = 0.0


@dataclass
class EVContext:
    """All inputs the EV optimizer needs."""

    pv_power_w: float
    load_power_w: float
    battery_charging_w: float
    battery_soc: float
    battery_capacity_kwh: float
    battery_min_soc: float
    charger_status: str
    currently_charging: bool
    vehicle_soc: float
    vehicle_capacity_kwh: float
    vehicle_target_soc: float
    departure: datetime
    current_price: float
    raw_prices: list
    max_charge_kw: float
    driving_range_km: float | None = None
    min_range_km: float = 0.0
    vehicle_efficiency_km_per_kwh: float = 6.0
    now: datetime = field(default_factory=datetime.now)
    solar_forecast_to_departure_kwh: float = 0.0
    ev_plan_expected_soc_now: float = 0.0
    current_price_dkk: float = 0.0
    hybrid_slots: list[EVHybridSlot] = field(default_factory=list)


@dataclass
class EVOptimizeResult:
    should_charge: bool
    target_w: float
    phases: int
    target_amps: float
    reason: str
    vehicle_soc: float = 0.0
    vehicle_target_soc: float = 80.0
    surplus_w: float = 0.0
    charger_status: str = "disconnected"
    is_emergency: bool = False


def _strip_tz(dt: datetime) -> datetime:
    """Return a naive datetime by stripping tzinfo."""
    return dt.replace(tzinfo=None)


def _hour_start(dt: datetime) -> datetime:
    """Return dt truncated to the hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


def _parse_prices(raw_prices: list) -> list[tuple[datetime, float]]:
    """Parse raw price entries into sorted naive datetime/price tuples."""
    result: list[tuple[datetime, float]] = []
    for entry in raw_prices:
        try:
            raw_dt = entry.get("hour") or entry.get("start")
            if raw_dt is None:
                continue
            dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_price is None:
                continue
            result.append((_strip_tz(dt), float(raw_price)))
        except (TypeError, ValueError):
            continue
    result.sort(key=lambda item: item[0])
    return result


def _find_cheapest_charge_hours(
    raw_prices: list,
    now: datetime,
    departure: datetime,
    n_hours: int,
) -> set[datetime]:
    """Return the cheapest hour starts before departure."""
    if n_hours <= 0:
        return set()

    now_naive = _strip_tz(now)
    dep_naive = _strip_tz(departure)
    candidates = [
        (dt, price)
        for dt, price in _parse_prices(raw_prices)
        if now_naive <= dt < dep_naive
    ]
    candidates.sort(key=lambda item: item[1])
    return {dt for dt, _ in candidates[:n_hours]}


def _battery_needs_priority(ctx: EVContext) -> bool:
    """True if battery SOC is below its minimum."""
    return ctx.battery_soc < ctx.battery_min_soc


def _surplus_w(ctx: EVContext) -> float:
    """Return solar surplus available for EV charging without using house battery."""
    battery_load_w = max(0.0, -ctx.battery_charging_w)
    battery_discharge_w = max(0.0, ctx.battery_charging_w)
    return ctx.pv_power_w - ctx.load_power_w - battery_load_w - battery_discharge_w


def _needed_kwh(ctx: EVContext) -> float:
    """Return kWh needed to reach target SOC."""
    return max(
        0.0,
        (ctx.vehicle_target_soc - ctx.vehicle_soc) / 100.0 * ctx.vehicle_capacity_kwh,
    )


def _needed_charge_hours(ctx: EVContext) -> int:
    """Return the number of full-power hours needed."""
    if ctx.max_charge_kw <= 0:
        return 1
    return max(1, math.ceil(_needed_kwh(ctx) / ctx.max_charge_kw))


def _normalize_for_compare(dt: datetime) -> datetime:
    """Normalize datetime for hour-level comparison."""
    return dt.replace(second=0, microsecond=0, tzinfo=None)


def _should_prioritize_ev_solar(
    *,
    ev_alt_grid_price: float | None,
    battery_future_value: float | None,
    recoverable_battery_kwh: float,
    reserved_ev_solar_kwh: float,
) -> bool:
    """Return True when EV should keep solar priority over the house battery."""
    if reserved_ev_solar_kwh <= 0:
        return False
    if recoverable_battery_kwh + 1e-6 < reserved_ev_solar_kwh:
        return False
    if ev_alt_grid_price is None:
        return True
    if battery_future_value is None:
        return True
    return ev_alt_grid_price >= battery_future_value - 1e-9


class EVOptimizer:
    """Determine whether and how an EV should charge."""

    def _calc_phase_and_amps(
        self,
        surplus_w: float,
        max_charge_w: float,
    ) -> tuple[bool, int, float, float]:
        """Return (should_charge, phases, amps, actual_w)."""
        effective_w = min(surplus_w, max_charge_w)

        if effective_w < MIN_1PHASE_W:
            return False, 0, 0.0, 0.0

        if effective_w >= MIN_3PHASE_W:
            amps = min(MAX_AMPS, effective_w / 3 / VOLTAGE)
            actual_w = amps * 3 * VOLTAGE
            return True, 3, round(amps, 1), round(actual_w)

        amps = min(MAX_AMPS, effective_w / VOLTAGE)
        actual_w = amps * VOLTAGE
        return True, 1, round(amps, 1), round(actual_w)

    def _price_map(self, raw_prices: list) -> dict[datetime, float]:
        """Return hour-truncated price map."""
        return {_hour_start(dt): price for dt, price in _parse_prices(raw_prices)}

    def _build_planning_slots(self, ctx: EVContext) -> list[EVHybridSlot]:
        """Build hour slots for hybrid/grid planning."""
        now_naive = _strip_tz(ctx.now)
        dep_naive = _strip_tz(ctx.departure)

        if ctx.hybrid_slots:
            slots = [
                EVHybridSlot(
                    start=_hour_start(_strip_tz(slot.start)),
                    duration_h=max(0.0, float(slot.duration_h)),
                    price_dkk=float(slot.price_dkk),
                    solar_surplus_w=max(0.0, float(slot.solar_surplus_w)),
                )
                for slot in ctx.hybrid_slots
                if now_naive <= _hour_start(_strip_tz(slot.start)) < dep_naive
                and float(slot.duration_h) > 0
            ]
            slots.sort(key=lambda item: item.start)
            return slots

        price_map = self._price_map(ctx.raw_prices)
        current_surplus = max(0.0, _surplus_w(ctx))
        slots: list[EVHybridSlot] = []
        slot_start = _hour_start(now_naive)
        while slot_start < dep_naive:
            slot_end = slot_start + timedelta(hours=1)
            effective_start = max(slot_start, now_naive)
            effective_end = min(slot_end, dep_naive)
            duration_h = (effective_end - effective_start).total_seconds() / 3600.0
            if duration_h > 0:
                slots.append(
                    EVHybridSlot(
                        start=slot_start,
                        duration_h=duration_h,
                        price_dkk=float(price_map.get(slot_start, float("inf"))),
                        solar_surplus_w=current_surplus if slot_start == _hour_start(now_naive) else 0.0,
                    )
                )
            slot_start = slot_end
        return slots

    def build_plan(self, ctx: EVContext, mode: str = "solar_only") -> list[dict]:
        """Build a slot-based EV plan from now until departure."""
        slots = self._build_planning_slots(ctx)
        if not slots:
            return []

        max_charge_w = max(0.0, ctx.max_charge_kw * 1000.0)
        remaining_need_kwh = _needed_kwh(ctx)
        plan: list[dict] = []

        for slot in slots:
            solar_capacity_w = min(max_charge_w, max(0.0, slot.solar_surplus_w))
            solar_used_w = 0.0
            if mode in ("solar_only", "hybrid") and solar_capacity_w >= MIN_1PHASE_W and remaining_need_kwh > 0:
                solar_kwh = min(remaining_need_kwh, solar_capacity_w / 1000.0 * slot.duration_h)
                solar_used_w = solar_kwh / slot.duration_h * 1000.0 if slot.duration_h > 0 else 0.0
                remaining_need_kwh -= solar_kwh
            plan.append(
                {
                    "start": slot.start,
                    "duration_h": slot.duration_h,
                    "price_dkk": slot.price_dkk,
                    "solar_surplus_w": solar_capacity_w,
                    "solar_w": solar_used_w,
                    "grid_w": 0.0,
                    "total_w": solar_used_w,
                }
            )

        if mode == "solar_only":
            return plan

        grid_needed_kwh = max(0.0, remaining_need_kwh)
        if grid_needed_kwh <= 0:
            return plan

        indexed_slots = sorted(
            enumerate(plan),
            key=lambda item: (float(item[1]["price_dkk"]), item[1]["start"]),
        )
        if not indexed_slots or math.isinf(float(indexed_slots[0][1]["price_dkk"])):
            return plan

        for idx, slot in indexed_slots:
            if grid_needed_kwh <= 1e-6:
                break
            duration_h = float(slot["duration_h"])
            solar_used_w = float(slot["solar_w"])
            grid_headroom_w = max(0.0, max_charge_w - solar_used_w)
            if duration_h <= 0 or grid_headroom_w <= 0:
                continue

            grid_capacity_kwh = grid_headroom_w / 1000.0 * duration_h
            if grid_capacity_kwh <= 0:
                continue

            grid_used_kwh = min(grid_needed_kwh, grid_capacity_kwh)
            grid_used_w = grid_used_kwh / duration_h * 1000.0
            slot["grid_w"] = grid_used_w
            slot["total_w"] = solar_used_w + grid_used_w
            plan[idx] = slot
            grid_needed_kwh -= grid_used_kwh

        return plan

    def _solar_only(self, ctx: EVContext) -> EVOptimizeResult:
        """Charge only when solar surplus is available."""
        surplus = _surplus_w(ctx)
        base = EVOptimizeResult(
            should_charge=False,
            target_w=0.0,
            phases=0,
            target_amps=0.0,
            reason="",
            vehicle_soc=ctx.vehicle_soc,
            vehicle_target_soc=ctx.vehicle_target_soc,
            surplus_w=surplus,
            charger_status=ctx.charger_status,
        )

        if ctx.charger_status == "disconnected":
            base.reason = "Bil ikke tilsluttet laderen"
            return base

        if ctx.vehicle_soc >= ctx.vehicle_target_soc:
            base.reason = f"Bil fuld ({ctx.vehicle_soc:.0f}% >= {ctx.vehicle_target_soc:.0f}%)"
            return base

        threshold = STOP_THRESHOLD_W if ctx.currently_charging else MIN_1PHASE_W
        if surplus < threshold:
            base.reason = f"For lidt sol-overskud ({surplus:.0f}W < {threshold:.0f}W)"
            return base

        effective_surplus = max(surplus, MIN_1PHASE_W)
        _, phases, amps, actual_w = self._calc_phase_and_amps(effective_surplus, ctx.max_charge_kw * 1000)
        if actual_w <= 0:
            base.reason = "Max ladeeffekt under minimum"
            return base
        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        base.reason = f"Sol-overskud {surplus:.0f}W -> {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
        return base

    def _hybrid(self, ctx: EVContext) -> EVOptimizeResult:
        """Solar-first; supplement with cheapest grid slots before departure."""
        now = ctx.now

        hours_to_departure = max(0.1, (_strip_tz(ctx.departure) - _strip_tz(now)).total_seconds() / 3600.0)
        missing_kwh = _needed_kwh(ctx)
        max_possible_kwh = hours_to_departure * ctx.max_charge_kw

        if (
            ctx.charger_status != "disconnected"
            and ctx.vehicle_soc < ctx.vehicle_target_soc
            and missing_kwh > 0
            and missing_kwh >= max_possible_kwh * 0.9
        ):
            _, phases, amps, actual_w = self._calc_phase_and_amps(ctx.max_charge_kw * 1000, ctx.max_charge_kw * 1000)
            if actual_w > 0:
                return EVOptimizeResult(
                    should_charge=True,
                    target_w=actual_w,
                    phases=phases,
                    target_amps=amps,
                    reason=f"Deadline - {missing_kwh:.1f} kWh / {hours_to_departure:.1f}t",
                    vehicle_soc=ctx.vehicle_soc,
                    vehicle_target_soc=ctx.vehicle_target_soc,
                    surplus_w=_surplus_w(ctx),
                    charger_status=ctx.charger_status,
                )

        deficit_kwh = max(
            0.0,
            (ctx.ev_plan_expected_soc_now - ctx.vehicle_soc) / 100.0 * ctx.vehicle_capacity_kwh,
        )
        if deficit_kwh > 0.5 and ctx.charger_status != "disconnected" and ctx.vehicle_soc < ctx.vehicle_target_soc:
            sol_remaining = ctx.solar_forecast_to_departure_kwh
            if sol_remaining >= deficit_kwh * 1.2:
                result = self._solar_only(ctx)
                result.reason = f"Bagud {deficit_kwh:.1f} kWh - sol indhenter ({sol_remaining:.1f} kWh)"
                return result

            now_naive = _strip_tz(now)
            dep_naive = _strip_tz(ctx.departure)
            future_prices = [price for dt, price in _parse_prices(ctx.raw_prices) if now_naive < dt < dep_naive]
            if future_prices:
                cheapest_future = min(future_prices)
                if cheapest_future < ctx.current_price_dkk * 0.7:
                    result = self._solar_only(ctx)
                    result.reason = (
                        f"Bagud {deficit_kwh:.1f} kWh - venter pa billigere time "
                        f"({cheapest_future:.2f} kr vs {ctx.current_price_dkk:.2f} kr nu)"
                    )
                    return result

            grid_boost_w = min(deficit_kwh / max(0.5, hours_to_departure) * 1000.0, ctx.max_charge_kw * 1000.0)
            _, phases, amps, actual_w = self._calc_phase_and_amps(grid_boost_w, ctx.max_charge_kw * 1000.0)
            if actual_w > 0:
                return EVOptimizeResult(
                    should_charge=True,
                    target_w=actual_w,
                    phases=phases,
                    target_amps=amps,
                    reason=f"Bagud {deficit_kwh:.1f} kWh - supplerer med {grid_boost_w:.0f}W grid",
                    vehicle_soc=ctx.vehicle_soc,
                    vehicle_target_soc=ctx.vehicle_target_soc,
                    surplus_w=_surplus_w(ctx),
                    charger_status=ctx.charger_status,
                )

        solar_result = self._solar_only(ctx)
        if solar_result.should_charge:
            return solar_result

        surplus = _surplus_w(ctx)
        base = EVOptimizeResult(
            should_charge=False,
            target_w=0.0,
            phases=0,
            target_amps=0.0,
            reason=solar_result.reason,
            vehicle_soc=ctx.vehicle_soc,
            vehicle_target_soc=ctx.vehicle_target_soc,
            surplus_w=surplus,
            charger_status=ctx.charger_status,
        )

        if ctx.charger_status == "disconnected":
            return base
        if ctx.vehicle_soc >= ctx.vehicle_target_soc:
            return base
        if _battery_needs_priority(ctx):
            base.reason = f"Batteri prioritet (SOC {ctx.battery_soc:.0f}% < {ctx.battery_min_soc:.0f}%)"
            return base
        if not ctx.raw_prices and not ctx.hybrid_slots:
            base.reason = "Ingen prisdata tilgaengelig for hybrid-planlaegning"
            return base

        plan = self.build_plan(ctx, mode="hybrid")
        current_hour = _hour_start(_strip_tz(ctx.now))
        current_slot = next(
            (slot for slot in plan if _normalize_for_compare(slot["start"]) == _normalize_for_compare(current_hour)),
            None,
        )
        if current_slot is None:
            base.reason = "Ingen hybrid-slot for nuvaerende time"
            return base

        grid_w = float(current_slot["grid_w"])
        total_w = float(current_slot["total_w"])
        solar_w = float(current_slot["solar_w"])
        if grid_w <= 0:
            if any(float(slot["grid_w"]) > 0 for slot in plan if slot["start"] > current_hour):
                base.reason = f"Ikke billigste time (nuvaerende {ctx.current_price:.3f} kr/kWh)"
                return base

            guaranteed_solar_kwh = sum(
                float(slot["solar_w"]) * float(slot["duration_h"]) / 1000.0
                for slot in plan
            )
            if max(0.0, _needed_kwh(ctx) - guaranteed_solar_kwh) <= 0:
                base.reason = "Forventet sol daekker behovet frem mod afgang"
            else:
                base.reason = "Ingen brugbar hybrid-gridplan fundet"
            return base

        _, phases, amps, actual_w = self._calc_phase_and_amps(total_w, ctx.max_charge_kw * 1000.0)
        if actual_w == 0.0:
            base.reason = "Max ladeeffekt under minimum"
            return base

        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        if solar_w > 0:
            base.reason = (
                f"Hybrid billig time {ctx.current_price:.3f} kr/kWh med sol {solar_w:.0f}W "
                f"+ grid {grid_w:.0f}W -> {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
            )
        else:
            base.reason = (
                f"Billig netladning {ctx.current_price:.3f} kr/kWh "
                f"-> {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
            )
        return base

    def _grid_schedule(self, ctx: EVContext) -> EVOptimizeResult:
        """Charge during the cheapest grid hours before departure."""
        surplus = _surplus_w(ctx)
        base = EVOptimizeResult(
            should_charge=False,
            target_w=0.0,
            phases=0,
            target_amps=0.0,
            reason="",
            vehicle_soc=ctx.vehicle_soc,
            vehicle_target_soc=ctx.vehicle_target_soc,
            surplus_w=surplus,
            charger_status=ctx.charger_status,
        )

        if ctx.charger_status == "disconnected":
            base.reason = "Bil ikke tilsluttet laderen"
            return base
        if ctx.vehicle_soc >= ctx.vehicle_target_soc:
            base.reason = f"Bil fuld ({ctx.vehicle_soc:.0f}% >= {ctx.vehicle_target_soc:.0f}%)"
            return base
        if _battery_needs_priority(ctx):
            base.reason = f"Batteri prioritet (SOC {ctx.battery_soc:.0f}% < {ctx.battery_min_soc:.0f}%)"
            return base
        if not ctx.raw_prices and not ctx.hybrid_slots:
            base.reason = "Ingen prisdata tilgaengelig"
            return base

        plan = self.build_plan(ctx, mode="grid_schedule")
        current_hour = _hour_start(_strip_tz(ctx.now))
        current_slot = next(
            (slot for slot in plan if _normalize_for_compare(slot["start"]) == _normalize_for_compare(current_hour)),
            None,
        )
        if current_slot is None:
            base.reason = "Ingen grid-slot for nuvaerende time"
            return base
        if float(current_slot["grid_w"]) <= 0:
            base.reason = f"Ikke billigste time (nuvaerende {ctx.current_price:.3f} kr/kWh)"
            return base

        _, phases, amps, actual_w = self._calc_phase_and_amps(float(current_slot["total_w"]), ctx.max_charge_kw * 1000.0)
        if actual_w == 0.0:
            base.reason = "Max ladeeffekt under minimum"
            return base

        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        base.reason = (
            f"Planlagt netladning {ctx.current_price:.3f} kr/kWh "
            f"-> {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
        )
        return base

    def _check_emergency_charging(self, ctx: EVContext) -> EVOptimizeResult | None:
        """Return emergency charge result if driving range is below minimum."""
        if ctx.min_range_km <= 0:
            return None
        if ctx.driving_range_km is None:
            return None
        if ctx.driving_range_km >= ctx.min_range_km:
            return None

        needed_soc = min(
            100.0,
            ctx.min_range_km / ctx.vehicle_efficiency_km_per_kwh / ctx.vehicle_capacity_kwh * 100.0,
        )
        _, phases, amps, actual_w = self._calc_phase_and_amps(ctx.max_charge_kw * 1000.0, ctx.max_charge_kw * 1000.0)
        return EVOptimizeResult(
            should_charge=True,
            target_w=actual_w,
            phases=phases,
            target_amps=amps,
            reason=(
                f"Nødopladning - {ctx.driving_range_km:.0f}km < {ctx.min_range_km:.0f}km minimum "
                f"(lader til {needed_soc:.0f}% SOC)"
            ),
            vehicle_soc=ctx.vehicle_soc,
            vehicle_target_soc=needed_soc,
            surplus_w=_surplus_w(ctx),
            charger_status=ctx.charger_status,
            is_emergency=True,
        )

    def optimize(self, ctx: EVContext, mode: str = "solar_only") -> EVOptimizeResult:
        """Dispatch to the mode-specific optimizer."""
        if mode == "solar_only":
            return self._solar_only(ctx)
        emergency = self._check_emergency_charging(ctx)
        if emergency is not None:
            return emergency
        if mode == "hybrid":
            return self._hybrid(ctx)
        if mode == "grid_schedule":
            return self._grid_schedule(ctx)
        _LOGGER.warning("Unknown EV charge mode: %s - using solar_only", mode)
        return self._solar_only(ctx)
