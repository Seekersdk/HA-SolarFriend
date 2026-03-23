"""EV charging optimizer — solar_only, hybrid, and grid_schedule modes."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime

_LOGGER = logging.getLogger(__name__)

VOLTAGE = 235.0                            # Mere præcis end 230V
MIN_AMPS = 6.0                             # Easee minimum (IEC 61851)
MAX_AMPS = 16.0                            # Easee maximum
MIN_1PHASE_W = MIN_AMPS * VOLTAGE          # 1410 W
MIN_3PHASE_W = MIN_AMPS * 3 * VOLTAGE      # 4230 W

# Backwards-compat aliases used by tests
MIN_CHARGE_AMPS = MIN_AMPS
MAX_CHARGE_AMPS = MAX_AMPS
MIN_SURPLUS_W = MIN_1PHASE_W
SURPLUS_HYSTERESIS_W = 200.0
STOP_THRESHOLD_W = MIN_1PHASE_W - SURPLUS_HYSTERESIS_W   # 1210 W


@dataclass
class EVContext:
    """All inputs the EV optimizer needs — build once, pass everywhere."""
    pv_power_w: float
    load_power_w: float
    battery_charging_w: float       # negative = charging battery, positive = discharging
    battery_soc: float              # 0–100 %
    battery_capacity_kwh: float
    battery_min_soc: float          # 0–100 %
    charger_status: str             # "connected" | "charging" | "disconnected" | "error"
    currently_charging: bool
    vehicle_soc: float              # 0–100 %
    vehicle_capacity_kwh: float
    vehicle_target_soc: float       # 0–100 %
    departure: datetime
    current_price: float            # kr/kWh
    raw_prices: list                # [{"hour": ISO-str, "price": float}, ...]
    max_charge_kw: float            # Max charger power, e.g. 7.4
    driving_range_km: float | None = None   # None = sensor not configured
    min_range_km: float = 0.0               # 0 = feature disabled
    vehicle_efficiency_km_per_kwh: float = 6.0
    now: datetime = field(default_factory=datetime.now)


@dataclass
class EVOptimizeResult:
    should_charge: bool
    target_w: float                 # ønsket effekt i W
    phases: int                     # 1 eller 3
    target_amps: float              # beregnet amps (til logging)
    reason: str
    vehicle_soc: float = 0.0
    vehicle_target_soc: float = 80.0
    surplus_w: float = 0.0
    charger_status: str = "disconnected"
    is_emergency: bool = False


# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------

def _strip_tz(dt: datetime) -> datetime:
    """Return a naive datetime by stripping tzinfo. All comparisons use naive times."""
    return dt.replace(tzinfo=None)


def _parse_prices(raw_prices: list) -> list[tuple[datetime, float]]:
    """Parse raw_prices list → sorted list of naive (datetime, price) tuples.

    Handles multiple sensor formats:
      EDS/custom:   {"hour": "ISO_string", "price": float}
      Nordpool:     {"start": datetime_or_ISO, "end": ..., "value": float}
    """
    result: list[tuple[datetime, float]] = []
    for entry in raw_prices:
        try:
            # Datetime: try "hour" key first, then "start"
            raw_dt = entry.get("hour") or entry.get("start")
            if raw_dt is None:
                continue
            if isinstance(raw_dt, datetime):
                dt = _strip_tz(raw_dt)
            else:
                dt = _strip_tz(datetime.fromisoformat(str(raw_dt)))

            # Price: try "price" key first, then "value"
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_price is None:
                continue

            result.append((dt, float(raw_price)))
        except (ValueError, TypeError):
            continue
    result.sort(key=lambda x: x[0])
    return result


def _find_cheapest_charge_hours(
    raw_prices: list,
    now: datetime,
    departure: datetime,
    n_hours: int,
) -> set[datetime]:
    """Return set of naive hour-truncated datetimes for the cheapest n_hours before departure."""
    if n_hours <= 0:
        return set()

    parsed = _parse_prices(raw_prices)

    # Normalise to naive for all comparisons
    now_naive = _strip_tz(now)
    dep_naive = _strip_tz(departure)

    candidates = [
        (dt, price) for dt, price in parsed
        if now_naive <= dt < dep_naive
    ]

    if not candidates:
        return set()

    candidates.sort(key=lambda x: x[1])
    return {dt for dt, _ in candidates[:n_hours]}


def _battery_needs_priority(ctx: EVContext) -> bool:
    """True if battery SOC is below its minimum — battery takes priority over EV."""
    return ctx.battery_soc < ctx.battery_min_soc


def _surplus_w(ctx: EVContext) -> float:
    """
    Net solar surplus available for EV charging.

    battery_charging_w < 0 → batteri lader (trækker fra sol)
    battery_charging_w > 0 → batteri aflader (giver til hus, men er ikke sol)

    Vi trækker begge dele fra surplus så vi aldrig bruger husbatteri til EV.
    """
    battery_load_w = max(0.0, -ctx.battery_charging_w)   # batteri lader
    battery_discharge_w = max(0.0, ctx.battery_charging_w)  # batteri aflader
    return ctx.pv_power_w - ctx.load_power_w - battery_load_w - battery_discharge_w


def _needed_kwh(ctx: EVContext) -> float:
    """kWh needed to reach target SOC."""
    return max(
        0.0,
        (ctx.vehicle_target_soc - ctx.vehicle_soc) / 100.0 * ctx.vehicle_capacity_kwh,
    )


def _needed_charge_hours(ctx: EVContext) -> int:
    """Number of full charge hours required to reach target SOC."""
    kwh = _needed_kwh(ctx)
    if ctx.max_charge_kw <= 0:
        return 1
    return max(1, math.ceil(kwh / ctx.max_charge_kw))


def _normalize_for_compare(dt: datetime) -> datetime:
    """Strip microseconds and tzinfo for hour-level comparison."""
    return dt.replace(second=0, microsecond=0, tzinfo=None)


# ---------------------------------------------------------------------------
# EVOptimizer
# ---------------------------------------------------------------------------

class EVOptimizer:
    """
    Bestemmer om og hvordan elbilen skal oplades.

    Modes:
      solar_only    — lad kun fra sol-overskud
      hybrid        — sol-overskud primært; supplér med billige net-timer til afgang
      grid_schedule — planlæg ladning i de billigste timer frem til afgang

    Fase-logik (porteret fra HA template):
      surplus < 1410W               → ingen ladning
      1410W ≤ surplus < 4230W       → 1-fase, amps = surplus / 235
      surplus ≥ 4230W               → 3-fase, amps = surplus / 3 / 235
      max 16A per fase
    """

    def _calc_phase_and_amps(
        self, surplus_w: float, max_charge_w: float
    ) -> tuple[bool, int, float, float]:
        """
        Returnerer: (should_charge, phases, amps, actual_w)

        Cap surplus til brugerens max ladehastighed, beregn derefter
        1- eller 3-fase amps efter template-logikken.
        """
        effective_w = min(surplus_w, max_charge_w)

        if effective_w < MIN_1PHASE_W:
            return False, 0, 0.0, 0.0

        if effective_w >= MIN_3PHASE_W:
            amps = min(MAX_AMPS, effective_w / 3 / VOLTAGE)
            actual_w = amps * 3 * VOLTAGE
            return True, 3, round(amps, 1), round(actual_w)

        # 1-fase
        amps = min(MAX_AMPS, effective_w / VOLTAGE)
        actual_w = amps * VOLTAGE
        return True, 1, round(amps, 1), round(actual_w)

    # ------------------------------------------------------------------
    # Mode implementations
    # ------------------------------------------------------------------

    def _solar_only(self, ctx: EVContext) -> EVOptimizeResult:
        """Charge only when solar surplus is available — no grid charging."""
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
            base.reason = (
                f"Bil fuld ({ctx.vehicle_soc:.0f}% ≥ {ctx.vehicle_target_soc:.0f}%)"
            )
            return base

        # Hysterese: lavere threshold for at stoppe end for at starte
        threshold = STOP_THRESHOLD_W if ctx.currently_charging else MIN_1PHASE_W

        if surplus < threshold:
            base.reason = (
                f"For lidt sol-overskud ({surplus:.0f}W < {threshold:.0f}W)"
            )
            return base

        # I hysterese-zonen (STOP_THRESHOLD ≤ surplus < MIN_1PHASE_W):
        # fortsæt på minimums 1-fase. Ellers beregn normalt.
        effective_surplus = max(surplus, MIN_1PHASE_W)
        _, phases, amps, actual_w = self._calc_phase_and_amps(
            effective_surplus, ctx.max_charge_kw * 1000
        )

        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        base.reason = (
            f"Sol-overskud {surplus:.0f}W → "
            f"{phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
        )
        return base

    def _hybrid(self, ctx: EVContext) -> EVOptimizeResult:
        """Solar-first; supplement with cheapest grid hours to reach departure target."""
        # 1. Try solar first
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
            base.reason = (
                f"Batteri prioritet "
                f"(SOC {ctx.battery_soc:.0f}% < {ctx.battery_min_soc:.0f}%)"
            )
            return base

        n_hours = _needed_charge_hours(ctx)
        cheapest = _find_cheapest_charge_hours(
            ctx.raw_prices, ctx.now, ctx.departure, n_hours
        )
        if not cheapest:
            base.reason = "Ingen prisdata tilgængelig for hybrid-planlægning"
            return base

        current_hour = ctx.now.replace(minute=0, second=0, microsecond=0)
        if _normalize_for_compare(current_hour) not in {
            _normalize_for_compare(h) for h in cheapest
        }:
            base.reason = (
                f"Ikke billigste time "
                f"(nuværende {ctx.current_price:.3f} kr/kWh)"
            )
            return base

        # Charge at max rate during cheap grid hour
        _, phases, amps, actual_w = self._calc_phase_and_amps(
            ctx.max_charge_kw * 1000, ctx.max_charge_kw * 1000
        )
        if actual_w == 0.0:
            base.reason = "Max ladeeffekt under minimum"
            return base

        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        base.reason = (
            f"Billig netladning {ctx.current_price:.3f} kr/kWh "
            f"→ {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
        )
        return base

    def _grid_schedule(self, ctx: EVContext) -> EVOptimizeResult:
        """Charge during the cheapest grid hours before departure — no solar preference."""
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
            base.reason = (
                f"Bil fuld ({ctx.vehicle_soc:.0f}% ≥ {ctx.vehicle_target_soc:.0f}%)"
            )
            return base

        if _battery_needs_priority(ctx):
            base.reason = (
                f"Batteri prioritet "
                f"(SOC {ctx.battery_soc:.0f}% < {ctx.battery_min_soc:.0f}%)"
            )
            return base

        n_hours = _needed_charge_hours(ctx)
        cheapest = _find_cheapest_charge_hours(
            ctx.raw_prices, ctx.now, ctx.departure, n_hours
        )
        if not cheapest:
            base.reason = "Ingen prisdata tilgængelig"
            return base

        current_hour = ctx.now.replace(minute=0, second=0, microsecond=0)
        if _normalize_for_compare(current_hour) not in {
            _normalize_for_compare(h) for h in cheapest
        }:
            base.reason = (
                f"Ikke billigste time "
                f"(nuværende {ctx.current_price:.3f} kr/kWh)"
            )
            return base

        _, phases, amps, actual_w = self._calc_phase_and_amps(
            ctx.max_charge_kw * 1000, ctx.max_charge_kw * 1000
        )
        if actual_w == 0.0:
            base.reason = "Max ladeeffekt under minimum"
            return base

        base.should_charge = True
        base.target_w = actual_w
        base.phases = phases
        base.target_amps = amps
        base.reason = (
            f"Planlagt netladning {ctx.current_price:.3f} kr/kWh "
            f"→ {phases}-fase {amps:.1f}A ({actual_w:.0f}W)"
        )
        return base

    def _check_emergency_charging(self, ctx: EVContext) -> EVOptimizeResult | None:
        """Return an emergency charge result if driving range is below minimum.

        Returns None when:
        - min_range_km is 0 (feature disabled)
        - driving_range_km is None (sensor not configured)
        - driving_range_km >= min_range_km (no emergency)
        """
        if ctx.min_range_km <= 0:
            return None
        if ctx.driving_range_km is None:
            return None
        if ctx.driving_range_km >= ctx.min_range_km:
            return None

        needed_soc = min(
            100.0,
            ctx.min_range_km
            / ctx.vehicle_efficiency_km_per_kwh
            / ctx.vehicle_capacity_kwh
            * 100,
        )

        _, phases, amps, actual_w = self._calc_phase_and_amps(
            ctx.max_charge_kw * 1000,
            ctx.max_charge_kw * 1000,
        )
        return EVOptimizeResult(
            should_charge=True,
            target_w=actual_w,
            phases=phases,
            target_amps=amps,
            reason=(
                f"\u26a1 Nødopladning — {ctx.driving_range_km:.0f}km "
                f"< {ctx.min_range_km:.0f}km minimum "
                f"(lader til {needed_soc:.0f}% SOC)"
            ),
            vehicle_soc=ctx.vehicle_soc,
            vehicle_target_soc=needed_soc,
            surplus_w=_surplus_w(ctx),
            charger_status=ctx.charger_status,
            is_emergency=True,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def optimize(self, ctx: EVContext, mode: str = "solar_only") -> EVOptimizeResult:
        """Dispatch to mode-specific optimiser. Falls back to solar_only."""
        # Emergency charging overrides all modes — charge at max regardless of price/solar
        emergency = self._check_emergency_charging(ctx)
        if emergency is not None:
            return emergency

        if mode == "solar_only":
            return self._solar_only(ctx)
        if mode == "hybrid":
            return self._hybrid(ctx)
        if mode == "grid_schedule":
            return self._grid_schedule(ctx)
        _LOGGER.warning("Ukendt EV charge mode: %s — bruger solar_only", mode)
        return self._solar_only(ctx)
