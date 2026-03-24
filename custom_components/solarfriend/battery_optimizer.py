"""SolarFriend BatteryOptimizer — Step 2: dataclass, slot helper, strategy comparison."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, List

from .consumption_profile import ConsumptionProfile
from .forecast_adapter import get_forecast_for_period
from .price_adapter import get_current_price_from_raw

_LOGGER = logging.getLogger(__name__)
LOW_GRID_HOLD_PRICE = 0.10  # kr/kWh: prefer grid over battery wear near zero-price periods


# ---------------------------------------------------------------------------
# OptimizeResult
# ---------------------------------------------------------------------------

@dataclass
class OptimizeResult:
    """Holds the optimizer's recommendation for the current cycle."""

    strategy: str                          # SAVE_SOLAR / USE_BATTERY / CHARGE_GRID / CHARGE_NIGHT / IDLE
    reason: str                            # Forklaring på dansk
    target_soc: Optional[float]            # Mål-SOC % (kun relevant ved CHARGE_NIGHT)
    charge_now: bool                       # Skal vi lade i denne time?
    cheapest_charge_hour: Optional[str]    # "HH:MM"
    night_charge_kwh: float                # Hvor meget skal lades om natten (kWh)
    morning_need_kwh: float                # Energibehov inden solopgang (kWh)
    day_deficit_kwh: float                 # Forventet underskud i løbet af dagen (kWh)
    peak_need_kwh: float                   # Behov i dyreste periode (kWh)
    expected_saving_dkk: float             # Forventet besparelse (kr)
    weighted_battery_cost: float           # Aktuel vægtet batteriomkostning (kr/kWh)
    solar_fraction: float                  # Andel sol i batteriet (0.0–1.0)
    best_discharge_hours: List[str]        # "HH:MM" — timer med højest discharge-value
    solar_sell: bool = True               # False → slå solar-salg til net fra (negativ pris)

    @classmethod
    def idle(
        cls,
        reason: str,
        weighted_cost: float = 0.0,
        solar_fraction: float = 0.0,
    ) -> "OptimizeResult":
        """Return a no-action result."""
        return cls(
            strategy="IDLE",
            reason=reason,
            target_soc=None,
            charge_now=False,
            cheapest_charge_hour=None,
            night_charge_kwh=0.0,
            morning_need_kwh=0.0,
            day_deficit_kwh=0.0,
            peak_need_kwh=0.0,
            expected_saving_dkk=0.0,
            weighted_battery_cost=weighted_cost,
            solar_fraction=solar_fraction,
            best_discharge_hours=[],
            solar_sell=True,
        )


# ---------------------------------------------------------------------------
# _get_future_slots
# ---------------------------------------------------------------------------

def _get_future_slots(
    raw_prices: list[dict[str, Any]],
    from_hour: int,
    to_hour: int,
    profile: ConsumptionProfile,
    is_weekend: bool,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Build a sorted list of hourly slots between from_hour and to_hour.

    Supports wrap-around over midnight (e.g. from_hour=22, to_hour=6).
    Only slots whose start time is strictly in the future are included.
    When raw_prices contains both today and tomorrow entries (e.g. from
    Energi Data Service raw_today + raw_tomorrow), the nearest future
    entry for each hour-of-day wins, preventing same-hour collisions.

    Args:
        raw_prices:  List of {"hour": <datetime | ISO str | int>, "price": float}
        from_hour:   First hour to include (inclusive, 0–23).
        to_hour:     Last hour to include (inclusive, 0–23).
        profile:     ConsumptionProfile for predicted load lookup.
        is_weekend:  Passed to profile.get_predicted_watt().

    Returns:
        List of slot dicts sorted chronologically within the window.
    """
    if now is None:
        now = datetime.now().astimezone()  # timezone-aware local time

    # Normalise `now` to timezone-aware for consistent comparison
    if now.tzinfo is None:
        now = now.astimezone()

    # ── Step 1: parse every entry to a timezone-aware local datetime ──────
    parsed: list[tuple[datetime, float]] = []
    for entry in raw_prices:
        raw_hour = entry.get("hour") if entry.get("hour") is not None else entry.get("start", "")
        price = float(entry.get("price", 0.0))
        slot_dt: datetime | None = None
        try:
            if isinstance(raw_hour, datetime):
                # Already a datetime object — make timezone-aware if needed
                if raw_hour.tzinfo is None:
                    slot_dt = raw_hour.astimezone().replace(minute=0, second=0, microsecond=0)
                else:
                    slot_dt = raw_hour.astimezone().replace(minute=0, second=0, microsecond=0)
            elif isinstance(raw_hour, int):
                # Plain hour int — use today if still future, otherwise tomorrow
                candidate = now.replace(hour=raw_hour % 24, minute=0, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                slot_dt = candidate
            else:
                # ISO string: "2026-03-22T17:00:00+01:00" or with space separator
                parsed_dt = datetime.fromisoformat(str(raw_hour))
                if parsed_dt.tzinfo is None:
                    parsed_dt = parsed_dt.astimezone()
                else:
                    parsed_dt = parsed_dt.astimezone()
                slot_dt = parsed_dt.replace(minute=0, second=0, microsecond=0)
            parsed.append((slot_dt, price))
        except (ValueError, TypeError, AttributeError):
            continue

    # ── Step 2: keep only slots that start after right now ────────────────
    future_parsed = [(dt, price) for dt, price in parsed if dt > now]

    # ── Step 3: build the hour-of-day window (midnight wrap-around) ───────
    if from_hour <= to_hour:
        hour_set: set[int] = set(range(from_hour, to_hour + 1))
    else:
        hour_set = set(range(from_hour, 24)) | set(range(0, to_hour + 1))

    # ── Step 4: for each hour-of-day in window, keep the nearest entry ────
    best_by_hour: dict[int, tuple[datetime, float]] = {}
    for slot_dt, price in future_parsed:
        h = slot_dt.hour
        if h in hour_set:
            if h not in best_by_hour or slot_dt < best_by_hour[h][0]:
                best_by_hour[h] = (slot_dt, price)

    # ── Step 5: build slot dicts ──────────────────────────────────────────
    slots: list[dict[str, Any]] = []
    for hour in hour_set:
        if hour not in best_by_hour:
            continue
        _, price = best_by_hour[hour]
        predicted_watt = profile.get_predicted_watt(hour, is_weekend)
        predicted_kwh = predicted_watt / 1000.0  # one hourly slot → kWh
        slots.append(
            {
                "hour": hour,
                "hour_str": f"{hour:02d}:00",
                "price": price,
                "predicted_watt": predicted_watt,
                "predicted_kwh": predicted_kwh,
                "value_per_kwh": 0.0,  # filled in by optimizer
            }
        )

    # Sort chronologically: wrapped hours (0…to_hour) sort after late hours
    if from_hour > to_hour:
        slots.sort(key=lambda s: s["hour"] if s["hour"] >= from_hour else s["hour"] + 24)
    else:
        slots.sort(key=lambda s: s["hour"])

    return slots


# ---------------------------------------------------------------------------
# _calculate_cheapest_charge
# ---------------------------------------------------------------------------

def _calculate_cheapest_charge(
    need_kwh: float,
    night_slots: list[dict[str, Any]],
    charge_rate_kw: float,
    battery_cost_per_kwh: float,
) -> dict[str, Any]:
    """Find the cheapest night slots that cover need_kwh.

    Charge slid = battery_cost_per_kwh / 2 (only the charging half of a full cycle).

    Returns:
        total_kwh       — actual kWh that can be charged (≤ need_kwh if slots are scarce)
        total_cost_dkk  — total cost including charge slid
        avg_price       — weighted average grid price (without slid)
        charge_slots    — selected slots [{hour_str, price, kwh}]
        feasible        — True if need_kwh is fully covered
    """
    if not night_slots or need_kwh <= 0:
        return {"total_kwh": 0.0, "total_cost_dkk": 0.0, "avg_price": 0.0,
                "charge_slots": [], "feasible": need_kwh <= 0}

    charge_slid = battery_cost_per_kwh / 2
    kwh_per_slot = charge_rate_kw * 1.0  # hourly slot → kWh

    sorted_slots = sorted(night_slots, key=lambda s: s["price"])

    charge_slots: list[dict[str, Any]] = []
    remaining = need_kwh
    total_kwh = 0.0
    total_cost = 0.0

    for slot in sorted_slots:
        if remaining <= 0:
            break
        kwh = min(kwh_per_slot, remaining)
        cost = kwh * (slot["price"] + charge_slid)
        charge_slots.append({"hour_str": slot["hour_str"], "price": slot["price"], "kwh": kwh})
        total_kwh += kwh
        total_cost += cost
        remaining -= kwh

    avg_price = (
        sum(s["price"] * s["kwh"] for s in charge_slots) / total_kwh
        if total_kwh > 0 else 0.0
    )

    return {
        "total_kwh": round(total_kwh, 4),
        "total_cost_dkk": round(total_cost, 4),
        "avg_price": round(avg_price, 4),
        "charge_slots": charge_slots,
        "feasible": remaining <= 1e-9,
    }


# ---------------------------------------------------------------------------
# _find_best_discharge_hours
# ---------------------------------------------------------------------------

def _find_best_discharge_hours(
    future_slots: list[dict[str, Any]],
    available_kwh: float,
    weighted_battery_cost: float,
    min_saving: float,
) -> list[dict[str, Any]]:
    """Allocate available_kwh to the highest-value slots above min_saving.

    value = slot price − weighted_battery_cost.

    Returns:
        List of slot dicts (sorted by value descending) with:
            value          — kr/kWh gained vs buying from grid
            allocated_kwh  — kWh assigned to this slot
    """
    candidates = []
    for slot in future_slots:
        value = slot["price"] - weighted_battery_cost
        if value > min_saving:
            candidates.append({**slot, "value": round(value, 4), "allocated_kwh": 0.0})

    candidates.sort(key=lambda s: s["value"], reverse=True)

    remaining = available_kwh
    result = []
    for slot in candidates:
        if remaining <= 0:
            break
        allocated = min(slot["predicted_kwh"], remaining)
        if allocated > 0:
            result.append({**slot, "allocated_kwh": round(allocated, 4)})
            remaining -= allocated

    return result


# ---------------------------------------------------------------------------
# _compare_strategies
# ---------------------------------------------------------------------------

def _compare_strategies(
    available_kwh: float,
    future_slots: list[dict[str, Any]],
    night_slots: list[dict[str, Any]],
    weighted_battery_cost: float,
    charge_rate_kw: float,
    battery_cost_per_kwh: float,
    min_charge_saving: float,
) -> dict[str, Any]:
    """Compare three discharge/charge strategies and return the most profitable.

    Strategy A — Gem batteri til dyre perioder:
        Discharge only to slots where value > min_charge_saving.
        No night charging. net_gain_a = saving_a.

    Strategy B — Brug alt nu + lad billigt om natten:
        Discharge to ALL positive-value slots (min_saving = 0).
        Then charge back cheaply at night if rentable.
        net_gain_b = saving_b − charge_cost  (if rentable)
                   = saving_b               (if not rentable)

    Strategy C — Spar til morgen:
        No discharge now. Value = opportunity to use battery at the best future
        slot prices (same future_slots, treated as tomorrow's use case).
        net_gain_c = saving_a  (same calculation, deferred use).
    """
    # ── Strategy A ──────────────────────────────────────────────────────────
    best_hours_a = _find_best_discharge_hours(
        future_slots, available_kwh, weighted_battery_cost, min_charge_saving
    )
    saving_a = sum(s["allocated_kwh"] * s["value"] for s in best_hours_a)
    net_gain_a = saving_a

    # ── Strategy B ──────────────────────────────────────────────────────────
    best_hours_b = _find_best_discharge_hours(
        future_slots, available_kwh, weighted_battery_cost, 0.0  # no threshold
    )
    saving_b = sum(s["allocated_kwh"] * s["value"] for s in best_hours_b)
    need_kwh_b = sum(s["allocated_kwh"] for s in best_hours_b)

    charge_result = _calculate_cheapest_charge(
        need_kwh_b, night_slots, charge_rate_kw, battery_cost_per_kwh
    )

    peak_price = max((s["price"] for s in future_slots), default=0.0)
    is_rentable = (
        charge_result["feasible"]
        and charge_result["avg_price"] + battery_cost_per_kwh < peak_price - min_charge_saving
    )

    if is_rentable:
        net_gain_b = saving_b - charge_result["total_cost_dkk"]
        should_charge_night = True
    else:
        net_gain_b = saving_b
        should_charge_night = False

    # ── Strategy C ──────────────────────────────────────────────────────────
    # Treat future_slots as tomorrow's opportunity — same value, deferred.
    net_gain_c = saving_a  # identical math, just not acted on today

    # ── Pick winner ─────────────────────────────────────────────────────────
    gains = {"A": net_gain_a, "B": net_gain_b, "C": net_gain_c}
    best = max(gains, key=lambda k: gains[k])

    if best == "B":
        discharge_slots = best_hours_b
        night_kwh = charge_result["total_kwh"] if should_charge_night else 0.0
        night_charge_slots = charge_result["charge_slots"] if should_charge_night else []
    elif best == "A":
        discharge_slots = best_hours_a
        should_charge_night = False
        night_kwh = 0.0
        night_charge_slots = []
    else:  # C — save for later, no immediate action
        discharge_slots = []
        should_charge_night = False
        night_kwh = 0.0
        night_charge_slots = []

    return {
        "best_strategy": best,
        "net_gain_dkk": round(gains[best], 4),
        "discharge_slots": discharge_slots,
        "should_charge_night": should_charge_night,
        "night_charge_kwh": round(night_kwh, 4),
        "night_charge_slots": night_charge_slots,
        "strategy_details": {
            "gain_a": round(net_gain_a, 4),
            "gain_b": round(net_gain_b, 4),
            "gain_c": round(net_gain_c, 4),
        },
    }


# ---------------------------------------------------------------------------
# BatteryOptimizer
# ---------------------------------------------------------------------------

class BatteryOptimizer:
    """High-level optimizer: wraps strategy helpers with day/night planning."""

    def __init__(
        self,
        config_entry: Any,
        battery_tracker: Any,
        consumption_profile: Any,
    ) -> None:
        self._config_entry = config_entry
        self._tracker = battery_tracker
        self._profile = consumption_profile

        cfg = config_entry.data
        self.battery_capacity_kwh: float = float(cfg.get("battery_capacity_kwh", 10.0))
        self.battery_min_soc: float = float(cfg.get("battery_min_soc", 10.0))
        self.battery_max_soc: float = float(cfg.get("battery_max_soc", 100.0))
        self.charge_rate_kw: float = float(cfg.get("charge_rate_kw", 6.0))
        self.battery_cost_per_kwh: float = float(cfg.get("battery_cost_per_kwh", 0.20))
        self.min_charge_saving: float = float(cfg.get("min_charge_saving", 0.10))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _price_for_hour(raw_prices: list[dict[str, Any]], hour: int) -> "float | None":
        """Return the grid price for *hour* (0–23) from raw_prices, or None."""
        for entry in raw_prices:
            raw_hour = entry.get("hour", "")
            try:
                if isinstance(raw_hour, int):
                    h = raw_hour % 24
                else:
                    time_part = str(raw_hour).split("T")[1] if "T" in str(raw_hour) else str(raw_hour)
                    h = int(time_part[:2])
                if h == hour:
                    return float(entry.get("price", 0.0))
            except (ValueError, IndexError):
                continue
        return None

    # ------------------------------------------------------------------
    # Day planner
    # ------------------------------------------------------------------

    def _plan_day(
        self,
        now: "datetime",
        available_kwh: float,
        weighted_cost: float,
        solar_fraction: float,
        raw_prices: list[dict[str, Any]],
        forecast_today_kwh: float,
        is_weekend: bool,
        pv_power: float,
        load_power: float,
        sunset_time: "datetime",
        sunrise_time: "datetime",
        hourly_forecast: list | None = None,
    ) -> OptimizeResult:
        """Return a strategy for daytime hours (between sunrise and sunset)."""

        # Precompute precise solar forecasts when hourly data is available
        if hourly_forecast:
            solar_remaining = get_forecast_for_period(hourly_forecast, now, sunset_time)
            solar_next_2h = get_forecast_for_period(
                hourly_forecast, now, now + timedelta(hours=2)
            )
            _LOGGER.debug(
                "Optimizer solar: remaining=%.1f kWh next2h=%.1f kWh tomorrow_morning=%.1f kWh",
                solar_remaining, solar_next_2h, 0.0,
            )
        else:
            solar_remaining = forecast_today_kwh
            solar_next_2h = 0.0

        # TRIN 0 — SELL_BATTERY: sælg kun ægte overskud når sol forventes at genoplade bagefter
        current_price = self._price_for_hour(raw_prices, now.hour)
        if current_price is not None:
            reserve_buffer_kwh = 0.5
            cur = now.replace(minute=0, second=0, microsecond=0)

            load_until_sunset_kwh = 0.0
            while cur < sunset_time:
                load_until_sunset_kwh += self._profile.get_predicted_watt(cur.hour, is_weekend) / 1000.0
                cur += timedelta(hours=1)

            # If solar already covers the house, keep requirement is zero.
            if pv_power >= load_power:
                load_until_solar_kwh = 0.0
            elif hourly_forecast and solar_next_2h > 0:
                next_2h_load_kwh = 0.0
                cur = now.replace(minute=0, second=0, microsecond=0)
                for _ in range(2):
                    next_2h_load_kwh += self._profile.get_predicted_watt(cur.hour, is_weekend) / 1000.0
                    cur += timedelta(hours=1)
                load_until_solar_kwh = max(0.0, next_2h_load_kwh - solar_next_2h)
            else:
                solar_start = sunrise_time + timedelta(hours=1)
                load_until_solar_kwh = 0.0
                cur = now.replace(minute=0, second=0, microsecond=0)
                while cur < solar_start:
                    load_until_solar_kwh += self._profile.get_predicted_watt(cur.hour, is_weekend) / 1000.0
                    cur += timedelta(hours=1)

            future_recharge_kwh = max(0.0, solar_remaining - load_until_sunset_kwh)
            sellable_kwh = min(
                available_kwh,
                max(0.0, available_kwh + future_recharge_kwh - load_until_solar_kwh - reserve_buffer_kwh),
            )
            net_gain = sellable_kwh * (current_price - weighted_cost)

            _LOGGER.debug(
                "SELL_BATTERY vurdering: sellable=%.1f kWh "
                "load_until_solar=%.1f kWh future_recharge=%.1f kWh "
                "weighted_cost=%.2f net_gain=%.2f kr",
                sellable_kwh, load_until_solar_kwh, future_recharge_kwh,
                weighted_cost, net_gain,
            )

            if (sellable_kwh > 0.5
                    and future_recharge_kwh > 0.5
                    and net_gain > self.min_charge_saving
                    and current_price > weighted_cost + self.min_charge_saving):
                return OptimizeResult(
                    strategy="SELL_BATTERY",
                    reason=(
                        f"Sælger {sellable_kwh:.1f} kWh til {current_price:.2f} kr — "
                        f"behov til sol: {load_until_solar_kwh:.1f} kWh — "
                        f"forventet genladning {future_recharge_kwh:.1f} kWh — "
                        f"gevinst {net_gain:.2f} kr"
                    ),
                    target_soc=float(self.battery_min_soc),
                    charge_now=False,
                    cheapest_charge_hour=None,
                    night_charge_kwh=0.0,
                    morning_need_kwh=0.0,
                    day_deficit_kwh=0.0,
                    peak_need_kwh=0.0,
                    expected_saving_dkk=round(net_gain, 4),
                    weighted_battery_cost=weighted_cost,
                    solar_fraction=solar_fraction,
                    best_discharge_hours=[],
                )

        # TRIN 1 — Sol-overskud (realtid eller prognose)?
        surplus = pv_power - load_power
        if surplus > 50:
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=f"Solcelleoverskud {surplus:.0f}W — gemmer i batteri",
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=0.0,
                morning_need_kwh=0.0,
                day_deficit_kwh=0.0,
                peak_need_kwh=0.0,
                expected_saving_dkk=0.0,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=[],
            )

        if hourly_forecast and solar_next_2h > 1.0:
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=f"Solprognose {solar_next_2h:.1f} kWh næste 2 timer — gemmer batteri til sol",
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=0.0,
                morning_need_kwh=0.0,
                day_deficit_kwh=0.0,
                peak_need_kwh=0.0,
                expected_saving_dkk=0.0,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=[],
            )

        # TRIN 2 — Fremtidige slots (nu → midnat)
        future_slots = _get_future_slots(raw_prices, now.hour, 23, self._profile, is_weekend, now)
        usable_capacity_kwh = (
            self.battery_capacity_kwh * (self.battery_max_soc - self.battery_min_soc) / 100
        )
        battery_has_headroom = available_kwh < usable_capacity_kwh * 0.9

        # TRIN 3 — Billig netstrøm: hold batteriet tilbage og tag udsving fra nettet
        if (
            current_price is not None
            and current_price <= max(weighted_cost, LOW_GRID_HOLD_PRICE)
            and battery_has_headroom
        ):
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=(
                    f"Billig netstrøm ({current_price:.2f} kr) — "
                    f"gemmer batteri og tager udsving fra nettet"
                ),
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=0.0,
                morning_need_kwh=0.0,
                day_deficit_kwh=0.0,
                peak_need_kwh=0.0,
                expected_saving_dkk=0.0,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=[],
            )

        # TRIN 4 — Sammenlign strategier A / B / C
        night_slots = _get_future_slots(raw_prices, 22, 6, self._profile, is_weekend, now)
        result = _compare_strategies(
            available_kwh=available_kwh,
            future_slots=future_slots,
            night_slots=night_slots,
            weighted_battery_cost=weighted_cost,
            charge_rate_kw=self.charge_rate_kw,
            battery_cost_per_kwh=self.battery_cost_per_kwh,
            min_charge_saving=self.min_charge_saving,
        )

        # TRIN 5 — Er vi i en af de bedste discharge-timer NU?
        current_hour_str = now.strftime("%H:00")
        is_best_hour = current_hour_str in [s["hour_str"] for s in result["discharge_slots"]]

        if is_best_hour and available_kwh > 0.5:
            return OptimizeResult(
                strategy="USE_BATTERY",
                reason=(
                    f"Top {len(result['discharge_slots'])} dyreste timer — "
                    f"batteri sparer {result['net_gain_dkk']:.2f} kr"
                ),
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=0.0,
                morning_need_kwh=0.0,
                day_deficit_kwh=0.0,
                peak_need_kwh=0.0,
                expected_saving_dkk=result["net_gain_dkk"],
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=[s["hour_str"] for s in result["discharge_slots"]],
            )

        # TRIN 6 — Billig pris (bund 25%) AND lav sol AND batteri ikke fuld?
        today_future: list[dict[str, Any]] = []
        for entry in raw_prices:
            raw_hour = entry.get("hour", "")
            try:
                if isinstance(raw_hour, int):
                    h = raw_hour % 24
                else:
                    time_part = str(raw_hour).split("T")[1] if "T" in str(raw_hour) else str(raw_hour)
                    h = int(time_part[:2])
                if h >= now.hour:
                    today_future.append({"hour": h, "price": float(entry.get("price", 0.0))})
            except (ValueError, IndexError):
                continue

        if today_future and current_price is not None:
            sorted_prices = sorted(today_future, key=lambda e: e["price"])
            cheap_threshold = sorted_prices[len(sorted_prices) // 4]["price"]

            low_solar = solar_remaining < self.battery_capacity_kwh * 0.3
            not_full = battery_has_headroom

            if current_price <= cheap_threshold and low_solar and not_full:
                return OptimizeResult(
                    strategy="CHARGE_GRID",
                    reason=(
                        f"Lav sol-forecast ({solar_remaining:.1f} kWh frem til solnedgang) og "
                        f"billig strøm ({current_price:.2f} kr)"
                    ),
                    target_soc=None,
                    charge_now=True,
                    cheapest_charge_hour=current_hour_str,
                    night_charge_kwh=0.0,
                    morning_need_kwh=0.0,
                    day_deficit_kwh=0.0,
                    peak_need_kwh=0.0,
                    expected_saving_dkk=0.0,
                    weighted_battery_cost=weighted_cost,
                    solar_fraction=solar_fraction,
                    best_discharge_hours=[],
                )

        # TRIN 7 — IDLE
        return OptimizeResult.idle(
            reason="Ingen optimal handling — venter på bedre pris eller sol",
            weighted_cost=weighted_cost,
            solar_fraction=solar_fraction,
        )

    # ------------------------------------------------------------------
    # Night planner
    # ------------------------------------------------------------------

    def _plan_night(
        self,
        now: "datetime",
        available_kwh: float,
        weighted_cost: float,
        solar_fraction: float,
        raw_prices: list[dict[str, Any]],
        forecast_tomorrow_kwh: float,
        sunrise_time: "datetime",
        sunset_time: "datetime",
        is_weekend: bool,
        hourly_forecast: list | None = None,
    ) -> OptimizeResult:
        """Return a strategy for nighttime hours (between sunset and sunrise)."""

        # TRIN 0 — Aftenafladning: brug batteri over morgenbehov til dyre aftentimer
        # Morgenbehov estimeres konservativt; kun overskud bruges til aftenafladning.
        morning_slots_pre = _get_future_slots(
            raw_prices, now.hour, sunrise_time.hour, self._profile, is_weekend, now
        )
        morning_reserve_kwh = sum(s["predicted_kwh"] for s in morning_slots_pre) * 1.10
        available_for_evening = max(0.0, available_kwh - morning_reserve_kwh)

        if available_for_evening > 0.3:
            evening_slots = _get_future_slots(
                raw_prices, now.hour, 23, self._profile, is_weekend, now
            )
            evening_discharge = _find_best_discharge_hours(
                evening_slots, available_for_evening, weighted_cost, self.min_charge_saving
            )
            current_hour_str = now.strftime("%H:00")
            if current_hour_str in [s["hour_str"] for s in evening_discharge]:
                return OptimizeResult(
                    strategy="USE_BATTERY",
                    reason=(
                        f"Aftenpeak — bruger {available_for_evening:.1f} kWh over "
                        f"morgenbehov ({morning_reserve_kwh:.1f} kWh) "
                        f"til dyre timer: {[s['hour_str'] for s in evening_discharge]}"
                    ),
                    target_soc=None,
                    charge_now=False,
                    cheapest_charge_hour=None,
                    night_charge_kwh=0.0,
                    morning_need_kwh=round(morning_reserve_kwh, 4),
                    day_deficit_kwh=0.0,
                    peak_need_kwh=0.0,
                    expected_saving_dkk=round(
                        sum(s["allocated_kwh"] * s["value"] for s in evening_discharge), 4
                    ),
                    weighted_battery_cost=weighted_cost,
                    solar_fraction=solar_fraction,
                    best_discharge_hours=[s["hour_str"] for s in evening_discharge],
                )

        # TRIN 1 — Forbrug inden solopgang
        morning_slots = _get_future_slots(
            raw_prices, now.hour, sunrise_time.hour, self._profile, is_weekend, now
        )
        morning_need_kwh = sum(s["predicted_kwh"] for s in morning_slots) * 1.10

        # TRIN 3 — Forventet sol-produktion og dagunderskud
        usable_capacity_kwh = (
            self.battery_capacity_kwh * (self.battery_max_soc - self.battery_min_soc) / 100
        )

        if hourly_forecast:
            # Sol fra solopgang til middag (næste 6 timer efter solopgang)
            tomorrow_morning_solar = get_forecast_for_period(
                hourly_forecast, sunrise_time, sunrise_time + timedelta(hours=6)
            )
            # Total sol i morgen (fra solopgang til næste dags solnedgang)
            tomorrow_solar = get_forecast_for_period(
                hourly_forecast, sunrise_time, sunset_time + timedelta(hours=12)
            )
            expected_solar_charge = min(tomorrow_solar, usable_capacity_kwh)
            _LOGGER.debug(
                "Optimizer solar: remaining=%.1f kWh next2h=%.1f kWh tomorrow_morning=%.1f kWh",
                tomorrow_solar, 0.0, tomorrow_morning_solar,
            )
        else:
            tomorrow_morning_solar = 0.0
            expected_solar_charge = min(forecast_tomorrow_kwh, usable_capacity_kwh)

        day_slots = _get_future_slots(
            raw_prices, sunrise_time.hour, 22, self._profile, is_weekend, now
        )
        day_consumption = sum(s["predicted_kwh"] for s in day_slots)
        day_deficit_kwh = max(0.0, day_consumption - expected_solar_charge)

        # TRIN 4 — Peak-periode (top 4 tidslots sorteret efter pris × forbrug)
        all_tomorrow_slots = _get_future_slots(
            raw_prices, sunrise_time.hour, 23, self._profile, is_weekend, now
        )
        peak_slots = sorted(
            all_tomorrow_slots,
            key=lambda s: s["price"] * s["predicted_kwh"],
            reverse=True,
        )
        peak_need_kwh = sum(s["predicted_kwh"] for s in peak_slots[:4])

        # TRIN 5 — Total behov og underskud
        total_need_kwh = morning_need_kwh + day_deficit_kwh
        deficit = max(0.0, total_need_kwh - available_kwh)

        if deficit <= 0:
            return OptimizeResult.idle(
                reason=(
                    f"Batteri dækker morgen ({morning_need_kwh:.1f} kWh) "
                    "— ingen opladning nødvendig"
                ),
                weighted_cost=weighted_cost,
                solar_fraction=solar_fraction,
            )

        # TRIN 6 — Er nat-opladning rentabel?
        night_slots = _get_future_slots(
            raw_prices, now.hour, sunrise_time.hour, self._profile, is_weekend, now
        )
        charge_result = _calculate_cheapest_charge(
            deficit, night_slots, self.charge_rate_kw, self.battery_cost_per_kwh
        )

        _LOGGER.debug(
            "Night plan: morning_need=%.2f available=%.2f "
            "deficit=%.2f night_slots=%d charge_result=%s",
            morning_need_kwh,
            available_kwh,
            deficit,
            len(night_slots),
            charge_result,
        )

        peak_price = max((s["price"] for s in all_tomorrow_slots), default=0.0)
        saving_per_kwh = peak_price - charge_result["avg_price"] - self.battery_cost_per_kwh

        if saving_per_kwh < self.min_charge_saving:
            return OptimizeResult.idle(
                reason=(
                    f"Nat-opladning ikke rentabel — besparelse {saving_per_kwh:.2f} kr/kWh "
                    f"under grænse {self.min_charge_saving:.2f}"
                ),
                weighted_cost=weighted_cost,
                solar_fraction=solar_fraction,
            )

        # TRIN 7 — Skal vi lade NU?
        current_hour_str = now.strftime("%H:00")
        charge_now = current_hour_str in [s["hour_str"] for s in charge_result["charge_slots"]]
        cheapest_hour = (
            charge_result["charge_slots"][0]["hour_str"]
            if charge_result["charge_slots"]
            else None
        )
        target_soc = min(
            self.battery_max_soc,
            self.battery_min_soc + (total_need_kwh / self.battery_capacity_kwh * 100) + 5,
        )

        return OptimizeResult(
            strategy="CHARGE_NIGHT",
            reason=(
                f"Lader {deficit:.1f} kWh til {charge_result['avg_price']:.2f} kr "
                f"— sparer {saving_per_kwh * deficit:.2f} kr vs. peak {peak_price:.2f} kr"
            ),
            target_soc=round(target_soc, 1),
            charge_now=charge_now,
            cheapest_charge_hour=cheapest_hour,
            night_charge_kwh=round(deficit, 4),
            morning_need_kwh=round(morning_need_kwh, 4),
            day_deficit_kwh=round(day_deficit_kwh, 4),
            peak_need_kwh=round(peak_need_kwh, 4),
            expected_saving_dkk=round(saving_per_kwh * deficit, 4),
            weighted_battery_cost=weighted_cost,
            solar_fraction=solar_fraction,
            best_discharge_hours=[],
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def optimize(
        self,
        now: "datetime",
        pv_power: float,
        load_power: float,
        current_soc: float,
        raw_prices: list[dict[str, Any]],
        forecast_today_kwh: float,
        forecast_tomorrow_kwh: float,
        sunrise_time: "datetime",
        sunset_time: "datetime",
        is_weekend: bool,
        hourly_forecast: list | None = None,
    ) -> OptimizeResult:
        """Main entry point called by the coordinator.

        Routes to _plan_day or _plan_night based on sun position, then returns
        an OptimizeResult describing the recommended action for this cycle.
        """
        available_kwh = max(
            0.0,
            (current_soc - self.battery_min_soc) / 100.0 * self.battery_capacity_kwh,
        )
        weighted_cost = self._tracker.weighted_cost
        solar_fraction = self._tracker.solar_fraction

        # ── Anti-eksport: negativ/nul spotpris ────────────────────────────
        current_price = get_current_price_from_raw(raw_prices, now, fallback=0.0) or 0.0

        if current_price <= 0 and raw_prices:
            return OptimizeResult(
                strategy="ANTI_EXPORT",
                reason=f"Negativ/nul pris ({current_price:.4f} kr/kWh) — solar sell OFF",
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=0.0,
                morning_need_kwh=0.0,
                day_deficit_kwh=0.0,
                peak_need_kwh=0.0,
                expected_saving_dkk=0.0,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=[],
                solar_sell=False,
            )

        is_night = (
            now.time() < sunrise_time.time() or now.time() > sunset_time.time()
        )

        if not raw_prices:
            return OptimizeResult.idle(
                "Ingen prisdata tilgængelig",
                weighted_cost=weighted_cost,
                solar_fraction=solar_fraction,
            )

        if is_night:
            return self._plan_night(
                now, available_kwh, weighted_cost, solar_fraction,
                raw_prices, forecast_tomorrow_kwh, sunrise_time, sunset_time, is_weekend,
                hourly_forecast=hourly_forecast,
            )
        return self._plan_day(
            now, available_kwh, weighted_cost, solar_fraction,
            raw_prices, forecast_today_kwh, is_weekend, pv_power, load_power,
            sunset_time, sunrise_time, hourly_forecast=hourly_forecast,
        )


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _dt = datetime  # module-level import

    class _MockEntry:
        def __init__(self, data: dict) -> None:
            self.data = data

    class _MockTracker:
        weighted_cost: float = 0.20
        solar_fraction: float = 0.5

    class _MockProfile:
        def get_predicted_watt(self, hour: int, is_weekend: bool) -> float:
            return 500.0  # 500 W constant load

    _CONFIG = {
        "battery_capacity_kwh": 10.0,
        "battery_min_soc": 10,
        "battery_max_soc": 100,
        "charge_rate_kw": 6.0,
        "battery_cost_per_kwh": 0.20,
        "min_charge_saving": 0.10,
    }

    _SUNRISE = _dt(2026, 3, 22, 6, 30, 0)
    _SUNSET  = _dt(2026, 3, 22, 19, 0, 0)

    # 24 timers prisliste: 0.38 kr om natten (22–06), 1.38 kr om dagen (07–21)
    _PRICES = [
        {"hour": h, "price": 1.38 if 7 <= h < 22 else 0.38}
        for h in range(24)
    ]

    _opt = BatteryOptimizer(_MockEntry(_CONFIG), _MockTracker(), _MockProfile())

    # --- Scenario 1: Dag, sol-overskud → SAVE_SOLAR ---
    r1 = _opt.optimize(
        now=_dt(2026, 3, 22, 12, 0, 0),
        pv_power=5000, load_power=800, current_soc=60,
        raw_prices=_PRICES, forecast_today_kwh=15.0, forecast_tomorrow_kwh=15.0,
        sunrise_time=_SUNRISE, sunset_time=_SUNSET, is_weekend=False,
    )
    assert r1.strategy == "SAVE_SOLAR", f"Scenario 1 fejlede: {r1.strategy}"
    print(f"Scenario 1 OK  strategy={r1.strategy!r:12}  reason='{r1.reason}'")

    # --- Scenario 2: Nat, underskud, rentabel opladning → CHARGE_NIGHT ---
    r2 = _opt.optimize(
        now=_dt(2026, 3, 22, 22, 0, 0),
        pv_power=0, load_power=300, current_soc=20,
        raw_prices=_PRICES, forecast_today_kwh=0.0, forecast_tomorrow_kwh=2.0,
        sunrise_time=_SUNRISE, sunset_time=_SUNSET, is_weekend=False,
    )
    assert r2.strategy == "CHARGE_NIGHT", f"Scenario 2 fejlede: {r2.strategy}"
    assert r2.charge_now is True, f"Scenario 2: charge_now={r2.charge_now}"
    print(f"Scenario 2 OK  strategy={r2.strategy!r:14}  charge_now={r2.charge_now}  reason='{r2.reason}'")

    # --- Scenario 3: Nat, nok batteri → IDLE ---
    r3 = _opt.optimize(
        now=_dt(2026, 3, 22, 22, 0, 0),
        pv_power=0, load_power=300, current_soc=80,
        raw_prices=_PRICES, forecast_today_kwh=0.0, forecast_tomorrow_kwh=30.0,
        sunrise_time=_SUNRISE, sunset_time=_SUNSET, is_weekend=False,
    )
    assert r3.strategy == "IDLE", f"Scenario 3 fejlede: {r3.strategy}"
    print(f"Scenario 3 OK  strategy={r3.strategy!r:7}  reason='{r3.reason}'")

    # --- Scenario 4: Dag, dyr time, nok batteri → USE_BATTERY ---
    r4 = _opt.optimize(
        now=_dt(2026, 3, 22, 14, 0, 0),
        pv_power=0, load_power=800, current_soc=70,
        raw_prices=_PRICES, forecast_today_kwh=1.0, forecast_tomorrow_kwh=15.0,
        sunrise_time=_SUNRISE, sunset_time=_SUNSET, is_weekend=False,
    )
    assert r4.strategy == "USE_BATTERY", f"Scenario 4 fejlede: {r4.strategy}"
    print(f"Scenario 4 OK  strategy={r4.strategy!r:13}  reason='{r4.reason}'")

    print("\nAlle 4 test-scenarier bestået!")
