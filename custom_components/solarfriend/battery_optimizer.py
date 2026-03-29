"""SolarFriend battery optimizer based on a full known-price horizon."""
from __future__ import annotations

import logging
from functools import lru_cache
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional, List

from .forecast_adapter import get_forecast_for_period
from .price_adapter import get_current_price_from_raw

_LOGGER = logging.getLogger(__name__)
LOW_GRID_HOLD_PRICE = 0.10  # kr/kWh: prefer grid over battery wear near zero-price periods
ALLOWED_DISCHARGE_SOLAR_THRESHOLD_W = 2000.0


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
    allowed_discharge_slots: List[dict[str, Any]] = field(default_factory=list)
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
            allowed_discharge_slots=[],
            solar_sell=True,
        )


# ---------------------------------------------------------------------------
# BatteryOptimizer
# ---------------------------------------------------------------------------

class BatteryOptimizer:
    """Horizon-based battery optimizer."""

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
        self.cheap_grid_threshold: float = float(cfg.get("cheap_grid_threshold", LOW_GRID_HOLD_PRICE))
        self._last_plan: list[dict[str, Any]] = []

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

    @staticmethod
    def _normalize_datetime(value: datetime, reference: datetime) -> datetime:
        """Normalise datetimes for comparison while preserving test-friendly naive inputs."""
        if reference.tzinfo is None:
            return value.replace(tzinfo=None) if value.tzinfo is not None else value
        if value.tzinfo is None:
            return value.replace(tzinfo=reference.tzinfo)
        return value.astimezone(reference.tzinfo)

    def _build_price_horizon(
        self,
        raw_prices: list[dict[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Return chronological hourly price slots from the current hour onward."""
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        by_start: dict[datetime, float] = {}

        for entry in raw_prices:
            raw_hour = entry.get("hour") if entry.get("hour") is not None else entry.get("start")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_hour is None or raw_price is None:
                continue
            try:
                if isinstance(raw_hour, datetime):
                    slot_start = self._normalize_datetime(raw_hour, now)
                elif isinstance(raw_hour, int):
                    slot_start = current_hour.replace(hour=raw_hour % 24)
                    if slot_start < current_hour:
                        slot_start += timedelta(days=1)
                else:
                    slot_start = self._normalize_datetime(datetime.fromisoformat(str(raw_hour)), now)
                slot_start = slot_start.replace(minute=0, second=0, microsecond=0)
                if slot_start < current_hour:
                    continue
                by_start[slot_start] = float(raw_price)
            except (ValueError, TypeError, AttributeError):
                continue

        return [
            {"start": start, "price": price}
            for start, price in sorted(by_start.items(), key=lambda item: item[0])
        ]

    @staticmethod
    def _has_prices_beyond_next_midnight(
        raw_prices: list[dict[str, Any]],
        now: datetime,
    ) -> bool:
        """Return True when the known price horizon reaches beyond the next midnight."""
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        next_midnight = (current_hour + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        for entry in raw_prices:
            raw_hour = entry.get("hour") if entry.get("hour") is not None else entry.get("start")
            if raw_hour is None:
                continue
            try:
                slot_start = raw_hour if isinstance(raw_hour, datetime) else datetime.fromisoformat(str(raw_hour))
                slot_start = BatteryOptimizer._normalize_datetime(slot_start, now).replace(
                    minute=0, second=0, microsecond=0
                )
            except (ValueError, TypeError, AttributeError):
                continue
            if slot_start >= next_midnight:
                return True
        return False

    def _build_forecast_map(
        self,
        hourly_forecast: list | None,
        now: datetime,
        reserved_solar_kwh: dict[datetime, float] | None = None,
    ) -> dict[datetime, float]:
        """Aggregate forecast entries by local hour."""
        forecast_by_hour: dict[datetime, float] = {}
        if not hourly_forecast:
            return forecast_by_hour

        for entry in hourly_forecast:
            raw_start = entry.get("period_start")
            if raw_start is None:
                continue
            try:
                slot_start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                slot_start = self._normalize_datetime(slot_start, now).replace(
                    minute=0, second=0, microsecond=0
                )
            except (ValueError, TypeError, AttributeError):
                continue
            forecast_by_hour[slot_start] = forecast_by_hour.get(slot_start, 0.0) + float(
                entry.get("pv_estimate_kwh", 0.0)
            )

        if reserved_solar_kwh:
            for slot_start, reserved_kwh in reserved_solar_kwh.items():
                normalized_start = self._normalize_datetime(slot_start, now).replace(
                    minute=0, second=0, microsecond=0
                )
                if normalized_start in forecast_by_hour:
                    forecast_by_hour[normalized_start] = max(
                        0.0,
                        forecast_by_hour[normalized_start] - float(reserved_kwh),
                    )

        return forecast_by_hour

    def _build_horizon_plan(
        self,
        *,
        now: datetime,
        current_soc: float,
        raw_prices: list[dict[str, Any]],
        weighted_cost: float,
        hourly_forecast: list | None,
        allow_battery_export: bool = True,
        reserved_solar_kwh: dict[datetime, float] | None = None,
        raw_sell_prices: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Build a battery plan over the full known price horizon."""
        horizon = self._build_price_horizon(raw_prices, now)
        if not horizon:
            return []
        sell_horizon = self._build_price_horizon(raw_sell_prices or raw_prices, now)
        sell_price_by_start = {
            item["start"]: float(item["price"]) for item in sell_horizon
        }

        usable_capacity_kwh = (
            self.battery_capacity_kwh * (self.battery_max_soc - self.battery_min_soc) / 100.0
        )
        available_kwh = max(
            0.0,
            (current_soc - self.battery_min_soc) / 100.0 * self.battery_capacity_kwh,
        )
        forecast_by_hour = self._build_forecast_map(hourly_forecast, now, reserved_solar_kwh)

        slots: list[dict[str, Any]] = []
        for item in horizon:
            start = item["start"]
            load_w = float(self._profile.get_predicted_watt(start.hour, start.weekday() >= 5))
            load_kwh = load_w / 1000.0
            solar_kwh = forecast_by_hour.get(start, 0.0)
            net_load_kwh = max(0.0, load_kwh - solar_kwh)
            solar_surplus_kwh = max(0.0, solar_kwh - load_kwh)
            slots.append(
                {
                    "start": start,
                    "price": float(item["price"]),
                    "load_w": load_w,
                    "load_kwh": load_kwh,
                    "solar_kwh": solar_kwh,
                    "net_load_kwh": net_load_kwh,
                    "solar_surplus_kwh": solar_surplus_kwh,
                    "charge_limit_kwh": self.charge_rate_kw,
                    "discharge_value": float(item["price"]) - weighted_cost,
                    "sell_price": sell_price_by_start.get(start, float(item["price"])),
                    "grid_charge_planned_kwh": 0.0,
                    "discharge_planned_kwh": 0.0,
                }
            )

        quantum_kwh = 0.1
        capacity_units = max(1, int(round(usable_capacity_kwh / quantum_kwh)))
        initial_units = int(round(min(usable_capacity_kwh, available_kwh) / quantum_kwh))

        net_load_units = [
            int(round(min(slot["net_load_kwh"], slot["charge_limit_kwh"]) / quantum_kwh))
            for slot in slots
        ]
        solar_surplus_units = [
            int(round(min(slot["solar_surplus_kwh"], slot["charge_limit_kwh"]) / quantum_kwh))
            for slot in slots
        ]
        charge_limit_units = [
            max(0, int(round(slot["charge_limit_kwh"] / quantum_kwh)))
            for slot in slots
        ]

        @lru_cache(maxsize=16384)
        def _solve(slot_idx: int, stored_units: int) -> tuple[float, tuple[tuple[int, int], ...]]:
            if slot_idx >= len(slots):
                return 0.0, ()

            solar_stored_units = min(solar_surplus_units[slot_idx], max(0, capacity_units - stored_units))
            stored_after_solar = min(capacity_units, stored_units + solar_stored_units)
            max_discharge_units = min(
                charge_limit_units[slot_idx],
                stored_after_solar,
            )
            max_charge_units = min(
                charge_limit_units[slot_idx],
                capacity_units - stored_after_solar,
            )

            best_cost = float("inf")
            best_actions: tuple[tuple[int, int], ...] = ()

            for discharge_units in range(max_discharge_units + 1):
                charge_range = range(0, max_charge_units + 1) if discharge_units == 0 else range(0, 1)
                for charge_units in charge_range:
                    discharge_to_load_units = min(net_load_units[slot_idx], discharge_units)
                    export_units = max(0, discharge_units - discharge_to_load_units)
                    if not allow_battery_export and export_units > 0:
                        continue
                    if solar_stored_units > 0 and export_units > 0:
                        continue
                    next_stored_units = stored_after_solar - discharge_units + charge_units
                    grid_import_units = max(0, net_load_units[slot_idx] - discharge_to_load_units) + charge_units
                    sell_credit = max(
                        0.0,
                        float(slots[slot_idx]["sell_price"]) - self.min_charge_saving,
                    )
                    step_cost = (
                        (grid_import_units * quantum_kwh * slots[slot_idx]["price"])
                        + (charge_units * quantum_kwh * self.battery_cost_per_kwh)
                        + (solar_stored_units * quantum_kwh * slots[slot_idx]["sell_price"])
                        + (discharge_units * quantum_kwh * weighted_cost)
                        - (export_units * quantum_kwh * sell_credit)
                    )
                    future_cost, future_actions = _solve(slot_idx + 1, next_stored_units)
                    total_cost = step_cost + future_cost
                    if total_cost < best_cost - 1e-9:
                        best_cost = total_cost
                        best_actions = ((discharge_units, charge_units),) + future_actions

            return best_cost, best_actions

        _, best_actions = _solve(0, initial_units)

        stored_kwh = min(usable_capacity_kwh, available_kwh)
        plan: list[dict[str, Any]] = []
        for slot, (discharge_units, charge_units) in zip(slots, best_actions):
            soc_start_pct = self.battery_min_soc + (stored_kwh / self.battery_capacity_kwh * 100.0)

            solar_charge_kwh = min(
                slot["solar_surplus_kwh"],
                slot["charge_limit_kwh"],
                max(0.0, usable_capacity_kwh - stored_kwh),
            )
            stored_kwh += solar_charge_kwh

            discharge_total_kwh = min(
                discharge_units * quantum_kwh,
                slot["charge_limit_kwh"],
                stored_kwh,
            )
            discharge_to_load_kwh = min(slot["net_load_kwh"], discharge_total_kwh)
            battery_export_kwh = max(0.0, discharge_total_kwh - discharge_to_load_kwh)
            if not allow_battery_export and battery_export_kwh > 0:
                stored_kwh += battery_export_kwh
                discharge_total_kwh = discharge_to_load_kwh
                battery_export_kwh = 0.0
            if battery_export_kwh > 0:
                sell_spread = float(slot["sell_price"]) - weighted_cost
                if sell_spread < self.min_charge_saving:
                    stored_kwh += battery_export_kwh
                    discharge_total_kwh = discharge_to_load_kwh
                    battery_export_kwh = 0.0
            stored_kwh -= discharge_total_kwh
            grid_import_kwh = max(0.0, slot["net_load_kwh"] - discharge_to_load_kwh)

            grid_charge_kwh = min(
                charge_units * quantum_kwh,
                max(0.0, slot["charge_limit_kwh"] - solar_charge_kwh),
                max(0.0, usable_capacity_kwh - stored_kwh),
            )
            stored_kwh += grid_charge_kwh
            grid_import_kwh += grid_charge_kwh

            soc_end_pct = self.battery_min_soc + (stored_kwh / self.battery_capacity_kwh * 100.0)
            plan.append(
                {
                    "hour": slot["start"].isoformat(),
                    "hour_str": slot["start"].strftime("%H:00"),
                    "soc_start": round(soc_start_pct, 1),
                    "soc": round(max(self.battery_min_soc, min(self.battery_max_soc, soc_end_pct)), 1),
                    "solar_charge_w": round(solar_charge_kwh * 1000),
                    "grid_charge_w": round(grid_charge_kwh * 1000),
                    "discharge_w": round(discharge_total_kwh * 1000),
                    "discharge_to_load_w": round(discharge_to_load_kwh * 1000),
                    "battery_export_w": round(battery_export_kwh * 1000),
                    "grid_import_w": round(grid_import_kwh * 1000),
                    "price_dkk": round(slot["price"], 4),
                    "sell_price_dkk": round(slot["sell_price"], 4),
                    "forecast_solar_w": round(slot["solar_kwh"] * 1000),
                    "forecast_load_w": round(slot["load_w"]),
                    "discharge_value": round(slot["discharge_value"], 4),
                }
            )

        return plan

    def _build_allowed_discharge_slots(
        self,
        *,
        now: datetime,
        current_soc: float,
        raw_prices: list[dict[str, Any]],
        weighted_cost: float,
        hourly_forecast: list | None,
        raw_sell_prices: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Return fallback slots where battery discharge is allowed if solar underdelivers."""
        if not raw_prices:
            return []

        no_solar_plan = self._build_horizon_plan(
            now=now,
            current_soc=current_soc,
            raw_prices=raw_prices,
            weighted_cost=weighted_cost,
            hourly_forecast=None,
            reserved_solar_kwh=None,
            raw_sell_prices=raw_sell_prices,
        )
        forecast_by_hour = self._build_forecast_map(hourly_forecast, now)
        allowed_slots: list[dict[str, Any]] = []

        for slot in no_solar_plan:
            slot_start = datetime.fromisoformat(slot["hour"])
            forecast_solar_w = round(forecast_by_hour.get(slot_start, 0.0) * 1000)
            if forecast_solar_w >= ALLOWED_DISCHARGE_SOLAR_THRESHOLD_W:
                continue

            baseline_discharge_w = round(
                float(slot.get("discharge_to_load_w", slot["discharge_w"]))
            )
            if baseline_discharge_w <= 0:
                continue

            allowed_slots.append(
                {
                    "hour": slot["hour"],
                    "hour_str": slot["hour_str"],
                    "forecast_solar_w": forecast_solar_w,
                    "baseline_discharge_w": baseline_discharge_w,
                    "price_dkk": round(float(slot["price_dkk"]), 4),
                    "value_dkk_per_kwh": round(
                        max(0.0, float(slot["price_dkk"]) - weighted_cost),
                        4,
                    ),
                }
            )

        return allowed_slots

    def get_last_plan(self) -> list[dict[str, Any]]:
        """Return the latest optimizer horizon plan."""
        return list(self._last_plan)

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
        reserved_solar_kwh: dict[datetime, float] | None = None,
        raw_sell_prices: list[dict[str, Any]] | None = None,
    ) -> OptimizeResult:
        """Main entry point called by the coordinator.

        Plans over the full known price horizon and maps the first slot to the
        action for the current cycle.
        """
        available_kwh = max(
            0.0,
            (current_soc - self.battery_min_soc) / 100.0 * self.battery_capacity_kwh,
        )
        weighted_cost = self._tracker.weighted_cost
        solar_fraction = self._tracker.solar_fraction
        usable_capacity_kwh = (
            self.battery_capacity_kwh * (self.battery_max_soc - self.battery_min_soc) / 100.0
        )

        # ── Anti-eksport: negativ/nul spotpris ────────────────────────────
        current_price = get_current_price_from_raw(raw_prices, now, fallback=0.0) or 0.0
        sell_prices = raw_sell_prices if raw_sell_prices is not None else raw_prices
        sell_price = (
            get_current_price_from_raw(sell_prices, now, fallback=current_price)
            if sell_prices
            else current_price
        ) or current_price
        allow_battery_export = (
            now.hour < 12
            or self._has_prices_beyond_next_midnight(raw_prices, now)
        )

        if current_price < 0:
            self._last_plan = self._build_horizon_plan(
                now=now,
                current_soc=current_soc,
                raw_prices=raw_prices,
                weighted_cost=weighted_cost,
                hourly_forecast=hourly_forecast,
                allow_battery_export=allow_battery_export,
                reserved_solar_kwh=reserved_solar_kwh,
                raw_sell_prices=sell_prices,
            )
            return OptimizeResult(
                strategy="NEGATIVE_IMPORT",
                reason=f"Negativ købspris ({current_price:.4f} kr/kWh) — køber alt fra nettet",
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
                allowed_discharge_slots=[],
                solar_sell=False,
            )

        if sell_price <= 0 and sell_prices:
            self._last_plan = self._build_horizon_plan(
                now=now,
                current_soc=current_soc,
                raw_prices=raw_prices,
                weighted_cost=weighted_cost,
                hourly_forecast=hourly_forecast,
                allow_battery_export=allow_battery_export,
                reserved_solar_kwh=reserved_solar_kwh,
                raw_sell_prices=sell_prices,
            )
            return OptimizeResult(
                strategy="ANTI_EXPORT",
                reason=f"Negativ/nul salgspris ({sell_price:.4f} kr/kWh) — solar sell OFF",
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
                allowed_discharge_slots=[],
                solar_sell=False,
            )

        if not raw_prices:
            self._last_plan = []
            return OptimizeResult.idle(
                "Ingen prisdata tilgængelig",
                weighted_cost=weighted_cost,
                solar_fraction=solar_fraction,
            )

        next_sunrise = self._normalize_datetime(sunrise_time, now)
        if next_sunrise <= now:
            next_sunrise += timedelta(days=1)

        self._last_plan = self._build_horizon_plan(
            now=now,
            current_soc=current_soc,
            raw_prices=raw_prices,
            weighted_cost=weighted_cost,
            hourly_forecast=hourly_forecast,
            allow_battery_export=allow_battery_export,
            reserved_solar_kwh=reserved_solar_kwh,
            raw_sell_prices=sell_prices,
        )
        current_slot = self._last_plan[0] if self._last_plan else None

        morning_need_kwh = 0.0
        day_deficit_kwh = 0.0
        if self._last_plan:
            for slot in self._last_plan:
                slot_start = datetime.fromisoformat(slot["hour"])
                slot_net_load = max(
                    0.0,
                    (float(slot["forecast_load_w"]) - float(slot["forecast_solar_w"])) / 1000.0,
                )
                if slot_start < next_sunrise:
                    morning_need_kwh += slot_net_load
                else:
                    day_deficit_kwh += slot_net_load
            morning_need_kwh *= 1.10

        best_discharge_hours = [
            slot["hour_str"]
            for slot in sorted(
                (s for s in self._last_plan if s["discharge_w"] > 0),
                key=lambda item: item["price_dkk"],
                reverse=True,
            )
        ]
        peak_need_kwh = round(
            sum(
                max(0.0, (float(slot["forecast_load_w"]) - float(slot["forecast_solar_w"])) / 1000.0)
                for slot in sorted(self._last_plan, key=lambda item: item["price_dkk"], reverse=True)[:4]
            ),
            4,
        )
        total_planned_charge_kwh = round(
            sum(float(slot["grid_charge_w"]) for slot in self._last_plan) / 1000.0,
            4,
        )
        expected_saving_dkk = round(
            sum(
                (float(slot.get("discharge_to_load_w", slot["discharge_w"])) / 1000.0)
                * max(0.0, float(slot["price_dkk"]) - weighted_cost)
                + (float(slot.get("battery_export_w", 0.0)) / 1000.0)
                * max(0.0, float(slot.get("sell_price_dkk", slot["price_dkk"])) - weighted_cost)
                for slot in self._last_plan
            )
            - sum(
                (float(slot["grid_charge_w"]) / 1000.0) * (float(slot["price_dkk"]) + self.battery_cost_per_kwh)
                for slot in self._last_plan
            ),
            4,
        )
        allowed_discharge_slots = self._build_allowed_discharge_slots(
            now=now,
            current_soc=current_soc,
            raw_prices=raw_prices,
            weighted_cost=weighted_cost,
            hourly_forecast=hourly_forecast,
            raw_sell_prices=sell_prices,
        )

        # Keep the real-time solar heuristics that users already rely on.
        solar_remaining = forecast_today_kwh
        solar_next_2h = 0.0
        if hourly_forecast:
            solar_remaining = get_forecast_for_period(hourly_forecast, now, sunset_time)
            solar_next_2h = get_forecast_for_period(hourly_forecast, now, now + timedelta(hours=2))

        battery_has_headroom = available_kwh < usable_capacity_kwh * 0.9
        if pv_power - load_power > 50:
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=f"Solcelleoverskud {pv_power - load_power:.0f}W — gemmer i batteri",
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )
        if hourly_forecast and solar_next_2h > 1.0 and battery_has_headroom:
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=f"Solprognose {solar_next_2h:.1f} kWh næste 2 timer — gemmer batteri til sol",
                target_soc=None,
                charge_now=False,
                cheapest_charge_hour=None,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )

        cheapest_charge_hour = next(
            (slot["hour_str"] for slot in sorted(self._last_plan, key=lambda item: item["price_dkk"]) if slot["grid_charge_w"] > 0),
            None,
        )
        cheapest_plan_price = min((slot["price_dkk"] for slot in self._last_plan), default=current_price)
        target_soc = None
        if total_planned_charge_kwh > 0:
            target_soc = min(
                self.battery_max_soc,
                self.battery_min_soc + ((available_kwh + total_planned_charge_kwh) / self.battery_capacity_kwh * 100.0) + 5,
            )

        if current_slot and float(current_slot.get("battery_export_w", 0.0)) > 0:
            planned_self_use_kwh = round(
                sum(
                    float(slot.get("discharge_to_load_w", slot["discharge_w"])) / 1000.0
                    for slot in self._last_plan
                ),
                4,
            )
            future_recharge_slot = next(
                (
                    slot["hour_str"]
                    for slot in self._last_plan[1:]
                    if float(slot.get("solar_charge_w", 0.0)) > 0 or float(slot.get("grid_charge_w", 0.0)) > 0
                ),
                None,
            )
            future_recharge_kwh = round(
                sum(
                    (float(slot["solar_charge_w"]) + float(slot["grid_charge_w"])) / 1000.0
                    for slot in self._last_plan[1:]
                ),
                4,
            )
            sell_now_kwh = round(float(current_slot["battery_export_w"]) / 1000.0, 4)
            current_self_use_kwh = round(
                float(current_slot.get("discharge_to_load_w", current_slot["discharge_w"])) / 1000.0,
                4,
            )
            current_sell_price = float(current_slot.get("sell_price_dkk", current_slot["price_dkk"]))
            current_sell_spread = current_sell_price - weighted_cost
            reserved_for_self_use_kwh = max(0.0, planned_self_use_kwh - future_recharge_kwh)
            exportable_surplus_kwh = max(0.0, available_kwh - reserved_for_self_use_kwh)
            if exportable_surplus_kwh + 1e-6 < sell_now_kwh or current_self_use_kwh > 0.1:
                reserve_reason = (
                    f"Reserverer batteri til forventet egetforbrug ({reserved_for_self_use_kwh:.1f} kWh)"
                )
                if current_self_use_kwh > 0.1:
                    reserve_reason += f" — forventet husforbrug i denne time {current_self_use_kwh:.1f} kWh"
                return OptimizeResult(
                    strategy="USE_BATTERY",
                    reason=reserve_reason,
                    target_soc=round(target_soc, 1) if target_soc is not None else None,
                    charge_now=False,
                    cheapest_charge_hour=cheapest_charge_hour,
                    night_charge_kwh=total_planned_charge_kwh,
                    morning_need_kwh=round(morning_need_kwh, 4),
                    day_deficit_kwh=round(day_deficit_kwh, 4),
                    peak_need_kwh=peak_need_kwh,
                    expected_saving_dkk=expected_saving_dkk,
                    weighted_battery_cost=weighted_cost,
                    solar_fraction=solar_fraction,
                    best_discharge_hours=best_discharge_hours,
                    allowed_discharge_slots=allowed_discharge_slots,
                )
            reason = f"Sælger {sell_now_kwh:.1f} kWh nu til {float(current_slot['sell_price_dkk']):.2f} kr"
            if future_recharge_slot:
                reason += f" — næste genladning {future_recharge_slot}"
            if future_recharge_kwh > 0:
                reason += f", forventet genladning {future_recharge_kwh:.1f} kWh"
            if current_sell_spread < self.min_charge_saving:
                return OptimizeResult(
                    strategy="IDLE",
                    reason=(
                        f"Holder batteri - eksportspread {current_sell_spread:.2f} kr/kWh "
                        f"er under minimum {self.min_charge_saving:.2f} kr/kWh"
                    ),
                    target_soc=round(target_soc, 1) if target_soc is not None else None,
                    charge_now=False,
                    cheapest_charge_hour=cheapest_charge_hour,
                    night_charge_kwh=total_planned_charge_kwh,
                    morning_need_kwh=round(morning_need_kwh, 4),
                    day_deficit_kwh=round(day_deficit_kwh, 4),
                    peak_need_kwh=peak_need_kwh,
                    expected_saving_dkk=expected_saving_dkk,
                    weighted_battery_cost=weighted_cost,
                    solar_fraction=solar_fraction,
                    best_discharge_hours=best_discharge_hours,
                    allowed_discharge_slots=allowed_discharge_slots,
                )
            return OptimizeResult(
                strategy="SELL_BATTERY",
                reason=reason,
                target_soc=float(self.battery_min_soc),
                charge_now=False,
                cheapest_charge_hour=cheapest_charge_hour,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )

        if total_planned_charge_kwh > 0 and (
            available_kwh < morning_need_kwh or expected_saving_dkk > self.min_charge_saving
        ):
            charge_strategy = "CHARGE_NIGHT" if now < next_sunrise or now.time() > sunset_time.time() else "CHARGE_GRID"
            return OptimizeResult(
                strategy=charge_strategy,
                reason=(
                    f"Planlagt opladning {total_planned_charge_kwh:.1f} kWh i billige timer "
                    f"frem mod næste peak"
                ),
                target_soc=round(target_soc, 1) if target_soc is not None else None,
                charge_now=bool(current_slot and current_slot["grid_charge_w"] > 0),
                cheapest_charge_hour=cheapest_charge_hour,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )

        if (
            current_slot
            and current_slot["discharge_w"] > 0
            and current_price > weighted_cost
        ):
            return OptimizeResult(
                strategy="USE_BATTERY",
                reason=(
                    f"Bruger batteri i højværdi-time {current_slot['hour_str']} "
                    f"til {current_slot['price_dkk']:.2f} kr"
                ),
                target_soc=round(target_soc, 1) if target_soc is not None else None,
                charge_now=False,
                cheapest_charge_hour=cheapest_charge_hour,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )

        if current_slot and current_slot["grid_charge_w"] > 0:
            in_night_window = now < next_sunrise or now.time() < sunrise_time.time() or now.time() > sunset_time.time()
            strategy = "CHARGE_NIGHT" if in_night_window else "CHARGE_GRID"
            return OptimizeResult(
                strategy=strategy,
                reason=(
                    f"Lader i billig time {current_slot['hour_str']} til {current_slot['price_dkk']:.2f} kr "
                    f"for senere højt forbrug"
                ),
                target_soc=round(target_soc, 1) if target_soc is not None else None,
                charge_now=True,
                cheapest_charge_hour=cheapest_charge_hour,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
                allowed_discharge_slots=allowed_discharge_slots,
            )

        future_discharge_exists = any(slot["discharge_w"] > 0 for slot in self._last_plan[1:])
        if current_price <= max(weighted_cost, self.cheap_grid_threshold) and battery_has_headroom and future_discharge_exists:
            return OptimizeResult(
                strategy="SAVE_SOLAR",
                reason=(
                    f"Billig netstrøm ({current_price:.2f} kr) — holder batteriet til dyrere timer senere"
                ),
                target_soc=round(target_soc, 1) if target_soc is not None else None,
                charge_now=False,
                cheapest_charge_hour=cheapest_charge_hour,
                night_charge_kwh=total_planned_charge_kwh,
                morning_need_kwh=round(morning_need_kwh, 4),
                day_deficit_kwh=round(day_deficit_kwh, 4),
                peak_need_kwh=peak_need_kwh,
                expected_saving_dkk=expected_saving_dkk,
                weighted_battery_cost=weighted_cost,
                solar_fraction=solar_fraction,
                best_discharge_hours=best_discharge_hours,
            )

        return OptimizeResult.idle(
            reason="Ingen optimal handling — venter på bedre pris eller sol",
            weighted_cost=weighted_cost,
            solar_fraction=solar_fraction,
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
    _NEGATIVE_IMPORT_PRICES = [
        {"hour": h, "price": (-0.25 if h == 12 else (1.38 if 7 <= h < 22 else 0.38))}
        for h in range(24)
    ]
    r0 = _opt.optimize(
        now=_dt(2026, 3, 22, 12, 0, 0),
        pv_power=5000, load_power=800, current_soc=60,
        raw_prices=_NEGATIVE_IMPORT_PRICES, forecast_today_kwh=15.0, forecast_tomorrow_kwh=15.0,
        sunrise_time=_SUNRISE, sunset_time=_SUNSET, is_weekend=False,
    )
    assert r0.strategy == "NEGATIVE_IMPORT", f"Scenario 0 fejlede: {r0.strategy}"
    print(f"Scenario 0 OK  strategy={r0.strategy!r:16}  reason='{r0.reason}'")

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
