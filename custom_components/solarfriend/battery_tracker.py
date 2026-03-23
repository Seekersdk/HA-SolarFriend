"""SolarFriend BatteryTracker — tracks battery content by origin (solar vs. grid)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
_DRIFT_THRESHOLD = 0.10  # log warning if tracker drifts >10% from actual SOC


class BatteryTracker:
    """Tracks kWh in battery split by origin and computes weighted cost."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        battery_cost_per_kwh: float,
    ) -> None:
        self._battery_cost_per_kwh = battery_cost_per_kwh
        self._store = Store(hass, STORAGE_VERSION, f"solarfriend_battery_tracker_{entry_id}")
        self.solar_kwh: float = 0.0
        self.grid_kwh: float = 0.0
        self.grid_avg_cost: float = 0.0  # weighted avg cost of grid kWh in battery

        # Savings tracking — today (reset at midnight) + lifetime totals
        self.today_solar_direct_kwh: float = 0.0
        self.today_solar_direct_saved_dkk: float = 0.0
        self.today_optimizer_saved_dkk: float = 0.0
        self.total_solar_direct_saved_dkk: float = 0.0
        self.total_optimizer_saved_dkk: float = 0.0
        self._last_reset_date: str = ""  # ISO date, e.g. "2026-03-22"

    # ------------------------------------------------------------------
    # Slid (wear cost) helpers
    # ------------------------------------------------------------------

    @property
    def _charge_slid(self) -> float:
        return self._battery_cost_per_kwh / 2

    @property
    def _discharge_slid(self) -> float:
        return self._battery_cost_per_kwh / 2

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def total_kwh(self) -> float:
        return self.solar_kwh + self.grid_kwh

    @property
    def solar_fraction(self) -> float:
        total = self.total_kwh
        return self.solar_kwh / total if total > 0 else 0.0

    @property
    def grid_fraction(self) -> float:
        total = self.total_kwh
        return self.grid_kwh / total if total > 0 else 0.0

    @property
    def weighted_cost(self) -> float:
        """Weighted average cost of all energy currently in the battery.

        solar kWh carries only charge_slid (wear on charging side).
        grid kWh carries grid_avg_cost (already includes charge_slid) + discharge_slid.
        """
        total = self.total_kwh
        if total == 0:
            return 0.0
        solar_cost = self.solar_kwh * self._charge_slid
        grid_cost = self.grid_kwh * (self.grid_avg_cost + self._discharge_slid)
        return (solar_cost + grid_cost) / total

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def async_load(self) -> None:
        """Load tracker state from HA storage."""
        data: dict[str, Any] | None = await self._store.async_load()
        if not data:
            _LOGGER.debug("BatteryTracker: no stored data, starting fresh")
            return
        self.solar_kwh = float(data.get("solar_kwh", 0.0))
        self.grid_kwh = float(data.get("grid_kwh", 0.0))
        self.grid_avg_cost = float(data.get("grid_avg_cost", 0.0))
        self.today_solar_direct_kwh = float(data.get("today_solar_direct_kwh", 0.0))
        self.today_solar_direct_saved_dkk = float(data.get("today_solar_direct_saved_dkk", 0.0))
        self.today_optimizer_saved_dkk = float(data.get("today_optimizer_saved_dkk", 0.0))
        self.total_solar_direct_saved_dkk = float(data.get("total_solar_direct_saved_dkk", 0.0))
        self.total_optimizer_saved_dkk = float(data.get("total_optimizer_saved_dkk", 0.0))
        self._last_reset_date = data.get("last_reset_date", "")
        _LOGGER.debug(
            "BatteryTracker loaded: solar=%.3f kWh grid=%.3f kWh avg_cost=%.4f "
            "today_solar_saved=%.4f kr today_optimizer_saved=%.4f kr",
            self.solar_kwh, self.grid_kwh, self.grid_avg_cost,
            self.today_solar_direct_saved_dkk, self.today_optimizer_saved_dkk,
        )

    async def async_save(self) -> None:
        """Persist tracker state to HA storage."""
        await self._store.async_save(
            {
                "solar_kwh": round(self.solar_kwh, 6),
                "grid_kwh": round(self.grid_kwh, 6),
                "grid_avg_cost": round(self.grid_avg_cost, 6),
                "today_solar_direct_kwh": round(self.today_solar_direct_kwh, 6),
                "today_solar_direct_saved_dkk": round(self.today_solar_direct_saved_dkk, 4),
                "today_optimizer_saved_dkk": round(self.today_optimizer_saved_dkk, 4),
                "total_solar_direct_saved_dkk": round(self.total_solar_direct_saved_dkk, 4),
                "total_optimizer_saved_dkk": round(self.total_optimizer_saved_dkk, 4),
                "last_reset_date": self._last_reset_date,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        )

    # ------------------------------------------------------------------
    # Charge / discharge events
    # ------------------------------------------------------------------

    def on_solar_charge(self, kwh: float) -> None:
        """Record kWh charged from solar surplus."""
        if kwh <= 0:
            return
        self.solar_kwh += kwh
        _LOGGER.debug("BatteryTracker solar charge +%.4f kWh → solar=%.3f", kwh, self.solar_kwh)

    def on_grid_charge(self, kwh: float, grid_price: float) -> None:
        """Record kWh charged from the grid, updating weighted average cost."""
        if kwh <= 0:
            return
        new_cost = grid_price + self._charge_slid
        if self.grid_kwh > 0:
            self.grid_avg_cost = (
                self.grid_kwh * self.grid_avg_cost + kwh * new_cost
            ) / (self.grid_kwh + kwh)
        else:
            self.grid_avg_cost = new_cost
        self.grid_kwh += kwh
        _LOGGER.debug(
            "BatteryTracker grid charge +%.4f kWh @ %.4f → grid=%.3f avg_cost=%.4f",
            kwh, grid_price, self.grid_kwh, self.grid_avg_cost,
        )

    def on_discharge(self, kwh: float) -> None:
        """Discharge kWh — grid kWh consumed first, then solar."""
        if kwh <= 0:
            return
        from_grid = min(kwh, self.grid_kwh)
        self.grid_kwh -= from_grid
        remaining = kwh - from_grid
        self.solar_kwh = max(0.0, self.solar_kwh - remaining)
        _LOGGER.debug(
            "BatteryTracker discharge %.4f kWh (grid=%.4f solar=%.4f) → solar=%.3f grid=%.3f",
            kwh, from_grid, remaining, self.solar_kwh, self.grid_kwh,
        )

    # ------------------------------------------------------------------
    # SOC correction
    # ------------------------------------------------------------------

    def on_soc_correction(
        self, actual_soc: float, capacity_kwh: float, min_soc: float
    ) -> None:
        """Reconcile tracker with actual SOC reading.

        If the tracker has drifted more than 10% from the measured value,
        scale solar_kwh and grid_kwh proportionally to match.
        """
        actual_total = max(0.0, (actual_soc - min_soc) / 100 * capacity_kwh)
        tracked_total = self.total_kwh

        if tracked_total == 0 and actual_total == 0:
            return

        if tracked_total == 0:
            # Tracker was reset but battery has charge — attribute all to solar (unknown origin)
            self.solar_kwh = actual_total
            _LOGGER.debug("BatteryTracker cold-start correction: %.3f kWh → solar", actual_total)
            return

        drift = abs(actual_total - tracked_total) / max(tracked_total, 0.001)
        if drift > _DRIFT_THRESHOLD:
            scale = actual_total / tracked_total
            self.solar_kwh *= scale
            self.grid_kwh *= scale
            _LOGGER.warning(
                "BatteryTracker drift %.1f%% (tracked=%.3f actual=%.3f) — scaled by %.4f",
                drift * 100, tracked_total, actual_total, scale,
            )

    # ------------------------------------------------------------------
    # Savings tracking
    # ------------------------------------------------------------------

    def _check_midnight_reset(self) -> None:
        """Roll today's savings into lifetime totals when the date has changed."""
        today = datetime.now().date().isoformat()
        if self._last_reset_date and self._last_reset_date != today:
            self.total_solar_direct_saved_dkk += self.today_solar_direct_saved_dkk
            self.total_optimizer_saved_dkk += self.today_optimizer_saved_dkk
            self.today_solar_direct_kwh = 0.0
            self.today_solar_direct_saved_dkk = 0.0
            self.today_optimizer_saved_dkk = 0.0
            _LOGGER.info(
                "BatteryTracker: ny dag %s — "
                "total_sol=%.2f kr total_opt=%.2f kr",
                today,
                self.total_solar_direct_saved_dkk,
                self.total_optimizer_saved_dkk,
            )
        self._last_reset_date = today

    def update_savings(
        self,
        pv_w: float,
        load_w: float,
        battery_w: float,
        price_dkk: float,
        dt_seconds: float,
    ) -> None:
        """Track solar-direct and optimizer savings for this tick.

        Called every poll cycle when dt_seconds > 0 and price > 0.
        Handles daily reset: today's totals roll into lifetime totals at midnight.
        """
        if dt_seconds <= 0 or price_dkk <= 0:
            return

        self._check_midnight_reset()

        # W × seconds / 3_600_000 = kWh
        pv_kwh = pv_w * dt_seconds / 3_600_000
        load_kwh = load_w * dt_seconds / 3_600_000
        battery_kwh = abs(battery_w) * dt_seconds / 3_600_000

        # Direct solar to house: sol der hverken går i batteri eller eksporteres
        direct_solar_kwh = max(0.0, min(pv_kwh, load_kwh))
        self.today_solar_direct_kwh += direct_solar_kwh
        self.today_solar_direct_saved_dkk += direct_solar_kwh * price_dkk

        # Optimizer saving: batteri aflader til bedre pris end hvad det kostede
        if battery_w > 0 and price_dkk > self.weighted_cost:
            self.today_optimizer_saved_dkk += battery_kwh * (price_dkk - self.weighted_cost)

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all tracked energy (e.g. after battery replacement)."""
        self.solar_kwh = 0.0
        self.grid_kwh = 0.0
        self.grid_avg_cost = 0.0
        self.today_solar_direct_kwh = 0.0
        self.today_solar_direct_saved_dkk = 0.0
        self.today_optimizer_saved_dkk = 0.0
        self.total_solar_direct_saved_dkk = 0.0
        self.total_optimizer_saved_dkk = 0.0
        self._last_reset_date = ""
        _LOGGER.debug("BatteryTracker reset")
