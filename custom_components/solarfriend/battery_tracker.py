"""SolarFriend BatteryTracker tracks battery content and savings over time."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 2
STORAGE_KEY = "solarfriend_battery_tracker"
_DRIFT_THRESHOLD = 0.10  # log warning if tracker drifts >10% from actual SOC


class BatteryTracker:
    """Tracks kWh in battery split by origin and computes weighted cost."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        battery_cost_per_kwh: float,
    ) -> None:
        self._hass = hass
        self._legacy_entry_id = entry_id
        self._battery_cost_per_kwh = battery_cost_per_kwh
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self.solar_kwh: float = 0.0
        self.grid_kwh: float = 0.0
        self.grid_avg_cost: float = 0.0

        # Savings tracking: today (reset at midnight) + persisted totals.
        self.today_solar_direct_kwh: float = 0.0
        self.today_solar_direct_saved_dkk: float = 0.0
        self.today_optimizer_saved_dkk: float = 0.0
        self.total_solar_direct_saved_dkk: float = 0.0
        self.total_optimizer_saved_dkk: float = 0.0
        self.today_battery_sell_kwh: float = 0.0
        self.today_battery_sell_saved_dkk: float = 0.0
        self.total_battery_sell_saved_dkk: float = 0.0
        self._last_reset_date: str = ""

    @property
    def _charge_slid(self) -> float:
        return self._battery_cost_per_kwh / 2

    @property
    def _discharge_slid(self) -> float:
        return self._battery_cost_per_kwh / 2

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
        """Weighted average cost of all energy currently in the battery."""
        total = self.total_kwh
        if total == 0:
            return 0.0
        solar_cost = self.solar_kwh * self._charge_slid
        grid_cost = self.grid_kwh * (self.grid_avg_cost + self._discharge_slid)
        return (solar_cost + grid_cost) / total

    @property
    def live_total_solar_saved_dkk(self) -> float:
        """Lifetime solar saving including today's running total."""
        return self.total_solar_direct_saved_dkk + self.today_solar_direct_saved_dkk

    @property
    def live_total_optimizer_saved_dkk(self) -> float:
        """Lifetime optimizer saving including today's running total."""
        return self.total_optimizer_saved_dkk + self.today_optimizer_saved_dkk

    @property
    def live_total_battery_sell_saved_dkk(self) -> float:
        """Lifetime battery-sell value including today's running total."""
        return self.total_battery_sell_saved_dkk + self.today_battery_sell_saved_dkk

    async def async_load(self) -> None:
        """Load tracker state from HA storage."""
        data = await self._async_safe_load(self._store, STORAGE_KEY)
        if not data and self._legacy_entry_id:
            legacy_store = Store(
                self._hass,
                STORAGE_VERSION,
                f"{STORAGE_KEY}_{self._legacy_entry_id}",
            )
            data = await self._async_safe_load(
                legacy_store,
                f"{STORAGE_KEY}_{self._legacy_entry_id}",
            )
            if data:
                _LOGGER.info(
                    "BatteryTracker migrated legacy storage for entry_id=%s to stable key",
                    self._legacy_entry_id,
                )
                await self._store.async_save(data)
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
        self.today_battery_sell_kwh = float(data.get("today_battery_sell_kwh", 0.0))
        self.today_battery_sell_saved_dkk = float(data.get("today_battery_sell_saved_dkk", 0.0))
        self.total_battery_sell_saved_dkk = float(data.get("total_battery_sell_saved_dkk", 0.0))
        self._last_reset_date = data.get("last_reset_date", "")
        _LOGGER.debug(
            "BatteryTracker loaded: solar=%.3f kWh grid=%.3f kWh avg_cost=%.4f "
            "today_solar_saved=%.4f kr today_optimizer_saved=%.4f kr",
            self.solar_kwh,
            self.grid_kwh,
            self.grid_avg_cost,
            self.today_solar_direct_saved_dkk,
            self.today_optimizer_saved_dkk,
        )

    async def _async_safe_load(
        self,
        store: Store,
        storage_key: str,
    ) -> dict[str, Any] | None:
        """Load persisted state without letting storage corruption abort startup."""
        try:
            data = await store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "BatteryTracker storage load failed for %s; starting fresh: %s",
                storage_key,
                exc,
            )
            return None
        return data if isinstance(data, dict) else None

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
                "today_battery_sell_kwh": round(self.today_battery_sell_kwh, 6),
                "today_battery_sell_saved_dkk": round(self.today_battery_sell_saved_dkk, 4),
                "total_battery_sell_saved_dkk": round(self.total_battery_sell_saved_dkk, 4),
                "last_reset_date": self._last_reset_date,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        )

    def on_solar_charge(self, kwh: float) -> None:
        """Record kWh charged from solar surplus."""
        if kwh <= 0:
            return
        self.solar_kwh += kwh
        _LOGGER.debug("BatteryTracker solar charge +%.4f kWh -> solar=%.3f", kwh, self.solar_kwh)

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
            "BatteryTracker grid charge +%.4f kWh @ %.4f -> grid=%.3f avg_cost=%.4f",
            kwh,
            grid_price,
            self.grid_kwh,
            self.grid_avg_cost,
        )

    def on_discharge(self, kwh: float) -> None:
        """Discharge kWh. Grid kWh is consumed first, then solar."""
        if kwh <= 0:
            return
        from_grid = min(kwh, self.grid_kwh)
        self.grid_kwh -= from_grid
        remaining = kwh - from_grid
        self.solar_kwh = max(0.0, self.solar_kwh - remaining)
        _LOGGER.debug(
            "BatteryTracker discharge %.4f kWh (grid=%.4f solar=%.4f) -> solar=%.3f grid=%.3f",
            kwh,
            from_grid,
            remaining,
            self.solar_kwh,
            self.grid_kwh,
        )

    def on_soc_correction(
        self, actual_soc: float, capacity_kwh: float, min_soc: float
    ) -> None:
        """Reconcile tracker with the measured SOC."""
        actual_total = max(0.0, (actual_soc - min_soc) / 100 * capacity_kwh)
        tracked_total = self.total_kwh

        if tracked_total == 0 and actual_total == 0:
            return

        if tracked_total == 0:
            self.solar_kwh = actual_total
            _LOGGER.debug("BatteryTracker cold-start correction: %.3f kWh -> solar", actual_total)
            return

        drift = abs(actual_total - tracked_total) / max(tracked_total, 0.001)
        if drift > _DRIFT_THRESHOLD:
            scale = actual_total / tracked_total
            self.solar_kwh *= scale
            self.grid_kwh *= scale
            _LOGGER.warning(
                "BatteryTracker drift %.1f%% (tracked=%.3f actual=%.3f) -> scaled by %.4f",
                drift * 100,
                tracked_total,
                actual_total,
                scale,
            )

    def _check_midnight_reset(self) -> bool:
        """Roll today's savings into persisted totals when the date changes."""
        rolled = False
        today = datetime.now().date().isoformat()
        if self._last_reset_date and self._last_reset_date != today:
            self.total_solar_direct_saved_dkk += self.today_solar_direct_saved_dkk
            self.total_optimizer_saved_dkk += self.today_optimizer_saved_dkk
            self.total_battery_sell_saved_dkk += self.today_battery_sell_saved_dkk
            self.today_solar_direct_kwh = 0.0
            self.today_solar_direct_saved_dkk = 0.0
            self.today_optimizer_saved_dkk = 0.0
            self.today_battery_sell_kwh = 0.0
            self.today_battery_sell_saved_dkk = 0.0
            rolled = True
            _LOGGER.info(
                "BatteryTracker: new day %s -> total_sol=%.2f kr total_opt=%.2f kr total_sell=%.2f kr",
                today,
                self.total_solar_direct_saved_dkk,
                self.total_optimizer_saved_dkk,
                self.total_battery_sell_saved_dkk,
            )
        self._last_reset_date = today
        return rolled

    def update_battery_sell_savings(
        self,
        *,
        battery_w: float,
        sell_price_dkk: float,
        dt_seconds: float,
    ) -> bool:
        """Track gross battery-export value during SELL_BATTERY runtime."""
        if dt_seconds <= 0 or sell_price_dkk <= 0 or battery_w <= 0:
            return False

        changed = self._check_midnight_reset()
        sell_kwh = battery_w * dt_seconds / 3_600_000
        if sell_kwh <= 0:
            return changed

        self.today_battery_sell_kwh += sell_kwh
        self.today_battery_sell_saved_dkk += sell_kwh * sell_price_dkk
        return True

    def update_savings(
        self,
        pv_w: float,
        load_w: float,
        battery_w: float,
        price_dkk: float,
        dt_seconds: float,
    ) -> bool:
        """Track solar-direct and optimizer savings for this tick."""
        if dt_seconds <= 0 or price_dkk <= 0:
            return False

        changed = self._check_midnight_reset()

        pv_kwh = pv_w * dt_seconds / 3_600_000
        load_kwh = load_w * dt_seconds / 3_600_000
        battery_kwh = abs(battery_w) * dt_seconds / 3_600_000

        direct_solar_kwh = max(0.0, min(pv_kwh, load_kwh))
        if direct_solar_kwh > 0:
            self.today_solar_direct_kwh += direct_solar_kwh
            self.today_solar_direct_saved_dkk += direct_solar_kwh * price_dkk
            changed = True

        if battery_w > 0 and price_dkk > self.weighted_cost and battery_kwh > 0:
            self.today_optimizer_saved_dkk += battery_kwh * (price_dkk - self.weighted_cost)
            changed = True

        return changed

    def reset(self) -> None:
        """Clear all tracked energy and savings."""
        self.solar_kwh = 0.0
        self.grid_kwh = 0.0
        self.grid_avg_cost = 0.0
        self.today_solar_direct_kwh = 0.0
        self.today_solar_direct_saved_dkk = 0.0
        self.today_optimizer_saved_dkk = 0.0
        self.total_solar_direct_saved_dkk = 0.0
        self.total_optimizer_saved_dkk = 0.0
        self.today_battery_sell_kwh = 0.0
        self.today_battery_sell_saved_dkk = 0.0
        self.total_battery_sell_saved_dkk = 0.0
        self._last_reset_date = ""
        _LOGGER.debug("BatteryTracker reset")
