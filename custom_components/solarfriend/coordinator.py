"""SolarFriend DataUpdateCoordinator."""
from __future__ import annotations

import asyncio
import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as ha_dt

from .const import DOMAIN
from .consumption_profile import ConsumptionProfile
from .battery_tracker import BatteryTracker
from .battery_optimizer import (
    BatteryOptimizer,
    LOW_GRID_HOLD_PRICE,
    OptimizeResult,
)
from .forecast_adapter import ForecastAdapter, ForecastData, get_forecast_for_period
from .forecast_tracker import ForecastTracker
from .price_adapter import PriceAdapter, PriceData
from .inverter_controller import InverterController
from .ev_charger_controller import EVChargerController
from .vehicle_controller import VehicleController
from .ev_optimizer import EVContext, EVOptimizer

_LOGGER = logging.getLogger(__name__)


def _parse_dt(val) -> datetime:
    """Parse period_start — handles both str and datetime objects."""
    if isinstance(val, datetime):
        return val
    return datetime.fromisoformat(str(val))


def _normalize_local_datetime(value: datetime) -> datetime:
    """Return a timezone-aware local datetime for safe comparisons."""
    if value.tzinfo is None:
        return ha_dt.as_local(value.replace(tzinfo=timezone.utc))
    return ha_dt.as_local(value)

UPDATE_INTERVAL = timedelta(seconds=30)

# Battery strategy thresholds
PRICE_SURPLUS_FACTOR = 1.20   # price > avg * 1.20 → USE_BATTERY
PRICE_CHEAP_FACTOR = 0.80     # price < avg * 0.80 → CHARGE_GRID

# Rolling window for price average (number of samples kept)
PRICE_HISTORY_MAX = 48        # ~24 min at 30 s interval

# Night hours used for charge-threshold calculation (22:00–06:00)
NIGHT_HOURS: frozenset[int] = frozenset(range(22, 24)) | frozenset(range(0, 7))

# Minimum SOC change (%) that triggers a re-optimize
SOC_TRIGGER_DELTA = 5.0

# Minimum interval between optimizer runs triggered by an event
OPTIMIZE_MIN_INTERVAL = timedelta(minutes=5)
STRATEGY_SOFT_COOLDOWN = timedelta(minutes=5)
STRATEGY_CONFIRMATION_REQUIRED = 2
PV_DROP_OVERRIDE_FRACTION = 0.40
PV_DROP_OVERRIDE_MIN_W = 500.0
SOC_OVERRIDE_MARGIN = 2.0
SUNSET_OVERRIDE_REMAINING_KWH = 0.25

# Battery power sign convention (Deye via ESPHome):
#   battery_power > 0  → discharging (leverer strøm)
#   battery_power < 0  → charging   (modtager strøm)
_BATTERY_NOISE_W = 50  # ignore changes below this threshold


@dataclass
class SolarFriendData:
    # Raw sensor readings
    pv_power: float = 0.0
    grid_power: float = 0.0
    battery_soc: float = 0.0
    battery_power: float = 0.0
    load_power: float = 0.0
    price: float = 0.0
    forecast: float = 0.0
    price_data: PriceData | None = None

    # Derived values
    solar_surplus: float = 0.0
    battery_strategy: str = "IDLE"
    price_level: str = "NORMAL"

    # Battery economics
    battery_cost_per_kwh: float = 0.0
    charge_threshold: float | None = None

    # Consumption profile
    profile_confidence: str = "LEARNING"
    profile_days_collected: int = 0

    # Optimizer result (None until first run)
    optimize_result: OptimizeResult | None = None

    # Forecast data snapshot (None until first optimizer run)
    forecast_data: ForecastData | None = None

    # Battery tracker snapshot (updated every poll cycle)
    battery_solar_kwh: float = 0.0
    battery_grid_kwh: float = 0.0
    battery_weighted_cost: float = 0.0
    battery_solar_fraction: float = 0.0  # 0.0–1.0

    # Savings tracking
    today_solar_direct_kwh: float = 0.0
    today_solar_direct_saved_dkk: float = 0.0
    today_optimizer_saved_dkk: float = 0.0
    total_solar_direct_saved_dkk: float = 0.0
    total_optimizer_saved_dkk: float = 0.0

    # Hourly-forecast derived sensors
    solar_next_2h: float = 0.0
    solar_until_sunset: float = 0.0

    # Consumption profile chart (24 hourly averages in W)
    consumption_profile_chart: list[float] = field(default_factory=list)
    consumption_profile_day_type: str = "weekday"

    # Forecast SOC curve (24 values in %, None for past hours)
    forecast_soc_chart: list = field(default_factory=list)
    battery_plan: list[dict[str, Any]] = field(default_factory=list)
    forecast_actual_today_so_far_kwh: float = 0.0
    forecast_predicted_today_so_far_kwh: float = 0.0
    forecast_error_today_so_far_kwh: float = 0.0
    forecast_accuracy_today_so_far_pct: float = 0.0
    forecast_actual_yesterday_kwh: float = 0.0
    forecast_predicted_yesterday_kwh: float = 0.0
    forecast_error_yesterday_kwh: float = 0.0
    forecast_accuracy_yesterday_pct: float = 0.0
    forecast_bias_factor_14d: float = 1.0
    forecast_mae_14d_kwh: float = 0.0
    forecast_mape_14d_pct: float = 0.0
    forecast_accuracy_14d_pct: float = 0.0
    forecast_valid_days_14d: int = 0
    forecast_correction_valid: bool = False
    forecast_history_14d: list[dict[str, Any]] = field(default_factory=list)

    # Which sensors were unavailable this cycle
    unavailable: list[str] = field(default_factory=list)

    # EV charging
    ev_charging_enabled: bool = False
    ev_charging_power: float = 0.0
    ev_vehicle_soc: float = 0.0
    ev_target_soc: float = 80.0
    ev_surplus_w: float = 0.0
    ev_strategy_reason: str = ""
    ev_charger_status: str = "disconnected"
    ev_target_w: float = 0.0
    ev_phases: int = 0
    ev_vehicle_soc_kwh: float = 0.0
    ev_needed_kwh: float = 0.0
    ev_hours_to_departure: float = 0.0
    ev_charge_mode: str = ""
    ev_min_range_km: float = 0.0
    ev_emergency_charging: bool = False
    ev_min_soc_from_range: float = 0.0
    ev_plan: list = field(default_factory=list)


def ev_device_info(coordinator: "SolarFriendCoordinator") -> DeviceInfo:
    """Shared EV DeviceInfo used by all EV entities."""
    entry = coordinator._entry
    charger_type_name = {"easee": "Easee", "manual": "Manuel ladeboks"}.get(
        entry.data.get("ev_charger_type", "manual"),
        entry.data.get("ev_charger_type", "manual"),
    )
    vehicle_type_name = {
        "kia_hyundai": "Kia / Hyundai",
        "manual": "Manuel bil",
        "none": "Ingen bil",
    }.get(entry.data.get("vehicle_type", "none"), entry.data.get("vehicle_type", "none"))
    return DeviceInfo(
        identifiers={(DOMAIN, f"{entry.entry_id}_ev")},
        name="SolarFriend EV",
        manufacturer=charger_type_name,
        model=vehicle_type_name,
        via_device=(DOMAIN, entry.entry_id),
    )


class SolarFriendCoordinator(DataUpdateCoordinator[SolarFriendData]):
    """Fetch and derive SolarFriend data on a fixed interval."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=UPDATE_INTERVAL,
        )
        self._entry = entry
        self.config_entry = entry  # public alias for use by entity unique_ids
        self._price_history: list[float] = []
        self._night_prices: dict[int, float] = {}  # hour → min price seen this night
        self._profile = ConsumptionProfile()
        self._last_profile_update: datetime | None = None

        # BatteryTracker — initialised in async_startup
        self._tracker: BatteryTracker | None = None
        self._forecast_tracker: ForecastTracker | None = None

        # BatteryOptimizer — instantiated in async_startup after tracker is ready
        self._optimizer: BatteryOptimizer | None = None

        # InverterController — instantiated in async_startup
        self._inverter: InverterController | None = None

        # Tracker tick state
        self._prev_battery_power: float = 0.0
        self._prev_update_time: datetime | None = None
        self._last_tracker_save: datetime | None = None
        self._last_forecast_tracker_save: datetime | None = None
        self._last_soc_correction: datetime | None = None

        # Optimizer state
        self._last_optimize_dt: datetime | None = None
        self._last_optimize_soc: float | None = None
        self._active_strategy_since: datetime | None = None
        self._active_strategy_reference_pv: float = 0.0
        self._pending_strategy: str | None = None
        self._pending_strategy_count: int = 0
        self._shadow_log_enabled: bool = bool(entry.data.get("shadow_log_enabled", True))
        self._shadow_log_path: Path = Path(hass.config.path("solarfriend_shadow_log.jsonl"))
        self._shadow_log_lock = asyncio.Lock()

        # Battery sign convention check (runs once)
        self._battery_sign_warned: bool = False

        # Event listener unsubscribe handles
        self._unsub_listeners: list[Callable[[], None]] = []

        # EV charging
        self.ev_charge_mode: str = "solar_only"  # overwritten by SolarFriendEVModeSelect on restore
        self.ev_target_soc_override: float | None = None
        self.ev_departure_time: time = time(7, 30)  # overwritten by SolarFriendEVDepartureTime on restore
        self.ev_min_range_km: float = 0.0
        self.vehicle_battery_kwh: float = float(
            entry.data.get("vehicle_battery_capacity_kwh", 77.0)
        )
        self._ev_enabled: bool = entry.data.get("ev_charging_enabled", False)
        self._ev_charger: EVChargerController | None = None
        self._vehicle: VehicleController | None = None
        self._ev_optimizer: EVOptimizer | None = None
        self._ev_last_action_time: datetime | None = None
        self._ev_currently_charging: bool = False
        self._ev_sync_on_startup: bool = True
        self.ev_charging_allowed: bool = True  # styres af SolarFriendEVSwitch

        if self._ev_enabled:
            self._ev_charger = EVChargerController.from_config(hass, entry)
            self._vehicle = VehicleController.from_config(hass, entry)
            self._ev_optimizer = EVOptimizer()
            _LOGGER.info(
                "EV charging enabled: charger=%s vehicle=%s",
                entry.data.get("ev_charger_type", "none"),
                entry.data.get("vehicle_type", "none"),
            )

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def async_startup(self) -> None:
        """Load persisted state and register event listeners."""
        await self._profile.async_load(self.hass)

        # ── Bootstrap consumption profile from recorder (runs once) ───────
        bootstrap_done = self._entry.data.get("bootstrap_done", False)
        load_entity = self._entry.data.get("load_power_sensor", "")

        if not bootstrap_done and load_entity and self._profile.days_collected < 3:
            try:
                entries = await self._profile.bootstrap_from_history(
                    self.hass, load_entity, days=14
                )
                if entries > 0:
                    new_data = {**self._entry.data, "bootstrap_done": True}
                    self.hass.config_entries.async_update_entry(
                        self._entry, data=new_data
                    )
                    _LOGGER.info(
                        "ConsumptionProfile bootstrap færdig: %d timer — "
                        "live data overtager gradvist (n=1 per bucket)",
                        entries,
                    )
            except Exception as exc:
                _LOGGER.warning("Bootstrap fejl (ikke kritisk): %s", exc)
                # Gem IKKE flag ved fejl — prøv igen næste genstart

        battery_cost = float(self._entry.data.get("battery_cost_per_kwh", 0.0))
        self._tracker = BatteryTracker(self.hass, self._entry.entry_id, battery_cost)
        await self._tracker.async_load()
        self._forecast_tracker = ForecastTracker(self.hass, self._entry.entry_id)
        await self._forecast_tracker.async_load()

        self._optimizer = BatteryOptimizer(
            config_entry=self._entry,
            battery_tracker=self._tracker,
            consumption_profile=self._profile,
        )

        self._inverter = InverterController.from_config(self.hass, self._entry)
        if self._inverter.is_configured:
            _LOGGER.info(
                "InverterController: %s klar",
                self._entry.data.get("inverter_type", "deye_klatremis"),
            )
        else:
            _LOGGER.warning(
                "InverterController: ingen Deye-entiteter konfigureret — styring deaktiveret"
            )

        self._register_event_listeners()

    async def async_persist_state(self) -> None:
        """Persist runtime state that should survive restarts."""
        if self._tracker is not None:
            await self._tracker.async_save()
        if self._forecast_tracker is not None:
            await self._forecast_tracker.async_save()

    def _reset_pending_strategy(self) -> None:
        self._pending_strategy = None
        self._pending_strategy_count = 0

    def _mark_strategy_applied(self, result: OptimizeResult, now: datetime, pv_power: float) -> None:
        self._active_strategy_since = now
        self._active_strategy_reference_pv = max(0.0, pv_power)
        self._reset_pending_strategy()

    def _strategy_override_allowed(
        self,
        active_result: OptimizeResult,
        desired_result: OptimizeResult,
        *,
        now: datetime,
        current_soc: float,
        pv_power: float,
        sunset: datetime,
    ) -> bool:
        """Allow immediate strategy switch on major regime changes or safety limits."""
        now = _normalize_local_datetime(now)
        sunset = _normalize_local_datetime(sunset)

        if desired_result.strategy == "ANTI_EXPORT":
            return True

        cfg = self._entry.data
        min_soc = float(cfg.get("battery_min_soc", 10.0))
        max_soc = float(cfg.get("battery_max_soc", 100.0))
        if current_soc <= (min_soc + SOC_OVERRIDE_MARGIN):
            return True
        if current_soc >= (max_soc - SOC_OVERRIDE_MARGIN):
            return True

        if desired_result.strategy == active_result.strategy:
            return True

        if now >= sunset:
            return True

        solar_remaining = self.data.solar_until_sunset if self.data else 0.0
        if desired_result.strategy == "SAVE_SOLAR" and solar_remaining <= SUNSET_OVERRIDE_REMAINING_KWH:
            return True

        reference_pv = max(0.0, self._active_strategy_reference_pv)
        pv_drop_w = max(0.0, reference_pv - max(0.0, pv_power))
        if reference_pv > 0:
            pv_drop_fraction = pv_drop_w / reference_pv
            if (
                pv_drop_w >= PV_DROP_OVERRIDE_MIN_W
                and pv_drop_fraction >= PV_DROP_OVERRIDE_FRACTION
            ):
                return True

        return False

    def _select_strategy_result(
        self,
        desired_result: OptimizeResult,
        *,
        now: datetime,
        current_soc: float,
        pv_power: float,
        sunset: datetime,
    ) -> tuple[OptimizeResult, bool]:
        """Apply hysteresis/hold logic and return (result_to_apply, strategy_changed)."""
        active_result = self.data.optimize_result if self.data else None
        if active_result is None:
            self._mark_strategy_applied(desired_result, now, pv_power)
            return desired_result, True

        if desired_result.strategy == active_result.strategy:
            self._reset_pending_strategy()
            return desired_result, False

        if self._strategy_override_allowed(
            active_result,
            desired_result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
        ):
            self._mark_strategy_applied(desired_result, now, pv_power)
            return desired_result, True

        if self._pending_strategy == desired_result.strategy:
            self._pending_strategy_count += 1
        else:
            self._pending_strategy = desired_result.strategy
            self._pending_strategy_count = 1

        hold_elapsed = (
            self._active_strategy_since is None
            or (now - self._active_strategy_since) >= STRATEGY_SOFT_COOLDOWN
        )
        if hold_elapsed and self._pending_strategy_count >= STRATEGY_CONFIRMATION_REQUIRED:
            self._mark_strategy_applied(desired_result, now, pv_power)
            return desired_result, True

        return active_result, False

    # ------------------------------------------------------------------
    # Event listeners
    # ------------------------------------------------------------------

    def _register_event_listeners(self) -> None:
        """Register state-change listeners for optimizer triggers."""
        cfg = self._entry.data
        watch_entities: list[str] = []

        for key in ("price_sensor", "forecast_sensor"):
            eid = cfg.get(key, "")
            if eid:
                watch_entities.append(eid)

        # Solcast sensor (always watch — harmless if not installed)
        watch_entities.append("sensor.solcast_pv_forecast_forecast_today")

        watch_entities.append("sun.sun")

        soc_sensor = cfg.get("battery_soc_sensor", "")
        if soc_sensor:
            watch_entities.append(soc_sensor)

        for key in ("charge_rate_kw", "battery_min_soc", "battery_max_soc", "min_charge_saving", "cheap_grid_threshold"):
            watch_entities.append(f"number.{key}")

        if watch_entities:
            unsub = async_track_state_change_event(
                self.hass,
                watch_entities,
                self._async_on_relevant_state_change,
            )
            self._unsub_listeners.append(unsub)

        _LOGGER.debug(
            "BatteryOptimizer: registered event listeners for %d entities",
            len(watch_entities),
        )

    @callback
    def _async_on_relevant_state_change(self, event: Event) -> None:
        """Called when a watched entity changes."""
        entity_id: str = event.data.get("entity_id", "")
        soc_sensor = self._entry.data.get("battery_soc_sensor", "")

        if entity_id == "sensor.solcast_pv_forecast_forecast_today":
            used, limit = 0, 0
            try:
                used_state  = self.hass.states.get("sensor.solcast_pv_forecast_api_used")
                limit_state = self.hass.states.get("sensor.solcast_pv_forecast_api_limit")
                if used_state and limit_state:
                    used  = int(float(used_state.state))
                    limit = int(float(limit_state.state))
            except (ValueError, TypeError):
                pass
            _LOGGER.info(
                "Solcast forecast opdateret — kører optimizer (API: %d/%d)",
                used, limit,
            )
            self.hass.async_create_task(self._trigger_optimize(reason="solcast_updated"))
            return

        if entity_id == soc_sensor:
            new_state = event.data.get("new_state")
            if new_state is None or new_state.state in ("unavailable", "unknown", ""):
                return
            try:
                new_soc = float(new_state.state)
            except (ValueError, TypeError):
                return
            if self._last_optimize_soc is not None:
                if abs(new_soc - self._last_optimize_soc) < SOC_TRIGGER_DELTA:
                    return

        self.hass.async_create_task(self._trigger_optimize("event", notify=True))

    def unregister_listeners(self) -> None:
        """Cancel all registered state-change listeners."""
        for unsub in self._unsub_listeners:
            unsub()
        self._unsub_listeners.clear()

    # ------------------------------------------------------------------
    # Optimizer trigger
    # ------------------------------------------------------------------

    async def _trigger_optimize(self, reason: str = "event", *, notify: bool = False) -> None:
        """Run BatteryOptimizer if cooldown has elapsed and data is available."""
        if self._optimizer is None:
            _LOGGER.debug("BatteryOptimizer: not ready yet — skipping (%s)", reason)
            return

        _LOGGER.info("Optimizer triggered: reason=%s", reason)

        # Refresh runtime-configurable values from config entry (changed via number entities)
        _cfg = self._entry.data
        self._optimizer.charge_rate_kw     = float(_cfg.get("charge_rate_kw",     6.0))
        self._optimizer.battery_min_soc    = float(_cfg.get("battery_min_soc",   10.0))
        self._optimizer.battery_max_soc    = float(_cfg.get("battery_max_soc",  100.0))
        self._optimizer.min_charge_saving  = float(_cfg.get("min_charge_saving",  0.10))
        self._optimizer.cheap_grid_threshold = float(_cfg.get("cheap_grid_threshold", LOW_GRID_HOLD_PRICE))

        now = ha_dt.now()

        if (
            self._last_optimize_dt is not None
            and (now - self._last_optimize_dt) < OPTIMIZE_MIN_INTERVAL
        ):
            _LOGGER.debug("BatteryOptimizer: skipping (%s) — cooldown active", reason)
            return

        if self.data is None:
            _LOGGER.debug("BatteryOptimizer: no coordinator data yet — skipping (%s)", reason)
            return

        cfg = self._entry.data

        # ── Raw prices from Energi Data Service / Nordpool sensor ──────────
        raw_prices = self.data.price_data.to_legacy_raw_prices() if self.data.price_data else []

        # Fallback to night-price history when sensor has no attribute list
        if not raw_prices:
            raw_prices = [{"hour": h, "price": p} for h, p in self._night_prices.items()]
            if self.data.price > 0:
                raw_prices.append({"hour": now.hour, "price": self.data.price})

        # ── Forecast ───────────────────────────────────────────────────────
        forecast_data = self.data.forecast_data

        if forecast_data is None:
            _LOGGER.warning("BatteryOptimizer: forecast data not available — using zeros")
            forecast_today    = 0.0
            forecast_tomorrow = 0.0
            hourly_forecast: list = []
        else:
            forecast_today    = forecast_data.total_today_kwh
            forecast_tomorrow = forecast_data.total_tomorrow_kwh
            hourly_forecast   = forecast_data.hourly_forecast

        # ── Sunrise / sunset from sun.sun ──────────────────────────────────
        sunrise: datetime
        sunset: datetime

        sun_state = self.hass.states.get("sun.sun")
        if sun_state is not None:
            def _parse_sun_dt(attr_key: str) -> datetime | None:
                raw = sun_state.attributes.get(attr_key)
                if not raw:
                    return None
                try:
                    return _normalize_local_datetime(datetime.fromisoformat(str(raw)))
                except (ValueError, TypeError):
                    return None

            _sunrise = _parse_sun_dt("next_rising")
            _sunset  = _parse_sun_dt("next_setting")

            if _sunrise is None:
                _LOGGER.warning(
                    "sun.sun mangler 'next_rising' attribut — bruger fallback sunrise=06:00. "
                    "Dette kan give forkert optimizer-adfærd om vinteren!"
                )
            if _sunset is None:
                _LOGGER.warning(
                    "sun.sun mangler 'next_setting' attribut — bruger fallback sunset=20:00. "
                    "Dette kan give forkert optimizer-adfærd om vinteren!"
                )

            sunrise = _sunrise or now.replace(hour=6, minute=0, second=0, microsecond=0)
            sunset  = _sunset  or now.replace(hour=20, minute=0, second=0, microsecond=0)
        else:
            _LOGGER.warning(
                "sun.sun utilgængelig — bruger fallback sunrise=06:00 sunset=20:00. "
                "Dette kan give forkert optimizer-adfærd om vinteren!"
            )
            sunrise = now.replace(hour=6, minute=0, second=0, microsecond=0)
            sunset  = now.replace(hour=20, minute=0, second=0, microsecond=0)

        # ── Current state ─────────────────────────────────────────────────
        now = _normalize_local_datetime(now)
        sunrise = _normalize_local_datetime(sunrise)
        sunset = _normalize_local_datetime(sunset)

        battery_soc = self.data.battery_soc
        if battery_soc is None:
            _LOGGER.debug("BatteryOptimizer: skipping — battery_soc not available yet")
            return

        current_soc = battery_soc
        pv_power    = self.data.pv_power or 0.0
        load_power  = self.data.load_power or 0.0
        is_weekend  = now.weekday() >= 5

        _LOGGER.debug(
            "Optimizer inputs: now=%s sunrise=%s sunset=%s "
            "raw_prices_count=%d is_night=%s soc=%.1f",
            now.strftime("%H:%M"),
            sunrise.strftime("%H:%M") if sunrise else "None",
            sunset.strftime("%H:%M") if sunset else "None",
            len(raw_prices),
            now.time() < sunrise.time() or now.time() > sunset.time(),
            current_soc,
        )

        # ── Call optimizer ────────────────────────────────────────────────
        result = self._optimizer.optimize(
            now=now,
            pv_power=pv_power,
            load_power=load_power,
            current_soc=current_soc,
            raw_prices=raw_prices,
            forecast_today_kwh=forecast_today,
            forecast_tomorrow_kwh=forecast_tomorrow,
            sunrise_time=sunrise,
            sunset_time=sunset,
            is_weekend=is_weekend,
            hourly_forecast=hourly_forecast,
        )

        selected_result, strategy_changed = self._select_strategy_result(
            result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
        )

        self._last_optimize_dt = now
        self._last_optimize_soc = current_soc

        self.data.optimize_result = selected_result

        if selected_result.strategy == "ANTI_EXPORT":
            _LOGGER.warning(
                "Negativ/nul spotpris %.4f kr/kWh — solar sell deaktiveret",
                self.data.price,
            )

        if self._inverter is not None and self._inverter.is_configured:
            await self._inverter.apply(selected_result)

        if notify:
            self.async_set_updated_data(self.data)

        _LOGGER.info(
            "BatteryOptimizer [%s] desired=%s applied=%s changed=%s reason=%s saving=%.2f kr "
            "morning_need=%.1f kWh night_charge=%.1f kWh target_soc=%s "
            "solar_next2h=%.1f kWh solar_remaining=%.1f kWh",
            reason,
            result.strategy,
            selected_result.strategy,
            "yes" if strategy_changed else "no",
            selected_result.reason,
            selected_result.expected_saving_dkk,
            selected_result.morning_need_kwh,
            selected_result.night_charge_kwh,
            f"{selected_result.target_soc:.0f}%" if selected_result.target_soc else "N/A",
            self.data.solar_next_2h if self.data else 0.0,
            self.data.solar_until_sunset if self.data else 0.0,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_state(self, entity_id: str) -> tuple[float | None, bool]:
        """Return (float_value, is_available) for an entity."""
        state = self.hass.states.get(entity_id)
        if state is None or state.state in ("unavailable", "unknown", ""):
            return None, False
        try:
            return float(state.state), True
        except (ValueError, TypeError):
            return None, False

    def _get_raw_prices(self) -> list[dict[str, Any]]:
        """Return the normalised price list from the current data snapshot."""
        if self.data and self.data.price_data is not None:
            return self.data.price_data.to_legacy_raw_prices()
        return []

    def _forecast_kwh_between(self, from_dt: datetime, to_dt: datetime) -> float:
        """Return forecast kWh in a time window from the current snapshot."""
        if self.data is None or self.data.forecast_data is None:
            return 0.0
        return get_forecast_for_period(self.data.forecast_data.hourly_forecast, from_dt, to_dt)

    async def _update_forecast_tracker(
        self,
        *,
        now: datetime,
        pv_power: float,
        forecast_total_today_kwh: float | None,
    ) -> None:
        """Track actual PV generation and forecast quality over time."""
        if self._forecast_tracker is None:
            return

        if self._prev_update_time is None:
            dt_seconds = 0.0
        else:
            dt_seconds = (now - self._prev_update_time).total_seconds()

        self._forecast_tracker.update(
            now=now,
            pv_power_w=pv_power,
            dt_seconds=dt_seconds,
            forecast_total_today_kwh=forecast_total_today_kwh,
        )

        if (
            self._last_forecast_tracker_save is None
            or (now - self._last_forecast_tracker_save) >= timedelta(minutes=15)
        ):
            await self._forecast_tracker.async_save()
            self._last_forecast_tracker_save = now

    def _build_battery_plan(self, data: SolarFriendData, now: datetime) -> list[dict[str, Any]]:
        """Return the optimizer's own horizon plan preview."""
        return self._optimizer.get_last_plan()

    @staticmethod
    def _json_safe(value: Any) -> Any:
        """Convert nested values to JSON-safe primitives."""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): SolarFriendCoordinator._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [SolarFriendCoordinator._json_safe(item) for item in value]
        return value

    def _build_shadow_horizon(
        self,
        data: SolarFriendData,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Build a replayable horizon of price, load and raw/corrected forecast inputs."""
        if data.price_data is None:
            return []

        raw_prices = data.price_data.to_legacy_raw_prices()
        price_by_start: dict[datetime, float] = {}
        for entry in raw_prices:
            raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_dt is None or raw_price is None:
                continue
            try:
                dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
                local_dt = _normalize_local_datetime(dt).replace(minute=0, second=0, microsecond=0)
                if local_dt >= now.replace(minute=0, second=0, microsecond=0):
                    price_by_start[local_dt] = float(raw_price)
            except (TypeError, ValueError):
                continue

        raw_forecast_by_start: dict[datetime, float] = {}
        if data.forecast_data is not None:
            for slot in data.forecast_data.hourly_forecast:
                raw_start = slot.get("period_start")
                if raw_start is None:
                    continue
                try:
                    dt = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
                    local_dt = _normalize_local_datetime(dt).replace(minute=0, second=0, microsecond=0)
                    raw_forecast_by_start[local_dt] = raw_forecast_by_start.get(local_dt, 0.0) + float(
                        slot.get("pv_estimate_kwh", 0.0)
                    )
                except (TypeError, ValueError):
                    continue

        correction_factor = (
            data.forecast_bias_factor_14d
            if data.forecast_correction_valid and data.forecast_bias_factor_14d > 0
            else 1.0
        )

        horizon: list[dict[str, Any]] = []
        for start, price in sorted(price_by_start.items(), key=lambda item: item[0]):
            load_w = float(self._profile.get_predicted_watt(start.hour, start.weekday() >= 5))
            raw_pv_kwh = raw_forecast_by_start.get(start, 0.0)
            corrected_pv_kwh = raw_pv_kwh * correction_factor
            horizon.append(
                {
                    "start": start.isoformat(),
                    "price_dkk": round(price, 4),
                    "forecast_load_w": round(load_w, 1),
                    "forecast_load_kwh": round(load_w / 1000.0, 4),
                    "raw_pv_kwh": round(raw_pv_kwh, 4),
                    "corrected_pv_kwh": round(corrected_pv_kwh, 4),
                    "raw_net_load_kwh": round(max(0.0, (load_w / 1000.0) - raw_pv_kwh), 4),
                    "corrected_net_load_kwh": round(max(0.0, (load_w / 1000.0) - corrected_pv_kwh), 4),
                }
            )

        return horizon

    def _build_shadow_payload(
        self,
        data: SolarFriendData,
        now: datetime,
        *,
        optimizer_ran: bool,
    ) -> dict[str, Any]:
        """Build a structured shadow-log payload for later replay and evaluation."""
        optimize_result = data.optimize_result
        payload = {
            "schema_version": 1,
            "timestamp": now.isoformat(),
            "entry_id": self._entry.entry_id,
            "optimizer_ran": optimizer_ran,
            "current_actuals": {
                "pv_power_w": round(data.pv_power, 1),
                "load_power_w": round(data.load_power, 1),
                "grid_power_w": round(data.grid_power, 1),
                "battery_power_w": round(data.battery_power, 1),
                "battery_soc_pct": round(data.battery_soc, 2),
                "price_dkk": round(data.price, 4),
            },
            "battery_context": {
                "battery_capacity_kwh": float(self._entry.data.get("battery_capacity_kwh", 10.0)),
                "battery_min_soc": float(self._entry.data.get("battery_min_soc", 10.0)),
                "battery_max_soc": float(self._entry.data.get("battery_max_soc", 100.0)),
                "charge_rate_kw": float(self._entry.data.get("charge_rate_kw", 6.0)),
                "battery_cost_per_kwh": float(self._entry.data.get("battery_cost_per_kwh", 0.0)),
                "battery_weighted_cost": round(data.battery_weighted_cost, 4),
                "battery_solar_fraction": round(data.battery_solar_fraction, 4),
                "battery_solar_kwh": round(data.battery_solar_kwh, 4),
                "battery_grid_kwh": round(data.battery_grid_kwh, 4),
            },
            "learning_model": {
                "profile_confidence": data.profile_confidence,
                "profile_days_collected": data.profile_days_collected,
                "consumption_profile_day_type": data.consumption_profile_day_type,
                "consumption_profile_chart_w": [round(v, 1) for v in data.consumption_profile_chart],
            },
            "forecast_quality": {
                "forecast_type": data.forecast_data.forecast_type if data.forecast_data else None,
                "forecast_confidence": data.forecast_data.confidence if data.forecast_data else None,
                "forecast_correction_valid": data.forecast_correction_valid,
                "forecast_bias_factor_14d": data.forecast_bias_factor_14d,
                "forecast_mae_14d_kwh": data.forecast_mae_14d_kwh,
                "forecast_mape_14d_pct": data.forecast_mape_14d_pct,
                "forecast_accuracy_14d_pct": data.forecast_accuracy_14d_pct,
                "forecast_valid_days_14d": data.forecast_valid_days_14d,
                "forecast_actual_today_so_far_kwh": data.forecast_actual_today_so_far_kwh,
                "forecast_predicted_today_so_far_kwh": data.forecast_predicted_today_so_far_kwh,
                "forecast_error_today_so_far_kwh": data.forecast_error_today_so_far_kwh,
                "forecast_accuracy_today_so_far_pct": data.forecast_accuracy_today_so_far_pct,
                "forecast_actual_yesterday_kwh": data.forecast_actual_yesterday_kwh,
                "forecast_predicted_yesterday_kwh": data.forecast_predicted_yesterday_kwh,
                "forecast_error_yesterday_kwh": data.forecast_error_yesterday_kwh,
                "forecast_accuracy_yesterday_pct": data.forecast_accuracy_yesterday_pct,
                "forecast_history_14d": self._json_safe(data.forecast_history_14d),
            },
            "forecast_snapshot": {
                "total_today_kwh": data.forecast_data.total_today_kwh if data.forecast_data else None,
                "total_tomorrow_kwh": data.forecast_data.total_tomorrow_kwh if data.forecast_data else None,
                "remaining_today_kwh": data.forecast_data.remaining_today_kwh if data.forecast_data else None,
                "power_now_w": data.forecast_data.power_now_w if data.forecast_data else None,
                "power_next_hour_w": data.forecast_data.power_next_hour_w if data.forecast_data else None,
                "solar_next_2h_kwh": data.solar_next_2h,
                "solar_until_sunset_kwh": data.solar_until_sunset,
                "raw_hourly_forecast": self._json_safe(data.forecast_data.hourly_forecast if data.forecast_data else []),
                "corrected_hourly_forecast": self._json_safe(
                    [
                        {
                            **slot,
                            "pv_estimate_kwh": round(
                                float(slot.get("pv_estimate_kwh", 0.0))
                                * (
                                    data.forecast_bias_factor_14d
                                    if data.forecast_correction_valid and data.forecast_bias_factor_14d > 0
                                    else 1.0
                                ),
                                4,
                            ),
                        }
                        for slot in (data.forecast_data.hourly_forecast if data.forecast_data else [])
                    ]
                ),
            },
            "optimizer_inputs": {
                "price_horizon": self._build_shadow_horizon(data, now),
                "raw_prices": self._json_safe(data.price_data.to_legacy_raw_prices() if data.price_data else []),
            },
            "optimizer_output": {
                "strategy": optimize_result.strategy if optimize_result else None,
                "reason": optimize_result.reason if optimize_result else None,
                "target_soc": optimize_result.target_soc if optimize_result else None,
                "charge_now": optimize_result.charge_now if optimize_result else None,
                "cheapest_charge_hour": optimize_result.cheapest_charge_hour if optimize_result else None,
                "night_charge_kwh": optimize_result.night_charge_kwh if optimize_result else None,
                "morning_need_kwh": optimize_result.morning_need_kwh if optimize_result else None,
                "day_deficit_kwh": optimize_result.day_deficit_kwh if optimize_result else None,
                "peak_need_kwh": optimize_result.peak_need_kwh if optimize_result else None,
                "expected_saving_dkk": optimize_result.expected_saving_dkk if optimize_result else None,
                "best_discharge_hours": optimize_result.best_discharge_hours if optimize_result else [],
                "battery_plan": self._json_safe(data.battery_plan),
                "forecast_soc_chart": self._json_safe(data.forecast_soc_chart),
            },
        }
        return payload

    async def _append_shadow_log(self, payload: dict[str, Any]) -> None:
        """Append a JSONL shadow-log row."""
        if not self._shadow_log_enabled:
            return

        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"

        def _write() -> None:
            self._shadow_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._shadow_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)

        try:
            async with self._shadow_log_lock:
                await asyncio.to_thread(_write)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.debug("Shadow log write failed: %s", exc)

    def _update_price_history(self, price: float) -> None:
        self._price_history.append(price)
        if len(self._price_history) > PRICE_HISTORY_MAX:
            self._price_history.pop(0)

    def _price_average(self) -> float | None:
        if not self._price_history:
            return None
        return statistics.mean(self._price_history)

    def _battery_strategy(
        self, solar_surplus: float, price: float, avg_price: float | None
    ) -> str:
        if solar_surplus > 0:
            return "CHARGE_SOLAR"
        if avg_price is not None:
            if price > avg_price * PRICE_SURPLUS_FACTOR:
                return "USE_BATTERY"
            if price < avg_price * PRICE_CHEAP_FACTOR:
                return "CHARGE_GRID"
        return "IDLE"

    def _record_night_price(self, hour: int, price: float) -> None:
        if hour not in NIGHT_HOURS:
            return
        existing = self._night_prices.get(hour)
        if existing is None or price < existing:
            self._night_prices[hour] = price

    def _min_night_price(self) -> float | None:
        if not self._night_prices:
            return None
        return min(self._night_prices.values())

    def _price_level(self, price: float, avg_price: float | None) -> str:
        if avg_price is None:
            return "NORMAL"
        if price > avg_price * PRICE_SURPLUS_FACTOR:
            return "EXPENSIVE"
        if price < avg_price * PRICE_CHEAP_FACTOR:
            return "CHEAP"
        return "NORMAL"

    async def _update_tracker(
        self,
        now: datetime,
        pv_power: float,
        battery_power: float,
        load_power: float,
        battery_soc: float,
        current_price: float,
    ) -> None:
        """Feed BatteryTracker with this tick's charge/discharge delta."""
        if self._tracker is None:
            return

        # dt in hours since last tick
        if self._prev_update_time is None:
            dt_hours = 0.0
        else:
            dt_hours = (now - self._prev_update_time).total_seconds() / 3600

        if dt_hours > 0:
            surplus_w = pv_power - load_power

            if battery_power < -_BATTERY_NOISE_W:
                # Charging
                charge_kwh = abs(battery_power) / 1000 * dt_hours
                if surplus_w > _BATTERY_NOISE_W:
                    self._tracker.on_solar_charge(charge_kwh)
                else:
                    self._tracker.on_grid_charge(charge_kwh, current_price)

            elif battery_power > _BATTERY_NOISE_W:
                # Discharging
                discharge_kwh = battery_power / 1000 * dt_hours
                self._tracker.on_discharge(discharge_kwh)

            self._tracker.update_savings(
                pv_w=pv_power,
                load_w=load_power,
                battery_w=battery_power,
                price_dkk=current_price,
                dt_seconds=dt_hours * 3600,
            )

        # SOC correction every 5 minutes
        if self._last_soc_correction is None or (now - self._last_soc_correction) >= timedelta(minutes=5):
            cfg = self._entry.data
            self._tracker.on_soc_correction(
                actual_soc=battery_soc,
                capacity_kwh=float(cfg.get("battery_capacity_kwh", 0.0)),
                min_soc=float(cfg.get("battery_min_soc", 0.0)),
            )
            self._last_soc_correction = now

        # Battery sign convention heuristic — warn once if polarity looks inverted
        if not self._battery_sign_warned and self.data is not None:
            prev_soc = self.data.battery_soc
            if (
                prev_soc > 0
                and battery_soc - prev_soc > 2.0        # SOC steg tydeligt
                and battery_power > _BATTERY_NOISE_W    # men power er positiv (= aflader?)
            ):
                _LOGGER.warning(
                    "battery_power ser ud til at have OMVENDT FORTEGN! "
                    "SOC steg %.1f%% → %.1f%% mens battery_power=%.0fW (positiv). "
                    "Forventet konvention: negativ = lader, positiv = aflader. "
                    "Tjek din battery_power_sensor konfiguration.",
                    prev_soc, battery_soc, battery_power,
                )
                self._battery_sign_warned = True

        # Persist tracker every 15 minutes
        if self._last_tracker_save is None or (now - self._last_tracker_save) >= timedelta(minutes=15):
            await self._tracker.async_save()
            self._last_tracker_save = now

    # ------------------------------------------------------------------
    # DataUpdateCoordinator entrypoint
    # ------------------------------------------------------------------

    async def _async_update_data(self) -> SolarFriendData:
        cfg = self._entry.data
        prev_data = self.data  # may be None on first call
        data = SolarFriendData()

        # Carry over optimizer + forecast results between polling cycles
        if prev_data is not None:
            data.optimize_result = prev_data.optimize_result
            data.forecast_data   = prev_data.forecast_data

        sensor_map: dict[str, str] = {
            "pv_power":      cfg.get("pv_power_sensor", ""),
            "pv2_power":     cfg.get("pv2_power_sensor", ""),
            "grid_power":    cfg.get("grid_power_sensor", ""),
            "battery_soc":   cfg.get("battery_soc_sensor", ""),
            "battery_power": cfg.get("battery_power_sensor", ""),
            "load_power":    cfg.get("load_power_sensor", ""),
        }

        readings: dict[str, float] = {}
        unavailable: list[str] = []

        for field_name, entity_id in sensor_map.items():
            if not entity_id:
                unavailable.append(field_name)
                continue
            value, available = self._read_state(entity_id)
            if available and value is not None:
                readings[field_name] = value
            else:
                unavailable.append(field_name)
                _LOGGER.debug("Sensor unavailable: %s (%s)", field_name, entity_id)

        now = ha_dt.now()

        price_snapshot = PriceAdapter.from_hass(self.hass, cfg.get("price_sensor"))
        if price_snapshot is None or price_snapshot.current_price is None:
            unavailable.append("price")
        else:
            data.price_data = price_snapshot
            data.price = price_snapshot.current_price

        forecast_snapshot = await ForecastAdapter.from_hass(
            hass=self.hass,
            forecast_type=cfg.get("forecast_type", "forecast_solar"),
            forecast_sensor_entity=cfg.get("forecast_sensor"),
        )
        if forecast_snapshot is None:
            unavailable.append("forecast")
        else:
            data.forecast_data = forecast_snapshot
            data.forecast = forecast_snapshot.total_today_kwh

        if unavailable:
            data.unavailable = unavailable

        # Populate raw readings — sum PV1 + PV2 if both are configured
        data.pv_power      = readings.get("pv_power", 0.0) + readings.get("pv2_power", 0.0)
        data.grid_power    = readings.get("grid_power", 0.0)
        data.battery_soc   = readings.get("battery_soc", 0.0)
        data.battery_power = readings.get("battery_power", 0.0)
        data.load_power    = readings.get("load_power", 0.0)

        # Derived: surplus
        data.solar_surplus = data.pv_power - data.load_power

        # Rolling price history and night price tracking
        if data.price_data is not None and data.price_data.current_price is not None:
            self._update_price_history(data.price)
            self._record_night_price(now.hour, data.price)

        avg = self._price_average()
        data.battery_strategy = self._battery_strategy(data.solar_surplus, data.price, avg)
        data.price_level = self._price_level(data.price, avg)

        # Battery economics
        data.battery_cost_per_kwh = float(cfg.get("battery_cost_per_kwh", 0.0))
        min_night = self._min_night_price()
        if min_night is not None:
            data.charge_threshold = round(min_night + data.battery_cost_per_kwh, 4)

        # Update consumption profile every 15 minutes

        # ── BatteryTracker update ─────────────────────────────────────────
        have_tracker_inputs = (
            "pv_power" in readings
            and "battery_power" in readings
            and "battery_soc" in readings
        )
        if have_tracker_inputs:
            await self._update_tracker(
                now=now,
                pv_power=data.pv_power,
                battery_power=data.battery_power,
                load_power=data.load_power,
                battery_soc=data.battery_soc,
                current_price=data.price,
            )

        if "pv_power" in readings or "pv2_power" in readings:
            await self._update_forecast_tracker(
                now=now,
                pv_power=data.pv_power,
                forecast_total_today_kwh=(
                    data.forecast_data.total_today_kwh if data.forecast_data else None
                ),
            )

        self._prev_battery_power = data.battery_power
        self._prev_update_time = now

        # Snapshot tracker into data
        if self._tracker is not None:
            data.battery_solar_kwh    = self._tracker.solar_kwh
            data.battery_grid_kwh     = self._tracker.grid_kwh
            data.battery_weighted_cost = self._tracker.weighted_cost
            data.battery_solar_fraction = self._tracker.solar_fraction
            data.today_solar_direct_kwh = self._tracker.today_solar_direct_kwh
            data.today_solar_direct_saved_dkk = self._tracker.today_solar_direct_saved_dkk
            data.today_optimizer_saved_dkk = self._tracker.today_optimizer_saved_dkk
            data.total_solar_direct_saved_dkk = self._tracker.total_solar_direct_saved_dkk
            data.total_optimizer_saved_dkk = self._tracker.total_optimizer_saved_dkk

        if self._forecast_tracker is not None:
            metrics = self._forecast_tracker.build_metrics(now, data.forecast_data)
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

        # ── Consumption profile chart (24h curve) ────────────────────────────
        is_weekend = now.weekday() >= 5
        profile_key = "weekend" if is_weekend else "weekday"
        hourly: list[float] = []
        for hour in range(24):
            slot = self._profile._profiles[profile_key][hour]
            avg = round(slot["avg_watt"], 1) if slot["samples"] >= 3 else 0.0
            hourly.append(avg)
        data.consumption_profile_chart = hourly
        data.consumption_profile_day_type = profile_key

        # ── Forecast SOC curve (24h simulation) ──────────────────────────
        current_soc = data.battery_soc or 35.0
        capacity_kwh = float(cfg.get("battery_capacity_kwh", 10.0))
        min_soc = float(cfg.get("battery_min_soc", 10.0))
        max_soc = 100.0  # forecast shows physical maximum, not optimizer limit
        current_hour = now.hour

        solcast_hourly: dict[int, float] = {}
        if data.forecast_data and data.forecast_data.hourly_forecast:
            for slot in data.forecast_data.hourly_forecast:
                h = slot["period_start"].hour
                # Accumulate both 30-min slots → full hourly kWh stored as *1000 for later /1000
                solcast_hourly[h] = solcast_hourly.get(h, 0.0) + slot.get("pv_estimate_kwh", 0.0) * 1000

        forecast_soc: list = []
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

        # ── Solar-derived sensors (next 2h + until sunset) ───────────────
        if data.forecast_data is not None and data.forecast_data.hourly_forecast:
            data.solar_next_2h = get_forecast_for_period(
                data.forecast_data.hourly_forecast, now, now + timedelta(hours=2)
            )
            sun_state = self.hass.states.get("sun.sun")
            if sun_state is not None:
                raw_sunset = sun_state.attributes.get("next_setting")
                if raw_sunset:
                    try:
                        sunset_dt = _normalize_local_datetime(datetime.fromisoformat(str(raw_sunset)))
                        data.solar_until_sunset = get_forecast_for_period(
                            data.forecast_data.hourly_forecast, now, sunset_dt
                        )
                    except (ValueError, TypeError):
                        pass

        # ── Hourly optimizer fallback ─────────────────────────────────────
        should_run_hourly = (
            self._last_optimize_dt is None
            or (now - self._last_optimize_dt) >= timedelta(hours=1)
        )
        if should_run_hourly:
            # Temporarily expose the new data so _trigger_optimize can read it
            self.data = data  # type: ignore[assignment]
            await self._trigger_optimize("hourly-fallback", notify=False)
            # Pull back whatever the optimizer wrote
            data.optimize_result = self.data.optimize_result if self.data else data.optimize_result

        data.battery_plan = self._build_battery_plan(data, now)

        # ── EV charging ───────────────────────────────────────────────────
        if self._ev_enabled:
            self.data = data  # type: ignore[assignment]
            await self._update_ev()

        if "load_power" in readings:
            await self._maybe_update_profile(
                data.load_power,
                ev_power_w=data.ev_charging_power,
                battery_power_w=data.battery_power,
            )

        data.profile_confidence = self._profile.confidence
        data.profile_days_collected = self._profile.days_collected

        await self._append_shadow_log(
            self._build_shadow_payload(
                data,
                now,
                optimizer_ran=should_run_hourly,
            )
        )

        _LOGGER.debug(
            "Update: PV=%.0fW load=%.0fW battery=%.0fW SOC=%.1f%% "
            "price=%.4f avg=%.4f strategy=%s level=%s "
            "solar_kwh=%.3f grid_kwh=%.3f weighted_cost=%.3f",
            data.pv_power, data.load_power, data.battery_power, data.battery_soc,
            data.price, avg or 0, data.battery_strategy, data.price_level,
            data.battery_solar_kwh, data.battery_grid_kwh, data.battery_weighted_cost,
        )

        return data

    async def _update_ev(self) -> None:
        """Run EV optimizer and act on the result."""
        try:
            charger_status = await self._ev_charger.get_status()
            charger_power = await self._ev_charger.get_power_w()
            vehicle_soc = self._vehicle.get_soc()
            vehicle_target_soc = (
                self.ev_target_soc_override
                if self.ev_target_soc_override is not None
                else self._vehicle.get_target_soc()
            )

            # Synkroniser _ev_currently_charging fra faktisk status ved opstart
            if self._ev_sync_on_startup:
                self._ev_sync_on_startup = False
                self._ev_currently_charging = charger_status == "charging"
                if self._ev_currently_charging:
                    _LOGGER.info("EV: synkroniseret til charging ved opstart")

            # Manuel EV-switch: hvis slukket → behandl som disconnected
            if not self.ev_charging_allowed:
                charger_status = "disconnected"
                _LOGGER.debug("EV: ladning deaktiveret af manuel switch")

            vehicle_efficiency = float(
                self._entry.data.get("vehicle_efficiency_km_per_kwh", 6.0)
            )
            driving_range_km = self._vehicle.get_driving_range()
            min_range_km = self.ev_min_range_km

            _now = ha_dt.now()
            _departure = self.ev_next_departure

            # Solar kWh expected from now until departure — reads both Solcast sensors
            # because forecast_today only covers until midnight.
            # Normalize every timestamp to local time before comparing, matching
            # the same approach used in _compute_ev_plan.
            solar_to_departure = self._forecast_kwh_between(_now, _departure)
            _LOGGER.debug(
                "EV solar forecast til afgang %s: %.2f kWh",
                _departure.strftime("%H:%M"),
                solar_to_departure,
            )

            # Expected SOC at this moment from previous plan — used for behind-schedule check
            _now_hour = _now.replace(minute=0, second=0, microsecond=0)
            _expected_soc = self.data.ev_vehicle_soc or 0.0
            for _slot in self.data.ev_plan:
                try:
                    _slot_dt = datetime.fromisoformat(_slot["hour"])
                    if abs((_slot_dt - _now_hour).total_seconds()) < 3600:
                        _expected_soc = _slot["soc"]
                        break
                except (KeyError, ValueError, TypeError):
                    pass

            ctx = EVContext(
                pv_power_w=self.data.pv_power,
                load_power_w=self.data.load_power,
                battery_charging_w=self.data.battery_power,
                battery_soc=self.data.battery_soc,
                battery_capacity_kwh=float(
                    self._entry.data.get("battery_capacity_kwh", 10.0)
                ),
                battery_min_soc=float(
                    self._entry.data.get("battery_min_soc", 10.0)
                ),
                charger_status=charger_status,
                currently_charging=self._ev_currently_charging,
                vehicle_soc=vehicle_soc,
                vehicle_capacity_kwh=self.vehicle_battery_kwh,
                vehicle_target_soc=vehicle_target_soc,
                departure=_departure,
                current_price=self.data.price,
                raw_prices=self._get_raw_prices(),
                max_charge_kw=float(
                    self._entry.data.get("ev_max_charge_kw", 7.4)
                ),
                driving_range_km=driving_range_km,
                min_range_km=min_range_km,
                vehicle_efficiency_km_per_kwh=vehicle_efficiency,
                now=_now,
                solar_forecast_to_departure_kwh=solar_to_departure,
                ev_plan_expected_soc_now=_expected_soc,
                current_price_dkk=self.data.price,
            )

            ev_result = self._ev_optimizer.optimize(ctx, mode=self.ev_charge_mode)

            # Opdater data FØRST — altid konsistente sensorer selv hvis service calls fejler
            self.data.ev_charging_enabled = True
            self.data.ev_charging_power = charger_power
            self.data.ev_vehicle_soc = vehicle_soc
            self.data.ev_target_soc = vehicle_target_soc
            self.data.ev_surplus_w = ev_result.surplus_w
            self.data.ev_strategy_reason = ev_result.reason
            self.data.ev_charger_status = charger_status
            self.data.ev_target_w = ev_result.target_w if ev_result.should_charge else 0.0
            self.data.ev_phases = ev_result.phases if ev_result.should_charge else 0
            self.data.ev_vehicle_soc_kwh = round(
                vehicle_soc / 100 * self.vehicle_battery_kwh, 2
            )
            self.data.ev_needed_kwh = round(
                max(0.0, (vehicle_target_soc - vehicle_soc) / 100 * self.vehicle_battery_kwh), 2
            )
            departure = self.ev_next_departure
            self.data.ev_hours_to_departure = round(
                (departure - ha_dt.now()).total_seconds() / 3600, 1
            )
            self.data.ev_charge_mode = self.ev_charge_mode
            self.data.ev_min_range_km = min_range_km
            self.data.ev_emergency_charging = ev_result.is_emergency
            # Compute min SOC needed to cover min_range_km (for sensor display)
            if min_range_km > 0 and vehicle_efficiency > 0 and self.vehicle_battery_kwh > 0:
                self.data.ev_min_soc_from_range = min(
                    100.0,
                    min_range_km / vehicle_efficiency / self.vehicle_battery_kwh * 100,
                )
            else:
                self.data.ev_min_soc_from_range = 0.0

            # Anti-flap: vent mindst 5 min mellem handlinger
            now = ha_dt.now()
            can_act = (
                self._ev_last_action_time is None
                or (now - self._ev_last_action_time).total_seconds() > 300
            )

            if can_act:
                if ev_result.should_charge and not self._ev_currently_charging:
                    await self._ev_charger.resume()
                    await asyncio.sleep(2)  # FIX 3: vent på Easee at komme ud af pause
                    await self._ev_charger.set_power(ev_result.target_w, ev_result.phases)
                    self._ev_currently_charging = True
                    self._ev_last_action_time = now
                    _LOGGER.info(
                        "EV: start ladning %d-fase %.1fA (%.0fW) — %s",
                        ev_result.phases, ev_result.target_amps,
                        ev_result.target_w, ev_result.reason,
                    )

                elif ev_result.should_charge and self._ev_currently_charging:
                    await self._ev_charger.set_power(ev_result.target_w, ev_result.phases)

                elif not ev_result.should_charge and self._ev_currently_charging:
                    await self._ev_charger.pause()
                    self._ev_currently_charging = False
                    self._ev_last_action_time = now
                    _LOGGER.info("EV: stop ladning — %s", ev_result.reason)

            # Compute EV plan after data is updated
            self.data.ev_plan = self._compute_ev_plan()

        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("EV update fejlede: %s", e)

    def _compute_ev_plan(self) -> list[dict]:
        """Simulate EV charging hour-by-hour from now until departure.

        Returns a list of dicts with keys: hour (ISO timestamp), soc, solar_w, grid_w, total_w.

        hybrid / grid_schedule logic:
          1. Calculate how many hours of full charging are needed to reach target_soc.
          2. Pick the cheapest N hour-slots within the window.
          3. In cheap slots: charge at full rate (solar covers what it can, grid tops up).
          4. Outside cheap slots (hybrid) or in non-scheduled slots (grid_schedule):
             charge only when solar surplus >= MIN_SURPLUS_W threshold.
          solar_only: never use grid — solar surplus only.
        """
        if self.data is None:
            return []

        import math

        # Minimum surplus to start 1-phase charging (mirrors ev_optimizer.MIN_SURPLUS_W)
        _MIN_SURPLUS_W = 1410.0

        now = ha_dt.now()
        departure = self.ev_next_departure
        current_soc = self.data.ev_vehicle_soc or 0.0
        target_soc = self.data.ev_target_soc or 80.0
        capacity_kwh = max(0.1, self.vehicle_battery_kwh)
        max_charge_kw = float(self._entry.data.get("ev_max_charge_kw", 7.4))
        max_charge_w = max_charge_kw * 1000.0
        mode = self.ev_charge_mode

        # ── Solar forecast lookup by hour (W) ────────────────────────────
        solar_by_hour: dict[int, float] = {}
        if self.data.forecast_data and self.data.forecast_data.hourly_forecast:
            for slot in self.data.forecast_data.hourly_forecast:
                h_slot = slot["period_start"].hour
                # Accumulate both 30-min slots → full hourly kWh, then *2000 gives avg W for surplus check
                solar_by_hour[h_slot] = solar_by_hour.get(h_slot, 0.0) + slot.get("pv_estimate_kwh", 0.0) * 1000.0

        hours_in_window = max(1, int((departure - now).total_seconds() / 3600) + 1)
        hours_in_window = min(hours_in_window, 24)

        slot_base = now.replace(minute=0, second=0, microsecond=0)
        slot_dts = [slot_base + timedelta(hours=i) for i in range(hours_in_window)]

        # ── Price lookup mapped to slot datetime ─────────────────────────
        # Keys are UTC hour-truncated datetimes to avoid timezone object
        # mismatches (e.g. ZoneInfo vs fixed-offset) causing dict misses.
        # Handles both EDS ("hour": ISO string) and Nordpool ("start": ISO/dt).
        # Fallback dict (hour int → price) for sensors that only provide an
        # integer hour with no date (cannot disambiguate multi-day windows).
        raw_prices = self._get_raw_prices()
        price_by_utc: dict[datetime, float] = {}
        price_by_hour_fallback: dict[int, float] = {}
        for p in raw_prices:
            raw_price = p.get("price") if p.get("price") is not None else p.get("value")
            if raw_price is None:
                continue
            # Try "start" (Nordpool) then "hour" (EDS ISO string) as datetime source
            raw_dt = p.get("start") or p.get("hour")
            if raw_dt is not None:
                try:
                    dt = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
                    dt_utc = (
                        ha_dt.as_utc(dt) if dt.tzinfo is not None
                        else ha_dt.as_utc(ha_dt.as_local(dt))
                    )
                    price_by_utc[dt_utc.replace(minute=0, second=0, microsecond=0)] = float(raw_price)
                    continue
                except (ValueError, TypeError):
                    pass
            # Last resort: integer hour key (no date, 24-hour window only)
            h = p.get("hour")
            if h is not None:
                try:
                    price_by_hour_fallback[int(h)] = float(raw_price)
                except (TypeError, ValueError):
                    pass

        _LOGGER.debug(
            "_compute_ev_plan: price_by_utc=%d entries raw_prices=%d "
            "departure=%s first_5_utc=%s",
            len(price_by_utc), len(raw_prices), departure,
            list(price_by_utc.keys())[:5],
        )

        def _price_for(slot_dt: datetime, default: float = 0.0) -> float:
            slot_utc = (
                ha_dt.as_utc(slot_dt) if slot_dt.tzinfo is not None
                else ha_dt.as_utc(ha_dt.as_local(slot_dt))
            ).replace(minute=0, second=0, microsecond=0)
            if slot_utc in price_by_utc:
                return price_by_utc[slot_utc]
            fallback = price_by_hour_fallback.get(slot_dt.hour, default)
            _LOGGER.debug(
                "_price_for: %s (UTC %s) → no match in price_by_utc, fallback=%.4f",
                slot_dt.isoformat(), slot_utc.isoformat(), fallback,
            )
            return fallback

        # ── Solar forecast to departure (for hybrid grid-hour reduction) ──
        # Read directly from both Solcast sensors (same logic as _update_ev) so
        # the plan and optimizer always use the same solar estimate.
        solar_to_departure_kwh = self._forecast_kwh_between(now, departure)

        # ── Determine cheap slots for hybrid / grid_schedule ─────────────
        cheap_slot_set: set[int] = set()  # indices into slot_dts
        total_needed_kwh = max(0.0, (target_soc - current_soc) / 100.0 * capacity_kwh)
        grid_needed_kwh = 0.0
        if mode in ("hybrid", "grid_schedule"):
            # hybrid: solar covers part of the need — only schedule grid for the remainder
            if mode == "hybrid":
                grid_needed_kwh = max(0.0, total_needed_kwh - solar_to_departure_kwh)
            else:
                grid_needed_kwh = total_needed_kwh
            needed_hours = math.ceil(grid_needed_kwh / max_charge_kw) if grid_needed_kwh > 0 and max_charge_kw > 0 else 0
            # Sort slots by price, pick cheapest N
            priced = sorted(
                range(hours_in_window),
                key=lambda i: _price_for(slot_dts[i], default=9999.0),
            )
            cheap_slot_set = set(priced[:needed_hours])
            _LOGGER.debug(
                "EV plan hybrid DEBUG: "
                "current_soc=%.1f%% target_soc=%.1f%% capacity=%.1f kWh "
                "total_needed=%.2f kWh solar_forecast=%.2f kWh "
                "grid_needed=%.2f kWh grid_hours=%d max_charge_kw=%.1f",
                current_soc, target_soc, capacity_kwh,
                total_needed_kwh, solar_to_departure_kwh,
                grid_needed_kwh, needed_hours, max_charge_kw,
            )
            _LOGGER.debug(
                "EV plan hybrid cheap_hours: %s",
                sorted(slot_dts[i].isoformat() for i in cheap_slot_set),
            )

        # ── Build plan hour by hour ───────────────────────────────────────
        plan: list[dict] = []
        soc = current_soc
        remaining_grid_kwh = grid_needed_kwh

        for i, hour_dt in enumerate(slot_dts):
            hour = hour_dt.hour
            solar_w = solar_by_hour.get(hour, 0.0)
            load_w = (
                self.data.consumption_profile_chart[hour]
                if self.data.consumption_profile_chart
                else 850.0
            )
            surplus_w = max(0.0, solar_w - load_w)

            remaining_kwh = max(0.0, (target_soc - soc) / 100.0 * capacity_kwh)
            remaining_hours = max(0.1, (departure - hour_dt).total_seconds() / 3600.0)
            max_useful_w = min(
                max_charge_w,
                (remaining_kwh / remaining_hours) * 1000.0 if remaining_kwh > 0 else 0.0,
            )

            solar_contribution = 0.0
            grid_contribution = 0.0
            if soc < target_soc and max_useful_w > 0:
                if mode == "solar_only":
                    # Only solar surplus — respect minimum threshold
                    if surplus_w >= _MIN_SURPLUS_W:
                        solar_contribution = min(surplus_w, max_useful_w)
                elif mode in ("hybrid", "grid_schedule"):
                    if i in cheap_slot_set:
                        solar_contribution = min(surplus_w, max_useful_w, max_charge_w)
                        cheap_slots_left = sum(1 for idx in cheap_slot_set if idx >= i)
                        grid_budget_w = (
                            (remaining_grid_kwh / max(1, cheap_slots_left)) * 1000.0
                            if remaining_grid_kwh > 0
                            else 0.0
                        )
                        grid_contribution = min(
                            grid_budget_w,
                            max(0.0, max_charge_w - solar_contribution),
                            max(0.0, max_useful_w - solar_contribution),
                        )
                        remaining_grid_kwh = max(
                            0.0,
                            remaining_grid_kwh - (grid_contribution / 1000.0),
                        )
                    elif surplus_w >= _MIN_SURPLUS_W:
                        # Outside cheap slot: opportunistic solar only
                        solar_contribution = min(surplus_w, max_useful_w, max_charge_w)

            charge_w = solar_contribution + grid_contribution

            soc_gain = (charge_w / 1000.0) / capacity_kwh * 100.0
            soc = min(target_soc, soc + soc_gain)

            slot = {
                "hour": hour_dt.isoformat(),
                "soc": round(soc, 1),
                "solar_w": round(solar_contribution),
                "grid_w": round(grid_contribution),
                "total_w": round(charge_w),
                "price_dkk": round(_price_for(hour_dt), 4),
            }
            plan.append(slot)
            _LOGGER.debug(
                "EV plan slot %s: soc=%.1f%% solar=%.0fW grid=%.0fW total=%.0fW price=%.3f",
                slot["hour"], slot["soc"], slot["solar_w"],
                slot["grid_w"], slot["total_w"], slot["price_dkk"],
            )

        return plan

    @property
    def ev_next_departure(self) -> datetime:
        """Næste afgangstidspunkt — altid i fremtiden.

        Reads from self.ev_departure_time (set by SolarFriendEVDepartureTime on restore).
        """
        now = ha_dt.now()
        dep = now.replace(
            hour=self.ev_departure_time.hour,
            minute=self.ev_departure_time.minute,
            second=0,
            microsecond=0,
        )
        if dep <= now:
            dep += timedelta(days=1)
        return dep

    async def _maybe_update_profile(
        self,
        load_watt: float,
        ev_power_w: float = 0.0,
        battery_power_w: float = 0.0,
    ) -> None:
        """Call async_update on the profile at most once every 15 minutes."""
        now = ha_dt.now()
        if (
            self._last_profile_update is None
            or (now - self._last_profile_update) >= timedelta(minutes=15)
        ):
            await self._profile.async_update(
                self.hass, load_watt,
                ev_power_w=ev_power_w,
                battery_power_w=battery_power_w,
            )
            self._last_profile_update = now
