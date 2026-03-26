"""SolarFriend DataUpdateCoordinator.

Coordinator responsibility:
- Orchestrate polling, runtime state and Home Assistant side effects.
- Delegate EV planning to `ev_planning.py`.
- Delegate structured replay logging to `shadow_logging.py`.
- Keep battery optimization in `battery_optimizer.py`.

When adding new logic, prefer these homes:
- EV slot building / EV preview / EV-vs-battery priority: `ev_planning.py`
- Shadow-log payloads and file writes: `shadow_logging.py`
- Battery economics and horizon planning: `battery_optimizer.py`
- Consumption learning and historical seeding: `consumption_profile.py`
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as ha_dt

from .const import CONF_BUY_PRICE_SENSOR, CONF_SELL_PRICE_SENSOR, DOMAIN
from .consumption_profile import ConsumptionProfile
from .battery_tracker import BatteryTracker
from .battery_optimizer import (
    BatteryOptimizer,
    LOW_GRID_HOLD_PRICE,
    OptimizeResult,
)
from .forecast_adapter import ForecastAdapter, ForecastData, get_forecast_for_period
from .forecast_correction_model import ForecastCorrectionModel
from .forecast_tracker import ForecastTracker
from .price_adapter import PriceAdapter, PriceData, get_current_price_from_raw
from .inverter_controller import InverterController
from .ev_charger_controller import EVChargerController
from .vehicle_controller import VehicleController
from .ev_optimizer import EVContext, EVHybridSlot, EVOptimizer
from .ev_planning import EVPlanningHelper
from .shadow_logging import ShadowLogger

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
_PLAN_DEVIATION_MIN_W = 400.0
_PLAN_DEVIATION_FRACTION = 0.25


@dataclass
class SolarFriendData:
    # Raw sensor readings
    pv_power: float = 0.0
    grid_power: float = 0.0
    battery_soc: float = 0.0
    battery_power: float = 0.0
    load_power: float = 0.0
    price: float = 0.0
    sell_price: float = 0.0
    forecast: float = 0.0
    price_data: PriceData | None = None
    sell_price_data: PriceData | None = None

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
    consumption_profile_debug: dict[str, Any] = field(default_factory=dict)

    # Optimizer result (None until first run)
    optimize_result: OptimizeResult | None = None
    plan_optimize_result: OptimizeResult | None = None

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
    forecast_correction_model_state: str = "inactive"
    forecast_correction_current_month: int = 0
    forecast_correction_active_buckets: int = 0
    forecast_correction_confident_buckets: int = 0
    forecast_correction_average_factor_this_month: float = 1.0
    forecast_correction_today_hourly_factors: dict[str, Any] = field(default_factory=dict)
    forecast_correction_current_hour_factor: float = 1.0
    forecast_correction_current_hour_samples: int = 0
    forecast_correction_raw_vs_corrected_delta_today: float = 0.0

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
        self._cached_buy_price_data: PriceData | None = None
        self._cached_sell_price_data: PriceData | None = None
        self._profile = ConsumptionProfile()
        self._last_profile_update: datetime | None = None

        # BatteryTracker — initialised in async_startup
        self._tracker: BatteryTracker | None = None
        self._forecast_tracker: ForecastTracker | None = None
        self._forecast_correction_model: ForecastCorrectionModel | None = None

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
        self._last_plan_optimize_result: OptimizeResult | None = None
        self._active_strategy_since: datetime | None = None
        self._active_strategy_reference_pv: float = 0.0
        self._pending_strategy: str | None = None
        self._pending_strategy_count: int = 0
        self._last_plan_deviation_key: str | None = None
        self._shadow_log_enabled: bool = bool(entry.data.get("shadow_log_enabled", False))
        self._shadow_logger = ShadowLogger(
            entry=entry,
            profile=self._profile,
            log_path=hass.config.path("solarfriend_shadow_log.jsonl"),
            enabled=self._shadow_log_enabled,
        )

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
            self._ev_planning = EVPlanningHelper(
                entry=entry,
                ev_optimizer=self._ev_optimizer,
                vehicle=self._vehicle,
                vehicle_battery_kwh=self.vehicle_battery_kwh,
                ev_min_range_km=self.ev_min_range_km,
                get_raw_prices=self._get_raw_prices,
                forecast_kwh_between=self._forecast_kwh_between,
                normalize_local_datetime=_normalize_local_datetime,
            )
            _LOGGER.info(
                "EV charging enabled: charger=%s vehicle=%s",
                entry.data.get("ev_charger_type", "none"),
                entry.data.get("vehicle_type", "none"),
            )
        else:
            self._ev_planning = None

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
                        "ConsumptionProfile bootstrap faerdig: %d timer - historiske buckets er nu straks brugbare",
                        entries,
                    )
            except Exception as exc:
                _LOGGER.warning("Bootstrap fejl (ikke kritisk): %s", exc)
                # Gem IKKE flag ved fejl - prov igen n?ste genstart

        battery_cost = float(self._entry.data.get("battery_cost_per_kwh", 0.0))
        self._tracker = BatteryTracker(self.hass, self._entry.entry_id, battery_cost)
        await self._tracker.async_load()
        self._forecast_tracker = ForecastTracker(self.hass, self._entry.entry_id)
        await self._forecast_tracker.async_load()
        self._forecast_correction_model = ForecastCorrectionModel(self.hass, self._entry.entry_id)
        await self._forecast_correction_model.async_load()

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
                "InverterController: ingen Deye-entiteter konfigureret - styring deaktiveret"
            )

        self._register_event_listeners()

    async def async_persist_state(self) -> None:
        """Persist runtime state that should survive restarts."""
        await self._profile.async_save(self.hass)
        if self._tracker is not None:
            await self._tracker.async_save()
        if self._forecast_tracker is not None:
            await self._forecast_tracker.async_save()
        if self._forecast_correction_model is not None:
            await self._forecast_correction_model.async_save()

    async def async_force_populate_load_model(self, days: int = 14) -> int:
        """Force-populate the load model from recorder history."""
        load_entity = self._entry.data.get("load_power_sensor", "")
        if not load_entity:
            raise ValueError("No load_power_sensor configured")

        days = max(1, min(int(days), 14))
        entries = await self._profile.bootstrap_from_history(
            self.hass,
            load_entity,
            days=days,
            force=True,
        )
        if entries > 0:
            new_data = {**self._entry.data, "bootstrap_done": True}
            self.hass.config_entries.async_update_entry(self._entry, data=new_data)
            if self.data is not None:
                self.data.profile_confidence = self._profile.confidence
                self.data.profile_days_collected = self._profile.days_collected
                self.data.consumption_profile_debug = self._profile.build_debug_snapshot()
                self.data.consumption_profile_chart = [
                    self._profile.get_predicted_watt(hour, False) for hour in range(24)
                ]
            _LOGGER.info(
                "ConsumptionProfile tvangspopuleret fra %d dages historik: %d buckets",
                days,
                entries,
            )
        return entries


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

        for key in (CONF_BUY_PRICE_SENSOR, CONF_SELL_PRICE_SENSOR, "price_sensor", "forecast_sensor"):
            eid = cfg.get(key, "")
            if eid:
                watch_entities.append(eid)

        # Solcast sensor (always watch — harmless if not installed)
        watch_entities.append("sensor.solcast_pv_forecast_forecast_today")

        watch_entities.append("sun.sun")

        soc_sensor = cfg.get("battery_soc_sensor", "")
        if soc_sensor:
            watch_entities.append(soc_sensor)

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

    async def async_on_runtime_setting_changed(self, *, reason: str) -> None:
        """Refresh coordinator data and force a fresh optimizer run."""
        await self.async_request_refresh()
        await self._trigger_optimize(reason=reason, notify=True, force=True)

    # ------------------------------------------------------------------
    # Optimizer trigger
    # ------------------------------------------------------------------

    async def _trigger_optimize(
        self,
        reason: str = "event",
        *,
        notify: bool = False,
        force: bool = False,
    ) -> None:
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
            not force
            and
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
        raw_sell_prices = (
            self.data.sell_price_data.to_legacy_raw_prices()
            if self.data.sell_price_data
            else list(raw_prices)
        )

        # Last-resort fallback when no actual price snapshot is available
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
        reserved_ev_solar_kwh: dict[datetime, float] | None = None
        if self._ev_enabled:
            reserved_ev_solar_kwh = self._build_ev_battery_priority_reservations(
                now,
                self.ev_next_departure,
            )

        result = self._optimizer.optimize(
            now=now,
            pv_power=pv_power,
            load_power=load_power,
            current_soc=current_soc,
            raw_prices=raw_prices,
            raw_sell_prices=raw_sell_prices,
            forecast_today_kwh=forecast_today,
            forecast_tomorrow_kwh=forecast_tomorrow,
            sunrise_time=sunrise,
            sunset_time=sunset,
            is_weekend=is_weekend,
            hourly_forecast=hourly_forecast,
            reserved_solar_kwh=reserved_ev_solar_kwh,
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
        if self._optimizer.get_last_plan():
            self._last_plan_optimize_result = selected_result
            self.data.plan_optimize_result = selected_result

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

    def _trim_price_snapshot(self, snapshot: PriceData, now: datetime) -> PriceData | None:
        """Return a forward-looking price snapshot with past hours removed."""
        raw_prices = snapshot.to_legacy_raw_prices()
        current_hour = _normalize_local_datetime(now).replace(minute=0, second=0, microsecond=0)
        points = [point for point in snapshot.points if point.end > current_hour]
        current_price = get_current_price_from_raw(raw_prices, now, fallback=snapshot.current_price)

        if current_price is None and not points:
            return None

        return PriceData(
            points=points,
            current_price=current_price,
            source_entity=snapshot.source_entity,
        )

    def _resolve_price_snapshot(
        self,
        now: datetime,
        cache_kind: str,
        fresh_snapshot: PriceData | None,
    ) -> PriceData | None:
        """Prefer fresh actual prices, otherwise fall back to the last valid snapshot."""
        cache_attr = "_cached_sell_price_data" if cache_kind == "sell" else "_cached_buy_price_data"
        if fresh_snapshot is not None:
            trimmed_fresh = self._trim_price_snapshot(fresh_snapshot, now)
            if trimmed_fresh is not None:
                setattr(self, cache_attr, trimmed_fresh)
                return trimmed_fresh

        cached_snapshot = getattr(self, cache_attr)
        if cached_snapshot is None:
            return None

        trimmed_cached = self._trim_price_snapshot(cached_snapshot, now)
        if trimmed_cached is None:
            return None

        setattr(self, cache_attr, trimmed_cached)
        return trimmed_cached

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

    def _should_trigger_plan_deviation_replan(
        self,
        *,
        now: datetime,
        battery_power: float,
    ) -> bool:
        """Detect a clear mismatch between planned and actual battery behavior."""
        if self._optimizer is None:
            return False

        plan = self._optimizer.get_last_plan()
        if not plan:
            self._last_plan_deviation_key = None
            return False

        current_hour = now.replace(minute=0, second=0, microsecond=0)
        current_slot = next(
            (
                slot for slot in plan
                if _normalize_local_datetime(datetime.fromisoformat(slot["hour"])).replace(
                    minute=0,
                    second=0,
                    microsecond=0,
                ) == current_hour
            ),
            None,
        )
        if current_slot is None:
            self._last_plan_deviation_key = None
            return False

        planned_discharge_w = float(current_slot.get("discharge_w", 0.0))
        planned_charge_w = float(current_slot.get("grid_charge_w", 0.0)) + float(
            current_slot.get("solar_charge_w", 0.0)
        )
        actual_discharge_w = max(0.0, battery_power)
        actual_charge_w = max(0.0, -battery_power)

        deviation_kind: str | None = None
        if (
            planned_discharge_w >= _PLAN_DEVIATION_MIN_W
            and actual_discharge_w < max(_PLAN_DEVIATION_MIN_W, planned_discharge_w * _PLAN_DEVIATION_FRACTION)
        ):
            deviation_kind = "missed_discharge"
        elif (
            planned_charge_w >= _PLAN_DEVIATION_MIN_W
            and actual_charge_w < max(_PLAN_DEVIATION_MIN_W, planned_charge_w * _PLAN_DEVIATION_FRACTION)
        ):
            deviation_kind = "missed_charge"

        if deviation_kind is None:
            self._last_plan_deviation_key = None
            return False

        deviation_key = f"{current_slot['hour']}|{deviation_kind}"
        if deviation_key == self._last_plan_deviation_key:
            return False

        self._last_plan_deviation_key = deviation_key
        _LOGGER.info(
            "Battery plan deviation detected: %s planned=%.0fW actual_battery=%.0fW slot=%s",
            deviation_kind,
            planned_discharge_w if deviation_kind == "missed_discharge" else planned_charge_w,
            battery_power,
            current_slot["hour_str"],
        )
        return True

    @staticmethod
    def _json_safe(value: Any) -> Any:
        """Convert nested values to JSON-safe primitives."""
        return ShadowLogger.json_safe(value)

    def _build_shadow_horizon(
        self,
        data: SolarFriendData,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Build a replayable horizon of price, load and forecast inputs."""
        return self._shadow_logger.build_horizon(data, now, _normalize_local_datetime)

    def _build_ev_battery_priority_reservations(
        self,
        now: datetime,
        departure: datetime,
    ) -> dict[datetime, float]:
        """Reserve EV solar before departure when EV should win over the house battery."""
        if self._ev_planning is None or self.data is None:
            return {}
        return self._ev_planning.build_ev_battery_priority_reservations(
            ev_enabled=self._ev_enabled,
            ev_charging_allowed=self.ev_charging_allowed,
            data=self.data,
            ev_charge_mode=self.ev_charge_mode,
            ev_currently_charging=self._ev_currently_charging,
            ev_min_range_km=self.ev_min_range_km,
            vehicle_target_soc_override=self.ev_target_soc_override,
            now=now,
            departure=departure,
            ev_next_departure=self.ev_next_departure,
        )

    def _build_shadow_payload(
        self,
        data: SolarFriendData,
        now: datetime,
        *,
        optimizer_ran: bool,
    ) -> dict[str, Any]:
        """Build a structured shadow-log payload for later replay and evaluation."""
        return self._shadow_logger.build_payload(
            data,
            now,
            optimizer_ran=optimizer_ran,
            normalize_local_datetime=_normalize_local_datetime,
        )

    async def _append_shadow_log(self, payload: dict[str, Any]) -> None:
        """Append a JSONL shadow-log row."""
        try:
            self._shadow_logger.enabled = self._shadow_log_enabled
            await self._shadow_logger.append(payload)
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

            savings_changed = self._tracker.update_savings(
                pv_w=pv_power,
                load_w=load_power,
                battery_w=battery_power,
                price_dkk=current_price,
                dt_seconds=dt_hours * 3600,
            )
        else:
            savings_changed = False

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

        # Persist tracker quickly when savings changed, otherwise checkpoint every 15 minutes.
        save_interval = timedelta(minutes=1) if savings_changed else timedelta(minutes=15)
        if self._last_tracker_save is None or (now - self._last_tracker_save) >= save_interval:
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
            data.plan_optimize_result = prev_data.plan_optimize_result
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

        buy_price_sensor = cfg.get(CONF_BUY_PRICE_SENSOR) or cfg.get("price_sensor")
        sell_price_sensor = cfg.get(CONF_SELL_PRICE_SENSOR) or buy_price_sensor

        price_snapshot = self._resolve_price_snapshot(
            now,
            "buy",
            PriceAdapter.from_hass(self.hass, buy_price_sensor),
        )
        if price_snapshot is None or price_snapshot.current_price is None:
            unavailable.append("price")
        else:
            data.price_data = price_snapshot
            data.price = price_snapshot.current_price

        sell_price_snapshot = self._resolve_price_snapshot(
            now,
            "sell",
            PriceAdapter.from_hass(self.hass, sell_price_sensor),
        )
        if sell_price_snapshot is None or sell_price_snapshot.current_price is None:
            unavailable.append("sell_price")
        else:
            data.sell_price_data = sell_price_snapshot
            data.sell_price = sell_price_snapshot.current_price

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
            data.total_solar_direct_saved_dkk = self._tracker.live_total_solar_saved_dkk
            data.total_optimizer_saved_dkk = self._tracker.live_total_optimizer_saved_dkk

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

        if self._forecast_correction_model is not None:
            correction_snapshot = self._forecast_correction_model.build_snapshot(
                now=now,
                hourly_forecast=data.forecast_data.hourly_forecast if data.forecast_data else [],
            )
            data.forecast_correction_model_state = correction_snapshot.state
            data.forecast_correction_current_month = correction_snapshot.current_month
            data.forecast_correction_active_buckets = correction_snapshot.active_buckets
            data.forecast_correction_confident_buckets = correction_snapshot.confident_buckets
            data.forecast_correction_average_factor_this_month = correction_snapshot.average_factor_this_month
            data.forecast_correction_today_hourly_factors = correction_snapshot.today_hourly_factors
            data.forecast_correction_current_hour_factor = correction_snapshot.current_hour_factor
            data.forecast_correction_current_hour_samples = correction_snapshot.current_hour_samples
            data.forecast_correction_raw_vs_corrected_delta_today = (
                correction_snapshot.raw_vs_corrected_delta_today
            )

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
        should_run_deviation = self._should_trigger_plan_deviation_replan(
            now=now,
            battery_power=data.battery_power,
        )
        should_run_hourly = (
            self._last_optimize_dt is None
            or (now - self._last_optimize_dt) >= timedelta(hours=1)
        )
        if should_run_deviation or should_run_hourly:
            # Temporarily expose the new data so _trigger_optimize can read it
            self.data = data  # type: ignore[assignment]
            await self._trigger_optimize(
                "plan-deviation" if should_run_deviation else "hourly-fallback",
                notify=False,
                force=should_run_deviation,
            )
            # Pull back whatever the optimizer wrote
            data.optimize_result = self.data.optimize_result if self.data else data.optimize_result
            data.plan_optimize_result = (
                self.data.plan_optimize_result if self.data else data.plan_optimize_result
            )

        data.battery_plan = self._build_battery_plan(data, now)
        if data.battery_plan and self._last_plan_optimize_result is not None:
            data.plan_optimize_result = self._last_plan_optimize_result

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
        data.consumption_profile_debug = self._profile.build_debug_snapshot()

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
                hybrid_slots=self._build_ev_hybrid_slots(_now, _departure),
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
        """Build EV plan from the active slot-based EV optimizer."""
        if self.data is None or self._ev_planning is None:
            return []

        return self._ev_planning.compute_ev_plan(
            data=self.data,
            ev_charge_mode=self.ev_charge_mode,
            ev_currently_charging=self._ev_currently_charging,
            ev_min_range_km=self.ev_min_range_km,
            now=ha_dt.now(),
            departure=self.ev_next_departure,
        )

    def _build_ev_hybrid_slots(self, now: datetime, departure: datetime) -> list[EVHybridSlot]:
        """Build EV planning slots from forecast, load profile, and battery plan."""
        if self.data is None or self._ev_planning is None:
            return []
        return self._ev_planning.build_ev_hybrid_slots(
            data=self.data,
            now=now,
            departure=departure,
        )

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


