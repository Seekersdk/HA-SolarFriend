"""SolarFriend coordinator.

Mini guide for AI/code bots:
- `coordinator.py`: thin orchestration layer and update order.
- `coordinator_models.py`: shared snapshot model plus EV device metadata helper.
- `coordinator_policy.py`: coordinator-side thresholds and timing policy.
- `price_runtime.py`: rolling price state and cached price snapshots.
- `strategy_runtime.py`: battery strategy hysteresis and confirmation logic.
- `tracker_runtime.py`: tracker cadence, SOC correction cadence, plan deviation checks.
- `state_reader.py`: raw sensor reads and live load cleanup.
- `weather_service.py` / `weather_profile.py`: weather fetch/cache and Solar Only profile mapping.
- `ev_runtime_controller.py`: EV anti-flap and Solar Only runtime hysteresis.
- `ev_planning.py`: EV slot planning and EV-vs-battery reservation logic.
- `battery_optimizer.py`: battery planning and economics.
- `shadow_logging.py`: replay-friendly shadow logs.

Maintenance rule:
- Prefer adding policy/runtime logic in dedicated helper files.
- Keep this file focused on orchestration, data flow order and cross-service wiring.
"""
from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field, replace
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as ha_dt

from .const import (
    CONF_BATTERY_SELL_ENABLED,
    CONF_BUY_PRICE_SENSOR,
    CONF_EV_SOLAR_ONLY_GRID_BUFFER_ENABLED,
    CONF_SELL_PRICE_SENSOR,
    DOMAIN,
)
from .consumption_profile import ConsumptionProfile
from .battery_tracker import BatteryTracker
from .battery_optimizer import BatteryOptimizer, OptimizeResult
from .forecast_adapter import ForecastAdapter, ForecastData, get_forecast_for_period
from .forecast_correction_model import ForecastCorrectionModel
from .forecast_tracker import ForecastTracker
from .price_adapter import PriceAdapter, PriceData, get_current_price_from_raw
from .inverter_controller import InverterController
from .ev_charger_controller import EVChargerController
from .vehicle_controller import VehicleController
from .ev_optimizer import EVContext, EVHybridSlot, EVOptimizer
from .ev_planning import EVPlanningHelper
from .ev_runtime_controller import EVRuntimeController
from .coordinator_models import SolarFriendData, ev_device_info
from .coordinator_policy import DEFAULT_COORDINATOR_POLICY
from .price_runtime import PriceRuntime
from .runtime_config import build_runtime_components, refresh_optimizer_runtime_settings
from .shadow_logging import ShadowLogger
from .state_reader import SolarFriendStateReader
from .strategy_runtime import StrategyRuntime
from .tracker_runtime import TrackerRuntime
from .weather_profile import SolarOnlyWeatherProfile
from .weather_service import WeatherProfileService

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
_EV_GRID_PRIORITY_MARGIN_W = 200.0
_EV_BATTERY_PROTECTION_MARGIN_W = 250.0


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
            update_interval=DEFAULT_COORDINATOR_POLICY.update_interval,
        )
        self._entry = entry
        self.config_entry = entry  # public alias for use by entity unique_ids
        self._policy = DEFAULT_COORDINATOR_POLICY
        self._price_runtime = PriceRuntime(self._policy)
        self._strategy_runtime = StrategyRuntime(self._policy, config_entry=entry)
        self._tracker_runtime = TrackerRuntime(self._policy, config_entry=entry)
        self._night_prices: dict[int, float] = {}  # hour → min price seen this night
        self._cached_buy_price_data: PriceData | None = None
        self._cached_sell_price_data: PriceData | None = None
        self._profile = ConsumptionProfile()
        self._last_profile_update: datetime | None = None
        self._state_reader = SolarFriendStateReader(hass, entry)
        self._weather_service = WeatherProfileService(
            hass,
            weather_entity=entry.data.get("weather_entity"),
        )

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
        self._ev_currently_charging: bool = False
        self._ev_active_solar_slot: bool = False
        self.ev_solar_only_grid_buffer_enabled: bool = bool(
            entry.data.get(CONF_EV_SOLAR_ONLY_GRID_BUFFER_ENABLED, True)
        )
        self.battery_sell_enabled: bool = bool(
            entry.data.get(CONF_BATTERY_SELL_ENABLED, True)
        )
        self._ev_runtime: EVRuntimeController | None = None
        self.ev_charging_allowed: bool = True  # styres af SolarFriendEVSwitch

        if self._ev_enabled:
            self._ev_charger = EVChargerController.from_config(hass, entry)
            self._vehicle = VehicleController.from_config(hass, entry)
            self._ev_optimizer = EVOptimizer()
            self._ev_runtime = EVRuntimeController(
                ev_optimizer=self._ev_optimizer,
                ev_charger=self._ev_charger,
            )
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

        runtime_components = build_runtime_components(
            self.hass,
            self._entry,
            battery_tracker=self._tracker,
            consumption_profile=self._profile,
        )
        self._optimizer = runtime_components.optimizer
        self._inverter = runtime_components.inverter
        self._state_reader = runtime_components.state_reader
        self._weather_service = runtime_components.weather_service
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
        self._ensure_strategy_runtime().reset_pending()

    def _mark_strategy_applied(self, result: OptimizeResult, now: datetime, pv_power: float) -> None:
        # Compatibility wrapper. New code should call StrategyRuntime directly.
        self._ensure_strategy_runtime()._mark_applied(result, now, pv_power)

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
        """Compatibility wrapper around StrategyRuntime override logic."""
        return self._ensure_strategy_runtime()._override_allowed(
            active_result,
            desired_result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
            solar_until_sunset_kwh=self.data.solar_until_sunset if self.data else 0.0,
        )

    def _select_strategy_result(
        self,
        desired_result: OptimizeResult,
        *,
        now: datetime,
        current_soc: float,
        pv_power: float,
        sunset: datetime,
    ) -> tuple[OptimizeResult, bool]:
        """Compatibility wrapper around StrategyRuntime selection logic."""
        return self._ensure_strategy_runtime().select_result(
            desired_result,
            active_result=self.data.optimize_result if self.data else None,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
            solar_until_sunset_kwh=self.data.solar_until_sunset if self.data else 0.0,
        )

    def _apply_optimizer_runtime_overrides(
        self,
        result: OptimizeResult,
    ) -> OptimizeResult:
        """
        Apply user-facing runtime overrides before strategy hysteresis/execution.

        Keep these overrides here instead of in the optimizer so the optimizer
        stays a pure economic planner and runtime toggles remain coordinator policy.
        """
        if self.battery_sell_enabled:
            return result
        if result.strategy != "SELL_BATTERY":
            return result
        return replace(
            result,
            strategy="USE_BATTERY",
            reason=(
                "Battery sell er deaktiveret af bruger-override. "
                f"{result.reason}"
            ),
            solar_sell=True,
        )

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
                if abs(new_soc - self._last_optimize_soc) < self._policy.soc_trigger_delta:
                    return

        self.hass.async_create_task(self._trigger_optimize("event", notify=True))

    def unregister_listeners(self) -> None:
        """Cancel all registered state-change listeners."""
        for unsub in self._unsub_listeners:
            unsub()
        self._unsub_listeners.clear()

    async def async_on_runtime_setting_changed(self, *, reason: str) -> None:
        """Refresh coordinator data and force a fresh optimizer run."""
        runtime_components = build_runtime_components(
            self.hass,
            self._entry,
            battery_tracker=self._tracker,
            consumption_profile=self._profile,
        )
        self._optimizer = runtime_components.optimizer
        self._inverter = runtime_components.inverter
        self._state_reader = runtime_components.state_reader
        self._weather_service = runtime_components.weather_service
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
        refresh_optimizer_runtime_settings(self._optimizer, self._entry)

        now = ha_dt.now()

        if (
            not force
            and
            self._last_optimize_dt is not None
            and (now - self._last_optimize_dt) < self._policy.optimize_min_interval
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

        def _run_battery_optimizer(reserved_solar_kwh: dict[datetime, float] | None):
            return self._optimizer.optimize(
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
                reserved_solar_kwh=reserved_solar_kwh,
            )

        result = _run_battery_optimizer(reserved_ev_solar_kwh)

        if self._ev_enabled and self.ev_charge_mode == "solar_only" and reserved_ev_solar_kwh:
            plan = self._optimizer.get_last_plan()
            has_grid_charge = any(float(slot.get("grid_charge_w", 0.0)) > 0 for slot in plan)
            if has_grid_charge:
                trimmed_reservations = dict(reserved_ev_solar_kwh)
                for start in sorted(trimmed_reservations.keys(), reverse=True):
                    trimmed_reservations.pop(start, None)
                    candidate = _run_battery_optimizer(trimmed_reservations or None)
                    candidate_plan = self._optimizer.get_last_plan()
                    if not any(float(slot.get("grid_charge_w", 0.0)) > 0 for slot in candidate_plan):
                        reserved_ev_solar_kwh = trimmed_reservations or None
                        result = candidate
                        break
                else:
                    reserved_ev_solar_kwh = None
                    result = _run_battery_optimizer(None)

        self._ev_active_solar_slot = (
            self._ev_enabled
            and self.ev_charge_mode == "solar_only"
            and self._has_current_ev_solar_slot(reserved_ev_solar_kwh, now)
        )

        result = self._apply_optimizer_runtime_overrides(result)

        selected_result, strategy_changed = self._select_strategy_result(
            result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
        )

        if (
            self._ev_active_solar_slot
            and selected_result.strategy in {"CHARGE_GRID", "CHARGE_NIGHT"}
        ):
            selected_result = replace(
                selected_result,
                strategy="SAVE_SOLAR",
                charge_now=False,
                reason=(
                    "EV solar-slot aktiv - inverter tvinges til Load first. "
                    f"{selected_result.reason}"
                ),
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
        """Backward-compatible wrapper around the dedicated state reader."""
        return self._state_reader.read_float_state(entity_id)

    def _get_raw_prices(self) -> list[dict[str, Any]]:
        """Return the normalised price list from the current data snapshot."""
        if self.data and self.data.price_data is not None:
            return self.data.price_data.to_legacy_raw_prices()
        return []

    def _trim_price_snapshot(self, snapshot: PriceData, now: datetime) -> PriceData | None:
        """Return a forward-looking price snapshot with past hours removed."""
        return self._ensure_price_runtime().trim_snapshot(
            snapshot,
            now,
            _normalize_local_datetime,
        )

    def _resolve_price_snapshot(
        self,
        now: datetime,
        cache_kind: str,
        fresh_snapshot: PriceData | None,
    ) -> PriceData | None:
        """Prefer fresh actual prices, otherwise fall back to the last valid snapshot."""
        return self._ensure_price_runtime().resolve_snapshot(
            now,
            cache_kind,
            fresh_snapshot,
            _normalize_local_datetime,
        )

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
        await self._ensure_tracker_runtime().update_forecast_tracker(
            forecast_tracker=self._forecast_tracker,
            now=now,
            pv_power=pv_power,
            forecast_total_today_kwh=forecast_total_today_kwh,
        )
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
        return self._ensure_tracker_runtime().should_trigger_plan_deviation_replan(
            optimizer=self._optimizer,
            now=now,
            battery_power=battery_power,
            normalize_local_datetime=_normalize_local_datetime,
        )

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
            ev_currently_charging=self._ev_runtime.currently_charging if self._ev_runtime else False,
            ev_min_range_km=self.ev_min_range_km,
            vehicle_target_soc_override=self.ev_target_soc_override,
            now=now,
            departure=departure,
            ev_next_departure=self.ev_next_departure,
        )

    @staticmethod
    def _has_current_ev_solar_slot(
        reservations: dict[datetime, float] | None,
        now: datetime,
    ) -> bool:
        """Return True when the current hour is reserved for EV solar charging."""
        if not reservations:
            return False
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        return float(reservations.get(current_hour, 0.0)) > 0.0

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
        self._ensure_price_runtime().update_history(price)

    def _price_average(self) -> float | None:
        return self._ensure_price_runtime().price_average()

    def _battery_strategy(
        self, solar_surplus: float, price: float, avg_price: float | None
    ) -> str:
        return self._ensure_price_runtime().battery_strategy(solar_surplus, price, avg_price)

    def _record_night_price(self, hour: int, price: float) -> None:
        self._ensure_price_runtime().record_night_price(hour, price)

    def _min_night_price(self) -> float | None:
        return self._ensure_price_runtime().min_night_price()

    def _price_level(self, price: float, avg_price: float | None) -> str:
        return self._ensure_price_runtime().price_level(price, avg_price)

    def _load_sensor_is_total_load(self) -> bool:
        """Return True when the configured load sensor is a whole-site total load."""
        return self._state_reader.load_sensor_is_total_load()

    def _clean_live_house_load(
        self,
        total_load_w: float,
        *,
        ev_power_w: float = 0.0,
    ) -> float:
        """Return house-only load when the configured load sensor is total site load."""
        return self._state_reader.clean_live_house_load(
            total_load_w,
            ev_power_w=ev_power_w,
        )

    def _ensure_weather_service(self) -> WeatherProfileService:
        """Return weather service, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_weather_service"):
            self._weather_service = WeatherProfileService(
                self.hass,
                weather_entity=self._entry.data.get("weather_entity"),
            )
        return self._weather_service

    def _ensure_ev_runtime(self) -> EVRuntimeController:
        """Return EV runtime controller, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_ev_runtime"):
            self._ev_runtime = EVRuntimeController(
                ev_optimizer=self._ev_optimizer,
                ev_charger=getattr(self, "_ev_charger", None),
            )
        return self._ev_runtime

    def _ensure_price_runtime(self) -> PriceRuntime:
        """Return price runtime helper, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_policy"):
            self._policy = DEFAULT_COORDINATOR_POLICY
        if not hasattr(self, "_price_runtime"):
            self._price_runtime = PriceRuntime(self._policy)
        return self._price_runtime

    def _ensure_strategy_runtime(self) -> StrategyRuntime:
        """Return strategy runtime helper, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_policy"):
            self._policy = DEFAULT_COORDINATOR_POLICY
        if not hasattr(self, "_strategy_runtime"):
            self._strategy_runtime = StrategyRuntime(self._policy, config_entry=self._entry)
        return self._strategy_runtime

    def _ensure_tracker_runtime(self) -> TrackerRuntime:
        """Return tracker runtime helper, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_policy"):
            self._policy = DEFAULT_COORDINATOR_POLICY
        if not hasattr(self, "_tracker_runtime"):
            self._tracker_runtime = TrackerRuntime(self._policy, config_entry=self._entry)
        return self._tracker_runtime

    def _ev_requires_battery_hold(self, ev_result: Any) -> bool:
        """Return True when EV charging should hold battery SOC via TOU."""
        if self.data is None or not ev_result.should_charge:
            return False
        if self.ev_charge_mode not in {"hybrid", "grid_schedule"}:
            return False

        surplus_w = max(0.0, float(ev_result.surplus_w))
        needs_grid_support = (
            float(ev_result.target_w) > surplus_w + self._policy.ev_grid_priority_margin_w
        )
        battery_to_ev = (
            self.data.battery_power > self.data.load_power + self._policy.ev_battery_protection_margin_w
        )
        return needs_grid_support or battery_to_ev

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
        await self._ensure_tracker_runtime().update_battery_tracker(
            tracker=self._tracker,
            now=now,
            pv_power=pv_power,
            battery_power=battery_power,
            load_power=load_power,
            battery_soc=battery_soc,
            current_price=current_price,
            previous_soc=self.data.battery_soc if self.data is not None else None,
        )
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
        raw_load_power = 0.0
        prefetched_ev_power = prev_data.ev_charging_power if prev_data is not None else 0.0
        prefetched_ev_status: str | None = None

        # Carry over optimizer + forecast results between polling cycles
        if prev_data is not None:
            data.optimize_result = prev_data.optimize_result
            data.plan_optimize_result = prev_data.plan_optimize_result
            data.forecast_data   = prev_data.forecast_data

        state_result = self._state_reader.read_core_sensors()
        readings = state_result.readings
        unavailable = list(state_result.unavailable)
        sensor_id_map: dict[str, str] = {
            "pv_power": cfg.get("pv_power_sensor", ""),
            "pv2_power": cfg.get("pv2_power_sensor", ""),
            "grid_power": cfg.get("grid_power_sensor", ""),
            "battery_soc": cfg.get("battery_soc_sensor", ""),
            "battery_power": cfg.get("battery_power_sensor", ""),
            "load_power": cfg.get("load_power_sensor", ""),
        }
        for field_name in unavailable:
            entity_id = sensor_id_map.get(field_name, "")
            if entity_id:
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
        raw_load_power     = readings.get("load_power", 0.0)

        if self._ev_enabled and self._ev_charger is not None:
            try:
                prefetched_ev_status = await self._ev_charger.get_status()
                prefetched_ev_power = await self._ev_charger.get_power_w()
            except Exception as exc:  # noqa: BLE001
                _LOGGER.debug("EV prefetch failed during load cleanup: %s", exc)

        data.ev_charging_power = prefetched_ev_power
        data.load_power = self._clean_live_house_load(
            raw_load_power,
            ev_power_w=prefetched_ev_power,
        )

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
            await self._update_ev(
                charger_status=prefetched_ev_status,
                charger_power=prefetched_ev_power,
            )

        if "load_power" in readings:
            await self._maybe_update_profile(
                raw_load_power,
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

    async def _update_ev(
        self,
        *,
        charger_status: str | None = None,
        charger_power: float | None = None,
    ) -> None:
        """Run EV optimizer and act on the result."""
        try:
            ev_runtime = self._ensure_ev_runtime()
            if charger_status is None:
                charger_status = await self._ev_charger.get_status()
            if charger_power is None:
                charger_power = await self._ev_charger.get_power_w()
            vehicle_soc = self._vehicle.get_soc()
            vehicle_target_soc = (
                self.ev_target_soc_override
                if self.ev_target_soc_override is not None
                else self._vehicle.get_target_soc()
            )

            ev_runtime.sync_startup(charger_status)

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
            solar_to_departure = self._forecast_kwh_between(_now, _departure)
            _LOGGER.debug(
                "EV solar forecast til afgang %s: %.2f kWh",
                _departure.strftime("%H:%M"),
                solar_to_departure,
            )
            solar_only_profile = await self._get_current_solar_only_profile(_now)
            if not self.ev_solar_only_grid_buffer_enabled:
                solar_only_profile = replace(solar_only_profile, grid_buffer_w=0.0)

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
                grid_power_w=self.data.grid_power,
                battery_charging_w=self.data.battery_power,
                battery_soc=self.data.battery_soc,
                battery_capacity_kwh=float(
                    self._entry.data.get("battery_capacity_kwh", 10.0)
                ),
                battery_min_soc=float(
                    self._entry.data.get("battery_min_soc", 10.0)
                ),
                charger_status=charger_status,
                currently_charging=ev_runtime.currently_charging,
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
                allow_battery_charge_reclaim=self._ev_active_solar_slot,
                solar_only_profile_name=solar_only_profile.key,
                solar_only_start_threshold_w=solar_only_profile.start_surplus_w,
                solar_only_stop_threshold_w=solar_only_profile.stop_surplus_w,
                solar_only_grid_buffer_w=solar_only_profile.grid_buffer_w,
            )

            ev_result = self._ev_optimizer.optimize(ctx, mode=self.ev_charge_mode)
            actual_charging = ev_runtime.set_currently_charging_from_actual(
                charger_status=charger_status,
                charger_power=charger_power,
            )
            if self.ev_charge_mode == "solar_only":
                ev_result = ev_runtime.apply_solar_only_hysteresis(
                    ctx=ctx,
                    result=ev_result,
                    profile=solar_only_profile,
                    actual_charging=actual_charging,
                )

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
            if min_range_km > 0 and vehicle_efficiency > 0 and self.vehicle_battery_kwh > 0:
                self.data.ev_min_soc_from_range = min(
                    100.0,
                    min_range_km / vehicle_efficiency / self.vehicle_battery_kwh * 100,
                )
            else:
                self.data.ev_min_soc_from_range = 0.0

            if (
                self._inverter is not None
                and self._inverter.is_configured
                and self.data.optimize_result is not None
                and self._ev_requires_battery_hold(ev_result)
            ):
                await self._inverter.apply(
                    replace(
                        self.data.optimize_result,
                        strategy="EV_HOLD_BATTERY",
                        charge_now=False,
                        target_soc=math.ceil(self.data.battery_soc),
                        reason=(
                            "EV holder batteri-SOC via midlertidig TOU. "
                            f"{self.data.optimize_result.reason}"
                        ),
                    )
                )

            self._ev_currently_charging = actual_charging
            now = ha_dt.now()
            await ev_runtime.async_apply_charge_decision(
                ev_result=ev_result,
                now=now,
            )
            self._ev_currently_charging = ev_runtime.currently_charging
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
            ev_currently_charging=self._ev_runtime.currently_charging if self._ev_runtime else False,
            ev_min_range_km=self.ev_min_range_km,
            now=ha_dt.now(),
            departure=self.ev_next_departure,
        )

    async def _fetch_weather_hourly_forecast(self) -> list[dict[str, Any]]:
        """Fetch and cache hourly weather forecast for Solar Only profiling."""
        return await self._ensure_weather_service().async_fetch_hourly_forecast()

    async def _get_current_solar_only_profile(self, now: datetime) -> SolarOnlyWeatherProfile:
        """Return the active Solar Only weather profile for the current hour."""
        return await self._ensure_weather_service().async_get_current_profile(now)

    def _apply_solar_only_hysteresis(
        self,
        *,
        ctx: EVContext,
        result: Any,
        profile: SolarOnlyWeatherProfile,
        actual_charging: bool,
    ):
        """Apply time-based start/stop hysteresis for Solar Only EV charging."""
        return self._ensure_ev_runtime().apply_solar_only_hysteresis(
            ctx=ctx,
            result=result,
            profile=profile,
            actual_charging=actual_charging,
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


