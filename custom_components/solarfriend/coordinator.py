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
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as ha_dt

from .const import (
    CONF_ADVANCED_CONSUMPTION_MODEL_ENABLED,
    CONF_BATTERY_SELL_ENABLED,
    CONF_BUY_PRICE_SENSOR,
    CONF_EV_SOLAR_ONLY_GRID_BUFFER_ENABLED,
    CONF_SELL_PRICE_SENSOR,
    DOMAIN,
)
from .advanced_consumption_model import AdvancedConsumptionModel
from .consumption_profile import ConsumptionProfile
from .battery_tracker import BatteryTracker
from .battery_optimizer import (
    ALLOWED_DISCHARGE_SOLAR_THRESHOLD_W,
    BatteryOptimizer,
    OptimizeResult,
)
from .forecast_adapter import ForecastAdapter, get_forecast_for_period
from .forecast_correction_model import ForecastCorrectionModel
from .solar_installation_profile import DEFAULT_PROFILE_RESOLUTIONS, SolarInstallationProfile
from .forecast_tracker import ForecastTracker
from .flex_load_manager import FlexLoadReservationManager, NullFlexLoadReservationManager
from .price_adapter import PriceAdapter, PriceData, get_current_price_from_raw
from .inverter_controller import InverterController
from .ev_charger_controller import EVChargerController
from .vehicle_controller import VehicleController
from .ev_optimizer import EVOptimizer
from .ev_planning import EVPlanningHelper
from .ev_runtime_controller import EVRuntimeController
from .ev_runtime_service import EVRuntimeService
from .coordinator_models import SolarFriendData, ev_device_info
from .coordinator_policy import DEFAULT_COORDINATOR_POLICY
from .model_evaluation_logging import ModelEvaluationLogger, lookup_forecast_kwh, lookup_weather_value
from .price_runtime import PriceRuntime
from .runtime_config import build_runtime_components, refresh_optimizer_runtime_settings
from .shadow_logging import ShadowLogger
from .snapshot_builder import SnapshotBuilder
from .state_reader import SolarFriendStateReader
from .strategy_runtime import StrategyRuntime
from .tracker_runtime import TrackerRuntime
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
        value = value.replace(tzinfo=timezone.utc)
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
        self._snapshot_builder = SnapshotBuilder()
        self._flex_load_manager = FlexLoadReservationManager(hass, entry.entry_id)
        self._weather_service = WeatherProfileService(
            hass,
            weather_entity=entry.data.get("weather_entity"),
        )
        self._ev_service = EVRuntimeService(
            policy=self._policy,
            weather_service=self._weather_service,
        )

        # BatteryTracker — initialised in async_startup
        self._tracker: BatteryTracker | None = None
        self._forecast_tracker: ForecastTracker | None = None
        self._forecast_correction_model: ForecastCorrectionModel | None = None
        self._solar_installation_profile: SolarInstallationProfile | None = None
        self._solar_installation_profiles: dict[str, SolarInstallationProfile] = {}

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
        self._startup_at: datetime = ha_dt.now()
        self._startup_price_recovery_optimize_done: bool = False
        self._shadow_log_enabled: bool = bool(entry.data.get("shadow_log_enabled", False))
        self._shadow_logger = ShadowLogger(
            entry=entry,
            profile=self._profile,
            log_path=hass.config.path("solarfriend_shadow_log.jsonl"),
            enabled=self._shadow_log_enabled,
        )
        self._model_evaluation_logger = ModelEvaluationLogger(
            hass,
            entry_id=entry.entry_id,
            log_path=hass.config.path("solarfriend_model_evaluation.jsonl"),
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
        self.advanced_consumption_model_enabled: bool = bool(
            entry.data.get(CONF_ADVANCED_CONSUMPTION_MODEL_ENABLED, False)
        )
        self._advanced_consumption_model = AdvancedConsumptionModel()
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
        await self._advanced_consumption_model.async_load(self.hass)
        await self._ensure_flex_load_manager().async_load()
        await self._model_evaluation_logger.async_load()

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
        self._solar_installation_profiles = {
            resolution.key: SolarInstallationProfile(
                self.hass,
                self._entry.entry_id,
                resolution=resolution,
            )
            for resolution in DEFAULT_PROFILE_RESOLUTIONS
        }
        for profile in self._solar_installation_profiles.values():
            await profile.async_load()
        self._solar_installation_profile = self._solar_installation_profiles.get("medium")

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
        await self._advanced_consumption_model.async_save(self.hass)
        await self._ensure_flex_load_manager().async_save()
        await self._model_evaluation_logger.async_save()
        if self._tracker is not None:
            await self._tracker.async_save()
        if self._forecast_tracker is not None:
            await self._forecast_tracker.async_save()
        if self._forecast_correction_model is not None:
            await self._forecast_correction_model.async_save()
        solar_profiles = getattr(self, "_solar_installation_profiles", {})
        if solar_profiles:
            for profile in solar_profiles.values():
                await profile.async_save()
        elif self._solar_installation_profile is not None:
            await self._solar_installation_profile.async_save()

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

    def _apply_allowed_discharge_slot_override(
        self,
        result: OptimizeResult,
        *,
        allowed_slots_source: OptimizeResult | None = None,
        now: datetime,
        pv_power: float,
        load_power: float,
        current_soc: float,
    ) -> OptimizeResult:
        """Open the battery in economically approved fallback slots when live deficit appears."""
        if result.strategy not in {"IDLE", "SAVE_SOLAR"}:
            return result

        source_result = allowed_slots_source or result
        allowed_slots = getattr(source_result, "allowed_discharge_slots", None) or []
        if not allowed_slots:
            return result

        current_hour = now.replace(minute=0, second=0, microsecond=0)
        active_slot = next(
            (
                slot
                for slot in allowed_slots
                if _normalize_local_datetime(_parse_dt(slot["hour"])).replace(
                    minute=0, second=0, microsecond=0
                )
                == current_hour
            ),
            None,
        )
        if active_slot is None:
            return result

        live_deficit_w = max(0.0, load_power - pv_power)
        if live_deficit_w <= self._policy.battery_noise_w:
            return result

        min_soc = float(self._entry.data.get("battery_min_soc", 10.0))
        if current_soc <= (min_soc + self._policy.soc_override_margin):
            return result

        return replace(
            result,
            strategy="USE_BATTERY",
            reason=(
                f"Allowed batteri-slot {active_slot['hour_str']} aktiv: "
                f"forecast {float(active_slot['forecast_solar_w']):.0f}W under "
                f"{ALLOWED_DISCHARGE_SOLAR_THRESHOLD_W:.0f}W og live underskud "
                f"{live_deficit_w:.0f}W"
            ),
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
        price_sensors = {
            self._entry.data.get(CONF_BUY_PRICE_SENSOR, ""),
            self._entry.data.get(CONF_SELL_PRICE_SENSOR, ""),
            self._entry.data.get("price_sensor", ""),
        }
        price_sensors.discard("")

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

        if entity_id in price_sensors:
            _LOGGER.info("Pris opdateret på %s - tvinger straks ny optimizer-plan", entity_id)
            self.hass.async_create_task(
                self._trigger_optimize("price_updated", notify=True, force=True)
            )
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
            startup_grace_active = (now - self._startup_at) < timedelta(minutes=2)
            if startup_grace_active:
                _LOGGER.info(
                    "BatteryOptimizer: price data not available during startup grace — skipping optimize (%s)",
                    reason,
                )
                return
            raw_prices = [{"hour": h, "price": p} for h, p in self._night_prices.items()]
            if self.data.price > 0:
                raw_prices.append({"hour": now.hour, "price": self.data.price})

        # ── Forecast ───────────────────────────────────────────────────────
        forecast_data = self.data.forecast_data

        if forecast_data is None:
            startup_grace_active = (now - self._startup_at) < timedelta(minutes=2)
            if startup_grace_active:
                _LOGGER.info(
                    "BatteryOptimizer: forecast data not available during startup grace — skipping optimize (%s)",
                    reason,
                )
                return
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
        reserved_flex_solar_kwh = self._build_flex_load_reservations(now)
        reserved_solar_kwh = self._merge_reserved_solar_maps(
            reserved_ev_solar_kwh,
            reserved_flex_solar_kwh,
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

        result = _run_battery_optimizer(reserved_solar_kwh)

        if self._ev_enabled and self.ev_charge_mode == "solar_only" and reserved_ev_solar_kwh:
            plan = self._optimizer.get_last_plan()
            has_grid_charge = any(float(slot.get("grid_charge_w", 0.0)) > 0 for slot in plan)
            if has_grid_charge:
                trimmed_reservations = dict(reserved_ev_solar_kwh)
                for start in sorted(trimmed_reservations.keys(), reverse=True):
                    trimmed_reservations.pop(start, None)
                    candidate = _run_battery_optimizer(
                        self._merge_reserved_solar_maps(
                            trimmed_reservations or None,
                            reserved_flex_solar_kwh,
                        )
                    )
                    candidate_plan = self._optimizer.get_last_plan()
                    if not any(float(slot.get("grid_charge_w", 0.0)) > 0 for slot in candidate_plan):
                        reserved_ev_solar_kwh = trimmed_reservations or None
                        result = candidate
                        break
                else:
                    reserved_ev_solar_kwh = None
                    result = _run_battery_optimizer(reserved_flex_solar_kwh)

        self._ev_active_solar_slot = (
            self._ev_enabled
            and self.ev_charge_mode == "solar_only"
            and self._has_current_ev_solar_slot(reserved_ev_solar_kwh, now)
        )

        result = self._ensure_strategy_runtime().apply_runtime_overrides(
            result,
            battery_sell_enabled=self.battery_sell_enabled,
            ev_enabled=getattr(self, "_ev_enabled", False),
            ev_charge_mode=getattr(self, "ev_charge_mode", ""),
            ev_currently_charging=(
                getattr(self, "_ev_runtime", None) is not None
                and self._ev_runtime.currently_charging
            ),
            ev_charging_power=(
                float(self.data.ev_charging_power)
                if getattr(self, "data", None) is not None
                else 0.0
            ),
        )

        selected_result, strategy_changed = self._select_strategy_result(
            result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
        )
        allowed_slot_result = self._apply_allowed_discharge_slot_override(
            selected_result,
            allowed_slots_source=result,
            now=now,
            pv_power=pv_power,
            load_power=load_power,
            current_soc=current_soc,
        )
        if allowed_slot_result.strategy != selected_result.strategy:
            selected_result = allowed_slot_result
            strategy_changed = True

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

    def _build_flex_load_reservations(
        self,
        now: datetime,
    ) -> dict[datetime, float]:
        """Return active flex-load solar reservations by hour."""
        return self._ensure_flex_load_manager().reserved_solar_kwh_by_hour(now)

    @staticmethod
    def _merge_reserved_solar_maps(
        first: dict[datetime, float] | None,
        second: dict[datetime, float] | None,
    ) -> dict[datetime, float] | None:
        """Merge two hourly reserved-solar maps."""
        merged: dict[datetime, float] = {}
        for source in (first, second):
            if not source:
                continue
            for slot_start, reserved_kwh in source.items():
                merged[slot_start] = merged.get(slot_start, 0.0) + float(reserved_kwh)
        return merged or None

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

    async def async_book_flex_load(
        self,
        *,
        job_id: str,
        name: str,
        duration_minutes: int,
        deadline: datetime,
        earliest_start: datetime | None,
        preferred_source: str,
        energy_wh: float | None,
        power_w: float | None,
        min_solar_w: float | None,
        max_grid_w: float | None,
        allow_battery: bool,
    ) -> dict[str, Any]:
        """Create or replace a flex-load reservation and return the computed slot."""
        if self.data is None:
            self.data = await self._async_update_data()
        now = ha_dt.now()
        response = self._ensure_flex_load_manager().upsert(
            now=now,
            job_id=job_id,
            name=name,
            duration_minutes=int(duration_minutes),
            deadline=deadline,
            earliest_start=earliest_start or now,
            preferred_source=preferred_source,
            energy_wh=float(energy_wh or 0.0),
            power_w=float(power_w or 0.0),
            min_solar_w=min_solar_w,
            max_grid_w=max_grid_w,
            allow_battery=allow_battery,
            hourly_forecast=self.data.forecast_data.hourly_forecast
            if self.data and self.data.forecast_data
            else [],
            raw_prices=self._get_raw_prices(),
            consumption_profile=self._profile,
        )
        await self._ensure_flex_load_manager().async_save()
        await self._trigger_optimize("flex-load-booked", notify=False, force=True)
        self.data = await self._async_update_data()
        self.async_set_updated_data(self.data)
        return response

    async def async_cancel_flex_load(self, job_id: str) -> dict[str, Any]:
        """Cancel an existing flex-load reservation."""
        removed = self._ensure_flex_load_manager().cancel(job_id)
        if removed:
            await self._ensure_flex_load_manager().async_save()
            await self._trigger_optimize("flex-load-cancelled", notify=False, force=True)
            self.data = await self._async_update_data()
            self.async_set_updated_data(self.data)
        return {"removed": removed, "job_id": job_id}

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

    async def _append_model_evaluation_log(
        self,
        *,
        finalized_slot: dict[str, Any] | None,
        raw_hourly_forecast: list[dict[str, Any]],
        empirical_hourly_forecast: list[dict[str, Any]],
        active_solar_profiles: dict[str, Any],
        hourly_weather_forecast: list[dict[str, Any]] | None,
    ) -> None:
        """Append one compact forecast-comparison row for a finalized slot."""
        logger = getattr(self, "_model_evaluation_logger", None)
        if logger is None:
            return
        if not finalized_slot:
            return
        slot_start = finalized_slot.get("period_start")
        if not isinstance(slot_start, datetime):
            return

        actual_kwh = float(finalized_slot.get("actual_kwh", 0.0))
        solcast_kwh = float(finalized_slot.get("solcast_kwh", 0.0))
        if max(actual_kwh, solcast_kwh) <= 0:
            return

        empirical_kwh = lookup_forecast_kwh(empirical_hourly_forecast, slot_start)
        temperature_c = lookup_weather_value(hourly_weather_forecast, slot_start, "temperature")
        slot_minutes = max(1, int(_forecast_slot_delta(raw_hourly_forecast).total_seconds() // 60))

        solar_elevation = finalized_slot.get("solar_elevation")
        solar_azimuth = finalized_slot.get("solar_azimuth")
        track2_rows: dict[str, dict[str, float | None]] = {}
        if solar_elevation is not None and solar_azimuth is not None:
            for key, profile in active_solar_profiles.items():
                if profile is None:
                    continue
                factor, confidence = profile.get_factor_with_confidence(
                    float(solar_elevation),
                    float(solar_azimuth),
                )
                track2_rows[key] = {
                    "kwh": (solcast_kwh * factor) if factor is not None else None,
                    "confidence": confidence,
                }

        try:
            await logger.append_slot(
                slot_start=slot_start,
                slot_minutes=slot_minutes,
                actual_kwh=actual_kwh,
                solcast_kwh=solcast_kwh,
                empirical_kwh=empirical_kwh,
                solar_elevation=float(solar_elevation) if solar_elevation is not None else None,
                solar_azimuth=float(solar_azimuth) if solar_azimuth is not None else None,
                cloud_coverage_pct=(
                    float(finalized_slot["cloud_coverage_pct"])
                    if finalized_slot.get("cloud_coverage_pct") is not None
                    else None
                ),
                temperature_c=temperature_c,
                track2_rows=track2_rows,
            )
        except Exception as exc:  # noqa: BLE001
            _LOGGER.debug("Model evaluation log write failed: %s", exc)

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
        ev_power_available: bool = True,
    ) -> float:
        """Return house-only load when the configured load sensor is total site load."""
        return self._state_reader.clean_live_house_load(
            total_load_w,
            ev_power_w=ev_power_w,
            ev_power_available=ev_power_available,
        )

    def _ensure_weather_service(self) -> WeatherProfileService:
        """Return weather service, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_weather_service"):
            self._weather_service = WeatherProfileService(
                self.hass,
                weather_entity=self._entry.data.get("weather_entity"),
            )
        return self._weather_service

    def _ensure_flex_load_manager(self) -> FlexLoadReservationManager:
        """Return flex-load manager, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_flex_load_manager"):
            try:
                self._flex_load_manager = FlexLoadReservationManager(self.hass, self._entry.entry_id)
            except Exception:  # noqa: BLE001 - lightweight test harness may not provide Store
                self._flex_load_manager = NullFlexLoadReservationManager()
        return self._flex_load_manager

    def _ensure_snapshot_builder(self) -> SnapshotBuilder:
        """Return snapshot builder, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_snapshot_builder"):
            self._snapshot_builder = SnapshotBuilder()
        return self._snapshot_builder

    def _ensure_ev_runtime(self) -> EVRuntimeController:
        """Return EV runtime controller, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_ev_runtime"):
            self._ev_runtime = EVRuntimeController(
                ev_optimizer=self._ev_optimizer,
                ev_charger=getattr(self, "_ev_charger", None),
            )
        return self._ev_runtime

    def _ensure_ev_service(self) -> EVRuntimeService:
        """Return EV orchestration service, creating a fallback for tests that bypass __init__."""
        if not hasattr(self, "_policy"):
            self._policy = DEFAULT_COORDINATOR_POLICY
        if not hasattr(self, "_weather_service"):
            self._weather_service = WeatherProfileService(
                self.hass,
                weather_entity=self._entry.data.get("weather_entity"),
            )
        if not hasattr(self, "_ev_service"):
            self._ev_service = EVRuntimeService(
                policy=self._policy,
                weather_service=self._weather_service,
            )
        return self._ev_service

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

    async def _update_tracker(
        self,
        now: datetime,
        pv_power: float,
        battery_power: float,
        load_power: float,
        battery_soc: float,
        current_price: float,
        sell_price: float,
        load_is_trustworthy: bool = True,
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
            sell_price=sell_price,
            previous_soc=self.data.battery_soc if self.data is not None else None,
            load_is_trustworthy=load_is_trustworthy,
            active_strategy=self.data.optimize_result.strategy if self.data and self.data.optimize_result else None,
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
        prefetched_ev_power_available: bool = prev_data is not None
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
        data.battery_soc   = readings.get("battery_soc")
        data.battery_power = readings.get("battery_power", 0.0)
        raw_load_power     = readings.get("load_power", 0.0)

        if self._ev_enabled and self._ev_charger is not None:
            try:
                prefetched_ev_status = await self._ev_charger.get_status()
                prefetched_ev_power = await self._ev_charger.get_power_w()
                prefetched_ev_power_available = True
            except Exception as exc:  # noqa: BLE001
                _LOGGER.debug("EV prefetch failed during load cleanup: %s", exc)

        data.ev_charging_power = prefetched_ev_power
        data.load_power = self._clean_live_house_load(
            raw_load_power,
            ev_power_w=prefetched_ev_power,
            ev_power_available=prefetched_ev_power_available,
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
        load_learning_allowed = self._ensure_strategy_runtime().load_learning_allowed(
            data.optimize_result
        )
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
                sell_price=data.sell_price,
                load_is_trustworthy=load_learning_allowed,
            )

        if "pv_power" in readings or "pv2_power" in readings:
            await self._update_forecast_tracker(
                now=now,
                pv_power=data.pv_power,
                forecast_total_today_kwh=(
                    data.forecast_data.total_today_kwh if data.forecast_data else None
                ),
            )

        weather_service = self._ensure_weather_service()
        weather_snapshot = await weather_service.async_get_current_hour_snapshot(now)
        hourly_weather_forecast = await weather_service.async_fetch_hourly_forecast()
        sun_state = self.hass.states.get("sun.sun")
        solar_elevation = None
        solar_azimuth = None
        sunrise_dt = None
        sunset_dt = None
        if sun_state is not None:
            try:
                solar_elevation = float(sun_state.attributes.get("elevation"))
            except (TypeError, ValueError):
                solar_elevation = None
            try:
                solar_azimuth = float(sun_state.attributes.get("azimuth"))
            except (TypeError, ValueError):
                solar_azimuth = None
            raw_sunrise = sun_state.attributes.get("next_rising")
            raw_sunset = sun_state.attributes.get("next_setting")
            if raw_sunrise:
                try:
                    sunrise_dt = _normalize_local_datetime(datetime.fromisoformat(str(raw_sunrise)))
                except (TypeError, ValueError):
                    sunrise_dt = None
            if raw_sunset:
                try:
                    sunset_dt = _normalize_local_datetime(datetime.fromisoformat(str(raw_sunset)))
                except (TypeError, ValueError):
                    sunset_dt = None
        if self._forecast_correction_model is not None:
            dt_seconds = (
                0.0
                if self._prev_update_time is None
                else (now - self._prev_update_time).total_seconds()
            )
            self._forecast_correction_model.update(
                now=now,
                pv_power_w=data.pv_power,
                dt_seconds=dt_seconds,
                hourly_forecast=data.forecast_data.hourly_forecast if data.forecast_data else [],
                sunrise=sunrise_dt,
                sunset=sunset_dt,
                weather_snapshot=weather_snapshot,
                solar_elevation=solar_elevation,
                solar_azimuth=solar_azimuth,
            )

        solar_profiles = getattr(self, "_solar_installation_profiles", {})
        active_solar_profiles = (
            solar_profiles
            if solar_profiles
            else (
                {"medium": self._solar_installation_profile}
                if self._solar_installation_profile is not None
                else {}
            )
        )
        finalized_solar_slot: dict[str, Any] | None = None
        if active_solar_profiles and solar_elevation is not None and solar_azimuth is not None:
            _dt = (
                0.0
                if self._prev_update_time is None
                else (now - self._prev_update_time).total_seconds()
            )
            for profile in active_solar_profiles.values():
                if profile is None:
                    continue
                finalized = profile.update(
                    now=now,
                    pv_power_w=data.pv_power,
                    dt_seconds=_dt,
                    elevation_deg=solar_elevation,
                    azimuth_deg=solar_azimuth,
                    cloud_coverage_pct=weather_snapshot.get("cloud_coverage_pct") if weather_snapshot else None,
                    slot_forecast_kwh=_get_slot_forecast_kwh(
                        data.forecast_data.hourly_forecast if data.forecast_data else [], now
                    ),
                )
                if finalized_solar_slot is None and finalized is not None:
                    finalized_solar_slot = finalized

        self._prev_battery_power = data.battery_power
        self._prev_update_time = now

        snapshot_builder = self._ensure_snapshot_builder()
        snapshot_builder.apply_battery_tracker(data=data, tracker=self._tracker)
        snapshot_builder.apply_flex_loads(
            data=data,
            now=now,
            manager=self._ensure_flex_load_manager(),
        )
        snapshot_builder.apply_forecast_tracker(
            data=data,
            now=now,
            forecast_tracker=self._forecast_tracker,
        )
        snapshot_builder.apply_forecast_correction(
            data=data,
            now=now,
            correction_model=self._forecast_correction_model,
            weather_snapshot=weather_snapshot,
            solar_elevation=solar_elevation,
            solar_azimuth=solar_azimuth,
            latitude=self.hass.config.latitude,
            longitude=self.hass.config.longitude,
            hourly_weather_forecast=hourly_weather_forecast,
        )

        _raw_hourly = data.forecast_data.hourly_forecast if data.forecast_data else []
        _empirical_hourly = (
            self._forecast_correction_model.get_corrected_hourly_forecast(
                now=now,
                hourly_forecast=_raw_hourly,
                latitude=self.hass.config.latitude,
                longitude=self.hass.config.longitude,
                hourly_weather_forecast=hourly_weather_forecast,
            )
            if self._forecast_correction_model is not None
            else []
        )
        snapshot_builder.apply_solar_installation_profiles(
            data=data,
            now=now,
            profiles=active_solar_profiles,
            latitude=self.hass.config.latitude,
            longitude=self.hass.config.longitude,
            raw_hourly_forecast=_raw_hourly,
            empirical_hourly_forecast=_empirical_hourly,
        )
        await self._append_model_evaluation_log(
            finalized_slot=finalized_solar_slot,
            raw_hourly_forecast=_raw_hourly,
            empirical_hourly_forecast=_empirical_hourly,
            active_solar_profiles=active_solar_profiles,
            hourly_weather_forecast=hourly_weather_forecast,
        )

        # ── Consumption profile chart (24h curve) ────────────────────────────
        snapshot_builder.apply_consumption_profile_chart(
            data=data,
            now=now,
            profile=self._profile,
        )

        # ── Forecast SOC curve (24h simulation) ──────────────────────────
        snapshot_builder.apply_forecast_soc_chart(
            data=data,
            now=now,
            capacity_kwh=float(cfg.get("battery_capacity_kwh", 10.0)),
            min_soc=float(cfg.get("battery_min_soc", 10.0)),
        )
        snapshot_builder.apply_solar_lookahead(
            data=data,
            now=now,
            sunset_dt=sunset_dt,
        )

        should_run_deviation = self._should_trigger_plan_deviation_replan(
            now=now,
            battery_power=data.battery_power,
        )
        should_run_hourly = (
            self._last_optimize_dt is None
            or (now - self._last_optimize_dt) >= timedelta(hours=1)
        )
        startup_grace_active = (now - self._startup_at) < timedelta(minutes=2)
        startup_inputs_recovered = (
            startup_grace_active
            and not self._startup_price_recovery_optimize_done
            and data.price_data is not None
            and data.price_data.current_price is not None
            and data.forecast_data is not None
            and (
                prev_data is None
                or prev_data.price_data is None
                or prev_data.price_data.current_price is None
                or prev_data.forecast_data is None
            )
        )
        if startup_inputs_recovered:
            self._startup_price_recovery_optimize_done = True
            self.data = data  # type: ignore[assignment]
            await self._trigger_optimize(
                "startup-inputs-recovered",
                notify=False,
                force=True,
            )
            data.optimize_result = self.data.optimize_result if self.data else data.optimize_result
            data.plan_optimize_result = (
                self.data.plan_optimize_result if self.data else data.plan_optimize_result
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

        optimizer_ran = (
            startup_inputs_recovered
            or should_run_deviation
            or should_run_hourly
        )

        data.battery_plan = self._build_battery_plan(data, now)
        if data.battery_plan and self._last_plan_optimize_result is not None:
            data.plan_optimize_result = self._last_plan_optimize_result

        # ── EV charging ───────────────────────────────────────────────────
        if self._ev_enabled:
            self.data = data  # type: ignore[assignment]
            await self._update_ev()
            await self._handle_unexpected_grid_events(data=data, now=now)

        if "load_power" in readings and load_learning_allowed:
            await self._maybe_update_profile(
                raw_load_power,
                ev_power_w=data.ev_charging_power,
                battery_power_w=data.battery_power,
            )

        snapshot_builder.apply_advanced_consumption(
            data=data,
            now=now,
            model=self._advanced_consumption_model,
            enabled=self.advanced_consumption_model_enabled,
            load_learning_allowed=load_learning_allowed,
            weather_snapshot=weather_snapshot,
        )

        snapshot_builder.apply_profile_debug(data=data, profile=self._profile)
        evaluation_logger = getattr(self, "_model_evaluation_logger", None)
        if evaluation_logger is not None:
            snapshot_builder.apply_model_evaluation_summary(
                data=data,
                summary=await evaluation_logger.build_summary(now=now),
            )

        await self._append_shadow_log(
            self._build_shadow_payload(
                data,
                now,
                optimizer_ran=optimizer_ran,
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
    ) -> None:
        """Run EV optimizer and act on the result."""
        try:
            self._ev_currently_charging = await self._ensure_ev_service().update(
                data=self.data,
                entry=self._entry,
                ev_runtime=self._ensure_ev_runtime(),
                ev_optimizer=self._ev_optimizer,
                ev_charger=self._ev_charger,
                vehicle=self._vehicle,
                ev_planning=self._ev_planning,
                inverter=self._inverter,
                optimize_result=self.data.optimize_result if self.data is not None else None,
                vehicle_battery_kwh=self.vehicle_battery_kwh,
                ev_charge_mode=self.ev_charge_mode,
                ev_target_soc_override=self.ev_target_soc_override,
                ev_charging_allowed=self.ev_charging_allowed,
                ev_min_range_km=self.ev_min_range_km,
                ev_solar_only_grid_buffer_enabled=self.ev_solar_only_grid_buffer_enabled,
                ev_active_solar_slot=self._ev_active_solar_slot,
                ev_next_departure=self.ev_next_departure,
                get_raw_prices=self._get_raw_prices,
                forecast_kwh_between=self._forecast_kwh_between,
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("EV update fejlede: %s", e)

    async def _handle_unexpected_grid_events(
        self,
        *,
        data: SolarFriendData,
        now: datetime,
    ) -> None:
        """Detect and handle persistent unexpected import/charging situations."""
        if (now - self._startup_at) < timedelta(minutes=2):
            return

        strategy = data.optimize_result.strategy if data.optimize_result is not None else ""
        events = self._ensure_tracker_runtime().detect_unexpected_grid_events(
            now=now,
            strategy=strategy,
            grid_power=data.grid_power,
            battery_power=data.battery_power,
            pv_power=data.pv_power,
            load_power=data.load_power,
            ev_charge_mode=getattr(self, "ev_charge_mode", ""),
            ev_charging_power=data.ev_charging_power,
        )
        for event in events:
            if event == "unauthorized_battery_grid_charge":
                _LOGGER.warning(
                    "Unexpected battery grid charging detected: strategy=%s grid=%.0fW "
                    "battery=%.0fW pv=%.0fW load=%.0fW ev=%.0fW. Re-applying safe inverter state.",
                    strategy,
                    data.grid_power,
                    data.battery_power,
                    data.pv_power,
                    data.load_power,
                    data.ev_charging_power,
                )
                if self._inverter is not None and self._inverter.is_configured:
                    await self._inverter.apply(
                        OptimizeResult.idle(
                            "Unexpected battery charging under grid import - forcing safe inverter state",
                            weighted_cost=data.battery_weighted_cost,
                            solar_fraction=data.battery_solar_fraction,
                        )
                    )
            elif event == "ev_battery_grid_conflict":
                _LOGGER.warning(
                    "Persistent EV/battery grid conflict detected: mode=%s strategy=%s grid=%.0fW "
                    "battery=%.0fW pv=%.0fW load=%.0fW ev=%.0fW. EV runtime should have corrected this already.",
                    getattr(self, "ev_charge_mode", ""),
                    strategy,
                    data.grid_power,
                    data.battery_power,
                    data.pv_power,
                    data.load_power,
                    data.ev_charging_power,
                )

    async def _fetch_weather_hourly_forecast(self) -> list[dict[str, Any]]:
        """Fetch and cache hourly weather forecast for Solar Only profiling."""
        return await self._ensure_weather_service().async_fetch_hourly_forecast()

    async def _get_current_solar_only_profile(self, now: datetime) -> SolarOnlyWeatherProfile:
        """Return the active Solar Only weather profile for the current hour."""
        return await self._ensure_ev_service().get_current_solar_only_profile(now)

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


def _get_slot_forecast_kwh(hourly_forecast: list[dict], now: datetime) -> float:
    """Return Solcast forecast kWh for the 30-min slot containing `now`."""
    minute = 0 if now.minute < 30 else 30
    slot_start = now.replace(minute=minute, second=0, microsecond=0)
    for entry in hourly_forecast:
        ps = entry.get("period_start")
        if ps is None:
            continue
        try:
            if isinstance(ps, str):
                ps = datetime.fromisoformat(ps)
            if ps.tzinfo is not None:
                from homeassistant.util import dt as _ha_dt
                ps = _ha_dt.as_local(ps)
            ps_norm = ps.replace(
                minute=0 if ps.minute < 30 else 30,
                second=0,
                microsecond=0,
                tzinfo=None,
            )
            if ps_norm == slot_start.replace(tzinfo=None):
                return float(entry.get("pv_estimate_kwh", 0.0))
        except (TypeError, ValueError):
            continue
    return 0.0


def _forecast_slot_delta(hourly_forecast: list[dict]) -> timedelta:
    """Infer the forecast slot duration from period_start deltas."""
    starts: list[datetime] = []
    for entry in hourly_forecast:
        ps = entry.get("period_start")
        if ps is None:
            continue
        try:
            if isinstance(ps, str):
                ps = datetime.fromisoformat(ps)
            if ps.tzinfo is not None:
                ps = ha_dt.as_local(ps)
            starts.append(ps.replace(second=0, microsecond=0, tzinfo=None))
        except (TypeError, ValueError):
            continue
    starts.sort()
    for idx in range(1, len(starts)):
        delta = starts[idx] - starts[idx - 1]
        if delta.total_seconds() > 0:
            return delta
    return timedelta(hours=1)


