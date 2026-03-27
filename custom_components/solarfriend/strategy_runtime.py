"""Battery strategy hold/hysteresis runtime helper."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from .coordinator_policy import CoordinatorPolicy


@dataclass
class StrategyRuntimeState:
    """Mutable strategy confirmation state."""

    active_strategy_since: datetime | None = None
    active_strategy_reference_pv: float = 0.0
    pending_strategy: str | None = None
    pending_strategy_count: int = 0


class StrategyRuntime:
    """Encapsulate strategy soft cooldown and confirmation logic."""

    def __init__(self, policy: CoordinatorPolicy, *, config_entry: Any) -> None:
        self._policy = policy
        self._config_entry = config_entry
        self._state = StrategyRuntimeState()

    @property
    def state(self) -> StrategyRuntimeState:
        """Expose state for debugging/tests if needed."""
        return self._state

    def reset_pending(self) -> None:
        self._state.pending_strategy = None
        self._state.pending_strategy_count = 0

    def _mark_applied(self, result: Any, now: datetime, pv_power: float) -> None:
        self._state.active_strategy_since = now
        self._state.active_strategy_reference_pv = max(0.0, pv_power)
        self.reset_pending()

    def _override_allowed(
        self,
        active_result: Any,
        desired_result: Any,
        *,
        now: datetime,
        current_soc: float,
        pv_power: float,
        sunset: datetime,
        solar_until_sunset_kwh: float,
    ) -> bool:
        if desired_result.strategy == "ANTI_EXPORT":
            return True

        cfg = self._config_entry.data
        min_soc = float(cfg.get("battery_min_soc", 10.0))
        max_soc = float(cfg.get("battery_max_soc", 100.0))
        if current_soc <= (min_soc + self._policy.soc_override_margin):
            return True
        if current_soc >= (max_soc - self._policy.soc_override_margin):
            return True
        if desired_result.strategy == active_result.strategy:
            return True
        if now >= sunset:
            return True
        if (
            desired_result.strategy == "SAVE_SOLAR"
            and solar_until_sunset_kwh <= self._policy.sunset_override_remaining_kwh
        ):
            return True

        reference_pv = max(0.0, self._state.active_strategy_reference_pv)
        pv_drop_w = max(0.0, reference_pv - max(0.0, pv_power))
        if reference_pv > 0:
            pv_drop_fraction = pv_drop_w / reference_pv
            if (
                pv_drop_w >= self._policy.pv_drop_override_min_w
                and pv_drop_fraction >= self._policy.pv_drop_override_fraction
            ):
                return True
        return False

    def select_result(
        self,
        desired_result: Any,
        *,
        active_result: Any | None,
        now: datetime,
        current_soc: float,
        pv_power: float,
        sunset: datetime,
        solar_until_sunset_kwh: float,
    ) -> tuple[Any, bool]:
        """Apply hysteresis/hold logic and return (result_to_apply, strategy_changed)."""
        if active_result is None:
            self._mark_applied(desired_result, now, pv_power)
            return desired_result, True

        if desired_result.strategy == active_result.strategy:
            self.reset_pending()
            return desired_result, False

        if self._override_allowed(
            active_result,
            desired_result,
            now=now,
            current_soc=current_soc,
            pv_power=pv_power,
            sunset=sunset,
            solar_until_sunset_kwh=solar_until_sunset_kwh,
        ):
            self._mark_applied(desired_result, now, pv_power)
            return desired_result, True

        if self._state.pending_strategy == desired_result.strategy:
            self._state.pending_strategy_count += 1
        else:
            self._state.pending_strategy = desired_result.strategy
            self._state.pending_strategy_count = 1

        hold_elapsed = (
            self._state.active_strategy_since is None
            or (now - self._state.active_strategy_since) >= self._policy.strategy_soft_cooldown
        )
        if hold_elapsed and self._state.pending_strategy_count >= self._policy.strategy_confirmation_required:
            self._mark_applied(desired_result, now, pv_power)
            return desired_result, True

        return active_result, False

    def apply_runtime_overrides(
        self,
        result: Any,
        *,
        battery_sell_enabled: bool,
        ev_enabled: bool,
        ev_charge_mode: str,
        ev_currently_charging: bool,
        ev_charging_power: float,
    ) -> Any:
        """Apply runtime gating on top of the pure optimizer result."""
        if result.strategy != "SELL_BATTERY":
            return result

        ev_actively_charging = bool(
            ev_enabled
            and ev_charge_mode == "solar_only"
            and (
                ev_currently_charging
                or float(ev_charging_power) > self._policy.ev_active_charge_w
            )
        )
        if ev_actively_charging:
            return replace(
                result,
                strategy="SAVE_SOLAR",
                reason=(
                    "Battery sell er blokeret, fordi EV lader aktivt i solar_only. "
                    f"{result.reason}"
                ),
                solar_sell=True,
            )

        if battery_sell_enabled:
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

    @staticmethod
    def load_learning_allowed(result: Any | None) -> bool:
        """Return True when live load/grid telemetry is safe to learn from."""
        return result is None or result.strategy != "SELL_BATTERY"
