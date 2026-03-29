"""Runtime EV state machine and charger-side action handling."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime
from typing import Any

from .weather_profile import SolarOnlyWeatherProfile

_LOGGER = logging.getLogger(__name__)


class EVRuntimeController:
    """Own mutable EV charger runtime state and charger actions."""

    def __init__(
        self,
        *,
        ev_optimizer: Any,
        ev_charger: Any,
        min_action_interval_seconds: int = 120,
        min_session_seconds: int = 300,
        min_power_change_interval_seconds: int = 30,
    ) -> None:
        self._ev_optimizer = ev_optimizer
        self._ev_charger = ev_charger
        self._min_action_interval_seconds = min_action_interval_seconds
        self._min_session_seconds = min_session_seconds
        self._min_power_change_interval_seconds = min_power_change_interval_seconds
        self._last_action_time: datetime | None = None
        self._charging_started_at: datetime | None = None
        self._currently_charging: bool = False
        self._last_power_command: tuple[float, int] | None = None
        self._last_power_command_time: datetime | None = None
        self._sync_on_startup: bool = True
        self._solar_start_candidate_since: datetime | None = None
        self._solar_stop_candidate_since: datetime | None = None

    @property
    def currently_charging(self) -> bool:
        """Return whether runtime state believes charging is active."""
        return self._currently_charging

    def actual_charging(self, *, charger_status: str | None, charger_power: float | None) -> bool:
        """Return True when charger status/power indicate active charging."""
        return charger_status == "charging" or float(charger_power or 0.0) > 100.0

    def sync_startup(self, charger_status: str | None) -> None:
        """Sync runtime state once on startup from the actual charger status."""
        if not self._sync_on_startup:
            return
        self._sync_on_startup = False
        self._currently_charging = charger_status == "charging"
        if self._currently_charging:
            _LOGGER.info("EV: synkroniseret til charging ved opstart")

    def set_currently_charging_from_actual(
        self,
        *,
        charger_status: str | None,
        charger_power: float | None,
    ) -> bool:
        """Sync runtime charging flag from actual charger behavior."""
        actual = self.actual_charging(charger_status=charger_status, charger_power=charger_power)
        self._currently_charging = actual
        return actual

    def apply_solar_only_hysteresis(
        self,
        *,
        ctx: Any,
        result: Any,
        profile: SolarOnlyWeatherProfile,
        actual_charging: bool,
    ):
        """Apply time-based start/stop hysteresis for Solar Only EV charging."""
        now = ctx.now

        def _prefix_reason(reason: str) -> str:
            return f"{profile.label}: {reason}"

        if ctx.charger_status == "disconnected" or ctx.vehicle_soc >= ctx.vehicle_target_soc:
            self._solar_start_candidate_since = None
            self._solar_stop_candidate_since = None
            result.reason = _prefix_reason(result.reason)
            return result

        if result.should_charge:
            self._solar_stop_candidate_since = None
            if actual_charging:
                self._solar_start_candidate_since = None
                result.reason = _prefix_reason(result.reason)
                return result

            if self._solar_start_candidate_since is None:
                self._solar_start_candidate_since = now

            elapsed = (now - self._solar_start_candidate_since).total_seconds()
            if elapsed < profile.start_hold_seconds:
                return replace(
                    result,
                    should_charge=False,
                    target_w=0.0,
                    phases=0,
                    target_amps=0.0,
                    reason=_prefix_reason(
                        f"Start hysterese {int(elapsed)}/{profile.start_hold_seconds}s "
                        f"({result.surplus_w:.0f}W >= {profile.start_surplus_w:.0f}W)"
                    ),
                )

            self._solar_start_candidate_since = None
            result.reason = _prefix_reason(result.reason)
            return result

        self._solar_start_candidate_since = None
        if not actual_charging:
            self._solar_stop_candidate_since = None
            result.reason = _prefix_reason(result.reason)
            return result

        if self._solar_stop_candidate_since is None:
            self._solar_stop_candidate_since = now

        elapsed = (now - self._solar_stop_candidate_since).total_seconds()
        if elapsed < profile.stop_hold_seconds:
            hold_surplus_w = max(0.0, result.surplus_w)
            hold_target_candidate_w = max(
                hold_surplus_w + profile.grid_buffer_w,
                1410.0,
            )
            _, phases, amps, actual_w = self._ev_optimizer._calc_phase_and_amps(
                hold_target_candidate_w,
                ctx.max_charge_kw * 1000.0,
            )
            if actual_w <= 0:
                phases = 1
                amps = 6.0
                actual_w = 1410.0
            return replace(
                result,
                should_charge=True,
                target_w=actual_w,
                phases=phases,
                target_amps=amps,
                reason=_prefix_reason(
                    f"Stop hysterese {int(elapsed)}/{profile.stop_hold_seconds}s "
                    f"({result.surplus_w:.0f}W < {profile.stop_surplus_w:.0f}W)"
                ),
            )

        self._solar_stop_candidate_since = None
        result.reason = _prefix_reason(result.reason)
        return result

    async def async_apply_charge_decision(self, *, ev_result: Any, now: datetime) -> None:
        """Apply resume/pause/set-power decisions with anti-flap handling."""
        can_act = (
            self._last_action_time is None
            or (now - self._last_action_time).total_seconds() > self._min_action_interval_seconds
        )

        if not can_act:
            return

        if ev_result.should_charge and not self._currently_charging:
            await self._ev_charger.resume()
            await asyncio.sleep(2)
            try:
                await self._ev_charger.set_power(ev_result.target_w, ev_result.phases)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning(
                    "EV: set_power fejlede efter resume; forsøger rollback med pause: %s",
                    exc,
                )
                try:
                    await self._ev_charger.pause()
                except Exception as rollback_exc:  # noqa: BLE001
                    _LOGGER.warning(
                        "EV: rollback pause fejlede efter delvis start; runtime efterlades som ikke-ladende: %s",
                        rollback_exc,
                    )
                self._charging_started_at = None
                self._currently_charging = False
                self._last_power_command = None
                self._last_power_command_time = None
                return
            self._currently_charging = True
            self._charging_started_at = now
            self._last_action_time = now
            self._last_power_command = (float(ev_result.target_w), int(ev_result.phases))
            self._last_power_command_time = now
            _LOGGER.info(
                "EV: start ladning %d-fase %.1fA (%.0fW) — %s",
                ev_result.phases,
                ev_result.target_amps,
                ev_result.target_w,
                ev_result.reason,
            )
            return

        if ev_result.should_charge and self._currently_charging:
            command = (float(ev_result.target_w), int(ev_result.phases))
            if self._last_power_command == command:
                return
            if self._last_power_command_time is not None:
                power_elapsed = (now - self._last_power_command_time).total_seconds()
                if power_elapsed < self._min_power_change_interval_seconds:
                    _LOGGER.debug(
                        "EV: skip power change %.0fW/%d-fase -> %.0fW/%d-fase; cooldown %ds ikke nået (%.0fs)",
                        self._last_power_command[0] if self._last_power_command else 0.0,
                        self._last_power_command[1] if self._last_power_command else 0,
                        command[0],
                        command[1],
                        self._min_power_change_interval_seconds,
                        power_elapsed,
                    )
                    return
            await self._ev_charger.set_power(ev_result.target_w, ev_result.phases)
            self._last_power_command = command
            self._last_power_command_time = now
            return

        if not ev_result.should_charge and self._currently_charging:
            if self._charging_started_at is not None:
                session_elapsed = (now - self._charging_started_at).total_seconds()
                if session_elapsed < self._min_session_seconds:
                    _LOGGER.debug(
                        "EV: min session %ds ikke nået (%.0fs) — skip stop: %s",
                        self._min_session_seconds,
                        session_elapsed,
                        ev_result.reason,
                    )
                    return
            await self._ev_charger.pause()
            self._charging_started_at = None
            self._currently_charging = False
            self._last_action_time = now
            self._last_power_command = None
            self._last_power_command_time = None
            _LOGGER.info("EV: stop ladning — %s", ev_result.reason)
