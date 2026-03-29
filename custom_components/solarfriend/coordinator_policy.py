"""Coordinator policy defaults.

AI bot guide:
- Keep coordinator tuning values here instead of scattering magic numbers.
- Runtime helpers should depend on this policy object, not module globals.
- If behavior needs changing, prefer editing policy values before editing control flow.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True)
class CoordinatorPolicy:
    """Central runtime tuning knobs for the coordinator layer."""

    update_interval: timedelta = timedelta(seconds=30)
    price_surplus_factor: float = 1.20
    price_cheap_factor: float = 0.80
    price_history_max: int = 48
    night_hours: frozenset[int] = field(
        default_factory=lambda: frozenset(range(22, 24)) | frozenset(range(0, 7))
    )
    soc_trigger_delta: float = 5.0
    optimize_min_interval: timedelta = timedelta(minutes=5)
    strategy_soft_cooldown: timedelta = timedelta(minutes=5)
    strategy_confirmation_required: int = 2
    pv_drop_override_fraction: float = 0.40
    pv_drop_override_min_w: float = 500.0
    soc_override_margin: float = 2.0
    sunset_override_remaining_kwh: float = 0.25
    battery_noise_w: float = 50.0
    plan_deviation_min_w: float = 400.0
    plan_deviation_fraction: float = 0.25
    ev_grid_priority_margin_w: float = 200.0
    ev_battery_protection_margin_w: float = 250.0
    # Treat EV charging as "active" once measured charging power is above
    # this threshold. The coordinator uses it to block battery-selling while
    # Solar Only charging is consuming the available solar directly.
    ev_active_charge_w: float = 100.0
    unexpected_grid_import_w: float = 500.0
    unexpected_battery_charge_w: float = 500.0
    unexpected_battery_grid_charge_duration: timedelta = timedelta(minutes=1)
    unexpected_ev_grid_conflict_duration: timedelta = timedelta(minutes=10)
    unexpected_grid_solar_margin_w: float = 300.0


DEFAULT_COORDINATOR_POLICY = CoordinatorPolicy()
