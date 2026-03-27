"""Price snapshot and price-history runtime helpers."""
from __future__ import annotations

import statistics
from datetime import datetime
from typing import Any

from .coordinator_policy import CoordinatorPolicy
from .price_adapter import PriceData, get_current_price_from_raw


class PriceRuntime:
    """Own rolling price state and snapshot resolution logic."""

    def __init__(self, policy: CoordinatorPolicy) -> None:
        self._policy = policy
        self._price_history: list[float] = []
        self._night_prices: dict[int, float] = {}
        self._cached_buy_price_data: PriceData | None = None
        self._cached_sell_price_data: PriceData | None = None

    def trim_snapshot(
        self,
        snapshot: PriceData,
        now: datetime,
        normalize_local_datetime: Any,
    ) -> PriceData | None:
        """Return a forward-looking price snapshot with past hours removed."""
        raw_prices = snapshot.to_legacy_raw_prices()
        current_hour = normalize_local_datetime(now).replace(minute=0, second=0, microsecond=0)
        points = [point for point in snapshot.points if point.end > current_hour]
        current_price = get_current_price_from_raw(raw_prices, now, fallback=snapshot.current_price)
        if current_price is None and not points:
            return None
        return PriceData(
            points=points,
            current_price=current_price,
            source_entity=snapshot.source_entity,
        )

    def resolve_snapshot(
        self,
        now: datetime,
        cache_kind: str,
        fresh_snapshot: PriceData | None,
        normalize_local_datetime: Any,
    ) -> PriceData | None:
        """Prefer fresh actual prices, otherwise fall back to the last valid snapshot."""
        cache_attr = "_cached_sell_price_data" if cache_kind == "sell" else "_cached_buy_price_data"
        if fresh_snapshot is not None:
            trimmed_fresh = self.trim_snapshot(fresh_snapshot, now, normalize_local_datetime)
            if trimmed_fresh is not None:
                setattr(self, cache_attr, trimmed_fresh)
                return trimmed_fresh

        cached_snapshot = getattr(self, cache_attr)
        if cached_snapshot is None:
            return None

        trimmed_cached = self.trim_snapshot(cached_snapshot, now, normalize_local_datetime)
        if trimmed_cached is None:
            return None

        setattr(self, cache_attr, trimmed_cached)
        return trimmed_cached

    def update_history(self, price: float) -> None:
        """Maintain the rolling price history used for cheap/expensive heuristics."""
        self._price_history.append(price)
        if len(self._price_history) > self._policy.price_history_max:
            self._price_history.pop(0)

    def price_average(self) -> float | None:
        """Return the rolling average price, if any history exists."""
        if not self._price_history:
            return None
        return statistics.mean(self._price_history)

    def battery_strategy(self, solar_surplus: float, price: float, avg_price: float | None) -> str:
        """Return the coarse live battery strategy label for sensors/debugging."""
        if solar_surplus > 0:
            return "CHARGE_SOLAR"
        if avg_price is not None:
            if price > avg_price * self._policy.price_surplus_factor:
                return "USE_BATTERY"
            if price < avg_price * self._policy.price_cheap_factor:
                return "CHARGE_GRID"
        return "IDLE"

    def record_night_price(self, hour: int, price: float) -> None:
        """Track the cheapest seen night price per hour bucket."""
        if hour not in self._policy.night_hours:
            return
        existing = self._night_prices.get(hour)
        if existing is None or price < existing:
            self._night_prices[hour] = price

    def min_night_price(self) -> float | None:
        """Return the cheapest recorded night price."""
        if not self._night_prices:
            return None
        return min(self._night_prices.values())

    def price_level(self, price: float, avg_price: float | None) -> str:
        """Map the live price into CHEAP/NORMAL/EXPENSIVE."""
        if avg_price is None:
            return "NORMAL"
        if price > avg_price * self._policy.price_surplus_factor:
            return "EXPENSIVE"
        if price < avg_price * self._policy.price_cheap_factor:
            return "CHEAP"
        return "NORMAL"
