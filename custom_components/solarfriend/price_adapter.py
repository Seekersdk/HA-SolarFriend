"""SolarFriend price adapter.

Normalises spot-price data from Home Assistant sensors so the rest of the
integration can work against a stable in-memory snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from homeassistant.util import dt as ha_dt


def _to_local_aware(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in local time."""
    if dt.tzinfo is None:
        return ha_dt.as_local(dt.replace(tzinfo=ha_dt.UTC))
    return ha_dt.as_local(dt)


def _parse_entry_start(raw_dt: Any, now: datetime, previous_start: datetime | None) -> datetime | None:
    """Parse the start time from a raw price entry."""
    if isinstance(raw_dt, datetime):
        return _to_local_aware(raw_dt).replace(minute=0, second=0, microsecond=0)

    if isinstance(raw_dt, int):
        candidate = now.replace(hour=raw_dt % 24, minute=0, second=0, microsecond=0)
        while candidate < now - timedelta(hours=1):
            candidate += timedelta(days=1)
        if previous_start is not None:
            while candidate <= previous_start:
                candidate += timedelta(days=1)
        return candidate

    if raw_dt is None:
        return None

    try:
        parsed = datetime.fromisoformat(str(raw_dt))
    except (ValueError, TypeError):
        return None
    return _to_local_aware(parsed).replace(minute=0, second=0, microsecond=0)


def get_current_price_from_raw(
    raw_prices: list[dict[str, Any]],
    now: datetime,
    fallback: float | None = None,
) -> float | None:
    """Return the price for the hour containing ``now`` if it can be resolved."""
    now_local = _to_local_aware(now).replace(minute=0, second=0, microsecond=0)
    previous_start: datetime | None = None

    for entry in raw_prices:
        raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
        if raw_price is None:
            continue

        raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
        if isinstance(raw_dt, int):
            if raw_dt % 24 == now_local.hour:
                try:
                    return float(raw_price)
                except (TypeError, ValueError):
                    continue
            continue

        start = _parse_entry_start(raw_dt, now_local, previous_start)
        if start is None:
            continue
        previous_start = start
        if start <= now_local < start + timedelta(hours=1):
            try:
                return float(raw_price)
            except (TypeError, ValueError):
                return fallback

    return fallback


@dataclass(frozen=True)
class PricePoint:
    """Canonical hourly price point."""

    start: datetime
    end: datetime
    price: float


@dataclass
class PriceData:
    """Normalised price snapshot for the current poll cycle."""

    points: list[PricePoint] = field(default_factory=list)
    current_price: float | None = None
    source_entity: str = ""

    def to_legacy_raw_prices(self) -> list[dict[str, Any]]:
        """Return a stable list format understood by current optimizers."""
        return [
            {"start": point.start, "end": point.end, "price": point.price}
            for point in self.points
        ]


class PriceAdapter:
    """Build a PriceData snapshot from a configured HA sensor."""

    @staticmethod
    def from_hass(hass: Any, price_sensor_entity: str | None) -> PriceData | None:
        if not price_sensor_entity:
            return None

        state = hass.states.get(price_sensor_entity)
        if state is None or state.state in ("unavailable", "unknown", ""):
            return None

        now = ha_dt.now()
        raw_prices: list[dict[str, Any]] = []

        raw_today = state.attributes.get("raw_today", []) or []
        raw_tomorrow = state.attributes.get("raw_tomorrow", []) or []
        if isinstance(raw_today, list):
            raw_prices.extend(raw_today)
        if isinstance(raw_tomorrow, list):
            raw_prices.extend(raw_tomorrow)

        if not raw_prices:
            for attr_key in ("today", "prices"):
                candidate = state.attributes.get(attr_key)
                if isinstance(candidate, list) and candidate:
                    raw_prices = candidate
                    break

        points: list[PricePoint] = []
        previous_start: datetime | None = None
        for entry in raw_prices:
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_price is None:
                continue
            start = _parse_entry_start(
                entry.get("start") if entry.get("start") is not None else entry.get("hour"),
                now,
                previous_start,
            )
            if start is None:
                continue
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                continue
            points.append(PricePoint(start=start, end=start + timedelta(hours=1), price=price))
            previous_start = start

        points.sort(key=lambda p: p.start)

        try:
            state_price = float(state.state)
        except (TypeError, ValueError):
            state_price = None

        current_price = get_current_price_from_raw(
            raw_prices,
            now,
            fallback=state_price,
        )

        if current_price is None and not points:
            return None

        return PriceData(
            points=points,
            current_price=current_price,
            source_entity=price_sensor_entity,
        )
