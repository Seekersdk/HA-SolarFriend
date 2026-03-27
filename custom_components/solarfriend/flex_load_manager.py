"""Flexible load reservation storage and slot booking.

AI bot guide:
- This module owns user-facing flex-load reservations created via HA services.
- Keep booking semantics idempotent via `job_id`; updates replace active requests.
- Return plan data here, but let coordinator/services decide when to refresh/apply.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
import math
from typing import Any

from homeassistant.helpers.storage import Store
from homeassistant.util import dt as ha_dt

STORAGE_VERSION = 1
STORAGE_KEY = "solarfriend.flex_load_reservations"
_STEP_MINUTES = 30


def _ensure_local(value: datetime) -> datetime:
    """Normalise datetimes to local timezone for stable comparisons."""
    if value.tzinfo is None:
        return ha_dt.as_local(value)
    return ha_dt.as_local(value)


def _round_up_to_step(value: datetime, *, minutes: int) -> datetime:
    """Round a datetime up to the next booking step."""
    value = _ensure_local(value)
    remainder = value.minute % minutes
    if remainder == 0 and value.second == 0 and value.microsecond == 0:
        return value.replace(second=0, microsecond=0)
    delta_minutes = minutes - remainder if remainder else 0
    return (value + timedelta(minutes=delta_minutes)).replace(second=0, microsecond=0)


def _slot_start(value: datetime) -> datetime:
    """Return the reservation slice start for a given datetime."""
    value = _ensure_local(value)
    minute = 0 if value.minute < _STEP_MINUTES else _STEP_MINUTES
    return value.replace(minute=minute, second=0, microsecond=0)


@dataclass
class FlexLoadReservation:
    """Stored flex-load reservation."""

    job_id: str
    name: str
    preferred_source: str
    duration_minutes: int
    energy_wh: float
    power_w: float
    earliest_start: str
    deadline: str
    start_time: str
    end_time: str
    allow_battery: bool
    max_grid_w: float | None
    min_solar_w: float | None
    expected_solar_kwh: float
    expected_grid_kwh: float
    expected_cost_dkk: float
    reserved_solar_kwh_by_hour: dict[str, float] = field(default_factory=dict)
    status: str = "booked"
    created_at: str = ""
    updated_at: str = ""
    source_note: str = ""


@dataclass
class FlexLoadSnapshot:
    """UI-facing snapshot of upcoming flex reservations."""

    reservations_count: int = 0
    next_name: str = ""
    next_start: str = ""
    next_end: str = ""
    next_power_w: float = 0.0
    reserved_solar_today_kwh: float = 0.0
    reserved_solar_tomorrow_kwh: float = 0.0
    reservations: list[dict[str, Any]] = field(default_factory=list)


class FlexLoadReservationManager:
    """Persist and compute flexible-load reservations."""

    def __init__(self, hass: Any, entry_id: str) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY}.{entry_id}")
        self._reservations: dict[str, FlexLoadReservation] = {}

    async def async_load(self) -> None:
        """Load reservations from HA storage."""
        try:
            data: dict[str, Any] | None = await self._store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "FlexLoadReservationManager storage load failed for %s; starting fresh: %s",
                f"{STORAGE_KEY}.{self._entry_id}",
                exc,
            )
            return
        if not data:
            return
        reservations = data.get("reservations", {})
        self._reservations = {
            job_id: FlexLoadReservation(**payload)
            for job_id, payload in reservations.items()
        }

    async def async_save(self) -> None:
        """Persist reservations to HA storage."""
        await self._store.async_save(
            {
                "reservations": {
                    job_id: asdict(reservation)
                    for job_id, reservation in self._reservations.items()
                }
            }
        )

    @property
    def reservations(self) -> dict[str, FlexLoadReservation]:
        """Expose reservations for read-only consumers."""
        return self._reservations

    def _active_reservations(self, now: datetime) -> list[FlexLoadReservation]:
        """Return future-active reservations and mark expired ones."""
        now = _ensure_local(now)
        active: list[FlexLoadReservation] = []
        for reservation in self._reservations.values():
            try:
                end_time = _ensure_local(datetime.fromisoformat(reservation.end_time))
            except ValueError:
                reservation.status = "expired"
                continue
            if end_time <= now:
                reservation.status = "expired"
                continue
            active.append(reservation)
        active.sort(key=lambda item: item.start_time)
        return active

    def cancel(self, job_id: str) -> bool:
        """Remove an existing reservation."""
        return self._reservations.pop(job_id, None) is not None

    def reserved_solar_kwh_by_hour(
        self,
        now: datetime,
        *,
        exclude_job_id: str | None = None,
    ) -> dict[datetime, float]:
        """Return active solar reservations aggregated by local hour."""
        reserved: dict[datetime, float] = {}
        for reservation in self._active_reservations(now):
            if reservation.job_id == exclude_job_id:
                continue
            for hour_iso, reserved_kwh in reservation.reserved_solar_kwh_by_hour.items():
                try:
                    hour_start = _ensure_local(datetime.fromisoformat(hour_iso)).replace(
                        minute=0, second=0, microsecond=0
                    )
                except ValueError:
                    continue
                reserved[hour_start] = reserved.get(hour_start, 0.0) + float(reserved_kwh)
        return reserved

    def _reserved_solar_kwh_by_slot(
        self,
        now: datetime,
        *,
        exclude_job_id: str | None = None,
    ) -> dict[datetime, float]:
        """Return active solar reservations keyed by booking slice."""
        reserved: dict[datetime, float] = {}
        for reservation in self._active_reservations(now):
            if reservation.job_id == exclude_job_id:
                continue
            for slot_iso, reserved_kwh in reservation.reserved_solar_kwh_by_hour.items():
                try:
                    slot_start = _slot_start(datetime.fromisoformat(slot_iso))
                except ValueError:
                    continue
                reserved[slot_start] = reserved.get(slot_start, 0.0) + float(reserved_kwh)
        return reserved

    def build_snapshot(self, now: datetime) -> FlexLoadSnapshot:
        """Build a dashboard-friendly reservation snapshot."""
        now = _ensure_local(now)
        active = self._active_reservations(now)
        snapshot = FlexLoadSnapshot(
            reservations_count=len(active),
            reservations=[
                {
                    "job_id": reservation.job_id,
                    "name": reservation.name,
                    "preferred_source": reservation.preferred_source,
                    "start_time": reservation.start_time,
                    "end_time": reservation.end_time,
                    "power_w": round(reservation.power_w),
                    "energy_wh": round(reservation.energy_wh),
                    "expected_solar_kwh": round(reservation.expected_solar_kwh, 3),
                    "expected_grid_kwh": round(reservation.expected_grid_kwh, 3),
                    "expected_cost_dkk": round(reservation.expected_cost_dkk, 3),
                    "status": reservation.status,
                    "source_note": reservation.source_note,
                }
                for reservation in active
            ],
        )
        if active:
            upcoming = active[0]
            snapshot.next_name = upcoming.name
            snapshot.next_start = upcoming.start_time
            snapshot.next_end = upcoming.end_time
            snapshot.next_power_w = round(upcoming.power_w)

        today = now.date()
        tomorrow = (now + timedelta(days=1)).date()
        for reservation in active:
            for hour_iso, reserved_kwh in reservation.reserved_solar_kwh_by_hour.items():
                try:
                    hour_dt = _ensure_local(datetime.fromisoformat(hour_iso))
                except ValueError:
                    continue
                if hour_dt.date() == today:
                    snapshot.reserved_solar_today_kwh += float(reserved_kwh)
                elif hour_dt.date() == tomorrow:
                    snapshot.reserved_solar_tomorrow_kwh += float(reserved_kwh)
        snapshot.reserved_solar_today_kwh = round(snapshot.reserved_solar_today_kwh, 3)
        snapshot.reserved_solar_tomorrow_kwh = round(snapshot.reserved_solar_tomorrow_kwh, 3)
        return snapshot

    def upsert(
        self,
        *,
        now: datetime,
        job_id: str,
        name: str,
        energy_wh: float,
        power_w: float,
        duration_minutes: int,
        earliest_start: datetime,
        deadline: datetime,
        preferred_source: str,
        min_solar_w: float | None,
        max_grid_w: float | None,
        allow_battery: bool,
        hourly_forecast: list[dict[str, Any]],
        raw_prices: list[dict[str, Any]],
        consumption_profile: Any,
    ) -> dict[str, Any]:
        """Create or replace a reservation and return the computed booking."""
        now = _ensure_local(now)
        earliest_start = max(now, _ensure_local(earliest_start))
        deadline = _ensure_local(deadline)
        if deadline <= earliest_start:
            raise ValueError("deadline must be after earliest_start")
        if duration_minutes <= 0:
            raise ValueError("duration_minutes must be positive")
        if energy_wh <= 0 and power_w <= 0:
            raise ValueError("energy_wh or power_w must be positive")
        duration_h = duration_minutes / 60.0
        if energy_wh <= 0:
            energy_wh = power_w * duration_h
        if power_w <= 0:
            power_w = energy_wh / duration_h

        reserved_existing = self._reserved_solar_kwh_by_slot(now, exclude_job_id=job_id)
        forecast_by_hour: dict[datetime, float] = {}
        for entry in hourly_forecast or []:
            raw_start = entry.get("period_start")
            if raw_start is None:
                continue
            try:
                slot_start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start))
            except (TypeError, ValueError):
                continue
            slot_start = _ensure_local(slot_start).replace(minute=0, second=0, microsecond=0)
            forecast_by_hour[slot_start] = forecast_by_hour.get(slot_start, 0.0) + float(
                entry.get("pv_estimate_kwh", 0.0)
            ) * 1000.0

        price_by_hour: dict[datetime, float] = {}
        for entry in raw_prices or []:
            raw_dt = entry.get("start") if entry.get("start") is not None else entry.get("hour")
            raw_price = entry.get("price") if entry.get("price") is not None else entry.get("value")
            if raw_dt is None or raw_price is None:
                continue
            try:
                hour_start = raw_dt if isinstance(raw_dt, datetime) else datetime.fromisoformat(str(raw_dt))
            except (TypeError, ValueError):
                continue
            price_by_hour[_ensure_local(hour_start).replace(minute=0, second=0, microsecond=0)] = float(raw_price)

        duration = timedelta(minutes=duration_minutes)
        slot_start = _round_up_to_step(earliest_start, minutes=_STEP_MINUTES)
        latest_start = deadline - duration
        if slot_start > latest_start:
            raise ValueError("No valid slot before deadline")

        best_candidate: dict[str, Any] | None = None
        while slot_start <= latest_start:
            cursor = slot_start
            solar_kwh = 0.0
            grid_kwh = 0.0
            cost_dkk = 0.0
            solar_surplus_samples: list[float] = []
            reserved_solar_by_hour: dict[str, float] = {}
            while cursor < slot_start + duration:
                slice_start = _slot_start(cursor)
                hour_start = cursor.replace(minute=0, second=0, microsecond=0)
                pv_w = forecast_by_hour.get(hour_start, 0.0)
                load_w = float(
                    consumption_profile.get_predicted_watt(
                        hour_start.hour,
                        hour_start.weekday() >= 5,
                    )
                )
                reserved_w = reserved_existing.get(slice_start, 0.0) * 1000.0 / (_STEP_MINUTES / 60.0)
                solar_surplus_w = max(0.0, pv_w - load_w - reserved_w)
                solar_used_w = min(power_w, solar_surplus_w)
                grid_used_w = max(0.0, power_w - solar_used_w)
                if max_grid_w is not None and grid_used_w > max_grid_w:
                    cost_dkk = math.inf
                    break
                slice_hours = _STEP_MINUTES / 60.0
                solar_kwh += solar_used_w / 1000.0 * slice_hours
                grid_slice_kwh = grid_used_w / 1000.0 * slice_hours
                grid_kwh += grid_slice_kwh
                price = price_by_hour.get(hour_start)
                if grid_slice_kwh > 0 and price is None:
                    cost_dkk = math.inf
                    break
                cost_dkk += 0.0 if price is None else grid_slice_kwh * price
                solar_surplus_samples.append(solar_surplus_w)
                reserved_key = slice_start.isoformat()
                reserved_solar_by_hour[reserved_key] = reserved_solar_by_hour.get(reserved_key, 0.0) + (
                    solar_used_w / 1000.0 * slice_hours
                )
                cursor += timedelta(minutes=_STEP_MINUTES)

            avg_solar_surplus_w = (
                sum(solar_surplus_samples) / len(solar_surplus_samples)
                if solar_surplus_samples
                else 0.0
            )
            if cost_dkk != float("inf") and (
                min_solar_w is None or avg_solar_surplus_w >= min_solar_w
            ):
                total_kwh = max(energy_wh / 1000.0, 0.001)
                solar_share = solar_kwh / total_kwh
                candidate = {
                    "start": slot_start,
                    "end": slot_start + duration,
                    "solar_kwh": solar_kwh,
                    "grid_kwh": grid_kwh,
                    "cost_dkk": cost_dkk,
                    "solar_share": solar_share,
                    "avg_solar_surplus_w": avg_solar_surplus_w,
                    "reserved_solar_by_hour": reserved_solar_by_hour,
                }
                if best_candidate is None:
                    best_candidate = candidate
                elif preferred_source == "solar":
                    if min_solar_w is not None:
                        if candidate["start"] < best_candidate["start"]:
                            best_candidate = candidate
                    elif (candidate["solar_share"], -candidate["cost_dkk"]) > (
                        best_candidate["solar_share"],
                        -best_candidate["cost_dkk"],
                    ):
                        best_candidate = candidate
                else:
                    if (candidate["cost_dkk"], candidate["start"]) < (
                        best_candidate["cost_dkk"],
                        best_candidate["start"],
                    ):
                        best_candidate = candidate

            slot_start += timedelta(minutes=_STEP_MINUTES)

        if best_candidate is None:
            raise ValueError("No valid flex-load slot found")

        created_at = now.isoformat()
        if job_id in self._reservations:
            created_at = self._reservations[job_id].created_at or created_at
        reservation = FlexLoadReservation(
            job_id=job_id,
            name=name,
            preferred_source=preferred_source,
            duration_minutes=duration_minutes,
            energy_wh=float(energy_wh),
            power_w=float(power_w),
            earliest_start=earliest_start.isoformat(),
            deadline=deadline.isoformat(),
            start_time=best_candidate["start"].isoformat(),
            end_time=best_candidate["end"].isoformat(),
            allow_battery=allow_battery,
            max_grid_w=max_grid_w,
            min_solar_w=min_solar_w,
            expected_solar_kwh=best_candidate["solar_kwh"],
            expected_grid_kwh=best_candidate["grid_kwh"],
            expected_cost_dkk=best_candidate["cost_dkk"],
            reserved_solar_kwh_by_hour=best_candidate["reserved_solar_by_hour"],
            status="booked",
            created_at=created_at,
            updated_at=now.isoformat(),
            source_note=(
                "Solar-first booking"
                if preferred_source == "solar"
                else "Price-first booking"
            ),
        )
        operation = "created" if job_id not in self._reservations else "updated"
        self._reservations[job_id] = reservation
        return {
            "operation": operation,
            "job_id": job_id,
            "name": name,
            "start_time": reservation.start_time,
            "end_time": reservation.end_time,
            "delay_seconds": max(
                0,
                int((_ensure_local(datetime.fromisoformat(reservation.start_time)) - now).total_seconds()),
            ),
            "duration_minutes": duration_minutes,
            "energy_wh": round(float(energy_wh), 1),
            "power_w": round(float(power_w), 1),
            "expected_solar_kwh": round(reservation.expected_solar_kwh, 3),
            "expected_grid_kwh": round(reservation.expected_grid_kwh, 3),
            "expected_cost_dkk": round(reservation.expected_cost_dkk, 3),
            "preferred_source": preferred_source,
            "status": reservation.status,
        }


class NullFlexLoadReservationManager:
    """No-op fallback used by lightweight tests that bypass HA storage."""

    async def async_load(self) -> None:
        return None

    async def async_save(self) -> None:
        return None

    def reserved_solar_kwh_by_hour(self, now: datetime, *, exclude_job_id: str | None = None) -> dict[datetime, float]:
        return {}

    def build_snapshot(self, now: datetime) -> FlexLoadSnapshot:
        return FlexLoadSnapshot()

    def cancel(self, job_id: str) -> bool:
        return False
