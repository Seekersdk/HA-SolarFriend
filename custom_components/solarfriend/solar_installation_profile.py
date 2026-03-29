"""Solar installation response profile.

Learns the unique geometric response function of a specific solar installation
from clear-sky empirical observations, then projects corrections for any future
date using astronomical solar position calculations.

Two-track architecture:
  Track 1 (ForecastCorrectionModel): All-weather bucket learning, real-time.
  Track 2 (SolarInstallationProfile): Clear-sky response surface, annual projection.

AI bot guide:
- Call update() every coordinator tick (30 s). It accumulates pv_power_w and solar
  position over a 30-min Solcast slot and auto-finalizes at slot boundaries.
- observe() is the internal per-slot finalizer — do not call it directly from the
  coordinator; use update() instead.
- get_factor() returns IDW-interpolated factor for any (elevation, azimuth).
- build_annual_projection() uses astral to sweep 365 days and apply learned surface.
- build_comparison_data() exposes actual / empirical / calculated for graphing.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as ha_dt

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
STORAGE_KEY = "solarfriend_solar_installation_profile"

_MIN_ELEVATION_DEG = 5.0        # Ignore near-horizon slots (cosine effect distorts factor)
_MIN_VALID_KWH = 0.10           # Minimum slot production to be a useful observation
_MAX_CLOUD_PCT = 15.0           # Maximum cloud coverage to count as clear-sky
_MIN_FACTOR = 0.10
_MAX_FACTOR = 2.00
_ALPHA_CAP = 20                 # EMA cap — factor stabilises after ~20 clear-sky observations per cell
_MIN_SAMPLES_CONFIDENT = 5      # Samples needed before a cell is used for projection
_MIN_CELLS_READY = 10           # Confident cells needed before Track 2 activates
_IDW_ELEVATION_WEIGHT = 1.0     # Relative weight of elevation distance in IDW
_IDW_AZIMUTH_WEIGHT = 0.4       # Azimuth matters less — panel tilt affects elevation response more
_BLEND_SIGMA = 25.0             # Distance scale for local confidence in solar-path space
_BLEND_FULL_SAMPLES = 12        # Local sample strength where Track 2 gets full trust
_MIN_FACTOR_CONFIDENCE = 0.55   # Below this, Track 2 withholds output instead of extrapolating


@dataclass
class ResponseCell:
    """Learned correction factor for one (elevation_bucket, azimuth_bucket) cell."""

    factor: float = 1.0
    samples: int = 0
    avg_abs_error_kwh: float = 0.0


@dataclass
class ProfileSnapshot:
    """Diagnostics + comparison data exposed via sensors."""

    state: str = "inactive"             # inactive | learning | ready
    populated_cells: int = 0
    confident_cells: int = 0
    astronomical_coverage_pct: float = 0.0
    annual_paths_total: int = 0
    annual_paths_covered: int = 0
    annual_paths_missing: int = 0
    clear_sky_observations: int = 0
    estimated_hours_to_ready: float = 0.0
    response_surface: dict[str, float] = field(default_factory=dict)
    # Comparison series: actual / empirical (Track 1) / calculated (Track 2)
    comparison_today: list[dict[str, Any]] = field(default_factory=list)
    comparison_tomorrow: list[dict[str, Any]] = field(default_factory=list)


class SolarInstallationProfile:
    """Learn and project the installation's unique solar response function.

    The response surface f(elevation, azimuth) captures all installation-specific
    characteristics: panel orientation, tilt, multi-roof layouts, local shading.
    Under clear-sky conditions these are the dominant production drivers, so
    ~20-30 clear-sky hours across varied sun positions are enough to bootstrap
    a useful model.
    """

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY}.{entry_id}")
        # (elevation_bucket, azimuth_bucket) → ResponseCell
        self._cells: dict[tuple[int, int], ResponseCell] = {}
        self._clear_sky_observations: int = 0
        # Daily cache for the expensive astronomical coverage sweep
        self._coverage_cache_date: str = ""
        self._coverage_cache_value: float = 0.0
        self._coverage_cache_expected_count: int = 0
        self._coverage_cache_covered_count: int = 0
        self._actual_date: str = ""
        self._today_actual_kwh_by_slot: dict[str, float] = {}
        # Slot accumulator — reset at each 30-min Solcast slot boundary
        self._active_slot: datetime | None = None
        self._slot_forecast_kwh: float = 0.0
        self._slot_actual_kwh: float = 0.0
        self._slot_elevation_samples: list[float] = []
        self._slot_azimuth_samples: list[float] = []
        self._slot_cloud_samples: list[float] = []

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def async_load(self) -> None:
        """Load persisted profile from HA storage."""
        try:
            data = await self._store.async_load()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("SolarInstallationProfile load failed; starting fresh: %s", exc)
            return
        if not isinstance(data, dict):
            return
        for key, raw in (data.get("cells", {}) or {}).items():
            try:
                e, a = map(int, key.split("|"))
                self._cells[(e, a)] = ResponseCell(
                    factor=float(raw.get("factor", 1.0)),
                    samples=int(raw.get("samples", 0)),
                    avg_abs_error_kwh=float(raw.get("avg_abs_error_kwh", 0.0)),
                )
            except (ValueError, TypeError, AttributeError):
                continue
        self._clear_sky_observations = int(data.get("clear_sky_observations", 0))
        self._actual_date = str(data.get("actual_date", ""))
        self._today_actual_kwh_by_slot = {
            str(slot): float(value)
            for slot, value in (data.get("today_actual_kwh_by_slot", {}) or {}).items()
        }
        if not self._today_actual_kwh_by_slot:
            for hour, value in (data.get("today_actual_kwh_by_hour", {}) or {}).items():
                try:
                    slot_dt = datetime.fromisoformat(self._actual_date).replace(
                        hour=int(hour),
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    self._today_actual_kwh_by_slot[slot_dt.isoformat()] = float(value)
                except (TypeError, ValueError):
                    continue
        _LOGGER.debug(
            "SolarInstallationProfile loaded: %d celler, %d klarvejrs-observationer",
            len(self._cells),
            self._clear_sky_observations,
        )

    async def async_save(self) -> None:
        """Persist profile to HA storage."""
        await self._store.async_save({
            "cells": {
                f"{e}|{a}": {
                    "factor": round(cell.factor, 4),
                    "samples": cell.samples,
                    "avg_abs_error_kwh": round(cell.avg_abs_error_kwh, 4),
                }
                for (e, a), cell in self._cells.items()
            },
            "clear_sky_observations": self._clear_sky_observations,
            "actual_date": self._actual_date,
            "today_actual_kwh_by_slot": {
                str(slot): round(value, 6)
                for slot, value in self._today_actual_kwh_by_slot.items()
            },
        })

    # ------------------------------------------------------------------
    # Tick-level accumulation
    # ------------------------------------------------------------------

    def update(
        self,
        *,
        now: datetime,
        pv_power_w: float,
        dt_seconds: float,
        elevation_deg: float,
        azimuth_deg: float,
        cloud_coverage_pct: float | None,
        slot_forecast_kwh: float,
    ) -> bool:
        """Called every coordinator tick (30 s).

        Accumulates production and solar position over the current 30-min
        Solcast slot. Finalizes and records an observation when the slot ends.
        Returns True if a new observation was finalized this tick.
        """
        slot_start = _current_slot_start(now)
        local_now = ha_dt.as_local(now) if now.tzinfo is not None else now
        today_str = local_now.date().isoformat()
        if self._actual_date != today_str:
            self._actual_date = today_str
            self._today_actual_kwh_by_slot = {}

        # Slot boundary crossed — finalize previous slot
        if self._active_slot is not None and slot_start != self._active_slot:
            self._finalize_slot()

        # Start new slot
        if self._active_slot is None:
            self._active_slot = slot_start
            self._slot_forecast_kwh = slot_forecast_kwh

        # Accumulate actual production
        if dt_seconds > 0 and pv_power_w >= 0:
            self._slot_actual_kwh += pv_power_w * dt_seconds / 3_600_000

        # Track solar position (only when sun is meaningfully up)
        if elevation_deg >= _MIN_ELEVATION_DEG:
            self._slot_elevation_samples.append(elevation_deg)
            self._slot_azimuth_samples.append(azimuth_deg)

        if cloud_coverage_pct is not None:
            self._slot_cloud_samples.append(cloud_coverage_pct)

        return False

    def _finalize_slot(self) -> None:
        """Finalize accumulated slot data and record observation if eligible."""
        if not self._slot_elevation_samples:
            self._reset_slot()
            return

        avg_elevation = sum(self._slot_elevation_samples) / len(self._slot_elevation_samples)
        avg_azimuth = _circular_mean_azimuth(self._slot_azimuth_samples)
        avg_cloud = (
            sum(self._slot_cloud_samples) / len(self._slot_cloud_samples)
            if self._slot_cloud_samples else None
        )

        accepted = self.observe(
            elevation_deg=avg_elevation,
            azimuth_deg=avg_azimuth,
            cloud_coverage_pct=avg_cloud,
            actual_kwh=self._slot_actual_kwh,
            forecast_kwh=self._slot_forecast_kwh,
        )
        if accepted:
            if self._active_slot is not None:
                slot_key = self._active_slot.replace(tzinfo=None).isoformat()
                self._today_actual_kwh_by_slot[slot_key] = round(self._slot_actual_kwh, 6)
            _LOGGER.debug(
                "SolarInstallationProfile: slot finaliseret e=%.1f° a=%.1f° "
                "faktisk=%.3f kWh forecast=%.3f kWh cloud=%s%%",
                avg_elevation,
                avg_azimuth,
                self._slot_actual_kwh,
                self._slot_forecast_kwh,
                f"{avg_cloud:.0f}" if avg_cloud is not None else "?",
            )
        self._reset_slot()

    def _reset_slot(self) -> None:
        self._active_slot = None
        self._slot_forecast_kwh = 0.0
        self._slot_actual_kwh = 0.0
        self._slot_elevation_samples = []
        self._slot_azimuth_samples = []
        self._slot_cloud_samples = []

    # ------------------------------------------------------------------
    # Learning
    # ------------------------------------------------------------------

    def observe(
        self,
        *,
        elevation_deg: float,
        azimuth_deg: float,
        cloud_coverage_pct: float | None,
        actual_kwh: float,
        forecast_kwh: float,
    ) -> bool:
        """Record one clear-sky slot observation.

        Returns True if the observation was accepted into the model.
        """
        if cloud_coverage_pct is not None and cloud_coverage_pct > _MAX_CLOUD_PCT:
            return False
        if elevation_deg < _MIN_ELEVATION_DEG:
            return False
        if actual_kwh < _MIN_VALID_KWH or forecast_kwh < _MIN_VALID_KWH:
            return False

        e_bucket = _elevation_bucket(elevation_deg)
        a_bucket = _azimuth_bucket(azimuth_deg)
        if e_bucket is None or a_bucket is None:
            return False

        observed_factor = max(_MIN_FACTOR, min(_MAX_FACTOR, actual_kwh / forecast_kwh))
        key = (e_bucket, a_bucket)
        cell = self._cells.get(key)

        if cell is None:
            self._cells[key] = ResponseCell(
                factor=observed_factor,
                samples=1,
                avg_abs_error_kwh=abs(actual_kwh - forecast_kwh),
            )
        else:
            alpha = 1.0 / min(cell.samples + 1, _ALPHA_CAP)
            cell.factor = max(_MIN_FACTOR, min(_MAX_FACTOR,
                cell.factor + (observed_factor - cell.factor) * alpha,
            ))
            cell.avg_abs_error_kwh = (
                (cell.avg_abs_error_kwh * cell.samples) + abs(actual_kwh - forecast_kwh)
            ) / (cell.samples + 1)
            cell.samples += 1

        self._clear_sky_observations += 1
        return True

    # ------------------------------------------------------------------
    # Projection
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        """True when enough confident cells exist for meaningful projection."""
        return self._confident_cell_count >= _MIN_CELLS_READY

    @property
    def _confident_cell_count(self) -> int:
        return sum(1 for c in self._cells.values() if c.samples >= _MIN_SAMPLES_CONFIDENT)

    def get_factor(self, elevation_deg: float, azimuth_deg: float) -> float | None:
        """Return Track-2 correction factor for a solar position.

        Returns None if the model is not ready yet.
        """
        if not self.is_ready:
            return None
        if elevation_deg < _MIN_ELEVATION_DEG:
            return None
        factor, confidence = _idw_interpolate_with_confidence(
            self._cells,
            elevation_deg,
            azimuth_deg,
            min_samples=_MIN_SAMPLES_CONFIDENT,
        )
        return factor if confidence >= _MIN_FACTOR_CONFIDENCE else None

    def build_annual_projection(
        self,
        *,
        latitude: float,
        longitude: float,
        raw_hourly_forecast: list[dict[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Project corrected production for every solar hour over the next 365 days.

        Uses astral for astronomical solar position — no external API required.
        Returns empty list if model is not ready or astral is unavailable.
        """
        if not self.is_ready:
            return []
        try:
            from astral import LocationInfo
            from astral.sun import elevation as calc_elev, azimuth as calc_az
        except ImportError:
            _LOGGER.warning("SolarInstallationProfile: astral bibliotek ikke tilgængeligt")
            return []

        observer = LocationInfo(latitude=latitude, longitude=longitude).observer
        forecast_by_slot = _forecast_lookup(raw_hourly_forecast)
        slot_delta = _forecast_step(raw_hourly_forecast)
        projection: list[dict[str, Any]] = []

        cursor = _align_to_forecast_slot(ha_dt.as_local(now), slot_delta)
        end = cursor + timedelta(days=365)

        while cursor < end:
            elev = calc_elev(observer, dateandtime=cursor)
            if elev >= _MIN_ELEVATION_DEG:
                az = calc_az(observer, dateandtime=cursor)
                factor, confidence = _idw_interpolate_with_confidence(
                    self._cells, elev, az, min_samples=_MIN_SAMPLES_CONFIDENT
                )
                raw_kwh = forecast_by_slot.get(
                    cursor.replace(tzinfo=None).replace(second=0, microsecond=0),
                    0.0,
                )
                projected_kwh = raw_kwh * factor if confidence >= _MIN_FACTOR_CONFIDENCE else None
                projection.append({
                    "period_start": cursor.isoformat(),
                    "elevation": round(elev, 1),
                    "azimuth": round(az, 1),
                    "factor": round(factor, 4) if confidence >= _MIN_FACTOR_CONFIDENCE else None,
                    "factor_confidence": round(confidence, 4),
                    "raw_forecast_kwh": round(raw_kwh, 4),
                    "projected_kwh": round(projected_kwh, 4) if projected_kwh is not None else None,
                })
            cursor += slot_delta

        return projection

    def build_comparison_data(
        self,
        *,
        now: datetime,
        latitude: float,
        longitude: float,
        raw_hourly_forecast: list[dict[str, Any]],
        empirical_hourly_forecast: list[dict[str, Any]],
        actual_hourly_kwh: dict[datetime, float],
    ) -> list[dict[str, Any]]:
        """Build per-slot comparison: faktisk / empirisk (Track 1) / beregnet (Track 2).

        Designed to feed directly into an ApexCharts or similar HA graph card.
        """
        try:
            from astral import LocationInfo
            from astral.sun import elevation as calc_elev, azimuth as calc_az
        except ImportError:
            return []

        observer = LocationInfo(latitude=latitude, longitude=longitude).observer
        raw_by_slot = _forecast_lookup(raw_hourly_forecast)
        empirical_by_slot = _forecast_lookup(empirical_hourly_forecast)
        slot_delta = _forecast_step(raw_hourly_forecast or empirical_hourly_forecast)

        rows: list[dict[str, Any]] = []
        cursor = ha_dt.as_local(now).replace(hour=0, minute=0, second=0, microsecond=0)
        end = cursor + timedelta(days=2)

        while cursor < end:
            elev = calc_elev(observer, dateandtime=cursor)
            slot_key = cursor.replace(tzinfo=None).replace(second=0, microsecond=0)
            raw_kwh = raw_by_slot.get(slot_key, 0.0)
            empirical_kwh = empirical_by_slot.get(slot_key, raw_kwh)
            actual_kwh = actual_hourly_kwh.get(slot_key, None)

            calculated_kwh: float | None = None
            factor_confidence: float | None = None
            if self.is_ready and elev >= _MIN_ELEVATION_DEG and raw_kwh > 0:
                az = calc_az(observer, dateandtime=cursor)
                factor, factor_confidence = _idw_interpolate_with_confidence(
                    self._cells, elev, az, min_samples=_MIN_SAMPLES_CONFIDENT
                )
                if factor_confidence >= _MIN_FACTOR_CONFIDENCE:
                    calculated_kwh = round(raw_kwh * factor, 4)

            rows.append({
                "period_start": cursor.isoformat(),
                "solcast_kwh": round(raw_kwh, 4) if raw_kwh > 0 else None,
                "faktisk_kwh": round(actual_kwh, 4) if actual_kwh is not None else None,
                "empirisk_kwh": round(empirical_kwh, 4) if empirical_kwh > 0 else None,
                "beregnet_kwh": calculated_kwh,
                "beregnet_confidence": round(factor_confidence, 4) if factor_confidence is not None else None,
                "elevation": round(elev, 1),
            })
            cursor += slot_delta

        return rows

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def build_snapshot(
        self,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        now: datetime | None = None,
        raw_hourly_forecast: list[dict[str, Any]] | None = None,
        empirical_hourly_forecast: list[dict[str, Any]] | None = None,
        actual_hourly_kwh: dict[datetime, float] | None = None,
    ) -> ProfileSnapshot:
        """Build diagnostics + comparison snapshot for sensor exposure."""
        confident = self._confident_cell_count

        if confident >= _MIN_CELLS_READY:
            state = "ready"
        elif self._cells:
            state = "learning"
        else:
            state = "inactive"

        # Astronomical coverage: how many expected sun-positions do we have data for?
        coverage_pct = 0.0
        annual_paths_total = 0
        annual_paths_covered = 0
        if latitude is not None and longitude is not None and now is not None:
            coverage_pct = self._compute_astronomical_coverage(latitude, longitude, now)
            annual_paths_total = self._coverage_cache_expected_count
            annual_paths_covered = self._coverage_cache_covered_count
        annual_paths_missing = max(0, annual_paths_total - annual_paths_covered)
        estimated_hours_to_ready = self._estimate_hours_to_ready()

        comparison: list[dict[str, Any]] = []
        if (
            latitude is not None
            and longitude is not None
            and now is not None
            and raw_hourly_forecast is not None
        ):
            comparison = self.build_comparison_data(
                now=now,
                latitude=latitude,
                longitude=longitude,
                raw_hourly_forecast=raw_hourly_forecast,
                empirical_hourly_forecast=empirical_hourly_forecast or [],
                actual_hourly_kwh=actual_hourly_kwh or self._actual_hourly_kwh_for_snapshot(now),
            )

        today = now.date() if now is not None else None
        tomorrow = (now + timedelta(days=1)).date() if now is not None else None

        return ProfileSnapshot(
            state=state,
            populated_cells=len(self._cells),
            confident_cells=confident,
            astronomical_coverage_pct=round(coverage_pct, 1),
            annual_paths_total=annual_paths_total,
            annual_paths_covered=annual_paths_covered,
            annual_paths_missing=annual_paths_missing,
            clear_sky_observations=self._clear_sky_observations,
            estimated_hours_to_ready=round(estimated_hours_to_ready, 1),
            response_surface={
                f"e{e}|a{a}": round(cell.factor, 4)
                for (e, a), cell in sorted(self._cells.items())
                if cell.samples >= _MIN_SAMPLES_CONFIDENT
            },
            comparison_today=[r for r in comparison if today and r["period_start"][:10] == str(today)],
            comparison_tomorrow=[r for r in comparison if tomorrow and r["period_start"][:10] == str(tomorrow)],
        )

    def _actual_hourly_kwh_for_snapshot(self, now: datetime | None) -> dict[datetime, float]:
        """Return naive-local slot datetime -> kWh for today's observed production."""
        if now is None:
            return {}
        local_now = ha_dt.as_local(now) if now.tzinfo is not None else now
        today_str = local_now.date().isoformat()
        if self._actual_date != today_str:
            return {}
        result: dict[datetime, float] = {}
        for slot_str, kwh in self._today_actual_kwh_by_slot.items():
            try:
                slot_dt = datetime.fromisoformat(slot_str)
            except ValueError:
                continue
            result[slot_dt] = kwh
        return result

    def _estimate_hours_to_ready(self) -> float:
        """Estimate clear-sky hours still needed to reach Track-2 ready state.

        The estimate is optimistic but explicit:
        - existing near-ready cells are filled first
        - any remaining missing confident cells are assumed to need full sample count
        """
        confident_cells = [cell for cell in self._cells.values() if cell.samples >= _MIN_SAMPLES_CONFIDENT]
        if len(confident_cells) >= _MIN_CELLS_READY:
            return 0.0

        deficits = sorted(
            _MIN_SAMPLES_CONFIDENT - cell.samples
            for cell in self._cells.values()
            if 0 < cell.samples < _MIN_SAMPLES_CONFIDENT
        )
        confident_count = len(confident_cells)
        missing_slot_samples = 0
        for deficit in deficits:
            if confident_count >= _MIN_CELLS_READY:
                break
            missing_slot_samples += deficit
            confident_count += 1

        if confident_count < _MIN_CELLS_READY:
            missing_slot_samples += (_MIN_CELLS_READY - confident_count) * _MIN_SAMPLES_CONFIDENT

        return missing_slot_samples * 0.5

    def _compute_astronomical_coverage(
        self, latitude: float, longitude: float, now: datetime
    ) -> float:
        """Estimate % of next-365-day solar positions covered by confident cells.

        Result is cached per calendar date — the 8 760-iteration astral sweep
        is only rerun once per day, not every coordinator tick.
        """
        today_str = ha_dt.as_local(now).date().isoformat()
        if today_str == self._coverage_cache_date:
            return self._coverage_cache_value

        try:
            from astral import LocationInfo
            from astral.sun import elevation as calc_elev, azimuth as calc_az
        except ImportError:
            return 0.0

        observer = LocationInfo(latitude=latitude, longitude=longitude).observer
        expected: set[tuple[int, int]] = set()
        cursor = ha_dt.as_local(now).replace(minute=0, second=0, microsecond=0)

        for _ in range(365 * 24):
            elev = calc_elev(observer, dateandtime=cursor)
            if elev >= _MIN_ELEVATION_DEG:
                e = _elevation_bucket(elev)
                a = _azimuth_bucket(calc_az(observer, dateandtime=cursor))
                if e is not None and a is not None:
                    expected.add((e, a))
            cursor += timedelta(hours=1)

        result = 0.0
        covered_count = 0
        if expected:
            confident_keys = {k for k, c in self._cells.items() if c.samples >= _MIN_SAMPLES_CONFIDENT}
            covered_count = len(confident_keys & expected)
            result = covered_count / len(expected) * 100

        self._coverage_cache_date = today_str
        self._coverage_cache_value = result
        self._coverage_cache_expected_count = len(expected)
        self._coverage_cache_covered_count = covered_count
        return result


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _current_slot_start(now: datetime) -> datetime:
    """Return the start of the current 30-min Solcast slot."""
    minute = 0 if now.minute < 30 else 30
    return now.replace(minute=minute, second=0, microsecond=0)


def _circular_mean_azimuth(angles: list[float]) -> float:
    """Circular mean of azimuth angles — handles 350°/10° wrap correctly."""
    if not angles:
        return 0.0
    sin_sum = sum(math.sin(math.radians(a)) for a in angles)
    cos_sum = sum(math.cos(math.radians(a)) for a in angles)
    return math.degrees(math.atan2(sin_sum, cos_sum)) % 360


def _elevation_bucket(value: float) -> int | None:
    if value < -10 or value > 90:
        return None
    return int((max(-10.0, min(90.0, value)) // 10) * 10)


def _azimuth_bucket(value: float) -> int | None:
    return int((value % 360.0 // 30) * 30)


def _idw_interpolate(
    cells: dict[tuple[int, int], ResponseCell],
    elevation_deg: float,
    azimuth_deg: float,
    *,
    min_samples: int,
) -> float:
    """Inverse-distance weighting interpolation over confident response cells."""
    factor, _confidence = _idw_interpolate_with_confidence(
        cells,
        elevation_deg,
        azimuth_deg,
        min_samples=min_samples,
    )
    return factor


def _idw_interpolate_with_confidence(
    cells: dict[tuple[int, int], ResponseCell],
    elevation_deg: float,
    azimuth_deg: float,
    *,
    min_samples: int,
) -> tuple[float, float]:
    """Return raw IDW factor plus confidence for a solar position.

    Confidence falls with distance to the nearest confident cell and rises with
    local sample strength. Callers should suppress output when confidence is low.
    """
    usable = {k: v for k, v in cells.items() if v.samples >= min_samples}
    if not usable:
        return (1.0, 0.0)

    total_weight = 0.0
    weighted_factor = 0.0
    nearest_dist_sq: float | None = None
    nearest_samples = 0

    for (e_bucket, a_bucket), cell in usable.items():
        de = elevation_deg - (e_bucket + 5.0)       # compare to bucket centre
        da = azimuth_deg - (a_bucket + 15.0)
        # Wrap azimuth distance to [-180, 180]
        da = (da + 180) % 360 - 180
        dist_sq = (
            (de * _IDW_ELEVATION_WEIGHT) ** 2
            + (da * _IDW_AZIMUTH_WEIGHT) ** 2
        )
        if nearest_dist_sq is None or dist_sq < nearest_dist_sq:
            nearest_dist_sq = dist_sq
            nearest_samples = cell.samples
        if dist_sq < 0.01:
            sample_confidence = min(1.0, cell.samples / _BLEND_FULL_SAMPLES)
            return (cell.factor, sample_confidence)
        weight = 1.0 / dist_sq
        weighted_factor += cell.factor * weight
        total_weight += weight

    raw_factor = weighted_factor / total_weight if total_weight > 0 else 1.0
    if nearest_dist_sq is None:
        return (1.0, 0.0)

    distance_confidence = math.exp(-nearest_dist_sq / (_BLEND_SIGMA ** 2))
    sample_confidence = min(1.0, nearest_samples / _BLEND_FULL_SAMPLES)
    confidence = max(0.0, min(1.0, distance_confidence * sample_confidence))
    return (raw_factor, confidence)


def _forecast_lookup(
    hourly_forecast: list[dict[str, Any]],
) -> dict[datetime, float]:
    """Build naive-local-datetime → kWh lookup from a forecast list."""
    lookup: dict[datetime, float] = {}
    for entry in hourly_forecast or []:
        ps = entry.get("period_start")
        if ps is None:
            continue
        try:
            if isinstance(ps, str):
                ps = datetime.fromisoformat(ps)
            if ps.tzinfo is not None:
                ps = ha_dt.as_local(ps)
            key = ps.replace(second=0, microsecond=0, tzinfo=None)
            lookup[key] = lookup.get(key, 0.0) + float(entry.get("pv_estimate_kwh", 0.0))
        except (TypeError, ValueError):
            continue
    return lookup


def _forecast_step(hourly_forecast: list[dict[str, Any]]) -> timedelta:
    """Infer forecast slot size from period_start deltas; default to 1 hour."""
    starts: list[datetime] = []
    for entry in hourly_forecast or []:
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


def _align_to_forecast_slot(value: datetime, slot_delta: timedelta) -> datetime:
    """Align datetime to the start of its forecast slot."""
    normalized = value.replace(second=0, microsecond=0)
    slot_minutes = int(slot_delta.total_seconds() // 60)
    if slot_minutes <= 0:
        return normalized
    aligned_minute = (normalized.minute // slot_minutes) * slot_minutes
    return normalized.replace(minute=aligned_minute)
