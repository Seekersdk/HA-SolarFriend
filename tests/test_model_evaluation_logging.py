"""Unit tests for compact model evaluation logging."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import types
from datetime import datetime, timezone


def _mock(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


if "homeassistant" not in sys.modules:
    _mock("homeassistant")
    _mock("homeassistant.core", HomeAssistant=type("HomeAssistant", (), {}))
    _mock("homeassistant.helpers")
    _mock("homeassistant.util")
    _mock(
        "homeassistant.util.dt",
        as_local=lambda dt: dt,
        now=lambda: datetime(2026, 3, 29, 12, 30, 0),
        UTC=timezone.utc,
    )


class _FakeStore:
    def __init__(self):
        self._data: dict | None = None

    async def async_load(self):
        return self._data

    async def async_save(self, data: dict):
        import copy

        self._data = copy.deepcopy(data)


_store = _FakeStore()
_mock("homeassistant.helpers.storage", Store=lambda hass, version, key: _store)

module_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "custom_components",
    "solarfriend",
    "model_evaluation_logging.py",
)
spec = importlib.util.spec_from_file_location("test_model_evaluation_logging_module", module_path)
assert spec is not None and spec.loader is not None
_mel = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = _mel
spec.loader.exec_module(_mel)
_mel.Store = lambda hass, version, key: _store
ModelEvaluationLogger = _mel.ModelEvaluationLogger
summarize_evaluation_log = _mel.summarize_evaluation_log


def run(coro):
    return asyncio.run(coro)


def test_model_evaluation_logger_appends_one_row_per_slot():
    _store._data = None
    path = os.path.join(os.path.dirname(__file__), "_tmp_model_evaluation.jsonl")
    if os.path.exists(path):
        os.remove(path)
    try:
        logger = ModelEvaluationLogger(object(), entry_id="entry-1", log_path=path)

        run(logger.async_load())
        run(
            logger.append_slot(
                slot_start=datetime(2026, 3, 29, 12, 0),
                slot_minutes=30,
                actual_kwh=1.84,
                solcast_kwh=1.66,
                empirical_kwh=1.79,
                solar_elevation=41.2,
                solar_azimuth=163.8,
                cloud_coverage_pct=4.0,
                temperature_c=13.7,
                track2_rows={
                    "fast": {"kwh": 1.81, "confidence": 0.92},
                    "medium": {"kwh": 1.76, "confidence": 0.71},
                    "fine": {"kwh": None, "confidence": 0.18},
                },
            )
        )
        run(
            logger.append_slot(
                slot_start=datetime(2026, 3, 29, 12, 0),
                slot_minutes=30,
                actual_kwh=1.84,
                solcast_kwh=1.66,
                empirical_kwh=1.79,
                solar_elevation=41.2,
                solar_azimuth=163.8,
                cloud_coverage_pct=4.0,
                temperature_c=13.7,
                track2_rows={"fast": {"kwh": 1.81, "confidence": 0.92}},
            )
        )

        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
    finally:
        if os.path.exists(path):
            os.remove(path)

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["period_start"] == "2026-03-29T12:00:00"
    assert payload["period_minutes"] == 30
    assert payload["actual_kwh"] == 1.84
    assert payload["solcast_kwh"] == 1.66
    assert payload["empirisk_kwh"] == 1.79
    assert payload["beregnet_fast_kwh"] == 1.81
    assert payload["beregnet_medium_confidence"] == 0.71
    assert payload["beregnet_fine_kwh"] is None


def test_summarize_evaluation_log_returns_monthly_metrics():
    path = os.path.join(os.path.dirname(__file__), "_tmp_model_evaluation_summary.jsonl")
    if os.path.exists(path):
        os.remove(path)
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "period_start": "2026-03-29T12:00:00",
                        "actual_kwh": 2.0,
                        "solcast_kwh": 1.6,
                        "empirisk_kwh": 1.9,
                        "beregnet_fast_kwh": 2.1,
                        "beregnet_medium_kwh": 1.8,
                        "beregnet_fine_kwh": None,
                    }
                )
                + "\n"
            )
            fh.write(
                json.dumps(
                    {
                        "period_start": "2026-03-30T12:00:00",
                        "actual_kwh": 1.0,
                        "solcast_kwh": 0.8,
                        "empirisk_kwh": 1.05,
                        "beregnet_fast_kwh": 1.2,
                        "beregnet_medium_kwh": 0.95,
                        "beregnet_fine_kwh": 1.4,
                    }
                )
                + "\n"
            )

        summary = summarize_evaluation_log(path, month_key="2026-03")
    finally:
        if os.path.exists(path):
            os.remove(path)

    assert summary.period_month == "2026-03"
    assert summary.rows == 2
    assert summary.best_model == "empirisk"
    assert summary.mae_by_model["empirisk"] == 0.075
    assert summary.mape_by_model["solcast"] == 20.0
    assert summary.bias_by_model["beregnet_fast"] == 0.15
