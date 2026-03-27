import logging

from homeassistant.helpers import entity_registry as er
from homeassistant.exceptions import ServiceNotFound

from .ev_charger_controller import EVChargerController

_LOGGER = logging.getLogger(__name__)

VOLTAGE = 235.0
MIN_AMPS = 6.0
MAX_AMPS = 16.0

EASEE_STATUS_MAP = {
    "disconnected":    "disconnected",
    "awaiting_start":  "connected",
    "ready_to_charge": "connected",
    "charging":        "charging",
    "completed":       "connected",
    "error":           "error",
}


class EaseeController(EVChargerController):

    def __init__(self, hass, config_entry):
        self._hass = hass
        self._status_entity = config_entry.data.get("ev_charger_status_entity")
        self._power_entity = config_entry.data.get("ev_charger_power_entity")
        self._charger_id = config_entry.data.get("ev_charger_id")
        self._entity_registry = er.async_get(hass)

    def _get_device_id(self) -> str | None:
        """Resolve the HA device_id for the configured Easee charger."""
        if not self._status_entity:
            return None
        entry = self._entity_registry.async_get(self._status_entity)
        return entry.device_id if entry else None

    async def get_status(self) -> str:
        state = self._hass.states.get(self._status_entity)
        if not state or state.state in ("unknown", "unavailable"):
            return "error"
        return EASEE_STATUS_MAP.get(state.state, "error")

    async def get_power_w(self) -> float:
        if not self._power_entity:
            return 0.0
        state = self._hass.states.get(self._power_entity)
        try:
            if not state:
                return 0.0
            value = float(state.state)
            unit = str(state.attributes.get("unit_of_measurement", "")).lower()
            if unit == "kw":
                return value * 1000.0
            return value
        except (ValueError, TypeError):
            return 0.0

    async def set_power(self, target_w: float, phases: int) -> None:
        device_id = self._get_device_id()
        if not device_id:
            _LOGGER.warning("Easee set_power fejl: ingen device_id fundet for %s", self._status_entity)
            return
        if phases == 3:
            amps = max(MIN_AMPS, min(MAX_AMPS, round(target_w / 3 / VOLTAGE, 1)))
            service_data = {
                "device_id": device_id,
                "current_p1": amps,
                "current_p2": amps,
                "current_p3": amps,
            }
        else:  # 1-fase
            amps = max(MIN_AMPS, min(MAX_AMPS, round(target_w / VOLTAGE, 1)))
            service_data = {
                "device_id": device_id,
                "current_p1": amps,
                "current_p2": 0,
                "current_p3": 0,
            }
        try:
            await self._hass.services.async_call(
                "easee", "set_circuit_dynamic_limit",
                service_data,
                blocking=False,
            )
            _LOGGER.info(
                "Easee: %d-fase %.1fA (%.0fW) device_id=%s",
                phases, amps, target_w, device_id,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee set_power fejl: %s", e)

    async def pause(self) -> None:
        device_id = self._get_device_id()
        if not device_id:
            _LOGGER.warning("Easee pause fejl: ingen device_id fundet for %s", self._status_entity)
            return
        _LOGGER.debug("Easee: action_command pause (device_id=%s)", device_id)
        try:
            await self._hass.services.async_call(
                "easee", "action_command",
                {"device_id": device_id, "action_command": "pause"},
                blocking=False,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee pause fejl: %s", e)

    async def resume(self) -> None:
        device_id = self._get_device_id()
        if not device_id:
            _LOGGER.warning("Easee resume fejl: ingen device_id fundet for %s", self._status_entity)
            return
        _LOGGER.debug("Easee: action_command resume (device_id=%s)", device_id)
        try:
            await self._hass.services.async_call(
                "easee", "action_command",
                {"device_id": device_id, "action_command": "resume"},
                blocking=False,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee resume fejl: %s", e)
