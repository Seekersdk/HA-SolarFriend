import logging

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
            return float(state.state) if state else 0.0
        except (ValueError, TypeError):
            return 0.0

    async def set_power(self, target_w: float, phases: int) -> None:
        if phases == 3:
            amps = max(MIN_AMPS, min(MAX_AMPS, round(target_w / 3 / VOLTAGE, 1)))
            service_data = {
                "charger_id": self._charger_id,
                "current_p1": amps,
                "current_p2": amps,
                "current_p3": amps,
            }
        else:  # 1-fase
            amps = max(MIN_AMPS, min(MAX_AMPS, round(target_w / VOLTAGE, 1)))
            service_data = {
                "charger_id": self._charger_id,
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
                "Easee: %d-fase %.1fA (%.0fW) charger_id=%s",
                phases, amps, target_w, self._charger_id,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee set_power fejl: %s", e)

    async def pause(self) -> None:
        _LOGGER.debug("Easee: pause_charger (charger_id=%s)", self._charger_id)
        try:
            await self._hass.services.async_call(
                "easee", "pause_charger",
                {"charger_id": self._charger_id},
                blocking=False,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee pause fejl: %s", e)

    async def resume(self) -> None:
        _LOGGER.debug("Easee: resume_charger (charger_id=%s)", self._charger_id)
        try:
            await self._hass.services.async_call(
                "easee", "resume_charger",
                {"charger_id": self._charger_id},
                blocking=False,
            )
        except ServiceNotFound:
            _LOGGER.warning(
                "Easee service ikke fundet — er Easee integrationen installeret?"
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Easee resume fejl: %s", e)
