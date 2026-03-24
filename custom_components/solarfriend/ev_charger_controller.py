import logging
from abc import ABC, abstractmethod
from typing import Any

_LOGGER = logging.getLogger(__name__)


class EVChargerController(ABC):

    @property
    def is_configured(self) -> bool:
        return True

    @abstractmethod
    async def get_status(self) -> str:
        """Returner: disconnected / connected / charging / error"""

    @abstractmethod
    async def get_power_w(self) -> float:
        """Aktuel ladeeffekt i W"""

    @abstractmethod
    async def set_power(self, target_w: float, phases: int) -> None:
        """Sæt ladeeffekt i W fordelt på 1 eller 3 faser"""

    @abstractmethod
    async def pause(self) -> None:
        """Pause ladning"""

    @abstractmethod
    async def resume(self) -> None:
        """Genoptag ladning"""

    @classmethod
    def from_config(cls, hass, config_entry) -> "EVChargerController":
        from .easee_controller import EaseeController
        charger_type = config_entry.data.get("ev_charger_type", "none")
        if charger_type == "manual":
            return ManualEVChargerController(hass, config_entry)
        if charger_type == "easee":
            return EaseeController(hass, config_entry)
        return NullEVChargerController()


class ManualEVChargerController(EVChargerController):
    """Generic charger controller backed by selected HA entities."""

    _STATUS_MAP = {
        "charging": "charging",
        "on": "charging",
        "true": "charging",
        "ready_to_charge": "connected",
        "awaiting_start": "connected",
        "connected": "connected",
        "available": "connected",
        "idle": "connected",
        "completed": "connected",
        "paused": "connected",
        "off": "disconnected",
        "false": "disconnected",
        "disconnected": "disconnected",
        "unplugged": "disconnected",
        "not_connected": "disconnected",
        "error": "error",
        "fault": "error",
    }

    def __init__(self, hass, config_entry) -> None:
        self._hass = hass
        self._status_entity = config_entry.data.get("ev_charger_status_entity")
        self._power_entity = config_entry.data.get("ev_charger_power_entity")
        self._pause_switch = config_entry.data.get("ev_charger_pause_switch")

    @property
    def is_configured(self) -> bool:
        return bool(self._status_entity)

    def _state(self, entity_id: str | None) -> Any:
        return self._hass.states.get(entity_id) if entity_id else None

    async def get_status(self) -> str:
        state = self._state(self._status_entity)
        if state is None or state.state in ("unknown", "unavailable", ""):
            return "error"

        normalized = self._STATUS_MAP.get(str(state.state).lower())
        if normalized is not None:
            if normalized == "connected":
                power_w = await self.get_power_w()
                if power_w > 50:
                    return "charging"
            return normalized

        power_w = await self.get_power_w()
        return "charging" if power_w > 50 else "connected"

    async def get_power_w(self) -> float:
        state = self._state(self._power_entity)
        if state is None:
            return 0.0
        try:
            return float(state.state)
        except (ValueError, TypeError):
            return 0.0

    async def set_power(self, target_w: float, phases: int) -> None:
        _LOGGER.debug(
            "Manual charger: requested set_power %.0fW %d-phase — no writable entity configured",
            target_w,
            phases,
        )

    async def pause(self) -> None:
        if not self._pause_switch:
            _LOGGER.debug("Manual charger: pause requested without pause switch")
            return
        await self._hass.services.async_call(
            "switch",
            "turn_on",
            {"entity_id": self._pause_switch},
            blocking=True,
        )

    async def resume(self) -> None:
        if not self._pause_switch:
            _LOGGER.debug("Manual charger: resume requested without pause switch")
            return
        await self._hass.services.async_call(
            "switch",
            "turn_off",
            {"entity_id": self._pause_switch},
            blocking=True,
        )


class NullEVChargerController(EVChargerController):
    """Bruges når EV-opladning ikke er aktiveret"""

    @property
    def is_configured(self) -> bool:
        return False

    async def get_status(self) -> str:
        return "disconnected"

    async def get_power_w(self) -> float:
        return 0.0

    async def set_power(self, target_w: float, phases: int) -> None:
        pass

    async def pause(self) -> None:
        pass

    async def resume(self) -> None:
        pass
