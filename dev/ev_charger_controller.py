from abc import ABC, abstractmethod
import logging

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
        if charger_type == "easee":
            return EaseeController(hass, config_entry)
        return NullEVChargerController()


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
