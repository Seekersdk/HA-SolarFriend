"""InverterController — abstract base and factory for inverter strategy controllers."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.config_entries import ConfigEntry
    from .battery_optimizer import OptimizeResult


class InverterController(ABC):
    """Abstract base — all inverter types inherit from this."""

    @property
    @abstractmethod
    def is_configured(self) -> bool:
        """True if enough entities are configured to send commands."""
        ...

    @abstractmethod
    async def apply(self, result: OptimizeResult) -> None:
        """Apply optimizer strategy to the inverter."""
        ...

    @staticmethod
    def from_config(hass: HomeAssistant, config_entry: ConfigEntry) -> InverterController:
        """Factory — return the correct controller based on inverter_type."""
        from .deye_controller import DeyeController

        inverter_type = config_entry.data.get("inverter_type", "deye_klatremis")

        controllers = {
            "deye_klatremis": DeyeController,
        }

        cls = controllers.get(inverter_type, DeyeController)
        return cls(hass, config_entry)
