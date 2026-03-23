from abc import ABC, abstractmethod
import logging

_LOGGER = logging.getLogger(__name__)


class VehicleController(ABC):

    @property
    def is_configured(self) -> bool:
        return True

    @abstractmethod
    def get_soc(self) -> float:
        """Batteri SOC 0-100%"""

    @abstractmethod
    def is_plugged_in(self) -> bool:
        """Er bilen tilsluttet laderen?"""

    def get_target_soc(self) -> float:
        """Mål SOC — default fra config eller 80%"""
        return getattr(self, "_target_soc", 80.0)

    def get_driving_range(self) -> float | None:
        """Resterende rækkevidde i km — None hvis sensor ikke er konfigureret."""
        return None

    @classmethod
    def from_config(cls, hass, config_entry) -> "VehicleController":
        from .kia_controller import KiaController
        vehicle_type = config_entry.data.get("vehicle_type", "none")
        if vehicle_type == "kia_hyundai":
            return KiaController(hass, config_entry)
        if vehicle_type == "manual":
            return ManualVehicleController(hass, config_entry)
        return NullVehicleController()


class NullVehicleController(VehicleController):
    """Bruges når ingen bil-integration er valgt"""

    @property
    def is_configured(self) -> bool:
        return False

    def get_soc(self) -> float:
        return 0.0

    def is_plugged_in(self) -> bool:
        # Ingen bil-integration → antag altid tilsluttet
        # Laderen styrer selv via status-sensoren
        return True

    def get_target_soc(self) -> float:
        return 80.0


class ManualVehicleController(VehicleController):
    """Generisk controller med manuelle entity-valg"""

    def __init__(self, hass, config_entry):
        self._hass = hass
        self._soc_entity = config_entry.data.get("vehicle_soc_entity")
        self._plugged_in_entity = config_entry.data.get("vehicle_plugged_in_entity")
        self._target_soc_entity = config_entry.data.get("vehicle_target_soc_entity")
        self._range_entity = config_entry.data.get("vehicle_range_entity")

    def get_soc(self) -> float:
        if not self._soc_entity:
            return 0.0
        state = self._hass.states.get(self._soc_entity)
        try:
            return float(state.state) if state else 0.0
        except (ValueError, TypeError):
            return 0.0

    def is_plugged_in(self) -> bool:
        if not self._plugged_in_entity:
            return True
        state = self._hass.states.get(self._plugged_in_entity)
        return state.state in ("on", "true", "charging") if state else False

    def get_target_soc(self) -> float:
        if self._target_soc_entity:
            state = self._hass.states.get(self._target_soc_entity)
            try:
                val = float(state.state) if state else None
                if val is not None and 50 <= val <= 100:
                    return val
            except (ValueError, TypeError):
                pass
        return 80.0

    def get_driving_range(self) -> float | None:
        if not self._range_entity:
            return None
        state = self._hass.states.get(self._range_entity)
        try:
            return float(state.state) if state else None
        except (ValueError, TypeError):
            return None
