import logging

from .vehicle_controller import VehicleController

_LOGGER = logging.getLogger(__name__)


class KiaController(VehicleController):
    """VehicleController for Kia/Hyundai via kia_uvo integration."""

    def __init__(self, hass, config_entry):
        self._hass = hass
        self._soc_entity = config_entry.data.get("vehicle_soc_entity")
        self._plugged_in_entity = config_entry.data.get("vehicle_plugged_in_entity")
        self._target_soc_entity = config_entry.data.get("vehicle_target_soc_entity")
        self._range_entity = config_entry.data.get("vehicle_range_entity")
        # ALDRIG kald force_update — slider 12V batteri!

    def get_soc(self) -> float:
        state = self._hass.states.get(self._soc_entity)
        try:
            return float(state.state) if state else 0.0
        except (ValueError, TypeError):
            return 0.0

    def is_plugged_in(self) -> bool:
        state = self._hass.states.get(self._plugged_in_entity)
        return state.state in ("on", "true") if state else False

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
