"""Constants for the SolarFriend integration."""

DOMAIN = "solarfriend"
MQTT_BASE_TOPIC = "deye/sensor"

CONF_MQTT_TOPIC_PREFIX = "topic_prefix"
DEFAULT_MQTT_TOPIC_PREFIX = "deye/sensor"

# EV charging
CONF_VEHICLE_BATTERY_CAPACITY = "vehicle_battery_capacity_kwh"
CONF_EV_CHARGE_MODE = "ev_charge_mode"
CONF_EV_DEPARTURE_TIME = "ev_departure_time"
CONF_EV_MAX_CHARGE_KW = "ev_max_charge_kw"
EV_CHARGE_MODE_SOLAR = "solar_only"
EV_CHARGE_MODE_HYBRID = "hybrid"
EV_CHARGE_MODE_GRID = "grid_schedule"
