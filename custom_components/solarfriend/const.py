"""Constants for the SolarFriend integration."""

DOMAIN = "solarfriend"
MQTT_BASE_TOPIC = "deye/sensor"
SERVICE_POPULATE_LOAD_MODEL = "populate_load_model"
SERVICE_BOOK_FLEX_LOAD = "book_flex_load"
SERVICE_CANCEL_FLEX_LOAD = "cancel_flex_load"
CONF_BUY_PRICE_SENSOR = "buy_price_sensor"
CONF_SELL_PRICE_SENSOR = "sell_price_sensor"
CONF_WEATHER_ENTITY = "weather_entity"
CONF_EV_SOLAR_ONLY_GRID_BUFFER_ENABLED = "ev_solar_only_grid_buffer_enabled"
CONF_BATTERY_SELL_ENABLED = "battery_sell_enabled"
CONF_ADVANCED_CONSUMPTION_MODEL_ENABLED = "advanced_consumption_model_enabled"

CONF_MQTT_TOPIC_PREFIX = "topic_prefix"
DEFAULT_MQTT_TOPIC_PREFIX = "deye/sensor"

# Inverter control
CONF_SOLAR_SELL_ENTITY = "solar_sell_entity"

# EV charging
CONF_VEHICLE_BATTERY_CAPACITY = "vehicle_battery_capacity_kwh"
CONF_EV_CHARGE_MODE = "ev_charge_mode"
CONF_EV_DEPARTURE_TIME = "ev_departure_time"
CONF_EV_MAX_CHARGE_KW = "ev_max_charge_kw"
EV_CHARGE_MODE_SOLAR = "solar_only"
EV_CHARGE_MODE_HYBRID = "hybrid"
EV_CHARGE_MODE_GRID = "grid_schedule"
