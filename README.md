# SolarFriend WIP🌞

This project is under development. There will be bugs and is not meant for production

Smart solar battery optimization for Home Assistant.
Automatically manages battery charging and discharging based on
spot prices, solar forecasts and your consumption patterns.

## Features

- **Spot price optimization** — charges battery when electricity
  is cheap, uses battery when it's expensive
- **Solcast integration** — uses solar forecasts to plan ahead
- **Consumption profiling** — learns your usage patterns over 28 days
- **Pre-emptive discharge** — empties battery before solar peaks
  so you maximize free solar energy
- **Deye/klatremis support** — controls Deye inverters directly

## Strategies

| Strategy | When | Action |
|----------|------|--------|
| `CHARGE_NIGHT` | Night, low price | Charge battery at cheapest hour |
| `SAVE_SOLAR` | Day, solar incoming | Reserve capacity for solar |
| `USE_BATTERY` | Day, high price | Use battery instead of grid |
| `SELL_BATTERY` | Day, high price + solar coming | Sell battery, recharge with solar |
| `CHARGE_GRID` | Any, very cheap price | Opportunistic grid charge |
| `IDLE` | Default | No intervention |

## Requirements

- Home Assistant 2024.1+
- Solcast PV Forecast (recommended) or Forecast.Solar
- Energi Data Service or similar spot price sensor
- Deye inverter with klatremis YAML (for inverter control)

## Installation

### HACS (recommended)
1. Add this repo as a custom repository in HACS
2. Install "SolarFriend"
3. Restart Home Assistant
4. Settings → Integrations → Add Integration → SolarFriend

### Manual
Copy `custom_components/solarfriend/` to your HA
`config/custom_components/` and restart.

## Configuration

Setup via UI — you will need:
1. Inverter sensors (PV, grid, battery SOC, battery power, load)
2. Battery settings (capacity kWh, min/max SOC, cost per kWh)
3. Spot price sensor
4. Solcast or Forecast.Solar sensor
5. Deye klatremis control entities (optional)

## Key Sensors

| Sensor | Description |
|--------|-------------|
| `sensor.solarfriend_optimizer_strategy` | Current strategy |
| `sensor.solarfriend_battery_soc` | Battery state of charge |
| `sensor.solarfriend_forecast_today` | Solar forecast today (kWh) |
| `sensor.solarfriend_solar_next_2_hours` | Solar forecast next 2h |
| `sensor.solarfriend_sparet_pa_sol_i_dag` | Money saved by solar today |
| `sensor.solarfriend_sparet_via_optimering_i_dag` | Money saved by optimization today |
| `sensor.solarfriend_consumption_profile_chart` | 24h consumption profile |
| `sensor.solarfriend_forecast_soc_chart` | Predicted SOC for rest of day |

## Supported Inverters

| Inverter | Status |
|----------|--------|
| Deye SUN-12K (via klatremis) | ✅ Supported |
| Huawei | 🔜 Planned |
| Solarman | 🔜 Planned |

## License

MIT
