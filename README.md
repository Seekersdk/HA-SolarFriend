# SolarFriend

SolarFriend is a Home Assistant integration for energy-aware battery and EV charging control.

It combines live power data, spot prices, solar forecasts, and a learned household consumption profile to make short-horizon decisions inside Home Assistant. The integration is designed to keep the current state actionable, explainable, and visible in dashboards.

## What the integration does

### Home battery optimization

SolarFriend plans the home battery across the full known price horizon instead of splitting decisions into simple day/night rules.

It can:

- charge from grid in cheap hours when that improves later savings
- save battery energy for higher-value hours later in the horizon
- avoid unnecessary discharge in low-value hours
- account for expected solar production and household demand
- respond to negative or zero-price conditions
- re-optimize when the battery does not follow the currently planned slot

The battery planner uses:

- current battery state of charge
- live battery, grid, PV, and load power
- hourly spot prices
- hourly solar forecast
- learned hourly household demand
- weighted battery energy cost from tracked battery history

### Battery value and savings tracking

SolarFriend keeps track of what is stored in the battery and where that energy came from.

It tracks:

- solar-charged energy in the battery
- grid-charged energy in the battery
- weighted battery energy cost
- daily and total solar savings
- daily and total optimizer savings

The total savings sensors update live and persist across restarts.

### Learned household consumption profile

The integration builds a household demand model from measured load data.

It includes:

- hourly weekday/weekend buckets
- live learning from current measurements
- historical seeding from recorder history
- force-populate support from existing load history
- confidence reporting for the learned model

This profile is used by the battery optimizer and by EV planning.

### Forecast quality tracking

SolarFriend tracks forecast quality over time and exposes diagnostics for:

- today versus predicted production
- yesterday versus predicted production
- rolling 14-day accuracy
- rolling 14-day bias
- rolling forecast error metrics

It also includes a passive month/hour forecast correction model that learns how forecast output differs from actual production throughout the year. This model is currently diagnostics-only and does not modify live control behavior.

### EV charging optimization

SolarFriend also supports optional EV charging control.

It includes three charging modes:

- `solar_only`: use real-time surplus solar only
- `hybrid`: combine expected solar with the cheapest grid hours before departure
- `grid_schedule`: charge in the cheapest slots before departure

The EV planner uses slot-based logic and can consider:

- charger power limits
- solar surplus available to the car
- planned home battery charging
- departure time
- target state of charge
- minimum range

When both the EV and the home battery want the same solar energy, SolarFriend can prioritize one over the other based on timing and economic value.

## Main entities and diagnostics

The integration exposes live state, planning data, and diagnostics for use in dashboards and automations.

Examples include:

- current optimizer strategy
- battery plan preview
- EV charging strategy and reason
- learned consumption profile chart
- forecast SOC chart
- solar next 2 hours
- solar until sunset
- daily and total savings
- forecast quality metrics
- forecast correction model status

## Logging and analysis

SolarFriend includes structured shadow logging for replay and analysis of optimizer behavior.

It also includes a dedicated forecast correction dashboard file:

- `ForecastCorrectionBoard.yaml`

This is intended as a diagnostic/debug dashboard for monitoring the passive forecast correction model.

## Installation

### HACS

1. Add this repository as a custom repository in HACS.
2. Install `SolarFriend`.
3. Restart Home Assistant.
4. Add the `SolarFriend` integration from Settings → Devices & Services.

### Manual

Copy `custom_components/solarfriend/` into your Home Assistant `custom_components/` directory and restart Home Assistant.

## Configuration

The integration is configured through the Home Assistant UI.

Typical inputs include:

- PV power sensor
- grid power sensor
- battery state of charge sensor
- battery power sensor
- household load sensor
- spot price sensor
- solar forecast source

If EV control is enabled, charger and vehicle entities are configured through the same integration flow.

## License

MIT
