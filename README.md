# SolarFriend

SolarFriend is a Home Assistant integration for energy-aware battery and EV charging control.

It combines live power data, spot prices, solar forecasts, and a learned household consumption profile to make short-horizon decisions inside Home Assistant. The integration is designed to keep the current state actionable, explainable, and visible in dashboards.

## Currently supported setup

SolarFriend currently supports the following control stack:

- Deye inverter control via the Klatremis-based Deye setup
- Kia vehicle integration for EV state and target data
- Easee charger integration for EV charging control

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

### Flexible load booking

SolarFriend can also reserve future energy slots for appliances without directly controlling the appliance itself.

This is intended for Home Assistant automations where:

- an automation decides that a job should run
- SolarFriend calculates the best slot
- the automation uses the returned time to set a timer, delay, or appliance start option

Examples:

- dishwasher
- washing machine
- dryer
- water heater
- any other flexible appliance with a known runtime and approximate energy need

The booking system is exposed as Home Assistant services:

- `solarfriend.book_flex_load`
- `solarfriend.cancel_flex_load`

Important behavior:

- `job_id` is mandatory and idempotent
- reusing the same `job_id` updates/replaces the existing active reservation
- SolarFriend does not send the actual start command in v1
- the returned slot is meant to be consumed by your HA automation

Typical request fields:

- `job_id`
- `name`
- `duration_minutes`
- `deadline`
- `earliest_start` optional
- `energy_wh` or `power_w`
- `preferred_source`
  - `cheap`
  - `solar`
- `min_solar_w` optional
- `max_grid_w` optional
- `allow_battery` currently stored for future policy use

Typical response fields:

- `operation`
  - `created`
  - `updated`
- `start_time`
- `end_time`
- `delay_seconds`
- `expected_solar_kwh`
- `expected_grid_kwh`
- `expected_cost_dkk`

Recommended user flow:

1. Your automation is triggered by a user action or device state.
2. The automation calls `solarfriend.book_flex_load`.
3. SolarFriend returns the planned slot.
4. Your automation uses the returned `start_time` or `delay_seconds` to configure the appliance.

Example service call:

```yaml
action: solarfriend.book_flex_load
data:
  job_id: dishwasher_tonight
  name: Dishwasher tonight
  duration_minutes: 150
  deadline: "2026-03-28T06:00:00+01:00"
  earliest_start: "2026-03-27T22:00:00+01:00"
  energy_wh: 2000
  preferred_source: solar
  min_solar_w: 2000
  max_grid_w: 300
response_variable: booking
```

Example follow-up in the same automation:

```yaml
- action: input_datetime.set_datetime
  target:
    entity_id: input_datetime.dishwasher_reserved_start
  data:
    datetime: "{{ booking.start_time }}"
```

If the same automation runs again with the same `job_id`, SolarFriend updates the reservation instead of creating duplicates.

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
