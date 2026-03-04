# Generator — Usage Guide

This directory contains the Python event generator for the food-delivery streaming pipeline.

## Files

| File | Description |
|------|-------------|
| `main.py` | CLI entry point — run this to generate data |
| `config.py` | All tuneable parameters, zone definitions, menu data |
| `order_generator.py` | `OrderEventGenerator` — produces Feed 1 (order lifecycle) |
| `courier_generator.py` | `CourierFleetGenerator` + `CourierSimulator` — produces Feed 2 |
| `avro_writer.py` | Pure-stdlib AVRO OCF binary writer (no third-party deps) |

## Architecture

```
main.py
  ├── loads schemas from ../schemas/
  ├── OrderEventGenerator (order_generator.py)
  │     ├── builds restaurant & customer reference pools
  │     ├── for each order: generate_order_lifecycle()
  │     │     ├── sample items, promo, delivery fee
  │     │     ├── decide: cancel? skip step? anomalous duration?
  │     │     ├── emit one event per status transition
  │     │     └── inject duplicates and late arrivals
  │     └── stream(): sort all events by ingestion_timestamp
  │
  ├── CourierFleetGenerator (courier_generator.py)
  │     ├── one CourierSimulator per courier_id
  │     ├── CourierSimulator.generate_shift()
  │     │     ├── GPS pings every ~15s when active
  │     │     ├── complete IDLE→HEADING→WAITING→PICKED_UP→EN_ROUTE→DELIVERED cycle
  │     │     └── inject: offline-mid-delivery, location jumps, impossible speeds
  │     └── merge all courier event lists, sort by ingestion_timestamp
  │
  └── writes JSON Lines + AVRO OCF for each feed
```

## CLI Reference

```
usage: main.py [-h] [--orders N] [--couriers N] [--restaurants N]
               [--customers N] [--duration SECONDS] [--seed N]
               [--output-dir PATH] [--late-rate FLOAT]
               [--duplicate-rate FLOAT] [--cancel-rate FLOAT]
               [--anomaly-rate FLOAT] [--surge]
               [--surge-zone ZONE_ID] [--surge-multiplier FLOAT]

arguments:
  --orders N            Number of orders to simulate (default: 100)
  --couriers N          Courier fleet size (default: 80)
  --restaurants N       Restaurant pool size (default: 50)
  --customers N         Customer pool size (default: 500)
  --duration SECONDS    Simulation time window in seconds (default: 3600)
  --seed N              Random seed for reproducibility (default: 42)
  --output-dir PATH     Where to write output files (default: ./sample_data)

Edge case rates:
  --late-rate FLOAT     Fraction of events with late arrival (default: 0.05)
  --duplicate-rate FLOAT  Fraction of events duplicated (default: 0.02)
  --cancel-rate FLOAT   Order cancellation probability (default: 0.12)
  --anomaly-rate FLOAT  Fraction with impossible durations (default: 0.02)

Demand surge:
  --surge               Enable zone-level demand surge
  --surge-zone ZONE_ID  Zone to surge (default: zone_downtown)
  --surge-multiplier N  Demand multiplier during surge (default: 3.0)
```

## Available Zones

| Zone ID | Label | Demand Weight | Courier Density |
|---------|-------|:---:|:---:|
| `zone_downtown` | Downtown | 3.5 | 3.0 |
| `zone_midtown` | Midtown | 2.5 | 2.0 |
| `zone_uptown` | Uptown | 1.5 | 1.2 |
| `zone_east_side` | East Side | 1.8 | 1.5 |
| `zone_brooklyn` | Brooklyn | 2.0 | 1.8 |
| `zone_suburbs` | Suburbs | 0.8 | 0.5 |

Zones are assigned with probability proportional to their weight, producing the downtown skew typical of real delivery platforms.

## Demand Time-of-Day Model

The generator uses an hourly demand curve to model lunch/dinner peaks:

```
Demand weight
  2.2 │                          ████
  2.0 │                     ████████████
  1.8 │           █    ████████████████
  1.5 │           ███  ████████████████████
  1.2 │                ████████████████████████
  0.5 │  ██                                     ████
  0.1 │████                                         ████
      └─────────────────────────────────────────────────▶ Hour (UTC)
        0  2  4  6  8  10 12 14 16 18 20 22
```

Weekend orders are boosted by a **1.25× multiplier** vs. weekdays.

## Realism Notes

- **Restaurant prep times** are sampled from a normal distribution centred on each restaurant's `avg_prep_time_seconds` with configurable standard deviation (60–300s).
- **Courier GPS** positions move realistically towards their target using great-circle geometry, at speeds appropriate to the courier's vehicle type.
- **Order items** are sampled from cuisine-appropriate menus with realistic quantity and price distributions.
- **Promos** are applied with configurable probability; discount is deducted from `order_total_cents`.
- **Courier earnings** accumulate per session and are reset when the courier goes offline.

## Extending the Generator

To add a new field to a feed:

1. Add the field to the AVRO schema in `../schemas/`
2. Add a `"default"` value in the schema for backward compatibility
3. Populate the field in the relevant `_make_event()` or `make_base_event()` method
4. Bump `schema_version` to `"1.1.0"`

To add a new zone:

```python
# In config.py → ZONES dict:
"zone_airport": {
    "lat_range": (40.6350, 40.6450),
    "lon_range": (-73.7850, -73.7750),
    "demand_weight": 1.2,
    "courier_density": 0.8,
    "label": "Airport",
},
```
