# 🛵 Real-Time Food Delivery Analytics Pipeline

> **Milestone 1 — Streaming Data Feed Design & Generation**  
> Course: Big Data & Streaming Systems

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Team Structure](#team-structure)
3. [Repository Layout](#repository-layout)
4. [Feed Design — Rationale & Analytics Intent](#feed-design)
5. [Schema Overview](#schema-overview)
6. [Edge Cases & Streaming Correctness](#edge-cases)
7. [Quick Start](#quick-start)
8. [Planned Analytics (Milestones 2–4)](#planned-analytics)

---

## Project Overview

This project designs and implements a **real-time analytics pipeline** for a synthetic food-delivery platform modelled after systems like Uber Eats, Glovo, and Deliveroo.

The platform is a **three-sided marketplace**:

```
Customer ──places order──▶ Platform ──dispatches──▶ Courier ──delivers──▶ Customer
                               │
                               ▼
                          Restaurant
                      (accepts & prepares)
```

Every interaction produces **streaming events**. This repository implements:

- **Two streaming data feeds** covering the order lifecycle and courier fleet
- **AVRO schemas** with full type safety and schema versioning support
- **A configurable Python generator** that produces realistic, time-stamped events
- **Sample datasets** in both JSON Lines and AVRO binary format

---

## Team Structure

| Role | Responsibility |
|------|----------------|
| Feed Architect | Schema design, AVRO versioning strategy |
| Generator Engineer | Python generator, edge-case injection |
| Data Quality Lead | Watermark testing, anomaly labelling |
| Pipeline Engineer (x2) | Spark Structured Streaming (Milestone 2+) |
| Dashboard Engineer | Visualization layer (Milestone 4) |

---

## Repository Layout

```
food-delivery-streaming/
│
├── README.md                        ← This file
│
├── schemas/
│   ├── order_lifecycle_event.avsc   ← AVRO schema: Feed 1
│   └── courier_status_event.avsc    ← AVRO schema: Feed 2
│
├── generator/
│   ├── main.py                      ← CLI entry point
│   ├── config.py                    ← All tuneable parameters & reference data
│   ├── order_generator.py           ← Feed 1 generator
│   ├── courier_generator.py         ← Feed 2 generator
│   ├── avro_writer.py               ← Pure-stdlib AVRO OCF writer
│   └── README.md                    ← Generator usage guide
│
└── sample_data/
    ├── order_lifecycle_events.jsonl  ← Feed 1 sample (JSON Lines)
    ├── order_lifecycle_events.avro   ← Feed 1 sample (AVRO binary)
    ├── courier_status_events.jsonl   ← Feed 2 sample (JSON Lines)
    └── courier_status_events.avro    ← Feed 2 sample (AVRO binary)
```

---

## Feed Design

### Why Two Feeds?

A food delivery platform's core operational risk centres on two questions:

> **"What is happening to each order right now?"**  
> **"Where is each courier and can they take a delivery?"**

These questions are answered by fundamentally different data sources with different schemas, cardinalities, and emission rates:

| Dimension | Feed 1: Order Lifecycle | Feed 2: Courier Status |
|-----------|------------------------|----------------------|
| **Emission trigger** | State transition (sparse) | GPS ping + state change (dense) |
| **Frequency** | ~6–10 events per order | Every 10–30 seconds per active courier |
| **Key entity** | `order_id` | `courier_id` |
| **Join surface** | `restaurant_id`, `courier_id`, `zone_id` | `order_id`, `zone_id` |
| **Primary analytics** | Fulfilment rates, cancellation, ETA accuracy | Supply/demand ratio, delivery speed, anomaly detection |

Together they enable **stream-table joins** (e.g. enrich order events with the assigned courier's live position) and **supply-demand gap analysis** (orders waiting vs. idle couriers per zone per minute).

---

### Feed 1: Order Lifecycle Events

**What it models:**  
Each event represents a **single state transition** in the order finite state machine. One event is emitted per status change; downstream processors reconstruct the full order history using `order_id` as the join key.

**State machine:**

```
PLACED → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP → EN_ROUTE → DELIVERED
                                                                          ↘ CANCELLED / FAILED
```

**Key fields for event-time processing:**

| Field | Purpose |
|-------|---------|
| `event_timestamp` | Watermark anchor — when the transition *actually* occurred |
| `ingestion_timestamp` | Processing time — broker arrival; delta reveals late data |
| `order_id` | Join key for all downstream enrichments |
| `courier_id` | Late-binding foreign key (null until ACCEPTED) |
| `restaurant_id` / `zone_id` | Reference-table join keys for zone and restaurant analytics |
| `is_late_arrival` | Pre-labelled late data for watermark validation |
| `is_duplicate` | Pre-labelled duplicates for idempotency testing |

**Analytics enabled:**

- **End-to-end order latency** (tumbling window on `event_timestamp`, pivot PLACED→DELIVERED)
- **Cancellation rate by zone, hour, cuisine** (filter on CANCELLED + group by `zone_id`)
- **ETA accuracy** (`estimated_delivery_time_seconds` vs `actual_delivery_time_seconds`)
- **Promotion effectiveness** (promo orders vs non-promo orders: cancellation rate, basket size)
- **Revenue per zone per window** (sum `order_total_cents` in sliding windows)
- **Restaurant prep-time SLA breaches** (`actual_prep_time_seconds` > threshold)

---

### Feed 2: Courier Status Events

**What it models:**  
High-frequency GPS pings and status-change events for each courier. A single courier emits events every ~15 seconds when active, making this feed **20–50× denser** than the order feed.

**Courier state machine:**

```
OFFLINE → ONLINE_IDLE → HEADING_TO_RESTAURANT → WAITING_AT_RESTAURANT
                                                         ↓
                                                    PICKED_UP → EN_ROUTE_TO_CUSTOMER → DELIVERED
                                                         ↘ OFFLINE (mid-delivery anomaly)
```

**Key fields for event-time processing:**

| Field | Purpose |
|-------|---------|
| `event_timestamp` | GPS event time; watermark anchor |
| `ingestion_timestamp` | Network/device delay; clock skew detection |
| `courier_id` | Primary key; join with order feed |
| `order_id` | Null-able foreign key; links active delivery |
| `zone_id` | Supply-side zone signal |
| `latitude` / `longitude` | Geospatial analytics, ETA computation |
| `session_id` | Groups events within a single courier shift |
| `anomaly_flag` | Pre-labelled streaming anomalies |

**Analytics enabled:**

- **Real-time supply/demand heatmap** (count `ONLINE_IDLE` couriers per `zone_id` per minute)
- **Courier utilisation rate** (time in delivery states / total session time)
- **Delivery speed distribution** (distance / time between PICKED_UP and DELIVERED)
- **Anomaly detection** (IMPOSSIBLE_SPEED, LOCATION_JUMP, OFFLINE_MID_DELIVERY)
- **Fleet availability forecasting** (sliding-window count of available couriers)
- **Courier earnings analytics** (sum `earnings_session_cents` over shift)

---

## Schema Overview

Both schemas are defined in [Apache AVRO](https://avro.apache.org/) format (`.avsc`), stored in `schemas/`.

### Design principles

1. **`schema_version` field** — Every record carries a string version field (`"1.0.0"`). This allows Kafka Schema Registry or a custom compatibility layer to detect breaking vs. additive changes.

2. **Union types for nullable fields** — All optional fields use `["null", <type>]` unions with `"default": null`, enabling backward-compatible additions without breaking existing consumers.

3. **Enum types for controlled vocabularies** — `order_status`, `courier_status`, `vehicle_type`, `cancellation_reason`, `anomaly_flag` are enums, preventing invalid state strings and enabling efficient integer encoding in AVRO binary.

4. **`timestamp-millis` logical type** — `event_timestamp` and `ingestion_timestamp` are stored as `long` with the `timestamp-millis` logical type annotation, making them compatible with Spark's `timestamp` type and Flink's watermark APIs out of the box.

5. **`metadata` map field** — An extensible `map<string>` field on both schemas absorbs future key-value pairs (e.g. A/B test variant IDs, app version strings) without requiring a schema migration.

---

## Edge Cases & Streaming Correctness

The generator injects the following edge cases to stress-test downstream watermark and deduplication logic:

| Edge Case | How It's Injected | Streaming Challenge |
|-----------|------------------|---------------------|
| **Late arrivals** | `event_timestamp` set in the past; `ingestion_timestamp` = now | Watermark must tolerate late data up to configured threshold |
| **Duplicates** | Same logical event re-emitted with new `event_id` and slightly later `ingestion_timestamp` | Exactly-once semantics; deduplicate on `event_id` |
| **Missing lifecycle steps** | One intermediate status removed (e.g. PLACED → ACCEPTED → EN_ROUTE, skipping PREPARING) | State machines must not assume contiguous transitions |
| **Impossible durations** | Prep or delivery time set to 1–5 seconds or 5+ hours | Anomaly detection; outlier filtering in aggregations |
| **Courier offline mid-delivery** | OFFLINE event injected after PICKED_UP | Order status stuck — timeout detection required |
| **Location jumps** | GPS coordinates teleport >5 km in one ping | Spatial outlier detection in courier analytics |
| **Impossible speed** | `speed_kmh` set to 180–350 for a bicycle/foot courier | Speed-envelope validation rule |
| **Out-of-order events** | Rating events have `event_timestamp` minutes/hours after DELIVERED | Late-arriving enrichment; must not drop with tight watermarks |
| **Clock skew** | `event_timestamp` > `ingestion_timestamp` (device clock ahead) | Watermark strategy must handle negative lag |

All edge-case events carry boolean flags (`is_duplicate`, `is_late_arrival`) or an enum (`anomaly_flag`) so downstream tests can measure recall/precision of detection logic.

---

## Quick Start

### Requirements

- Python 3.8+
- No third-party packages required (pure stdlib)

### Generate sample data

```bash
# Default: 100 orders, 80 couriers, 1-hour window
python generator/main.py

# Larger dataset
python generator/main.py --orders 1000 --couriers 200 --restaurants 80 --duration 7200

# With lunch-hour demand surge
python generator/main.py --orders 500 --surge --surge-zone zone_downtown --surge-multiplier 3.5

# Reproducible run
python generator/main.py --seed 99 --orders 250
```

### Output files

| File | Format | Description |
|------|--------|-------------|
| `order_lifecycle_events.jsonl` | JSON Lines | One event per line; easy to inspect with `jq` |
| `order_lifecycle_events.avro` | AVRO OCF | Binary; load with Spark, Flink, or `avro-tools` |
| `courier_status_events.jsonl` | JSON Lines | One GPS/status event per line |
| `courier_status_events.avro` | AVRO OCF | Binary format for pipeline testing |

### Inspect sample JSON

```bash
# First 5 order events
head -5 sample_data/order_lifecycle_events.jsonl | python3 -m json.tool

# Count events by status
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l)['order_status'] for l in sys.stdin)
[print(f'{s:<25} {n}') for s,n in sorted(c.items())]
"

# Count anomaly types in courier feed
cat sample_data/courier_status_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l).get('anomaly_flag') for l in sys.stdin)
[print(f'{s:<30} {n}') for s,n in sorted(c.items())]
"
```

---

## Planned Analytics (Milestones 2–4)

| Milestone | Technology | Key Deliverable |
|-----------|-----------|-----------------|
| **M2** | Kafka / Redpanda | Publish both feeds to topics; configure partitioning by `zone_id` |
| **M3** | Spark Structured Streaming | Windowed aggregations, watermarks, stream-table joins, anomaly detection |
| **M4** | Grafana / Streamlit | Live dashboard: zone heatmaps, cancellation rate, ETA accuracy, courier utilisation |

Planned streaming SQL queries (M3):

```sql
-- 5-minute tumbling window: orders placed per zone
SELECT zone_id, window(event_timestamp, '5 minutes') AS w, COUNT(*) AS orders
FROM order_stream WHERE order_status = 'PLACED'
GROUP BY zone_id, w;

-- Supply/demand ratio: available couriers vs open orders per zone
SELECT o.zone_id,
       COUNT(DISTINCT o.order_id) AS open_orders,
       COUNT(DISTINCT c.courier_id) AS idle_couriers,
       COUNT(DISTINCT c.courier_id) / NULLIF(COUNT(DISTINCT o.order_id), 0) AS ratio
FROM order_stream o
JOIN courier_stream c ON o.zone_id = c.zone_id
WHERE o.order_status IN ('PLACED','ACCEPTED','PREPARING')
  AND c.courier_status = 'ONLINE_IDLE'
GROUP BY o.zone_id;
```

---

*Generated with Python 3.8+ — no external dependencies required.*
