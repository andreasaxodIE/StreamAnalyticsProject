"""
courier_generator.py — Generates synthetic CourierStatusEvent streams.

Models a fleet of couriers across geographic zones, emitting:
  - Periodic GPS pings (position updates every 10-30 seconds when active)
  - Status change events (IDLE → HEADING_TO_RESTAURANT → ... → DELIVERED)
  - Edge cases: offline mid-delivery, impossible speeds, location jumps,
    duplicate pings, late-arriving GPS data.
"""

from __future__ import annotations
import random
import uuid
import math
import copy
from typing import List, Dict, Optional, Iterator

from config import (
    GeneratorConfig, ZONES, weighted_choice,
    zone_courier_weighted_choice, random_coords_in_zone,
)


# ---------------------------------------------------------------------------
# Vehicle speed envelopes (km/h)
# ---------------------------------------------------------------------------
VEHICLE_SPEED: Dict[str, Dict] = {
    "BICYCLE":    {"min": 8,  "max": 25,  "avg": 15},
    "SCOOTER":    {"min": 15, "max": 45,  "avg": 28},
    "MOTORCYCLE": {"min": 20, "max": 80,  "avg": 40},
    "CAR":        {"min": 10, "max": 90,  "avg": 35},
    "FOOT":       {"min": 3,  "max": 8,   "avg": 5},
}

VEHICLE_TYPES = list(VEHICLE_SPEED.keys())
VEHICLE_WEIGHTS = [0.35, 0.30, 0.15, 0.12, 0.08]

PING_INTERVAL_SECONDS = 15   # GPS update every ~15s when active


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def _move_towards(lat, lon, target_lat, target_lon, speed_kmh, seconds) -> tuple:
    """Move position towards target at given speed."""
    dist_km = _haversine_km(lat, lon, target_lat, target_lon)
    if dist_km < 0.01:
        return target_lat, target_lon
    travel_km = (speed_kmh * seconds) / 3600.0
    frac = min(travel_km / dist_km, 1.0)
    new_lat = lat + frac * (target_lat - lat)
    new_lon = lon + frac * (target_lon - lon)
    return round(new_lat, 6), round(new_lon, 6)


def _heading(lat1, lon1, lat2, lon2) -> float:
    d_lon = math.radians(lon2 - lon1)
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    x = math.sin(d_lon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(d_lon)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


class CourierSimulator:
    """Simulates one courier's complete shift."""

    def __init__(self, courier_id: str, config: GeneratorConfig):
        self.courier_id = courier_id
        self.cfg = config
        self.vehicle_type = weighted_choice(VEHICLE_TYPES, VEHICLE_WEIGHTS)
        self.zone_id = zone_courier_weighted_choice()
        self.lat, self.lon = random_coords_in_zone(self.zone_id)
        self.status = "ONLINE_IDLE"
        self.prev_status = None
        self.session_id = str(uuid.uuid4())
        self.deliveries_completed = 0
        self.earnings_cents = 0
        self.shift_start_ts = 0
        self.battery = random.randint(50, 100)
        self.order_id: Optional[str] = None

    def _speed(self) -> float:
        s = VEHICLE_SPEED[self.vehicle_type]
        return random.uniform(s["min"], s["max"])

    def _make_event(
        self,
        ts_ms: int,
        status: str,
        anomaly_flag=None,
        is_duplicate=False,
        is_late=False,
        distance_to_restaurant=None,
        distance_to_customer=None,
    ) -> Dict:
        speed = self._speed() if status not in ("ONLINE_IDLE", "OFFLINE", "BREAK", "WAITING_AT_RESTAURANT") else 0.0

        ingestion_lag = random.randint(50, 2000)
        if is_late:
            ingestion_lag += random.randint(30_000, self.cfg.max_late_arrival_seconds * 1000)

        shift_dur = (ts_ms - self.shift_start_ts) // 1000 if self.shift_start_ts else 0

        return {
            "schema_version": "1.0.0",
            "event_id": str(uuid.uuid4()),
            "courier_id": self.courier_id,
            "order_id": self.order_id,
            "zone_id": self.zone_id,
            "courier_status": status,
            "previous_status": self.prev_status,
            "event_timestamp": ts_ms,
            "ingestion_timestamp": ts_ms + ingestion_lag,
            "latitude": self.lat,
            "longitude": self.lon,
            "location_accuracy_meters": round(random.uniform(3.0, 25.0), 1),
            "speed_kmh": round(speed, 1) if speed > 0 else None,
            "heading_degrees": round(random.uniform(0, 360), 1) if speed > 0 else None,
            "vehicle_type": self.vehicle_type,
            "battery_level_pct": self.battery,
            "distance_to_restaurant_meters": distance_to_restaurant,
            "distance_to_customer_meters": distance_to_customer,
            "session_id": self.session_id,
            "shift_duration_seconds": shift_dur,
            "deliveries_completed_in_session": self.deliveries_completed,
            "earnings_session_cents": self.earnings_cents,
            "is_duplicate": is_duplicate,
            "is_late_arrival": is_late,
            "anomaly_flag": anomaly_flag,
            "metadata": None,
        }

    def generate_shift(self, start_ts_ms: int, duration_seconds: int) -> List[Dict]:
        """Simulate a full courier shift and return all events."""
        self.shift_start_ts = start_ts_ms
        ts = start_ts_ms
        end_ts = start_ts_ms + duration_seconds * 1000
        events: List[Dict] = []
        cfg = self.cfg

        # Online event
        self.status = "ONLINE_IDLE"
        self.prev_status = None
        events.append(self._make_event(ts, "ONLINE_IDLE"))

        while ts < end_ts:
            # Decide: take an order or stay idle
            idle_wait = random.randint(30, 300)  # 30s–5min idle
            ts += idle_wait * 1000

            if ts >= end_ts:
                break

            # Assign order
            self.order_id = f"ord_{uuid.uuid4().hex[:12]}"
            restaurant_zone = zone_courier_weighted_choice()
            rest_lat, rest_lon = random_coords_in_zone(restaurant_zone)
            cust_lat, cust_lon = random_coords_in_zone(restaurant_zone)

            # ── HEADING_TO_RESTAURANT ──────────────────────────────────────
            self.prev_status = self.status
            self.status = "HEADING_TO_RESTAURANT"
            dist_to_rest_m = int(_haversine_km(self.lat, self.lon, rest_lat, rest_lon) * 1000)
            ev = self._make_event(ts, "HEADING_TO_RESTAURANT", distance_to_restaurant=dist_to_rest_m)
            events.append(ev)

            # GPS pings while travelling to restaurant
            travel_time = max(60, int(dist_to_rest_m / (self._speed() * 1000 / 3600)))
            ping_ts = ts
            while ping_ts < ts + travel_time * 1000:
                ping_ts += PING_INTERVAL_SECONDS * 1000
                self.lat, self.lon = _move_towards(self.lat, self.lon, rest_lat, rest_lon,
                                                    self._speed(), PING_INTERVAL_SECONDS)
                remaining = int(_haversine_km(self.lat, self.lon, rest_lat, rest_lon) * 1000)
                is_late = random.random() < cfg.late_arrival_rate
                ping_ev = self._make_event(ping_ts, "HEADING_TO_RESTAURANT",
                                           distance_to_restaurant=remaining, is_late=is_late)
                events.append(ping_ev)
                # Possible duplicate ping
                if random.random() < cfg.duplicate_rate:
                    dup = copy.deepcopy(ping_ev)
                    dup["event_id"] = str(uuid.uuid4())
                    dup["ingestion_timestamp"] += random.randint(100, 3000)
                    dup["is_duplicate"] = True
                    events.append(dup)

            ts += travel_time * 1000
            self.lat, self.lon = rest_lat, rest_lon

            # ── WAITING_AT_RESTAURANT ──────────────────────────────────────
            self.prev_status = self.status
            self.status = "WAITING_AT_RESTAURANT"
            events.append(self._make_event(ts, "WAITING_AT_RESTAURANT"))
            wait_time = random.randint(120, 900)  # 2–15 min wait
            ts += wait_time * 1000

            # Courier offline mid-delivery edge case
            if random.random() < cfg.courier_offline_mid_delivery_rate:
                self.prev_status = self.status
                self.status = "OFFLINE"
                offline_ev = self._make_event(ts, "OFFLINE", anomaly_flag="OFFLINE_MID_DELIVERY")
                events.append(offline_ev)
                self.order_id = None
                # Resume after a break
                break_dur = random.randint(300, 1800)
                ts += break_dur * 1000
                self.prev_status = "OFFLINE"
                self.status = "ONLINE_IDLE"
                self.session_id = str(uuid.uuid4())  # new session
                self.shift_start_ts = ts
                events.append(self._make_event(ts, "ONLINE_IDLE"))
                continue

            # ── PICKED_UP ─────────────────────────────────────────────────
            self.prev_status = self.status
            self.status = "PICKED_UP"
            dist_to_cust_m = int(_haversine_km(self.lat, self.lon, cust_lat, cust_lon) * 1000)
            events.append(self._make_event(ts, "PICKED_UP", distance_to_customer=dist_to_cust_m))

            # ── EN_ROUTE_TO_CUSTOMER ───────────────────────────────────────
            self.prev_status = self.status
            self.status = "EN_ROUTE_TO_CUSTOMER"
            events.append(self._make_event(ts, "EN_ROUTE_TO_CUSTOMER", distance_to_customer=dist_to_cust_m))

            # GPS pings en route
            delivery_time = max(60, int(dist_to_cust_m / (self._speed() * 1000 / 3600)))
            ping_ts = ts

            # Inject location jump anomaly occasionally
            if random.random() < 0.03:
                jump_lat = self.lat + random.uniform(-0.05, 0.05)
                jump_lon = self.lon + random.uniform(-0.05, 0.05)
                jump_ev = self._make_event(ping_ts + 5000, "EN_ROUTE_TO_CUSTOMER",
                                           anomaly_flag="LOCATION_JUMP")
                jump_ev["latitude"] = round(jump_lat, 6)
                jump_ev["longitude"] = round(jump_lon, 6)
                events.append(jump_ev)

            # Inject impossible speed anomaly occasionally
            if random.random() < cfg.impossible_duration_rate:
                fast_ev = self._make_event(ping_ts + 8000, "EN_ROUTE_TO_CUSTOMER",
                                           anomaly_flag="IMPOSSIBLE_SPEED")
                fast_ev["speed_kmh"] = round(random.uniform(180, 350), 1)  # clearly wrong for courier
                events.append(fast_ev)

            while ping_ts < ts + delivery_time * 1000:
                ping_ts += PING_INTERVAL_SECONDS * 1000
                self.lat, self.lon = _move_towards(self.lat, self.lon, cust_lat, cust_lon,
                                                    self._speed(), PING_INTERVAL_SECONDS)
                remaining = int(_haversine_km(self.lat, self.lon, cust_lat, cust_lon) * 1000)
                is_late = random.random() < cfg.late_arrival_rate
                ping_ev = self._make_event(ping_ts, "EN_ROUTE_TO_CUSTOMER",
                                           distance_to_customer=remaining, is_late=is_late)
                events.append(ping_ev)

            ts += delivery_time * 1000
            self.lat, self.lon = cust_lat, cust_lon

            # ── DELIVERED ─────────────────────────────────────────────────
            self.prev_status = self.status
            self.status = "DELIVERED"
            self.deliveries_completed += 1
            earn = random.randint(350, 800)  # per-delivery earnings
            self.earnings_cents += earn
            self.battery = max(5, self.battery - random.randint(2, 8))
            events.append(self._make_event(ts, "DELIVERED"))

            # Back to IDLE
            self.order_id = None
            self.prev_status = "DELIVERED"
            self.status = "ONLINE_IDLE"
            events.append(self._make_event(ts + 5000, "ONLINE_IDLE"))
            ts += 5000

        # End shift
        self.prev_status = self.status
        self.status = "OFFLINE"
        events.append(self._make_event(ts, "OFFLINE"))

        # Sort by event_timestamp
        events.sort(key=lambda e: e["event_timestamp"])
        return events


class CourierFleetGenerator:
    def __init__(self, config: GeneratorConfig):
        self.cfg = config
        random.seed(config.random_seed + 1)  # different seed from orders
        self.courier_ids = [f"courier_{i:04d}" for i in range(config.num_couriers)]

    def generate(self, start_ts_ms: int) -> List[Dict]:
        all_events: List[Dict] = []
        for cid in self.courier_ids:
            sim = CourierSimulator(cid, self.cfg)
            # Stagger shift starts
            shift_start = start_ts_ms + random.randint(0, 3600_000)
            shift_dur = random.randint(3600, self.cfg.simulation_duration_seconds)
            events = sim.generate_shift(shift_start, shift_dur)
            all_events.extend(events)
        all_events.sort(key=lambda e: e["ingestion_timestamp"])
        return all_events

    def stream(self, start_ts_ms: int) -> Iterator[Dict]:
        yield from self.generate(start_ts_ms)
