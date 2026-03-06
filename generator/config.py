"""
config.py — Centralised configuration and reference data for the food-delivery event generator.
"""

from __future__ import annotations
import random
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


# ---------------------------------------------------------------------------
# Zone definitions — lat/lon bounding boxes for a fictional mid-size city
# ---------------------------------------------------------------------------
ZONES: Dict[str, Dict] = {
    "zone_downtown": {
        "lat_range": (40.7050, 40.7250),
        "lon_range": (-74.0100, -73.9900),
        "demand_weight": 3.5,
        "courier_density": 3.0,
        "label": "Downtown",
    },
    "zone_midtown": {
        "lat_range": (40.7250, 40.7550),
        "lon_range": (-74.0050, -73.9750),
        "demand_weight": 2.5,
        "courier_density": 2.0,
        "label": "Midtown",
    },
    "zone_uptown": {
        "lat_range": (40.7550, 40.7900),
        "lon_range": (-73.9900, -73.9500),
        "demand_weight": 1.5,
        "courier_density": 1.2,
        "label": "Uptown",
    },
    "zone_east_side": {
        "lat_range": (40.7100, 40.7450),
        "lon_range": (-73.9700, -73.9400),
        "demand_weight": 1.8,
        "courier_density": 1.5,
        "label": "East Side",
    },
    "zone_brooklyn": {
        "lat_range": (40.6500, 40.7000),
        "lon_range": (-74.0100, -73.9300),
        "demand_weight": 2.0,
        "courier_density": 1.8,
        "label": "Brooklyn",
    },
    "zone_suburbs": {
        "lat_range": (40.6000, 40.6500),
        "lon_range": (-74.0500, -73.9000),
        "demand_weight": 0.8,
        "courier_density": 0.5,
        "label": "Suburbs",
    },
}

# ---------------------------------------------------------------------------
# Demand time-of-day curve  (hour → relative multiplier)
# Lunch peak: 12-14, Dinner peak: 19-21
# ---------------------------------------------------------------------------
HOURLY_DEMAND_WEIGHTS: Dict[int, float] = {
    0: 0.1, 1: 0.05, 2: 0.03, 3: 0.02, 4: 0.02, 5: 0.05,
    6: 0.15, 7: 0.35, 8: 0.50,
    9: 0.40, 10: 0.35, 11: 0.70,
    12: 1.80, 13: 2.00, 14: 1.20,   # lunch peak
    15: 0.80, 16: 0.70, 17: 1.00,
    18: 1.80, 19: 2.20, 20: 2.00, 21: 1.50,  # dinner peak
    22: 0.90, 23: 0.40,
}

# Peak hour prep time multiplier: restaurants are slower during rush
PEAK_HOUR_PREP_MULTIPLIER: float = 1.4

# Weekend uplift
WEEKEND_MULTIPLIER: float = 1.25
WEEKDAY_MULTIPLIER: float = 1.00

# ---------------------------------------------------------------------------
# Weather — city-wide state, probability weights and delivery delay multipliers
# ---------------------------------------------------------------------------
WEATHER_CONDITIONS = ["CLEAR", "RAIN", "HEAVY_RAIN", "SNOW"]
WEATHER_WEIGHTS    = [0.60,   0.25,   0.10,        0.05]

# How much extra time bad weather adds to estimated_delivery_time_seconds
WEATHER_DELIVERY_MULTIPLIER: Dict[str, float] = {
    "CLEAR":      1.00,
    "RAIN":       1.20,
    "HEAVY_RAIN": 1.45,
    "SNOW":       1.60,
}

# ---------------------------------------------------------------------------
# Restaurant & menu catalogue
# ---------------------------------------------------------------------------
RESTAURANT_CUISINES = [
    "Italian", "Japanese", "Mexican", "Indian", "American",
    "Chinese", "Thai", "Mediterranean", "Korean", "French",
]

MENU_ITEMS_BY_CUISINE: Dict[str, List[Tuple[str, int]]] = {
    "Italian":       [("Margherita Pizza", 1299), ("Carbonara Pasta", 1599), ("Tiramisu", 699), ("Caesar Salad", 999)],
    "Japanese":      [("Salmon Sushi Roll", 1499), ("Ramen Bowl", 1399), ("Gyoza (6pc)", 799), ("Miso Soup", 399)],
    "Mexican":       [("Beef Burrito", 1099), ("Tacos Al Pastor (3pc)", 999), ("Guacamole & Chips", 699), ("Churros", 599)],
    "Indian":        [("Butter Chicken", 1349), ("Garlic Naan", 349), ("Biryani Rice", 1199), ("Mango Lassi", 449)],
    "American":      [("Smash Burger", 1299), ("Mac & Cheese", 999), ("Buffalo Wings", 1099), ("Milkshake", 649)],
    "Chinese":       [("Kung Pao Chicken", 1199), ("Dim Sum (4pc)", 899), ("Fried Rice", 849), ("Spring Rolls", 699)],
    "Thai":          [("Pad Thai", 1249), ("Green Curry", 1299), ("Mango Sticky Rice", 749), ("Tom Yum Soup", 849)],
    "Mediterranean": [("Falafel Wrap", 1099), ("Hummus Plate", 799), ("Shakshuka", 1199), ("Baklava", 549)],
    "Korean":        [("Bibimbap", 1349), ("Korean BBQ Platter", 1999), ("Tteokbokki", 999), ("Kimchi Pancake", 849)],
    "French":        [("Croque Monsieur", 1149), ("French Onion Soup", 999), ("Crêpes Suzette", 849), ("Quiche Lorraine", 1099)],
}

# ---------------------------------------------------------------------------
# Generator configuration dataclass
# ---------------------------------------------------------------------------
@dataclass
class GeneratorConfig:
    # Scale
    num_restaurants: int = 50
    num_couriers: int = 80
    num_customers: int = 500

    # Simulation window
    simulation_duration_seconds: int = 3600
    events_per_second_target: float = 10.0

    # Edge case injection rates (0.0 – 1.0)
    late_arrival_rate: float = 0.05
    max_late_arrival_seconds: int = 300
    duplicate_rate: float = 0.02
    cancellation_rate: float = 0.12
    missing_step_rate: float = 0.03
    impossible_duration_rate: float = 0.02
    courier_offline_mid_delivery_rate: float = 0.015

    # Demand surge
    surge_active: bool = False
    surge_zone: str = "zone_downtown"
    surge_multiplier: float = 3.0

    # Output
    output_dir: str = "./sample_data"
    json_sample_orders: int = 100
    json_sample_courier_events: int = 200
    random_seed: int = 42


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def weighted_choice(population: List, weights: List[float]):
    total = sum(weights)
    r = random.uniform(0, total)
    cumulative = 0.0
    for item, w in zip(population, weights):
        cumulative += w
        if r <= cumulative:
            return item
    return population[-1]


def zone_weighted_choice() -> str:
    zones = list(ZONES.keys())
    weights = [ZONES[z]["demand_weight"] for z in zones]
    return weighted_choice(zones, weights)


def zone_courier_weighted_choice() -> str:
    zones = list(ZONES.keys())
    weights = [ZONES[z]["courier_density"] for z in zones]
    return weighted_choice(zones, weights)


def random_coords_in_zone(zone_id: str) -> Tuple[float, float]:
    z = ZONES[zone_id]
    lat = random.uniform(*z["lat_range"])
    lon = random.uniform(*z["lon_range"])
    return round(lat, 6), round(lon, 6)


def hourly_demand_weight(hour: int, is_weekend: bool) -> float:
    base = HOURLY_DEMAND_WEIGHTS.get(hour, 0.5)
    return base * (WEEKEND_MULTIPLIER if is_weekend else WEEKDAY_MULTIPLIER)


def is_peak_hour(hour: int) -> bool:
    """True during lunch (12-14) and dinner (19-21) rush hours."""
    return hour in (12, 13, 14, 19, 20, 21)


def sample_weather() -> str:
    return weighted_choice(WEATHER_CONDITIONS, WEATHER_WEIGHTS)
