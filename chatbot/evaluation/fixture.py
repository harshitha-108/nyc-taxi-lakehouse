"""Small, internally reconciled Gold-like marts for offline chatbot evaluation.

These invented figures are deliberately unlike production TLC totals. Every month
has one weekday and one weekend observation; March and April are absent so a
question about them must not silently interpolate data.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def _base(month: int) -> dict[str, Any]:
    return {"_source_taxi_type": "yellow", "_source_year": 2024,
            "_source_month": month}


DAILY = [
    # pickup_date, month, trips, TLC total amount, tips, average fare, total distance
    ("2024-01-01", 1, 10, 100, 10, 8, 30),
    ("2024-01-06", 1, 20, 240, 24, 10, 80),
    ("2024-02-01", 2, 30, 390, 39, 12, 120),
    ("2024-02-03", 2, 30, 450, 45, 14, 150),
    ("2024-05-01", 5, 50, 800, 100, 16, 250),
    ("2024-05-04", 5, 40, 600, 80, 15, 160),
    ("2024-06-01", 6, 30, 420, 60, 11, 90),
    ("2024-06-03", 6, 20, 280, 40, 13, 80),
]

HOURLY = [
    # month, hour, trips, TLC total amount
    (1, 8, 10, 100), (1, 18, 20, 240),
    (2, 8, 20, 260), (2, 18, 40, 580),
    (5, 8, 30, 450), (5, 18, 60, 950),
    (6, 8, 20, 280), (6, 18, 30, 420),
]

PAYMENT = [
    # month, TLC payment code (1 card, 2 cash), trips, amount, tips
    (1, 1, 24, 300, 34), (1, 2, 6, 40, 0),
    (2, 1, 45, 700, 84), (2, 2, 15, 140, 0),
    (5, 1, 72, 1100, 180), (5, 2, 18, 300, 0),
    (6, 1, 35, 500, 100), (6, 2, 15, 200, 0),
]

ZONES = [
    # month, borough, zone, trips, amount, tips
    (1, "Manhattan", "Midtown", 18, 200, 24),
    (1, "Queens", "Astoria", 7, 80, 6),
    (1, "Queens", "Jackson Heights", 5, 60, 4),
    (2, "Manhattan", "Midtown", 36, 500, 60),
    (2, "Queens", "Astoria", 14, 200, 15),
    (2, "Queens", "Jackson Heights", 10, 140, 9),
    (5, "Manhattan", "Midtown", 50, 800, 110),
    (5, "Queens", "Astoria", 25, 350, 45),
    (5, "Queens", "Jackson Heights", 15, 250, 25),
    (6, "Manhattan", "Midtown", 20, 280, 40),
    (6, "Queens", "Astoria", 20, 280, 40),
    (6, "Queens", "Jackson Heights", 10, 140, 20),
]

BREAKDOWN = [
    # month, borough, hour, payment code, trips
    (1, "Manhattan", 8, 1, 5), (1, "Manhattan", 8, 2, 1),
    (1, "Manhattan", 18, 1, 11), (1, "Manhattan", 18, 2, 1),
    (1, "Queens", 8, 1, 3), (1, "Queens", 8, 2, 1),
    (1, "Queens", 18, 1, 5), (1, "Queens", 18, 2, 3),
    (2, "Manhattan", 8, 1, 10), (2, "Manhattan", 8, 2, 2),
    (2, "Manhattan", 18, 1, 19), (2, "Manhattan", 18, 2, 5),
    (2, "Queens", 8, 1, 6), (2, "Queens", 8, 2, 2),
    (2, "Queens", 18, 1, 10), (2, "Queens", 18, 2, 6),
    (5, "Manhattan", 8, 1, 15), (5, "Manhattan", 8, 2, 3),
    (5, "Manhattan", 18, 1, 26), (5, "Manhattan", 18, 2, 6),
    (5, "Queens", 8, 1, 10), (5, "Queens", 8, 2, 2),
    (5, "Queens", 18, 1, 21), (5, "Queens", 18, 2, 7),
    (6, "Manhattan", 8, 1, 6), (6, "Manhattan", 8, 2, 2),
    (6, "Manhattan", 18, 1, 8), (6, "Manhattan", 18, 2, 4),
    (6, "Queens", 8, 1, 8), (6, "Queens", 8, 2, 4),
    (6, "Queens", 18, 1, 13), (6, "Queens", 18, 2, 5),
]


def mart_rows() -> dict[str, list[dict[str, Any]]]:
    """Return rows matching the five published mart CSV schemas."""
    return {
        "daily_trip_metrics": [
            {**_base(month), "pickup_date": date, "trip_count": trips,
             "total_revenue": amount, "total_tip_amount": tips,
             "average_fare_amount": fare, "total_trip_distance": distance}
            for date, month, trips, amount, tips, fare, distance in DAILY
        ],
        "hourly_demand": [
            {**_base(month), "pickup_hour": hour, "trip_count": trips,
             "total_revenue": amount}
            for month, hour, trips, amount in HOURLY
        ],
        "payment_type_summary": [
            {**_base(month), "payment_type": code, "trip_count": trips,
             "total_revenue": amount, "total_tip_amount": tips}
            for month, code, trips, amount, tips in PAYMENT
        ],
        "pickup_zone_performance": [
            {**_base(month), "borough": borough, "zone": zone,
             "pickup_location_id": {"Midtown": 101, "Astoria": 102,
                                    "Jackson Heights": 103}[zone],
             "trip_count": trips, "total_revenue": amount,
             "total_tip_amount": tips}
            for month, borough, zone, trips, amount, tips in ZONES
        ],
        "takeaway_breakdown": [
            {**_base(month), "borough": borough, "pickup_hour": hour,
             "payment_type": code, "trip_count": trips}
            for month, borough, hour, code, trips in BREAKDOWN
        ],
    }


def write_fixture(directory: Path) -> None:
    """Write only the isolated synthetic CSV exports into a caller-owned directory."""
    directory.mkdir(parents=True, exist_ok=True)
    for table, rows in mart_rows().items():
        with (directory / f"{table}.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
