"""Export the aggregated serving marts used by the report and its local chat.

The CSVs stay under ignored ``data/gold/powerbi``; no trip-level data or
Power BI cache is stored in Git.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

OUTPUT = Path(__file__).resolve().parents[1] / "data" / "gold" / "powerbi"
MARTS = {
    "daily_trip_metrics": ("_source_month", "pickup_date"),
    "hourly_demand": ("_source_month", "pickup_date", "pickup_hour"),
    "payment_type_summary": ("_source_month", "payment_type"),
    "pickup_zone_performance": ("_source_month", "pickup_location_id"),
    "takeaway_breakdown": ("_source_month", "borough", "pickup_hour", "payment_type"),
}


def main() -> None:
    """Export each mart via PostgreSQL COPY, replacing a CSV only on success."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with psycopg2.connect(
        host=os.environ["ANALYTICS_DB_HOST"],
        port=os.environ["ANALYTICS_DB_PORT"],
        dbname=os.environ["ANALYTICS_DB_NAME"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
    ) as connection:
        with connection.cursor() as cursor:
            for table, sort_columns in MARTS.items():
                target = OUTPUT / f"{table}.csv"
                temporary = OUTPUT / f"{table}.csv.part"
                query = sql.SQL(
                    "COPY (SELECT * FROM {}.{} ORDER BY {}) TO STDOUT WITH CSV HEADER"
                ).format(
                    sql.Identifier("analytics"),
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, sort_columns)),
                )
                try:
                    with temporary.open("w", encoding="utf-8", newline="") as stream:
                        cursor.copy_expert(query.as_string(connection), stream)
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
                print(f"Exported {table}: {target.stat().st_size} bytes")


if __name__ == "__main__":
    main()
