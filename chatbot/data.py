"""Read only the published, aggregated taxi marts used by the report."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

TABLES = frozenset({
    "daily_trip_metrics",
    "hourly_demand",
    "payment_type_summary",
    "pickup_zone_performance",
    "takeaway_breakdown",
})
BOROUGH_TABLES = frozenset({"pickup_zone_performance", "takeaway_breakdown"})
DEFAULT_CSV_DIR = Path(__file__).resolve().parents[1] / "data" / "gold" / "powerbi"


class SourceError(RuntimeError):
    """The selected published mart cannot be read."""


def _check_table(table: str, borough: str | None) -> None:
    if table not in TABLES:
        raise ValueError("Unsupported mart")
    if borough and table not in BOROUGH_TABLES:
        raise ValueError("This mart has no borough grain")


def _borough_labels(table: str, borough: str | None) -> tuple[str, ...]:
    if borough is None:
        return ()
    if table == "takeaway_breakdown" and borough.casefold() == "unknown":
        return (borough, "Unknown / unmapped")
    return (borough,)


class CsvSource:
    """Match the Power BI Desktop report's local CSV snapshot."""

    def __init__(self, directory: Path = DEFAULT_CSV_DIR) -> None:
        self.directory = Path(directory)

    def fetch(
        self, table: str, period: tuple[int | None, int] | None = None,
        borough: str | None = None,
    ) -> list[dict[str, Any]]:
        _check_table(table, borough)
        borough_labels = {label.casefold() for label in _borough_labels(table, borough)}
        path = self.directory / f"{table}.csv"
        try:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                required = {"_source_taxi_type", "_source_year", "_source_month", "trip_count"}
                if borough:
                    required.add("borough")
                if not reader.fieldnames or not required.issubset(reader.fieldnames):
                    raise SourceError(f"{table}.csv is not a published serving mart export")
                return [
                    row for row in reader
                    if row["_source_taxi_type"] == "yellow"
                    and (period is None or (
                        (period[0] is None or int(row["_source_year"]) == period[0])
                        and int(row["_source_month"]) == period[1]
                    ))
                    and (not borough_labels or row["borough"].casefold() in borough_labels)
                ]
        except (OSError, UnicodeError, csv.Error, ValueError, KeyError) as exc:
            raise SourceError(
                f"Could not read {table}.csv from the Power BI export folder"
            ) from exc

    def label(self, table: str) -> str:
        return f"Power BI CSV snapshot: {table}.csv"

    def health(self) -> dict[str, Any]:
        missing = sorted(
            table for table in TABLES if not (self.directory / f"{table}.csv").is_file()
        )
        return {"status": "ready" if not missing else "missing_exports", "source": "csv",
                "missing_exports": missing}


class PostgresSource:
    """Parameterised SELECTs against the published PostgreSQL analytics schema."""

    def __init__(self) -> None:
        self.connect_kwargs = {
            "host": os.environ.get("ANALYTICS_DB_HOST", "127.0.0.1"),
            "port": int(os.environ.get("ANALYTICS_DB_PORT", "5432")),
            "dbname": os.environ.get("ANALYTICS_DB_NAME", "nyc_taxi_analytics"),
            "user": os.environ.get("ANALYTICS_DB_USER", "analytics_dev"),
            "password": os.environ.get("ANALYTICS_DB_PASSWORD", ""),
            "connect_timeout": 3,
        }

    def fetch(
        self, table: str, period: tuple[int | None, int] | None = None,
        borough: str | None = None,
    ) -> list[dict[str, Any]]:
        _check_table(table, borough)
        try:
            import psycopg2
            from psycopg2 import sql
        except ImportError as exc:
            raise SourceError("PostgreSQL mode needs psycopg2-binary") from exc
        if not self.connect_kwargs["password"]:
            raise SourceError("ANALYTICS_DB_PASSWORD is required in PostgreSQL mode")
        statement = sql.SQL("SELECT * FROM analytics.{} WHERE _source_taxi_type = %s").format(
            sql.Identifier(table))
        parameters: list[Any] = ["yellow"]
        if period:
            if period[0] is not None:
                statement += sql.SQL(" AND _source_year = %s")
                parameters.append(period[0])
            statement += sql.SQL(" AND _source_month = %s")
            parameters.append(period[1])
        if borough:
            borough_labels = _borough_labels(table, borough)
            if len(borough_labels) == 1:
                statement += sql.SQL(" AND lower(borough) = lower(%s)")
            else:
                statement += sql.SQL(" AND lower(borough) IN (lower(%s), lower(%s))")
            parameters.extend(borough_labels)
        try:
            with psycopg2.connect(**self.connect_kwargs) as connection:
                connection.set_session(readonly=True)
                with connection.cursor() as cursor:
                    cursor.execute(statement, parameters)
                    columns = [column.name for column in cursor.description]
                    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        except psycopg2.Error as exc:
            raise SourceError("Could not read the analytics serving database") from exc

    def label(self, table: str) -> str:
        return f"PostgreSQL serving mart: analytics.{table}"

    def health(self) -> dict[str, Any]:
        try:
            for table in sorted(TABLES):
                self.fetch(table, (1900, 1))
        except SourceError:
            return {"status": "unavailable", "source": "postgres"}
        return {"status": "ready", "source": "postgres"}


def source_from_env() -> CsvSource | PostgresSource:
    mode = os.environ.get("CHAT_DATA_SOURCE", "csv").casefold()
    if mode == "csv":
        path = Path(os.environ.get("CHAT_CSV_DIR", str(DEFAULT_CSV_DIR)))
        return CsvSource(path)
    if mode == "postgres":
        return PostgresSource()
    raise ValueError("CHAT_DATA_SOURCE must be csv or postgres")
