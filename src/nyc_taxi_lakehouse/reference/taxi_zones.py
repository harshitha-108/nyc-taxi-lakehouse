"""Download, validate, and load the official TLC Taxi Zone Lookup CSV."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)
TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
REQUIRED_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")


class TaxiZoneReferenceError(Exception):
    """Raised when the Taxi Zone Lookup cannot be downloaded or validated."""


@dataclass(frozen=True)
class TaxiZoneValidation:
    """Validation results for one authoritative Taxi Zone Lookup CSV."""

    path: Path
    row_count: int
    unique_location_ids: int
    duplicate_location_ids: int
    null_location_ids: int
    invalid_location_ids: int
    blank_boroughs: int
    blank_zones: int
    blank_service_zones: int


@dataclass(frozen=True)
class TaxiZoneReferenceResult:
    """Lineage and validation metadata for one successful reference ingestion."""

    source_url: str
    local_path: Path
    downloaded_at: datetime
    file_size_bytes: int
    validation: TaxiZoneValidation
    skipped_download: bool
    manifest_path: Path | None


def taxi_zone_csv_path(reference_dir: Path = Path("data/reference")) -> Path:
    """Return the stable local Taxi Zone Lookup CSV location."""
    return reference_dir / "taxi_zones" / "taxi_zone_lookup.csv"


def taxi_zone_manifest_path(reference_dir: Path = Path("data/reference")) -> Path:
    """Return the ignored runtime lineage manifest location."""
    return reference_dir / "taxi_zones" / "metadata" / "taxi_zone_lookup.json"


def _is_blank(value: str | None) -> bool:
    return value is None or not value.strip()


def validate_taxi_zone_csv(path: Path) -> TaxiZoneValidation:
    """Validate required CSV schema and key/data completeness without assuming a row count."""
    if not path.is_file() or path.stat().st_size == 0:
        raise TaxiZoneReferenceError(f"Taxi Zone Lookup file is missing or empty: {path}")
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            if reader.fieldnames is None:
                raise TaxiZoneReferenceError("Taxi Zone Lookup CSV does not contain a header.")
            missing_columns = set(REQUIRED_COLUMNS).difference(reader.fieldnames)
            if missing_columns:
                raise TaxiZoneReferenceError(
                    f"Taxi Zone Lookup CSV is missing columns: {sorted(missing_columns)}"
                )
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        raise TaxiZoneReferenceError(f"Taxi Zone Lookup CSV is unreadable: {path}") from exc
    if not rows:
        raise TaxiZoneReferenceError("Taxi Zone Lookup CSV contains no data rows.")

    parsed_ids: list[int] = []
    null_ids = invalid_ids = blank_boroughs = blank_zones = blank_service_zones = 0
    for row in rows:
        location_id = row["LocationID"]
        if _is_blank(location_id):
            null_ids += 1
        else:
            try:
                parsed_ids.append(int(location_id))
            except ValueError:
                invalid_ids += 1
        blank_boroughs += _is_blank(row["Borough"])
        blank_zones += _is_blank(row["Zone"])
        blank_service_zones += _is_blank(row["service_zone"])
    duplicate_ids = len(parsed_ids) - len(set(parsed_ids))
    if null_ids or invalid_ids or duplicate_ids or blank_boroughs or blank_zones:
        raise TaxiZoneReferenceError(
            "Taxi Zone Lookup validation failed: "
            f"null_location_ids={null_ids}, invalid_location_ids={invalid_ids}, "
            f"duplicate_location_ids={duplicate_ids}, blank_boroughs={blank_boroughs}, "
            f"blank_zones={blank_zones}."
        )
    return TaxiZoneValidation(
        path=path,
        row_count=len(rows),
        unique_location_ids=len(set(parsed_ids)),
        duplicate_location_ids=duplicate_ids,
        null_location_ids=null_ids,
        invalid_location_ids=invalid_ids,
        blank_boroughs=blank_boroughs,
        blank_zones=blank_zones,
        blank_service_zones=blank_service_zones,
    )


def _write_manifest(path: Path, result: TaxiZoneReferenceResult) -> Path | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.part")
    payload = asdict(result)
    payload["local_path"] = str(result.local_path)
    payload["downloaded_at"] = result.downloaded_at.isoformat()
    payload["manifest_path"] = str(path)
    payload["validation"]["path"] = str(result.validation.path)
    try:
        temporary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        LOGGER.warning("Could not write Taxi Zone reference manifest %s: %s", path, exc)
        return None
    return path


def ingest_taxi_zones(
    *,
    reference_dir: Path = Path("data/reference"),
    source_url: str = TAXI_ZONE_LOOKUP_URL,
    timeout_seconds: int = 30,
    force: bool = False,
) -> TaxiZoneReferenceResult:
    """Safely download or validate a cached official Taxi Zone Lookup CSV."""
    started_at = time.monotonic()
    target_path = taxi_zone_csv_path(reference_dir)
    skipped_download = target_path.exists() and not force
    if skipped_download:
        validation = validate_taxi_zone_csv(target_path)
        LOGGER.info(
            "Taxi Zone Lookup is valid and already present; skipping download: %s", target_path
        )
    else:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = target_path.with_suffix(".csv.part")
        temporary_path.unlink(missing_ok=True)
        try:
            LOGGER.info(
                "Downloading official Taxi Zone Lookup: source=%s target=%s",
                source_url,
                target_path,
            )
            with requests.get(source_url, stream=True, timeout=timeout_seconds) as response:
                response.raise_for_status()
                with temporary_path.open("wb") as output_file:
                    for chunk in response.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            output_file.write(chunk)
            validation = validate_taxi_zone_csv(temporary_path)
            os.replace(temporary_path, target_path)
        except (OSError, requests.RequestException) as exc:
            temporary_path.unlink(missing_ok=True)
            raise TaxiZoneReferenceError(
                f"Could not download Taxi Zone Lookup from {source_url}"
            ) from exc

    result = TaxiZoneReferenceResult(
        source_url=source_url,
        local_path=target_path,
        downloaded_at=datetime.now(UTC),
        file_size_bytes=target_path.stat().st_size,
        validation=validation,
        skipped_download=skipped_download,
        manifest_path=None,
    )
    manifest_path = _write_manifest(taxi_zone_manifest_path(reference_dir), result)
    result = TaxiZoneReferenceResult(
        source_url=result.source_url,
        local_path=result.local_path,
        downloaded_at=result.downloaded_at,
        file_size_bytes=result.file_size_bytes,
        validation=result.validation,
        skipped_download=result.skipped_download,
        manifest_path=manifest_path,
    )
    LOGGER.info(
        "Taxi Zone Lookup ready: rows=%s unique_location_ids=%s size_bytes=%s elapsed_seconds=%.2f",
        validation.row_count,
        validation.unique_location_ids,
        result.file_size_bytes,
        time.monotonic() - started_at,
    )
    return result


def load_taxi_zones(spark: SparkSession, path: Path) -> DataFrame:
    """Load authoritative lookup data with stable Spark-friendly names."""
    validate_taxi_zone_csv(path)
    dataframe = spark.read.option("header", True).csv(str(path))
    return dataframe.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and validate official TLC Taxi Zone Lookup data."
    )
    parser.add_argument("--reference-dir", type=Path, default=Path("data/reference"))
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        ingest_taxi_zones(reference_dir=args.reference_dir, force=args.force)
    except TaxiZoneReferenceError as exc:
        LOGGER.error("Taxi Zone Lookup ingestion failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
