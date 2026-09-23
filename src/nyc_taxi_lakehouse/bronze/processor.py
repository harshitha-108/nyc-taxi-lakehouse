"""Idempotent, source-preserving Bronze processing for NYC TLC Taxi data."""

from __future__ import annotations

import argparse
import logging
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from nyc_taxi_lakehouse.ingestion.nyc_taxi import (
    InvalidRequestError,
    TaxiDataRequest,
    raw_data_path,
)
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.spark import create_spark_session as shared_spark_session

LOGGER = logging.getLogger(__name__)
LINEAGE_COLUMNS = (
    "_bronze_ingested_at",
    "_source_file",
    "_source_taxi_type",
    "_source_year",
    "_source_month",
)


class BronzeError(Exception):
    """Raised when Bronze processing cannot complete safely."""


@dataclass(frozen=True)
class BronzeRequest:
    """Identifies the raw source period and Bronze partition to process."""

    taxi_type: str
    year: int
    month: int

    def __post_init__(self) -> None:
        TaxiDataRequest(taxi_type=self.taxi_type, year=self.year, month=self.month)

    @property
    def source_request(self) -> TaxiDataRequest:
        return TaxiDataRequest(taxi_type=self.taxi_type, year=self.year, month=self.month)


@dataclass(frozen=True)
class BronzeResult:
    """Operational metrics from a completed Bronze partition write."""

    source_path: Path
    target_path: Path
    source_rows: int
    source_columns: int
    bronze_rows: int
    bronze_columns: int
    output_file_count: int
    elapsed_seconds: float


def bronze_partition_path(request: BronzeRequest, bronze_dir: Path) -> Path:
    """Return the Hive-style year/month directory for one Bronze source period."""
    return (
        bronze_dir
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}"
    )


def create_spark_session(app_name: str = "nyc-taxi-bronze") -> SparkSession:
    """Create the local Spark session used by the Bronze job."""
    return shared_spark_session(app_name, StorageConfig.from_env("filesystem"))


def add_lineage_columns(
    source_dataframe: DataFrame,
    request: BronzeRequest,
    source_path: Path,
    ingested_at: datetime,
) -> DataFrame:
    """Append only technical lineage fields while retaining every source column unchanged."""
    if ingested_at.tzinfo is None:
        raise BronzeError("Bronze ingestion timestamp must be timezone-aware.")
    return (
        source_dataframe.withColumn(
            "_bronze_ingested_at", F.lit(ingested_at).cast(TimestampType())
        )
        .withColumn("_source_file", F.lit(source_path.name))
        .withColumn("_source_taxi_type", F.lit(request.taxi_type))
        .withColumn("_source_year", F.lit(request.year))
        .withColumn("_source_month", F.lit(request.month))
    )


def _temporary_partition_path(target_path: Path, label: str) -> Path:
    """Create a sibling path so a failed write cannot look like a completed partition."""
    return target_path.with_name(f"{target_path.name}.__{label}__{uuid.uuid4().hex}")


def _replace_partition(temporary_path: Path, target_path: Path) -> None:
    """Atomically replace one completed partition while retaining a rollback path on failure."""
    backup_path: Path | None = None
    try:
        if target_path.exists():
            backup_path = _temporary_partition_path(target_path, "backup")
            target_path.replace(backup_path)
        temporary_path.replace(target_path)
    except OSError as exc:
        if backup_path is not None and backup_path.exists() and not target_path.exists():
            backup_path.replace(target_path)
        raise BronzeError(f"Unable to promote Bronze partition: {target_path}") from exc
    else:
        if backup_path is not None:
            shutil.rmtree(backup_path)


def _remove_directory_if_present(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _validate_bronze_output(
    bronze_dataframe: DataFrame,
    *,
    source_columns: list[str],
    source_rows: int,
    request: BronzeRequest,
    source_filename: str,
) -> int:
    """Validate source preservation and required technical lineage after a temporary write."""
    bronze_columns = bronze_dataframe.columns
    missing_source_columns = set(source_columns).difference(bronze_columns)
    missing_lineage_columns = set(LINEAGE_COLUMNS).difference(bronze_columns)
    if missing_source_columns:
        raise BronzeError(
            f"Bronze output is missing source columns: {sorted(missing_source_columns)}"
        )
    if missing_lineage_columns:
        raise BronzeError(
            f"Bronze output is missing lineage columns: {sorted(missing_lineage_columns)}"
        )

    bronze_rows = bronze_dataframe.count()
    if bronze_rows == 0:
        raise BronzeError("Bronze output is empty.")
    if bronze_rows != source_rows:
        raise BronzeError(f"Row-count mismatch: source={source_rows}, bronze={bronze_rows}.")

    invalid_lineage_rows = bronze_dataframe.filter(
        (F.col("_source_file") != source_filename)
        | (F.col("_source_taxi_type") != request.taxi_type)
        | (F.col("_source_year") != request.year)
        | (F.col("_source_month") != request.month)
        | F.col("_bronze_ingested_at").isNull()
    ).count()
    if invalid_lineage_rows:
        raise BronzeError(f"Bronze output has {invalid_lineage_rows} rows with invalid lineage.")
    return bronze_rows


def write_bronze_partition(
    spark: SparkSession,
    request: BronzeRequest,
    *,
    raw_dir: Path = Path("data/raw"),
    bronze_dir: Path = Path("data/bronze"),
) -> BronzeResult:
    """Create or safely replace one source-aligned Bronze year/month partition."""
    started_at = time.monotonic()
    source_path = raw_data_path(request.source_request, raw_dir)
    target_path = bronze_partition_path(request, bronze_dir)
    if not source_path.is_file():
        raise BronzeError(f"Raw source file does not exist: {source_path}")

    LOGGER.info(
        "Bronze job started: taxi_type=%s year=%s month=%02d input=%s output=%s",
        request.taxi_type,
        request.year,
        request.month,
        source_path,
        target_path,
    )
    source_dataframe = spark.read.parquet(str(source_path))
    source_columns = source_dataframe.columns
    source_rows = source_dataframe.count()
    if source_rows == 0:
        raise BronzeError(f"Raw source file is empty: {source_path}")

    run_timestamp = datetime.now(UTC)
    bronze_dataframe = add_lineage_columns(source_dataframe, request, source_path, run_timestamp)
    temporary_path = _temporary_partition_path(target_path, "temporary")
    _remove_directory_if_present(temporary_path)

    try:
        # One file is appropriate for the current ~50 MB local monthly development source.
        bronze_dataframe.coalesce(1).write.mode("overwrite").parquet(str(temporary_path))
        readback_dataframe = spark.read.parquet(str(temporary_path))
        bronze_rows = _validate_bronze_output(
            readback_dataframe,
            source_columns=source_columns,
            source_rows=source_rows,
            request=request,
            source_filename=source_path.name,
        )
        _replace_partition(temporary_path, target_path)
    except Exception:
        _remove_directory_if_present(temporary_path)
        raise

    output_file_count = len(list(target_path.glob("part-*.parquet")))
    elapsed_seconds = time.monotonic() - started_at
    LOGGER.info(
        "Bronze job succeeded: input_rows=%s output_rows=%s source_columns=%s "
        "output_columns=%s output_files=%s elapsed_seconds=%.2f",
        source_rows,
        bronze_rows,
        len(source_columns),
        len(readback_dataframe.columns),
        output_file_count,
        elapsed_seconds,
    )
    return BronzeResult(
        source_path=source_path,
        target_path=target_path,
        source_rows=source_rows,
        source_columns=len(source_columns),
        bronze_rows=bronze_rows,
        bronze_columns=len(readback_dataframe.columns),
        output_file_count=output_file_count,
        elapsed_seconds=elapsed_seconds,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create one source-aligned NYC Taxi Bronze partition."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    return parser.parse_args()


def main() -> int:
    """Run the Bronze command-line interface."""
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        request = BronzeRequest(taxi_type=args.taxi_type, year=args.year, month=args.month)
        spark = create_spark_session()
        try:
            write_bronze_partition(
                spark,
                request,
                raw_dir=args.raw_dir,
                bronze_dir=args.bronze_dir,
            )
        finally:
            spark.stop()
    except (BronzeError, InvalidRequestError) as exc:
        LOGGER.error("Bronze job failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
