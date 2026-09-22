"""Idempotent downloader for official NYC TLC Yellow Taxi Parquet files."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

LOGGER = logging.getLogger(__name__)
TLC_TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SUPPORTED_TAXI_TYPES = frozenset({"yellow"})
DOWNLOAD_CHUNK_SIZE_BYTES = 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 30
MAX_DOWNLOAD_ATTEMPTS = 3


class IngestionError(Exception):
    """Base exception for a recoverable ingestion failure."""


class InvalidRequestError(IngestionError):
    """Raised when a requested dataset identifier is invalid."""


class ParquetValidationError(IngestionError):
    """Raised when a downloaded file cannot be read as Parquet."""


@dataclass(frozen=True)
class TaxiDataRequest:
    """Identifies one monthly TLC Taxi trip-record dataset."""

    taxi_type: str
    year: int
    month: int

    def __post_init__(self) -> None:
        if self.taxi_type not in SUPPORTED_TAXI_TYPES:
            supported = ", ".join(sorted(SUPPORTED_TAXI_TYPES))
            raise InvalidRequestError(
                f"Unsupported taxi type '{self.taxi_type}'. Supported: {supported}."
            )
        if not 2009 <= self.year <= 2100:
            raise InvalidRequestError("Year must be between 2009 and 2100.")
        if not 1 <= self.month <= 12:
            raise InvalidRequestError("Month must be between 1 and 12.")

    @property
    def filename(self) -> str:
        return f"{self.taxi_type}_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def source_url(self) -> str:
        return f"{TLC_TRIP_DATA_BASE_URL}/{self.filename}"


@dataclass(frozen=True)
class IngestionResult:
    """Result and lineage information for one ingestion attempt."""

    request: TaxiDataRequest
    local_path: Path
    metadata_path: Path | None
    file_size_bytes: int
    skipped: bool
    elapsed_seconds: float


def raw_data_path(request: TaxiDataRequest, destination_dir: Path) -> Path:
    """Return the partition-like local location for a raw TLC file."""
    return (
        destination_dir
        / request.taxi_type
        / str(request.year)
        / f"{request.month:02d}"
        / request.filename
    )


def metadata_path(request: TaxiDataRequest, destination_dir: Path) -> Path:
    """Return the manifest location parallel to raw source data."""
    return (
        destination_dir
        / "metadata"
        / request.taxi_type
        / str(request.year)
        / f"{request.month:02d}"
        / f"{request.filename.removesuffix('.parquet')}.json"
    )


def validate_parquet(file_path: Path) -> None:
    """Validate a non-empty Parquet file by reading its footer metadata only."""
    try:
        if not file_path.is_file():
            raise ParquetValidationError(f"File does not exist: {file_path}")
        if file_path.stat().st_size == 0:
            raise ParquetValidationError(f"Parquet file is empty: {file_path}")
        metadata = pq.ParquetFile(file_path).metadata
        if metadata is None:
            raise ParquetValidationError(f"Parquet metadata is unavailable: {file_path}")
    except (OSError, ValueError, pa.ArrowInvalid, pa.ArrowIOError) as exc:
        raise ParquetValidationError(f"File is not a readable Parquet file: {file_path}") from exc


def _download_chunks(response: requests.Response) -> Iterator[bytes]:
    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
        if chunk:
            yield chunk


def _download_to_partial_file(
    source_url: str,
    partial_path: Path,
    session: requests.Session,
) -> None:
    """Download one source object to a temporary file with bounded retries."""
    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        response: requests.Response | None = None
        try:
            LOGGER.info(
                "Downloading %s (attempt %s/%s)", source_url, attempt, MAX_DOWNLOAD_ATTEMPTS
            )
            response = session.get(source_url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS)
            response.raise_for_status()
            with partial_path.open("wb") as output_file:
                for chunk in _download_chunks(response):
                    output_file.write(chunk)
            return
        except (OSError, requests.RequestException) as exc:
            partial_path.unlink(missing_ok=True)
            if attempt == MAX_DOWNLOAD_ATTEMPTS:
                raise IngestionError(
                    f"Failed to download {source_url} after {MAX_DOWNLOAD_ATTEMPTS} attempts."
                ) from exc
            LOGGER.warning("Download attempt %s failed: %s", attempt, exc)
        finally:
            if response is not None:
                response.close()


def _write_manifest(
    request: TaxiDataRequest,
    source_path: Path,
    manifest_path: Path,
) -> None:
    """Write a small, atomic lineage manifest for a successful source download."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset": "nyc_tlc_trip_record_data",
        "taxi_type": request.taxi_type,
        "year": request.year,
        "month": request.month,
        "source_url": request.source_url,
        "local_path": str(source_path.resolve()),
        "filename": source_path.name,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "file_size_bytes": source_path.stat().st_size,
    }
    partial_manifest_path = manifest_path.with_suffix(".json.part")
    try:
        with partial_manifest_path.open("w", encoding="utf-8") as manifest_file:
            json.dump(manifest, manifest_file, indent=2, sort_keys=True)
            manifest_file.write("\n")
        os.replace(partial_manifest_path, manifest_path)
    except OSError as exc:
        partial_manifest_path.unlink(missing_ok=True)
        raise IngestionError(f"Failed to write lineage manifest: {manifest_path}") from exc


def ingest_taxi_data(
    request: TaxiDataRequest,
    destination_dir: Path = Path("data/raw"),
    *,
    force: bool = False,
    session: requests.Session | None = None,
) -> IngestionResult:
    """Download, validate, and record lineage for one official TLC Parquet file.

    A valid existing destination is skipped unless ``force`` is set. Downloads first write to a
    ``.part`` file, then atomically replace the final filename only after Parquet validation
    succeeds.
    """
    started_at = time.monotonic()
    final_path = raw_data_path(request, destination_dir)
    manifest_path = metadata_path(request, destination_dir)

    if final_path.exists() and not force:
        try:
            validate_parquet(final_path)
        except ParquetValidationError:
            LOGGER.warning("Existing file is invalid and will be replaced: %s", final_path)
        else:
            LOGGER.info("Skipping existing valid file: %s", final_path)
            return IngestionResult(
                request=request,
                local_path=final_path,
                metadata_path=manifest_path if manifest_path.exists() else None,
                file_size_bytes=final_path.stat().st_size,
                skipped=True,
                elapsed_seconds=time.monotonic() - started_at,
            )

    final_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = final_path.with_suffix(f"{final_path.suffix}.part")
    partial_path.unlink(missing_ok=True)
    active_session = session or requests.Session()

    try:
        _download_to_partial_file(request.source_url, partial_path, active_session)
        validate_parquet(partial_path)
        os.replace(partial_path, final_path)
        _write_manifest(request, final_path, manifest_path)
    except (OSError, IngestionError):
        partial_path.unlink(missing_ok=True)
        raise
    finally:
        if session is None:
            active_session.close()

    file_size = final_path.stat().st_size
    elapsed_seconds = time.monotonic() - started_at
    LOGGER.info(
        "Downloaded and validated %s (%s bytes) in %.2f seconds",
        final_path,
        file_size,
        elapsed_seconds,
    )
    return IngestionResult(
        request=request,
        local_path=final_path,
        metadata_path=manifest_path,
        file_size_bytes=file_size,
        skipped=False,
        elapsed_seconds=elapsed_seconds,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download one official NYC TLC Taxi Parquet dataset."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=sorted(SUPPORTED_TAXI_TYPES))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--destination-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--force", action="store_true", help="Replace an existing local file.")
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    return parser.parse_args()


def main() -> int:
    """Run the module command-line interface."""
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        request = TaxiDataRequest(taxi_type=args.taxi_type, year=args.year, month=args.month)
        result = ingest_taxi_data(request, destination_dir=args.destination_dir, force=args.force)
    except IngestionError as exc:
        LOGGER.error("Ingestion failed: %s", exc)
        return 1

    action = "Skipped" if result.skipped else "Downloaded"
    LOGGER.info("%s %s", action, result.local_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
