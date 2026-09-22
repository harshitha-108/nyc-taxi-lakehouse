"""Unit tests for the NYC TLC Taxi downloader."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from nyc_taxi_lakehouse.ingestion.nyc_taxi import (
    IngestionError,
    InvalidRequestError,
    TaxiDataRequest,
    ingest_taxi_data,
    metadata_path,
    raw_data_path,
    validate_parquet,
)


class FakeResponse:
    """Minimal requests response double for deterministic downloader tests."""

    def __init__(self, payload: bytes, status_error: Exception | None = None) -> None:
        self.payload = payload
        self.status_error = status_error
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_error:
            raise self.status_error

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self.payload

    def close(self) -> None:
        self.closed = True


class FakeSession:
    """Minimal session double that records requested URLs."""

    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str, **_: object) -> FakeResponse:
        self.urls.append(url)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.fixture
def taxi_request() -> TaxiDataRequest:
    return TaxiDataRequest(taxi_type="yellow", year=2024, month=1)


@pytest.fixture
def parquet_bytes(tmp_path: Path) -> bytes:
    parquet_path = tmp_path / "fixture.parquet"
    pq.write_table(pa.table({"trip_id": [1, 2]}), parquet_path)
    return parquet_path.read_bytes()


def test_request_constructs_official_filename_and_url(taxi_request: TaxiDataRequest) -> None:
    assert taxi_request.filename == "yellow_tripdata_2024-01.parquet"
    assert taxi_request.source_url == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )


@pytest.mark.parametrize("month", [0, 13])
def test_request_rejects_invalid_month(month: int) -> None:
    with pytest.raises(InvalidRequestError, match="Month"):
        TaxiDataRequest(taxi_type="yellow", year=2024, month=month)


def test_destination_paths_are_partitioned_by_taxi_type_year_and_month(
    taxi_request: TaxiDataRequest, tmp_path: Path
) -> None:
    assert raw_data_path(taxi_request, tmp_path) == (
        tmp_path / "yellow" / "2024" / "01" / taxi_request.filename
    )
    assert metadata_path(taxi_request, tmp_path) == (
        tmp_path / "metadata" / "yellow" / "2024" / "01" / "yellow_tripdata_2024-01.json"
    )


def test_existing_valid_file_is_skipped(
    taxi_request: TaxiDataRequest, tmp_path: Path, parquet_bytes: bytes
) -> None:
    existing_file = raw_data_path(taxi_request, tmp_path)
    existing_file.parent.mkdir(parents=True)
    existing_file.write_bytes(parquet_bytes)
    session = FakeSession(FakeResponse(parquet_bytes))

    result = ingest_taxi_data(taxi_request, destination_dir=tmp_path, session=session)  # type: ignore[arg-type]

    assert result.skipped is True
    assert session.urls == []


def test_force_replaces_file_and_writes_manifest(
    taxi_request: TaxiDataRequest, tmp_path: Path, parquet_bytes: bytes
) -> None:
    session = FakeSession(FakeResponse(parquet_bytes))

    result = ingest_taxi_data(
        taxi_request, destination_dir=tmp_path, force=True, session=session
    )  # type: ignore[arg-type]

    assert result.skipped is False
    assert session.urls == [taxi_request.source_url]
    validate_parquet(result.local_path)
    assert result.metadata_path is not None
    manifest = json.loads(result.metadata_path.read_text(encoding="utf-8"))
    assert manifest["source_url"] == taxi_request.source_url
    assert manifest["file_size_bytes"] == result.file_size_bytes


def test_failed_download_removes_partial_file(
    taxi_request: TaxiDataRequest, tmp_path: Path
) -> None:
    session = FakeSession(requests.ConnectionError("network unavailable"))
    expected_file = raw_data_path(taxi_request, tmp_path)

    with pytest.raises(IngestionError, match="Failed to download"):
        ingest_taxi_data(taxi_request, destination_dir=tmp_path, session=session)  # type: ignore[arg-type]

    assert not expected_file.with_suffix(".parquet.part").exists()
    assert not expected_file.exists()


def test_invalid_download_is_rejected_and_not_promoted(
    taxi_request: TaxiDataRequest, tmp_path: Path
) -> None:
    session = FakeSession(FakeResponse(b""))
    expected_file = raw_data_path(taxi_request, tmp_path)

    with pytest.raises(IngestionError, match="Parquet file is empty"):
        ingest_taxi_data(taxi_request, destination_dir=tmp_path, session=session)  # type: ignore[arg-type]

    assert not expected_file.with_suffix(".parquet.part").exists()
    assert not expected_file.exists()
