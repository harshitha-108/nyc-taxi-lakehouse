"""Local unit tests for official Taxi Zone reference validation."""

from pathlib import Path

import pytest

from nyc_taxi_lakehouse.reference.taxi_zones import (
    TaxiZoneReferenceError,
    ingest_taxi_zones,
    validate_taxi_zone_csv,
)


def write_csv(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def valid_csv(path: Path) -> Path:
    return write_csv(
        path,
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Alpha,Boro Zone\n"
        "2,Queens,Beta,Yellow Zone\n",
    )


def test_valid_reference_csv_returns_metrics(tmp_path: Path) -> None:
    result = validate_taxi_zone_csv(valid_csv(tmp_path / "zones.csv"))
    assert result.row_count == 2
    assert result.unique_location_ids == 2
    assert result.blank_service_zones == 0


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("LocationID,Borough,Zone\n1,Manhattan,Alpha\n", "missing columns"),
        (
            "LocationID,Borough,Zone,service_zone\n1,Manhattan,Alpha,x\n1,Queens,Beta,y\n",
            "duplicate_location_ids=1",
        ),
        (
            "LocationID,Borough,Zone,service_zone\nx,Manhattan,Alpha,x\n",
            "invalid_location_ids=1",
        ),
        ("LocationID,Borough,Zone,service_zone\n", "contains no data rows"),
    ],
)
def test_invalid_reference_csv_is_rejected(tmp_path: Path, content: str, message: str) -> None:
    with pytest.raises(TaxiZoneReferenceError, match=message):
        validate_taxi_zone_csv(write_csv(tmp_path / "zones.csv", content))


def test_existing_valid_reference_is_idempotently_skipped(tmp_path: Path) -> None:
    target = tmp_path / "taxi_zones" / "taxi_zone_lookup.csv"
    target.parent.mkdir()
    valid_csv(target)
    result = ingest_taxi_zones(reference_dir=tmp_path)
    assert result.skipped_download is True
    assert result.validation.row_count == 2
    assert result.manifest_path is not None
