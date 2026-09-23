"""Metadata-only Parquet contract validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, raw_data_path
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod, period_range


class Compatibility(StrEnum):
    COMPATIBLE = "COMPATIBLE"
    WARNING = "WARNING"
    BREAKING = "BREAKING"


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    nullable: bool
    required: bool = True


@dataclass(frozen=True)
class Contract:
    contract_name: str
    version: int
    taxi_type: str
    fields: tuple[Field, ...]


def fingerprint(fields: tuple[Field, ...]) -> str:
    logical = sorted((f.name, f.type, f.nullable) for f in fields)
    return hashlib.sha256(json.dumps(logical, separators=(",", ":")).encode()).hexdigest()


def load_contract(path: Path = Path("configs/contracts/yellow_taxi.json")) -> Contract:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict) or not isinstance(doc.get("fields"), list):
            raise ValueError("Contract must contain a field list.")
        fields = tuple(Field(**field) for field in doc["fields"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid Yellow Taxi schema contract: {path}") from exc
    valid = (
        isinstance(doc.get("contract_name"), str)
        and bool(doc["contract_name"].strip())
        and type(doc.get("version")) is int
        and doc["version"] >= 1
        and doc.get("taxi_type") == "yellow"
    )
    valid = valid and fields and len({field.name for field in fields}) == len(fields)
    valid = valid and all(
        isinstance(field.name, str) and bool(field.name.strip())
        and isinstance(field.type, str) and bool(field.type.strip())
        and type(field.required) is bool and type(field.nullable) is bool
        for field in fields
    )
    if not valid:
        raise ValueError("Invalid Yellow Taxi schema contract.")
    try:
        for field in fields:
            pa.type_for_alias(field.type)
    except ValueError as exc:
        raise ValueError("Invalid Yellow Taxi schema contract type.") from exc
    return Contract(doc["contract_name"], doc["version"], doc["taxi_type"], fields)


def inspect(path: Path) -> tuple[Field, ...]:
    return tuple(Field(f.name, str(f.type), f.nullable) for f in pq.ParquetFile(path).schema_arrow)


def validate(
    path: Path, period: ProcessingPeriod, contract: Contract | None = None
) -> dict[str, object]:
    contract = contract or load_contract()
    actual = inspect(path)
    expected = {f.name: f for f in contract.fields}
    observed = {f.name: f for f in actual}
    changes = []
    level = Compatibility.COMPATIBLE
    for name, field in expected.items():
        value = observed.get(name)
        if value is None:
            changes.append({"type": "REMOVED_COLUMN", "field": name})
            if field.required:
                level = Compatibility.BREAKING
            elif level != Compatibility.BREAKING:
                level = Compatibility.WARNING
            continue
        if value.type != field.type:
            changes.append(
                {
                    "type": "TYPE_CHANGED",
                    "field": name,
                    "expected": field.type,
                    "actual": value.type,
                }
            )
            level = Compatibility.BREAKING
        if value.nullable != field.nullable:
            changes.append(
                {
                    "type": "NULLABILITY_CHANGED",
                    "field": name,
                    "expected": field.nullable,
                    "actual": value.nullable,
                }
            )
            if field.required and not field.nullable and value.nullable:
                level = Compatibility.BREAKING
            elif level != Compatibility.BREAKING:
                level = Compatibility.WARNING
    for name, value in observed.items():
        if name not in expected:
            changes.append({"type": "ADDED_COLUMN", "field": name, "nullable": value.nullable})
            if not value.nullable and level == Compatibility.COMPATIBLE:
                level = Compatibility.WARNING
    return {
        "contract_name": contract.contract_name,
        "contract_version": contract.version,
        "period": period.identifier,
        "expected_fingerprint": fingerprint(contract.fields),
        "actual_fingerprint": fingerprint(actual),
        "schema_match": not changes,
        "compatibility": level,
        "changes": changes,
        "decision": "BLOCK" if level == Compatibility.BREAKING else "PASS",
        "field_count": len(actual),
    }


def validate_and_report(
    path: Path,
    period: ProcessingPeriod,
    taxi_type: str = "yellow",
    state_dir: Path = Path("data/state"),
    contract: Contract | None = None,
) -> dict[str, object]:
    """Validate a source schema and atomically persist an ignored audit record."""
    started = time.monotonic()
    report = validate(path, period, contract)
    report["taxi_type"] = taxi_type
    report["validation_timestamp"] = datetime.now(UTC).isoformat()
    report["validation_duration_seconds"] = round(time.monotonic() - started, 6)
    target = (
        state_dir
        / "schema"
        / taxi_type
        / str(period.year)
        / f"{period.month:02d}"
        / "schema_validation.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".json.part")
    temporary.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    periods = (
        period_range(ProcessingPeriod.parse(args.start), ProcessingPeriod.parse(args.end))
        if args.start and args.end
        else [ProcessingPeriod(args.year, args.month)]
    )
    for period in periods:
        print(
            json.dumps(
                validate_and_report(
                    raw_data_path(
                        TaxiDataRequest(args.taxi_type, period.year, period.month), Path("data/raw")
                    ),
                    period,
                ),
                default=str,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
