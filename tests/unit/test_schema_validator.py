"""Schema evolution compatibility and logical-fingerprint tests."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.schema.validator import (
    Compatibility,
    Contract,
    Field,
    fingerprint,
    load_contract,
    validate,
    validate_and_report,
)


def _contract() -> Contract:
    return Contract(
        "test",
        1,
        "yellow",
        (Field("trip_distance", "double", True), Field("vendor", "int32", True)),
    )


def _validate(tmp_path: Path, fields: list[pa.Field]):
    path = tmp_path / "source.parquet"
    pq.write_table(
        pa.Table.from_arrays([pa.array([], type=f.type) for f in fields], schema=pa.schema(fields)),
        path,
    )
    return validate(path, ProcessingPeriod(2024, 1), _contract())


def test_exact_added_and_removed_schema_changes(tmp_path: Path) -> None:
    exact = _validate(
        tmp_path, [pa.field("vendor", pa.int32()), pa.field("trip_distance", pa.float64())]
    )
    assert exact["compatibility"] == Compatibility.COMPATIBLE
    added = _validate(
        tmp_path,
        [
            pa.field("vendor", pa.int32()),
            pa.field("trip_distance", pa.float64()),
            pa.field("extra", pa.string()),
        ],
    )
    assert added["compatibility"] == Compatibility.COMPATIBLE
    assert {c["type"] for c in added["changes"]} == {"ADDED_COLUMN"}
    removed = _validate(tmp_path, [pa.field("vendor", pa.int32())])
    assert removed["decision"] == "BLOCK"


def test_type_nullability_and_fingerprint_policy(tmp_path: Path) -> None:
    changed = _validate(
        tmp_path, [pa.field("vendor", pa.int32()), pa.field("trip_distance", pa.string())]
    )
    assert changed["compatibility"] == Compatibility.BREAKING
    restrictive = _validate(
        tmp_path,
        [pa.field("vendor", pa.int32(), nullable=False), pa.field("trip_distance", pa.float64())],
    )
    assert restrictive["compatibility"] == Compatibility.WARNING
    first = (Field("a", "int32", True), Field("b", "double", True))
    assert fingerprint(first) == fingerprint(tuple(reversed(first)))
    assert fingerprint(first) != fingerprint(
        (Field("a", "int64", True), Field("b", "double", True))
    )


def test_permissive_nullability_and_multiple_changes_are_breaking(tmp_path: Path) -> None:
    strict_contract = Contract("test", 1, "yellow", (Field("required", "int32", False),))
    path = tmp_path / "source.parquet"
    pq.write_table(pa.table({"required": pa.array([None], type=pa.int32())}), path)
    assert (
        validate(path, ProcessingPeriod(2024, 1), strict_contract)["compatibility"]
        == Compatibility.BREAKING
    )
    multiple = _validate(
        tmp_path,
        [
            pa.field("trip_distance", pa.string()),
            pa.field("extra", pa.string()),
        ],
    )
    assert multiple["decision"] == "BLOCK"
    assert {change["type"] for change in multiple["changes"]} == {
        "ADDED_COLUMN",
        "REMOVED_COLUMN",
        "TYPE_CHANGED",
    }


def test_optional_removal_cannot_downgrade_breaking_change(tmp_path: Path) -> None:
    path = tmp_path / "mixed.parquet"
    fields = [
        pa.field("trip_distance", pa.string()),
        pa.field("vendor", pa.int32(), nullable=False),
        pa.field("extra", pa.string()),
    ]
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in fields], schema=pa.schema(fields)
        ),
        path,
    )
    for expected_fields in (
        (
            Field("required_missing", "int32", True),
            Field("trip_distance", "double", True),
            Field("vendor", "int32", True),
            Field("optional_missing", "int32", True, required=False),
        ),
        (
            Field("optional_missing", "int32", True, required=False),
            Field("vendor", "int32", True),
            Field("trip_distance", "double", True),
            Field("required_missing", "int32", True),
        ),
    ):
        contract = Contract("test", 1, "yellow", expected_fields)
        result = validate(path, ProcessingPeriod(2024, 1), contract)
        assert {change["type"] for change in result["changes"]} == {
            "ADDED_COLUMN",
            "REMOVED_COLUMN",
            "TYPE_CHANGED",
            "NULLABILITY_CHANGED",
        }
        assert result["compatibility"] == Compatibility.BREAKING
        assert result["decision"] == "BLOCK"


def test_atomic_runtime_report_contains_audit_fields(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    pq.write_table(
        pa.table(
            {
                "vendor": pa.array([], type=pa.int32()),
                "trip_distance": pa.array([], type=pa.float64()),
            }
        ),
        path,
    )
    report = validate_and_report(
        path, ProcessingPeriod(2024, 1), state_dir=tmp_path / "state", contract=_contract()
    )
    target = tmp_path / "state/schema/yellow/2024/01/schema_validation.json"
    assert target.is_file() and not list(target.parent.glob("*.part"))
    assert report["decision"] == "PASS"
    assert isinstance(report["validation_duration_seconds"], float)


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"contract_name": "x", "version": 0, "taxi_type": "yellow", "fields": []},
        {
            "contract_name": "x",
            "version": 1,
            "taxi_type": "yellow",
            "fields": [
                {"name": "x", "type": "int32", "required": True, "nullable": True},
                {"name": "x", "type": "int32", "required": True, "nullable": True},
            ],
        },
        {
            "contract_name": "x",
            "version": 1,
            "taxi_type": "yellow",
            "fields": [{"name": "x", "type": "int32", "required": "yes", "nullable": True}],
        },
    ],
)
def test_invalid_contracts_fail_closed(tmp_path: Path, document: dict[str, object]) -> None:
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        load_contract(path)


@pytest.mark.parametrize("change", [
    {"name": "", "type": "int32", "required": True, "nullable": True},
    {"name": "x", "type": "not_a_type", "required": True, "nullable": True},
    {"name": "x", "type": "int32", "required": "true", "nullable": True},
    {"name": "x", "type": "int32", "required": True, "nullable": "false"},
])
def test_malformed_field_contract_fails_closed(tmp_path: Path, change: dict) -> None:
    path = tmp_path / "contract.json"
    path.write_text(json.dumps({"contract_name": "test", "version": 1,
                                "taxi_type": "yellow", "fields": [change]}), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        load_contract(path)


@pytest.mark.parametrize("contents", ["{not json", "[]", "null"])
def test_malformed_json_and_wrong_document_shape_fail_closed(
    tmp_path: Path, contents: str
) -> None:
    path = tmp_path / "contract.json"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        load_contract(path)


def test_missing_contract_and_boolean_version_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Invalid"):
        load_contract(tmp_path / "missing.json")
    path = tmp_path / "contract.json"
    path.write_text(json.dumps({"contract_name": "test", "version": True,
                                "taxi_type": "yellow", "fields": [
                                    {"name": "x", "type": "int32", "required": True,
                                     "nullable": True}]}), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        load_contract(path)


@pytest.mark.parametrize("reverse", [False, True])
def test_breaking_severity_is_order_independent_amid_warnings(
    tmp_path: Path, reverse: bool
) -> None:
    path = tmp_path / "source.parquet"
    source_schema = pa.schema([pa.field("wrong_type", pa.string()),
                               pa.field("narrower", pa.int32(), nullable=False)])
    pq.write_table(pa.Table.from_arrays([pa.array(["a"]), pa.array([1])],
                                        schema=source_schema), path)
    fields = [Field("wrong_type", "int32", True),
              Field("narrower", "int32", True),
              Field("optional_missing", "int32", True, required=False)]
    if reverse:
        fields.reverse()
    result = validate(path, ProcessingPeriod(2024, 1), Contract(
        "test", 1, "yellow", tuple(fields)))
    assert result["compatibility"] == Compatibility.BREAKING
    assert result["decision"] == "BLOCK"
    assert {item["type"] for item in result["changes"]} == {
        "TYPE_CHANGED", "NULLABILITY_CHANGED", "REMOVED_COLUMN",
    }
