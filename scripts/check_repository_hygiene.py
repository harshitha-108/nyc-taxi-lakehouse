"""Fail CI when runtime material or a personal Power BI data path is tracked."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import PurePosixPath, PureWindowsPath

DATA_LAYERS = {"raw", "bronze", "silver", "gold", "reference", "quarantine", "state"}
RUNTIME_ROOTS = {"logs", "volumes", "warehouse", "spark-warehouse", "metastore_db",
                 "minio-data", "postgres-data"}
FORBIDDEN_SUFFIXES = {".parquet", ".part", ".db", ".sqlite", ".sqlite3", ".pem", ".p12"}
POWERBI_MODEL = "powerbi/NYC Taxi - City Pulse Showcase.SemanticModel/model.bim"
CSV_ROOT_PLACEHOLDER = r"C:\path\to\nyc-taxi-lakehouse\data\gold\powerbi"


def forbidden_path(name: str) -> bool:
    """Allow versioned placeholders/configuration but reject tracked runtime material."""
    path = PurePosixPath(name.replace("\\", "/"))
    parts = tuple(part.lower() for part in path.parts)
    if not parts:
        return False
    if parts[0] == "data" and len(parts) >= 3 and parts[1] in DATA_LAYERS:
        return parts != ("data", parts[1], ".gitkeep")
    if parts[0] in RUNTIME_ROOTS or parts[:2] == ("airflow", "logs"):
        return True
    if path.name.lower() == ".env.example":
        return False
    if path.name.lower() == ".env" or path.name.lower().startswith(".env."):
        return True
    if path.suffix.lower() in FORBIDDEN_SUFFIXES or path.name.lower() == "_success":
        return True
    return path.name.lower() in {"id_rsa", "id_ed25519", "derby.log"}


def personal_csv_root_in_model(contents: bytes) -> bool:
    """Check the Power Query parameter in a staged model, on any host OS."""
    model = json.loads(contents)
    expressions = model["model"]["expressions"]
    parameter = next(
        expression for expression in expressions if expression.get("name") == "CsvRootPath"
    )
    match = re.match(r'^\s*"((?:[^"]|"")*)"\s+meta\b', parameter["expression"])
    if match is None:
        raise ValueError("CsvRootPath is not a literal Power Query text parameter")
    path = match.group(1).replace('""', '"')
    return path != CSV_ROOT_PLACEHOLDER and (
        PureWindowsPath(path).is_absolute() or PurePosixPath(path).is_absolute()
    )


def main() -> int:
    """Inspect Git's index, not merely the working directory."""
    result = subprocess.run(["git", "ls-files", "-z"], capture_output=True, check=True)
    tracked = [entry.decode("utf-8") for entry in result.stdout.split(b"\0") if entry]
    bad = sorted(name for name in tracked if forbidden_path(name))
    if bad:
        print("Forbidden tracked paths:\n" + "\n".join(bad))
        return 1
    if POWERBI_MODEL in tracked:
        staged_model = subprocess.run(
            ["git", "show", f":{POWERBI_MODEL}"], capture_output=True, check=True
        ).stdout
        try:
            if personal_csv_root_in_model(staged_model):
                print(f"{POWERBI_MODEL}: CsvRootPath contains a local absolute path.")
                return 1
        except (AttributeError, KeyError, StopIteration, TypeError, ValueError) as exc:
            print(f"{POWERBI_MODEL}: cannot validate CsvRootPath ({exc}).")
            return 1
    print("Repository hygiene: no forbidden runtime paths or personal Power BI CSV roots.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
