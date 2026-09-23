"""Fail CI when generated datasets, local state, or obvious secret files are tracked."""

from __future__ import annotations

import subprocess
from pathlib import PurePosixPath

DATA_LAYERS = {"raw", "bronze", "silver", "gold", "reference", "quarantine", "state"}
RUNTIME_ROOTS = {"logs", "volumes", "warehouse", "spark-warehouse", "metastore_db",
                 "minio-data", "postgres-data"}
FORBIDDEN_SUFFIXES = {".parquet", ".part", ".db", ".sqlite", ".sqlite3", ".pem", ".p12"}


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


def main() -> int:
    """Inspect Git's index, not merely the working directory."""
    result = subprocess.run(["git", "ls-files", "-z"], capture_output=True, check=True)
    tracked = (entry.decode("utf-8") for entry in result.stdout.split(b"\0") if entry)
    bad = sorted(name for name in tracked if forbidden_path(name))
    if bad:
        print("Forbidden tracked paths:\n" + "\n".join(bad))
        return 1
    print("Repository hygiene: tracked files contain no forbidden runtime paths.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
