"""Explicit, backward-compatible storage configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


class StorageConfigError(ValueError):
    """Raised for incomplete or unsupported storage configuration."""


@dataclass(frozen=True)
class StorageConfig:
    backend: str = "filesystem"
    endpoint: str = ""
    bucket: str = "nyc-taxi-lakehouse"
    access_key: str = field(default="", repr=False)
    secret_key: str = field(default="", repr=False)
    catalog_path: Path = Path("data/state/iceberg_catalog.db")

    @classmethod
    def from_env(cls, backend: str | None = None) -> StorageConfig:
        """Read local storage settings, requiring credentials only for Iceberg."""
        selected = backend or os.getenv("STORAGE_BACKEND", "filesystem")
        if selected not in {"filesystem", "iceberg"}:
            raise StorageConfigError(f"Unsupported storage backend: {selected}")
        config = cls(
            backend=selected,
            endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            bucket=os.getenv("MINIO_BUCKET", "nyc-taxi-lakehouse"),
            access_key=os.getenv("MINIO_ROOT_USER", ""),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", ""),
            catalog_path=Path(os.getenv("ICEBERG_CATALOG_PATH", "data/state/iceberg_catalog.db")),
        )
        if selected == "iceberg" and not all(
            (config.endpoint, config.bucket, config.access_key, config.secret_key)
        ):
            raise StorageConfigError("Iceberg requires MinIO endpoint, bucket, user and password.")
        return config

    @property
    def warehouse(self) -> str:
        """Return the Iceberg object-storage warehouse URI."""
        return f"s3a://{self.bucket}/warehouse"

    @property
    def jdbc_uri(self) -> str:
        """Return the local, ignored SQLite catalog location."""
        return f"jdbc:sqlite:{self.catalog_path.resolve().as_posix()}"
