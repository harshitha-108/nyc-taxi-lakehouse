"""Environment-backed analytics database settings."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ServingConfig:
    """Connection settings and an isolated serving schema."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "analytics"

    def __post_init__(self) -> None:
        if not self.host or not self.database or not self.user or not self.password:
            raise ValueError("Analytics database host, name, user, and password are required.")
        if not 1 <= self.port <= 65535:
            raise ValueError("Analytics database port must be between 1 and 65535.")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.schema):
            raise ValueError("Serving schema must be a simple lowercase SQL identifier.")

    @classmethod
    def from_env(cls) -> ServingConfig:
        """Reject absent settings instead of connecting to an implicit database."""
        names = (
            "ANALYTICS_DB_HOST", "ANALYTICS_DB_PORT", "ANALYTICS_DB_NAME",
            "ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD",
        )
        missing = [name for name in names if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Missing analytics database configuration: {', '.join(missing)}")
        return cls(
            host=os.environ[names[0]], port=int(os.environ[names[1]]),
            database=os.environ[names[2]], user=os.environ[names[3]],
            password=os.environ[names[4]],
        )

    def connect_kwargs(self) -> dict[str, object]:
        """Arguments for psycopg2.connect, never included in operational logs."""
        return {"host": self.host, "port": self.port, "dbname": self.database,
                "user": self.user, "password": self.password, "connect_timeout": 10}
