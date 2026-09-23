"""Minimal local Superset configuration; all secrets come from environment variables."""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Superset requires environment setting {name}")
    return value


SECRET_KEY = required("SUPERSET_SECRET_KEY")
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{quote_plus(required('SUPERSET_DB_USER'))}:"
    f"{quote_plus(required('SUPERSET_DB_PASSWORD'))}@airflow-postgres:5432/"
    f"{quote_plus(required('SUPERSET_DB_NAME'))}"
)
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # Local loopback HTTP only; not a production security setting.
