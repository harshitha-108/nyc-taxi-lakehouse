"""Idempotently provision distinct local PostgreSQL analytics and Superset databases."""

from __future__ import annotations

import logging
import os
from contextlib import closing

import psycopg2
from psycopg2 import sql

LOGGER = logging.getLogger(__name__)


def required(name: str) -> str:
    """Reject missing connection settings without printing their values."""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required database setting: {name}")
    return value


def main() -> None:
    """Create local service roles and owned databases without touching Airflow tables."""
    logging.basicConfig(level="INFO")
    with closing(psycopg2.connect(
        host="airflow-postgres", port=5432, dbname="postgres",
        user=required("POSTGRES_USER"), password=required("POSTGRES_PASSWORD"),
        connect_timeout=10,
    )) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for db_name, user_name, password in (
                (required("ANALYTICS_DB_NAME"), required("ANALYTICS_DB_USER"),
                 required("ANALYTICS_DB_PASSWORD")),
                (required("SUPERSET_DB_NAME"), required("SUPERSET_DB_USER"),
                 required("SUPERSET_DB_PASSWORD")),
            ):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (user_name,))
                if cursor.fetchone() is None:
                    cursor.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(user_name), sql.Literal(password)))
                cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
                if cursor.fetchone() is None:
                    cursor.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(db_name), sql.Identifier(user_name)))
                LOGGER.info("Local database ready: %s (owner role: %s)", db_name, user_name)


if __name__ == "__main__":
    main()
