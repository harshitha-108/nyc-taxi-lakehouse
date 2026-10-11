"""Execute validated analytical SQL with a separate, least-privilege DB login."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from chatbot.sql_guard import STATEMENT_TIMEOUT_MS, ValidatedQuery

LOCK_TIMEOUT_MS = 1000


class SqlExecutionError(RuntimeError):
    """A safe query could not be run without compromising the read boundary."""


@dataclass(frozen=True)
class QueryResult:
    mart: str
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    elapsed_ms: int


class PostgresQueryExecutor:
    """Use only CHAT_READONLY_DB_* credentials; never fall back to mart ownership.

    Database-side SELECT grants are still mandatory. The application also sets
    transaction read-only, a three-second statement timeout, a one-second lock
    timeout, and fetches no more than the validated result limit plus one row.
    """

    def __init__(self, *, host: str, port: int, dbname: str, user: str,
                 password: str) -> None:
        if not host or not dbname or not user or not password:
            raise SqlExecutionError("Dedicated read-only database credentials are required")
        if not 1 <= port <= 65535:
            raise SqlExecutionError("Invalid read-only database port")
        owner_user = os.environ.get("ANALYTICS_DB_USER", "analytics_dev")
        if user.casefold() in {owner_user.casefold(), "postgres"}:
            raise SqlExecutionError("Chat must not use the database owner or superuser")
        self._connect_kwargs = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "connect_timeout": 3,
            "application_name": "city_pulse_chat_readonly",
        }

    def __repr__(self) -> str:
        return "PostgresQueryExecutor(credentials=<redacted>)"

    @classmethod
    def from_env(cls) -> PostgresQueryExecutor:
        """Read explicit chat credentials; only host/database may reuse DB routing."""
        try:
            port = int(os.environ.get(
                "CHAT_READONLY_DB_PORT", os.environ.get("ANALYTICS_DB_PORT", "5432")
            ))
        except ValueError as exc:
            raise SqlExecutionError("Invalid read-only database port") from exc
        return cls(
            host=os.environ.get("CHAT_READONLY_DB_HOST")
            or os.environ.get("ANALYTICS_DB_HOST", "127.0.0.1"),
            port=port,
            dbname=os.environ.get("CHAT_READONLY_DB_NAME")
            or os.environ.get("ANALYTICS_DB_NAME", "nyc_taxi_analytics"),
            user=os.environ.get("CHAT_READONLY_DB_USER", "").strip(),
            password=os.environ.get("CHAT_READONLY_DB_PASSWORD", ""),
        )

    def execute(self, query: ValidatedQuery) -> QueryResult:
        if not isinstance(query, ValidatedQuery):
            raise TypeError("A ValidatedQuery from the SQL guard is required")
        if not 1 <= query.timeout_ms <= STATEMENT_TIMEOUT_MS:
            raise SqlExecutionError("Query timeout is outside the approved bound")
        try:
            import psycopg2
        except ImportError as exc:
            raise SqlExecutionError("PostgreSQL query mode needs psycopg2-binary") from exc

        started = time.monotonic()
        try:
            with psycopg2.connect(**self._connect_kwargs) as connection:
                connection.set_session(readonly=True)
                with connection.cursor() as cursor:
                    # Set within this transaction, before running the data query.
                    cursor.execute("SELECT set_config(%s, %s, true)",
                                   ("statement_timeout", str(query.timeout_ms)))
                    cursor.execute("SELECT set_config(%s, %s, true)",
                                   ("lock_timeout", str(LOCK_TIMEOUT_MS)))
                    cursor.execute(query.sql, query.params)
                    if cursor.description is None:
                        raise SqlExecutionError("Analytics query did not return a result")
                    columns = tuple(column.name for column in cursor.description)
                    if columns != query.columns:
                        raise SqlExecutionError("Analytics query result schema changed")
                    fetched = cursor.fetchmany(query.limit + 1)
                    if len(fetched) > query.limit:
                        raise SqlExecutionError("Analytics query exceeded its result limit")
                    rows = tuple(dict(zip(columns, row, strict=True)) for row in fetched)
        except psycopg2.Error as exc:
            # Never return driver diagnostics, SQL, or connection details to chat.
            raise SqlExecutionError("Read-only analytics query is unavailable") from exc
        return QueryResult(query.mart, columns, rows,
                           max(0, round((time.monotonic() - started) * 1000)))
