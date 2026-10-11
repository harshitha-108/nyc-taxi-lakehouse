"""No live DB is needed to verify the read-only query execution contract."""

from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

from chatbot.sql_execution import PostgresQueryExecutor, SqlExecutionError
from chatbot.sql_guard import validate_analytic_sql


class _DriverError(Exception):
    pass


class _Column:
    def __init__(self, name: str) -> None:
        self.name = name


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]], columns: tuple[str, ...]) -> None:
        self.rows = rows
        self.output_columns = columns
        self.calls: list[tuple[str, object]] = []
        self.description: list[_Column] | None = None
        self.fetch_size = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: object) -> None:
        self.calls.append((sql, params))
        if sql.startswith("SELECT ") and not sql.startswith("SELECT set_config"):
            self.description = [_Column(column) for column in self.output_columns]

    def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        self.fetch_size = size
        return self.rows[:size]


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.session_options = None

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_session(self, **options: object) -> None:
        self.session_options = options

    def cursor(self) -> _Cursor:
        return self._cursor


class SqlExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.query = validate_analytic_sql(
            "SELECT _source_month, SUM(trip_count) AS trips "
            "FROM analytics.daily_trip_metrics "
            "WHERE _source_year = 2024 GROUP BY _source_month "
            "ORDER BY _source_month LIMIT 2"
        )

    def _executor(self) -> PostgresQueryExecutor:
        return PostgresQueryExecutor(host="127.0.0.1", port=5432,
                                     dbname="nyc_taxi_analytics", user="city_pulse_reader",
                                     password="secret-for-tests")

    def test_explicit_separate_credentials_required(self) -> None:
        with patch.dict(os.environ, {"ANALYTICS_DB_USER": "analytics_dev"}, clear=True):
            for user in ("", "analytics_dev", "postgres"):
                with self.subTest(user=user), self.assertRaises(SqlExecutionError):
                    PostgresQueryExecutor(host="127.0.0.1", port=5432,
                                          dbname="db", user=user, password="x")
            with self.assertRaises(SqlExecutionError):
                PostgresQueryExecutor.from_env()
            with self.assertRaises(SqlExecutionError):
                PostgresQueryExecutor(host="127.0.0.1", port=5432,
                                      dbname="db", user="city_pulse_reader", password="")

    def test_env_routes_but_does_not_reuse_owner_credentials(self) -> None:
        with patch.dict(os.environ, {
            "ANALYTICS_DB_HOST": "localhost", "ANALYTICS_DB_NAME": "analytics",
            "ANALYTICS_DB_USER": "owner", "ANALYTICS_DB_PASSWORD": "owner-secret",
            "CHAT_READONLY_DB_USER": "reader", "CHAT_READONLY_DB_PASSWORD": "reader-secret",
        }, clear=True):
            executor = PostgresQueryExecutor.from_env()
            self.assertEqual(executor._connect_kwargs["user"], "reader")
            self.assertEqual(executor._connect_kwargs["password"], "reader-secret")
            self.assertEqual(executor._connect_kwargs["host"], "localhost")
            self.assertNotIn("reader-secret", repr(executor))
            self.assertNotIn("owner-secret", repr(executor))

    def test_read_only_transaction_timeouts_bound_query_and_capped_fetch(self) -> None:
        cursor = _Cursor([(1, 100), (2, 200)], ("_source_month", "trips"))
        connection = _Connection(cursor)
        calls = []

        def connect(**kwargs: object) -> _Connection:
            calls.append(kwargs)
            return connection

        fake_psycopg2 = types.SimpleNamespace(connect=connect, Error=_DriverError)
        with patch.dict(sys.modules, {"psycopg2": fake_psycopg2}):
            result = self._executor().execute(self.query)
        self.assertEqual(connection.session_options, {"readonly": True})
        self.assertEqual(calls[0]["user"], "city_pulse_reader")
        self.assertEqual(calls[0]["connect_timeout"], 3)
        self.assertEqual(cursor.calls[0],
                         ("SELECT set_config(%s, %s, true)", ("statement_timeout", "3000")))
        self.assertEqual(cursor.calls[1],
                         ("SELECT set_config(%s, %s, true)", ("lock_timeout", "1000")))
        self.assertEqual(cursor.calls[2], (self.query.sql, self.query.params))
        self.assertEqual(cursor.fetch_size, 3)
        self.assertEqual(result.mart, "daily_trip_metrics")
        self.assertEqual(result.rows, ({"_source_month": 1, "trips": 100},
                                       {"_source_month": 2, "trips": 200}))

    def test_raw_sql_and_excess_rows_are_rejected(self) -> None:
        with self.assertRaises(TypeError):
            self._executor().execute("SELECT * FROM analytics.daily_trip_metrics")  # type: ignore[arg-type]
        cursor = _Cursor([(1, 100), (2, 200), (3, 300)], ("_source_month", "trips"))
        with patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(
            connect=lambda **_kw: _Connection(cursor), Error=_DriverError,
        )}):
            with self.assertRaisesRegex(SqlExecutionError, "exceeded"):
                self._executor().execute(self.query)

    def test_database_errors_are_redacted(self) -> None:
        def fail_connect(**_kwargs: object) -> None:
            raise _DriverError("secret-for-tests server said password wrong")

        with patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(
            connect=fail_connect, Error=_DriverError,
        )}):
            with self.assertRaises(SqlExecutionError) as caught:
                self._executor().execute(self.query)
        self.assertNotIn("secret", str(caught.exception))
        self.assertNotIn("password", str(caught.exception))

    def test_result_schema_drift_is_rejected(self) -> None:
        cursor = _Cursor([(1, 100)], ("other", "trips"))
        with patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(
            connect=lambda **_kw: _Connection(cursor), Error=_DriverError,
        )}):
            with self.assertRaisesRegex(SqlExecutionError, "schema changed"):
                self._executor().execute(self.query)


if __name__ == "__main__":
    unittest.main()
