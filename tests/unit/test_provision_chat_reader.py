"""Provisioning does not touch data, PUBLIC grants, or unrelated roles."""

from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

from chatbot.sql_guard import APPROVED_MARTS
from scripts.provision_chat_reader import (
    CHAT_MARTS,
    ProvisionConfig,
    ProvisionError,
    _provision,
    provision_reader,
)


def _config(**overrides: object) -> ProvisionConfig:
    fields = {
        "host": "airflow-postgres", "port": 5432, "database": "nyc_taxi_analytics",
        "admin_user": "airflow_dev", "admin_password": "admin-secret",
        "owner_user": "analytics_dev", "reader_user": "city_pulse_reader",
        "reader_password": "reader-secret",
    }
    fields.update(overrides)
    return ProvisionConfig(**fields)


class _Cursor:
    def __init__(self, *, role: tuple[bool, ...] | None = None,
                 membership: bool = False, ownership: bool = False,
                 missing_mart: bool = False, public_select: bool = False,
                 public_create: bool = False) -> None:
        self.role = role
        self.membership = membership
        self.ownership = ownership
        self.missing_mart = missing_mart
        self.public_select = public_select
        self.public_create = public_create
        self.calls: list[tuple[str, object]] = []
        self._answer: object = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.calls.append((sql, params))
        if sql.startswith("SELECT c.relname") and "has_table_privilege" in sql:
            self._answer = [("secret",)] if self.public_select else []
        elif sql.startswith("SELECT c.relname") and "c.relname = ANY" in sql:
            names = sorted(CHAT_MARTS)
            if self.missing_mart:
                names.remove("takeaway_breakdown")
            self._answer = [(name,) for name in names]
        elif sql.startswith("SELECT rolsuper"):
            self._answer = self.role
        elif sql.startswith("SELECT 1 FROM pg_auth_members"):
            self._answer = (1,) if self.membership else None
        elif sql.startswith("SELECT EXISTS"):
            self._answer = (self.ownership, False, False)
        elif sql.startswith("SELECT has_database_privilege"):
            self._answer = (self.public_create, False)

    def fetchone(self) -> object:
        return self._answer

    def fetchall(self) -> object:
        return self._answer


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.outcome = None
        self.closed = False

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        self.outcome = "committed" if exc_type is None else "rolled_back"

    def close(self) -> None:
        self.closed = True

    def cursor(self) -> _Cursor:
        return self._cursor


class _DriverError(Exception):
    pass


class ProvisionChatReaderTests(unittest.TestCase):
    def test_grants_stay_in_sync_with_sql_guard(self) -> None:
        self.assertEqual(set(CHAT_MARTS), set(APPROVED_MARTS))

    def test_requires_explicit_distinct_reader_and_admin_secrets(self) -> None:
        with patch.dict(os.environ, {
            "ANALYTICS_DB_HOST": "airflow-postgres", "ANALYTICS_DB_PORT": "5432",
            "ANALYTICS_DB_NAME": "nyc_taxi_analytics", "ANALYTICS_DB_USER": "analytics_dev",
            "POSTGRES_USER": "airflow_dev", "POSTGRES_PASSWORD": "admin-secret",
        }, clear=True):
            with self.assertRaisesRegex(ProvisionError, "CHAT_READONLY_DB_USER"):
                ProvisionConfig.from_env()
        for overrides in (
            {"reader_user": "analytics_dev"}, {"reader_user": "airflow_dev"},
            {"reader_user": "postgres"}, {"reader_user": "city-pulse"},
            {"reader_user": 'reader";DROP'}, {"reader_password": ""},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ProvisionError):
                _config(**overrides)

    def test_new_role_only_gets_six_mart_select_grants(self) -> None:
        cursor = _Cursor()
        _provision(cursor, _config())
        sql = [statement for statement, _ in cursor.calls]
        mutation = [statement for statement in sql if not statement.startswith("SELECT")]
        self.assertTrue(mutation[0].startswith('CREATE ROLE "city_pulse_reader" WITH LOGIN'))
        self.assertIn("NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE", mutation[0])
        self.assertIn("NOBYPASSRLS CONNECTION LIMIT 5 PASSWORD %s", mutation[0])
        self.assertEqual(next(params for statement, params in cursor.calls
                              if statement.startswith("CREATE ROLE")), ("reader-secret",))
        self.assertFalse(any("reader-secret" in statement for statement in sql))
        self.assertIn('GRANT CONNECT ON DATABASE "nyc_taxi_analytics" TO "city_pulse_reader"',
                      mutation)
        self.assertIn('GRANT USAGE ON SCHEMA "analytics" TO "city_pulse_reader"', mutation)
        grant = next(statement for statement in mutation
                     if statement.startswith("GRANT SELECT ON TABLE"))
        for name in CHAT_MARTS:
            self.assertIn(f'"analytics"."{name}"', grant)
        self.assertEqual(grant.count('"analytics".'), len(CHAT_MARTS))
        self.assertTrue(all("FROM PUBLIC" not in statement and "TO PUBLIC" not in statement
                            for statement in mutation))
        self.assertTrue(any("SET default_transaction_read_only = on" in statement
                            for statement in mutation))
        privilege_check = next(params for statement, params in cursor.calls
                               if statement.startswith("SELECT has_database_privilege"))
        self.assertEqual(privilege_check[0], "city_pulse_reader")
        self.assertEqual(privilege_check[2], "city_pulse_reader")

    def test_existing_safe_role_is_reconfigured_without_recreation(self) -> None:
        cursor = _Cursor(role=(False,) * 5)
        _provision(cursor, _config())
        mutation = [statement for statement, _ in cursor.calls
                    if statement.startswith(("CREATE", "ALTER", "GRANT", "REVOKE"))]
        self.assertTrue(mutation[0].startswith('ALTER ROLE "city_pulse_reader" WITH LOGIN'))
        self.assertFalse(any(statement.startswith("CREATE ROLE") for statement in mutation))

    def test_missing_mart_and_unsafe_existing_role_fail_before_mutation(self) -> None:
        for state in (
            {"missing_mart": True}, {"role": (True, False, False, False, False)},
            {"role": (False,) * 5, "membership": True},
            {"role": (False,) * 5, "ownership": True},
        ):
            cursor = _Cursor(**state)
            with self.subTest(state=state), self.assertRaises(ProvisionError):
                _provision(cursor, _config())
            self.assertTrue(all(statement.startswith("SELECT")
                                for statement, _ in cursor.calls))

    def test_unapproved_public_access_or_create_privilege_rolls_back(self) -> None:
        for state in ({"public_select": True}, {"public_create": True}):
            cursor = _Cursor(**state)
            connection = _Connection(cursor)
            def connect(_connection: _Connection = connection,
                        **_kwargs: object) -> _Connection:
                return _connection

            fake = types.SimpleNamespace(connect=connect, Error=_DriverError)
            with patch.dict(sys.modules, {"psycopg2": fake}):
                with self.subTest(state=state), self.assertRaises(ProvisionError):
                    provision_reader(_config())
            self.assertEqual(connection.outcome, "rolled_back")
            self.assertTrue(connection.closed)

    def test_successful_single_transaction_closes_connection(self) -> None:
        cursor = _Cursor()
        connection = _Connection(cursor)
        calls = []

        def connect(**kwargs: object) -> _Connection:
            calls.append(kwargs)
            return connection

        with patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(
            connect=connect, Error=_DriverError,
        )}):
            provision_reader(_config())
        self.assertEqual(connection.outcome, "committed")
        self.assertTrue(connection.closed)
        self.assertEqual(calls[0]["dbname"], "nyc_taxi_analytics")
        self.assertEqual(calls[0]["user"], "airflow_dev")
        self.assertEqual(calls[0]["password"], "admin-secret")


if __name__ == "__main__":
    unittest.main()
