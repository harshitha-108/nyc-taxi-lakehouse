"""Explicitly provision one least-privilege reader of published analytics marts.

Run this manually after the serving tables exist. It does not run during normal
pipeline startup, edit data, or change PUBLIC/other role grants. PostgreSQL's
default PUBLIC TEMP database privilege may still be inherited; the chat executor
also enforces a read-only transaction and accepts AST-validated SELECTs only.
"""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}\Z", re.ASCII)
_SCHEMA = "analytics"
# Keep this explicit and independent of the optional SQL parser so that the
# existing pipeline image can provision a reader without a dependency rebuild.
CHAT_MARTS = (
    "daily_trip_metrics", "hourly_demand", "payment_type_summary",
    "pickup_location_performance", "pickup_zone_performance", "takeaway_breakdown",
)


class ProvisionError(RuntimeError):
    """No privilege changes were committed because preconditions were not met."""


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ProvisionError(f"Missing required setting: {name}")
    return value


def _identifier(value: str, label: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ProvisionError(f"{label} must be a simple lowercase SQL identifier")
    return value


def _quoted(value: str) -> str:
    """Quote a previously validated simple identifier, including reserved words."""
    return f'"{value}"'


@dataclass(frozen=True)
class ProvisionConfig:
    host: str
    port: int
    database: str
    admin_user: str
    admin_password: str
    owner_user: str
    reader_user: str
    reader_password: str

    def __post_init__(self) -> None:
        if not self.host or not self.admin_password or not self.reader_password:
            raise ProvisionError("Database host and both passwords are required")
        if not 1 <= self.port <= 65535:
            raise ProvisionError("Invalid analytics database port")
        for label, value in (("Database name", self.database),
                             ("Admin role", self.admin_user),
                             ("Analytics owner role", self.owner_user),
                             ("Chat reader role", self.reader_user)):
            _identifier(value, label)
        if self.reader_user in {self.admin_user, self.owner_user, "postgres"}:
            raise ProvisionError("Chat reader must be separate from owner and admin roles")

    @classmethod
    def from_env(cls) -> ProvisionConfig:
        try:
            port = int(_required("ANALYTICS_DB_PORT"))
        except ValueError as exc:
            raise ProvisionError("Invalid analytics database port") from exc
        return cls(
            host=_required("ANALYTICS_DB_HOST"),
            port=port,
            database=_required("ANALYTICS_DB_NAME"),
            admin_user=_required("POSTGRES_USER"),
            admin_password=_required("POSTGRES_PASSWORD"),
            owner_user=_required("ANALYTICS_DB_USER"),
            reader_user=_required("CHAT_READONLY_DB_USER"),
            reader_password=_required("CHAT_READONLY_DB_PASSWORD"),
        )


def _preflight(cursor: object, config: ProvisionConfig) -> bool:
    """Return whether the dedicated role exists, before issuing any DDL."""
    names = sorted(CHAT_MARTS)
    cursor.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = ANY(%s) "
        "AND c.relkind IN ('r', 'p', 'v', 'm')",
        (_SCHEMA, names),
    )
    present = {row[0] for row in cursor.fetchall()}
    missing = sorted(set(names) - present)
    if missing:
        raise ProvisionError("Published analytics marts are missing: " + ", ".join(missing))

    cursor.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
        "rolbypassrls FROM pg_roles WHERE rolname = %s",
        (config.reader_user,),
    )
    role = cursor.fetchone()
    if role is None:
        return False
    if any(role):
        raise ProvisionError("Existing chat role has elevated attributes; refusing to alter it")
    cursor.execute(
        "SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
        "WHERE r.rolname = %s LIMIT 1",
        (config.reader_user,),
    )
    if cursor.fetchone() is not None:
        raise ProvisionError("Existing chat role has role memberships; refusing to alter it")
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba "
        "WHERE r.rolname = %s), "
        "EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner "
        "WHERE r.rolname = %s), "
        "EXISTS(SELECT 1 FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner "
        "WHERE r.rolname = %s)",
        (config.reader_user,) * 3,
    )
    if any(cursor.fetchone()):
        raise ProvisionError("Existing chat role owns database objects; refusing to alter it")
    return True


def _provision(cursor: object, config: ProvisionConfig) -> None:
    """All mutations are in the caller's one transaction and target this role."""
    existing = _preflight(cursor, config)
    role = _quoted(config.reader_user)
    database = _quoted(config.database)
    schema = _quoted(_SCHEMA)
    if existing:
        cursor.execute(
            f"ALTER ROLE {role} WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 5 PASSWORD %s",
            (config.reader_password,),
        )
    else:
        cursor.execute(
            f"CREATE ROLE {role} WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 5 PASSWORD %s",
            (config.reader_password,),
        )
    cursor.execute(f"REVOKE ALL PRIVILEGES ON DATABASE {database} FROM {role}")
    cursor.execute(f"GRANT CONNECT ON DATABASE {database} TO {role}")
    cursor.execute(f"REVOKE ALL PRIVILEGES ON SCHEMA {schema} FROM {role}")
    cursor.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
    cursor.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} FROM {role}")
    cursor.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} FROM {role}")
    tables = ", ".join(f"{schema}.{_quoted(name)}" for name in sorted(CHAT_MARTS))
    cursor.execute(f"GRANT SELECT ON TABLE {tables} TO {role}")
    cursor.execute(f"ALTER ROLE {role} IN DATABASE {database} "
                   "SET default_transaction_read_only = on")
    cursor.execute(f"ALTER ROLE {role} IN DATABASE {database} SET statement_timeout = '3s'")
    cursor.execute(f"ALTER ROLE {role} IN DATABASE {database} SET lock_timeout = '1s'")
    cursor.execute(f"ALTER ROLE {role} IN DATABASE {database} SET search_path = pg_catalog")
    # A direct REVOKE cannot cancel a PUBLIC SELECT grant. Fail/roll back rather
    # than modifying PUBLIC or inheriting reads outside the six approved marts.
    cursor.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname <> 'information_schema' AND left(n.nspname, 3) <> 'pg_' "
        "AND c.relkind IN ('r', 'p', 'v', 'm') "
        "AND NOT (n.nspname = %s AND c.relname = ANY(%s)) "
        "AND has_table_privilege(%s, c.oid, 'SELECT')",
        (_SCHEMA, sorted(CHAT_MARTS), config.reader_user),
    )
    if cursor.fetchall():
        raise ProvisionError("Chat role would inherit SELECT on an unapproved relation")
    cursor.execute("SELECT has_database_privilege(%s, %s, 'CREATE'), "
                   "has_schema_privilege(%s, %s, 'CREATE')",
                   (config.reader_user, config.database,
                    config.reader_user, _SCHEMA))
    if any(cursor.fetchone()):
        raise ProvisionError("Chat role would inherit CREATE privilege from another grant")


def provision_reader(config: ProvisionConfig) -> None:
    """Open one admin transaction; context manager rolls back on any failure."""
    try:
        import psycopg2
    except ImportError as exc:
        raise ProvisionError("Install psycopg2-binary before provisioning") from exc
    try:
        with closing(psycopg2.connect(
            host=config.host, port=config.port, dbname=config.database,
            user=config.admin_user, password=config.admin_password, connect_timeout=5,
            application_name="city_pulse_reader_provision",
        )) as connection:
            with connection:
                with connection.cursor() as cursor:
                    _provision(cursor, config)
    except psycopg2.Error as exc:
        raise ProvisionError("Could not provision the chat reader role") from exc


def main() -> None:
    config = ProvisionConfig.from_env()
    provision_reader(config)
    print(f"Dedicated chat reader ready for {_SCHEMA} marts: {config.reader_user}")


if __name__ == "__main__":
    main()
