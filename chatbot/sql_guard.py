"""Compile a deliberately small, read-only SQL dialect over published taxi marts.

This is a second boundary after question interpretation, not a replacement for a
database role with SELECT-only grants and a transaction-level statement timeout.
The model's SQL is *never* executed: SQLGlot parses it, this module validates the
entire supported AST, and a new statement is built from approved identifiers and
bound values. Extending the grammar requires an explicit security review.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError

MAX_SQL_BYTES = 4096
MAX_AST_NODES = 120
MAX_RESULT_ROWS = 200
DEFAULT_RESULT_ROWS = 100
STATEMENT_TIMEOUT_MS = 3000
_NAME = re.compile(r"[a-z_][a-z_0-9]{0,31}\Z", re.ASCII)


@dataclass(frozen=True)
class MartPolicy:
    dimensions: frozenset[str]
    measures: frozenset[str]


_PERIOD = frozenset({"_source_year", "_source_month"})
_TRIPS = frozenset({"trip_count"})
_MONEY = frozenset({"total_revenue", "total_tip_amount"})
APPROVED_MARTS: dict[str, MartPolicy] = {
    "daily_trip_metrics": MartPolicy(
        _PERIOD | {"pickup_date"}, _TRIPS | _MONEY | {"total_trip_distance"}
    ),
    "hourly_demand": MartPolicy(
        _PERIOD | {"pickup_date", "pickup_hour"}, _TRIPS | {"total_revenue"}
    ),
    "pickup_location_performance": MartPolicy(
        _PERIOD | {"pickup_location_id"}, _TRIPS | _MONEY
    ),
    "payment_type_summary": MartPolicy(
        _PERIOD | {"payment_type"}, _TRIPS | _MONEY
    ),
    "pickup_zone_performance": MartPolicy(
        _PERIOD | {"pickup_location_id", "borough", "zone", "service_zone"},
        _TRIPS | _MONEY,
    ),
    "takeaway_breakdown": MartPolicy(
        _PERIOD | {"borough", "pickup_hour", "payment_type"}, _TRIPS
    ),
}
_INTEGER_DIMENSIONS = frozenset({
    "_source_year", "_source_month", "pickup_hour", "payment_type", "pickup_location_id"
})
_DATE_DIMENSIONS = frozenset({"pickup_date"})
_AGGREGATES = {exp.Sum: "SUM", exp.Avg: "AVG", exp.Min: "MIN", exp.Max: "MAX"}
_COMPARISONS = {
    exp.EQ: "=", exp.NEQ: "<>", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<="
}


class UnsafeQuery(ValueError):
    """A proposed statement is outside the approved analytical SQL subset."""


@dataclass(frozen=True)
class ValidatedQuery:
    """Safe PostgreSQL query and ordered psycopg2 bind values.

    SQL contains ``%s`` placeholders. A SQLite test adapter would need to
    translate placeholders to ``?``; this statement is not SQLite SQL as-is.
    The caller must still use a SELECT-only role, a read-only transaction, and
    ``SET LOCAL statement_timeout`` before executing it.
    """

    sql: str
    params: tuple[str | int | Decimal, ...]
    mart: str
    columns: tuple[str, ...]
    limit: int
    timeout_ms: int = STATEMENT_TIMEOUT_MS


def _identifier(node: exp.Expression | None) -> str:
    if not isinstance(node, exp.Identifier):
        raise UnsafeQuery("Expected a simple identifier")
    value = str(node.this)
    if not node.args.get("quoted"):
        value = value.lower()
    if not _NAME.fullmatch(value):
        raise UnsafeQuery("Identifier is not in the approved form")
    return value


def _only_args(node: exp.Expression, allowed: set[str]) -> None:
    if any(key not in allowed and value is not None for key, value in node.args.items()):
        raise UnsafeQuery("Unsupported SQL construct")


def _column(node: exp.Expression, mart: str, alias: str | None,
            allowed: frozenset[str]) -> str:
    if not isinstance(node, exp.Column):
        raise UnsafeQuery("Only approved columns may be referenced")
    _only_args(node, {"this", "table"})
    qualifier = node.args.get("table")
    if qualifier is not None and _identifier(qualifier) not in {mart, alias}:
        raise UnsafeQuery("Column qualifier does not match the approved mart")
    name = _identifier(node.args.get("this"))
    if name not in allowed:
        raise UnsafeQuery("Column is not approved for this mart")
    return name


def _literal(node: exp.Expression, column: str) -> str | int | Decimal:
    negative = isinstance(node, exp.Neg)
    if negative:
        _only_args(node, {"this"})
        node = node.this
    if not isinstance(node, exp.Literal):
        raise UnsafeQuery("Predicates must compare columns with literal values")
    _only_args(node, {"this", "is_string"})
    if node.is_string:
        if negative or column in _INTEGER_DIMENSIONS:
            raise UnsafeQuery("This column needs a numeric literal")
        value = str(node.this)
        if len(value) > 96 or any(ord(char) < 32 for char in value):
            raise UnsafeQuery("Text literal is too long or contains a control character")
        if column in _DATE_DIMENSIONS:
            try:
                date.fromisoformat(value)
            except ValueError as exc:
                raise UnsafeQuery("Date filter must be an ISO calendar date") from exc
        return value
    if column in _DATE_DIMENSIONS or column not in _INTEGER_DIMENSIONS:
        raise UnsafeQuery("This dimension needs a text literal")
    try:
        number = Decimal(str(node.this))
    except InvalidOperation as exc:
        raise UnsafeQuery("Invalid numeric literal") from exc
    if negative:
        number = -number
    if not number.is_finite() or number != number.to_integral_value() or abs(number) > 10**12:
        raise UnsafeQuery("Integer filter is out of bounds")
    result = int(number)
    if column == "_source_month" and not 1 <= result <= 12:
        raise UnsafeQuery("Month must be between 1 and 12")
    if column == "pickup_hour" and not 0 <= result <= 23:
        raise UnsafeQuery("Pickup hour must be between 0 and 23")
    return result


def _predicate(node: exp.Expression, mart: str, alias: str | None,
               dimensions: frozenset[str], params: list[str | int | Decimal],
               depth: int = 0) -> str:
    if depth > 8:
        raise UnsafeQuery("Predicate is too deeply nested")
    if isinstance(node, exp.Paren):
        _only_args(node, {"this"})
        return f"({_predicate(node.this, mart, alias, dimensions, params, depth + 1)})"
    if isinstance(node, exp.And | exp.Or):
        _only_args(node, {"this", "expression"})
        left = _predicate(node.this, mart, alias, dimensions, params, depth + 1)
        right = _predicate(node.expression, mart, alias, dimensions, params, depth + 1)
        operator = "AND" if isinstance(node, exp.And) else "OR"
        return f"({left} {operator} {right})"
    if type(node) in _COMPARISONS:
        _only_args(node, {"this", "expression"})
        column = _column(node.this, mart, alias, dimensions)
        params.append(_literal(node.expression, column))
        return f"{column} {_COMPARISONS[type(node)]} %s"
    if isinstance(node, exp.In):
        _only_args(node, {"this", "expressions"})
        column = _column(node.this, mart, alias, dimensions)
        values = node.expressions
        if not 1 <= len(values) <= 12:
            raise UnsafeQuery("IN requires between 1 and 12 literal values")
        params.extend(_literal(value, column) for value in values)
        return f"{column} IN ({', '.join('%s' for _ in values)})"
    if isinstance(node, exp.Between):
        _only_args(node, {"this", "low", "high"})
        column = _column(node.this, mart, alias, dimensions)
        params.extend((_literal(node.args["low"], column),
                       _literal(node.args["high"], column)))
        return f"{column} BETWEEN %s AND %s"
    raise UnsafeQuery("Only bounded comparisons, IN, BETWEEN, AND, and OR are supported")


def _projection(node: exp.Expression, mart: str, alias: str | None,
                policy: MartPolicy) -> tuple[str, str, str, bool]:
    """Return SQL, output name, expression signature, and aggregate flag."""
    output_alias = None
    if isinstance(node, exp.Alias):
        _only_args(node, {"this", "alias"})
        output_alias = _identifier(node.args.get("alias"))
        node = node.this
    if isinstance(node, exp.Column):
        name = _column(node, mart, alias, policy.dimensions)
        expression = name
        generated = name
        aggregate = False
    elif type(node) in _AGGREGATES:
        _only_args(node, {"this"})
        name = _column(node.this, mart, alias, policy.measures)
        function = _AGGREGATES[type(node)]
        expression = f"{function}({name})"
        generated = f"{function.lower()}_{name}"
        aggregate = True
    else:
        raise UnsafeQuery("Select only dimensions or approved aggregate measures")
    output = output_alias or generated
    statement = f"{expression} AS {output}" if output != expression else expression
    return statement, output, expression, aggregate


def _source(tree: exp.Select) -> tuple[str, str | None]:
    from_clause = tree.args.get("from_")
    if not isinstance(from_clause, exp.From):
        raise UnsafeQuery("A single approved analytics mart is required")
    _only_args(from_clause, {"this"})
    table = from_clause.this
    if not isinstance(table, exp.Table):
        raise UnsafeQuery("Subqueries and generated tables are not allowed")
    _only_args(table, {"this", "db", "alias"})
    if _identifier(table.args.get("db")) != "analytics":
        raise UnsafeQuery("Only the analytics schema is approved")
    mart = _identifier(table.args.get("this"))
    if mart not in APPROVED_MARTS:
        raise UnsafeQuery("Mart is not approved for chat queries")
    table_alias = table.args.get("alias")
    alias = None
    if table_alias is not None:
        if not isinstance(table_alias, exp.TableAlias):
            raise UnsafeQuery("Unsupported table alias")
        _only_args(table_alias, {"this"})
        alias = _identifier(table_alias.this)
    return mart, alias


def validate_analytic_sql(sql_text: str, *, max_rows: int = DEFAULT_RESULT_ROWS) -> ValidatedQuery:
    """Validate and recompile one narrow PostgreSQL SELECT with bound values.

    An optional user ``LIMIT`` may only *reduce* ``max_rows``. The compiler
    always imposes a final LIMIT, and always adds a yellow-taxi predicate.
    Rejection is intentional for joins, CTEs, subqueries, windows, stars,
    non-approved functions, and any AST node outside this small grammar.
    """
    if (not isinstance(max_rows, int) or isinstance(max_rows, bool)
            or not 1 <= max_rows <= MAX_RESULT_ROWS):
        raise ValueError(f"max_rows must be between 1 and {MAX_RESULT_ROWS}")
    if not isinstance(sql_text, str) or not sql_text.strip() or "\x00" in sql_text:
        raise UnsafeQuery("A non-empty SQL statement is required")
    try:
        byte_count = len(sql_text.encode("utf-8"))
    except UnicodeError as exc:
        raise UnsafeQuery("SQL is not valid Unicode") from exc
    if byte_count > MAX_SQL_BYTES:
        raise UnsafeQuery("SQL statement is too long")
    try:
        statements = parse(sql_text, read="postgres")
    except (SqlglotError, RecursionError) as exc:
        raise UnsafeQuery("SQL could not be parsed") from exc
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise UnsafeQuery("Exactly one SELECT statement is allowed")
    tree = statements[0]
    if sum(1 for _ in tree.walk()) > MAX_AST_NODES:
        raise UnsafeQuery("SQL statement is too complex")
    _only_args(tree, {"expressions", "from_", "where", "group", "order", "limit", "distinct"})
    mart, alias = _source(tree)
    policy = APPROVED_MARTS[mart]
    projections = tree.expressions
    if not 1 <= len(projections) <= 10:
        raise UnsafeQuery("Select between 1 and 10 approved expressions")
    rendered = [_projection(item, mart, alias, policy) for item in projections]
    outputs = [item[1] for item in rendered]
    if len(outputs) != len(set(outputs)):
        raise UnsafeQuery("Output column names must be unique")
    aggregates = any(item[3] for item in rendered)
    dimensions = {item[2] for item in rendered if not item[3]}
    group_clause = tree.args.get("group")
    groups: list[str] = []
    if group_clause is not None:
        if not isinstance(group_clause, exp.Group):
            raise UnsafeQuery("Invalid GROUP BY clause")
        _only_args(group_clause, {"expressions"})
        groups = [_column(item, mart, alias, policy.dimensions)
                  for item in group_clause.expressions]
        if not 1 <= len(groups) <= 4 or len(groups) != len(set(groups)):
            raise UnsafeQuery("GROUP BY needs 1 to 4 distinct dimensions")
    if (aggregates or groups) and dimensions != set(groups):
        raise UnsafeQuery("Projected dimensions and GROUP BY dimensions must match")
    distinct = tree.args.get("distinct")
    if distinct is not None:
        if aggregates or groups or not isinstance(distinct, exp.Distinct):
            raise UnsafeQuery("DISTINCT is only supported for ungrouped dimensions")
        _only_args(distinct, set())
    parameters: list[str | int | Decimal] = ["yellow"]
    where = tree.args.get("where")
    predicate = None
    if where is not None:
        if not isinstance(where, exp.Where):
            raise UnsafeQuery("Invalid WHERE clause")
        _only_args(where, {"this"})
        predicate = _predicate(where.this, mart, alias, policy.dimensions, parameters)
    order = tree.args.get("order")
    order_parts = []
    if order is not None:
        if not isinstance(order, exp.Order):
            raise UnsafeQuery("Invalid ORDER BY clause")
        _only_args(order, {"expressions", "siblings"})
        if not 1 <= len(order.expressions) <= 4 or order.args.get("siblings"):
            raise UnsafeQuery("ORDER BY needs 1 to 4 output fields")
        allowed_order = set(outputs) | {item[2] for item in rendered}
        for item in order.expressions:
            if not isinstance(item, exp.Ordered):
                raise UnsafeQuery("Unsupported ORDER BY expression")
            _only_args(item, {"this", "desc", "nulls_first"})
            expression = item.this
            if isinstance(expression, exp.Column):
                _only_args(expression, {"this", "table"})
                if expression.args.get("table") is not None:
                    field = _column(expression, mart, alias, policy.dimensions)
                else:
                    field = _identifier(expression.this)
            elif type(expression) in _AGGREGATES:
                _only_args(expression, {"this"})
                order_measure = _column(expression.this, mart, alias, policy.measures)
                field = f"{_AGGREGATES[type(expression)]}({order_measure})"
            else:
                raise UnsafeQuery("ORDER BY must reference a selected output")
            if field not in allowed_order:
                raise UnsafeQuery("ORDER BY must reference a selected output")
            order_parts.append(f"{field} {'DESC' if item.args.get('desc') else 'ASC'} NULLS LAST")
    limit_clause = tree.args.get("limit")
    limit = max_rows
    if limit_clause is not None:
        if not isinstance(limit_clause, exp.Limit):
            raise UnsafeQuery("Invalid LIMIT clause")
        _only_args(limit_clause, {"expression"})
        node = limit_clause.expression
        if not isinstance(node, exp.Literal) or node.is_string:
            raise UnsafeQuery("LIMIT must be a positive integer literal")
        try:
            limit = int(str(node.this))
        except ValueError as exc:
            raise UnsafeQuery("LIMIT must be a positive integer literal") from exc
        if not 1 <= limit <= max_rows or str(limit) != str(node.this):
            raise UnsafeQuery("LIMIT exceeds the allowed result size")
    statement = (
        f"SELECT {'DISTINCT ' if distinct is not None else ''}"
        + ", ".join(item[0] for item in rendered)
        + f" FROM analytics.{mart} WHERE _source_taxi_type = %s"
    )
    if predicate:
        statement += f" AND ({predicate})"
    if groups:
        statement += " GROUP BY " + ", ".join(groups)
    if order_parts:
        statement += " ORDER BY " + ", ".join(order_parts)
    statement += " LIMIT %s"
    parameters.append(limit)
    return ValidatedQuery(statement, tuple(parameters), mart, tuple(outputs), limit)
