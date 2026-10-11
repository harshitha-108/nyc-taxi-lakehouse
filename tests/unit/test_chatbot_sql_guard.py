"""Adversarial contracts for the chatbot's SQL AST and bound-value boundary."""

from __future__ import annotations

import unittest

from chatbot.sql_guard import UnsafeQuery, validate_analytic_sql


class SqlGuardTests(unittest.TestCase):
    def assert_rejected(self, sql: str) -> None:
        with self.subTest(sql=sql), self.assertRaises(UnsafeQuery):
            validate_analytic_sql(sql)

    def test_monthly_grouping_recompiled_with_bind_values_and_bound_limit(self) -> None:
        query = validate_analytic_sql(
            "SELECT _source_month, SUM(trip_count) AS trips "
            "FROM analytics.daily_trip_metrics "
            "WHERE _source_year = 2024 AND _source_month BETWEEN 1 AND 6 "
            "GROUP BY _source_month ORDER BY trips DESC LIMIT 6"
        )
        self.assertEqual(query.mart, "daily_trip_metrics")
        self.assertEqual(query.columns, ("_source_month", "trips"))
        self.assertEqual(query.limit, 6)
        self.assertEqual(query.timeout_ms, 3000)
        self.assertEqual(query.params, ("yellow", 2024, 1, 6, 6))
        self.assertEqual(
            query.sql,
            "SELECT _source_month, SUM(trip_count) AS trips "
            "FROM analytics.daily_trip_metrics WHERE _source_taxi_type = %s "
            "AND ((_source_year = %s AND _source_month BETWEEN %s AND %s)) "
            "GROUP BY _source_month ORDER BY trips DESC NULLS LAST LIMIT %s",
        )

    def test_date_trend_uses_safe_literals_and_table_alias(self) -> None:
        query = validate_analytic_sql(
            "SELECT d.pickup_date, SUM(d.trip_count) AS trips "
            "FROM analytics.daily_trip_metrics AS d "
            "WHERE d.pickup_date >= '2024-01-01' AND d.pickup_date < '2024-02-01' "
            "GROUP BY d.pickup_date ORDER BY d.pickup_date",
            max_rows=31,
        )
        self.assertEqual(query.params, ("yellow", "2024-01-01", "2024-02-01", 31))
        self.assertIn("GROUP BY pickup_date ORDER BY pickup_date ASC", query.sql)
        self.assertNotIn(" AS d", query.sql)

    def test_borough_hour_payment_breakdown_can_be_grouped(self) -> None:
        query = validate_analytic_sql(
            "SELECT borough, pickup_hour, payment_type, SUM(trip_count) AS trips "
            "FROM analytics.takeaway_breakdown "
            "WHERE borough IN ('Manhattan', 'Queens') AND payment_type = -1 "
            "GROUP BY borough, pickup_hour, payment_type ORDER BY trips DESC LIMIT 10"
        )
        self.assertEqual(query.params, ("yellow", "Manhattan", "Queens", -1, 10))
        self.assertEqual(query.columns, ("borough", "pickup_hour", "payment_type", "trips"))

    def test_distinct_dimension_and_default_limit(self) -> None:
        query = validate_analytic_sql(
            "SELECT DISTINCT borough FROM analytics.pickup_zone_performance"
        )
        self.assertEqual(query.sql, "SELECT DISTINCT borough FROM analytics."
                         "pickup_zone_performance WHERE _source_taxi_type = %s LIMIT %s")
        self.assertEqual(query.params, ("yellow", 100))

    def test_source_literal_is_bound_not_interpolated(self) -> None:
        query = validate_analytic_sql(
            "SELECT borough FROM analytics.pickup_zone_performance "
            "WHERE borough = 'Queens''; DROP TABLE analytics.daily_trip_metrics; --'"
        )
        self.assertNotIn("DROP", query.sql)
        self.assertEqual(query.params[1],
                         "Queens'; DROP TABLE analytics.daily_trip_metrics; --")
        self.assertIn("WHERE _source_taxi_type = %s AND (borough = %s)", query.sql)

    def test_one_trailing_semicolon_is_harmless_but_multiple_statements_fail(self) -> None:
        valid = validate_analytic_sql(
            "SELECT SUM(trip_count) AS trips FROM analytics.daily_trip_metrics;"
        )
        self.assertEqual(valid.params, ("yellow", 100))
        for statement in (
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics; SELECT 1",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics; DELETE FROM x",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics; SET role postgres",
        ):
            self.assert_rejected(statement)

    def test_mutating_or_privileged_statements_fail(self) -> None:
        for statement in (
            "DELETE FROM analytics.daily_trip_metrics",
            "UPDATE analytics.daily_trip_metrics SET trip_count = 1",
            "DROP TABLE analytics.daily_trip_metrics",
            "CREATE TABLE analytics.x (id int)",
            "ALTER TABLE analytics.daily_trip_metrics ADD COLUMN secret text",
            "TRUNCATE analytics.daily_trip_metrics",
            "COPY analytics.daily_trip_metrics TO '/tmp/mart.csv'",
            "EXPLAIN SELECT trip_count FROM analytics.daily_trip_metrics",
            "SELECT trip_count INTO TEMP x FROM analytics.daily_trip_metrics",
            "SELECT trip_count FROM analytics.daily_trip_metrics FOR UPDATE",
        ):
            self.assert_rejected(statement)

    def test_unapproved_sources_and_joins_fail(self) -> None:
        for statement in (
            "SELECT trip_count FROM pg_catalog.pg_tables",
            "SELECT trip_count FROM information_schema.tables",
            "SELECT trip_count FROM public.daily_trip_metrics",
            "SELECT trip_count FROM analytics.secret_data",
            "SELECT trip_count FROM analytics.daily_trip_metrics, pg_catalog.pg_tables",
            "SELECT SUM(a.trip_count) FROM analytics.daily_trip_metrics a "
            "JOIN analytics.payment_type_summary b ON a._source_month = b._source_month",
            "SELECT SUM(trip_count) FROM daily_trip_metrics",
        ):
            self.assert_rejected(statement)

    def test_subqueries_ctes_unions_windows_and_functions_fail(self) -> None:
        for statement in (
            "WITH x AS (SELECT trip_count FROM analytics.daily_trip_metrics) SELECT * FROM x",
            "SELECT SUM(trip_count) FROM (SELECT * FROM analytics.daily_trip_metrics) x",
            "SELECT trip_count FROM analytics.daily_trip_metrics UNION SELECT 1",
            "SELECT SUM(trip_count) OVER () FROM analytics.daily_trip_metrics",
            "SELECT pg_sleep(1) FROM analytics.daily_trip_metrics",
            "SELECT current_setting('server_version') FROM analytics.daily_trip_metrics",
            "SELECT SUM(trip_count) FILTER (WHERE _source_month = 1) "
            "FROM analytics.daily_trip_metrics",
            "SELECT CASE WHEN trip_count > 0 THEN 1 ELSE 0 END "
            "FROM analytics.daily_trip_metrics",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE pickup_date = (SELECT CURRENT_DATE)",
        ):
            self.assert_rejected(statement)

    def test_star_and_unapproved_column_fail(self) -> None:
        for statement in (
            "SELECT * FROM analytics.daily_trip_metrics",
            "SELECT COUNT(*) FROM analytics.daily_trip_metrics",
            "SELECT SUM(password) FROM analytics.daily_trip_metrics",
            "SELECT SUM(trip_count) FROM analytics.hourly_demand WHERE borough = 'Queens'",
            "SELECT pg_catalog.pg_sleep(1) FROM analytics.daily_trip_metrics",
            "SELECT daily_trip_metrics._source_month FROM analytics.daily_trip_metrics "
            "ORDER BY not_projected",
        ):
            self.assert_rejected(statement)

    def test_tautology_filters_and_unsupported_operators_fail(self) -> None:
        for statement in (
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics WHERE 1 = 1",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE _source_month = 1 OR 1 = 1",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE _source_month IN (SELECT month FROM x)",
            "SELECT borough FROM analytics.pickup_zone_performance "
            "WHERE borough LIKE 'Man%'",
            "SELECT borough FROM analytics.pickup_zone_performance "
            "WHERE borough IS NULL",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE _source_taxi_type = 'green'",
        ):
            self.assert_rejected(statement)

    def test_invalid_group_and_order_shape_fail(self) -> None:
        for statement in (
            "SELECT _source_month, SUM(trip_count) FROM analytics.daily_trip_metrics",
            "SELECT _source_month, SUM(trip_count) FROM analytics.daily_trip_metrics "
            "GROUP BY _source_year",
            "SELECT _source_month, SUM(trip_count) FROM analytics.daily_trip_metrics "
            "GROUP BY _source_month ORDER BY _source_year",
            "SELECT _source_month, SUM(trip_count) FROM analytics.daily_trip_metrics "
            "GROUP BY _source_month HAVING SUM(trip_count) > 10",
            "SELECT _source_month, _source_month FROM analytics.daily_trip_metrics",
            "SELECT borough AS trips, SUM(trip_count) AS trips "
            "FROM analytics.pickup_zone_performance GROUP BY borough",
            "SELECT borough FROM analytics.pickup_zone_performance OFFSET 10",
        ):
            self.assert_rejected(statement)

    def test_limit_and_filter_bounds(self) -> None:
        for statement in (
            "SELECT borough FROM analytics.pickup_zone_performance LIMIT 0",
            "SELECT borough FROM analytics.pickup_zone_performance LIMIT 101",
            "SELECT borough FROM analytics.pickup_zone_performance LIMIT -1",
            "SELECT borough FROM analytics.pickup_zone_performance LIMIT 1.5",
            "SELECT borough FROM analytics.pickup_zone_performance "
            "WHERE _source_month = 13",
            "SELECT borough FROM analytics.pickup_zone_performance "
            "WHERE pickup_location_id = 999999999999999",
            "SELECT SUM(trip_count) FROM analytics.hourly_demand WHERE pickup_hour = 24",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE pickup_date = '2024-02-30'",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics "
            "WHERE _source_month = 'January'",
        ):
            self.assert_rejected(statement)
        for limit in (0, 201, -1, True):
            with self.subTest(max_rows=limit), self.assertRaises(ValueError):
                validate_analytic_sql(
                    "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics",
                    max_rows=limit,
                )

    def test_malformed_or_oversized_input_fails(self) -> None:
        for statement in (
            "", "SELECT FROM analytics.daily_trip_metrics",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics\x00",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics \ud800",
            "SELECT SUM(trip_count) FROM analytics.daily_trip_metrics " + " " * 4096,
        ):
            self.assert_rejected(statement)


if __name__ == "__main__":
    unittest.main()
