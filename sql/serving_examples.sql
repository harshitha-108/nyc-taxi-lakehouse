-- Read-only examples against compact Jan-Jun serving marts.
-- total_revenue is TLC total_amount summed by Gold, not company accounting revenue.

SELECT pickup_date, SUM(trip_count) AS trips
FROM analytics.daily_trip_metrics
GROUP BY pickup_date
ORDER BY trips DESC
LIMIT 1;

SELECT pickup_hour, SUM(trip_count) AS trips
FROM analytics.hourly_demand
GROUP BY pickup_hour
ORDER BY trips DESC
LIMIT 1;

SELECT borough, SUM(trip_count) AS pickup_trips
FROM analytics.pickup_zone_performance
GROUP BY borough
ORDER BY pickup_trips DESC;

SELECT borough, zone, SUM(trip_count) AS pickup_trips
FROM analytics.pickup_zone_performance
GROUP BY borough, zone
ORDER BY pickup_trips DESC
LIMIT 10;

SELECT payment_type, SUM(trip_count) AS trips
FROM analytics.payment_type_summary
GROUP BY payment_type
ORDER BY trips DESC;
