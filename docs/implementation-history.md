# Implementation history and validation evidence

This is the detailed record behind the [project overview](../README.md). Phase-specific counts below describe the system **at that phase**, not the current test inventory. Runtime data and benchmark JSON are intentionally excluded from Git; `PROJECT_STATUS.md` records additional observed results and limitations.

## Phase 1 — Local foundation

The `src/` package, Python 3.11 Docker image, Java 17, PySpark 3.5.3, pytest, Ruff, `.env.example`, and Git exclusions established a reproducible local baseline. A real SparkSession read a three-row DataFrame, counted three rows, filtered two, and shut down. `.env` is ignored and excluded from the Docker build context.

## Phase 2 — TLC ingestion

The Python CLI constructs the official `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<year>-<month>.parquet` URL. It streams to `.part`, applies bounded retries and HTTP/network error handling, validates nonzero size and the PyArrow Parquet footer, then atomically promotes the result. An existing valid file is revalidated and skipped unless `--force` is requested. A generated JSON manifest records source URL, period, filename, retrieval time, and size. The January 2024 raw file was 49,961,641 bytes at `data/raw/yellow/2024/01/`; a second real run skipped it. Nine tests passed at this phase.

## Phase 3 — Bronze

Spark preserved all 19 source columns and 2,964,624 January rows, adding `_bronze_ingested_at`, `_source_file`, `_source_taxi_type`, `_source_year`, and `_source_month`. The 24-column output was one 61,639,367-byte Snappy Parquet part at `data/bronze/yellow/year=2024/month=01/`. No business cleaning occurs here. A temporary sibling write is read back before target-month replacement; an identical rerun did not duplicate records or disturb another month. Twelve tests passed; the final local run took about 24 seconds.

## Phase 4 — Silver and quarantine

Silver standardizes `VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, and `Airport_fee` to snake_case, uses fixed-scale monetary types, derives pickup date and duration, preserves Bronze lineage, and adds `_silver_processed_at`. Its eight rules reject missing pickup/dropoff timestamps, dropoff before pickup, negative trip distance/fare/total, and missing or non-positive pickup/dropoff location IDs. Zero distance and null passenger count alone are retained. Rejected rows keep an array of all applicable rule identifiers. January reconciled 2,964,624 Bronze = 2,927,000 valid + 37,624 quarantined; prominent failures were 56 timestamp-order, 37,448 negative-fare, and 35,504 negative-total (overlap is possible). Temporary paired writes and rollback protect both partitions. Fifteen tests passed.

## Phase 5 — Four Gold marts

Valid Silver alone feeds `daily_trip_metrics` (pickup date), `hourly_demand` (pickup date/hour), `pickup_location_performance` (location ID), and `payment_type_summary` (numeric payment type). January yielded 35, 749, 260, and 5 rows respectively; each mart's summed trip count reconciled to 2,927,000. Payment percentages reconciled to 100% within tolerance. `total_revenue` sums TLC `total_amount`: a trip-charge measure, not company accounting revenue. The four monthly outputs are validated before coordinated replacement and retain period-level Gold lineage. Eighteen tests passed.

## Phase 6 — Taxi Zone reference and enrichment

The official TLC Taxi Zone CSV is validated independently for required columns, unique integer `LocationID`, and nonblank geography. January's lookup had 265 rows and no invalid keys. A broadcast left join enriches the pickup-location mart while retaining unmatched IDs as null geography and reporting match rates. The separate `pickup_zone_performance` mart had 260 January rows and represented all 2,927,000 valid trips with 100% location matching. Twenty-six tests passed.

## Phase 7 — Incremental periods and replay

The CLI orchestrator records atomic period/run state under ignored `data/state/`. Complete periods are skipped, existing valid outputs can be bootstrapped, and explicit replay starts at a selected stage. January–June reconciled 20,332,093 Bronze = 20,015,099 valid Silver + 316,994 quarantine. Repeating the identical incremental range skipped all six periods in 4.719 seconds, without new monthly Spark transformations or taxi-file downloads. A March Silver-onward replay left Raw and Bronze byte-identical and regenerated Silver, four Gold marts, and geographic enrichment. This CLI state is distinct from later Airflow task state.

## Phase 8 — Source schema contract

A versioned Yellow Taxi v1 contract covers the 19 source columns. PyArrow metadata-only inspection compares additions, removals, types, and nullability; an order-independent SHA-256 fingerprint identifies the logical schema. `COMPATIBLE`, `WARNING`, and `BREAKING` decisions gate Bronze rebuilds. A nullable addition can proceed; missing required fields and breaking type changes stop the run. All six real January–June schemas matched the contract; no actual source evolution was observed. Audit reports are generated runtime state, not contract updates. Synthetic evolution tests cover the nonmatching cases.

## Phase 9 — MinIO and Iceberg

The filesystem pipeline remains the default; explicit Iceberg mode publishes its validated monthly outputs. Spark uses Iceberg 1.10.1 with an embedded SQLite JDBC catalog pointing to a MinIO `s3a://` warehouse. This local catalog is single-writer, not a production metastore. Bronze, valid Silver, quarantine, and five Gold marts are eight independently committed, source-period-partitioned tables. The January–June migration read existing validated Parquet without rerunning business transformations. A repeated January Bronze load retained 2,964,624 rows while creating a new snapshot; an isolated two-month test proved another period survived. Table-level commits are atomic; no cross-table transaction is claimed.

## Phase 10 — Airflow control plane

The paused-by-default `nyc_taxi_monthly_lakehouse` DAG uses Airflow 2.10.5, LocalExecutor, and PostgreSQL metadata. It calls existing processor adapters rather than duplicating transformations. Ingestion, schema gate, Bronze, Silver, Gold, geography, optional Iceberg publication, and reconciliation were the original eight tasks; Phase 11 added serving publication as the ninth. A breaking schema blocks downstream tasks; filesystem mode skips only optional Iceberg publication. Small metadata travels through XCom, never trip DataFrames. March backfill was dry-run only; isolated `dag.test()` runs verified task success, skip, and failure states.

## Phase 11 — PostgreSQL serving and Superset

Five compact Gold Parquet marts are published into an `analytics` PostgreSQL database, separate from Airflow and Superset metadata databases. One transaction replaces all five marts for a source month, compares business values and trip totals, and rolls back on any failure. January–June each reconciled to valid Silver, with serving row totals 207 daily, 4,408 hourly, 1,550 location, 31 payment, and 1,550 zone. Superset 6.0.0 queries PostgreSQL, not raw lake files. An idempotent REST bootstrap provisions one dashboard, five datasets, and ten charts. All ten chart-data calls and a signed-in browser rendering passed; the top-zones chart displayed a row-limit warning.

## Phase 12 — Failure recovery

Injected failures covered downloads, schema gates, local partition promotions, MinIO outages/Iceberg pre-commit, PostgreSQL outage/third-mart insert, reconciliation corruption, Airflow task states, and dashboard definitions. Failed local promotions retain prior partitions; a failed Iceberg publication retains the prior snapshot and never silently falls back to filesystem; a failed serving insert rolls back all five tables in that period. Airflow marks the faulted task failed and dependent tasks upstream-failed. Isolated namespaces/schemas protected historical data. The full local regression at this phase passed 130 tests.

## Phase 13 — CI quality gates

GitHub Actions runs `Quality`, `Fast Tests`, `Docker Build`, and `Integration Tests` on push to main and pull requests. Quality covers Ruff, workflow YAML, Bash syntax, Compose, whitespace, and tracked-file hygiene. Fast tests use Python 3.11/Java 17; integration starts fresh MinIO/PostgreSQL services and exercises Airflow, Iceberg, and serving without historical TLC downloads. Two heavy tests and visual browser checks remain manual. A CI-only Compose profile error was fixed in `84dde5a`; [the subsequent hosted run](https://github.com/harshitha-108/nyc-taxi-lakehouse/actions/runs/35866909898) passed all four jobs. The workflow validates but does not deploy. The complete local regression then passed 143 tests.

## Phase 14 — Measured performance work

The January Gold persisted-input baseline had runs of 62.472, 59.916, and 58.972 seconds (median 59.916). After removing persistence, modified production runs were 37.296, 36.362, and 33.899 seconds (median 36.362): an observed 39.31% lower local median for the same January workload in separate, similarly warmed Docker sessions. Business-row digests, enriched geography, Iceberg/serving publications, failure recovery, and January–June read-only reconciliations remained correct. The physical plan no longer contains `InMemoryTableScan`, while `HashAggregate`, `Exchange`, and AQE remain.

Other experiments were **not** adopted: shuffle partition changes showed no reliable benefit; Silver `repartition(2)` was slower and produced larger output than `coalesce(1)`; an extra PostgreSQL index saved only 0.368 ms on a 1,550-row mart while adding maintenance. Existing Iceberg January pruning reduced planned scan tasks from six to one, and the Taxi Zone join already broadcast the small dimension, so neither needed a change. These are local workload observations, not cold-start guarantees or evidence for billion-row scale. The complete local regression passed 155 tests; the [implementation CI run](https://github.com/harshitha-108/nyc-taxi-lakehouse/actions/runs/35905605539) passed all four jobs.
