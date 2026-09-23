# NYC Taxi Lakehouse — Project Status

## Current Phase

Phase 10 — Airflow Production Orchestration (PASS)

## Completed

- Initial Git repository scaffold on `main`
- `src/`-layout Python package: `src/nyc_taxi_lakehouse/`
- Local runtime directories for raw, Bronze, Silver, Gold, and quarantine data, retained with
  `.gitkeep` files only
- Docker Compose development service with Python, Java, and PySpark
- Dependency, pytest, and Ruff configuration
- README, environment template, Git ignore rules, and Docker build-context exclusions
- Reusable Yellow Taxi ingestion module and `python -m` CLI
- Atomic `.part` download behavior, bounded retries, Parquet footer validation, and error handling
- Generated raw-data lineage manifests
- Deterministic mocked ingestion tests and one official TLC integration download
- Source-aligned PySpark Bronze processor and CLI
- Temporary-write, read-back validation, and partition-level replacement behavior
- Spark Bronze integration tests using small local Parquet fixtures
- PySpark Silver processor and CLI with standardized analytical column names and fixed-scale monetary
  values
- Valid Silver and quarantine outputs with technical lineage, row-level derived fields, and explicit
  quality-failure reasons
- Temporary paired writes, read-back validation, and partition-level replacement for Silver and
  quarantine outputs
- Spark Silver integration tests using small local Bronze fixtures
- PySpark Gold analytics processor and CLI reading valid Silver data only
- Four validated Gold marts: daily trip metrics, hourly demand, pickup location performance, and
  payment type summary
- Temporary multi-dataset writes, Spark read-back validation, and coordinated partition replacement
- Synthetic Gold unit and integration coverage for metrics, grains, reconciliation, percentages, and
  partition-level idempotency
- Official TLC Taxi Zone Lookup ingestion with CSV/key validation, safe partial-file handling, runtime
  lineage manifests, and idempotent local reuse
- Broadcast left-join enrichment and a new `pickup_zone_performance` Gold mart
- Reference-match monitoring, aggregate reconciliation, and metric-consistency validation against the
  existing pickup-location mart
- Stateful multi-month orchestration for ingestion through geographic enrichment, with atomic local
  period/run state, incremental skip behavior, bootstrap adoption, and explicit stage replay
- Yellow Taxi source contract v1 and metadata-only PyArrow schema validation before Bronze rebuilds
- Deterministic logical-schema fingerprints, compatibility decisions, and atomic runtime audit reports
- Synthetic schema-evolution and orchestration-gate regression tests
- Local MinIO service, idempotent bucket bootstrap, and pinned Iceberg/S3A dependencies
- Explicit Iceberg storage mode alongside the default filesystem Parquet pipeline
- SQLite JDBC Iceberg catalog, eight period-partitioned tables, migration/inspection/smoke CLIs
- January–June historical migration with per-period quality and Gold reconciliations
- Real MinIO/Iceberg snapshot, overwrite, time-travel, and breaking-schema-gate integration tests
- Airflow monthly control-plane DAG with eight explicit stage tasks and stage-level adapters
- LocalExecutor with a separate PostgreSQL metadata database, Docker health checks, and idempotent init
- Paused-by-default schedule, manual period override, bounded backfill dry-run, and task-level retries
- Isolated real Airflow DAG-run tests for success, optional skip, and breaking-schema blocking

## Current Architecture

One local Docker Compose `pipeline` service provides Python 3.11, Java 17, PySpark, and PyArrow. The
pipeline can download official Yellow Taxi source Parquet files into local raw storage, produce lineage
manifests, create source-aligned Bronze Parquet partitions, and produce valid Silver and quarantined
Silver partitions. It also creates four analytics-ready Gold Parquet datasets from valid Silver data.
Phase 6 adds an independent official Taxi Zone reference dataset and an enriched pickup-zone Gold mart.
The orchestrator also validates the Raw schema against the committed Yellow Taxi contract before any
Bronze rebuild. Audit reports are runtime state, not business data. Historical successful Phase 7
periods remain readable and skip processing. Phase 9 adds an optional Iceberg publication path: local
transformation outputs remain in place, while Spark commits period-scoped Iceberg tables whose metadata
and data live in MinIO. A local SQLite JDBC catalog stores table pointers. Phase 10 adds an Airflow DAG
that coordinates the same stage processors with separate task states, optional Iceberg publication,
and final reconciliation. Airflow uses PostgreSQL for orchestration state and leaves the Phase 7 CLI
state files intact; dbt, Superset, and dashboards are not implemented.

## Environment

- Docker Desktop 4.55.0 / Docker Engine 29.1.3
- Docker Compose v2.40.3
- Python 3.11.16 (container)
- OpenJDK 17.0.20.1 (container)
- PySpark 3.5.3 (container)
- PyArrow 18.1.0 (container)
- Hadoop 3.3.4 / Scala 2.12.18 (verified from Spark in the container)
- Apache Iceberg Spark runtime 3.5_2.12 version 1.10.1
- Hadoop AWS 3.3.4 / AWS Java SDK bundle 1.12.262 / SQLite JDBC 3.49.1.0
- MinIO RELEASE.2025-09-07T16-13-09Z (Quay image; loopback-only local development)
- Iceberg JDBC catalog: `lakehouse`, SQLite at `data/state/iceberg_catalog.db`
- MinIO bucket `nyc-taxi-lakehouse`; warehouse `s3a://nyc-taxi-lakehouse/warehouse`
- Airflow 2.10.5 on Python 3.11.16, LocalExecutor, PostgreSQL 16-alpine metadata database
- DAG `nyc_taxi_monthly_lakehouse`; monthly UTC interval, paused on creation, `catchup=False`
- Airflow web UI bound to `127.0.0.1:8080`; scheduler/webserver run as UID 50000

## How to Run

Docker Desktop must be running. Commands below were verified across the completed phases:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check src tests
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.ingestion.nyc_taxi --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.bronze.processor --taxi-type yellow --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.silver.processor --taxi-type yellow --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.gold.processor --taxi-type yellow --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.reference.taxi_zones
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.gold.geographic --taxi-type yellow --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.schema.validator --taxi-type yellow --start 2024-01 --end 2024-06
docker compose up -d minio minio-init
docker compose run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.storage.smoke
docker compose run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.storage.migrate --start 2024-01 --end 2024-06
docker compose run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.storage.inspect
docker compose run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.orchestration.pipeline --taxi-type yellow --start 2024-01 --end 2024-01 --mode incremental --storage-backend iceberg
docker compose run --rm --no-deps -e RUN_ICEBERG_INTEGRATION=1 pipeline pytest
docker compose --profile airflow build pipeline airflow-init
docker compose --profile airflow up -d airflow-postgres airflow-scheduler airflow-webserver
docker compose --profile airflow ps
docker compose --profile airflow run --rm --no-deps --user airflow airflow-init airflow dags list
docker compose --profile airflow run --rm --no-deps --user airflow airflow-init airflow dags backfill nyc_taxi_monthly_lakehouse --start-date 2024-03-01 --end-date 2024-03-01 --dry-run
docker compose --profile airflow run --rm --no-deps --user airflow -e RUN_ICEBERG_INTEGRATION=1 airflow-init pytest -p no:cacheprovider -q
```

## Validation Completed

- `docker compose config` passed.
- The Docker image built successfully after pinning the Python 3.11 base image to Debian Bookworm,
  which supplies Java 17.
- PySpark imported successfully.
- A local SparkSession started, read a three-row DataFrame schema, returned a count of 3, completed a
  filter action returning 2 rows, and shut down cleanly.
- `.env` is ignored and is not tracked.
- Yellow Taxi January 2024 was downloaded from the official TLC URL, saved as
  `data/raw/yellow/2024/01/yellow_tripdata_2024-01.parquet`, and validated as readable Parquet.
- The downloaded file was 49,961,641 bytes. Its generated manifest is under
  `data/raw/metadata/yellow/2024/01/`.
- The same CLI command was run again and skipped the valid existing file without a re-download.
- The Bronze job processed the existing Yellow Taxi January 2024 raw file, preserving 2,964,624 rows
  and all 19 source columns while adding 5 technical lineage columns.
- The Bronze output is `data/bronze/yellow/year=2024/month=01/`: 1 Snappy Parquet file totaling
  61,639,367 bytes, 2,964,624 rows, and 24 columns.
- A repeated Bronze run replaced only the January target partition; it retained 1 part file and the
  same 2,964,624-row result. The final measured run took approximately 24 seconds.
- Bronze January profiling found no null pickup/dropoff timestamps, 56 dropoffs before pickups, no
  negative distances, 37,448 negative fares, 35,504 negative totals, and no missing/non-positive
  location IDs. `passenger_count` was null for 140,162 rows, so it is retained rather than rejected.
- Pickup timestamps ranged from `2002-12-31 22:59:39` to `2024-02-01 00:01:15`; dropoff timestamps
  ranged from `2002-12-31 23:05:41` to `2024-02-02 13:56:52`. These out-of-period records are
  preserved because Phase 4 does not impose an arbitrary timestamp-window rejection rule.
- The Silver job processed the January Bronze partition into 2,927,000 valid rows and 37,624
  quarantined rows, satisfying `2,964,624 = 2,927,000 + 37,624`.
- The valid output is `data/silver/yellow/year=2024/month=01/` (1 Snappy Parquet file, 65,398,600
  bytes, 27 columns). The quarantine output is
  `data/quarantine/silver/yellow/year=2024/month=01/` (1 Snappy Parquet file, 897,649 bytes).
- January Silver rule failures were: 56 `INVALID_TIMESTAMP_ORDER`, 37,448
  `NEGATIVE_FARE_AMOUNT`, and 35,504 `NEGATIVE_TOTAL_AMOUNT`; the other five implemented rules had
  zero failures. Counts may overlap for a record with multiple reasons.
- A repeated Silver run replaced the January valid and quarantine partitions and produced the same
  reconciliation counts. The final measured run took 69.61 seconds.
- Gold processed 2,927,000 valid January Silver trips only. `total_revenue` is the sum of TLC
  `total_amount`, totaling 80,342,626.37 for the processed valid rows.
- Gold output layout is `data/gold/<dataset>/yellow/year=2024/month=01/`. The January output has 35
  daily-trip rows (14 columns, 6,507 bytes), 749 hourly-demand rows (10 columns, 22,060 bytes), 260
  pickup-location rows (11 columns, 13,863 bytes), and 5 payment-type rows (12 columns, 3,948 bytes).
  Each output has one Snappy Parquet part file.
- Each Gold mart's summed `trip_count` reconciled to the 2,927,000-row Silver input. Payment trip and
  total-revenue percentages each reconciled to 100% within floating-point tolerance.
- January's highest-volume day was 2024-01-18 (109,088 trips); the busiest date-hour was
  2024-01-17 at 18:00 (9,027 trips); pickup location ID 161 led with 141,742 trips; and payment type
  1 led with 2,319,009 trips (79.2282%). Average trip distance was 3.6605 and average duration was
  15.6586 minutes.
- The first Gold run took 49.17 seconds. A second run took 71.01 seconds and safely replaced all four
  January output partitions with identical row counts and reconciliations.
- The official TLC Taxi Zone Lookup was downloaded from
  `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` to
  `data/reference/taxi_zones/taxi_zone_lookup.csv`. It is 12,331 bytes with 265 rows and 265 unique
  LocationIDs; duplicate, null/invalid LocationID, and blank Borough/Zone/service_zone counts were all
  zero.
- The enriched `pickup_zone_performance` mart has 260 rows, 14 columns, one 18,362-byte Snappy Parquet
  file, and is located at `data/gold/pickup_zone_performance/yellow/year=2024/month=01/`. A broadcast
  left join matched all 260 distinct source IDs (100%); no unmatched IDs or trips were affected.
- The mart reconciled to all 2,927,000 valid Silver trips and the existing pickup-location mart. Core
  metrics had zero differences by `pickup_location_id`. Location 161 is Manhattan / Midtown Center /
  Yellow Zone with 141,742 trips. The first run took 16.37 seconds; the idempotent rerun took 16.59
  seconds.
- January–June 2024 Raw schemas all matched the 19-field Yellow Taxi v1 contract. Every month had the
  same logical SHA-256 fingerprint:
  `bb50ef4789e8c8308c722cc37849a2b7a0d7e46869a01909b69db4c7e3714ff7`.
  All six checks were `COMPATIBLE` / `PASS` with zero changes; no real schema evolution was observed.
- Final per-period metadata validation times were 0.038669, 0.037121, 0.037774, 0.032761, 0.037697,
  and 0.045336 seconds for January through June respectively. Six corresponding JSON reports under
  `data/state/schema/yellow/2024/` were verified and are ignored by Git.
- Breaking schema tests blocked Bronze and all downstream stages and recorded a schema-validation
  failure at both period and run level. Compatible added-column, replay-order, backfill-order, legacy
  state, January bootstrap, and incremental skip tests passed.
- Schema compatibility levels are `COMPATIBLE`, `WARNING`, and `BREAKING`; severity is preserved as
  `BREAKING > WARNING > COMPATIBLE` when multiple changes occur. Synthetic tests cover malformed
  contracts and show that an added nullable field can proceed through the orchestration gate.
- The final metadata-only audit caused zero size or modification-time changes among 210 existing Raw,
  Bronze, Silver, quarantine, and Gold files inspected.
- MinIO started from the pinned Quay image and its initializer created the bucket; rerunning the
  initializer completed successfully against the existing bucket. The API and console bind to
  `127.0.0.1` only.
- A real isolated Spark/Iceberg/MinIO smoke test wrote two January rows and one February row, rewrote
  January without changing February, observed a new snapshot, and queried the earlier snapshot.
- January–June 2024 filesystem outputs were migrated into eight `lakehouse.nyc_taxi` Iceberg tables.
  Per-month Bronze/Silver/quarantine counts: January 2,964,624/2,927,000/37,624; February
  3,007,526/2,966,785/40,741; March 3,582,628/3,523,905/58,723; April
  3,514,289/3,456,486/57,803; May 3,723,833/3,663,653/60,180; June
  3,539,193/3,477,270/61,923. Each month reconciled in both filesystem and Iceberg.
- Fresh Iceberg reads returned 20,332,093 Bronze, 20,015,099 valid Silver, and 316,994 quarantine
  rows, matching existing filesystem history. Each of the eight tables has six active data files.
  Gold row totals are daily 207, hourly 4,408, pickup location 1,550, payment 31, and pickup zone
  1,550. Every Gold mart's `trip_count` reconciled to valid Silver each month, and exact Gold values
  matched local Parquet in both directions.
- Migrated pickup-zone data had non-null zone names for all 1,550 location-period rows; each month's
  `trip_count` reconciled to valid Silver (100% geographic match at this level).
- January Bronze was migrated twice: both writes read back 2,964,624 rows; snapshot IDs changed from
  `6233696612276549882` to `3815177538866729148`. The final six-month Bronze total has no duplicate
  month and six active data files. The isolated two-month test also proved unrelated-month retention.
- MinIO inspection found 57 Iceberg metadata JSON objects and 49 Parquet objects (48 active table
  files plus one historical Bronze file). Bronze had eight snapshots; the other tables had seven each.
- One-month full January migration took 87.27 seconds; February–June took 82.79, 71.67, 70.80, 73.58,
  and 70.24 seconds per month respectively. These are local measurements, not performance claims.
- The first Iceberg-mode January incremental call adopted migrated outputs; the second recorded
  `processed=0 skipped=1` without a download or transformation. A real breaking source-schema test
  prevented any Bronze Iceberg table from being created in an isolated catalog.
- Airflow metadata initialization and local admin-user creation succeeded against dedicated
  PostgreSQL. Re-running initialization succeeded without duplicating the account. The initializer
  adjusted only the existing root-owned, ignored Iceberg catalog file, then dropped privileges.
- MinIO, PostgreSQL, Airflow scheduler, and Airflow webserver reported healthy. The DAG listed as
  paused with eight tasks and zero import errors. Airflow 2.10.5 retained PySpark 3.5.3 and PyArrow
  18.1.0 in its Python 3.11 image.
- Real Airflow `dag.test()` runs with stubbed data-plane stages proved `success` task states, a
  `publish_iceberg` skip with successful reconciliation in filesystem mode, and a failed
  `validate_schema` task leaving Bronze `upstream_failed`. A compatible schema stub allowed Bronze
  and Iceberg publication to succeed. These tests did not rewrite historical datasets.
- The March 2024 Airflow backfill `--dry-run` rendered all eight tasks and made no data changes.
  Period tests covered January, December, the year boundary, and an explicit March replay override.
- After the Compose change, MinIO bucket initialization succeeded and a fresh Iceberg read returned
  20,332,093 total Bronze rows and 2,964,624 January rows (six active files; eight snapshots).

## Tests

- `pytest`: 76 tests passed in 100.06 seconds as the non-root Airflow user with
  `RUN_ICEBERG_INTEGRATION=1`, preserving the 61-test Phase 9 baseline and adding Airflow DAG,
  period, schema-gate, storage-mode, and real DAG-run state tests.
- `ruff check src tests airflow`: passed.
- Spark environment smoke test: passed.

## Known Issues

The README roadmap retains historical placeholder labels for Phases 6–7, and older overview/reference
text still describes MinIO and Airflow as planned; strict README edit rules preserve those earlier
sections while the Phase 9–10 implementation subsections describe the current system. The pinned
community MinIO image is archived and should not be treated as a
production security baseline. SQLite JDBC is a local single-writer catalog, and the eight tables do
not share one cross-table transaction. A proposed real March Bronze overwrite was blocked by the
safety review because an earlier project instruction reserved March replay for Silver onward; no
March Bronze commit occurred. Real January double-load and an isolated two-month overwrite
provide the Phase 9 idempotency proof instead. Spark's missing `ps`, native Hadoop library, and S3A
metrics-config warnings did not affect execution. Airflow CLI warns that optional Graphviz is absent,
so graphical CLI rendering is unavailable; the UI and DAG parser work. The Airflow service setup is
for local development, not a production security baseline. The monthly source may not yet be
published when a current interval closes, so the paused DAG should only be enabled with awareness of
TLC release timing. No real historical Airflow backfill or full Spark transformation was run in this
phase; isolated Airflow execution and a March dry-run establish control-plane behavior.

## Architecture Decisions

- Use a `src/`-layout Python package so pipeline jobs, tests, and future orchestration import the same
  code.
- Use Docker for a reproducible local environment without requiring host Python or Spark installation.
- Use PySpark to establish the distributed processing engine that later transformations will use.
- Keep the initial architecture local-first and free of managed cloud services.
- Exclude generated data directories from Git while preserving their structure with `.gitkeep` files.
- Handle secrets through ignored environment files; `.env.example` contains only safe development
  values, and `.dockerignore` prevents local `.env` from entering image build contexts.
- Store raw files by taxi type/year/month to make source periods independently addressable and prepare
  for future partition-aware processing.
- Validate downloaded Parquet through PyArrow footer metadata, keeping ingestion lightweight and
  separate from later Spark transformations.
- Make reruns idempotent by skipping an existing valid file; use atomic promotion from `.part` files
  to avoid accepting interrupted downloads.
- Keep Bronze source-aligned: preserve TLC business data unchanged and add only technical lineage.
- Use a validated temporary sibling directory before replacing exactly one Bronze year/month partition.
- Standardize only at Silver, where canonical snake_case names and decimal money fields establish an
  analytical contract without changing Bronze source representation.
- Quarantine instead of silently dropping invalid rows; retain all applicable rule identifiers so
  quality remediation and observability remain possible.
- Keep zero-distance trips and nullable passenger counts because the January profile did not justify
  rejecting them for this analytical contract.
- Promote temporary Silver and quarantine partitions as a pair only after Spark read-back validation.
- Build Gold from valid Silver only, so quarantined records cannot silently influence analytical
  aggregates.
- Use distinct, business-question-driven marts rather than one generic aggregate. Each mart has an
  explicit grain and validates that its trip counts reconcile to the Silver input.
- Treat `sum(total_amount)` as a TLC trip-charge/revenue-like measure, not a financial accounting
  revenue assertion.
- Add period-level Gold lineage rather than fabricating a row-level source filename for aggregates.
- Write and validate all four Gold partitions before promoting them together, preserving a rollback
  path if one mart fails.
- Keep slowly changing reference data outside monthly taxi fact partitions and validate its business key
  before use.
- Broadcast the small Taxi Zone dimension and left-join it to preserve fact rows while avoiding an
  unnecessary large shuffle; monitor unmatched keys explicitly.
- Preserve the Phase 5 Gold contract by adding a separate enriched mart and reconcile its metrics to
  the existing location-performance dataset.
- Use explicit period state and artifact validation to make incremental runs skip only complete
  partitions; retain replay as an intentional operator action rather than implicit recovery.
- Keep the expected source schema in a version-controlled contract; use metadata-only inspection and
  an order-independent fingerprint to detect structural changes before Bronze rebuilds.
- Preserve historical completed-period state without rewriting it when adding the schema-validation
  stage. Keep runtime audit records separate from the contract and ignored by Git.
- Keep filesystem mode as the default; only explicit Iceberg mode publishes each stage after its
  existing local output, while historical migration reads validated Parquet without re-transforming.
- Use a SQLite JDBC catalog because the Iceberg Hadoop catalog requires atomic rename unavailable on
  S3-compatible object storage. Keep catalog state local/ignored and Iceberg data plus metadata in
  MinIO. This is a local single-writer compromise, not a production metastore recommendation.
- Partition Iceberg by source taxi type/year/month and overwrite by an explicit source-period filter;
  preserve independent table snapshots and reject failed storage writes without silent fallback.
- Keep Airflow as a control plane with explicit task dependencies; call reusable stage processors at
  task runtime rather than nesting the entire Phase 7 orchestrator or importing Spark at DAG parse.
- Use LocalExecutor with one active run and PostgreSQL metadata, avoiding Celery/Redis for local use.
  Keep Airflow task state distinct from Phase 7 CLI state and the Iceberg SQLite catalog.
- Return only small JSON-safe metadata through XCom; use existing partition replacement for data
  idempotency, not Airflow retry state as a substitute.

## Next Phase

Phase 11 — Analytics database and dashboard. This phase has **not** started.
