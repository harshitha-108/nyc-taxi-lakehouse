# NYC Taxi Lakehouse — Project Status

## Current Phase

Phase 13 — CI/CD Quality Gates & Automated Validation (PASS)

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
- Separate PostgreSQL analytics and Superset metadata databases/owner roles on the existing server
- Five period-keyed, indexed serving tables populated from validated compact Gold Parquet
- One-transaction-per-period five-mart replacement with value, count, and revenue reconciliation
- Real isolated PostgreSQL idempotency, unrelated-month, and rollback tests
- Airflow `publish_serving` task with filesystem/Iceberg compatibility and final serving reconciliation
- Pinned Superset 6.0.0 local dashboard, REST bootstrap, and ten saved PostgreSQL-backed charts
- Read-only six-month serving reconciliation and reusable SQL examples
- Fail-closed schema-contract checks, isolated stage failure/replay tests, and compact period-level
  reconciliation across local Parquet, Iceberg, and PostgreSQL serving
- MinIO/Iceberg snapshot recovery and PostgreSQL five-mart rollback/retry tests using isolated state
- Airflow task-state failure matrix and dashboard definition preflight/regression tests
- GitHub Actions `CI` workflow with separate Quality, Fast Tests, Integration Tests, and Docker Build
  jobs; push-to-main and pull-request triggers, branch/PR concurrency, and read-only permissions
- Explicit pytest `integration`, `docker`, and `heavy` markers, plus a tracked-file hygiene guard
- Fresh-run CI service configuration, MinIO/PostgreSQL health-gated integration, and failure diagnostics

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
state files intact. Phase 11 adds a separate PostgreSQL analytics database containing only compact
Gold-derived marts, plus a third PostgreSQL database for Superset metadata. The serving publisher
reads the validated local Gold Parquet path in both filesystem and Iceberg modes, with one five-table
transaction per source period. Superset queries serving tables only. dbt is not implemented.
GitHub Actions now checks the repository and image build on every main push and pull request. Its
integration job starts fresh MinIO/PostgreSQL services without launching Airflow scheduler/webserver
or Superset; no historical NYC data is used. There is no production deployment target or CD job.

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
- PostgreSQL 16.15: `airflow_metadata`, `nyc_taxi_analytics`, and `superset_metadata` use distinct
  owner roles; serving business tables are under the `analytics` schema
- Apache Superset 6.0.0 image (Python 3.10.19) with `psycopg2-binary==2.9.10`; UI on
  `127.0.0.1:8088`, PostgreSQL-backed metadata, and local in-memory rate limiting
- Hosted CI: Python 3.11, Temurin Java 17 for Spark tests, GitHub-hosted Ubuntu runner; Python
  dependencies come from `requirements.txt`, with pip caching keyed by that file in Fast Tests

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
docker compose --profile airflow --profile dashboard build pipeline airflow-init superset
docker compose --profile airflow run --rm analytics-db-init
docker compose --profile airflow run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.serving.publisher --taxi-type yellow --start 2024-01 --end 2024-06
docker compose --profile airflow run --rm --no-deps pipeline python -m nyc_taxi_lakehouse.serving.reconciliation --start 2024-01 --end 2024-06
docker compose --profile airflow --profile dashboard up -d superset
docker compose --profile airflow --profile dashboard run --rm --no-deps pipeline python scripts/bootstrap_dashboard.py
docker compose --profile airflow --profile dashboard run --rm --no-deps pipeline python -m scripts.smoke_dashboard
docker compose --profile airflow --profile dashboard run --rm --no-deps --user airflow -e RUN_ICEBERG_INTEGRATION=1 -e RUN_SERVING_INTEGRATION=1 -e RUN_SUPERSET_INTEGRATION=1 airflow-init pytest -p no:cacheprovider -q
```

Phase 13 developer checks (use the running local MinIO/PostgreSQL services for service tests):

```powershell
docker compose --profile airflow --profile dashboard run --rm --no-deps pipeline ruff check src tests airflow scripts
docker compose --profile airflow --profile dashboard config --quiet
python scripts/check_repository_hygiene.py
docker compose --profile airflow run --rm --no-deps pipeline pytest -p no:cacheprovider -m "not docker and not heavy" -q
docker compose --profile airflow run --rm --no-deps --user airflow -e RUN_ICEBERG_INTEGRATION=1 -e RUN_SERVING_INTEGRATION=1 airflow-init pytest -p no:cacheprovider -m "docker and not heavy" -q
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
- January Gold-to-serving publication and an identical rerun both represented 2,927,000 trips in
  each of the five marts. An isolated PostgreSQL test proved target-period replacement, unchanged
  February data, and rollback of all five January tables after a duplicate-key insert failure.
- January–June serving publication reconciled every Gold business value and all five mart trip totals
  to 20,015,099 valid Silver rows. Monthly valid-trip counts were 2,927,000; 2,966,785; 3,523,905;
  3,456,486; 3,663,653; and 3,477,270. Serving mart row totals were daily 207, hourly 4,408,
  pickup-location 1,550, payment 31, and pickup-zone 1,550.
- Per-month serving publication times were 0.69, 0.73, 1.12, 1.05, 0.77, and 0.84 seconds for
  January–June respectively. Five read-only serving SQL examples took 0.0030–0.0055 seconds each
  in one local client-observed run; no lakehouse-vs-PostgreSQL speedup is claimed.
- January–June highest-volume pickup date was 2024-05-16 (141,673 trips); hour 18 had 1,434,476
  trips; Manhattan had 17,845,670 pickup trips; Manhattan/Midtown Center led zones with 941,218;
  payment type 1 accounted for 15,111,735. These values came from serving SQL, not hard-coded logic.
- The real Airflow serving-stage adapter republished January successfully in filesystem and Iceberg
  modes. Isolated DAG runs verified the nine-task order, schema blocking, optional Iceberg skip, and
  serving failure isolation (Gold/Iceberg success retained, serving/downstream failed). The live
  scheduler listed nine task IDs and zero DAG import errors when invoked through its entrypoint.
- Superset initialization succeeded in its own PostgreSQL metadata database, and the service became
  healthy; re-running its initializer exited successfully without recreating the account. The
  idempotent REST bootstrap created one analytics connection, five serving datasets,
  ten charts, two scoped filters, and the `NYC Urban Mobility Overview` dashboard. All ten saved
  charts executed through Superset's chart-data API and returned rows.
- Read-only Iceberg inspection after publication found the existing eight tables with six active
  data files each and unchanged September 22 snapshot commit times; Bronze, Silver, quarantine,
  and five Gold row totals remained 20,332,093 / 20,015,099 / 316,994 / 207 / 4,408 / 1,550 /
  31 / 1,550. The latest local Raw/Bronze/Silver/Gold file timestamp predated Phase 11.

### Phase 12 reliability validation

- Injected a transient download error followed by a successful bounded retry, and a failed download
  that left no final Raw file or reusable partial file. A breaking schema blocks downstream work;
  malformed contracts fail closed. Added/reordered fields and severity precedence were tested.
- Isolated Bronze, Silver, Gold, and geographic promotion failures preserved previously valid
  partitions. Five Phase 7 stage-failure cases recorded failed/pending state and recovered with
  narrow replay. Existing legacy state without newer stage keys still loads.
- A dedicated Iceberg namespace/table retained its prior snapshot after an unreachable MinIO
  endpoint and a separate pre-commit failure. Recovery created a new snapshot; repeated January
  overwrite yielded one row and unrelated February remained one row. Iceberg mode did not silently
  fall back to filesystem mode. S3A filesystem caching was disabled so changed endpoint settings
  cannot reuse a prior filesystem instance; outage/recovery test phases use fresh Spark JVMs.
- An isolated PostgreSQL schema preserved all five prior mart versions when the third insert failed.
  Retry replaced all five atomically and remained idempotent; February was unchanged. A deliberately
  unreachable PostgreSQL endpoint failed serving without changing existing Gold or serving state.
- Airflow fixture runs reported the intended failed task, `UPSTREAM_FAILED` downstream tasks, and
  retained upstream successes for ingestion, schema, Silver, Iceberg, serving, and reconciliation
  failures. Serving recovery completed. The nine-task DAG has zero import errors.
- Read-only January–June reconciliation passed for each period: Bronze = valid Silver + quarantine;
  all five Gold marts represent valid Silver trips; eight Iceberg table row counts equal local
  period counts; serving business values and trip totals equal Gold. Overall Bronze 20,332,093 =
  Silver 20,015,099 + quarantine 316,994. All five Gold/serving trip totals are 20,015,099.
- The historical Iceberg Bronze table remained readable at 20,332,093 rows overall and 2,964,624
  January rows. Raw, Bronze, Silver, quarantine, and Gold file counts, byte totals, and
  path/size/mtime SHA-256 metadata fingerprints matched the pre-test snapshot exactly.
- Two Superset REST bootstraps reused dashboard ID 1 with five datasets and ten charts; all ten
  chart-data queries passed. Chart definitions, layout, supported visualization types, and filter
  scope passed static tests. A signed-in Chrome check displayed all ten rendered charts; the Top
  Pickup Zones chart displayed a row-limit warning while still rendering.

### Phase 13 hosted CI validation

- The first pushed workflow revealed a CI-only Compose profile error before integration tests ran.
  Commit `84dde5a` corrected that one command; the failure was reproduced locally and was not a
  test assertion failure.
- A fresh isolated Compose project brought MinIO and PostgreSQL to healthy status, initialized its
  bucket and databases, migrated Airflow metadata, and passed all 19 selected service tests in
  83.83 seconds. Its three temporary volumes and two containers were removed afterward.
- [Hosted CI run 35866909898](https://github.com/harshitha-108/nyc-taxi-lakehouse/actions/runs/35866909898)
  completed successfully on commit `84dde5a` in about 3 minutes 57 seconds: Quality 30 seconds,
  Fast Tests 74 seconds, Docker Build 47 seconds, Integration Tests 156 seconds (job durations from
  GitHub timestamps). All four jobs passed. No historical NYC data or live TLC call was required.
- The Quality job passed Ruff, workflow YAML parse, shell syntax, Compose configuration, tracked-file
  hygiene, and a meaningful changed-line whitespace check. Fast Tests cover contract decisions and
  dashboard-definition regression, including `dist_bar`; Integration Tests cover a real Airflow DAG,
  MinIO/Iceberg publication/recovery, PostgreSQL serving rollback, and five-mart atomicity.

## Tests

- Focused Phase 12 reliability files: 75 passed in 39.11 seconds.
- Final full `pytest`: 130 passed in 383.84 seconds as the non-root Airflow user with
  `RUN_ICEBERG_INTEGRATION=1`, `RUN_SERVING_INTEGRATION=1`, and
  `RUN_SUPERSET_INTEGRATION=1`; the prior 90-test baseline remains green.
- `ruff check src tests airflow scripts`: passed.
- `docker compose --profile airflow --profile dashboard config --quiet`: passed.
- Superset chart-data API: ten of ten charts executed and returned rows.
- Spark environment smoke test: passed.
- Phase 13 test inventory: 143 total (122 service-independent, 19 Docker/service integration,
  two heavy). Registered `integration`, `docker`, and `heavy` markers; no unknown-marker warnings.
- Hosted automatic CI: all four jobs passed on commit `84dde5a`.
- Final complete local Docker regression after documentation: 143 passed in 408.44 seconds,
  including both heavy tests and the previous 130-test baseline.

## Known Issues

The README roadmap retains historical placeholder labels for Phases 6–7, and older overview/reference
text still describes MinIO and Airflow as planned; strict README edit rules preserve those earlier
sections while the Phase 9–11 implementation subsections describe the current system. The pinned
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
TLC release timing. No real historical Airflow backfill or full Spark transformation was run in
Phase 10; isolated Airflow execution and a March dry-run establish control-plane behavior. Phase 11
publishes from local Gold Parquet rather than reading Iceberg directly so serving also works in
filesystem mode; Iceberg and serving remain independently validated paths. The Superset setup is
local-only: it uses in-memory rate limiting and has no Content Security Policy, which Superset warns
about on startup. The dashboard has source-month and scoped pickup-borough filters but no arbitrary
date-range filter across all marts. The REST smoke verifies saved layout and all chart queries; a
  automated browser-based visual-regression test or screenshot was not committed. A signed-in local
  browser inspection in Phase 12 showed all ten charts, with a row-limit warning on Top Pickup
  Zones. No formal database-vs-lakehouse
performance comparison was attempted.
Phase 13 normal PR CI does not automate the two heavy tests, full Superset bootstrap/browser
rendering, or the complete local full regression. These remain local/manual validations; the
GitHub workflow does not deploy the application, publish an image, or enforce branch protection.

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
- Reuse the PostgreSQL server but separate Airflow, analytics, and Superset metadata into different
  databases and owner roles. Keep business tables under `analytics`, not Airflow metadata or `public`.
- Publish the already validated, compact Gold Parquet from both storage modes; do not re-transform
  Raw/Silver or duplicate the 20-million-row trip-level dataset in PostgreSQL.
- Include source period in every serving primary key because the pickup date can fall outside the
  source month. Add only BI-useful date, location, payment, and borough/zone indexes.
- Use one PostgreSQL transaction across all five marts for each period, with read-back, metric, and
  cross-mart reconciliation before commit. Roll back all five on any failure.
- Bootstrap Superset via supported REST APIs and store its metadata separately; use a pinned image
  plus only the missing PostgreSQL driver, without Redis/Celery for this local dashboard.
- Separate fast PR feedback from health-gated MinIO/PostgreSQL integration and Docker build jobs.
  Use official GitHub actions, read-only repository permissions, and branch/PR concurrency.
- Generate ignored CI-only service configuration from the safe template, and reject an existing
  `.env`; never depend on a developer's secrets or historical data in hosted CI.
- Guard tracked files against datasets, credentials, runtime databases, and logs; keep the two
  expensive tests as explicit local/full-regression coverage rather than hiding their exclusion.

## Next Phase

Phase 14 — Performance/scalability measurement. It has **not** started. Establish reproducible
baselines before changing partitioning, Spark execution, or data layout; report only measured
improvements. The Phase 13 documentation-only CI run must pass before final push verification.
