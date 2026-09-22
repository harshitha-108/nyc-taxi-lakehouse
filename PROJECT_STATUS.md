# NYC Taxi Lakehouse — Project Status

## Current Phase

Phase 3 — Bronze Layer

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

## Current Architecture

One local Docker Compose `pipeline` service provides Python 3.11, Java 17, PySpark, and PyArrow. The
pipeline can download official Yellow Taxi source Parquet files into local raw storage, produce lineage
manifests, and create source-aligned Bronze Parquet partitions. Silver/Gold transformations, MinIO,
Airflow, dbt, Superset, and dashboards are not implemented.

## Environment

- Docker Desktop 4.55.0 / Docker Engine 29.1.3
- Docker Compose v2.40.3
- Python 3.11.16 (container)
- OpenJDK 17.0.20.1 (container)
- PySpark 3.5.3 (container)
- PyArrow 18.1.0 (container)

## How to Run

Docker Desktop must be running. These commands were verified during Phase 1:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check src tests
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.ingestion.nyc_taxi --year 2024 --month 1
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.bronze.processor --taxi-type yellow --year 2024 --month 1
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

## Tests

- `pytest`: 12 tests passed, including raw-to-Bronze Spark integration, source preservation, lineage,
  and partition-level idempotency tests.
- `ruff check src tests`: passed.
- Spark environment smoke test: passed.

## Known Issues

None. Spark's missing `ps` utility and native Hadoop library warnings during the Phase 1 smoke test
are expected for this minimal local container and did not affect execution.

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

## Next Phase

Phase 4 — Silver layer. This phase has **not** started.
