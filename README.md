# NYC Taxi Lakehouse

A local, production-style data engineering project that incrementally builds a lakehouse for official
NYC Taxi & Limousine Commission (TLC) Yellow Taxi trip records. The project is designed to demonstrate
reproducible ingestion, Spark processing, data quality, orchestration, and analytics without requiring
paid cloud services.

## Project overview

The implemented system downloads official TLC Yellow Taxi Parquet data into a local raw layer and
creates a source-aligned Bronze Parquet partition. Future phases will add cleaned Silver data and Gold
analytics; those layers are not implemented yet.

`PROJECT_STATUS.md` records the active development state. This README is cumulative technical
documentation for functionality that has been implemented and validated.

## Architecture

```mermaid
flowchart LR
    TLC[NYC TLC trip records] --> ING[Python ingestion]
    ING --> RAW[Local raw Parquet]
    RAW --> BRONZE[Bronze Parquet]
    BRONZE -. planned .-> SILVER[Silver Parquet]
    SILVER -. planned .-> GOLD[Gold analytics]
```

The eventual local platform may add MinIO, Airflow, dbt Core, an analytics database, Superset,
data-quality checks, and CI. Kafka and Debezium are intentionally deferred until the batch pipeline is
reliable.

## Technology stack

| Component | Current use |
| --- | --- |
| Python 3.11 | Ingestion package and CLI |
| Docker Compose | Reproducible local development environment |
| Java 17 | PySpark runtime dependency |
| PySpark 3.5.3 | Local Bronze processing engine |
| PyArrow 18.1.0 | Lightweight Parquet metadata validation |
| pytest and Ruff | Automated tests and linting |
| Git and GitHub | Versioned, reviewable project history |

## Repository structure

```text
src/nyc_taxi_lakehouse/  Reusable Python package
├── ingestion/           Official TLC raw-data ingestion
├── bronze/              Source-aligned Spark Bronze processing
configs/                 Versioned, non-secret configuration
data/                    Local runtime data; generated content is ignored
tests/                   Unit and integration tests
docs/architecture/       Architecture documentation
scripts/                 Developer utilities
```

The project uses a `src/` layout so command-line jobs, tests, and future orchestration import the same
package code instead of relying on loose scripts.

## Getting started

Docker Desktop must be running. Create local configuration from the safe template, build the image, and
run the current checks:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check src tests
```

The validated container environment is Python 3.11.16, OpenJDK 17.0.20.1, PySpark 3.5.3, and PyArrow
18.1.0.

## Completed: project foundation

Phase 1 established the project’s reproducible local foundation:

- Docker-based Python, Java, and PySpark environment, avoiding a host Python/Spark dependency.
- A local SparkSession smoke test that started Spark, inspected a DataFrame schema, and completed count
  and filter actions successfully.
- pytest and Ruff configuration, including a package-import test.
- `.env.example` for safe local configuration; actual `.env` is ignored and excluded from Docker build
  contexts.
- Git repository on `main`, with the project published to GitHub.

## Completed: NYC TLC Yellow Taxi ingestion

### Official source and parameterization

The ingestion module downloads official TLC-hosted monthly Yellow Taxi Parquet files. TLC publishes the
data on its [Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using
this direct-file pattern:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<year>-<month>.parquet
```

The CLI accepts `--taxi-type`, `--year`, `--month`, `--destination-dir`, and `--force`; production logic
does not hard-code a development month. Yellow Taxi is the only enabled taxi type at this stage.

```powershell
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.ingestion.nyc_taxi --year 2024 --month 1
```

### Raw-data layout and lineage

Raw source data is organized by taxi type, year, and month so each source period remains independently
addressable as later processing becomes partition-aware:

```text
data/raw/
├── yellow/2024/01/yellow_tripdata_2024-01.parquet
└── metadata/yellow/2024/01/yellow_tripdata_2024-01.json
```

For every successful download, the generated JSON manifest records the dataset, taxi type, year/month,
official source URL, local runtime path, filename, UTC download timestamp, and byte size.

### Reliable download behavior

The downloader uses Python logging, a bounded three-attempt retry strategy, HTTP error handling, and
clear exceptions for invalid requests, network failures, and filesystem errors. It writes downloads to a
`.part` file and validates it before atomically promoting it to the final filename, preventing an
interrupted download from appearing successful.

Validation confirms that the file exists, is non-empty, and exposes readable Parquet footer metadata
through PyArrow without loading the full dataset into memory.

The ingestion operation is idempotent at the source-file level: a rerun validates the final existing
Parquet file and skips the network download unless `--force` is supplied. `--force` explicitly replaces
the local file when required.

### Validated development example

Yellow Taxi `2024-01` was downloaded from the official source and validated successfully:

- File: `yellow_tripdata_2024-01.parquet`
- Raw size: 49,961,641 bytes
- Metadata manifest: generated under `data/raw/metadata/yellow/2024/01/`
- Rerun: skipped the valid existing file without re-downloading it

### Testing and Git safety

The automated test suite uses mocked network responses and tiny synthetic Parquet files; it never needs
to download the TLC dataset. Tests cover URL and path construction, invalid months, skip and force
behavior, partial-file cleanup, invalid-download rejection, and manifest generation.

Generated raw Parquet files, manifests, partial files, logs, Spark temporary output, Docker volumes,
virtual environments, and `.env` are excluded from Git. The repository contains only directory
placeholders under `data/`.

## Completed: Bronze data layer

### Purpose and source preservation

The Bronze job reads the successful raw source file with PySpark and preserves every source business
column, value, and Spark-inferred type. It deliberately performs no business cleaning, deduplication,
timestamp repair, null handling, or quality-based rejection; those responsibilities begin in Silver.

The validated `2024-01` Yellow Taxi source contains 2,964,624 rows and 19 columns:

```text
VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
```

### Bronze layout and lineage

Run the Bronze job for one raw source period with:

```powershell
docker compose run --rm pipeline python -m nyc_taxi_lakehouse.bronze.processor --taxi-type yellow --year 2024 --month 1
```

The output uses a Hive-style, month-granular layout:

```text
data/bronze/yellow/year=2024/month=01/
└── part-*.snappy.parquet
```

Year/month directories allow later partition-aware reads while avoiding high-cardinality partitions such
as trip IDs or pickup timestamps. The current approximately 50 MB development input is coalesced to one
Parquet part file to avoid a local small-file fan-out; this policy can be tuned after benchmarking at a
larger scale.

Bronze adds five technical lineage columns, separate from TLC business fields:

- `_bronze_ingested_at` — one UTC timestamp shared by the processing run
- `_source_file` — source Parquet filename
- `_source_taxi_type`
- `_source_year`
- `_source_month`

### Idempotency and safe writes

The job writes Spark output to a generated temporary sibling directory, reads it back, and validates
row preservation, source columns, lineage columns, and lineage values. Only after validation succeeds
does it replace the requested `year/month` target partition. If promotion fails, a prior target is kept
as a rollback path. A rerun replaces only the same year/month partition rather than appending duplicate
rows or touching unrelated months.

### Validated development example

Bronze was run against the existing Yellow Taxi `2024-01` raw file:

- Source: 49,961,641 bytes, 2,964,624 rows, 19 columns
- Bronze target: `data/bronze/yellow/year=2024/month=01/`
- Bronze result: 2,964,624 rows, 24 columns, 5 technical columns
- Parquet payload: 61,639,367 bytes in 1 part file
- Final validation run: approximately 24 seconds
- Idempotency rerun: retained one `month=01` partition and one part file; row count stayed 2,964,624

### Spark design notes

Spark transformations such as adding lineage columns are lazy; actions such as `count()` and writing
Parquet trigger execution. This job uses counts intentionally for operational validation and never
collects the source dataset to the driver. Spark DataFrame partitions are execution units, while the
`year=.../month=...` directories are data-layout partitions used for future pruning. PySpark is used
instead of Pandas so the same processing model can scale beyond this local monthly source.

## Engineering decisions

- **Parquet over CSV:** columnar storage supports efficient later reads through column and predicate
  pruning.
- **PySpark:** a distributed processing engine suitable for growth beyond a small local sample; it was
  validated in the local Docker environment before transformation work begins.
- **Raw-layer lineage:** a small per-file manifest is sufficient now and avoids prematurely introducing
  a metadata platform.
- **Local-first architecture:** Docker and open-source libraries keep the project reproducible and free
  to run; cloud mappings below are reference architecture, not deployed services.

## Implementation roadmap

- [x] Phase 1 — Project foundation
- [x] Phase 2 — NYC Taxi ingestion
- [x] Phase 3 — Bronze layer
- [ ] Phase 4 — Silver layer
- [ ] Phase 5 — Gold layer
- [ ] Phase 6 — Lakehouse/object storage
- [ ] Phase 7 — Containerization improvements
- [ ] Phase 8 — Airflow orchestration
- [ ] Phase 9 — dbt transformations
- [ ] Phase 10 — Data quality and observability
- [ ] Phase 11 — Analytics database and dashboard
- [ ] Phase 12 — Testing improvements
- [ ] Phase 13 — CI/CD
- [ ] Phase 14 — Performance/scalability
- [ ] Phase 15 — Final documentation/interview preparation

## Production mapping (reference only)

| Local component | Typical managed equivalent |
| --- | --- |
| MinIO (planned) | Amazon S3 / Azure Data Lake Storage |
| Local Spark | Amazon EMR / Databricks / Azure Synapse |
| Apache Airflow (planned) | MWAA / Cloud Composer / managed Airflow |
| Local analytics database (planned) | A cloud warehouse or managed PostgreSQL |
