# NYC Taxi Lakehouse

A local, production-style data engineering project that incrementally builds a lakehouse for official
NYC Taxi & Limousine Commission (TLC) Yellow Taxi trip records. The project is designed to demonstrate
reproducible ingestion, Spark processing, data quality, orchestration, and analytics without requiring
paid cloud services.

## Project overview

The implemented system currently downloads one official TLC Yellow Taxi Parquet file into a local raw
layer. Future phases will add source-aligned Bronze storage, cleaned Silver data, and Gold analytics;
those layers are not implemented yet.

`PROJECT_STATUS.md` records the active development state. This README is cumulative technical
documentation for functionality that has been implemented and validated.

## Architecture

```mermaid
flowchart LR
    TLC[NYC TLC trip records] --> ING[Python ingestion]
    ING --> RAW[Local raw Parquet]
    RAW -. planned .-> BRONZE[Bronze Parquet]
    BRONZE -. planned .-> SPARK[PySpark]
    SPARK -. planned .-> SILVER[Silver Parquet]
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
| PySpark 3.5.3 | Validated locally; planned processing engine |
| PyArrow 18.1.0 | Lightweight Parquet metadata validation |
| pytest and Ruff | Automated tests and linting |
| Git and GitHub | Versioned, reviewable project history |

## Repository structure

```text
src/nyc_taxi_lakehouse/  Reusable Python package
├── ingestion/           Official TLC raw-data ingestion
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
- [ ] Phase 3 — Bronze layer
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
