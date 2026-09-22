# NYC Taxi Lakehouse

A local, production-style data engineering project being built to process official NYC Taxi &
Limousine Commission (TLC) Yellow Taxi trip records through Bronze, Silver, and Gold layers.

## Current status

**Phase 1 — repository and reproducible development environment** is complete. The first working
pipeline milestone will implement this flow before object storage, orchestration, and BI are
introduced:

```mermaid
flowchart LR
    TLC[NYC TLC trip records] --> ING[Python ingestion]
    ING --> RAW[Local raw Parquet]
    RAW --> BRONZE[Bronze Parquet]
    BRONZE --> SPARK[PySpark]
    SPARK --> SILVER[Silver Parquet]
    SILVER --> GOLD[Gold analytics Parquet]
```

The eventual local platform will add MinIO, Airflow, dbt Core, an analytics database, Superset,
data-quality checks, and CI. Kafka and Debezium are intentionally deferred until the batch pipeline
is reliable.

## Why this architecture

- **Parquet** is a compressed, columnar format that enables column and predicate pruning.
- **Bronze/Silver/Gold** separates source preservation, trustworthy conformed data, and business-ready
  analytics. Invalid records will be quarantined rather than silently discarded.
- **PySpark** provides a scalable execution model that remains appropriate as the project grows from a
  sample to multiple months of TLC data.
- **Docker** makes the project reproducible on this machine without requiring a host Python install.

## Repository layout

```text
src/nyc_taxi_lakehouse/  Python package; ingestion and Spark jobs will live here
configs/                 Versioned, non-secret runtime configuration
data/                    Local runtime data (large files are ignored by Git)
tests/                   Unit and integration tests
docs/architecture/       Architecture decisions and diagrams
scripts/                 Developer entry points and utilities
```

This intentionally uses a `src/` package instead of loose top-level scripts: imports, tests, and
future Airflow tasks use the same code as the command-line pipeline.

## Local setup

Docker Desktop must be running. Create your local configuration, then build the development image:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check src tests
```

## Roadmap

1. Repository and development environment
2. Download official TLC data and process a small, reproducible sample
3. Bronze, Silver, and Gold PySpark pipeline with quarantine and metrics
4. Local MinIO lakehouse storage, Docker services, Airflow, dbt, quality checks, analytics, tests, and CI

## Production mapping (planned, not deployed)

| Local component | Typical managed equivalent |
| --- | --- |
| MinIO | Amazon S3 / Azure Data Lake Storage |
| Local Spark | Amazon EMR / Databricks / Azure Synapse |
| Apache Airflow | MWAA / Cloud Composer / managed Airflow |
| Local analytics database | A cloud warehouse or managed PostgreSQL |
