# NYC Taxi Lakehouse — Project Status

## Current Phase

Phase 1 — Repository and Development Environment

## Completed

- Initial Git repository scaffold on `main`
- `src/`-layout Python package: `src/nyc_taxi_lakehouse/`
- Local runtime directories for raw, Bronze, Silver, Gold, and quarantine data, retained with
  `.gitkeep` files only
- Docker Compose development service with Python, Java, and PySpark
- Dependency, pytest, and Ruff configuration
- README, environment template, Git ignore rules, and Docker build-context exclusions

## Current Architecture

One local Docker Compose `pipeline` service provides Python 3.11, Java 17, and PySpark. No NYC Taxi
data, ingestion pipeline, Bronze/Silver/Gold transformation, MinIO, Airflow, dbt, Superset, or
dashboard has been implemented yet.

## Environment

- Docker Desktop 4.55.0 / Docker Engine 29.1.3
- Docker Compose v2.40.3
- Python 3.11.16 (container)
- OpenJDK 17.0.20.1 (container)
- PySpark 3.5.3 (container)

## How to Run

Docker Desktop must be running. These commands were verified during Phase 1:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline python --version
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check src tests
```

## Validation Completed

- `docker compose config` passed.
- The Docker image built successfully after pinning the Python 3.11 base image to Debian Bookworm,
  which supplies Java 17.
- PySpark imported successfully.
- A local SparkSession started, read a three-row DataFrame schema, returned a count of 3, completed a
  filter action returning 2 rows, and shut down cleanly.
- `.env` is ignored and is not tracked.

## Tests

- `pytest`: one Phase 1 package-import test passed.
- `ruff check src tests`: passed.
- Spark environment smoke test: passed.

## Known Issues

None. Spark's missing `ps` utility and native Hadoop library warnings during the smoke test are
expected for this minimal local container and did not affect execution.

## Architecture Decisions

- Use a `src/`-layout Python package so pipeline jobs, tests, and future orchestration import the same
  code.
- Use Docker for a reproducible local environment without requiring host Python or Spark installation.
- Use PySpark to establish the distributed processing engine that later transformations will use.
- Keep the initial architecture local-first and free of managed cloud services.
- Exclude generated data directories from Git while preserving their structure with `.gitkeep` files.
- Handle secrets through ignored environment files; `.env.example` contains only safe development
  values, and `.dockerignore` prevents local `.env` from entering image build contexts.

## Next Phase

Phase 2 — NYC Taxi ingestion. This phase has **not** started.
