# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Dockerized data pipeline that fetches SpaceX launch data from a public API, stores it in PostgreSQL, and enables SQL querying via Trino as a query engine. Supports both batch (historical) and incremental ingestion modes.

## Running the Stack

```bash
# Start all services (postgres, trino, pipeline)
docker compose -f docker/docker-compose.yml up --build

# Run pipeline locally (change POSTGRES_URL host from 'postgres' to 'localhost' in .env first)
cd src && python data_pipeline.py
```

## Environment Variables (src/.env)

All variables are loaded via `config_handler.py`:

| Variable | Purpose |
|---|---|
| `POSTGRES_URL` | SQLAlchemy connection string (use `postgres` as host in Docker, `localhost` for local) |
| `LAUNCHES_LATEST_URL` | SpaceX API URL for latest launch |
| `LAUNCHES_HISTORY_URL` | SpaceX API URL for all launches |
| `PAYLOADS_URL` / `LAUNCHPADS_URL` / `LANDPADS_URL` | SpaceX API URLs for other entities |
| `LAUNCHES_TABLE_NAME` / `PAYLOADS_TABLE_NAME` / `LAUNCHPADS_TABLE_NAME` / `LANDPADS_TABLE_NAME` / `AGG_TABLE_NAME` | Postgres table names |
| `trino_host` / `trino_port` / `trino_user` / `trino_catalog` / `trino_schema` | Trino connection settings |
| `latest` | `True` = incremental mode, `False` = batch mode |
| `trino_query_file_name` | Filename from `sql/` folder to execute via Trino (e.g. `exe_1_Launch_Performance_Over_Time.sql`) |

## Architecture

```
data_pipeline.py          # Entrypoint: orchestrates batch or incremental flow
config_handler.py         # Reads all env vars into a single config object
utils.py                  # All DB/API logic: fetch, flatten, insert, Trino queries
sql/                      # SQL files executed via Trino (table names are substituted at runtime)
docker/
  docker-compose.yml      # Defines postgres, trino, and pipeline services
  app/Dockerfile          # Python 3.10-slim image; runs data_pipeline.py
  trino/etc/              # Trino config: catalog (postgres connector), node, JVM, resource groups
```

## Key Design Patterns

- **JSON flattening**: Nested SpaceX API responses are recursively flattened to column names like `payloads_0`, `links_patch_small`, etc. via `utils.flatten_json()`.
- **Append-only tables**: Every insert adds an `insert_time` column — tables are never updated in place.
- **SQL table name substitution**: SQL files use `LAUNCHES_TABLE_NAME` / `PAYLOADS_TABLE_NAME` as placeholders; `utils.py` does a string replace before execution.
- **Batch mode**: Checks if table exists in `pg_tables` before fetching from API — idempotent on re-run.
- **Incremental mode**: Fetches latest launch, aligns columns to existing table schema, appends one row, then rebuilds the aggregation table.
- **Aggregation**: Rebuilt from scratch on every incremental ingest by running `sql/aggregate_query.sql` and appending the result to the agg table.

## Trino Configuration

Trino connects to Postgres via the connector configured in `docker/trino/etc/catalog/postgres.properties`. Wait for Trino's healthcheck (`/v1/info`) to pass before the pipeline runs queries — it takes longer to start than Postgres.
