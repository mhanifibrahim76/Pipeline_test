# mart-pocc — Fleet Realization ETL (Polars + Airflow)

Extracts ferry fleet realization data from the **BRONZE** Oracle database, transforms
it with **Polars**, and loads it into the **POCC** Oracle mart table through a staging
table and an idempotent `MERGE INTO`. Scheduling and orchestration are handled by
**Apache Airflow 3** running in Docker.

## Architecture

```text
                     ┌──────────────────────────────┐
BRONZE Oracle DB ───▶│  src.main.run_pipeline()      │
 (REALIZATION.sql)   │  1. test both connections    │
                     │  2. extract (pl.read_database)│
                     │  3. transform (pl.SQLContext  │
                     │     via TRANSFORM.sql)       │
                     │  4. TRUNCATE staging          │
                     │  5. INSERT staging (1 txn)    │
                     │  6. MERGE INTO target (1 txn)  │
                     │  7. TRUNCATE staging (cleanup)│
                     └──────────────┬───────────────┘
                                    ▼
                     POCC Oracle DB: STG_…_DEV → MART_…_DEV
```

```text
src/
├── main.py          # Entry point and pipeline orchestration
├── common/          # Singleton logger, DB engines, connection test, SQL loader, Polars transform
├── config/          # Frozen dataclass config loaded from .env (secrets masked)
├── extract/         # Source extraction (read_source_data)
├── load/            # Staging & target load (truncate, insert_staging, merge_into_target, load_to_oracle)
└── query/           # Externalized SQL queries (REALIZATION.sql & TRANSFORM.sql)
dags/dag.py          # Airflow DAG (env-driven settings, 2 tasks)
tests/               # Unit + DAG tests; integration tests behind a flag
```

Key guarantees:

- **Fail fast**: both Oracle connections are validated before any data work.
- **Idempotent**: the merge upserts on `SCHEDULE_CODE`; reruns cannot duplicate.
- **Transactional**: staging insert and the merge each run in a single transaction;
  a failed merge rolls back and staging rows are kept for diagnosis until the
  merge succeeds.
- **Safe retries**: `max_active_runs=1` plus the pre-run staging `TRUNCATE` ensure
  a failed attempt cannot mix with or duplicate a retried one.
- **Parse-safe DAG**: no database access, no env loading during DAG parsing.

## Running

> **Docker permission**: commands must run with access to the Docker socket. If you
> see `permission denied while trying to connect to the docker API at
> unix:///var/run/docker.sock`, ensure your user is in the `docker` group
> (`sudo usermod -aG docker $USER`) and re-login — or run the command as
> `sg docker -c "<command>"` / with `sudo`.

```bash
cp .env .env            # edit credentials/tables as needed
docker compose build
docker compose up -d    # postgres + airflow api-server/scheduler/dag-processor
```

Airflow UI: http://localhost:8080 (admin/admin via `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`
plus the bind-mounted `simple_auth_manager_passwords.json` password file).
The DAG `mart_pocc_fleet_realization` runs every 3 minutes by default (Asia/Jakarta).

Manual one-shot run outside Airflow (needs Docker socket access too):

```bash
docker compose run --rm etl
```

Local run (Python 3.12, deps installed; no Docker needed): `python -m src.main`

## Tests

```bash
pytest tests/                       # unit + DAG tests (no Oracle needed)
RUN_ORACLE_INTEGRATION=1 pytest tests/integration/   # needs reachable *_DEV Oracle
```

Integration tests must never be pointed at production tables.

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `DB_HOST_BRONZE` / `DB_PORT_BRONZE` / `DB_NAME_BRONZE` | BRONZE source host / port / service name | required |
| `DB_USERNAME_BRONZE` / `DB_PASSWORD_BRONZE` | BRONZE credentials | required |
| `DB_HOST_POCC` / `DB_PORT_POCC` / `DB_NAME_POCC` | POCC target host / port / service name | required |
| `DB_USERNAME_POCC` / `DB_PASSWORD_POCC` | POCC credentials | required |
| `DB_TABLE_POCC` | Target mart table | required |
| `DB_TABLE_STG_POCC` | Staging table | required |
| `DB_PRIMARY_KEY` | Comma-separated merge key | required |
| `SQL_FILE_PATH` | Extraction query file | `src/query/REALIZATION.sql` |
| `TRANSFORM_SQL_FILE_PATH` | In-memory transform query file | `src/query/TRANSFORM.sql` |
| `DAG_ID` | Airflow DAG ID | `mart_pocc_fleet_realization` |
| `DAG_SCHEDULE` | Cron override (wins over interval) | derived from `ETL_INTERVAL_MINUTES` |
| `ETL_INTERVAL_MINUTES` | Minutes between runs (`*/n * * * *`) | `3` |
| `ETL_START_TIME` | DAG start date (ISO, WIB) | `2026-09-08T15:00:00+07:00` |
| `DAG_TIMEZONE` | DAG timezone | `Asia/Jakarta` |
| `DAG_CATCHUP` | Airflow catchup | `FALSE` |

