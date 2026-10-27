## 1. Project Purpose
An ETL Project (`mart-pocc` — Fleet Realization) using Polars and SQLAlchemy with details:
- Extract ship schedule realization data from the Oracle **BRONZE** (`DWHBRONZEUSER`) database
- Standardize, clean, validate, and deduplicate data in-memory using **Polars** (`pl.SQLContext`)
- Load processed data into the Oracle **POCC** database via staging table and `MERGE INTO`
- Run inside Docker containers (standalone CLI container or orchestrated via Apache Airflow 3)

This project prioritizes:
- Code readability & functional simplicity (explicit module imports without barrel imports)
- Observability (structured logging & secret masking)
- Reproducibility & strict data contract enforcement

## 2. Agent Responsibilities
The AI acts as a Senior Data Engineer with the responsibilities below:
1. Follow functional ETL best practices (no business-logic classes; only `Config` frozen dataclass, `logging.Logger`, and custom exceptions are permitted as classes).
2. Respect the 13-column data schema contract (`get_source_schema()`) and primary key deduplication rules.
3. Generate production-quality Polars and SQLAlchemy 2.0 code using explicit module files (no barrel imports in `__init__.py`).
4. Avoid hardcoding secrets; load all credentials via environment variables validated in `src/config/settings.py`.
5. Maintain full compatibility with both the standalone Docker container (`Dockerfile`) and Apache Airflow (`dags/dag.py`, `docker-compose.yaml`).
6. Guarantee safe database load operations (`T1: TRUNCATE` staging -> `T2: INSERT` staging -> `T3: MERGE INTO` target -> `T4: TRUNCATE` staging).
7. Produce clear logs and errors while masking sensitive credentials (`Config.__repr__()`).
8. Validate connections to **both** external databases (Oracle BRONZE and Oracle POCC) using the function `test_oracle_connection(engine, logger, name)` before running extraction.

## 3. In Scope
1. Maintain and refactor the functional Polars ETL pipeline under `src/` into cohesive, explicitly named `.py` modules (eliminating barrel imports in `__init__.py`).
2. Consolidate all SQL queries into standalone `.sql` files inside `src/query/` (removing the legacy top-level `sql/` folder):
   - `src/query/REALIZATION.sql`: Oracle SQL query to extract data from the BRONZE database.
   - `src/query/TRANSFORM.sql`: Polars SQL query executed via `pl.SQLContext` to trim string columns and cast datetime columns in memory.
3. Centralize shared infrastructure utilities and transformation logic (`logger.py`, `database.py`, `sql.py`, `transform.py`) in `src/common/`.
4. Manage environment configuration and validation in `src/config/settings.py`.
5. Maintain the Apache Airflow DAG inside `dags/dag.py`.
6. Follow PEP 8 and keep all unit, DAG, and integration test imports under `tests/` synchronized with the module structure.

## 4. Out of Scope
1. Hardcoding credentials, hostnames, or table names in Python or SQL.
2. Modifying database data manually, altering production table DDL, or connecting to live DWH databases during local test execution.
3. Skipping schema validation, null-key filtering, or primary-key deduplication.
4. Changing business logic, date filters, port filters, or `MERGE INTO` audit column behavior during structural refactoring.
5. Using barrel imports (putting implementation logic or re-export bundles inside `__init__.py`).
6. Using non-open-source dependencies without justification.

## 5. Project Architecture and Code Organization

### 5.1 Target Directory Structure (No Barrel Imports)
Use a clean, function-based modular architecture under `src/` (replacing the legacy `src/db/`, `src/etl/`, `src/logger/`, and top-level `sql/` directories). Keep all `__init__.py` files empty (or docstring-only) as pure Python package markers—do **not** use barrel imports:

```text
.
├── dags/
│   └── dag.py                          # Apache Airflow 3 DAG definition
├── src/
│   ├── __init__.py                     # Package marker only (empty / docstring)
│   ├── main.py                         # Entry point & pipeline orchestration
│   ├── common/
│   │   ├── __init__.py                 # Package marker only (no barrel exports)
│   │   ├── logger.py                   # Singleton logger factory (get_logger)
│   │   ├── database.py                 # Oracle URL builder, engine factories, test_oracle_connection
│   │   ├── sql.py                      # UTF-8 SQL file loader (load_sql_file)
│   │   └── transform.py                # Schema contract, column/key validators, transform() via pl.SQLContext
│   ├── config/
│   │   ├── __init__.py                 # Package marker only (no barrel exports)
│   │   └── settings.py                 # Frozen Config dataclass, env parsers, get_config(), reset_config()
│   ├── extract/
│   │   ├── __init__.py                 # Package marker only (no barrel exports)
│   │   └── reader.py                   # Source extraction (read_source_data)
│   ├── load/
│   │   ├── __init__.py                 # Package marker only (no barrel exports)
│   │   └── oracle.py                   # SQL builders, truncate_staging, insert_staging, merge_into_target, load_to_oracle
│   └── query/
│       ├── REALIZATION.sql             # Oracle BRONZE extraction query
│       └── TRANSFORM.sql               # Polars SQLContext in-memory transformation query
├── tests/                              # Unit tests, DAG tests, and Oracle integration tests
├── Dockerfile                          # Standalone ETL container image
├── Dockerfile.airflow                  # Custom Airflow container image
├── docker-compose.yaml                 # Multi-container orchestration
└── requirements.txt
```

### 5.2 Module Responsibilities & Symbol Mapping
Every module under `src/` must own the following exact responsibilities and symbols:

1. **`src/config/settings.py` (Configuration Layer — Zero Internal Imports):**
   - Constants: `DEFAULT_SQL_FILE_PATH = "src/query/REALIZATION.sql"`, `DEFAULT_TRANSFORM_SQL_FILE_PATH = "src/query/TRANSFORM.sql"`, `_BRONZE_VARS`, `_POCC_VARS`, `_TABLE_VARS`.
   - Helpers: `parse_bool()`, `parse_int()`, `env_bool()`, `env_int()`, `_require()`, `_check_required_vars()`.
   - Dataclass: `@dataclass(frozen=True) class Config` with all 13 required environment variables (`DB_HOST_BRONZE`, `DB_PORT_BRONZE`, `DB_NAME_BRONZE`, `DB_USERNAME_BRONZE`, `DB_PASSWORD_BRONZE`, `DB_HOST_POCC`, `DB_PORT_POCC`, `DB_NAME_POCC`, `DB_USERNAME_POCC`, `DB_PASSWORD_POCC`, `DB_TABLE_POCC`, `DB_TABLE_STG_POCC`, `DB_PRIMARY_KEY`), `sql_file_path`, `transform_sql_file_path`, `allowed_db_tables` property, and password-masking `__repr__()`.
   - Singleton accessors: `get_config() -> Config` and `reset_config() -> None`.

2. **`src/common/logger.py` (Logging Utility — Standard Library Only):**
   - `_logger_instance` and `get_logger() -> logging.Logger` (singleton logger `"etl"` with `INFO` level, stream handler guard, and `"%(asctime)s [%(levelname)s] %(message)s"` formatter configured to render `%(asctime)s` in **`UTC+7` (`Asia/Jakarta` / `WIB`)** via `ZoneInfo("Asia/Jakarta")` so logs in both local and Docker environments always print WIB timestamps).

3. **`src/common/database.py` (Database Connectivity — Imports `src.config.settings`):**
   - `_build_oracle_url(host, port, service_name, username, password) -> URL` (via `URL.create("oracle+oracledb", ...)`).
   - `create_bronze_engine(cfg: Config) -> Engine` and `create_pocc_engine(cfg: Config) -> Engine` (both with `pool_pre_ping=True` and `connect_args={"tcp_connect_timeout": 30}`).
   - `test_oracle_connection(engine: Engine, logger: Logger, name: str = "Oracle") -> bool` (executes `SELECT 1 FROM DUAL`).

4. **`src/common/sql.py` (SQL File Loader — Standard Library Only):**
   - `load_sql_file(path: str) -> str`: reads UTF-8 SQL file, strips outer whitespace and trailing `;`, and raises `ValueError(f"SQL file is empty: {path}")` when empty.

5. **`src/common/transform.py` (In-Memory Polars Transformation — Imports `src.common.sql` & `src.config.settings`):**
   - Schema Contract: `_STRING_COLUMNS` (6 columns), `_DATETIME_COLUMNS` (7 columns), and `get_source_schema() -> pl.Schema` (6 `pl.String` + 7 `pl.Datetime("us")`).
   - Validation & Deduplication Helpers:
     - `_validate_columns(raw_df: pl.DataFrame, schema: pl.Schema) -> None`
     - `_drop_null_keys(df: pl.DataFrame, primary_key: tuple[str, ...], logger: Logger) -> pl.DataFrame` (drops rows where **any** key component is `NULL`)
     - `_drop_duplicate_keys(df: pl.DataFrame, primary_key: tuple[str, ...], logger: Logger) -> pl.DataFrame` (`df.unique(subset=list(primary_key), keep="last", maintain_order=True)`)
   - Transformation Function:
     `transform(raw_df: pl.DataFrame, schema: pl.Schema, primary_key: tuple[str, ...], logger: Logger, transform_sql_path: str = DEFAULT_TRANSFORM_SQL_FILE_PATH) -> pl.DataFrame`
     Validates required columns first (`_validate_columns`), loads transformation SQL from `transform_sql_path` via `load_sql_file()` at runtime, executes it inside `pl.SQLContext(raw_source=raw_df, eager=True)`, drops null keys, deduplicates keys keeping `"last"`, and verifies `result.schema == schema`.

6. **`src/extract/reader.py` (Source Extraction Layer — Imports `src.common.sql` & `src.config.settings`):**
   - `read_source_data(engine: Engine, cfg: Config, logger: Logger) -> pl.DataFrame`: loads `cfg.sql_file_path` (`src/query/REALIZATION.sql`) via `load_sql_file()` and executes `pl.read_database(query=query, connection=engine)`.

7. **`src/load/oracle.py` (Database Load Layer — Imports `src.config.settings`):**
   - Table Validation & SQL Builders: `_validate_table(table, cfg)`, `_build_insert_sql(table, columns)`, and `_build_merge_sql(target_table, staging_table, primary_key, columns)` (using `CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` for `CREATED_ON` and `UPDATED_ON` so audit timestamps are always stored in **`UTC+7` / `WIB`**).
   - Step Functions: `truncate_staging(engine, cfg, logger)`, `insert_staging(engine, df, cfg, logger)`, `merge_into_target(engine, columns, cfg, logger)`.
   - Orchestrator: `load_to_oracle(engine, df, cfg, logger) -> None`.

8. **`src/main.py` (Pipeline Orchestration & Entry Points):**
   - Imports directly from concrete submodules (`src.common.database`, `src.common.logger`, `src.common.transform`, `src.config.settings`, `src.extract.reader`, `src.load.oracle`).
   - Exception: `class ETLConnectionError(RuntimeError)`.
   - Functions: `_check_connections(cfg, logger)`, `check_connections() -> None` (Airflow Task 1 callable), `run_pipeline() -> None` (Airflow Task 2 callable), and `main() -> None` (CLI / Docker entry point).

### 5.3 Business Query, 13-Field Contract, & Column Order
Preserve all business logic in `src/query/REALIZATION.sql` (5 active `INNER JOIN`s, 4 ports `MERAK`/`BAKAUHENI`/`KETAPANG`/`GILIMANUK`, `(PLOTING_DATE BETWEEN SYSDATE - INTERVAL '30' DAY AND SYSDATE OR TRUNC(PLOTING_DATE) = TRUNC(SYSDATE))`, and `ORDER BY bftts.PLOTING_DATE ASC`).

Note that `REALIZATION.sql` places `SCHEDULE_DATE` at column #2, whereas `TRANSFORM.sql` and `get_source_schema()` output the 6 string columns first followed by the 7 datetime columns (`SCHEDULE_DATE` at column #7). Both orders must be preserved:

| Output Order | Field / Alias | Oracle Source Expression | Raw Extract Order | Polars Output Type |
| :---: | :--- | :--- | :---: | :--- |
| 1 | `SCHEDULE_CODE` | `bftts.SCHEDULE_CODE` | 1 | `pl.String` |
| 2 | `CABANG` | `bftmp.name` | 3 | `pl.String` |
| 3 | `DERMAGA` | `bftmd.name` | 4 | `pl.String` |
| 4 | `JENIS_PELAYANAN` | `BFTMSC2.name` | 5 | `pl.String` |
| 5 | `NAMA_KAPAL_REALISASI` | `bftms.NAME` | 6 | `pl.String` |
| 6 | `OPERATOR_REALISASI` | `bftmsc.name` | 7 | `pl.String` |
| 7 | `SCHEDULE_DATE` | `bftts.SCHEDULE_DATE` | 2 | `pl.Datetime("us")` |
| 8 | `REALISASI_TIBA_ALUR` | `bftts.PLOTING_DATE` | 8 | `pl.Datetime("us")` |
| 9 | `REALISASI_SANDAR` | `bftts.DOCKING_DATE` | 9 | `pl.Datetime("us")` |
| 10 | `REALISASI_MULAI_PELAYANAN` | `bftts.OPEN_BOARDING_DATE` | 10 | `pl.Datetime("us")` |
| 11 | `REALISASI_SELESAI_PELAYANAN` | `bftts.CLOSE_BOARDING_DATE` | 11 | `pl.Datetime("us")` |
| 12 | `REALISASI_TUTUP_RAMPDOOR` | `bftts.CLOSE_RAMP_DOOR_DATE` | 12 | `pl.Datetime("us")` |
| 13 | `REALISASI_KEBERANGKATAN` | `bftts.SAIL_DATE` | 13 | `pl.Datetime("us")` |

Audit columns (`CREATED_ON` and `UPDATED_ON`) are populated inside `_build_merge_sql()` following the **`UTC+7` (`WIB` / `Asia/Jakarta`)** timestamp via `CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` (converting safely to `UTC+7` whether the database host OS runs in `UTC+0` or `UTC+7`, so audit timestamps never default to `UTC+0` and stay aligned with WIB operational logs for easier tracing and debugging) and are not part of the 13-column Polars DataFrame schema.

### 5.4 One-Way Import Graph & Parse-Time Safety
To prevent circular imports and guarantee Airflow DAG parse-time safety:
- Enforce a strict one-way dependency graph (`A -> B` means `A` imports `B`):
  - `dags/dag.py` -> `src.main`, `src.config.settings`
  - `src.main` -> `src.config.settings`, `src.common.logger`, `src.common.database`, `src.common.transform`, `src.extract.reader`, `src.load.oracle`
  - `src.extract.reader` -> `src.config.settings`, `src.common.sql`
  - `src.common.transform` -> `src.config.settings`, `src.common.sql`
  - `src.load.oracle` -> `src.config.settings`
  - `src.common.database` -> `src.config.settings`
  - `src.common.logger`, `src.common.sql`, `src.config.settings` -> Python standard library / third-party packages only
- `src.extract` and `src.load` must NEVER import each other or `src.main`.
- NEVER call `get_config()`, `load_dotenv()`, `create_engine()`, or `load_sql_file()` at module import time (top-level). All I/O must happen inside functions at execution time.

### 5.5 Precedence Over `prompt/STANDARD_FP.md` (CRITICAL OVERRIDES)
For general functional programming style, refer to `prompt/STANDARD_FP.md`. However, **where `STANDARD_FP.md` conflicts with this document, `CLAUDE.md` strictly overrides `STANDARD_FP.md`**:
1. **Folder & Module Structure Override (`STANDARD_FP.md` lines 9–26, 373–380, 413–426, 475–488):** Use `src/common/`, `src/config/`, `src/extract/`, `src/load/`, `src/query/`, and `src/main.py` with explicit submodule files (`logger.py`, `database.py`, `sql.py`, `transform.py`, `settings.py`, `reader.py`, `oracle.py`). Do NOT use the legacy `src/db/`, `src/etl/`, `src/logger/` structure or barrel imports in `__init__.py`.
2. **Externalized SQL Queries Override (`STANDARD_FP.md` lines 52, 103–126, 150–183, 387–388, 435–437):** Store both `REALIZATION.sql` and `TRANSFORM.sql` in `src/query/` and load them at runtime via `load_sql_file()`. Do NOT embed SQL queries as Python strings inside `read_source_data()` or `transform()`.
3. **Staging Load Override — NO `replace` (`STANDARD_FP.md` lines 217–236, 279–285, 402, 452, 537):** NEVER use `df.write_database(..., if_table_exists="replace")` (which drops the Oracle staging table, primary keys, and grants). Always use `truncate_staging()` + `insert_staging()` with named bind parameters (`INSERT INTO ... VALUES (:col)`).
4. **Audit Columns (`UTC+7` / `WIB`), Separate Transactions, & Post-Merge Cleanup Override (`STANDARD_FP.md` lines 54, 239–283, 391):** `_build_merge_sql()` MUST set `tgt.UPDATED_ON = CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` on `WHEN MATCHED` and `CREATED_ON, UPDATED_ON` to `CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` on `WHEN NOT MATCHED`, ensuring audit timestamps always follow **`UTC+7` (`WIB` / `Asia/Jakarta`)** and never default to `UTC+0`. `load_to_oracle()` runs 4 separate `engine.begin()` blocks (`T1: truncate_staging` -> `T2: insert_staging` -> `T3: merge_into_target` -> `T4: truncate_staging`). Note that Oracle `TRUNCATE TABLE` is DDL with an implicit commit and cannot be rolled back globally.
5. **13-Column Contract, Deduplication, & Entry Points Override (`STANDARD_FP.md` lines 32–45, 140–169, 490–519):** `transform()` MUST validate required columns before reading/executing SQL, drop null keys, and deduplicate keys (`keep="last", maintain_order=True`). `src/main.py` MUST export `check_connections()`, `run_pipeline()`, and `main()`.

## 6. Tech Stacks
- Python 3.12 slim (Docker)
- Polars 1.44.1 (Docker)
- SQLAlchemy 2.0.52 & python-oracledb 4.0.2
- python-dotenv 1.0.1 & pytest 9.1.1
- Apache Airflow 3.3.1 (Python 3.12)
- Oracle Database (BRONZE source database and POCC target database)

## 7. Scheduler (Apache Airflow)
Apache Airflow is responsible for scheduling and orchestrating the ETL job. The DAG code must be located under `dags/dag.py` and call `src.main.check_connections` and `src.main.run_pipeline` via `PythonOperator` (`validate_connections >> run_etl`).

### 7.1 Schedule Configuration
Read schedule-related values from environment variables at DAG parse time using `env_bool` and `env_int` from `src.config.settings` (without triggering `get_config()` or `load_dotenv()`). At minimum support:
- DAG ID (`DAG_ID`, default: `"mart_pocc_fleet_realization"`)
- Schedule (`DAG_SCHEDULE` / `ETL_INTERVAL_MINUTES`, default: `"*/3 * * * *"`)
- Time zone (`DAG_TIMEZONE`, default: `"Asia/Jakarta"`) & Start date (`ETL_START_TIME`, default: `"2026-09-08T15:00:00+07:00"`)
- Catchup behavior (`DAG_CATCHUP`, default: `False`)
- Retry count (`DAG_RETRIES`, default: `1`) & Retry delay (`DAG_RETRY_DELAY_MINUTES`, default: `1`)
- Execution timeout (`DAG_EXECUTION_TIMEOUT_MINUTES`, default: `10`)
- Maximum active runs (`DAG_MAX_ACTIVE_RUNS`, default: `1`)
- Initial paused state (`DAG_IS_PAUSED`, default: `False`)

### 7.2 DAG Requirements
The DAG must:
- Use a stable DAG ID.
- Use an explicit start date (avoid dynamic start dates such as `datetime.now()`).
- Use a configured time zone (UTC+7 for WIB / `Asia/Jakarta`).
- Disable catchup by default unless backfilling is required.
- Use `max_active_runs=1` by default when using a shared staging table.
- Use meaningful task IDs (`validate_connections` and `run_etl`).
- Define retries, retry delays, and execution timeout.
- Propagate ETL failures (`ETLConnectionError` and runtime exceptions) correctly without calling `sys.exit()` inside Airflow task callables.
- Avoid performing database work, opening database connections, or reading `.env` / `.sql` files during DAG parsing (at module import time).
- Keep task callables importable and testable.
- Avoid deprecated Airflow operators and arguments.
- Include appropriate tags and a clear DAG description.

Do not hardcode the production schedule directly in Python when the project requires environment-driven scheduling.

### 7.3 Airflow Retry Safety
Every retry must be safe. Verify that:
- Staging data is cleared safely via pre-load `T1: TRUNCATE TABLE` before `T2: INSERT`.
- The `T3: MERGE INTO` operation is key-idempotent on `SCHEDULE_CODE` (updating business columns and `UPDATED_ON = CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` in `UTC+7` / `WIB`).
- A failed prior attempt cannot produce duplicate keys in the target table.
- If `T3: merge_into_target()` fails, `T4: truncate_staging()` is not reached, preserving staged rows for inspection until the next non-empty run truncates staging at `T1`.

## 8. Connection Validation & Transaction Handling
**IMPORTANT**
Connection tests MUST pass for **both** databases before running the job:
1. Test Oracle Database connections for **both `BRONZE` and `POCC`** via `test_oracle_connection()` (`SELECT 1 FROM DUAL`), always disposing temporary engines inside a `finally:` block.
2. If either connection fails -> STOP immediately (do not run job), log detailed error, and raise `ETLConnectionError`.
3. If both succeed, then execute the pipeline in order:
   1. **Extract:** Fetch data from the `BRONZE` database using `src/query/REALIZATION.sql`.
   2. **Transform:** Validate the 13 required columns (`_validate_columns`), execute `src/query/TRANSFORM.sql` inside `pl.SQLContext(raw_source=raw_df, eager=True)`, drop rows where any component of `primary_key` (`SCHEDULE_CODE`) is `NULL`, deduplicate on `primary_key` using `keep="last", maintain_order=True`, and verify output schema equality (`result.schema == schema`).
   3. **Empty Dataset Check:** If `df.height == 0`, log `"No records to load; skipping staging and merge"` and return cleanly without calling `load_to_oracle()` (skipping both staging `TRUNCATE`s).
   4. **Load (`load_to_oracle()` across 4 separate transaction contexts `T1`–`T4`):**
      - `T1`: `TRUNCATE` the staging table (`with engine.begin():`) to clear any leftover rows from prior failed runs.
      - `T2`: `INSERT` the transformed DataFrame into the staging table using named bind parameters (`with engine.begin():`).
      - `T3`: Execute `MERGE INTO` from the staging table into the target table matching on `PRIMARY KEY` (`SCHEDULE_CODE`), updating the 12 non-key columns + `UPDATED_ON = CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` when matched, and inserting all 13 business columns + `CREATED_ON, UPDATED_ON` using `CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` (`UTC+7` / `WIB`) when not matched (`with engine.begin():`).
      - `T4`: After completing `T3`, `TRUNCATE` the staging table (`with engine.begin():`).
4. Transaction & Cleanup Semantics:
   - Each of `T1`–`T4` runs in its own `with engine.begin():` context. Because Oracle `TRUNCATE TABLE` is DDL with an implicit commit, `T1`–`T4` do not form a single global rollback unit; a failure inside `T2` or `T3` rolls back only the active uncommitted DML statement, while a failure in `T4` occurs after `T3` has already committed to the target table.
   - Both `bronze_engine` and `pocc_engine` must be disposed in the `finally:` block.

## 9. Credentials, Endpoints, Container Paths, & Time Zone
1. Load all database credentials and table names from the `.env` secret file / environment variables. Never commit `.env` or real DWH credentials to Git.
2. Keep container paths and **`UTC+7` (`Asia/Jakarta`)** time zone synchronized across Docker and Airflow:
   - `Dockerfile`: `COPY src/ ./src/` (remove `COPY sql/ ./sql/`), set `ENV TZ=Asia/Jakarta`, `ENV SQL_FILE_PATH=/app/src/query/REALIZATION.sql`, and `ENV TRANSFORM_SQL_FILE_PATH=/app/src/query/TRANSFORM.sql`.
   - `docker-compose.yaml`: mount `./src:/opt/airflow/src:ro` (remove `./sql:/opt/airflow/sql:ro`), set `TZ: Asia/Jakarta`, `SQL_FILE_PATH: /opt/airflow/src/query/REALIZATION.sql`, and `TRANSFORM_SQL_FILE_PATH: /opt/airflow/src/query/TRANSFORM.sql`.

## 10. Implementation Workflow
1. **Start with `CLAUDE.md`** -> Follow the exact target folder and submodule structure (`src/common/{logger,database,sql,transform}.py`, `src/config/settings.py`, `src/extract/reader.py`, `src/load/oracle.py`, `src/query/{REALIZATION,TRANSFORM}.sql`, `src/main.py`) with empty `__init__.py` markers (no barrel imports).
2. **Reference `STANDARD_FP.md` with Section 5.5 Overrides** -> Apply functional programming and naming conventions, while strictly enforcing Section 5.5 overrides where `STANDARD_FP.md` differs.
3. **Migrate SQL & Python Modules** -> Move `REALIZATION.sql` and `TRANSFORM.sql` into `src/query/`, split Python modules cleanly following the one-way import graph in Section 5.4, and remove legacy `src/db/`, `src/etl/`, `src/logger/`, and `sql/`.
4. **Synchronize Docker, Airflow, & Tests** -> Update `Dockerfile`, `docker-compose.yaml`, `dags/dag.py`, `tests/conftest.py`, and all test imports under `tests/`.
5. **Verify Local Regression Suite** -> Run the local unit and DAG test suite (excluding live Oracle DWH integration tests) and ensure 100% pass without weakening assertions.

## 11. Testing Requirements
All material logic must be testable and verified locally under `tests/` using synthetic fixtures and mocks/SQLite stubs at the database boundary (never connecting to live company DWH servers during local testing).

### 11.1 Local Unit Tests
Maintain and update unit tests for:
- **Configuration (`tests/test_config.py` & `tests/conftest.py`):**
  - Import from `src.config.settings` (and in `tests/conftest.py`, patch `load_dotenv` on `src.config.settings` via `from src.config import settings as config_module` so `monkeypatch.setattr(config_module, "load_dotenv", lambda: None)` targets the actual module where `load_dotenv` is called).
  - Validate all 13 required env vars, table allow-list, boolean/integer parsing, `DEFAULT_SQL_FILE_PATH` (`"src/query/REALIZATION.sql"`), `DEFAULT_TRANSFORM_SQL_FILE_PATH` (`"src/query/TRANSFORM.sql"`), caching/reset, and secret masking.
- **SQL File Loading (`tests/test_sql_loading.py`):**
  - Import `load_sql_file` from `src.common.sql`.
  - Test loading **both** `src/query/REALIZATION.sql` and `src/query/TRANSFORM.sql`, empty file rejection, missing file error, and trailing semicolon stripping.
- **Polars Transformations (`tests/test_etl.py`):**
  - Import `get_source_schema` and `transform` from `src.common.transform`.
  - Test 13-column schema validation (`get_source_schema()`), required-column validation (`_validate_columns()`) before SQL execution, string trimming (`TRIM`) and datetime casting (`CAST`) via `src/query/TRANSFORM.sql`, non-key null preservation, null primary-key dropping (`_drop_null_keys()`), duplicate primary-key deduplication keeping `"last"` (`_drop_duplicate_keys()`), empty dataset (`0` rows) handling, and invalid datetime error propagation (`pl.exceptions.InvalidOperationError`).
- **Database & Load Helpers (`tests/test_db.py`):**
  - Import connection helpers from `src.common.database` and SQL builders/validators from `src.load.oracle`.
  - Test table allow-list validation (`_validate_table()`), named-bind `INSERT` SQL builder (`_build_insert_sql()`), `MERGE INTO` SQL builder including `CREATED_ON` and `UPDATED_ON` with the **`UTC+7` (`Asia/Jakarta`)** timestamp expression `CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)` (`_build_merge_sql()`), and Oracle connection health check (`test_oracle_connection()`).

### 11.2 Airflow DAG Tests (`tests/test_dag.py`)
Test that:
- The DAG imports without errors (`bag.import_errors == {}`).
- The DAG ID, schedule, timezone, catchup, and `max_active_runs=1` are loaded correctly.
- Expected task IDs (`validate_connections` and `run_etl`) and dependency (`validate_connections >> run_etl`) exist.
- No database operation, engine creation, or `.env` loading occurs during DAG parsing (`test_no_database_access_during_parsing`).

### 11.3 Local Test Execution Command
Run the isolated local test suite explicitly:
```bash
pytest tests/test_config.py tests/test_sql_loading.py tests/test_etl.py tests/test_db.py tests/test_dag.py
```

### 11.4 Oracle Integration Tests (`tests/integration/test_oracle_integration.py`)
- Update top-level imports in `tests/integration/test_oracle_integration.py` to match the new explicit submodule paths (`src.common.database`, `src.common.logger`, `src.common.transform`, `src.config.settings`, `src.extract.reader`, `src.load.oracle`, `src.main`) so static import checks stay clean.
- Keep integration tests guarded by `RUN_ORACLE_INTEGRATION=1`; do NOT enable `RUN_ORACLE_INTEGRATION=1` or connect to company DWH databases during local development.

### 11.5 Test Quality Rules
- Do not write tests that only assert mocks were called for transformations; execute real `pl.SQLContext` transformations and validate Polars DataFrame outputs with `polars.testing.assert_frame_equal`.
- Cover both success and failure cases.
- Keep fixtures small, synthetic, and deterministic.
- Never weaken assertions to make tests pass, and do not delete valid existing tests.
