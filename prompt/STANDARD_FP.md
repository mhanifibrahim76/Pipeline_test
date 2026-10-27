## ETL Standards

Standards and guidelines for creating and maintaining ETL projects that follow the modular functional architecture, conventions, and safety contracts of this codebase.

**Paradigm: Functional Programming**: pure functions, explicit data flow, no classes except the Logger singleton, the frozen `Config` dataclass, and custom domain exceptions (e.g., `ETLConnectionError`).

**DataFrame engine: Polars**. Use real, public Polars APIs such as `pl.DataFrame`, `pl.Schema`, `pl.SQLContext`, `pl.read_database()`, `DataFrame.lazy()`, and `LazyFrame.collect()`.

### 1. Project Structure

```text
src/
├── main.py          # Entry point, Airflow task callables, and pipeline orchestration
├── common/          # Shared utilities: logger singleton, DB engine factories, connection test, SQL file loader
├── config/          # Configuration management, .env parsers, frozen dataclass, and singleton accessor
├── extract/         # Source database reads (pl.read_database) and in-memory Polars transformations (pl.SQLContext)
├── load/            # Staging & target load operations (TRUNCATE, named-bind INSERT, transactional MERGE INTO)
└── query/           # Externalized standalone SQL files (e.g., REALIZATION.sql and TRANSFORM.sql)
```

| Folder | Purpose |
|---|---|
| `main.py` | Pipeline orchestration, Airflow task callables (`check_connections`, `run_pipeline`), and CLI entry point (`main`) |
| `common/` | Singleton logger (`get_logger`), SQLAlchemy engine factories, `test_oracle_connection`, and `load_sql_file` |
| `config/` | Environment variables validated and loaded once into a frozen `Config` dataclass with secret masking |
| `extract/` | Source data extraction (`read_source_data`), Polars schema definition (`get_source_schema`), key validation/deduplication, and `transform()` |
| `load/` | Table allow-list validation, staging `TRUNCATE`/`INSERT` with bind parameters, and target `MERGE INTO` (`load_to_oracle`) |
| `query/` | Standalone `.sql` files for both source extraction (`REALIZATION.sql`) and in-memory Polars SQL transformation (`TRANSFORM.sql`) |

### 2. Architecture Pattern

**Functional Pipeline Architecture**: data flows explicitly through functions across a strict one-way module dependency graph (`src.config` $\rightarrow$ `src.common` $\rightarrow$ `src.extract` / `src.load` $\rightarrow$ `src.main`).

```text
main.py
│
│  cfg           = get_config()
│  logger        = get_logger()
│  _check_connections(cfg, logger)                        -> None (validates both source & target DBs)
│
│  bronze_engine = create_bronze_engine(cfg)
│  pocc_engine   = create_pocc_engine(cfg)
│
│  raw_df        = read_source_data(bronze_engine, cfg, logger)                    -> pl.DataFrame
│  schema        = get_source_schema()                                             -> pl.Schema
│  df            = transform(raw_df, schema, cfg.primary_key, logger)              -> pl.DataFrame
│                  load_to_oracle(pocc_engine, df, cfg, logger)                    -> None
```

**Key principles**:

- No service, transformer, or repository classes.
- Every dependency (`engine`, `cfg`, `logger`, `schema`, `primary_key`) is passed explicitly.
- Logger and Config are the only permitted singletons; never initialize them at module import time (top-level) to keep Airflow DAG parsing side-effect-free.
- Keep all SQL queries in standalone `.sql` files inside `src/query/` (`REALIZATION.sql` for Oracle source extraction, `TRANSFORM.sql` for `pl.SQLContext` in-memory transformation) and load them at runtime using `load_sql_file()`.
- Database connections are managed through SQLAlchemy `Engine` with `URL.create()`, `pool_pre_ping=True`, and `engine.dispose()` in `finally:` blocks.
- Loads are idempotent and preserve staging table DDL: `truncate_staging()` $\rightarrow$ `insert_staging()` (named bind parameters) $\rightarrow$ `merge_into_target()` (`MERGE INTO` on primary key with `SYSTIMESTAMP` audit columns) $\rightarrow$ `truncate_staging()`.

### 3. Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Folders | lowercase with underscore | `common/`, `extract/`, `load/`, `query/` |
| Module files | lowercase with underscore | `__init__.py`, `main.py` |
| SQL files | uppercase descriptive `.sql` | `REALIZATION.sql`, `TRANSFORM.sql` |
| Functions | snake_case verb phrases | `read_source_data()`, `load_to_oracle()` |
| Internal helpers | leading underscore | `_build_oracle_url()`, `_validate_table()` |
| Constants | upper snake case | `DEFAULT_SQL_FILE_PATH`, `DEFAULT_TRANSFORM_SQL_FILE_PATH` |
| Config fields | snake_case | `staging_table`, `target_table`, `primary_key` |
| Function parameters | snake_case | `df`, `cfg`, `logger`, `primary_key` |

### 4. Feature or Module Creation Guide

To add or extend a pipeline feature:

1. Place the source extraction SQL query (`<FEATURE>.sql`) and Polars transformation SQL query (`TRANSFORM.sql`) inside `src/query/`.
2. Add or update required environment variables and default SQL paths in `src/config/__init__.py`.
3. Use shared helpers from `src/common/__init__.py` (`get_logger`, `create_bronze_engine`, `create_pocc_engine`, `test_oracle_connection`, `load_sql_file`).
4. In `src/extract/__init__.py`, define `get_source_schema() -> pl.Schema`, `read_source_data(engine, cfg, logger) -> pl.DataFrame`, and `transform(raw_df, schema, primary_key, logger, transform_sql_path=...) -> pl.DataFrame`.
5. In `src/load/__init__.py`, implement staging and target load functions (`truncate_staging`, `insert_staging`, `merge_into_target`, `load_to_oracle`).
6. Wire the functions explicitly into `src/main.py` (`check_connections`, `run_pipeline`, `main`) and expose Airflow tasks in `dags/dag.py`.
7. Add unit tests, SQL loading tests, and DAG parse-safety tests under `tests/`.

### 5. Component Guidelines

Functions must:

- Do one thing.
- Receive dependencies explicitly.
- Include parameter and return type hints.
- Return new values instead of mutating caller-owned state.
- Log significant entry, completion, counts, and errors.
- Never perform file I/O, environment loading, or database connections at module import time.

#### Shared Infrastructure Pattern (`src/common/__init__.py`)

```python
# src/common/__init__.py
import logging
from logging import Logger
from pathlib import Path
from typing import Optional

from sqlalchemy import URL, Engine, create_engine, text

from src.config import Config

_logger_instance: Optional[ logging.Logger ] = None


def get_logger() -> logging.Logger:
    """Return the singleton ETL logger."""
    global _logger_instance
    if _logger_instance is None:
        logger = logging.getLogger("etl")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            )
            logger.addHandler(handler)

        _logger_instance = logger

    return _logger_instance


def _build_oracle_url(
    host: str,
    port: int,
    service_name: str,
    username: str,
    password: str,
) -> URL:
    """Build an Oracle oracledb URL without exposing credentials in string formatting."""
    return URL.create(
        "oracle+oracledb",
        username=username,
        password=password,
        host=host,
        port=port,
        query={"service_name": service_name},
    )


def create_bronze_engine(cfg: Config) -> Engine:
    """Create the SQLAlchemy engine for the BRONZE source database."""
    return create_engine(
        _build_oracle_url(
            cfg.bronze_host,
            cfg.bronze_port,
            cfg.bronze_service_name,
            cfg.bronze_username,
            cfg.bronze_password,
        ),
        pool_pre_ping=True,
        connect_args={"tcp_connect_timeout": 30},
    )


def create_pocc_engine(cfg: Config) -> Engine:
    """Create the SQLAlchemy engine for the POCC target database."""
    return create_engine(
        _build_oracle_url(
            cfg.pocc_host,
            cfg.pocc_port,
            cfg.pocc_service_name,
            cfg.pocc_username,
            cfg.pocc_password,
        ),
        pool_pre_ping=True,
        connect_args={"tcp_connect_timeout": 30},
    )


def test_oracle_connection(
    engine: Engine, logger: Logger, name: str = "Oracle"
) -> bool:
    """Return True when the database accepts a simple query."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM DUAL"))
        logger.info("%s connection: SUCCESS", name)
        return True
    except Exception:
        logger.exception("%s connection: FAILED", name)
        return False


def load_sql_file(path: str) -> str:
    """Read a SQL query from a file, stripping the trailing semicolon."""
    sql = Path(path).read_text(encoding="utf-8").strip()
    if not sql:
        raise ValueError(f"SQL file is empty: {path}")
    return sql.rstrip(";").strip()
```

#### Source Reader & ETL Transform Pattern (`src/extract/__init__.py`)

```python
# src/extract/__init__.py
from logging import Logger

import polars as pl
from sqlalchemy import Engine

from src.common import load_sql_file
from src.config import DEFAULT_TRANSFORM_SQL_FILE_PATH, Config


def read_source_data(
    engine: Engine,
    cfg: Config,
    logger: Logger,
) -> pl.DataFrame:
    """Read the source query from file into a Polars DataFrame."""
    query = load_sql_file(cfg.sql_file_path)
    logger.info("Reading source data from query file: %s", cfg.sql_file_path)

    result = pl.read_database(query=query, connection=engine)

    logger.info("Source read complete: %s records", result.height)
    return result


def get_source_schema() -> pl.Schema:
    """Return the expected schema for transformed records."""
    schema = {name: pl.String for name in _STRING_COLUMNS}
    schema.update({name: pl.Datetime("us") for name in _DATETIME_COLUMNS})
    return pl.Schema(schema)


def _validate_columns(raw_df: pl.DataFrame, schema: pl.Schema) -> None:
    """Stop the pipeline when required source columns are missing."""
    missing = [name for name in schema.keys() if name not in raw_df.columns]
    if missing:
        raise ValueError(f"Missing required source columns: {missing}")


def _drop_null_keys(
    df: pl.DataFrame, primary_key: tuple[str, ...], logger: Logger
) -> pl.DataFrame:
    """Drop rows whose primary key is null; they can never be merged."""
    null_mask = pl.any_horizontal(pl.col(name).is_null() for name in primary_key)
    null_count = df.filter(null_mask).height
    if null_count:
        logger.warning("Dropping %s rows with null primary key", null_count)
        df = df.filter(~null_mask)
    return df


def _drop_duplicate_keys(
    df: pl.DataFrame, primary_key: tuple[str, ...], logger: Logger
) -> pl.DataFrame:
    """Keep the last row per primary key, preserving the source order."""
    duplicate_count = df.filter(
        pl.any_horizontal(pl.col(name).is_duplicated() for name in primary_key)
    ).height
    if duplicate_count:
        logger.warning(
            "Detected %s duplicate primary key rows; keeping the last row",
            duplicate_count,
        )
        df = df.unique(
            subset=list(primary_key), keep="last", maintain_order=True
        )
    return df


def transform(
    raw_df: pl.DataFrame,
    schema: pl.Schema,
    primary_key: tuple[str, ...],
    logger: Logger,
    transform_sql_path: str = DEFAULT_TRANSFORM_SQL_FILE_PATH,
) -> pl.DataFrame:
    """Clean source records using externalized SQL, drop invalid keys, and verify schema."""
    logger.info("Starting transformation: %s source records", raw_df.height)

    _validate_columns(raw_df, schema)
    transform_sql = load_sql_file(transform_sql_path)

    with pl.SQLContext(raw_source=raw_df, eager=True) as sql_context:
        result = sql_context.execute(transform_sql)

    result = _drop_null_keys(result, primary_key, logger)
    result = _drop_duplicate_keys(result, primary_key, logger)

    if result.schema != schema:
        raise ValueError(f"Transformed schema mismatch: {result.schema}")

    logger.info("Transformation complete: %s records", result.height)
    return result
```

The functions above use public Polars APIs:

- `pl.Schema(...)` defines an ordered mapping of column names to Polars data types.
- `pl.read_database(query=..., connection=...)` executes the externalized SQL query from `src/query/REALIZATION.sql` against a SQLAlchemy `Engine` and returns a `pl.DataFrame`.
- `pl.SQLContext(raw_source=raw_df, eager=True)` registers the input frame and executes the transformation query loaded from `src/query/TRANSFORM.sql`.
- `DataFrame.height` returns the row count without triggering a Spark-style distributed action.

Store both the extraction SQL (`src/query/REALIZATION.sql`) and transformation SQL (`src/query/TRANSFORM.sql`) in `src/query/` and load them inside functions via `load_sql_file()`. Prefer SQL inside `pl.SQLContext` over long chains of `.select()` / `.with_columns()`, while using native Polars expressions for primary-key null filtering and deduplication (`df.unique(..., keep="last", maintain_order=True)`).

#### Database Loader Pattern (`src/load/__init__.py`)

```python
# src/load/__init__.py
from logging import Logger
from typing import Sequence

import polars as pl
from sqlalchemy import Engine, text

from src.config import Config


def _validate_table(table: str, cfg: Config) -> None:
    """Reject SQL identifiers that are not allow-listed in the configuration."""
    if table not in cfg.allowed_db_tables:
        raise ValueError(f"Table is not allowed: {table}")


def _build_insert_sql(table: str, columns: Sequence[str]) -> str:
    """Build an INSERT statement with named bind parameters."""
    column_list = ", ".join(columns)
    placeholders = ", ".join(f":{column}" for column in columns)
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def _build_merge_sql(
    target_table: str,
    staging_table: str,
    primary_key: Sequence[str],
    columns: Sequence[str],
) -> str:
    """Build a MERGE INTO statement on the target table's primary key with audit timestamps."""
    on_clause = " AND ".join(f"tgt.{key} = src.{key}" for key in primary_key)
    update_columns = [column for column in columns if column not in primary_key]
    update_set = ",\n    ".join(
        f"tgt.{column} = src.{column}" for column in update_columns
    )
    insert_columns = ", ".join([*columns, "CREATED_ON", "UPDATED_ON"])
    insert_values = ", ".join(
        [f"src.{column}" for column in columns] + ["SYSTIMESTAMP", "SYSTIMESTAMP"]
    )
    merge_sql = f"""
MERGE INTO {target_table} tgt
USING {staging_table} src
ON ({on_clause})
WHEN MATCHED THEN UPDATE SET
    {update_set},
    tgt.UPDATED_ON = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT ({insert_columns})
    VALUES ({insert_values})
"""
    return merge_sql.strip()


def truncate_staging(engine: Engine, cfg: Config, logger: Logger) -> None:
    """Clear the staging table so retries and reruns cannot leave stale rows."""
    _validate_table(cfg.staging_table, cfg)
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {cfg.staging_table}"))
    logger.info("Staging table truncated: %s", cfg.staging_table)


def insert_staging(
    engine: Engine, df: pl.DataFrame, cfg: Config, logger: Logger
) -> None:
    """Insert the transformed records into the staging table in one transaction."""
    _validate_table(cfg.staging_table, cfg)

    rows = list(df.iter_rows(named=True))
    if not rows:
        logger.info("No records to insert into staging")
        return

    insert_sql = _build_insert_sql(cfg.staging_table, df.columns)
    with engine.begin() as connection:
        connection.execute(text(insert_sql), rows)
    logger.info(
        "Inserted %s records into staging table: %s", len(rows), cfg.staging_table
    )


def merge_into_target(
    engine: Engine,
    columns: Sequence[str],
    cfg: Config,
    logger: Logger,
) -> None:
    """MERGE the staging table into the target table on its primary key, in one transaction."""
    _validate_table(cfg.target_table, cfg)
    _validate_table(cfg.staging_table, cfg)

    merge_sql = _build_merge_sql(
        cfg.target_table, cfg.staging_table, cfg.primary_key, columns
    )
    with engine.begin() as connection:
        result = connection.execute(text(merge_sql))
        logger.info(
            "Merge into %s complete: rows affected=%s",
            cfg.target_table,
            result.rowcount,
        )


def load_to_oracle(
    engine: Engine, df: pl.DataFrame, cfg: Config, logger: Logger
) -> None:
    """Stage a Polars DataFrame, MERGE it into the target table, then clean staging."""
    logger.info("Loading %s records to Oracle", df.height)
    _validate_table(cfg.staging_table, cfg)
    _validate_table(cfg.target_table, cfg)

    truncate_staging(engine, cfg, logger)
    insert_staging(engine, df, cfg, logger)
    merge_into_target(engine, list(df.columns), cfg, logger)
    truncate_staging(engine, cfg, logger)
```

> **Important Production Rule:** Never use `df.write_database(..., if_table_exists="replace")` on Oracle staging tables in production, as `replace` drops and recreates the table—destroying primary key constraints, `VARCHAR2` lengths, indexes, and role grants. Always use `truncate_staging()` followed by `insert_staging()` with named bind parameters, then `merge_into_target()`, and a final `truncate_staging()`.

#### Config Singleton with Frozen Dataclass (`src/config/__init__.py`)

```python
# src/config/__init__.py
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

DEFAULT_SQL_FILE_PATH = "src/query/REALIZATION.sql"
DEFAULT_TRANSFORM_SQL_FILE_PATH = "src/query/TRANSFORM.sql"

_config_instance: Optional["Config"] = None


@dataclass(frozen=True)
class Config:
    bronze_host: str
    bronze_port: int
    bronze_service_name: str
    bronze_username: str
    bronze_password: str
    pocc_host: str
    pocc_port: int
    pocc_service_name: str
    pocc_username: str
    pocc_password: str
    staging_table: str
    target_table: str
    primary_key: tuple[str, ...]
    allowed_db_tables: frozenset[str]
    sql_file_path: str = DEFAULT_SQL_FILE_PATH
    transform_sql_file_path: str = DEFAULT_TRANSFORM_SQL_FILE_PATH
```

Always mask password fields (`bronze_password`, `pocc_password`) in `Config.__repr__()` so credentials are never leaked in log output, and build database URLs via `sqlalchemy.URL.create(...)` in `src/common/__init__.py`.

### 6. State, Data Access, and Business Logic

| Concern | Location |
|---|---|
| Config values & env parsers | `src/config/__init__.py`, loaded once on demand and passed explicitly |
| Shared utilities (Logger, Engines, SQL file loader) | `src/common/__init__.py`, reusable infrastructure helpers |
| Source extraction & Polars transformations | `src/extract/__init__.py`, reading source queries and returning cleaned Polars frames |
| Target database staging & `MERGE INTO` | `src/load/__init__.py`, transactional `TRUNCATE`, `INSERT`, and `MERGE INTO` |
| SQL queries | `src/query/REALIZATION.sql` and `src/query/TRANSFORM.sql` |

Rules:

- Never hardcode credentials.
- Polars runs in-process.
- Do not call `.to_pandas()` for transformation logic.
- Store extraction and transformation SQL in `src/query/` and execute transformation SQL via `pl.SQLContext(..., eager=True)`.
- Drop null primary keys and deduplicate primary keys (`keep="last", maintain_order=True`) before loading to prevent Oracle `ORA-30926` / `ORA-00001` errors.
- Validate dynamic SQL table identifiers against `cfg.allowed_db_tables`.
- Read source data through `pl.read_database()` against a SQLAlchemy `Engine`.
- Load the target table only through `truncate_staging()` $\rightarrow$ `insert_staging()` $\rightarrow$ `merge_into_target()` $\rightarrow$ `truncate_staging()`, with each database step wrapped in `with engine.begin():`.

### 7. Reusable Components

| Component | Location | Purpose | Usage |
|---|---|---|---|
| `get_config()` | `src.config` | Return validated frozen configuration | `cfg = get_config()` |
| `get_logger()` | `src.common` | Return singleton logger | `logger = get_logger()` |
| `create_bronze_engine()` / `create_pocc_engine()` | `src.common` | Build SQLAlchemy engines for source & target Oracle DBs | `engine = create_bronze_engine(cfg)` |
| `test_oracle_connection()` | `src.common` | Verify Oracle connectivity (`SELECT 1 FROM DUAL`) | `test_oracle_connection(engine, logger, name)` |
| `load_sql_file()` | `src.common` | Read `.sql` file from `src/query/` and strip trailing `;` | `sql = load_sql_file(path)` |
| `read_source_data()` | `src.extract` | Execute source SQL query into a Polars DataFrame | `raw_df = read_source_data(engine, cfg, logger)` |
| `transform()` | `src.extract` | Execute `TRANSFORM.sql` in `pl.SQLContext`, clean keys, verify schema | `df = transform(raw_df, schema, cfg.primary_key, logger)` |
| `load_to_oracle()` | `src.load` | Truncate staging, insert batch, `MERGE INTO` target, truncate staging | `load_to_oracle(pocc_engine, df, cfg, logger)` |

### 8. Import and Export Standards

PEP 8 import order:

1. Standard library
2. Third-party packages, such as `polars` and `sqlalchemy`
3. Internal imports

```python
from src.common import (
    create_bronze_engine,
    create_pocc_engine,
    get_logger,
    test_oracle_connection,
)
from src.config import Config, get_config
from src.extract import get_source_schema, read_source_data, transform
from src.load import load_to_oracle
```

Rules:

- Use absolute imports starting with `src.`.
- Do not use wildcard imports.
- Enforce a strict one-way dependency graph (`src.config` $\rightarrow$ `src.common` $\rightarrow$ `src.extract` / `src.load` $\rightarrow$ `src.main`) to prevent circular imports.
- Keep one package per concern (`common`, `config`, `extract`, `load`, `query`).
- Public functions are exposed directly through package `__init__.py` files.

### 9. Best Practices

- Validate both source (`BRONZE`) and target (`POCC`) database connections before processing any data.
- Raise `ETLConnectionError` (or exit non-zero in CLI `main()`) on fail-fast connection checks.
- Log `DataFrame.height` for materialized frames and skip database writes cleanly when `df.height == 0`.
- Define schemas with `pl.Schema` and Polars data types (`pl.String`, `pl.Datetime("us")`, etc.).
- Keep SQL queries in standalone `.sql` files under `src/query/` loaded via `load_sql_file()` at function execution time.
- Filter out null primary keys and deduplicate duplicate primary keys (`keep="last"`) after executing `pl.SQLContext`.
- Always dispose of SQLAlchemy engines inside `finally:` blocks.
- Pin `polars`, `sqlalchemy`, `oracledb`, `python-dotenv`, and `pytest` versions in `requirements.txt`.
- Add unit tests with `polars.testing.assert_frame_equal` and verify Airflow DAG import safety.

### 10. Do and Don't Examples

| Do | Don't |
|---|---|
| `transform(raw_df, schema, cfg.primary_key, logger)` | `ETLTransformer().transform(data)` |
| Load SQL from `src/query/TRANSFORM.sql` via `load_sql_file()` | Hardcode multi-line SQL strings inside Python functions |
| `sql_context.execute(transform_sql)` | `df.lazy().select(...).filter(...).collect()` for routine SQL transforms |
| `truncate_staging()` + `insert_staging()` (named bind parameters) | `df.write_database(..., if_table_exists="replace")` on Oracle tables |
| `merge_into_target()` on primary key with `SYSTIMESTAMP` audit columns | `TRUNCATE` and reload the production target table |
| `result.height` | Use Spark-style `result.count()` for row count |
| `engine.dispose()` in `finally:` | Leave connection pools undisposed |
| `polars.testing.assert_frame_equal(...)` | Compare frames through string output |

### 11. New Feature Checklist

- [ ] Placed extraction and transformation `.sql` files in `src/query/`
- [ ] Implemented plain functions across `src/common/`, `src/config/`, `src/extract/`, and `src/load/`
- [ ] Passed `cfg`, `logger`, `schema`, `primary_key`, frames, and engines explicitly
- [ ] Added type hints and return types to all functions
- [ ] Defined expected schema with `pl.Schema` in `src/extract/__init__.py`
- [ ] Executed `src/query/TRANSFORM.sql` via `pl.SQLContext` and deduplicated primary keys
- [ ] Used `DataFrame.height` for eager row counts and handled empty DataFrames (`df.height == 0`)
- [ ] Validated database table names against `cfg.allowed_db_tables`
- [ ] Loaded the target table via `truncate_staging` $\rightarrow$ `insert_staging` $\rightarrow$ `merge_into_target` $\rightarrow$ `truncate_staging`
- [ ] Validated both source and target database connections before running extraction
- [ ] Added unit, SQL-loading, and DAG tests under `tests/`
- [ ] Verified locally with `pytest` and `python -m src.main`

### 12. Example Structure and Main Entry Point

```text
src/
├── __init__.py
├── main.py
├── common/
│   └── __init__.py
├── config/
│   └── __init__.py
├── extract/
│   └── __init__.py
├── load/
│   └── __init__.py
└── query/
    ├── REALIZATION.sql
    └── TRANSFORM.sql
```

```python
# src/main.py
import sys
from logging import Logger

from src.common import (
    create_bronze_engine,
    create_pocc_engine,
    get_logger,
    test_oracle_connection,
)
from src.config import Config, get_config
from src.extract import get_source_schema, read_source_data, transform
from src.load import load_to_oracle


class ETLConnectionError(RuntimeError):
    """Raised when a required database connection cannot be established."""


def _check_connections(cfg: Config, logger: Logger) -> None:
    bronze_engine = create_bronze_engine(cfg)
    pocc_engine = create_pocc_engine(cfg)
    try:
        bronze_ok = test_oracle_connection(bronze_engine, logger, "Oracle BRONZE (source)")
        pocc_ok = test_oracle_connection(pocc_engine, logger, "Oracle POCC (target)")
    finally:
        bronze_engine.dispose()
        pocc_engine.dispose()

    if not bronze_ok or not pocc_ok:
        raise ETLConnectionError(
            "Connection validation failed; refusing to run the ETL job"
        )


def check_connections() -> None:
    cfg = get_config()
    logger = get_logger()
    _check_connections(cfg, logger)


def run_pipeline() -> None:
    cfg = get_config()
    logger = get_logger()
    _check_connections(cfg, logger)

    bronze_engine = create_bronze_engine(cfg)
    pocc_engine = create_pocc_engine(cfg)
    try:
        raw_df = read_source_data(bronze_engine, cfg, logger)
        df = transform(raw_df, get_source_schema(), cfg.primary_key, logger)
        if df.height == 0:
            logger.info("No records to load; skipping staging and merge")
            return
        load_to_oracle(pocc_engine, df, cfg, logger)
    finally:
        bronze_engine.dispose()
        pocc_engine.dispose()


def main() -> None:
    try:
        run_pipeline()
    except Exception:
        get_logger().exception("ETL run failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

### Quick Reference

| Task | Command or pattern |
|---|---|
| Install core dependencies | `pip install -r requirements.txt` |
| Run locally | `python -m src.main` |
| Run test suite | `pytest` |
| Load SQL query from file | `query = load_sql_file(cfg.sql_file_path)` |
| Read source query into a frame | `pl.read_database(query=query, connection=bronze_engine)` |
| Execute SQL transformation | `pl.SQLContext(raw_source=raw_df, eager=True).execute(transform_sql)` |
| Deduplicate on primary key | `df.unique(subset=list(primary_key), keep="last", maintain_order=True)` |
| Get row count | `df.height` |
| Stage records safely | `truncate_staging(engine, cfg, logger)` then `insert_staging(engine, df, cfg, logger)` |
| Upsert into target table | `merge_into_target(engine, list(df.columns), cfg, logger)` |
| Test frames | `polars.testing.assert_frame_equal(actual, expected)` |
