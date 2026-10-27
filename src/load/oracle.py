"""Database load layer: staging truncate, insert, target merge, and cleanup."""

from logging import Logger
from typing import Sequence, Union

import polars as pl
from sqlalchemy import Engine, text

from src.config.settings import Config


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
    dialect: str = "oracle",
) -> str:
    """Build a MERGE INTO statement on the target table's primary key with Asia/Jakarta (WIB) audit timestamps."""
    on_clause = " AND ".join(f"tgt.{key} = src.{key}" for key in primary_key)
    update_columns = [column for column in columns if column not in primary_key]

    if dialect.startswith("postgres"):
        update_set = ",\n    ".join(
            f"{column} = src.{column}" for column in update_columns
        )
        updated_on = "UPDATED_ON = CURRENT_TIMESTAMP"
        insert_columns = ", ".join([*columns, "CREATED_ON", "UPDATED_ON"])
        insert_values = ", ".join(
            [f"src.{column}" for column in columns] + ["CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"]
        )
    else:
        update_set = ",\n    ".join(
            f"tgt.{column} = src.{column}" for column in update_columns
        )
        updated_on = "tgt.UPDATED_ON = CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)"
        insert_columns = ", ".join([*columns, "CREATED_ON", "UPDATED_ON"])
        insert_values = ", ".join(
            [f"src.{column}" for column in columns]
            + [
                "CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)",
                "CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)",
            ]
        )

    merge_sql = f"""
MERGE INTO {target_table} tgt
USING {staging_table} src
ON ({on_clause})
WHEN MATCHED THEN UPDATE SET
    {update_set},
    {updated_on}
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
    columns: Union[Sequence[str], pl.DataFrame],
    cfg: Config,
    logger: Logger,
) -> None:
    """MERGE the staging table into the target table on its primary key, in one transaction."""
    _validate_table(cfg.target_table, cfg)
    _validate_table(cfg.staging_table, cfg)

    col_list = list(columns.columns) if isinstance(columns, pl.DataFrame) else list(columns)
    dialect = getattr(engine.dialect, "name", "oracle")
    merge_sql = _build_merge_sql(
        cfg.target_table, cfg.staging_table, cfg.primary_key, col_list, dialect=dialect
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

