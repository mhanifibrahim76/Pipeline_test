"""In-memory Polars schema definitions, validation, and transformations."""

from logging import Logger

import polars as pl

from src.common.sql import load_sql_file
from src.config.settings import DEFAULT_TRANSFORM_SQL_FILE_PATH

_STRING_COLUMNS = (
    "SCHEDULE_CODE",
    "CABANG",
    "DERMAGA",
    "JENIS_PELAYANAN",
    "NAMA_KAPAL_REALISASI",
    "OPERATOR_REALISASI",
)
_DATETIME_COLUMNS = (
    "SCHEDULE_DATE",
    "REALISASI_TIBA_ALUR",
    "REALISASI_SANDAR",
    "REALISASI_MULAI_PELAYANAN",
    "REALISASI_SELESAI_PELAYANAN",
    "REALISASI_TUTUP_RAMPDOOR",
    "REALISASI_KEBERANGKATAN",
)


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

