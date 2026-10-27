"""Source extraction layer."""

from logging import Logger

import polars as pl
from sqlalchemy import Engine

from src.common.sql import load_sql_file
from src.config.settings import Config


def read_source_data(
    engine: Engine,
    cfg: Config,
    logger: Logger,
) -> pl.DataFrame:
    """Read the source query from file into a Polars DataFrame."""
    query = load_sql_file(cfg.sql_file_path)
    logger.info("Reading source data from query file: %s", cfg.sql_file_path)

    result = pl.read_database(query=query, connection=engine)
    result = result.rename({col: col.upper() for col in result.columns})

    logger.info("Source read complete: %s records", result.height)
    return result

