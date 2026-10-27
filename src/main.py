"""Entry point and explicit pipeline call chain for the fleet realization ETL."""

import sys
from logging import Logger

from src.common.database import (
    create_bronze_engine,
    create_pocc_engine,
    test_oracle_connection,
)
from src.common.logger import get_logger
from src.common.transform import get_source_schema, transform
from src.config.settings import Config, get_config
from src.extract.reader import read_source_data
from src.load.oracle import load_to_oracle


class ETLConnectionError(RuntimeError):
    """Raised when a required database connection cannot be established."""


def _check_connections(cfg: Config, logger: Logger) -> None:
    """Stop the job before any data work when a connection test fails."""
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
    """Airflow task callable: validate both Oracle connections or fail the task."""
    cfg = get_config()
    logger = get_logger()
    logger.info("Validating database connections")
    _check_connections(cfg, logger)
    logger.info("All database connections are valid")


def run_pipeline() -> None:
    """Airflow task callable: run one extract-transform-load batch."""
    cfg = get_config()
    logger = get_logger()
    logger.info("POCC fleet realization ETL run started")
    logger.info("Config: %s", cfg)

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

    logger.info("POCC fleet realization ETL run completed")


def main() -> None:
    """Container entry point: run the pipeline and exit non-zero on failure."""
    try:
        run_pipeline()
    except Exception:
        get_logger().exception("ETL run failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
