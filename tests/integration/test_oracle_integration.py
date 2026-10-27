"""Integration tests against a real Oracle environment.

Disabled by default: set RUN_ORACLE_INTEGRATION=1 (with the *_DEV .env values)
to run them. They must never be pointed at production tables.
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_ORACLE_INTEGRATION", "").strip().lower()
    not in {"1", "true", "yes"},
    reason="Oracle integration tests disabled; set RUN_ORACLE_INTEGRATION=1 to enable",
)

from sqlalchemy import text  # noqa: E402

from src.common.database import (  # noqa: E402
    create_bronze_engine,
    create_pocc_engine,
    test_oracle_connection,
)
from src.common.logger import get_logger  # noqa: E402
from src.common.transform import get_source_schema  # noqa: E402
from src.config.settings import get_config  # noqa: E402
from src.extract.reader import read_source_data  # noqa: E402
from src.load.oracle import insert_staging, truncate_staging  # noqa: E402
from src.main import run_pipeline  # noqa: E402

LOGGER = get_logger()


@pytest.fixture(scope="module")
def cfg():
    return get_config()


@pytest.fixture(scope="module")
def bronze_engine(cfg):
    engine = create_bronze_engine(cfg)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def pocc_engine(cfg):
    engine = create_pocc_engine(cfg)
    yield engine
    engine.dispose()


def _count(pocc_engine, table):
    with pocc_engine.connect() as connection:
        return connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()


def test_source_connection_validates(bronze_engine):
    assert test_oracle_connection(bronze_engine, LOGGER, "Oracle BRONZE (source)")


def test_target_connection_validates(pocc_engine):
    assert test_oracle_connection(pocc_engine, LOGGER, "Oracle POCC (target)")


def test_extraction_query_executes(bronze_engine, cfg):
    raw_df = read_source_data(bronze_engine, cfg, LOGGER)
    assert set(get_source_schema().keys()).issubset(raw_df.columns)


def test_pipeline_is_idempotent_and_cleans_staging(pocc_engine, cfg):
    run_pipeline()
    run_pipeline()

    assert _count(pocc_engine, cfg.staging_table) == 0

    duplicates_sql = (
        f"SELECT COUNT(*) FROM (SELECT {cfg.primary_key[0]} FROM {cfg.target_table} "
        f"GROUP BY {cfg.primary_key[0]} HAVING COUNT(*) > 1)"
    )
    with pocc_engine.connect() as connection:
        assert connection.execute(text(duplicates_sql)).scalar_one() == 0


def test_staging_rejects_duplicate_keys(pocc_engine, cfg):
    from datetime import datetime

    import polars as pl
    from sqlalchemy.exc import IntegrityError

    truncate_staging(pocc_engine, cfg, LOGGER)
    schema = get_source_schema()
    base_row = {
        "SCHEDULE_CODE": "INTEGRATION_TEST_DUP",
        "SCHEDULE_DATE": datetime(2026, 9, 8, 10, 0),
        "CABANG": "MERAK",
        "DERMAGA": "DERMAGA_MERAK",
        "JENIS_PELAYANAN": "EKONOMI",
        "NAMA_KAPAL_REALISASI": "KAPAL SATU",
        "OPERATOR_REALISASI": "PT PELNI",
        "REALISASI_TIBA_ALUR": datetime(2026, 9, 8, 11, 0),
        "REALISASI_SANDAR": datetime(2026, 9, 8, 11, 15),
        "REALISASI_MULAI_PELAYANAN": datetime(2026, 9, 8, 11, 30),
        "REALISASI_SELESAI_PELAYANAN": datetime(2026, 9, 8, 12, 30),
        "REALISASI_TUTUP_RAMPDOOR": datetime(2026, 9, 8, 12, 40),
        "REALISASI_KEBERANGKATAN": datetime(2026, 9, 8, 13, 0),
    }
    duplicate_df = pl.DataFrame(
        [base_row, base_row], schema=schema, strict=False
    )

    with pytest.raises(IntegrityError):
        insert_staging(pocc_engine, duplicate_df, cfg, LOGGER)

    # The failed insert rolled back completely: staging is empty.
    assert _count(pocc_engine, cfg.staging_table) == 0
