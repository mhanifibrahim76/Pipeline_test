"""Unit tests for database helpers that do not require a live Oracle."""

import logging

import pytest
from sqlalchemy import create_engine, text

from src.common.database import (
    _build_oracle_url,
    create_bronze_engine,
    create_pocc_engine,
    test_oracle_connection as oracle_connection_ok,
)
from src.config.settings import get_config
from src.load.oracle import (
    _build_insert_sql,
    _build_merge_sql,
    _validate_table,
)

LOGGER = logging.getLogger("etl.test")

BUSINESS_COLUMNS = [
    "SCHEDULE_CODE",
    "SCHEDULE_DATE",
    "CABANG",
    "DERMAGA",
    "JENIS_PELAYANAN",
    "NAMA_KAPAL_REALISASI",
    "OPERATOR_REALISASI",
    "REALISASI_TIBA_ALUR",
    "REALISASI_SANDAR",
    "REALISASI_MULAI_PELAYANAN",
    "REALISASI_SELESAI_PELAYANAN",
    "REALISASI_TUTUP_RAMPDOOR",
    "REALISASI_KEBERANGKATAN",
]


def test_validate_table_accepts_allow_listed_tables(config_env):
    cfg = get_config()
    _validate_table(cfg.target_table, cfg)
    _validate_table(cfg.staging_table, cfg)


def test_validate_table_rejects_unknown_tables(config_env):
    cfg = get_config()
    with pytest.raises(ValueError, match="Table is not allowed"):
        _validate_table("EVIL_TABLE", cfg)


def test_build_insert_sql_uses_named_binds(config_env):
    sql = _build_insert_sql("STG_TABLE", ["COL_A", "COL_B"])
    assert sql == "INSERT INTO STG_TABLE (COL_A, COL_B) VALUES (:COL_A, :COL_B)"


def test_build_merge_sql_merges_on_primary_key(config_env):
    sql = _build_merge_sql(
        "MART_TABLE", "STG_TABLE", ("SCHEDULE_CODE",), BUSINESS_COLUMNS
    )

    assert sql.startswith("MERGE INTO MART_TABLE tgt")
    assert "USING STG_TABLE src" in sql
    assert "ON (tgt.SCHEDULE_CODE = src.SCHEDULE_CODE)" in sql


def _merge_sections(sql):
    update_section = sql.split("WHEN MATCHED THEN UPDATE SET")[1]
    update_section = update_section.split("WHEN NOT MATCHED")[0]
    insert_section = sql.split("WHEN NOT MATCHED THEN INSERT")[1]
    return update_section, insert_section


def test_build_merge_sql_updates_non_key_columns_and_audit_timestamp(config_env):
    sql = _build_merge_sql(
        "MART_TABLE", "STG_TABLE", ("SCHEDULE_CODE",), BUSINESS_COLUMNS
    )

    update_section, insert_section = _merge_sections(sql)
    for column in BUSINESS_COLUMNS[1:]:
        assert f"tgt.{column} = src.{column}" in update_section
    assert "tgt.UPDATED_ON = CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)" in update_section
    # The primary key itself is never updated, and CREATED_ON is never overwritten.
    assert "tgt.SCHEDULE_CODE = " not in update_section
    assert "CREATED_ON" not in update_section


def test_build_merge_sql_sets_created_on_only_on_insert(config_env):
    sql = _build_merge_sql(
        "MART_TABLE", "STG_TABLE", ("SCHEDULE_CODE",), BUSINESS_COLUMNS
    )

    insert_section = _merge_sections(sql)[1]
    assert "CREATED_ON" in insert_section
    assert "UPDATED_ON" in insert_section
    assert "CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)" in insert_section
    for column in BUSINESS_COLUMNS:
        assert f"src.{column}" in insert_section


def test_oracle_url_masks_password():
    url = _build_oracle_url("db.host", 1524, "SVC", "user", "super-secret")
    rendered = str(url)
    assert "super-secret" not in rendered
    assert rendered.startswith("oracle+oracledb://user:***@db.host:1524?service_name=SVC")


def test_create_engines_use_configured_endpoints(config_env):
    cfg = get_config()

    bronze_url = create_bronze_engine(cfg).url
    assert bronze_url.host == "bronze.example.com"
    assert bronze_url.port == 1524
    assert bronze_url.drivername == "oracle+oracledb"
    assert bronze_url.query["service_name"] == "PDBDWHBRONZE"

    pocc_url = create_pocc_engine(cfg).url
    assert pocc_url.host == "pocc.example.com"
    assert pocc_url.port == 1522
    assert pocc_url.query["service_name"] == "POCC"


def test_oracle_connection_success_returns_true():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE DUAL (DUMMY INTEGER)"))
        connection.execute(text("INSERT INTO DUAL VALUES (1)"))

    assert oracle_connection_ok(engine, LOGGER, "Fake DUAL") is True


def test_oracle_connection_failure_returns_false(caplog):
    engine = create_engine("sqlite:///:memory:")
    with caplog.at_level(logging.ERROR, logger="etl.test"):
        assert oracle_connection_ok(engine, LOGGER, "Broken") is False
    assert "FAILED" in caplog.text
