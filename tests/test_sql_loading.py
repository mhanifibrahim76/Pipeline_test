"""Unit tests for SQL query file loading."""

from pathlib import Path

import pytest

from src.common.sql import load_sql_file

QUERY_DIR = Path(__file__).resolve().parents[1] / "src" / "query"
REALIZATION_SQL = QUERY_DIR / "REALIZATION.sql"
TRANSFORM_SQL = QUERY_DIR / "TRANSFORM.sql"


def test_load_sql_file_reads_the_realization_query():
    sql = load_sql_file(str(REALIZATION_SQL))
    assert sql.startswith("SELECT")
    assert "BRZ_FERIZY_T_TRX_SCHEDULE" in sql


def test_load_sql_file_reads_the_transform_query():
    sql = load_sql_file(str(TRANSFORM_SQL))
    assert sql.startswith("SELECT")
    assert "FROM raw_source" in sql
    assert "TRIM(SCHEDULE_CODE)" in sql


def test_load_sql_file_strips_trailing_semicolon(tmp_path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT 1 FROM DUAL;", encoding="utf-8")
    assert load_sql_file(str(sql_file)) == "SELECT 1 FROM DUAL"


def test_load_sql_file_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_sql_file(str(tmp_path / "missing.sql"))


def test_load_sql_file_empty_file_raises(tmp_path):
    sql_file = tmp_path / "empty.sql"
    sql_file.write_text("   \n  ", encoding="utf-8")
    with pytest.raises(ValueError, match="SQL file is empty"):
        load_sql_file(str(sql_file))
