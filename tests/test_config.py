"""Unit tests for configuration loading, parsing, and secret masking."""

import pytest

from src.config.settings import (
    DEFAULT_SQL_FILE_PATH,
    DEFAULT_TRANSFORM_SQL_FILE_PATH,
    get_config,
    parse_bool,
    parse_int,
    reset_config,
)
from tests.conftest import TEST_ENV

pytestmark = pytest.mark.usefixtures("config_env")


def test_get_config_loads_all_fields(config_env):
    cfg = get_config()
    assert cfg.bronze_host == "bronze.example.com"
    assert cfg.bronze_port == 1524
    assert cfg.bronze_service_name == "PDBDWHBRONZE"
    assert cfg.bronze_username == "BRONZE_USER"
    assert cfg.pocc_host == "pocc.example.com"
    assert cfg.pocc_port == 1522
    assert cfg.pocc_service_name == "POCC"
    assert cfg.target_table == "MART_POCC_FLEET_REALIZATION_DEV"


def test_get_config_strips_whitespace_and_uppercases_primary_key(config_env):
    cfg = get_config()
    assert cfg.staging_table == "STG_POCC_FLEET_REALIZATION_DEV"
    assert cfg.primary_key == ("SCHEDULE_CODE",)


def test_get_config_builds_table_allow_list(config_env):
    cfg = get_config()
    assert cfg.allowed_db_tables == frozenset(
        {"MART_POCC_FLEET_REALIZATION_DEV", "STG_POCC_FLEET_REALIZATION_DEV"}
    )


def test_get_config_uses_default_sql_file_path(config_env):
    assert get_config().sql_file_path == DEFAULT_SQL_FILE_PATH


def test_get_config_reads_sql_file_path_override(monkeypatch, config_env):
    monkeypatch.setenv("SQL_FILE_PATH", "/data/custom_query.sql")
    reset_config()
    assert get_config().sql_file_path == "/data/custom_query.sql"


def test_get_config_uses_default_transform_sql_file_path(config_env):
    assert get_config().transform_sql_file_path == DEFAULT_TRANSFORM_SQL_FILE_PATH


def test_get_config_reads_transform_sql_file_path_override(monkeypatch, config_env):
    monkeypatch.setenv("TRANSFORM_SQL_FILE_PATH", "/data/custom_transform.sql")
    reset_config()
    assert get_config().transform_sql_file_path == "/data/custom_transform.sql"


def test_get_config_is_cached(config_env):
    assert get_config() is get_config()


def test_missing_required_variable_is_reported(monkeypatch, config_env):
    monkeypatch.delenv("DB_HOST_BRONZE")
    monkeypatch.delenv("DB_PASSWORD_POCC")
    reset_config()
    with pytest.raises(ValueError, match="DB_HOST_BRONZE"):
        get_config()


def test_all_missing_variables_are_listed_at_once(monkeypatch, config_env):
    for name in TEST_ENV:
        monkeypatch.delenv(name)
    reset_config()
    with pytest.raises(ValueError) as error:
        get_config()
    for name in TEST_ENV:
        assert name in str(error.value)


def test_blank_required_variable_is_rejected(monkeypatch, config_env):
    monkeypatch.setenv("DB_PRIMARY_KEY", "   ")
    reset_config()
    with pytest.raises(ValueError, match="DB_PRIMARY_KEY"):
        get_config()


def test_invalid_port_is_rejected(monkeypatch, config_env):
    monkeypatch.setenv("DB_PORT_BRONZE", "not-a-port")
    reset_config()
    with pytest.raises(ValueError, match="Invalid integer value"):
        get_config()


def test_empty_primary_key_is_rejected(monkeypatch, config_env):
    monkeypatch.setenv("DB_PRIMARY_KEY", ",,")
    reset_config()
    with pytest.raises(ValueError, match="at least one column"):
        get_config()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("TRUE", True),
        ("true", True),
        ("1", True),
        ("YES", True),
        ("on", True),
        ("FALSE", False),
        ("no", False),
        ("0", False),
        ("off", False),
        (None, True),
        ("", False),
        ("  ", True),
    ],
)
def test_parse_bool(value, expected):
    assert parse_bool(value, expected) is expected


def test_parse_bool_rejects_garbage():
    with pytest.raises(ValueError, match="Invalid boolean value"):
        parse_bool("banana", False)


@pytest.mark.parametrize(
    "value,expected",
    [("42", 42), (" 7 ", 7), (None, 3), ("", 3), ("  ", 3)],
)
def test_parse_int(value, expected):
    assert parse_int(value, 3) == expected


def test_parse_int_rejects_garbage():
    with pytest.raises(ValueError, match="Invalid integer value"):
        parse_int("1.5", 0)


def test_config_repr_masks_passwords(config_env):
    cfg = get_config()
    text = repr(cfg)
    assert "bronze-secret" not in text
    assert "pocc-secret" not in text
    assert "passwords=***" in text
    assert cfg.bronze_host in text
    assert cfg.target_table in text
