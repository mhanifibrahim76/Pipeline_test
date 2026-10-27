"""Shared pytest fixtures and path setup for the ETL test suite."""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Keep Airflow DAG tests from writing configuration into the real home directory.
os.environ.setdefault("AIRFLOW_HOME", "/tmp/opencode/airflow_home")

import pytest  # noqa: E402

from src.config import settings as config_module  # noqa: E402

TEST_ENV = {
    "DB_HOST_BRONZE": "bronze.example.com",
    "DB_PORT_BRONZE": "1524",
    "DB_NAME_BRONZE": "PDBDWHBRONZE",
    "DB_USERNAME_BRONZE": "BRONZE_USER",
    "DB_PASSWORD_BRONZE": "bronze-secret",
    "DB_HOST_POCC": "pocc.example.com",
    "DB_PORT_POCC": "1522",
    "DB_NAME_POCC": "POCC",
    "DB_USERNAME_POCC": "POCC_USER",
    "DB_PASSWORD_POCC": "pocc-secret",
    "DB_TABLE_POCC": "MART_POCC_FLEET_REALIZATION_DEV",
    "DB_TABLE_STG_POCC": "  STG_POCC_FLEET_REALIZATION_DEV  ",
    "DB_PRIMARY_KEY": " schedule_code ",
}


@pytest.fixture
def config_env(monkeypatch):
    """Provide a complete deterministic environment and a freshly built Config."""
    for name, value in TEST_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SQL_FILE_PATH", raising=False)
    monkeypatch.delenv("TRANSFORM_SQL_FILE_PATH", raising=False)
    monkeypatch.delenv("DB_DRIVER_BRONZE", raising=False)
    monkeypatch.delenv("DB_DRIVER_POCC", raising=False)
    # Keep tests hermetic: never read the developer's real .env file.
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    config_module.reset_config()
    yield config_module.get_config()
    config_module.reset_config()
