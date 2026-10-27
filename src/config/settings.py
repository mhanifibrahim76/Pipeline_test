"""ETL configuration loaded once from environment variables."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

DEFAULT_SQL_FILE_PATH = "src/query/REALIZATION.sql"
DEFAULT_TRANSFORM_SQL_FILE_PATH = "src/query/TRANSFORM.sql"

_config_instance: Optional["Config"] = None

_BRONZE_VARS = (
    "DB_HOST_BRONZE",
    "DB_PORT_BRONZE",
    "DB_NAME_BRONZE",
    "DB_USERNAME_BRONZE",
    "DB_PASSWORD_BRONZE",
)
_POCC_VARS = (
    "DB_HOST_POCC",
    "DB_PORT_POCC",
    "DB_NAME_POCC",
    "DB_USERNAME_POCC",
    "DB_PASSWORD_POCC",
)
_TABLE_VARS = ("DB_TABLE_POCC", "DB_TABLE_STG_POCC", "DB_PRIMARY_KEY")

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def parse_bool(value: Optional[str], default: bool) -> bool:
    """Parse a boolean from an optional string, falling back to default."""
    if value is None or value.strip() == "":
        return default
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def parse_int(value: Optional[str], default: int) -> int:
    """Parse an integer from an optional string, falling back to default."""
    if value is None or value.strip() == "":
        return default
    try:
        return int(value.strip())
    except ValueError as error:
        raise ValueError(f"Invalid integer value: {value!r}") from error


@dataclass(frozen=True)
class Config:
    """Frozen configuration for one ETL run. Never log the raw repr."""

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
    target_table: str
    staging_table: str
    primary_key: tuple[str, ...]
    sql_file_path: str = DEFAULT_SQL_FILE_PATH
    transform_sql_file_path: str = DEFAULT_TRANSFORM_SQL_FILE_PATH
    bronze_driver: str = "oracle+oracledb"
    pocc_driver: str = "oracle+oracledb"

    @property
    def allowed_db_tables(self) -> frozenset[str]:
        """Allow-list of tables that queries are permitted to target."""
        return frozenset({self.target_table, self.staging_table})

    def __repr__(self) -> str:
        """Mask credentials so the config can be logged safely."""
        return (
            "Config("
            f"bronze={self.bronze_username}@{self.bronze_host}:"
            f"{self.bronze_port}/{self.bronze_service_name}, "
            f"pocc={self.pocc_username}@{self.pocc_host}:"
            f"{self.pocc_port}/{self.pocc_service_name}, "
            f"target_table={self.target_table!r}, "
            f"staging_table={self.staging_table!r}, "
            f"primary_key={self.primary_key!r}, "
            f"sql_file_path={self.sql_file_path!r}, "
            f"transform_sql_file_path={self.transform_sql_file_path!r}, "
            "passwords=***)"
        )


def _require(name: str) -> str:
    """Return a required, whitespace-stripped environment variable."""
    value = os.environ.get(name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    value = value.strip()
    if not value:
        raise ValueError(f"Environment variable is empty: {name}")
    return value


def _check_required_vars() -> None:
    """Fail fast, listing every missing variable in one error."""
    missing = [
        name
        for name in _BRONZE_VARS + _POCC_VARS + _TABLE_VARS
        if not os.environ.get(name, "").strip()
    ]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def get_config() -> Config:
    """Return the singleton Config, loaded once from the environment."""
    global _config_instance
    if _config_instance is None:
        load_dotenv()
        _check_required_vars()

        target_table = _require("DB_TABLE_POCC")
        staging_table = _require("DB_TABLE_STG_POCC")
        primary_key = tuple(
            key.strip().upper()
            for key in _require("DB_PRIMARY_KEY").split(",")
            if key.strip()
        )
        if not primary_key:
            raise ValueError("DB_PRIMARY_KEY must contain at least one column")

        _config_instance = Config(
            bronze_host=_require("DB_HOST_BRONZE"),
            bronze_port=parse_int(_require("DB_PORT_BRONZE"), default=1521),
            bronze_service_name=_require("DB_NAME_BRONZE"),
            bronze_username=_require("DB_USERNAME_BRONZE"),
            bronze_password=_require("DB_PASSWORD_BRONZE"),
            pocc_host=_require("DB_HOST_POCC"),
            pocc_port=parse_int(_require("DB_PORT_POCC"), default=1521),
            pocc_service_name=_require("DB_NAME_POCC"),
            pocc_username=_require("DB_USERNAME_POCC"),
            pocc_password=_require("DB_PASSWORD_POCC"),
            target_table=target_table,
            staging_table=staging_table,
            primary_key=primary_key,
            sql_file_path=os.environ.get("SQL_FILE_PATH", "").strip()
            or DEFAULT_SQL_FILE_PATH,
            transform_sql_file_path=os.environ.get("TRANSFORM_SQL_FILE_PATH", "").strip()
            or DEFAULT_TRANSFORM_SQL_FILE_PATH,
            bronze_driver=os.environ.get("DB_DRIVER_BRONZE", "").strip()
            or "oracle+oracledb",
            pocc_driver=os.environ.get("DB_DRIVER_POCC", "").strip()
            or "oracle+oracledb",
        )
    return _config_instance


def reset_config() -> None:
    """Clear the cached config so the next get_config() reloads the environment."""
    global _config_instance
    _config_instance = None


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean option from the environment with a default."""
    return parse_bool(os.environ.get(name), default)


def env_int(name: str, default: int) -> int:
    """Read an integer option from the environment with a default."""
    return parse_int(os.environ.get(name), default)

