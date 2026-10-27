"""Database connectivity, engine factories, and health checks."""

from logging import Logger
from sqlalchemy import URL, Engine, create_engine, text

from src.config.settings import Config


def _build_oracle_url(
    host: str,
    port: int,
    service_name: str,
    username: str,
    password: str,
    drivername: str = "oracle+oracledb",
) -> URL:
    """Build a database URL without exposing credentials in string formatting."""
    if drivername.startswith("postgresql"):
        return URL.create(
            drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=service_name,
        )
    return URL.create(
        drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        query={"service_name": service_name},
    )


def create_bronze_engine(cfg: Config) -> Engine:
    """Create the SQLAlchemy engine for the BRONZE source database."""
    driver = getattr(cfg, "bronze_driver", "oracle+oracledb")
    connect_args = {} if driver.startswith("postgresql") else {"tcp_connect_timeout": 30}
    return create_engine(
        _build_oracle_url(
            cfg.bronze_host,
            cfg.bronze_port,
            cfg.bronze_service_name,
            cfg.bronze_username,
            cfg.bronze_password,
            drivername=driver,
        ),
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_pocc_engine(cfg: Config) -> Engine:
    """Create the SQLAlchemy engine for the POCC target database."""
    driver = getattr(cfg, "pocc_driver", "oracle+oracledb")
    connect_args = {} if driver.startswith("postgresql") else {"tcp_connect_timeout": 30}
    return create_engine(
        _build_oracle_url(
            cfg.pocc_host,
            cfg.pocc_port,
            cfg.pocc_service_name,
            cfg.pocc_username,
            cfg.pocc_password,
            drivername=driver,
        ),
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def test_oracle_connection(
    engine: Engine, logger: Logger, name: str = "Oracle"
) -> bool:
    """Return True when the database accepts a simple query."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM DUAL"))
        logger.info("%s connection: SUCCESS", name)
        return True
    except Exception:
        logger.exception("%s connection: FAILED", name)
        return False

