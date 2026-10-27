"""Singleton logger factory configured with Asia/Jakarta (WIB / UTC+7) timestamps."""

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

_logger_instance: Optional[logging.Logger] = None
_JAKARTA_TZ = ZoneInfo("Asia/Jakarta")


class JakartaFormatter(logging.Formatter):
    """Formatter that renders %(asctime)s in Asia/Jakarta (WIB / UTC+7)."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=_JAKARTA_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def get_logger() -> logging.Logger:
    """Return the singleton ETL logger with Asia/Jakarta timestamps."""
    global _logger_instance
    if _logger_instance is None:
        logger = logging.getLogger("etl")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                JakartaFormatter("%(asctime)s [%(levelname)s] %(message)s")
            )
            logger.addHandler(handler)

        _logger_instance = logger

    return _logger_instance

