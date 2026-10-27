"""SQL file loader utility."""

from pathlib import Path


def load_sql_file(path: str) -> str:
    """Read a SQL query from a file, stripping the trailing semicolon."""
    sql = Path(path).read_text(encoding="utf-8").strip()
    if not sql:
        raise ValueError(f"SQL file is empty: {path}")
    return sql.rstrip(";").strip()

