"""Normalize PostgreSQL URLs at library boundaries without exposing credentials."""

from urllib.parse import urlsplit, urlunsplit


def psycopg_url(database_url: str) -> str:
    """Return a URL accepted by psycopg and PostgreSQL command helpers."""
    return _replace_scheme(database_url, "postgresql")


def sqlalchemy_url(database_url: str) -> str:
    """Select the installed psycopg v3 SQLAlchemy driver explicitly."""
    return _replace_scheme(database_url, "postgresql+psycopg")


def _replace_scheme(database_url: str, target_scheme: str) -> str:
    parsed = urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise ValueError("database URL must use a PostgreSQL scheme")
    if not parsed.netloc or not parsed.path.removeprefix("/"):
        raise ValueError("database URL must include a host and database name")
    return urlunsplit((target_scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment))
