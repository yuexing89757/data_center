import pytest

from market_data_center.database_urls import psycopg_url, sqlalchemy_url


@pytest.mark.parametrize(
    "source",
    [
        "postgres://worker:secret@db.example:5432/market",
        "postgresql://worker:secret@db.example:5432/market",
        "postgresql+psycopg://worker:secret@db.example:5432/market",
    ],
)
def test_database_url_is_normalized_for_each_client(source: str) -> None:
    assert psycopg_url(source).startswith("postgresql://")
    assert sqlalchemy_url(source).startswith("postgresql+psycopg://")
    assert psycopg_url(source).endswith("/market")


def test_database_url_rejects_non_postgresql_scheme() -> None:
    with pytest.raises(ValueError, match="PostgreSQL"):
        psycopg_url("https://db.example/market")
