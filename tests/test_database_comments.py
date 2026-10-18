"""Reviewable comment-only migration and complete application metadata coverage."""

import re

import psycopg
import pytest
from test_postgres_integration import (  # noqa: F401
    empty_database_url,
    migrated_database_url,
)

from market_data_center.migrations import MIGRATION_DIR
from market_data_center.recovery import APPLICATION_SCHEMAS

MIGRATION = MIGRATION_DIR / "20261018000100_add_table_column_comments.sql"
COMMENT = re.compile(r"comment on (table|column) ([a-z_0-9.]+) is '((?:[^']|'')*)';", re.IGNORECASE)


def test_dictionary_is_comment_only_and_keeps_existing_comments() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    statements, partition_block = migration.split("do $comments$", maxsplit=1)
    stripped = re.sub(r"--[^\n]*", "", statements)
    assert not COMMENT.sub("", stripped).strip()
    targets = [match[1] for match in COMMENT.findall(statements)]
    assert len(targets) == len(set(targets))
    assert len(targets) > 1300
    assert all(description.strip() for _, _, description in COMMENT.findall(statements))

    prior_targets = {
        target
        for path in MIGRATION_DIR.glob("*.sql")
        if path.name < MIGRATION.name
        for target in re.findall(
            r"comment on (?:table|column)\s+([\w.]+)\s+is",
            path.read_text(encoding="utf-8"),
            re.IGNORECASE,
        )
    }
    assert not set(targets) & prior_targets
    assert "obj_description(partition_record.oid, 'pg_class') is null" in partition_block
    assert "col_description(partition_record.oid, child_column.attnum) is null" in (partition_block)
    assert not re.search(
        r"\b(insert|update|delete|truncate|alter|grant|revoke|create|drop)\b",
        re.sub(r"--[^\n]*", "", partition_block),
        re.IGNORECASE,
    )


def test_dictionary_distinguishes_units_and_historical_semantics() -> None:
    comments = {
        target: description
        for _, target, description in COMMENT.findall(MIGRATION.read_text(encoding="utf-8"))
    }
    assert "股" in comments["core.daily_bar.volume"]
    assert "张" in comments["convertible_bond.daily_bar.volume"]
    assert "点位" in comments["core.board_index_daily_bar.close"]
    assert "报告期截止" in comments["core.top10_float_holder.end_date"]
    assert "同日未复权收盘价" in comments["today_limit_up.member.free_float_market_cap_cny"]
    assert "百分点" in comments["core.stock_daily_indicator.free_float_turnover_rate_pct"]
    assert "小数比例" in comments["derived.daily_metric.total_return_1d"]
    for table in (
        "realtime.call_auction_market_series_snapshot",
        "realtime.call_auction_market_series_snapshot_history",
    ):
        assert "auction_indicative" in comments[f"{table}.last_price"]
        assert "指示" in comments[f"{table}.cumulative_volume"]
    for schema in ("today_limit_up", "today_limit_down"):
        direction = "up" if schema.endswith("up") else "down"
        assert (
            "不由首末封板时间推算"
            in comments[f"{schema}.member.limit_{direction}_duration_seconds"]
        )
    assert "JSON null" in comments["core.stock_technical_factor_snapshot.unadjusted"]


@pytest.mark.integration
def test_all_application_tables_and_columns_have_comments(
    migrated_database_url: str,  # noqa: F811 - imported pytest fixture
) -> None:
    """Run against the real post-migration catalog, including inherited partitions."""
    with psycopg.connect(migrated_database_url) as connection:
        missing_tables = connection.execute(
            """
            select namespace.nspname, relation.relname
            from pg_catalog.pg_class relation
            join pg_catalog.pg_namespace namespace on namespace.oid = relation.relnamespace
            where namespace.nspname = any(%s) and relation.relkind in ('r', 'p')
              and nullif(btrim(obj_description(relation.oid, 'pg_class')), '') is null
            order by 1, 2
            """,
            (list(APPLICATION_SCHEMAS),),
        ).fetchall()
        missing_columns = connection.execute(
            """
            select namespace.nspname, relation.relname, attribute.attname
            from pg_catalog.pg_class relation
            join pg_catalog.pg_namespace namespace on namespace.oid = relation.relnamespace
            join pg_catalog.pg_attribute attribute on attribute.attrelid = relation.oid
            where namespace.nspname = any(%s) and relation.relkind in ('r', 'p')
              and attribute.attnum > 0 and not attribute.attisdropped
              and nullif(btrim(col_description(relation.oid, attribute.attnum)), '') is null
            order by 1, 2, 3
            """,
            (list(APPLICATION_SCHEMAS),),
        ).fetchall()
    assert missing_tables == []
    assert missing_columns == []
