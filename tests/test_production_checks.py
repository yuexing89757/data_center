import json
import os
import re
import subprocess
import sys
from pathlib import Path
from runpy import run_path
from typing import Any, cast

import pytest

from market_data_center.migrations import MIGRATION_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "apply_migrations.py"))
SMOKE_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "smoke_check.py"))
FASTAPI_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "check_fastapi_release.py"))
EXPECTED_TABLES = cast(set[tuple[str, str]], MIGRATION_CHECKS["EXPECTED_TABLES"])
EXPECTED_VIEWS = cast(set[tuple[str, str]], MIGRATION_CHECKS["EXPECTED_VIEWS"])
API_VIEWS = cast(tuple[str, ...], SMOKE_CHECKS["API_VIEWS"])
BASE_REQUIRED_METRICS = cast(set[str], SMOKE_CHECKS["BASE_REQUIRED_METRICS"])
BOARD_REQUIRED_METRICS = cast(set[str], SMOKE_CHECKS["BOARD_REQUIRED_METRICS"])
VIEW_COUNT = cast(Any, SMOKE_CHECKS["_view_count"])
PUBLISHED_FUNCTIONS = cast(tuple[str, ...], FASTAPI_CHECKS["PUBLISHED_FUNCTIONS"])


def test_regulation_migration_is_private_typed_and_has_26_official_rules() -> None:
    migration = (
        (MIGRATION_DIR / "20260902000100_create_regulation.sql").read_text(encoding="utf-8").lower()
    )

    assert "create schema if not exists regulation" in migration
    for table in ("rule", "event", "calculation_run", "status", "rule_result", "warning"):
        assert f"create table regulation.{table}" in migration
        assert f"alter table regulation.{table} enable row level security" in migration
    assert migration.count("-- official-rule:") == 26
    assert migration.count("'sse_main'") >= 9
    assert migration.count("'szse_main'") >= 9
    assert migration.count("'gem'") >= 8
    assert "cn-a-share-regulation-2026-07-06.v1" in migration
    assert "date '2026-07-06'" in migration
    assert "c_20260424_10816492.shtml" in migration
    assert "w020260424690713155663.pdf" in migration
    assert "exclude using gist" in migration
    assert "regulation_rule_kind_parameters_check" in migration
    assert "regulation_warning_scenario_reachability_check" in migration
    assert "grant select on regulation.rule to market_data_worker" in migration
    assert "grant select, insert on regulation.event to market_data_worker" in migration
    assert not re.search(
        r"(?im)^grant\s+.*\bon\s+regulation\..*\bto\s+(public|anon|authenticated|market_data_api)",
        migration,
    )
    assert all(token not in migration for token in ("ts_code", "'high'", "'medium'", "'low'"))
    assert "jsonb" not in migration


def test_dragon_tiger_replacement_migration_is_bounded_private_and_read_only() -> None:
    migration = (
        (MIGRATION_DIR / "20260902000300_replace_trading_billboard_with_dragon_tiger.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "drop table if exists billboard.entry" in migration
    assert "drop table if exists billboard.seat" in migration
    assert "create table billboard.dragon_tiger_event" in migration
    assert "create table billboard.seat_trade" in migration
    assert "create table billboard.trading_seat" in migration
    assert "'dragon_tiger'" in migration
    assert "'dragon_tiger_daily'" in migration
    assert "enable row level security" in migration
    assert "content_hash ~ '^[0-9a-f]{64}$'" in migration
    assert (
        "foreign key (\n        event_id, source_code, source_event_id, symbol, trade_date"
        in migration
    )
    assert migration.count("drop function if exists api_v1.query_trading_billboard") == 3
    assert "query_dragon_tiger_events_by_date" in migration
    assert "query_dragon_tiger_events_by_symbol" in migration
    assert "query_dragon_tiger_trades_by_seat" in migration
    assert "query_dragon_tiger_event_metrics" in migration
    assert migration.count("security definer") == 5
    assert migration.count("set statement_timeout = '5s'") == 5
    assert migration.count("using errcode = '22023'") >= 4
    assert "p_limit > 500" in migration
    assert "p_offset > 10000" in migration
    assert "p_end_date - p_start_date > 365" in migration
    assert "from public, anon, authenticated" in migration
    assert "to market_data_api" in migration
    assert "grant select, insert, update, delete on billboard.dragon_tiger_event" in migration
    assert "grant select, insert, update, delete on billboard.seat_trade" in migration
    assert "overall_score" not in migration
    assert all(token not in migration for token in ("schtasks", "crontab", "oncalendar"))


def test_auction_series_five_level_migration_is_bounded_and_preserves_history() -> None:
    migration = (
        (MIGRATION_DIR / "20260818000100_enrich_call_auction_market_series.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "update realtime.call_auction_market_series_snapshot" in migration
    update_body = migration.split(
        "update realtime.call_auction_market_series_snapshot", maxsplit=1
    )[1].split("alter table", maxsplit=1)[0]
    assert "set batch_code" in update_body
    assert "bid1_price" not in update_body
    assert "ask5_volume" not in update_body
    assert "security definer" in migration
    assert "set search_path = pg_catalog, api_v1, realtime, core" in migration
    assert "set statement_timeout = '5s'" in migration
    assert "from public, anon, authenticated" in migration
    assert "to market_data_api" in migration
    assert all(token not in migration for token in ("schtasks", "crontab", "oncalendar"))


def test_auction_series_rpc_qualifies_cte_payload_against_plpgsql_variable() -> None:
    signature = "query_call_auction_market_series_snapshots("
    defining_migrations = [
        path
        for path in sorted(MIGRATION_DIR.glob("*.sql"))
        if signature in path.read_text(encoding="utf-8")
    ]

    latest_definition = defining_migrations[-1].read_text(encoding="utf-8").lower()
    assert (
        "jsonb_agg(round_payloads.payload order by round_payloads.sample_seq)" in latest_definition
    )


def test_auction_series_batch_filter_migration_is_optional_bounded_and_private() -> None:
    migration = (
        (MIGRATION_DIR / "20260823000300_filter_call_auction_market_series_by_batch.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "p_batch_code text default null" in migration
    assert "p_batch_code !~ '^[0-9]{6}$'" in migration
    assert "round.scheduled_at at time zone 'asia/shanghai'" in migration
    assert "p_batch_code is null" in migration
    assert "set statement_timeout = '5s'" in migration
    assert "security definer" in migration
    assert "from public, anon, authenticated" in migration
    assert "to market_data_api" in migration


def test_realtime_auction_one_price_limit_decision_is_documented() -> None:
    adr = (PROJECT_ROOT / "docs/adr/ADR-0039-09点26沪深主板一字涨跌停实时计算.md").read_text(
        encoding="utf-8"
    )
    detail = (
        PROJECT_ROOT / "docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md"
    ).read_text(encoding="utf-8")
    for term in (
        "CN_MAINBOARD_2026_07_06",
        "realtime_read",
        "price_limit_calculation_id",
        "market_data_api",
        "09:25:30",
    ):
        assert term in adr + detail


def test_fastapi_preflight_checks_call_auction_market_snapshot_rpc() -> None:
    assert "api_v1.query_call_auction_market_snapshots(date,text[])" in PUBLISHED_FUNCTIONS
    assert (
        "api_v1.query_call_auction_market_series_snapshots(date,text[],text)" in PUBLISHED_FUNCTIONS
    )
    assert (
        "api_v1.query_call_auction_indicative_details(text,date,integer,integer)"
        in PUBLISHED_FUNCTIONS
    )
    assert "api_v1.query_board_index_bias_latest()" in PUBLISHED_FUNCTIONS
    assert "api_v1.query_close_price_new_highs_120d()" in PUBLISHED_FUNCTIONS
    assert not any("persist_board_index_daily_bars_live" in item for item in PUBLISHED_FUNCTIONS)
    assert "api_v1.query_latest_stock_daily_indicators(text[])" in PUBLISHED_FUNCTIONS
    assert "api_v1.query_latest_stock_quotes(text[],integer)" not in PUBLISHED_FUNCTIONS


def test_latest_stock_daily_indicator_rpc_is_private_and_bounded() -> None:
    migration = (
        (MIGRATION_DIR / "20260822000100_query_latest_stock_daily_indicators.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "create function api_v1.query_latest_stock_daily_indicators" in migration
    assert "cardinality(p_codes) > 500" in migration
    assert "set statement_timeout = '5s'" in migration
    assert "security definer" in migration
    assert "from public" in migration
    assert "from anon" in migration
    assert "from authenticated" in migration
    assert "to market_data_api" in migration


def test_shareholder_count_migration_is_append_only_bitemporal_and_bounded() -> None:
    migration = (MIGRATION_DIR / "20260824000100_create_shareholder_count.sql").read_text(
        encoding="utf-8"
    )
    normalized = migration.lower()

    assert "create table core.shareholder_count" in normalized
    assert "primary key (symbol, statistics_date, revision_key)" in normalized
    assert "statistics_date <= announcement_date" in normalized
    assert "shareholder_count > 0" in normalized
    assert "source_code = 'tushare'" in normalized
    assert "first_observed_at timestamptz not null default now()" in normalized
    assert "grant select, insert on core.shareholder_count to market_data_worker" in normalized
    assert "grant update" not in normalized
    assert "grant delete" not in normalized
    for function_name in (
        "query_shareholder_counts_as_of",
        "query_shareholder_count_history_as_of",
        "query_shareholder_count_history_latest",
    ):
        assert f"function api_v1.{function_name}" in normalized
    assert normalized.count("language plpgsql") == 3
    assert normalized.count("stable") >= 3
    assert normalized.count("security definer") == 3
    assert normalized.count("set statement_timeout = '5s'") == 3
    assert "errcode = '22023'" in normalized
    assert "cardinality(p_symbols) > 500" in normalized
    assert "greatest(1, least(coalesce(p_limit, 500), 2000))" in normalized
    assert "first_observed_at <" in normalized
    assert "at time zone 'asia/shanghai'" in normalized
    assert "row_number() over" in normalized
    assert "lag(" in normalized
    assert "return query" in normalized
    assert "from public" in normalized
    assert "to anon" in normalized
    assert "to authenticated" in normalized
    assert "to market_data_api" in normalized
    assert not re.search(r"(?im)^grant\s+(update|delete|all).*core\.shareholder_count", normalized)


def test_shareholder_count_date_constraint_tolerates_less_than_three_days() -> None:
    migration = (
        (MIGRATION_DIR / "20260826000100_tolerate_short_shareholder_count_date_inversion.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "drop constraint shareholder_count_date_order" in migration
    assert "add constraint shareholder_count_announcement_lag_check" in migration
    assert "announcement_date >= statistics_date - 2" in migration
    assert "validate constraint shareholder_count_announcement_lag_check" in migration


def test_fastapi_preflight_and_docs_publish_realtime_auction_limits() -> None:
    assert "api_v1.query_auction_one_price_limits(date)" in PUBLISHED_FUNCTIONS


def test_fastapi_preflight_publishes_auction_one_price_patterns() -> None:
    assert "api_v1.query_call_auction_one_price_patterns(date)" in PUBLISHED_FUNCTIONS
    documentation = "\n".join(
        (
            (PROJECT_ROOT / "docs/FastAPI外部接口.md").read_text(encoding="utf-8"),
            (PROJECT_ROOT / "docs/数据库导航.md").read_text(encoding="utf-8"),
        )
    )
    for term in (
        "realtime_read",
        "CN_MAINBOARD_2026_07_06",
        "1.0.0",
        "price_limit_calculation_id=null",
        "09:25:30",
        "10%",
    ):
        assert term in documentation


def _migration_sql() -> str:
    return "\n".join(
        migration.read_text(encoding="utf-8") for migration in sorted(MIGRATION_DIR.glob("*.sql"))
    )


def test_realtime_auction_limit_migration_is_read_only_and_independent() -> None:
    migration = (MIGRATION_DIR / "20260817000100_realtime_auction_one_price_limits.sql").read_text(
        encoding="utf-8"
    )
    normalized = migration.lower()
    assert "create or replace function api_v1.query_auction_one_price_limits" in normalized
    assert "derived.daily_price_limit" not in normalized
    assert "cn_a_mainboard_price_limit_pools" not in normalized
    assert "market_data_api" in normalized
    assert "realtime_read" in normalized
    assert "row_number() over" in normalized
    assert "prior_bar_counts" in normalized
    assert "abs(raw_upper_limit - previous_close)" in normalized
    assert "previous_close + 0.01::numeric" in normalized
    assert "greatest(" in normalized
    assert "security.code ~ '^[0-9]{6}$'" in normalized
    assert not re.search(r"(?im)^\s*(insert|update|delete)\s", migration)


def test_auction_snapshot_schedule_and_seal_rule_migration_is_versioned() -> None:
    migration = (
        MIGRATION_DIR / "20260819000100_adjust_call_auction_market_snapshot_schedule.sql"
    ).read_text(encoding="utf-8")
    normalized = migration.lower()
    assert "drop constraint call_auction_market_seal_amount_rule" in normalized
    assert "date '2026-08-20'" in normalized
    for level in (1, 2, 3):
        assert f"(ask{level}_volume is null or ask{level}_volume = 0)" in normalized
    assert "time '09:25:30'" in normalized
    assert "'snapshot_window', '09:25:30-09:29:59 asia/shanghai'" in normalized
    assert "create or replace function api_v1.query_auction_one_price_limits" in normalized
    assert "market_data_api" in normalized
    assert not re.search(r"(?im)^\s*(insert|update|delete)\s", migration)


def test_auction_series_bid1_semantics_are_governed() -> None:
    adr = (PROJECT_ROOT / "docs/adr/ADR-0040-竞价序列买一价量额语义.md").read_text(encoding="utf-8")
    detail = (PROJECT_ROOT / "docs/领域详设-沪深全市场竞价序列买一价量额-2026-08-17.md").read_text(
        encoding="utf-8"
    )
    for token in (
        "scheduled_at < 09:25:00",
        "auction_indicative",
        "opening_trade",
        "legacy_source_quote",
        "bid1.price * bid1.volume",
    ):
        assert token in adr + detail


def test_auction_series_value_semantics_migration_is_bounded() -> None:
    sql = (MIGRATION_DIR / "20260817000200_add_auction_series_value_semantics.sql").read_text(
        encoding="utf-8"
    )
    normalized = sql.lower()
    assert "legacy_source_quote" in normalized
    assert "auction_indicative" in normalized
    assert "opening_trade" in normalized
    assert (
        "create or replace function api_v1.query_call_auction_market_series_snapshots" in normalized
    )
    assert "drop table" not in normalized


def test_production_schema_expectations_follow_all_migrations() -> None:
    sql = _migration_sql()
    tables = set(re.findall(r"(?im)^create table ([a-z0-9_]+)\.([a-z0-9_]+)", sql))
    tables -= set(re.findall(r"(?im)^drop table if exists ([a-z0-9_]+)\.([a-z0-9_]+)", sql))
    views = set(
        re.findall(
            r"(?im)^create (?:or replace )?view ([a-z0-9_]+)\.([a-z0-9_]+)",
            sql,
        )
    )

    assert tables == EXPECTED_TABLES
    assert views == EXPECTED_VIEWS
    assert set(API_VIEWS) == {view for schema, view in views if schema == "api_v1"}


def test_production_schema_inventory_includes_call_auction_market_snapshot() -> None:
    assert ("realtime", "call_auction_market_snapshot") in EXPECTED_TABLES


def test_production_schema_inventory_includes_partitioned_call_auction_series() -> None:
    assert {
        ("realtime", "call_auction_market_series_session"),
        ("realtime", "call_auction_market_series_round"),
        ("realtime", "call_auction_market_series_snapshot"),
        *(
            ("realtime", f"call_auction_market_series_snapshot_{month}")
            for month in range(202608, 202613)
        ),
        *(
            ("realtime", f"call_auction_market_series_snapshot_{month}")
            for month in range(202701, 202710)
        ),
    } <= EXPECTED_TABLES


def test_production_schema_inventory_includes_today_limit_up_domain() -> None:
    assert {
        ("today_limit_up", "source_observation"),
        ("today_limit_up", "snapshot"),
        ("today_limit_up", "member"),
        ("today_limit_up", "calculation_quality"),
    } <= EXPECTED_TABLES


def test_production_schema_inventory_includes_close_price_new_high_snapshots() -> None:
    assert {
        ("derived", "close_price_new_high_120d_snapshot"),
        ("derived", "close_price_new_high_120d_member"),
    } <= EXPECTED_TABLES


def test_smoke_required_metrics_cover_phase_one_and_board_index() -> None:
    assert {
        "security",
        "security_name_history",
        "trading_calendar",
        "daily_bar",
        "raw_manifest",
        "succeeded_runs",
    } == BASE_REQUIRED_METRICS
    assert {
        "board_index",
        "board_index_daily_bar",
        "board_index_constituent_snapshot",
    } == BOARD_REQUIRED_METRICS


def test_view_count_rejects_names_outside_the_manifest() -> None:
    try:
        VIEW_COUNT(None, "daily_bars; drop schema core")
    except ValueError as error:
        assert "unsupported api_v1 view" in str(error)
    else:
        raise AssertionError("unsafe view name was accepted")


def test_postgres_only_release_check_flags_are_accepted() -> None:
    environment = os.environ.copy()
    environment.pop("DATABASE_URL", None)
    environment.pop("MIGRATION_DATABASE_URL", None)

    migration = subprocess.run(
        [sys.executable, "scripts/apply_migrations.py", "check", "--postgres-only"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    smoke = subprocess.run(
        [sys.executable, "scripts/smoke_check.py", "--postgres-only"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert migration.returncode != 0
    assert migration.stderr.strip() == "MIGRATION_DATABASE_URL is required"
    assert smoke.returncode != 0
    assert smoke.stderr.strip() == "DATABASE_URL is required"


def test_linux_fastapi_unit_is_independent_and_loopback_only() -> None:
    unit = (PROJECT_ROOT / "deploy/linux/market-data-center-api.service").read_text(
        encoding="utf-8"
    )
    template = (PROJECT_ROOT / "deploy/linux/market-data-center-api.env.example").read_text(
        encoding="utf-8"
    )

    assert "User=market-data-api" in unit
    assert "WorkingDirectory=/home/project-api" in unit
    assert "Environment=PYTHONPATH=/home/project-api/src" in unit
    assert "python -m market_data_center.public_api" in unit
    assert "EnvironmentFile=/etc/market-data-center/api.env" in unit
    assert "market-data-center worker" not in unit
    assert "ProtectHome=read-only" in unit
    assert "FASTAPI_HOST=127.0.0.1" in template
    assert "FASTAPI_DATABASE_URL=" in template
    assert "DATABASE_URL=" not in template.replace("FASTAPI_DATABASE_URL=", "")
    assert "SUPABASE" not in template


def test_fastapi_reader_migration_has_only_the_published_contract() -> None:
    migration = (MIGRATION_DIR / "20260809000100_create_fastapi_reader_role.sql").read_text(
        encoding="utf-8"
    )

    assert "create role market_data_api nologin" in migration
    assert "query_securities" in migration
    assert "query_daily_bars" in migration
    assert "query_classification_members_as_of" in migration
    assert "market_data_worker" not in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_limit_up_api_migration_uses_exact_governed_inputs() -> None:
    migration = (MIGRATION_DIR / "20260810000100_add_fastapi_limit_up_pool.sql").read_text(
        encoding="utf-8"
    )

    assert "snapshot.basis_trade_date = p_trade_date" in migration
    assert "event.close * indicator.free_float_shares" in migration
    assert "core.security_name_history" in migration
    assert "name_history.effective_from <= selected.basis_trade_date" in migration
    assert "indicator.trade_date = selected.basis_trade_date" in migration
    assert "close is not null and free_float_shares is not null" in migration
    assert "circulating_market_value" not in migration
    assert not re.search(r"\b(real|double precision|float[48]?)\b", migration.lower())
    assert "count(*) filter (where name is null)" in migration
    assert "count(*) filter (where close is null)" in migration
    assert "count(*) filter (where free_float_shares is null)" in migration
    assert "'omitted_count', omitted_count" in migration
    assert "order by symbol\n        limit p_limit" in migration
    assert "grant execute on function api_v1.query_limit_up_pool" in migration
    assert "market_data_worker" not in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_daily_limit_up_list_fix_limits_snapshot_not_members() -> None:
    migration = (
        MIGRATION_DIR / "20260811000100_fix_daily_limit_up_list_latest_snapshot.sql"
    ).read_text(encoding="utf-8")

    latest_snapshot, member_query = migration.split("limit_up_members as", maxsplit=1)
    assert "order by s.version desc\n        limit 1" in latest_snapshot
    assert "join stock_pool.member" in member_query
    assert "limit least(greatest(p_limit, 1), 500)" in member_query
    assert "grant execute on function api_v1.query_daily_limit_up_list" in migration


def test_daily_limit_up_list_switches_to_bounded_today_limit_up_contract() -> None:
    migration = (
        MIGRATION_DIR / "20260812000100_switch_daily_limit_up_list_to_today_limit_up.sql"
    ).read_text(encoding="utf-8")

    assert "join today_limit_up.member" in migration
    assert "join today_limit_up.calculation_quality" in migration
    assert "p_version integer default null" in migration
    assert "p_offset integer default 0" in migration
    assert "p_limit integer default 200" in migration
    assert "offset p_offset" in migration
    assert "limit p_limit" in migration
    assert "to market_data_api" in migration
    assert "to anon" not in migration
    assert "to authenticated" not in migration
    assert "realtime." not in migration
    assert "core." not in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_daily_limit_up_list_execute_is_restricted_to_fastapi_role() -> None:
    migration = (
        MIGRATION_DIR / "20260814000100_restrict_daily_limit_up_list_execute.sql"
    ).read_text(encoding="utf-8")

    assert "from public" in migration
    assert "from anon" in migration
    assert "from authenticated" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+execute.*\bto\s+(anon|authenticated)\b", migration)
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


@pytest.mark.parametrize(
    "filename,function_name",
    [
        ("20260814000200_add_top_gainers_20d_api.sql", "query_top_gainers_20d"),
        (
            "20260815000300_fix_top_gainers_pytdx_unknown_status.sql",
            "query_top_gainers_20d",
        ),
        (
            "20260814000300_add_auction_one_price_limits_api.sql",
            "query_auction_one_price_limits",
        ),
        (
            "20260815000100_add_board_index_bias_api.sql",
            "query_board_index_bias_latest",
        ),
        (
            "20260815000200_board_index_bias_live_fallback.sql",
            "persist_board_index_daily_bars_live",
        ),
    ],
)
def test_new_ranked_market_api_rpcs_are_fastapi_only(filename: str, function_name: str) -> None:
    migration = (MIGRATION_DIR / filename).read_text(encoding="utf-8")
    assert function_name in migration
    assert "from public" in migration
    assert "to market_data_api" in migration
    assert "to anon" not in migration
    assert "to authenticated" not in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_top_gainers_accepts_pytdx_unknown_bars_but_not_suspended_bars() -> None:
    migration = (
        MIGRATION_DIR / "20260815000300_fix_top_gainers_pytdx_unknown_status.sql"
    ).read_text(encoding="utf-8")

    assert "b.trade_status in ('trading', 'unknown')" in migration
    assert "start_status in ('trading', 'unknown')" in migration
    assert "end_status in ('trading', 'unknown')" in migration
    assert "start_status not in ('trading', 'unknown')" in migration
    assert "end_status not in ('trading', 'unknown')" in migration
    assert "to market_data_api" in migration


def test_close_price_new_highs_rpc_is_strict_hushen_only_and_bounded() -> None:
    migration = (
        MIGRATION_DIR / "20260816000200_materialize_close_price_new_highs_120d.sql"
    ).read_text(encoding="utf-8")

    assert "query_close_price_new_highs_120d" in migration
    assert "close_price_new_high_120d_snapshot" in migration
    assert "close_price_new_high_120d_member" in migration
    assert "from public, anon, authenticated" in migration
    assert "to market_data_api" in migration
    assert "set statement_timeout = '10s'" in migration
    function_sql = migration.split(
        "create or replace function api_v1.query_close_price_new_highs_120d()", maxsplit=1
    )[1]
    assert "from derived.close_price_new_high_120d_snapshot" in function_sql
    assert "from derived.close_price_new_high_120d_member" in function_sql
    assert "core.daily_bar" not in function_sql


def test_auction_indicative_rpc_is_bounded_fastapi_only_and_not_a_trade_contract() -> None:
    migration = (
        MIGRATION_DIR / "20260814000400_create_call_auction_indicative_detail.sql"
    ).read_text(encoding="utf-8")
    assert "p_trade_date <> (now() at time zone 'Asia/Shanghai')::date" in migration
    assert "p_limit > 500" in migration
    assert "'is_exchange_trade_tick', false" in migration
    assert "'is_order_by_order', false" in migration
    assert "from public, anon, authenticated" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_live_auction_persistence_is_narrow_idempotent_and_not_direct_table_dml() -> None:
    migration = (
        (MIGRATION_DIR / "20260814000500_persist_live_auction_indicative.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "security definer" in migration
    assert "p_trade_date <> (now() at time zone 'asia/shanghai')::date" in migration
    assert "p_symbol !~ '^(sse|szse):[0-9]{6}$'" in migration
    assert "p_byte_size > 2000000" in migration
    assert "p_source_row_count >= 5000" in migration
    assert "pg_advisory_xact_lock" in migration
    assert "call_auction_indicative_input_unique unique (symbol,trade_date,input_hash)" in migration
    assert "revoke all on function api_v1.persist_call_auction_indicative_details" in migration
    assert "from public,anon,authenticated" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_live_board_index_persistence_is_narrow_and_fastapi_only() -> None:
    migration = (
        (MIGRATION_DIR / "20260815000200_board_index_bias_live_fallback.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "security definer" in migration
    assert "pg_advisory_xact_lock" in migration
    assert "p_input_hash !~ '^[0-9a-f]{64}$'" in migration
    assert "p_byte_size > 5000000" in migration
    assert "jsonb_array_length(p_records) > 600" in migration
    assert "from public,anon,authenticated" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_board_index_worker_migration_removes_api_write_and_stale_gate() -> None:
    migration = (
        (MIGRATION_DIR / "20260820000100_board_index_daily_bar_worker_schedule.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "'board_index_daily_bar'" in migration
    assert "ready_count < 34" in migration
    assert "expected_trade_date" not in migration
    assert "drop function api_v1.persist_board_index_daily_bars_live" in migration
    assert "from public, anon, authenticated, market_data_api" in migration
    assert "grant execute on function api_v1.query_board_index_bias_latest" in migration


def test_recent_daily_bars_rpc_is_bounded_and_fastapi_only() -> None:
    migration = (
        (MIGRATION_DIR / "20260818000600_add_recent_daily_bars_api.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "query_recent_daily_bars" in migration
    assert "p_code !~ '^[0-9]{6}$'" in migration
    assert "p_limit > 5000" in migration
    assert "bar.trade_date <= p_trade_date" in migration
    assert "order by bar.trade_date desc" in migration
    assert "from public" in migration
    assert "from anon" in migration
    assert "from authenticated" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_daily_limit_up_list_quality_fix_uses_calculation_quality_table() -> None:
    """Regression guard: the quality CTE must read today_limit_up.calculation_quality,
    not the non-existent today_limit_up.member_quality that caused HTTP 503."""
    migration = (
        MIGRATION_DIR / "20260813000100_fix_daily_limit_up_list_quality_table.sql"
    ).read_text(encoding="utf-8")

    assert "from today_limit_up.calculation_quality" in migration
    # No executable reference to the non-existent table. The bug name may appear
    # in comments, so strip SQL comments before checking.
    stripped = "\n".join(
        line for line in migration.splitlines() if not line.lstrip().startswith("--")
    )
    assert "today_limit_up.member_quality" not in stripped
    # Preserves the (date, integer, integer, integer) signature and bounded grants.
    assert (
        "drop function if exists api_v1.query_daily_limit_up_list(date, integer, integer, integer)"
        in migration
    )
    assert "p_limit integer default 500" in migration
    assert "to market_data_api" in migration
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_snapshot_quality_dataset_codes_are_migrated_forward() -> None:
    migration = (MIGRATION_DIR / "20260811000200_allow_snapshot_quality_results.sql").read_text(
        encoding="utf-8"
    )

    assert "alter table audit.quality_result" in migration
    assert "'eod_quote_snapshot','call_auction_snapshot'" in migration


def test_operations_workflow_codes_are_migrated_as_one_controlled_catalog() -> None:
    migration = (
        MIGRATION_DIR / "20260811000300_constrain_operations_workflow_codes.sql"
    ).read_text(encoding="utf-8")
    controlled_codes = {
        "daily_market",
        "stock_daily_indicator",
        "stale_run_recovery",
        "deducted_profit",
        "stock_pool",
        "auction_collection",
        "eod_quote_snapshot",
        "call_auction_snapshot",
        "pytdx_pool_refresh",
    }

    assert "add constraint workflow_run_workflow_code_check" in migration
    assert "validate constraint workflow_run_workflow_code_check" in migration
    assert all(f"'{code}'" in migration for code in controlled_codes)
    assert not re.search(r"(?im)^grant\s+", migration)


def _write_runtime_pool(path: Path, *, szse: bool) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "pytdx.endpoint_pool.v1",
                "refreshed_at": "2026-08-11T10:00:00+08:00",
                "nodes": [
                    {
                        "host": "node.example",
                        "port": 7709,
                        "latency_ms": 10,
                        "capabilities": {
                            "quote": True,
                            "daily_bar_sse": True,
                            "daily_bar_szse": szse,
                            "daily_bar_bse": False,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_pytdx_pool_check_script_reports_capability_counts(tmp_path: Path) -> None:
    pool = tmp_path / "pytdx_pool.json"
    _write_runtime_pool(pool, szse=True)
    environment = os.environ.copy()
    environment["PYTDX_POOL_PATH"] = str(pool)

    result = subprocess.run(
        [sys.executable, "scripts/check_pytdx_pool.py"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert json.loads(result.stdout) == {
        "daily_bar_bse": 0,
        "daily_bar_sse": 1,
        "daily_bar_szse": 1,
        "quote": 1,
    }


def test_pytdx_pool_check_script_rejects_incomplete_pool(tmp_path: Path) -> None:
    pool = tmp_path / "pytdx_pool.json"
    _write_runtime_pool(pool, szse=False)
    environment = os.environ.copy()
    environment["PYTDX_POOL_PATH"] = str(pool)

    result = subprocess.run(
        [sys.executable, "scripts/check_pytdx_pool.py"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert result.stderr.strip() == "pytdx endpoint pool is missing required capabilities"


def test_linux_worker_uses_the_shared_pool_runtime_contract() -> None:
    unit = (PROJECT_ROOT / "deploy/linux/market-data-center-worker.service").read_text(
        encoding="utf-8"
    )
    template = (PROJECT_ROOT / "deploy/linux/market-data-center.env.example").read_text(
        encoding="utf-8"
    )
    smoke = (PROJECT_ROOT / "deploy/linux/smoke-check.sh").read_text(encoding="utf-8")

    assert "check_pytdx_" + "daily_bar_endpoints.py" not in unit
    assert "PYTDX_POOL_PATH=/var/lib/market-data-center/pytdx_pool.json" in template
    assert "AUCTION_COLLECTION_ENABLED" not in template
    assert "PYSNOWBALL_TOKEN" not in template
    assert "EOD_QUOTE_SNAPSHOT_ENABLED=true" in template
    assert "CALL_AUCTION_SNAPSHOT_ENABLED=true" in template
    assert "CALL_AUCTION_MARKET_SERIES_ENABLED=true" in template
    assert "DATA_CLEANUP_ENABLED=true" in template
    assert "scripts/check_pytdx_pool.py" in smoke
    assert "OnCalendar" not in unit


def test_release_templates_expose_task_switches_but_not_task_times() -> None:
    templates = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            PROJECT_ROOT / ".env.example",
            PROJECT_ROOT / "deploy/linux/market-data-center.env.example",
        )
    )
    switches = (
        "EOD_QUOTE_SNAPSHOT_ENABLED=true",
        "CALL_AUCTION_SNAPSHOT_ENABLED=true",
        "CALL_AUCTION_MARKET_SERIES_ENABLED=true",
        "DATA_CLEANUP_ENABLED=true",
    )
    forbidden = (
        "SCHEDULER_TIMEZONE",
        "DAILY_RUN_HOUR",
        "DAILY_RUN_MINUTE",
        "STOCK_DAILY_INDICATOR_HOUR",
        "STOCK_DAILY_INDICATOR_MINUTE",
        "STOCK_POOL_HOUR",
        "STOCK_POOL_MINUTE",
        "DEDUCTED_PROFIT_HOUR",
        "DEDUCTED_PROFIT_MINUTE",
        "SCHEDULER_MISFIRE_GRACE_SECONDS",
        "AUCTION_COLLECTION_HOUR",
        "AUCTION_COLLECTION_MINUTE",
        "AUCTION_COLLECTION_CADENCE_SECONDS",
        "EOD_QUOTE_HOUR",
        "EOD_QUOTE_MINUTE",
        "CALL_AUCTION_HOUR",
        "CALL_AUCTION_MINUTE",
        "CALL_AUCTION_MARKET_SERIES_HOUR",
        "CALL_AUCTION_MARKET_SERIES_MINUTE",
        "CALL_AUCTION_MARKET_SERIES_CADENCE_SECONDS",
        "CALL_AUCTION_MARKET_SERIES_BATCH_SIZE",
        "PYTDX_POOL_REFRESH_HOURS",
        "DATA_CLEANUP_HOUR",
        "DATA_CLEANUP_MINUTE",
        "DATA_CLEANUP_RETAINED_DAYS",
        "DATA_CLEANUP_TABLE",
    )

    assert all(templates.count(switch) == 2 for switch in switches)
    assert all(name not in templates for name in forbidden)


def test_active_release_files_do_not_reference_legacy_pytdx_settings() -> None:
    legacy_settings = (
        "PYTDX_DAILY_BAR_" + "ENDPOINTS",
        "PYTDX_DAILY_BAR_" + "POOL_PATH",
        "PYTDX_HQ_" + "HOST",
        "PYTDX_HQ_" + "PORT",
        "PYTDX_HQ_" + "POOL_PATH",
    )
    files = [
        *PROJECT_ROOT.joinpath("src").rglob("*.py"),
        *PROJECT_ROOT.joinpath("scripts").rglob("*.py"),
        *PROJECT_ROOT.joinpath("deploy").rglob("*"),
        PROJECT_ROOT / ".env.example",
        PROJECT_ROOT / "deploy.ps1",
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "INSTALL-WINDOWS.md",
        PROJECT_ROOT / "docs/Worker日常采集与调度.md",
        PROJECT_ROOT / "docs/Worker调度系统.md",
        PROJECT_ROOT / "docs/最小生产发布运行手册.md",
        PROJECT_ROOT / "docs/集合竞价五档采集运行手册.md",
        PROJECT_ROOT / "docs/领域详设-RemoteTdxDailyBar-2026-08-09.md",
    ]
    release_text = "\n".join(path.read_text(encoding="utf-8") for path in files if path.is_file())

    assert all(setting not in release_text for setting in legacy_settings)


def test_dragon_tiger_rpcs_reject_null_pagination_bounds() -> None:
    migration = (
        PROJECT_ROOT
        / "supabase/migrations/20260902000300_replace_trading_billboard_with_dragon_tiger.sql"
    ).read_text(encoding="utf-8")

    assert migration.count("p_limit is null") == 3
    assert migration.count("p_offset is null") == 3


def test_auction_series_cleanup_migration_is_narrow_and_non_destructive() -> None:
    migration_path = (
        PROJECT_ROOT
        / "supabase/migrations/20260903000100_add_auction_series_retention_cleanup.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    controlled_workflows = {
        "daily_market",
        "stock_daily_indicator",
        "stale_run_recovery",
        "deducted_profit",
        "shareholder_count_daily",
        "shareholder_count_backfill",
        "stock_pool",
        "auction_collection",
        "eod_quote_snapshot",
        "call_auction_snapshot",
        "call_auction_market_snapshot",
        "call_auction_market_series",
        "pytdx_pool_refresh",
        "today_limit_up_snapshot",
        "close_price_new_highs_120d",
        "board_index_daily_bar",
        "trading_billboard_daily",
        "dragon_tiger_daily",
        "regulation_daily_calculation",
        "data_cleanup",
    }

    assert migration_path == max(MIGRATION_DIR.glob("*.sql"))
    assert all(f"'{workflow}'" in migration for workflow in controlled_workflows)
    normalized = " ".join(migration.split())
    assert "for delete to market_data_worker using (true)" in normalized
    assert (
        "grant delete on realtime.call_auction_market_series_snapshot "
        "to market_data_worker"
    ) in normalized
    assert "delete from" not in migration
    assert all(
        token not in migration
        for token in (
            "truncate",
            "vacuum",
            "drop table",
            "detach partition",
            "call_auction_market_series_session",
            "call_auction_market_series_round",
            "realtime.call_auction_market_snapshot ",
            "ingestion.raw_manifest",
            "audit.quality_result",
        )
    )
