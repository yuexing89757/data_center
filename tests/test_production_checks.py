import os
import re
import subprocess
import sys
from pathlib import Path
from runpy import run_path
from typing import Any, cast

from market_data_center.migrations import MIGRATION_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "apply_migrations.py"))
SMOKE_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "smoke_check.py"))
EXPECTED_TABLES = cast(set[tuple[str, str]], MIGRATION_CHECKS["EXPECTED_TABLES"])
EXPECTED_VIEWS = cast(set[tuple[str, str]], MIGRATION_CHECKS["EXPECTED_VIEWS"])
API_VIEWS = cast(tuple[str, ...], SMOKE_CHECKS["API_VIEWS"])
BASE_REQUIRED_METRICS = cast(set[str], SMOKE_CHECKS["BASE_REQUIRED_METRICS"])
BOARD_REQUIRED_METRICS = cast(set[str], SMOKE_CHECKS["BOARD_REQUIRED_METRICS"])
VIEW_COUNT = cast(Any, SMOKE_CHECKS["_view_count"])


def _migration_sql() -> str:
    return "\n".join(
        migration.read_text(encoding="utf-8") for migration in sorted(MIGRATION_DIR.glob("*.sql"))
    )


def test_production_schema_expectations_follow_all_migrations() -> None:
    sql = _migration_sql()
    tables = set(re.findall(r"(?im)^create table ([a-z0-9_]+)\.([a-z0-9_]+)", sql))
    views = set(
        re.findall(
            r"(?im)^create (?:or replace )?view ([a-z0-9_]+)\.([a-z0-9_]+)",
            sql,
        )
    )

    assert tables == EXPECTED_TABLES
    assert views == EXPECTED_VIEWS
    assert set(API_VIEWS) == {view for schema, view in views if schema == "api_v1"}


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
    assert not re.search(r"(?im)^grant\s+(insert|update|delete|all)", migration)


def test_snapshot_quality_dataset_codes_are_migrated_forward() -> None:
    migration = (MIGRATION_DIR / "20260811000200_allow_snapshot_quality_results.sql").read_text(
        encoding="utf-8"
    )

    assert "alter table audit.quality_result" in migration
    assert "'eod_quote_snapshot','call_auction_snapshot'" in migration
