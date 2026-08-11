import json
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
    assert "AUCTION_COLLECTION_ENABLED=true" in template
    assert "EOD_QUOTE_SNAPSHOT_ENABLED=true" in template
    assert "CALL_AUCTION_SNAPSHOT_ENABLED=true" in template
    assert "scripts/check_pytdx_pool.py" in smoke


def test_release_templates_expose_task_switches_but_not_task_times() -> None:
    templates = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            PROJECT_ROOT / ".env.example",
            PROJECT_ROOT / "deploy/linux/market-data-center.env.example",
        )
    )
    switches = (
        "AUCTION_COLLECTION_ENABLED=true",
        "EOD_QUOTE_SNAPSHOT_ENABLED=true",
        "CALL_AUCTION_SNAPSHOT_ENABLED=true",
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
        "PYTDX_POOL_REFRESH_HOURS",
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
