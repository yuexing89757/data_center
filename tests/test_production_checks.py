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
