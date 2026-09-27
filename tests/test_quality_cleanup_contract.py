from pathlib import Path

from market_data_center.cli import _parser
from market_data_center.recovery import COUNT_QUERIES


def test_quality_cleanup_migration_removes_broad_policy():
    sql = Path("supabase/migrations/20260927000100_restrict_quality_cleanup.sql").read_text()
    assert "drop policy quality_result_worker_all" in sql
    assert "quality_result_id = any" in sql
    assert "quality_archive" in sql
    assert "for delete to market_data_worker" in sql
    assert "aggregation_version" in sql


def test_cleanup_cli_defaults_to_read_only():
    args = _parser().parse_args(["data-cleanup"])
    assert not args.execute and not args.confirm


def test_archive_metadata_is_part_of_recovery_counts():
    assert COUNT_QUERIES["quality_archive"] == "select count(*) from audit.quality_archive"
    assert (
        COUNT_QUERIES["data_cleanup_report"]
        == "select count(*) from operations.data_cleanup_report"
    )
