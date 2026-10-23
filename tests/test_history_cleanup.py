from datetime import date

from market_data_center.history_retention import POLICIES, month_cutoff


def test_round_robin_error_isolation_and_total_budget(tmp_path):
    from uuid import uuid4

    from market_data_center.history_cleanup_service import HistoryCleanupService
    from market_data_center.quality_cleanup_service import CleanupBudget

    calls = []

    class Repo:
        def clean_one(self, policy, cutoff, run_id, root, checkpoint, *, row_budget, after):
            calls.append(policy.table)
            if policy == POLICIES[0]:
                raise ValueError("history_group_bound")
            if calls.count(policy.table) > 1:
                return {"deleted_rows": 0}
            return {"deleted_rows": 2, "tables": {policy.table: 2}, "cursor": "last"}

    result = HistoryCleanupService(Repo(), tmp_path).run(
        date(2026, 10, 23), uuid4(), CleanupBudget(float("inf"))
    )
    assert calls[: len(POLICIES)] == [p.table for p in POLICIES]
    assert result.deleted_rows == 2 * (len(POLICIES) - 1)
    assert result.errors and result.partial
    assert result.tables[POLICIES[1].table] == 2


def test_calendar_month_and_explicit_scope():
    assert month_cutoff(date(2026, 10, 23)) == date(2026, 9, 23)
    assert month_cutoff(date(2024, 3, 31)) == date(2024, 2, 29)
    assert month_cutoff(date(2026, 1, 31)) == date(2025, 12, 31)
    tables = {t for p in POLICIES for t, _ in (*p.children, (p.table, ""))}
    assert {
        "realtime.auction_collection_session",
        "stock_pool.member",
        "regulation.warning",
        "core.stock_technical_factor_snapshot",
        "billboard.seat_trade",
    } <= tables
    assert not tables & {
        "core.daily_bar",
        "ingestion.raw_manifest",
        "audit.quality_result",
        "realtime.call_auction_market_series_snapshot_history",
    }


def test_parents_and_shared_quality_are_protected():
    policies = {p.table: p for p in POLICIES}
    assert "finished_at is not null" in policies["realtime.auction_collection_session"].predicate
    assert "snapshot_history" in policies["realtime.call_auction_market_series_session"].predicate
    assert "pool_snapshot_id" in policies["stock_pool.snapshot"].predicate
    assert "not exists" in policies["stock_pool.calculation_quality"].predicate
    assert "monitor_context" in policies["regulation.status"].predicate


def test_history_partial_is_recorded_in_operations():
    from market_data_center.domain.operations import ExecutionStatus
    from market_data_center.history_cleanup_service import HistoryCleanupResult
    from market_data_center.operations_service import _result_statistics

    result = HistoryCleanupResult(12, 1, errors=("history_group_bound",))
    assert _result_statistics(result) == (12, 12, 0, ExecutionStatus.PARTIAL)
