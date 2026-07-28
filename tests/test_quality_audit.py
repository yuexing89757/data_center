from dataclasses import replace
from datetime import UTC, date, datetime

from market_data_center.quality_audit import (
    CoverageSummary,
    DailyBarAuditReport,
    InvariantSummary,
    SourceCount,
    TraceabilitySummary,
    render_json,
    render_markdown,
)


def test_quality_audit_renderers_report_pass_without_binary_floats() -> None:
    report = _report()

    json_report = render_json(report)
    markdown_report = render_markdown(report)

    assert '"status": "PASS"' in json_report
    assert '"coverage_percent": "100.00"' in json_report
    assert "| 覆盖率 | 100.00% |" in markdown_report
    assert "| baostock | 3 |" in markdown_report


def test_quality_audit_status_distinguishes_warnings_and_errors() -> None:
    report = _report()
    warning = replace(
        report,
        coverage=replace(report.coverage, missing_eligible_symbol_days=1),
    )
    failed = replace(
        report,
        invariants=replace(report.invariants, duplicate_natural_keys=1),
    )
    empty = replace(
        report,
        total_rows=0,
        coverage=replace(
            report.coverage,
            stock_count=0,
            covered_stock_count=0,
            trading_day_count=0,
            all_stock_calendar_days=0,
            eligible_symbol_days=0,
            observed_eligible_rows=0,
        ),
    )

    assert '"status": "WARNING"' in render_json(warning)
    assert '"status": "FAIL"' in render_json(failed)
    assert '"status": "FAIL"' in render_json(empty)


def _report() -> DailyBarAuditReport:
    return DailyBarAuditReport(
        generated_at=datetime(2026, 7, 29, tzinfo=UTC),
        start_date=date(2026, 7, 27),
        end_date=date(2026, 7, 29),
        total_rows=3,
        distinct_symbols=1,
        first_trade_date=date(2026, 7, 27),
        last_trade_date=date(2026, 7, 29),
        coverage=CoverageSummary(
            stock_count=1,
            covered_stock_count=1,
            trading_day_count=3,
            all_stock_calendar_days=3,
            eligible_symbol_days=3,
            pre_ipo_symbol_days_excluded=0,
            post_delisting_symbol_days_excluded=0,
            observed_eligible_rows=3,
            suspended_rows=0,
            missing_eligible_symbol_days=0,
        ),
        invariants=InvariantSummary(0, 0, 0, 0, 0, 0),
        traceability=TraceabilitySummary(1, 1, 0, 0, 0, 0, 0),
        source_distribution=(SourceCount("baostock", 3),),
        gap_candidates=(),
    )
