"""Read-only, reproducible Daily Bar quality audits."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from json import dumps

import psycopg


@dataclass(frozen=True, slots=True)
class SourceCount:
    source_code: str
    row_count: int


@dataclass(frozen=True, slots=True)
class GapCandidate:
    symbol: str
    current_name: str
    ipo_date: date | None
    delisting_date: date | None
    expected_rows: int
    observed_rows: int
    suspended_rows: int
    missing_rows: int
    first_missing_date: date
    last_missing_date: date


@dataclass(frozen=True, slots=True)
class CoverageSummary:
    stock_count: int
    covered_stock_count: int
    trading_day_count: int
    all_stock_calendar_days: int
    eligible_symbol_days: int
    pre_ipo_symbol_days_excluded: int
    post_delisting_symbol_days_excluded: int
    observed_eligible_rows: int
    suspended_rows: int
    missing_eligible_symbol_days: int

    @property
    def coverage_percent(self) -> Decimal:
        if self.eligible_symbol_days == 0:
            return Decimal("0.00")
        return (
            Decimal(self.observed_eligible_rows) * Decimal(100) / Decimal(self.eligible_symbol_days)
        ).quantize(Decimal("0.01"))


@dataclass(frozen=True, slots=True)
class InvariantSummary:
    duplicate_natural_keys: int
    invalid_ohlc_rows: int
    negative_value_rows: int
    non_trading_date_rows: int
    outside_listing_window_rows: int
    unknown_trade_status_rows: int

    @property
    def error_count(self) -> int:
        return sum(
            (
                self.duplicate_natural_keys,
                self.invalid_ohlc_rows,
                self.negative_value_rows,
                self.non_trading_date_rows,
                self.outside_listing_window_rows,
            )
        )


@dataclass(frozen=True, slots=True)
class TraceabilitySummary:
    ingestion_run_count: int
    raw_manifest_count: int
    orphan_ingestion_rows: int
    provider_mismatch_rows: int
    missing_raw_manifest_runs: int
    manifest_row_count_mismatch_runs: int
    failed_quality_findings: int

    @property
    def error_count(self) -> int:
        return sum(
            (
                self.orphan_ingestion_rows,
                self.provider_mismatch_rows,
                self.missing_raw_manifest_runs,
                self.manifest_row_count_mismatch_runs,
            )
        )


@dataclass(frozen=True, slots=True)
class DailyBarAuditReport:
    generated_at: datetime
    start_date: date
    end_date: date
    total_rows: int
    distinct_symbols: int
    first_trade_date: date | None
    last_trade_date: date | None
    coverage: CoverageSummary
    invariants: InvariantSummary
    traceability: TraceabilitySummary
    source_distribution: tuple[SourceCount, ...]
    gap_candidates: tuple[GapCandidate, ...]

    @property
    def has_errors(self) -> bool:
        return (
            self.total_rows == 0
            or self.coverage.stock_count == 0
            or self.coverage.trading_day_count == 0
            or self.invariants.error_count > 0
            or self.traceability.error_count > 0
        )

    @property
    def has_warnings(self) -> bool:
        return (
            self.coverage.covered_stock_count < self.coverage.stock_count
            or self.coverage.missing_eligible_symbol_days > 0
            or self.invariants.unknown_trade_status_rows > 0
        )


def audit_daily_bars(
    database_url: str,
    start_date: date,
    end_date: date,
    *,
    top_gap_limit: int = 20,
    generated_at: datetime | None = None,
) -> DailyBarAuditReport:
    """Audit persisted facts without mutating the database or exposing credentials."""
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    if top_gap_limit < 1:
        raise ValueError("top_gap_limit must be positive")

    parameters = (start_date, end_date)
    with psycopg.connect(
        database_url,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as connection:
        fact_row = _required_row(
            connection,
            """
            select
                count(*),
                count(distinct symbol),
                min(trade_date),
                max(trade_date)
            from core.daily_bar
            where trade_date between %s and %s
            """,
            parameters,
        )
        coverage = _coverage_summary(connection, start_date, end_date)
        invariants = _invariant_summary(connection, start_date, end_date)
        traceability = _traceability_summary(connection, start_date, end_date)
        source_distribution = _source_distribution(connection, start_date, end_date)
        gap_candidates = _gap_candidates(connection, start_date, end_date, top_gap_limit)

    return DailyBarAuditReport(
        generated_at=generated_at or datetime.now(UTC),
        start_date=start_date,
        end_date=end_date,
        total_rows=_integer(fact_row[0]),
        distinct_symbols=_integer(fact_row[1]),
        first_trade_date=_optional_date(fact_row[2]),
        last_trade_date=_optional_date(fact_row[3]),
        coverage=coverage,
        invariants=invariants,
        traceability=traceability,
        source_distribution=source_distribution,
        gap_candidates=gap_candidates,
    )


def render_json(report: DailyBarAuditReport) -> str:
    """Render stable, machine-readable audit output."""
    payload = {
        "generated_at": report.generated_at.isoformat(),
        "scope": {
            "start_date": report.start_date.isoformat(),
            "end_date": report.end_date.isoformat(),
        },
        "status": _status(report),
        "facts": {
            "total_rows": report.total_rows,
            "distinct_symbols": report.distinct_symbols,
            "first_trade_date": _iso_date(report.first_trade_date),
            "last_trade_date": _iso_date(report.last_trade_date),
        },
        "coverage": {
            "stock_count": report.coverage.stock_count,
            "covered_stock_count": report.coverage.covered_stock_count,
            "trading_day_count": report.coverage.trading_day_count,
            "all_stock_calendar_days": report.coverage.all_stock_calendar_days,
            "eligible_symbol_days": report.coverage.eligible_symbol_days,
            "pre_ipo_symbol_days_excluded": (report.coverage.pre_ipo_symbol_days_excluded),
            "post_delisting_symbol_days_excluded": (
                report.coverage.post_delisting_symbol_days_excluded
            ),
            "observed_eligible_rows": report.coverage.observed_eligible_rows,
            "suspended_rows": report.coverage.suspended_rows,
            "missing_eligible_symbol_days": (report.coverage.missing_eligible_symbol_days),
            "coverage_percent": str(report.coverage.coverage_percent),
        },
        "invariants": {
            "duplicate_natural_keys": report.invariants.duplicate_natural_keys,
            "invalid_ohlc_rows": report.invariants.invalid_ohlc_rows,
            "negative_value_rows": report.invariants.negative_value_rows,
            "non_trading_date_rows": report.invariants.non_trading_date_rows,
            "outside_listing_window_rows": (report.invariants.outside_listing_window_rows),
            "unknown_trade_status_rows": (report.invariants.unknown_trade_status_rows),
        },
        "traceability": {
            "ingestion_run_count": report.traceability.ingestion_run_count,
            "raw_manifest_count": report.traceability.raw_manifest_count,
            "orphan_ingestion_rows": report.traceability.orphan_ingestion_rows,
            "provider_mismatch_rows": report.traceability.provider_mismatch_rows,
            "missing_raw_manifest_runs": (report.traceability.missing_raw_manifest_runs),
            "manifest_row_count_mismatch_runs": (
                report.traceability.manifest_row_count_mismatch_runs
            ),
            "failed_quality_findings": report.traceability.failed_quality_findings,
        },
        "source_distribution": [
            {"source_code": item.source_code, "row_count": item.row_count}
            for item in report.source_distribution
        ],
        "gap_candidates": [
            {
                "symbol": gap.symbol,
                "current_name": gap.current_name,
                "ipo_date": _iso_date(gap.ipo_date),
                "delisting_date": _iso_date(gap.delisting_date),
                "expected_rows": gap.expected_rows,
                "observed_rows": gap.observed_rows,
                "suspended_rows": gap.suspended_rows,
                "missing_rows": gap.missing_rows,
                "first_missing_date": gap.first_missing_date.isoformat(),
                "last_missing_date": gap.last_missing_date.isoformat(),
            }
            for gap in report.gap_candidates
        ],
        "interpretation": [
            "Pre-IPO and post-delisting symbol-days are excluded from expected coverage.",
            "Explicit suspended rows count as observed facts.",
            "Missing eligible symbol-days require suspension or provider reconciliation.",
        ],
    }
    return dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def render_markdown(report: DailyBarAuditReport) -> str:
    """Render an archival human-readable audit report."""
    lines = [
        "# Daily Bar 数据质量验收报告",
        "",
        f"- 状态: **{_status(report)}**",
        f"- 范围: `{report.start_date}` - `{report.end_date}` (闭区间)",
        f"- 生成时间: `{report.generated_at.isoformat()}`",
        "",
        "## 事实概览",
        "",
        "| 指标 | 值 |",
        "| --- | ---: |",
        f"| 总行数 | {report.total_rows} |",
        f"| 有数据证券数 | {report.distinct_symbols} |",
        f"| 最早交易日 | {_display_date(report.first_trade_date)} |",
        f"| 最晚交易日 | {_display_date(report.last_trade_date)} |",
        "",
        "## 股票覆盖率",
        "",
        "| 指标 | 值 |",
        "| --- | ---: |",
        f"| 股票总数 | {report.coverage.stock_count} |",
        f"| 有数据股票数 | {report.coverage.covered_stock_count} |",
        f"| 交易日数 | {report.coverage.trading_day_count} |",
        f"| 上市区间内应有证券日 | {report.coverage.eligible_symbol_days} |",
        f"| 实际证券日 | {report.coverage.observed_eligible_rows} |",
        f"| 覆盖率 | {report.coverage.coverage_percent}% |",
        f"| 上市前排除 | {report.coverage.pre_ipo_symbol_days_excluded} |",
        f"| 退市后排除 | {report.coverage.post_delisting_symbol_days_excluded} |",
        f"| 明确停牌记录 | {report.coverage.suspended_rows} |",
        f"| 上市区间内待核对缺口 | {report.coverage.missing_eligible_symbol_days} |",
        "",
        "上市前和退市后的证券日不计入应覆盖范围; 明确的停牌记录仍作为事实计入覆盖。",
        "上市区间内缺失记录只标记为待核对; 必须结合停牌或来源数据确认; 不能静默视为正常。",
        "",
        "## 约束审计",
        "",
        "| 指标 | 值 |",
        "| --- | ---: |",
        f"| 自然键重复 | {report.invariants.duplicate_natural_keys} |",
        f"| OHLC 区间异常 | {report.invariants.invalid_ohlc_rows} |",
        f"| 价格/数量负值 | {report.invariants.negative_value_rows} |",
        f"| 非交易日记录 | {report.invariants.non_trading_date_rows} |",
        f"| 上市区间外记录 | {report.invariants.outside_listing_window_rows} |",
        f"| 未知交易状态 | {report.invariants.unknown_trade_status_rows} |",
        "",
        "## 采集追溯",
        "",
        "| 指标 | 值 |",
        "| --- | ---: |",
        f"| Ingestion Run | {report.traceability.ingestion_run_count} |",
        f"| Raw Manifest | {report.traceability.raw_manifest_count} |",
        f"| 缺失 Ingestion Run 的事实 | {report.traceability.orphan_ingestion_rows} |",
        f"| 事实来源与 Run Provider 不一致 | {report.traceability.provider_mismatch_rows} |",
        f"| 缺失 Raw Manifest 的 Run | {report.traceability.missing_raw_manifest_runs} |",
        "| Raw 行数与 fetched_rows 不一致的 Run "
        f"| {report.traceability.manifest_row_count_mismatch_runs} |",
        f"| 历史失败质量记录 | {report.traceability.failed_quality_findings} |",
        "",
        "## 来源分布",
        "",
        "| 来源 | 行数 |",
        "| --- | ---: |",
    ]
    lines.extend(
        f"| {item.source_code} | {item.row_count} |" for item in report.source_distribution
    )
    if not report.source_distribution:
        lines.append("| (无数据) | 0 |")

    lines.extend(
        [
            "",
            "## 待核对缺口",
            "",
            "| 证券 | 名称 | IPO | 退市 | 应有 | 实际 | 停牌 | 缺失 | 缺失范围 |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    lines.extend(
        "| "
        f"{gap.symbol} | {gap.current_name} | {_display_date(gap.ipo_date)} | "
        f"{_display_date(gap.delisting_date)} | {gap.expected_rows} | "
        f"{gap.observed_rows} | {gap.suspended_rows} | {gap.missing_rows} | "
        f"{gap.first_missing_date} - {gap.last_missing_date} |"
        for gap in report.gap_candidates
    )
    if not report.gap_candidates:
        lines.append("| (无) |  |  |  | 0 | 0 | 0 | 0 |  |")
    return "\n".join(lines) + "\n"


def _coverage_summary(
    connection: psycopg.Connection[tuple[object, ...]],
    start_date: date,
    end_date: date,
) -> CoverageSummary:
    row = _required_row(
        connection,
        """
        with stocks as (
            select symbol, ipo_date, delisting_date
            from core.security
            where security_type = 'stock'
        ),
        open_days as (
            select trade_date
            from core.trading_calendar
            where market = 'CN_A_SHARE'
              and is_trading_day
              and trade_date between %s and %s
        ),
        symbol_days as (
            select
                stocks.symbol,
                open_days.trade_date,
                stocks.ipo_date,
                stocks.delisting_date
            from stocks cross join open_days
        )
        select
            (select count(*) from stocks),
            (select count(*) from open_days),
            count(*),
            count(*) filter (
                where (ipo_date is null or trade_date >= ipo_date)
                  and (delisting_date is null or trade_date <= delisting_date)
            ),
            count(*) filter (where ipo_date is not null and trade_date < ipo_date),
            count(*) filter (
                where delisting_date is not null and trade_date > delisting_date
            )
        from symbol_days
        """,
        (start_date, end_date),
    )
    observed = _required_row(
        connection,
        """
        select
            count(*),
            count(distinct bar.symbol),
            count(*) filter (where bar.trade_status = 'suspended')
        from core.daily_bar bar
        join core.security security using (symbol)
        join core.trading_calendar calendar
          on calendar.market = bar.market
         and calendar.trade_date = bar.trade_date
         and calendar.is_trading_day
        where security.security_type = 'stock'
          and bar.trade_date between %s and %s
          and (security.ipo_date is null or bar.trade_date >= security.ipo_date)
          and (
              security.delisting_date is null
              or bar.trade_date <= security.delisting_date
          )
        """,
        (start_date, end_date),
    )
    eligible = _integer(row[3])
    observed_rows = _integer(observed[0])
    return CoverageSummary(
        stock_count=_integer(row[0]),
        covered_stock_count=_integer(observed[1]),
        trading_day_count=_integer(row[1]),
        all_stock_calendar_days=_integer(row[2]),
        eligible_symbol_days=eligible,
        pre_ipo_symbol_days_excluded=_integer(row[4]),
        post_delisting_symbol_days_excluded=_integer(row[5]),
        observed_eligible_rows=observed_rows,
        suspended_rows=_integer(observed[2]),
        missing_eligible_symbol_days=max(eligible - observed_rows, 0),
    )


def _invariant_summary(
    connection: psycopg.Connection[tuple[object, ...]],
    start_date: date,
    end_date: date,
) -> InvariantSummary:
    row = _required_row(
        connection,
        """
        with scoped as (
            select *
            from core.daily_bar
            where trade_date between %s and %s
        )
        select
            (
                select count(*) from (
                    select symbol, trade_date
                    from scoped
                    group by symbol, trade_date
                    having count(*) > 1
                ) duplicates
            ),
            count(*) filter (
                where low > high
                   or open not between low and high
                   or close not between low and high
            ),
            count(*) filter (
                where open < 0 or high < 0 or low < 0 or close < 0
                   or previous_close < 0 or volume < 0 or amount < 0
            ),
            count(*) filter (
                where not exists (
                    select 1 from core.trading_calendar calendar
                    where calendar.market = scoped.market
                      and calendar.trade_date = scoped.trade_date
                      and calendar.is_trading_day
                )
            ),
            count(*) filter (
                where exists (
                    select 1 from core.security security
                    where security.symbol = scoped.symbol
                      and (
                          (security.ipo_date is not null
                           and scoped.trade_date < security.ipo_date)
                          or (security.delisting_date is not null
                              and scoped.trade_date > security.delisting_date)
                      )
                )
            ),
            count(*) filter (where trade_status = 'unknown')
        from scoped
        """,
        (start_date, end_date),
    )
    return InvariantSummary(*(_integer(value) for value in row))


def _traceability_summary(
    connection: psycopg.Connection[tuple[object, ...]],
    start_date: date,
    end_date: date,
) -> TraceabilitySummary:
    row = _required_row(
        connection,
        """
        with scoped as (
            select *
            from core.daily_bar
            where trade_date between %s and %s
        ),
        referenced_runs as (
            select distinct ingestion_id from scoped
        ),
        manifest_counts as (
            select
                run.ingestion_id,
                run.fetched_rows,
                count(manifest.raw_id) as manifest_count,
                coalesce(sum(manifest.row_count), 0) as raw_row_count
            from ingestion.ingestion_run run
            join referenced_runs referenced using (ingestion_id)
            left join ingestion.raw_manifest manifest using (ingestion_id)
            group by run.ingestion_id, run.fetched_rows
        )
        select
            (select count(*) from referenced_runs),
            (
                select count(*) from ingestion.raw_manifest manifest
                join referenced_runs referenced using (ingestion_id)
            ),
            count(*) filter (where run.ingestion_id is null),
            count(*) filter (
                where run.ingestion_id is not null
                  and scoped.source_code <> run.provider_code
            ),
            (
                select count(*) from manifest_counts where manifest_count = 0
            ),
            (
                select count(*) from manifest_counts
                where manifest_count > 0 and raw_row_count <> fetched_rows
            ),
            (
                select count(*)
                from audit.quality_result quality
                join referenced_runs referenced using (ingestion_id)
                where quality.dataset_code = 'daily_bar'
                  and quality.status = 'failed'
            )
        from scoped
        left join ingestion.ingestion_run run using (ingestion_id)
        """,
        (start_date, end_date),
    )
    return TraceabilitySummary(*(_integer(value) for value in row))


def _source_distribution(
    connection: psycopg.Connection[tuple[object, ...]],
    start_date: date,
    end_date: date,
) -> tuple[SourceCount, ...]:
    rows = connection.execute(
        """
        select source_code, count(*)
        from core.daily_bar
        where trade_date between %s and %s
        group by source_code
        order by source_code
        """,
        (start_date, end_date),
    ).fetchall()
    return tuple(SourceCount(_string(row[0]), _integer(row[1])) for row in rows)


def _gap_candidates(
    connection: psycopg.Connection[tuple[object, ...]],
    start_date: date,
    end_date: date,
    limit: int,
) -> tuple[GapCandidate, ...]:
    rows = connection.execute(
        """
        with open_days as (
            select trade_date
            from core.trading_calendar
            where market = 'CN_A_SHARE'
              and is_trading_day
              and trade_date between %s and %s
        ),
        eligible as (
            select
                security.symbol,
                security.current_name,
                security.ipo_date,
                security.delisting_date,
                open_days.trade_date
            from core.security security
            cross join open_days
            where security.security_type = 'stock'
              and (
                  security.ipo_date is null
                  or open_days.trade_date >= security.ipo_date
              )
              and (
                  security.delisting_date is null
                  or open_days.trade_date <= security.delisting_date
              )
        )
        select
            eligible.symbol,
            eligible.current_name,
            eligible.ipo_date,
            eligible.delisting_date,
            count(*) as expected_rows,
            count(bar.trade_date) as observed_rows,
            count(*) filter (where bar.trade_status = 'suspended') as suspended_rows,
            count(*) - count(bar.trade_date) as missing_rows,
            min(eligible.trade_date) filter (where bar.trade_date is null),
            max(eligible.trade_date) filter (where bar.trade_date is null)
        from eligible
        left join core.daily_bar bar
          on bar.symbol = eligible.symbol
         and bar.trade_date = eligible.trade_date
        group by
            eligible.symbol,
            eligible.current_name,
            eligible.ipo_date,
            eligible.delisting_date
        having count(*) > count(bar.trade_date)
        order by missing_rows desc, eligible.symbol
        limit %s
        """,
        (start_date, end_date, limit),
    ).fetchall()
    return tuple(
        GapCandidate(
            symbol=_string(row[0]),
            current_name=_string(row[1]),
            ipo_date=_optional_date(row[2]),
            delisting_date=_optional_date(row[3]),
            expected_rows=_integer(row[4]),
            observed_rows=_integer(row[5]),
            suspended_rows=_integer(row[6]),
            missing_rows=_integer(row[7]),
            first_missing_date=_date(row[8]),
            last_missing_date=_date(row[9]),
        )
        for row in rows
    )


def _required_row(
    connection: psycopg.Connection[tuple[object, ...]],
    statement: str,
    parameters: tuple[object, ...],
) -> tuple[object, ...]:
    row = connection.execute(statement, parameters).fetchone()
    if row is None:
        raise RuntimeError("Daily Bar audit query returned no row")
    return row


def _status(report: DailyBarAuditReport) -> str:
    if report.has_errors:
        return "FAIL"
    if report.has_warnings:
        return "WARNING"
    return "PASS"


def _integer(value: object) -> int:
    if not isinstance(value, int):
        raise RuntimeError("Daily Bar audit expected an integer")
    return value


def _string(value: object) -> str:
    if not isinstance(value, str):
        raise RuntimeError("Daily Bar audit expected text")
    return value


def _date(value: object) -> date:
    if not isinstance(value, date):
        raise RuntimeError("Daily Bar audit expected a date")
    return value


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    return _date(value)


def _iso_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _display_date(value: date | None) -> str:
    return value.isoformat() if value is not None else "—"
