"""PostgreSQL repository for opening-auction collection sessions."""

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from json import dumps
from typing import cast
from uuid import UUID

from sqlalchemy import Engine, text

from market_data_center.domain.auction import (
    AuctionCollectionSession,
    AuctionPoolMember,
    AuctionPoolSnapshotInput,
    AuctionQuoteMetric,
    AuctionQuoteSample,
    AuctionRoundSummary,
    AuctionSessionStatus,
    auction_phase,
)
from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.domain.stock_pool import MAINBOARD_LIMIT_UP_POOL


class AuctionDependencyNotReady(RuntimeError):
    """The exact calendar or frozen pool dependency is not ready."""


class PostgreSQLAuctionPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool:
        with self._engine.connect() as connection:
            return bool(
                connection.execute(
                    text("""
select is_trading_day from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:trade_date
"""),
                    {"trade_date": trade_date},
                ).scalar_one_or_none()
            )

    def load_exact_pool(self, trade_date: date) -> AuctionPoolSnapshotInput:
        with self._engine.connect() as connection:
            snapshot = (
                connection.execute(
                    text("""
select snapshot_id, calculation_id, version, basis_trade_date, effective_trade_date,
       member_count, rejected_count
from stock_pool.snapshot
where pool_code=:pool_code and effective_trade_date=:trade_date and status='ready'
order by version desc limit 1
"""),
                    {"pool_code": MAINBOARD_LIMIT_UP_POOL, "trade_date": trade_date},
                )
                .mappings()
                .one_or_none()
            )
            if snapshot is None:
                raise AuctionDependencyNotReady("exact ready limit-up pool snapshot is missing")
            if snapshot["member_count"] < 1:
                raise AuctionDependencyNotReady("limit-up pool has no members")
            rows = (
                connection.execute(
                    text("""
select member.symbol, price_limit.upper_limit, price_limit.rule_version
from stock_pool.member member
join derived.daily_price_limit price_limit
  on price_limit.calculation_id=:calculation_id
 and price_limit.symbol=member.symbol
 and price_limit.trade_date=:basis_trade_date
where member.snapshot_id=:snapshot_id and member.direction='up'
order by member.symbol
"""),
                    {
                        "calculation_id": snapshot["calculation_id"],
                        "basis_trade_date": snapshot["basis_trade_date"],
                        "snapshot_id": snapshot["snapshot_id"],
                    },
                )
                .mappings()
                .all()
            )
        if len(rows) != snapshot["member_count"]:
            raise AuctionDependencyNotReady("frozen pool members or price limits are incomplete")
        return AuctionPoolSnapshotInput(
            snapshot["snapshot_id"],
            snapshot["version"],
            snapshot["basis_trade_date"],
            snapshot["effective_trade_date"],
            tuple(
                AuctionPoolMember(row["symbol"], row["upper_limit"], row["rule_version"])
                for row in rows
            ),
        )

    def create_or_resume_session(
        self, session: AuctionCollectionSession
    ) -> AuctionCollectionSession:
        with self._engine.begin() as connection:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key,0))"),
                {"key": f"auction-session:{session.effective_trade_date.isoformat()}"},
            )
            existing = (
                connection.execute(
                    text("""
select * from realtime.auction_collection_session
where pool_snapshot_id=:pool_snapshot_id and effective_trade_date=:effective_trade_date
  and cadence_seconds=:cadence_seconds and provider_code=:provider_code
"""),
                    _session_parameters(session),
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                return _session(cast(Mapping[str, object], existing))
            connection.execute(
                text("""
insert into realtime.auction_collection_session (
 session_id, pool_snapshot_id, pool_snapshot_version, basis_trade_date,
 effective_trade_date, window_start, window_end, cadence_seconds, expected_rounds,
 expected_quotes, provider_code, status, started_at
) values (
 :session_id, :pool_snapshot_id, :pool_snapshot_version, :basis_trade_date,
 :effective_trade_date, :window_start, :window_end, :cadence_seconds, :expected_rounds,
 :expected_quotes, :provider_code, :status, :started_at
)
"""),
                _session_parameters(session),
            )
        return session

    def completed_sequences(self, session_id: UUID) -> set[int]:
        with self._engine.connect() as connection:
            return set(
                connection.execute(
                    text("""
select sample_seq from realtime.auction_collection_round where session_id=:session_id
"""),
                    {"session_id": session_id},
                ).scalars()
            )

    def create_ingestion_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
insert into ingestion.ingestion_run (
 ingestion_id, provider_code, dataset_code, status, requested_at, started_at,
 request_params, fetched_rows, accepted_rows, rejected_rows, error_summary
) values (
 :ingestion_id, :provider_code, :dataset_code, :status, :requested_at, :started_at,
 cast(:request_params as jsonb), :fetched_rows, :accepted_rows, :rejected_rows, :error_summary
)
"""),
                _run_parameters(run),
            )

    def fail_ingestion_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(_UPDATE_RUN, _run_parameters(run))

    def commit_round(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        summary: AuctionRoundSummary,
        samples: Sequence[AuctionQuoteSample],
        metrics: Sequence[AuctionQuoteMetric],
        findings: Sequence[QualityResult],
    ) -> None:
        if len(samples) != len(metrics):
            raise ValueError("auction samples and metrics must align")
        with self._engine.begin() as connection:
            connection.execute(
                text("""
insert into ingestion.raw_manifest (
 raw_id, ingestion_id, storage_backend, object_path, file_format,
 content_sha256, byte_size, row_count, schema_version
) values (
 :raw_id, :ingestion_id, :storage_backend, :object_path, :file_format,
 :content_sha256, :byte_size, :row_count, :schema_version
)
"""),
                _manifest_parameters(manifest),
            )
            if findings:
                connection.execute(
                    text("""
insert into audit.quality_result (
 quality_result_id, ingestion_id, dataset_code, rule_code, severity,
 status, natural_key, message, details
) values (
 :quality_result_id, :ingestion_id, :dataset_code, :rule_code, :severity,
 :status, cast(:natural_key as jsonb), :message, cast(:details as jsonb)
)
"""),
                    [_quality_parameters(item) for item in findings],
                )
            if samples:
                connection.execute(
                    text(_QUOTE_INSERT),
                    [
                        _sample_parameters(sample, run.ingestion_id, manifest.raw_id)
                        for sample in samples
                    ],
                )
                connection.execute(
                    text("""
insert into derived.auction_quote_metric (
 session_id, symbol, sample_seq, spread, mid_price, bid_depth_5, ask_depth_5,
 imbalance_5, seal_amount, calculated_at, algorithm_version, price_limit_rule_version
) values (
 :session_id, :symbol, :sample_seq, :spread, :mid_price, :bid_depth_5, :ask_depth_5,
 :imbalance_5, :seal_amount, :calculated_at, :algorithm_version,
 :price_limit_rule_version
)
"""),
                    [
                        {
                            "session_id": sample.session_id,
                            "symbol": sample.quote.symbol,
                            "sample_seq": sample.sample_seq,
                            **_metric_parameters(metric),
                        }
                        for sample, metric in zip(samples, metrics, strict=True)
                    ],
                )
            connection.execute(
                text("""
insert into realtime.auction_collection_round (
 session_id, sample_seq, ingestion_id, scheduled_at, collected_at, phase, status,
 expected_quotes, successful_quotes, failed_quotes, latency_ms
) values (
 :session_id, :sample_seq, :ingestion_id, :scheduled_at, :collected_at, :phase, :status,
 :expected_quotes, :successful_quotes, :failed_quotes, :latency_ms
)
"""),
                {
                    "session_id": (
                        samples[0].session_id
                        if samples
                        else UUID(str(run.request_params["session_id"]))
                    ),
                    "ingestion_id": run.ingestion_id,
                    **_round_parameters(summary),
                },
            )
            connection.execute(_UPDATE_RUN, _run_parameters(run))
            connection.execute(
                text("""
update realtime.auction_collection_session session set
 successful_rounds=stats.successful_rounds,
 partial_rounds=stats.partial_rounds,
 failed_rounds=stats.failed_rounds,
 successful_quotes=stats.successful_quotes,
 failed_quotes=stats.failed_quotes
from (
 select session_id,
  count(*) filter (where status='succeeded')::integer successful_rounds,
  count(*) filter (where status='partial')::integer partial_rounds,
  count(*) filter (where status='failed')::integer failed_rounds,
  coalesce(sum(successful_quotes),0) successful_quotes,
  coalesce(sum(failed_quotes),0) failed_quotes
 from realtime.auction_collection_round where session_id=:session_id group by session_id
) stats where session.session_id=stats.session_id
"""),
                {
                    "session_id": (
                        samples[0].session_id
                        if samples
                        else UUID(str(run.request_params["session_id"]))
                    )
                },
            )

    def finish_session(self, session_id: UUID, finished_at: datetime) -> AuctionCollectionSession:
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "select * from realtime.auction_collection_session "
                        "where session_id=:id for update"
                    ),
                    {"id": session_id},
                )
                .mappings()
                .one()
            )
            session = _session(cast(Mapping[str, object], row))
            if session.status is not AuctionSessionStatus.RUNNING:
                return session
            recorded_rounds = (
                session.successful_rounds + session.partial_rounds + session.failed_rounds
            )
            missing_rounds = max(0, session.expected_rounds - recorded_rounds)
            missing_quotes = max(
                0,
                session.expected_quotes - session.successful_quotes - session.failed_quotes,
            )
            failed_rounds = session.failed_rounds + missing_rounds
            failed_quotes = session.failed_quotes + missing_quotes
            status = (
                AuctionSessionStatus.SUCCEEDED
                if session.successful_rounds == session.expected_rounds
                else AuctionSessionStatus.PARTIAL
                if session.successful_quotes > 0
                else AuctionSessionStatus.FAILED
            )
            connection.execute(
                text("""
update realtime.auction_collection_session set status=:status, finished_at=:finished_at,
 failed_rounds=:failed_rounds, failed_quotes=:failed_quotes,
 error_summary=:error_summary where session_id=:session_id and status='running'
"""),
                {
                    "session_id": session_id,
                    "status": status.value,
                    "finished_at": finished_at,
                    "failed_rounds": failed_rounds,
                    "failed_quotes": failed_quotes,
                    "error_summary": "missed_sampling_rounds" if missing_rounds else None,
                },
            )
            updated = (
                connection.execute(
                    text("select * from realtime.auction_collection_session where session_id=:id"),
                    {"id": session_id},
                )
                .mappings()
                .one()
            )
        return _session(cast(Mapping[str, object], updated))

    def fail_session(self, session_id: UUID, finished_at: datetime, error_type: str) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
update realtime.auction_collection_session set status='failed', finished_at=:finished_at,
 error_summary=:error_summary where session_id=:session_id and status='running'
"""),
                {
                    "session_id": session_id,
                    "finished_at": finished_at,
                    "error_summary": error_type[:200],
                },
            )

    def recover_expired_sessions(self, now: datetime) -> int:
        """Fail sessions whose immutable live window has ended after a process crash."""
        with self._engine.begin() as connection:
            result = connection.execute(
                text("""
update realtime.auction_collection_session set
 status='failed', finished_at=:now, error_summary='worker_interrupted'
where status='running' and window_end < :now
"""),
                {"now": now},
            )
        return result.rowcount


_UPDATE_RUN = text("""
update ingestion.ingestion_run set status=:status, finished_at=:finished_at,
 fetched_rows=:fetched_rows, accepted_rows=:accepted_rows, rejected_rows=:rejected_rows,
 error_summary=:error_summary where ingestion_id=:ingestion_id
""")

_QUOTE_INSERT = """
insert into realtime.five_level_quote_snapshot (
 session_id,pool_snapshot_id,ingestion_id,raw_id,symbol,sample_seq,scheduled_at,
 collected_at,source_timestamp,phase,quote_semantics,quote_status,last_price,
 previous_close,open,high,low,cumulative_volume,cumulative_amount,
 bid1_price,bid1_volume,ask1_price,ask1_volume,bid2_price,bid2_volume,ask2_price,ask2_volume,
 bid3_price,bid3_volume,ask3_price,ask3_volume,bid4_price,bid4_volume,ask4_price,ask4_volume,
 bid5_price,bid5_volume,ask5_price,ask5_volume,source_code
) values (
 :session_id,:pool_snapshot_id,:ingestion_id,:raw_id,:symbol,:sample_seq,:scheduled_at,
 :collected_at,:source_timestamp,:phase,:quote_semantics,:quote_status,:last_price,
 :previous_close,:open,:high,:low,:cumulative_volume,:cumulative_amount,
 :bid1_price,:bid1_volume,:ask1_price,:ask1_volume,:bid2_price,:bid2_volume,:ask2_price,:ask2_volume,
 :bid3_price,:bid3_volume,:ask3_price,:ask3_volume,:bid4_price,:bid4_volume,:ask4_price,:ask4_volume,
 :bid5_price,:bid5_volume,:ask5_price,:ask5_volume,:source_code
)
"""


def _session_parameters(value: AuctionCollectionSession) -> dict[str, object]:
    return {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__
        for item in (getattr(value, name),)
    }


def _session(row: Mapping[str, object]) -> AuctionCollectionSession:
    return AuctionCollectionSession(
        session_id=row["session_id"],  # type: ignore[arg-type]
        pool_snapshot_id=row["pool_snapshot_id"],  # type: ignore[arg-type]
        pool_snapshot_version=cast(int, row["pool_snapshot_version"]),
        basis_trade_date=row["basis_trade_date"],  # type: ignore[arg-type]
        effective_trade_date=row["effective_trade_date"],  # type: ignore[arg-type]
        window_start=row["window_start"],  # type: ignore[arg-type]
        window_end=row["window_end"],  # type: ignore[arg-type]
        cadence_seconds=cast(int, row["cadence_seconds"]),
        expected_rounds=cast(int, row["expected_rounds"]),
        expected_quotes=cast(int, row["expected_quotes"]),
        provider_code=str(row["provider_code"]),
        status=AuctionSessionStatus(str(row["status"])),
        started_at=row["started_at"],  # type: ignore[arg-type]
        finished_at=row["finished_at"],  # type: ignore[arg-type]
        successful_rounds=cast(int, row["successful_rounds"]),
        partial_rounds=cast(int, row["partial_rounds"]),
        failed_rounds=cast(int, row["failed_rounds"]),
        successful_quotes=cast(int, row["successful_quotes"]),
        failed_quotes=cast(int, row["failed_quotes"]),
        error_summary=row["error_summary"] if isinstance(row["error_summary"], str) else None,
    )


def _run_parameters(run: IngestionRun) -> dict[str, object]:
    return {
        "ingestion_id": run.ingestion_id,
        "provider_code": run.provider_code.value,
        "dataset_code": run.dataset_code.value,
        "status": run.status.value,
        "requested_at": run.requested_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "request_params": dumps(run.request_params, sort_keys=True),
        "fetched_rows": run.fetched_rows,
        "accepted_rows": run.accepted_rows,
        "rejected_rows": run.rejected_rows,
        "error_summary": run.error_summary,
    }


def _manifest_parameters(value: RawManifest) -> dict[str, object]:
    return {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__
        for item in (getattr(value, name),)
    }


def _quality_parameters(value: QualityResult) -> dict[str, object]:
    return {
        "quality_result_id": value.quality_result_id,
        "ingestion_id": value.ingestion_id,
        "dataset_code": value.dataset_code.value,
        "rule_code": value.rule_code,
        "severity": value.severity.value,
        "status": value.status.value,
        "natural_key": dumps(value.natural_key, sort_keys=True),
        "message": value.message,
        "details": dumps(value.details, sort_keys=True),
    }


def _sample_parameters(
    sample: AuctionQuoteSample, ingestion_id: UUID, raw_id: UUID
) -> dict[str, object]:
    quote = sample.quote
    result: dict[str, object] = {
        "session_id": sample.session_id,
        "pool_snapshot_id": sample.pool_snapshot_id,
        "ingestion_id": ingestion_id,
        "raw_id": raw_id,
        "symbol": quote.symbol,
        "sample_seq": sample.sample_seq,
        "scheduled_at": sample.scheduled_at,
        "collected_at": sample.collected_at,
        "source_timestamp": quote.source_timestamp,
        "phase": sample.phase.value,
        "quote_semantics": sample.semantics.value,
        "quote_status": quote.quote_status.value,
        "last_price": quote.last_price,
        "previous_close": quote.previous_close,
        "open": quote.open,
        "high": quote.high,
        "low": quote.low,
        "cumulative_volume": quote.cumulative_volume,
        "cumulative_amount": quote.cumulative_amount,
        "source_code": quote.source_code,
    }
    for side, levels in (("bid", quote.bid_levels), ("ask", quote.ask_levels)):
        for level in levels:
            result[f"{side}{level.level}_price"] = level.price
            result[f"{side}{level.level}_volume"] = level.volume
    return result


def _metric_parameters(value: AuctionQuoteMetric) -> dict[str, object]:
    return {name: getattr(value, name) for name in value.__slots__}


def _round_parameters(value: AuctionRoundSummary) -> dict[str, object]:
    return {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__
        for item in (getattr(value, name),)
    } | {"phase": auction_phase(value.scheduled_at).value}
