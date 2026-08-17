"""PostgreSQL repository for full-market opening-auction series sessions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from json import dumps
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
)
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    QualityResult,
    RawManifest,
)


class PostgreSQLCallAuctionMarketSeriesPersistence:
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

    def listed_sse_szse_stock_symbols(self) -> list[str]:
        with self._engine.connect() as connection:
            return list(
                connection.execute(
                    text("""
                        select symbol from core.security
                        where exchange in ('SSE','SZSE')
                          and security_type='stock' and status='listed'
                        order by symbol
                    """)
                ).scalars()
            )

    def load_recovery_universe(self, trade_date: date) -> tuple[str, ...] | None:
        with self._engine.connect() as connection:
            value = connection.execute(
                text("""
                    select universe_symbols
                    from realtime.call_auction_market_series_session
                    where trade_date=:trade_date
                    order by started_at desc, session_id desc
                    limit 1
                """),
                {"trade_date": trade_date},
            ).scalar_one_or_none()
        return tuple(value) if value is not None else None

    def create_session(self, session: MarketSeriesSession) -> None:
        if session.status is not MarketSeriesStatus.RUNNING:
            raise ValueError("new market-series session must be running")
        with self._engine.begin() as connection:
            connection.execute(
                text("""
                    insert into realtime.call_auction_market_series_session (
                      session_id,workflow_run_id,trade_date,window_start,window_end,
                      cadence_seconds,expected_rounds,universe_symbols,universe_count,
                      universe_hash,status,started_at,finished_at,successful_rounds,
                      partial_rounds,failed_rounds,successful_quotes,failed_quotes,error_summary
                    ) values (
                      :session_id,:workflow_run_id,:trade_date,:window_start,:window_end,
                      :cadence_seconds,:expected_rounds,:universe_symbols,:universe_count,
                      :universe_hash,:status,:started_at,:finished_at,:successful_rounds,
                      :partial_rounds,:failed_rounds,:successful_quotes,:failed_quotes,:error_summary
                    )
                """),
                _session_parameters(session),
            )

    def create_ingestion_run(self, run: IngestionRun) -> None:
        if run.dataset_code is not DatasetCode.CALL_AUCTION_MARKET_SERIES:
            raise ValueError("unexpected market-series dataset")
        if run.status is not IngestionStatus.RUNNING:
            raise ValueError("new market-series ingestion must be running")
        with self._engine.begin() as connection:
            connection.execute(
                text("""
                    insert into ingestion.ingestion_run (
                      ingestion_id,provider_code,dataset_code,status,requested_at,started_at,
                      finished_at,request_params,fetched_rows,accepted_rows,rejected_rows,
                      error_summary,replayed_from_raw_id
                    ) values (
                      :ingestion_id,:provider_code,:dataset_code,:status,:requested_at,:started_at,
                      :finished_at,cast(:request_params as jsonb),:fetched_rows,:accepted_rows,
                      :rejected_rows,:error_summary,:replayed_from_raw_id
                    )
                """),
                _run_parameters(run),
            )

    def start_round(self, round_state: MarketSeriesRound) -> None:
        if round_state.status is not MarketSeriesStatus.RUNNING:
            raise ValueError("new market-series round must be running")
        with self._engine.begin() as connection:
            session = connection.execute(
                text("""
                    select window_start,universe_count,status
                    from realtime.call_auction_market_series_session
                    where session_id=:session_id for update
                """),
                {"session_id": round_state.session_id},
            ).one()
            expected_scheduled_at = session.window_start + round_state.sample_seq * _TWENTY_SECONDS
            if session.status != "running":
                raise RuntimeError("market-series session is no longer running")
            if round_state.scheduled_at != expected_scheduled_at:
                raise ValueError("round scheduled_at does not match its session")
            if round_state.expected_quotes != session.universe_count:
                raise ValueError("round expected_quotes does not match frozen universe")
            connection.execute(
                text("""
                    insert into realtime.call_auction_market_series_round (
                      session_id,sample_seq,scheduled_at,collected_at,status,attempt_count,
                      expected_quotes,successful_quotes,failed_quotes,selected_ingestion_id,
                      error_summary
                    ) values (
                      :session_id,:sample_seq,:scheduled_at,:collected_at,:status,:attempt_count,
                      :expected_quotes,:successful_quotes,:failed_quotes,:selected_ingestion_id,
                      :error_summary
                    )
                """),
                _round_parameters(round_state),
            )

    def commit_attempt(
        self,
        run: IngestionRun,
        records: Sequence[MarketSeriesSnapshotRecord],
        manifest: RawManifest,
        quality_results: Sequence[QualityResult],
    ) -> None:
        if run.dataset_code is not DatasetCode.CALL_AUCTION_MARKET_SERIES:
            raise ValueError("unexpected market-series dataset")
        if run.status not in {IngestionStatus.SUCCEEDED, IngestionStatus.PARTIAL}:
            raise ValueError("market-series attempt must have a supported terminal status")
        if manifest.ingestion_id != run.ingestion_id or manifest.row_count != run.fetched_rows:
            raise ValueError("market-series manifest does not match ingestion run")
        if len(records) != run.accepted_rows:
            raise ValueError("market-series facts do not match accepted rows")
        if any(
            result.ingestion_id != run.ingestion_id
            or result.dataset_code is not DatasetCode.CALL_AUCTION_MARKET_SERIES
            for result in quality_results
        ):
            raise ValueError("market-series quality result does not match ingestion run")
        session_id = _request_uuid(run.request_params, "session_id")
        sample_seq = _request_int(run.request_params, "sample_seq")
        if any(
            record.session_id != session_id or record.sample_seq != sample_seq for record in records
        ):
            raise ValueError("market-series fact does not match ingestion request")

        with self._engine.begin() as connection:
            connection.execute(
                text("""
                    insert into ingestion.raw_manifest (
                      raw_id,ingestion_id,storage_backend,object_path,file_format,
                      content_sha256,byte_size,row_count,schema_version
                    ) values (
                      :raw_id,:ingestion_id,:storage_backend,:object_path,:file_format,
                      :content_sha256,:byte_size,:row_count,:schema_version
                    )
                """),
                _manifest_parameters(manifest),
            )
            if quality_results:
                connection.execute(
                    text("""
                        insert into audit.quality_result (
                          quality_result_id,ingestion_id,dataset_code,rule_code,severity,
                          status,natural_key,message,details
                        ) values (
                          :quality_result_id,:ingestion_id,:dataset_code,:rule_code,:severity,
                          :status,cast(:natural_key as jsonb),:message,cast(:details as jsonb)
                        )
                    """),
                    [_quality_parameters(result) for result in quality_results],
                )
            if records:
                connection.execute(
                    text("""
                        insert into realtime.call_auction_market_series_snapshot (
                          trade_date,ingestion_id,session_id,sample_seq,scheduled_at,symbol,
                          observed_at,last_price,previous_close,high_price,low_price,
                          cumulative_volume,cumulative_amount,source_code,value_semantics
                        ) values (
                          :trade_date,:ingestion_id,:session_id,:sample_seq,:scheduled_at,:symbol,
                          :observed_at,:last_price,:previous_close,:high_price,:low_price,
                          :cumulative_volume,:cumulative_amount,:source_code,:value_semantics
                        )
                    """),
                    [_snapshot_parameters(record, run.ingestion_id) for record in records],
                )
            updated = connection.execute(
                text("""
                    update ingestion.ingestion_run set
                      status=:status,finished_at=:finished_at,fetched_rows=:fetched_rows,
                      accepted_rows=:accepted_rows,rejected_rows=:rejected_rows,
                      error_summary=:error_summary
                    where ingestion_id=:ingestion_id and status='running'
                """),
                _run_parameters(run),
            )
            if updated.rowcount != 1:
                raise RuntimeError("market-series ingestion is no longer running")

    def finish_round(self, round_summary: MarketSeriesRound) -> None:
        if round_summary.status is MarketSeriesStatus.RUNNING:
            raise ValueError("market-series round finish requires terminal status")
        with self._engine.begin() as connection:
            stored = connection.execute(
                text("""
                    select round.scheduled_at,round.expected_quotes,round.status,
                           session.window_start,session.status session_status
                    from realtime.call_auction_market_series_round round
                    join realtime.call_auction_market_series_session session using (session_id)
                    where round.session_id=:session_id and round.sample_seq=:sample_seq
                    for update of round,session
                """),
                {
                    "session_id": round_summary.session_id,
                    "sample_seq": round_summary.sample_seq,
                },
            ).one()
            if stored.status != "running" or stored.session_status != "running":
                raise RuntimeError("market-series round or session is no longer running")
            if (
                stored.scheduled_at != round_summary.scheduled_at
                or stored.expected_quotes != round_summary.expected_quotes
                or stored.window_start + round_summary.sample_seq * _TWENTY_SECONDS
                != round_summary.scheduled_at
            ):
                raise ValueError("market-series round identity changed")
            if round_summary.selected_ingestion_id is not None:
                selected = connection.execute(
                    text("""
                        select dataset_code,status,request_params->>'session_id' session_id,
                               request_params->>'sample_seq' sample_seq
                        from ingestion.ingestion_run where ingestion_id=:ingestion_id
                    """),
                    {"ingestion_id": round_summary.selected_ingestion_id},
                ).one_or_none()
                if selected is None or (
                    selected.dataset_code != DatasetCode.CALL_AUCTION_MARKET_SERIES.value
                    or selected.status not in {"succeeded", "partial"}
                    or selected.session_id != str(round_summary.session_id)
                    or selected.sample_seq != str(round_summary.sample_seq)
                ):
                    raise ValueError("selected ingestion does not match market-series round")
            updated = connection.execute(
                text("""
                    update realtime.call_auction_market_series_round set
                      collected_at=:collected_at,status=:status,attempt_count=:attempt_count,
                      successful_quotes=:successful_quotes,failed_quotes=:failed_quotes,
                      selected_ingestion_id=:selected_ingestion_id,error_summary=:error_summary
                    where session_id=:session_id and sample_seq=:sample_seq and status='running'
                """),
                _round_parameters(round_summary),
            )
            if updated.rowcount != 1:
                raise RuntimeError("market-series round is no longer running")
            _refresh_session_counts(connection, round_summary.session_id)

    def finish_session(self, session_id: UUID, finished_at: datetime) -> MarketSeriesSession:
        return self._finish_session(session_id, finished_at, None)

    def recover_expired_sessions(self, now: datetime) -> int:
        _require_aware(now)
        with self._engine.begin() as connection:
            session_ids = list(
                connection.execute(
                    text("""
                        select session_id
                        from realtime.call_auction_market_series_session
                        where status='running' and window_end < :now
                        order by session_id for update
                    """),
                    {"now": now},
                ).scalars()
            )
            if session_ids:
                connection.execute(
                    text("""
                        update realtime.call_auction_market_series_round set
                          collected_at=greatest(:now,scheduled_at),status='failed',
                          failed_quotes=expected_quotes,error_summary='worker_interrupted'
                        where session_id=any(:session_ids) and status='running'
                    """),
                    {"now": now, "session_ids": session_ids},
                )
        for session_id in session_ids:
            self._finish_session(session_id, now, "worker_interrupted")
        return len(session_ids)

    def _finish_session(
        self,
        session_id: UUID,
        finished_at: datetime,
        error_summary: str | None,
    ) -> MarketSeriesSession:
        _require_aware(finished_at)
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text("""
                    select * from realtime.call_auction_market_series_session
                    where session_id=:session_id for update
                """),
                    {"session_id": session_id},
                )
                .mappings()
                .one()
            )
            session = _session(cast(Mapping[str, object], row))
            if session.status is not MarketSeriesStatus.RUNNING:
                return session
            stats = connection.execute(
                text("""
                    select
                      count(*) filter (where status='succeeded')::integer successful_rounds,
                      count(*) filter (where status='partial')::integer partial_rounds,
                      count(*) filter (where status='failed')::integer failed_rounds,
                      coalesce(sum(successful_quotes),0)::bigint successful_quotes,
                      coalesce(sum(failed_quotes),0)::bigint failed_quotes
                    from realtime.call_auction_market_series_round
                    where session_id=:session_id and status <> 'running'
                """),
                {"session_id": session_id},
            ).one()
            recorded_rounds = stats.successful_rounds + stats.partial_rounds + stats.failed_rounds
            missing_rounds = max(0, session.expected_rounds - recorded_rounds)
            successful_quotes = stats.successful_quotes
            failed_quotes = stats.failed_quotes + missing_rounds * session.universe_count
            failed_rounds = stats.failed_rounds + missing_rounds
            status = (
                MarketSeriesStatus.SUCCEEDED
                if stats.successful_rounds == session.expected_rounds
                else MarketSeriesStatus.PARTIAL
                if successful_quotes > 0 or stats.partial_rounds > 0
                else MarketSeriesStatus.FAILED
            )
            summary = error_summary or ("missed_sampling_rounds" if missing_rounds else None)
            connection.execute(
                text("""
                    update realtime.call_auction_market_series_session set
                      status=:status,finished_at=:finished_at,
                      successful_rounds=:successful_rounds,partial_rounds=:partial_rounds,
                      failed_rounds=:failed_rounds,successful_quotes=:successful_quotes,
                      failed_quotes=:failed_quotes,error_summary=:error_summary
                    where session_id=:session_id and status='running'
                """),
                {
                    "session_id": session_id,
                    "status": status.value,
                    "finished_at": finished_at,
                    "successful_rounds": stats.successful_rounds,
                    "partial_rounds": stats.partial_rounds,
                    "failed_rounds": failed_rounds,
                    "successful_quotes": successful_quotes,
                    "failed_quotes": failed_quotes,
                    "error_summary": summary,
                },
            )
            updated = (
                connection.execute(
                    text("""
                    select * from realtime.call_auction_market_series_session
                    where session_id=:session_id
                """),
                    {"session_id": session_id},
                )
                .mappings()
                .one()
            )
        return _session(cast(Mapping[str, object], updated))


_TWENTY_SECONDS = timedelta(seconds=20)


def _refresh_session_counts(connection: Connection, session_id: UUID) -> None:
    connection.execute(
        text("""
            update realtime.call_auction_market_series_session session set
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
                coalesce(sum(successful_quotes),0)::bigint successful_quotes,
                coalesce(sum(failed_quotes),0)::bigint failed_quotes
              from realtime.call_auction_market_series_round
              where session_id=:session_id and status <> 'running'
              group by session_id
            ) stats where session.session_id=stats.session_id
        """),
        {"session_id": session_id},
    )


def _session_parameters(value: MarketSeriesSession) -> dict[str, object]:
    parameters = {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__
        for item in (getattr(value, name),)
    }
    parameters["universe_symbols"] = list(value.universe_symbols)
    return parameters


def _round_parameters(value: MarketSeriesRound) -> dict[str, object]:
    return {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__
        for item in (getattr(value, name),)
    }


def _run_parameters(value: IngestionRun) -> dict[str, object]:
    return {
        "ingestion_id": value.ingestion_id,
        "provider_code": value.provider_code.value,
        "dataset_code": value.dataset_code.value,
        "status": value.status.value,
        "requested_at": value.requested_at,
        "started_at": value.started_at,
        "finished_at": value.finished_at,
        "request_params": dumps(value.request_params, sort_keys=True),
        "fetched_rows": value.fetched_rows,
        "accepted_rows": value.accepted_rows,
        "rejected_rows": value.rejected_rows,
        "error_summary": value.error_summary,
        "replayed_from_raw_id": value.replayed_from_raw_id,
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


def _snapshot_parameters(
    value: MarketSeriesSnapshotRecord, ingestion_id: UUID
) -> dict[str, object]:
    return {
        "trade_date": value.trade_date,
        "ingestion_id": ingestion_id,
        "session_id": value.session_id,
        "sample_seq": value.sample_seq,
        "scheduled_at": value.scheduled_at,
        "symbol": value.symbol,
        "observed_at": value.observed_at,
        "last_price": value.last_price,
        "previous_close": value.previous_close,
        "high_price": value.high_price,
        "low_price": value.low_price,
        "cumulative_volume": value.cumulative_volume,
        "cumulative_amount": value.cumulative_amount,
        "source_code": value.source_code,
        "value_semantics": value.value_semantics.value,
    }


def _session(row: Mapping[str, object]) -> MarketSeriesSession:
    symbols = tuple(cast(Sequence[str], row["universe_symbols"]))
    return MarketSeriesSession(
        session_id=cast(UUID, row["session_id"]),
        workflow_run_id=cast(UUID, row["workflow_run_id"]),
        trade_date=cast(date, row["trade_date"]),
        window_start=_utc_datetime(row["window_start"]),
        window_end=_utc_datetime(row["window_end"]),
        cadence_seconds=cast(int, row["cadence_seconds"]),
        expected_rounds=cast(int, row["expected_rounds"]),
        universe_symbols=symbols,
        universe_count=cast(int, row["universe_count"]),
        universe_hash=str(row["universe_hash"]),
        status=MarketSeriesStatus(str(row["status"])),
        started_at=_utc_datetime(row["started_at"]),
        finished_at=(_utc_datetime(row["finished_at"]) if row["finished_at"] is not None else None),
        successful_rounds=cast(int, row["successful_rounds"]),
        partial_rounds=cast(int, row["partial_rounds"]),
        failed_rounds=cast(int, row["failed_rounds"]),
        successful_quotes=cast(int, row["successful_quotes"]),
        failed_quotes=cast(int, row["failed_quotes"]),
        error_summary=(str(row["error_summary"]) if row["error_summary"] is not None else None),
    )


def _request_uuid(values: Mapping[str, object], key: str) -> UUID:
    try:
        return UUID(str(values[key]))
    except (KeyError, ValueError) as error:
        raise ValueError(f"market-series request requires {key}") from error


def _request_int(values: Mapping[str, object], key: str) -> int:
    try:
        value = values[key]
    except KeyError as error:
        raise ValueError(f"market-series request requires {key}") from error
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"market-series request requires integer {key}")
    return value


def _utc_datetime(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("market-series timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("market-series timestamp must be timezone-aware")
