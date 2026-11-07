"""Orchestration of bounded source reads and immutable minute-volume publication."""

import re
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC, date, datetime
from time import monotonic
from typing import Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.stock_max_minute_volume import (
    MinuteVolumeBuildSummary,
    MinuteVolumePeak,
    MinuteVolumeSource,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import NORMALIZER_VERSION, UNIT_CONTRACT_VERSION
from market_data_center.providers.stock_minute_volume import SOURCE_SCHEMA_VERSION, replay
from market_data_center.quantity_certification import build_quantity_certificates
from market_data_center.raw_store import LocalRawStore
from market_data_center.stock_max_minute_volume_calculator import calculate_minute_volume_peak

type SourceRecord = tuple[IngestionRun, RawManifest | None, dict[str, object] | None]


class MinuteVolumePersistence(Protocol):
    def is_trading_day(self, trade_date: date) -> bool | None: ...
    def candidates(
        self, trade_date: date, symbols: tuple[str, ...] | None = None
    ) -> tuple[str, ...]: ...
    def build_lock(self, trade_date: date) -> AbstractContextManager[None]: ...
    def record_source(
        self, run: IngestionRun, manifest: RawManifest | None, certificate: dict[str, object] | None
    ) -> None: ...
    def publish(
        self, trade_date: date, peaks: tuple[MinuteVolumePeak, ...], evidence: dict[str, object]
    ) -> MinuteVolumeBuildSummary: ...


class MinuteVolumeProvider(Protocol):
    def fetch(self, symbol: str, trade_date: date) -> MinuteVolumeSource: ...


def _gap(symbol: str, day: date, reason: str) -> MinuteVolumePeak:
    return MinuteVolumePeak(symbol, day, None, None, 240, 0, (reason,))


class StockMaxMinuteVolumeService:
    def __init__(
        self,
        persistence: MinuteVolumePersistence,
        raw_store: LocalRawStore,
        provider: MinuteVolumeProvider,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        timer: Callable[[], float] = monotonic,
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider = provider
        self._clock = clock
        self._timer = timer

    def build(
        self, trade_date: date, *, symbols: tuple[str, ...] | None = None
    ) -> MinuteVolumeBuildSummary:
        if trade_date > datetime.now(ZoneInfo("Asia/Shanghai")).date():
            raise ValueError("trade date cannot be in the future")
        if symbols is not None:
            if not 1 <= len(symbols) <= 500 or any(
                re.fullmatch(r"(SSE|SZSE|BSE):[0-9]{6}", symbol) is None for symbol in symbols
            ):
                raise ValueError("explicit stock universe must contain 1 to 500 standard symbols")
            symbols = tuple(dict.fromkeys(symbols))
        trading = self._persistence.is_trading_day(trade_date)
        if trading is None:
            raise ValueError("exact target calendar is unknown")
        if not trading:
            return MinuteVolumeBuildSummary("skipped", trade_date, None, None, 0, 0)
        with self._persistence.build_lock(trade_date):
            candidates = self._persistence.candidates(trade_date, symbols)
            peaks = []
            stocks: dict[str, dict[str, object]] = {}
            pending: dict[str, SourceRecord] = {}
            blocked_markets: set[str] = set()
            deadline = self._timer() + 1800
            for symbol in candidates:
                proof: dict[str, object]
                exchange = symbol.partition(":")[0]
                if self._timer() >= deadline:
                    peak, proof = _gap(symbol, trade_date, "collection_budget_exhausted"), {}
                elif exchange in blocked_markets:
                    peak, proof = _gap(symbol, trade_date, "quantity_units_unverified"), {}
                else:
                    peak, proof, record = self._collect(symbol, trade_date)
                    pending[symbol] = record
                    if "quantity_units_unverified" in peak.quality_reasons:
                        blocked_markets.add(exchange)
                peaks.append(peak)
                stocks[symbol] = proof
            for index, peak in enumerate(peaks):
                if peak.symbol.partition(":")[0] in blocked_markets:
                    peaks[index] = replace(
                        peak,
                        max_minute_volume_shares=None,
                        peak_at=None,
                        quality_reasons=("quantity_units_unverified",),
                    )
                    stocks[peak.symbol].update(
                        evidence_hash=None, reason="quantity_units_unverified"
                    )
                    if peak.symbol in pending:
                        run, manifest, _ = pending[peak.symbol]
                        pending[peak.symbol] = (
                            replace(
                                run,
                                status=IngestionStatus.FAILED,
                                accepted_rows=0,
                                rejected_rows=run.fetched_rows,
                                error_summary="quantity_units_unverified",
                            ),
                            manifest,
                            None,
                        )
            # A market's unit gate must finish before any successful certificate commits.
            for run, manifest, certificate in pending.values():
                self._persistence.record_source(run, manifest, certificate)
            return self._persistence.publish(
                trade_date,
                tuple(peaks),
                {
                    "coverage_scope": "full_market" if symbols is None else "requested_symbols",
                    "stocks": stocks,
                    "unit_contract_version": UNIT_CONTRACT_VERSION,
                },
            )

    def _collect(
        self, symbol: str, day: date
    ) -> tuple[MinuteVolumePeak, dict[str, object], SourceRecord]:
        ingestion_id = uuid4()
        started = self._clock()
        try:
            source = self._provider.fetch(symbol, day)
            if source.symbol != symbol or source.trade_date != day:
                raise ProviderError("source_response_invalid: requested identity mismatch")
            verified = replay(source.raw_rows, source.metadata)
            if verified.bars != source.bars:
                raise ProviderError("source_response_invalid: normalized values disagree with Raw")
            verified = replace(verified, quality_reasons=source.quality_reasons)
            peak = calculate_minute_volume_peak(verified)
            if peak.peak_at is not None:
                peak = replace(peak, peak_at=peak.peak_at.replace(tzinfo=ZoneInfo("Asia/Shanghai")))
        except Exception as error:
            message = str(error) if isinstance(error, ProviderError) else ""
            reason = (
                "quantity_units_unverified"
                if "quantity unit contract" in message
                else "collection_budget_exhausted"
                if "collection_budget_exhausted" in message
                else "source_response_invalid"
                if "source_response_invalid" in message
                else "minute_capability_unavailable"
            )
            run = IngestionRun(
                ingestion_id,
                ProviderCode.PYTDX,
                DatasetCode.STOCK_MINUTE_VOLUME,
                IngestionStatus.FAILED,
                started,
                started,
                self._clock(),
                {"symbol": symbol, "trade_date": day.isoformat()},
                error_summary=type(error).__name__[:200],
            )
            return (
                _gap(symbol, day, reason),
                {"ingestion_id": str(ingestion_id), "reason": reason},
                (run, None, None),
            )

        params = {
            **verified.metadata,
            "provider_schema_version": SOURCE_SCHEMA_VERSION,
            "quantity_unit_contract_version": UNIT_CONTRACT_VERSION,
            "quantity_normalizer_version": NORMALIZER_VERSION,
        }
        stored = self._raw_store.write_jsonl(
            provider="pytdx",
            dataset="stock_minute_volume",
            partition_date=day,
            ingestion_id=ingestion_id,
            rows=verified.raw_rows,
            schema_version=SOURCE_SCHEMA_VERSION,
        )
        manifest = RawManifest(
            uuid4(),
            ingestion_id,
            stored.object_path,
            RawFileFormat.JSONL,
            stored.content_sha256,
            stored.byte_size,
            stored.row_count,
            SOURCE_SCHEMA_VERSION,
        )
        accepted = len(verified.bars) if peak.max_minute_volume_shares is not None else 0
        run = IngestionRun(
            ingestion_id,
            ProviderCode.PYTDX,
            DatasetCode.STOCK_MINUTE_VOLUME,
            IngestionStatus.SUCCEEDED if accepted else IngestionStatus.FAILED,
            started,
            started,
            self._clock(),
            params,
            fetched_rows=len(verified.raw_rows),
            accepted_rows=accepted,
            rejected_rows=len(verified.raw_rows) - accepted,
            error_summary=None if accepted else ",".join(peak.quality_reasons)[:200],
        )
        certificates = (
            build_quantity_certificates(
                ingestion_id=ingestion_id,
                provider="pytdx",
                dataset="stock_minute_volume",
                manifest=manifest,
                request_params=params,
            )
            if accepted
            else ()
        )
        certificate = certificates[0] if certificates else None
        return (
            peak,
            {
                "ingestion_id": str(ingestion_id),
                "raw_id": str(manifest.raw_id),
                "raw_hash": manifest.content_sha256,
                "evidence_hash": certificate["evidence_hash"] if certificate else None,
                "metadata": params,
            },
            (run, manifest, certificate),
        )
