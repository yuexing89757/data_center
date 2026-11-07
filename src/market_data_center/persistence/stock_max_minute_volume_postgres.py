"""Atomic source lineage and immutable exact-date minute-volume publications."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, date, datetime, time
from hashlib import sha256
from json import dumps
from re import fullmatch
from typing import cast
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.ingestion import IngestionRun, RawManifest
from market_data_center.domain.stock_max_minute_volume import (
    ALGORITHM_VERSION,
    MinuteVolumeBuildSummary,
    MinuteVolumePeak,
)
from market_data_center.persistence.postgres import INSERT_INGESTION_RUN, INSERT_RAW_MANIFEST
from market_data_center.quantity_certification import (
    quantity_certificates,
    write_quantity_certificates,
)

CALCULATION_CODE = "cn_a_stock_max_minute_volume"


def _json(value: object) -> str:
    return dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class PostgreSQLStockMaxMinuteVolumePersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool | None:
        with self._engine.connect() as c:
            return c.execute(
                text("""select is_trading_day from core.trading_calendar
                where market='CN_A_SHARE' and trade_date=:day"""),
                {"day": trade_date},
            ).scalar_one_or_none()

    def candidates(
        self, trade_date: date, symbols: tuple[str, ...] | None = None
    ) -> tuple[str, ...]:
        if symbols is not None and not 1 <= len(symbols) <= 500:
            raise ValueError("requested stock universe must contain 1 to 500 symbols")
        with self._engine.connect() as c:
            rows = (
                c.execute(
                    text("""select symbol,ipo_date,status,delisting_date from core.security
                where security_type='stock' and exchange in ('SSE','SZSE','BSE')
                  and (ipo_date is null or ipo_date<=:day)
                  and (delisting_date is null or delisting_date>=:day)
                  and (:all_symbols or symbol=any(cast(:symbols as text[])))
                order by symbol limit 10001"""),
                    {
                        "day": trade_date,
                        "all_symbols": symbols is None,
                        "symbols": list(symbols or ()),
                    },
                )
                .mappings()
                .all()
            )
        if not rows or len(rows) > 10_000:
            raise ValueError("exact stock universe must contain 1 to 10000 candidates")
        if any(
            r["ipo_date"] is None or (r["status"] != "listed" and r["delisting_date"] is None)
            for r in rows
        ):
            raise ValueError("stock lifecycle is unknown")
        result = tuple(r["symbol"] for r in rows)
        if symbols is not None:
            if set(symbols) != set(result):
                raise ValueError(
                    "requested symbols must be ordinary stocks active on the exact date"
                )
            return tuple(dict.fromkeys(symbols))
        return result

    @contextmanager
    def build_lock(self, trade_date: date) -> Iterator[None]:
        with self._engine.connect() as c:
            key = f"stock-max-minute-volume-build:{trade_date.isoformat()}"
            acquired = c.execute(
                text("select pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key}
            ).scalar_one()
            if not acquired:
                raise RuntimeError("minute-volume build is already running for this date")
            try:
                yield
            finally:
                c.execute(text("select pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key})

    def record_source(
        self, run: IngestionRun, manifest: RawManifest | None, certificate: dict[str, object] | None
    ) -> None:
        if run.dataset_code.value != "stock_minute_volume" or run.provider_code.value != "pytdx":
            raise ValueError("minute source requires its own pytdx dataset")
        if manifest is not None and (
            manifest.ingestion_id != run.ingestion_id
            or manifest.schema_version != "pytdx.stock_minute_volume.v1"
        ):
            raise ValueError("minute Raw manifest does not match source ingestion")
        if run.accepted_rows and (
            manifest is None or certificate is None or run.status.value != "succeeded"
        ):
            raise ValueError("accepted minute source requires a successful certified Raw batch")
        with self._engine.begin() as c:
            c.execute(
                INSERT_INGESTION_RUN, {**asdict(run), "request_params": _json(run.request_params)}
            )
            if manifest is not None:
                c.execute(INSERT_RAW_MANIFEST, asdict(manifest))
            if certificate is not None:
                expected = quantity_certificates(c, run, manifest)
                if len(expected) != 1 or expected[0] != certificate:
                    raise ValueError(
                        "minute source certificate does not match validated Raw evidence"
                    )
                write_quantity_certificates(c, expected)

    def publish(
        self, trade_date: date, peaks: tuple[MinuteVolumePeak, ...], evidence: dict[str, object]
    ) -> MinuteVolumeBuildSummary:
        if not 1 <= len(peaks) <= 10_000 or len({p.symbol for p in peaks}) != len(peaks):
            raise ValueError("publication requires 1 to 10000 unique stocks")
        if any(p.trade_date != trade_date for p in peaks):
            raise ValueError("publication requires one exact trade date")
        for peak in peaks:
            if not fullmatch(r"(?:SSE|SZSE|BSE):[0-9]{6}", peak.symbol):
                raise ValueError("publication requires standard stock symbols")
            if peak.expected_count != 240 or not 0 <= peak.observed_count <= 4800:
                raise ValueError("minute peak has invalid sample counts")
            value = peak.max_minute_volume_shares
            if value is None:
                if peak.peak_at is not None or not peak.quality_reasons:
                    raise ValueError("missing minute peak requires reasons and no timestamp")
                continue
            if type(value) is not int or not 0 <= value < 2**63:
                raise ValueError("minute peak must be nonnegative integral bigint shares")
            if peak.observed_count != 240 or peak.quality_reasons:
                raise ValueError("standard minute peak requires 240 complete samples")
            if peak.peak_at is None or peak.peak_at.utcoffset() is None:
                raise ValueError("minute peak requires timezone-aware source time")
            timestamp = peak.peak_at.astimezone(ZoneInfo("Asia/Shanghai"))
            if (
                timestamp.date() != trade_date
                or timestamp.second
                or timestamp.microsecond
                or not (
                    time(9, 31) <= timestamp.time() <= time(11, 30)
                    or time(13, 1) <= timestamp.time() <= time(15)
                )
            ):
                raise ValueError("minute peak requires a valid exact-day source minute label")
        if evidence.get("coverage_scope") not in {"full_market", "requested_symbols"}:
            raise ValueError("publication requires explicit coverage_scope")
        stocks = evidence.get("stocks")
        if not isinstance(stocks, dict) or any(
            not isinstance(stocks.get(p.symbol), dict) for p in peaks
        ):
            raise ValueError("publication requires per-stock input evidence")
        valid = {p.symbol for p in peaks if p.max_minute_volume_shares is not None}
        numerical = {
            "algorithm": ALGORITHM_VERSION,
            "date": trade_date,
            "peaks": [asdict(p) for p in sorted(peaks, key=lambda p: p.symbol)],
        }
        input_hash = sha256(_json(numerical).encode()).hexdigest()
        evidence_hash = sha256(_json(evidence).encode()).hexdigest()
        now = datetime.now(UTC)
        with self._engine.begin() as c:
            c.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key,0))"),
                {"key": f"stock-max-minute-volume-publish:{trade_date}"},
            )
            current = (
                c.execute(
                    text("""select calculation_id,version,evidence_hash
                from derived.stock_max_minute_volume_snapshot where trade_date=:day
                order by version desc limit 1"""),
                    {"day": trade_date},
                )
                .mappings()
                .one_or_none()
            )
            prior_valid = (
                set(
                    c.execute(
                        text("""select symbol from derived.stock_max_minute_volume
                where calculation_id=:id and trade_date=:day
                  and max_minute_volume_shares is not null
                """),
                        {"id": current["calculation_id"], "day": trade_date},
                    ).scalars()
                )
                if current
                else set()
            )
            if not valid or not prior_valid.issubset(valid):
                reason = (
                    "no complete certified minute inputs"
                    if not valid
                    else "publication would shrink prior valid coverage"
                )
                self._insert_run(c, uuid4(), trade_date, input_hash, evidence, now, failed=reason)
                return MinuteVolumeBuildSummary(
                    "partial" if current else "not_ready",
                    trade_date,
                    current["calculation_id"] if current else None,
                    current["version"] if current else None,
                    len(peaks),
                    len(valid),
                )
            self._validate_certificates(c, peaks, stocks)
            calculation_id = c.execute(
                text("""select calculation_id from derived.calculation_run
                where calculation_code=:code and algorithm_version=:algorithm
                  and start_date=:day and end_date=:day and input_hash=:hash and status='succeeded'
                """),
                {
                    "code": CALCULATION_CODE,
                    "algorithm": ALGORITHM_VERSION,
                    "day": trade_date,
                    "hash": input_hash,
                },
            ).scalar_one_or_none()
            if (
                current
                and current["calculation_id"] == calculation_id
                and current["evidence_hash"] == evidence_hash
            ):
                return MinuteVolumeBuildSummary(
                    "unchanged",
                    trade_date,
                    calculation_id,
                    current["version"],
                    len(peaks),
                    len(valid),
                )
            if calculation_id is None:
                calculation_id = uuid4()
                self._insert_run(
                    c, calculation_id, trade_date, input_hash, evidence, now, count=len(peaks)
                )
                c.execute(
                    text("""insert into derived.stock_max_minute_volume
                    (calculation_id,symbol,trade_date,max_minute_volume_shares,peak_at,
                     expected_count,observed_count,quality_reasons,input_evidence)
                    values (:id,:symbol,:trade_date,:max_minute_volume_shares,:peak_at,
                            :expected_count,:observed_count,:quality_reasons,
                            cast(:evidence as jsonb))"""),
                    [
                        {
                            **asdict(p),
                            "id": calculation_id,
                            "quality_reasons": list(p.quality_reasons),
                            "evidence": _json(stocks[p.symbol]),
                        }
                        for p in peaks
                    ],
                )
            version = current["version"] + 1 if current else 1
            c.execute(
                text("""insert into derived.stock_max_minute_volume_snapshot
                (trade_date,version,calculation_id,published_at,candidate_count,complete_count,input_evidence,evidence_hash)
                values (:day,:version,:id,:now,:candidates,:complete,cast(:evidence as jsonb),:hash)
                """),
                {
                    "day": trade_date,
                    "version": version,
                    "id": calculation_id,
                    "now": now,
                    "candidates": len(peaks),
                    "complete": len(valid),
                    "evidence": _json(evidence),
                    "hash": evidence_hash,
                },
            )
            status = (
                "ready"
                if len(valid) == len(peaks) and evidence["coverage_scope"] == "full_market"
                else "partial"
            )
            return MinuteVolumeBuildSummary(
                status, trade_date, calculation_id, version, len(peaks), len(valid)
            )

    @staticmethod
    def _validate_certificates(
        c: Connection, peaks: tuple[MinuteVolumePeak, ...], stocks: dict[str, object]
    ) -> None:
        for peak in peaks:
            if peak.max_minute_volume_shares is None:
                continue
            proof = cast(dict[str, object], stocks[peak.symbol])
            if not all(
                proof.get(k) for k in ("ingestion_id", "evidence_hash", "raw_id", "raw_hash")
            ):
                raise ValueError("complete minute peak requires certified source evidence")
            certified = c.execute(
                text("""select exists (
                select 1 from audit.quantity_unit_certification q
                join ingestion.ingestion_run i using(ingestion_id)
                join ingestion.raw_manifest r using(ingestion_id)
                where q.ingestion_id=:id and q.unit_contract_version='stock-quantity.v1'
                  and q.field_group='stock_minute_volume.volume' and q.status='certified'
                  and q.evidence_hash=:hash and i.dataset_code='stock_minute_volume'
                  and i.provider_code='pytdx' and i.status='succeeded' and i.accepted_rows=240
                  and i.request_params->>'symbol'=:symbol
                  and i.request_params->>'trade_date'=:day
                  and r.raw_id=:raw and r.content_sha256=:raw_hash and r.row_count=240
                  and q.raw_hashes @> cast(:hashes as jsonb)
                  and r.schema_version='pytdx.stock_minute_volume.v1'
                )"""),
                {
                    "id": proof["ingestion_id"],
                    "hash": proof["evidence_hash"],
                    "raw": proof["raw_id"],
                    "raw_hash": proof["raw_hash"],
                    "hashes": _json([proof["raw_hash"]]),
                    "symbol": peak.symbol,
                    "day": peak.trade_date.isoformat(),
                },
            ).scalar_one()
            if not certified:
                raise ValueError("complete minute peak source certification is unavailable")

    @staticmethod
    def _insert_run(
        c: Connection,
        calculation_id: UUID,
        trade_date: date,
        input_hash: str,
        evidence: dict[str, object],
        now: datetime,
        *,
        count: int = 0,
        failed: str | None = None,
    ) -> None:
        c.execute(
            text("""insert into derived.calculation_run
            (calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,
             input_watermark,input_hash,requested_at,calculated_at,finished_at,output_rows,error_summary)
            values (:id,:code,:algorithm,'full',:day,:day,:status,cast(:evidence as jsonb),:hash,
                    :now,:calculated,:now,:count,:error)"""),
            {
                "id": calculation_id,
                "code": CALCULATION_CODE,
                "algorithm": ALGORITHM_VERSION,
                "day": trade_date,
                "status": "failed" if failed else "succeeded",
                "evidence": _json(evidence),
                "hash": input_hash,
                "now": now,
                "calculated": None if failed else now,
                "count": count,
                "error": failed,
            },
        )
