"""Evidence-bound, opt-in repair of stock daily volume; Raw is always immutable.

Preview performs SELECTs and local Raw reads only. The sealed local artifact contains
protected row evidence: store it with the same access controls as an operational backup.
Repair writes use batches of at most 500 facts. Correct-history certification verifies
and locks one complete ingestion group of at most 5000 currently referenced facts.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from hashlib import sha256
from json import dumps
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.ingestion import DatasetCode, RawFileFormat, RawManifest
from market_data_center.domain.records import DailyBarRecord
from market_data_center.providers.akshare import normalize_akshare_raw
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx import normalize_pytdx_raw
from market_data_center.providers.quantity_units import (
    NORMALIZER_VERSION,
    quantity_unit_metadata,
    quantity_unit_rule,
    validate_quantity_metadata,
)
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

CONTRACT_VERSION = "stock-quantity.v1"
FIELD_GROUP = "daily_bar.volume"
CERTIFIED_FIELD_GROUPS = (FIELD_GROUP, "daily_bar.amount")
RULE_VERSION = "stock-daily-volume-repair.v1"
MAX_CERTIFICATION_ROWS = 5000
SCHEMAS = {
    "pytdx": frozenset(
        {
            "pytdx.local_daily_bar.v1",
            "pytdx.local_daily_bar.v2",
            "pytdx.local_daily_bar.v3",
            "pytdx.remote_daily_bar.v1",
            "pytdx.remote_daily_bar.v2",
        }
    ),
    "akshare": frozenset({"akshare.daily_bar.v1", "akshare.daily_bar.v2"}),
}
LEGACY_LOTS = frozenset({"pytdx.remote_daily_bar.v1", "akshare.daily_bar.v1"})
FACT_FIELDS = (
    "symbol",
    "trade_date",
    "market",
    "open",
    "high",
    "low",
    "close",
    "previous_close",
    "volume",
    "amount",
    "trade_status",
    "is_st",
    "source_code",
)
type NormalizationCache = dict[str, tuple[tuple[Mapping[str, str], ...], tuple[Any, ...]]]


@dataclass(frozen=True)
class RepairScope:
    start_date: date
    end_date: date
    providers: tuple[str, ...]
    symbols: tuple[str, ...] = ()
    max_rows: int = 50_000

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not exceed end_date")
        if not self.providers or any(provider not in SCHEMAS for provider in self.providers):
            raise ValueError("explicit supported stock providers are required")
        if not 1 <= self.max_rows <= 1_000_000:
            raise ValueError("max_rows must be between 1 and 1000000")


def canonical(value: Any) -> Any:
    """Serialize exact values without ever routing a market number through float."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite evidence")
        rendered = format(value, "f")
        return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, (date, UUID)):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): canonical(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [canonical(item) for item in value]
    if isinstance(value, float):
        raise ValueError("floating-point evidence is not accepted")
    return value


def fingerprint(value: Mapping[str, Any]) -> str:
    return sha256(
        dumps(canonical(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def seal_plan(
    scope: RepairScope,
    items: list[dict[str, Any]],
    dependencies: list[dict[str, Any]],
    *,
    database_identity_hash: str | None = None,
) -> dict[str, Any]:
    plan = cast(
        dict[str, Any],
        canonical(
            {
                "rule_version": RULE_VERSION,
                "unit_contract_version": CONTRACT_VERSION,
                "field_group": FIELD_GROUP,
                "normalizer_version": NORMALIZER_VERSION,
                "scope": asdict(scope),
                "items": items,
                "dependencies": dependencies,
                "database_identity_hash": database_identity_hash,
            }
        ),
    )
    plan["plan_hash"] = fingerprint(plan)
    return plan


def verify_plan(plan: Mapping[str, Any], expected_hash: str) -> None:
    unsigned = {key: value for key, value in plan.items() if key != "plan_hash"}
    if plan.get("plan_hash") != expected_hash or fingerprint(unsigned) != expected_hash:
        raise ValueError("preview plan hash does not match approval hash")
    if (
        plan.get("rule_version") != RULE_VERSION
        or plan.get("unit_contract_version") != CONTRACT_VERSION
        or plan.get("field_group") != FIELD_GROUP
        or plan.get("normalizer_version") != NORMALIZER_VERSION
    ):
        raise ValueError("unsupported repair plan version or scope")


def evaluate_fact(
    fact: Mapping[str, Any],
    security_type: str,
    source: Mapping[str, Any],
    manifests: Sequence[RawManifest],
    raw_store: LocalRawStore,
    *,
    normalization_cache: NormalizationCache | None = None,
) -> dict[str, Any]:
    """Prove one natural key against exactly one original, verified Raw object."""
    item: dict[str, Any] = canonical(
        {
            "symbol": fact["symbol"],
            "trade_date": fact["trade_date"],
            "original_ingestion_id": fact["ingestion_id"],
            "before": dict(fact),
            "before_hash": fingerprint(fact),
            "status": "quarantined",
            "source_evidence_hash": fingerprint(source),
            "raw_candidates": [
                {
                    "raw_id": manifest.raw_id,
                    "ingestion_id": manifest.ingestion_id,
                    "schema_version": manifest.schema_version,
                    "content_sha256": manifest.content_sha256,
                    "byte_size": manifest.byte_size,
                    "row_count": manifest.row_count,
                }
                for manifest in manifests
            ],
        }
    )

    def quarantine(reason: str) -> dict[str, Any]:
        return {**item, "reason": reason}

    if security_type != "stock":
        return quarantine("not_stock")
    provider = str(source.get("provider_code", ""))
    if provider != fact["source_code"] or source.get("dataset_code") != "daily_bar":
        return quarantine("lineage_mismatch")
    if not manifests:
        return quarantine("missing_raw")
    if len({manifest.schema_version for manifest in manifests}) != 1:
        return quarantine("mixed_raw_schema")
    matches: list[tuple[RawManifest, DailyBarRecord, int | None]] = []
    for manifest in manifests:
        if manifest.schema_version not in SCHEMAS.get(provider, ()):
            return quarantine("unregistered_unit_evidence")
        if str(manifest.ingestion_id) != str(source.get("source_ingestion_id")):
            return quarantine("lineage_mismatch")
        try:
            endpoint = (
                "stock_zh_a_hist"
                if provider == "akshare"
                else "get_security_bars"
                if ".remote_" in manifest.schema_version
                else "local_day_file"
            )
            rule = quantity_unit_rule(
                provider=provider,
                endpoint=endpoint,
                schema_version=manifest.schema_version,
                instrument_type="stock",
                field="成交量" if provider == "akshare" else "volume",
            )
            try:
                validate_quantity_metadata(
                    provider=provider,
                    schema_version=manifest.schema_version,
                    request_params=source["request_params"],
                    replay=True,
                )
            except ProviderError:
                return quarantine("contradictory_unit_evidence")
            cache_key = fingerprint({"manifest": asdict(manifest), "source": source})
            cached = normalization_cache.get(cache_key) if normalization_cache is not None else None
            if cached is None:
                rows = raw_store.read_jsonl(manifest)
                normalizer = normalize_pytdx_raw if provider == "pytdx" else normalize_akshare_raw
                records = normalizer(
                    DatasetCode.DAILY_BAR, manifest.schema_version, rows, source["request_params"]
                )
                if normalization_cache is not None:
                    normalization_cache[cache_key] = rows, records
            else:
                rows, records = cached
            raw_date_key = "日期" if provider == "akshare" else "date"
            raw_dates = [
                row.get(raw_date_key, "").replace("-", "")
                for row in rows
                if provider != "akshare" or row.get("股票代码") == fact["symbol"].split(":")[1]
            ]
            if raw_dates.count(str(fact["trade_date"]).replace("-", "")) > 1:
                return quarantine("duplicate_raw_natural_key")
            keyed = [
                record
                for record in records
                if isinstance(record, DailyBarRecord)
                and record.symbol == fact["symbol"]
                and str(record.trade_date) == str(fact["trade_date"])
            ]
            if len(keyed) > 1:
                return quarantine("duplicate_raw_natural_key")
            if keyed:
                record = keyed[0]
                target = record.volume
                # A known legacy mapper emitted source lots unchanged. This is evidence
                # of the previous mapping, never an inference from the current value.
                legacy: int | None = target
                if manifest.schema_version in LEGACY_LOTS and target is not None:
                    legacy = target // 100 if target % 100 == 0 else None
                matches.append((manifest, record, legacy))
        except RawIntegrityError:
            return quarantine("raw_integrity_failed")
        except (ValueError, KeyError, ArithmeticError, RuntimeError):
            return quarantine("raw_normalization_failed")
    if len(matches) != 1:
        return quarantine("ambiguous_raw" if matches else "raw_key_missing")
    manifest, record, legacy = matches[0]
    normalized = {field: getattr(record, field) for field in FACT_FIELDS}
    # Match SQL numeric precision without changing the exact integer volume target.
    with localcontext() as context:
        context.prec = 80
        for field in ("open", "high", "low", "close", "previous_close", "amount"):
            if normalized[field] is not None:
                places = Decimal("0.01") if field == "amount" else Decimal("0.0001")
                normalized[field] = normalized[field].quantize(places, rounding=ROUND_HALF_UP)
    projection = canonical(normalized)
    before_projection = canonical({field: fact[field] for field in FACT_FIELDS})
    item.update(
        raw_id=str(manifest.raw_id),
        raw_hash=manifest.content_sha256,
        schema_version=manifest.schema_version,
        provider_code=provider,
        source_ingestion_id=str(manifest.ingestion_id),
        request_params=canonical(source["request_params"]),
        target_volume=record.volume,
        normalizer_version=NORMALIZER_VERSION,
        endpoint=endpoint,
        unit_evidence=rule.evidence,
        source_quantity_unit=rule.source_unit,
    )
    if any(
        projection[field] != before_projection[field] for field in FACT_FIELDS if field != "volume"
    ):
        return quarantine("current_fact_does_not_match_raw")
    if fact["volume"] == record.volume:
        item.update(status="already_correct", reason="verified_raw_target")
    elif manifest.schema_version in LEGACY_LOTS and legacy == fact["volume"]:
        item.update(status="repair", reason="legacy_lots_mapping")
    else:
        return quarantine("current_fact_does_not_match_raw")
    item["item_key"] = fingerprint(
        {
            key: item[key]
            for key in ("symbol", "trade_date", "original_ingestion_id", "raw_hash", "before_hash")
        }
        | {"rule": RULE_VERSION}
    )
    return item


def _lineage(connection: Connection, ingestion_id: str) -> tuple[dict[str, Any], list[RawManifest]]:
    run = (
        connection.execute(
            text("""
        select provider_code, dataset_code, replayed_from_raw_id, request_params
        from ingestion.ingestion_run where ingestion_id = :id
    """),
            {"id": ingestion_id},
        )
        .mappings()
        .one()
    )
    manifest_filter = (
        "m.raw_id = :manifest_key"
        if run["replayed_from_raw_id"]
        else "m.ingestion_id = :manifest_key"
    )
    rows = (
        connection.execute(
            text(
                """
        select m.*, r.provider_code, r.dataset_code, r.request_params
        from ingestion.raw_manifest m
        join ingestion.ingestion_run r on r.ingestion_id = m.ingestion_id
        where """
                + manifest_filter
                + """
        order by m.raw_id
    """
            ),
            {"manifest_key": run["replayed_from_raw_id"] or ingestion_id},
        )
        .mappings()
        .all()
    )
    manifests = [
        RawManifest(
            raw_id=row["raw_id"],
            ingestion_id=row["ingestion_id"],
            object_path=row["object_path"],
            file_format=RawFileFormat(row["file_format"]),
            content_sha256=row["content_sha256"],
            byte_size=row["byte_size"],
            row_count=row["row_count"],
            schema_version=row["schema_version"],
            storage_backend=row["storage_backend"],
        )
        for row in rows
    ]
    source = dict(run)
    source["source_ingestion_id"] = rows[0]["ingestion_id"] if rows else ingestion_id
    if rows:
        if any(
            row["provider_code"] != run["provider_code"]
            or row["dataset_code"] != run["dataset_code"]
            for row in rows
        ):
            source["dataset_code"] = "lineage_mismatch"
        source["request_params"] = rows[0]["request_params"]
    return source, manifests


def _current(
    connection: Connection, item: Mapping[str, Any], *, lock: bool = False
) -> Mapping[str, Any] | None:
    row = (
        connection.execute(
            text(
                """
        select b.* from core.daily_bar b
        where symbol = :symbol and trade_date = :trade_date
    """
                + (" for update" if lock else "")
            ),
            {"symbol": item["symbol"], "trade_date": date.fromisoformat(str(item["trade_date"]))},
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row is not None else None


def _database_identity(connection: Connection) -> str:
    row = (
        connection.execute(
            text("""
        select current_database() as database_name, inet_server_addr()::text as server_address,
               inet_server_port() as server_port,
               (select oid::text from pg_database where datname=current_database()) as database_oid
    """)
        )
        .mappings()
        .one()
    )
    return fingerprint(dict(row))


def _verify_database(engine: Engine, plan: Mapping[str, Any]) -> None:
    with engine.connect() as connection, connection.begin():
        connection.execute(text("set transaction read only"))
        connection.execute(text("set local statement_timeout = '25s'"))
        if _database_identity(connection) != plan.get("database_identity_hash"):
            raise ValueError("preview target database identity does not match")


def preview(
    engine: Engine, raw_store: LocalRawStore, scope: RepairScope, *, page_size: int = 500
) -> dict[str, Any]:
    """Bounded keyset scan; each page is an explicit short read-only transaction."""
    if not 1 <= page_size <= 500:
        raise ValueError("page_size must be between 1 and 500")
    items: list[dict[str, Any]] = []
    identity_hash: str | None = None
    cursor_date, cursor_symbol = scope.start_date, ""
    counts: dict[str, int] = {}
    while True:
        normalization_cache: NormalizationCache = {}
        with engine.connect() as connection, connection.begin():
            connection.execute(text("set transaction read only"))
            connection.execute(text("set local statement_timeout = '25s'"))
            if identity_hash is None:
                identity_hash = _database_identity(connection)
            elif identity_hash != _database_identity(connection):
                raise ValueError("database identity changed during preview")
            rows = (
                connection.execute(
                    text("""
                select b.*, s.security_type
                from core.daily_bar b join core.security s using (symbol)
                where b.trade_date between :start and :end and s.security_type = 'stock'
                  and b.source_code = any(cast(:providers as text[]))
                  and (:all_symbols or b.symbol = any(cast(:symbols as text[])))
                  and (b.trade_date, b.symbol) > (:cursor_date, :cursor_symbol)
                order by b.trade_date, b.symbol limit :page_size
            """),
                    {
                        "start": scope.start_date,
                        "end": scope.end_date,
                        "providers": list(scope.providers),
                        "symbols": list(scope.symbols),
                        "all_symbols": not scope.symbols,
                        "cursor_date": cursor_date,
                        "cursor_symbol": cursor_symbol,
                        "page_size": page_size,
                    },
                )
                .mappings()
                .all()
            )
            if not rows:
                break
            if len(items) + len(rows) > scope.max_rows:
                raise ValueError("preview exceeds max_rows; narrow the explicit scope")
            for row in rows:
                fact = {key: value for key, value in row.items() if key != "security_type"}
                ingestion_id = str(fact["ingestion_id"])
                source, manifests = _lineage(connection, ingestion_id)
                item = evaluate_fact(
                    fact,
                    row["security_type"],
                    source,
                    manifests,
                    raw_store,
                    normalization_cache=normalization_cache,
                )
                if ingestion_id not in counts:
                    counts[ingestion_id] = connection.execute(
                        text("""
                        select count(*) from core.daily_bar where ingestion_id = :id
                    """),
                        {"id": ingestion_id},
                    ).scalar_one()
                item["ingestion_current_row_count"] = counts[ingestion_id]
                items.append(item)
            cursor_date, cursor_symbol = rows[-1]["trade_date"], rows[-1]["symbol"]
    dependencies = dependency_report(engine, items)
    return seal_plan(scope, items, dependencies, database_identity_hash=identity_hash)


def dependency_report(engine: Engine, items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Report immutable snapshot replacements and derived rebuilds; never mutate them."""
    affected = [item for item in items if item["status"] == "repair"]
    if not affected:
        return []
    result: list[dict[str, Any]] = []
    with engine.connect() as connection, connection.begin():
        connection.execute(text("set transaction read only"))
        connection.execute(text("set local statement_timeout = '25s'"))
        # Exact fact and lineage join avoids counting unrelated rows in a batch.
        for offset in range(0, len(affected), 500):
            keys = [
                {key: item[key] for key in ("symbol", "trade_date", "original_ingestion_id")}
                for item in affected[offset : offset + 500]
            ]
            members = (
                connection.execute(
                    text("""
                with keys as (
                    select * from jsonb_to_recordset(cast(:keys as jsonb))
                    as k(symbol text, trade_date date, original_ingestion_id uuid)
                )
                select distinct 'today_limit_up' as domain, s.snapshot_id,
                    s.trade_date, s.input_hash, s.calculation_id
                from today_limit_up.member m join today_limit_up.snapshot s using (snapshot_id)
                join keys k on k.symbol = m.symbol and k.trade_date = s.trade_date
                    and k.original_ingestion_id = m.daily_bar_ingestion_id
                union
                select distinct 'today_limit_down' as domain, s.snapshot_id,
                    s.trade_date, s.input_hash, s.calculation_id
                from today_limit_down.member m join today_limit_down.snapshot s using (snapshot_id)
                join keys k on k.symbol = m.symbol and k.trade_date = s.trade_date
                    and k.original_ingestion_id = m.daily_bar_ingestion_id
                order by domain, trade_date, snapshot_id
            """),
                    {"keys": dumps(keys)},
                )
                .mappings()
                .all()
            )
            result.extend(
                canonical(
                    {
                        **dict(row),
                        "action": "publish_replacement_snapshot_after_input_verification",
                    }
                )
                for row in members
            )
    result.append(
        {
            "domain": "classification_daily_metric",
            "action": "review_and_recalculate_exact_dates",
            "trade_dates": sorted({str(item["trade_date"]) for item in affected}),
        }
    )
    result.append(
        {
            "domain": "price_only_derivations",
            "action": "quantity_change_does_not_require_price_rebuild",
        }
    )
    # The same snapshot may depend on keys from different pages.
    return sorted({fingerprint(row): row for row in result}.values(), key=fingerprint)


def _certificate(
    connection: Connection,
    ingestion_id: str,
    raw_hashes: Sequence[str],
    evidence: Mapping[str, Any],
    *,
    status: str = "certified",
    preserve_certified: bool = False,
) -> None:
    for field_group in CERTIFIED_FIELD_GROUPS:
        connection.execute(
            text("""
        insert into audit.quantity_unit_certification
            (ingestion_id, unit_contract_version, field_group, status, raw_hashes,
             normalizer_version, evidence_hash)
        values (:id, :contract, :field_group, :status,
                cast(:hashes as jsonb), :normalizer, :evidence)
        on conflict (ingestion_id, unit_contract_version, field_group) do update
        set status = excluded.status, raw_hashes = excluded.raw_hashes,
            normalizer_version = excluded.normalizer_version,
            evidence_hash = excluded.evidence_hash, certified_at = now()
        where not :preserve_certified or audit.quantity_unit_certification.status <> 'certified'
    """),
            {
                "id": ingestion_id,
                "contract": CONTRACT_VERSION,
                "field_group": field_group,
                "status": status,
                "hashes": dumps(sorted(set(raw_hashes))),
                "normalizer": NORMALIZER_VERSION,
                "evidence": fingerprint({**evidence, "field_group": field_group}),
                "preserve_certified": preserve_certified,
            },
        )


def _recheck(
    connection: Connection,
    raw_store: LocalRawStore,
    item: Mapping[str, Any],
    current: Mapping[str, Any],
    normalization_cache: NormalizationCache | None = None,
) -> bool:
    security_type = connection.execute(
        text("select security_type from core.security where symbol=:s"), {"s": item["symbol"]}
    ).scalar_one()
    source, manifests = _lineage(connection, str(current["ingestion_id"]))
    checked = evaluate_fact(
        current,
        security_type,
        source,
        manifests,
        raw_store,
        normalization_cache=normalization_cache,
    )
    return all(
        checked.get(key) == item.get(key)
        for key in (
            "before_hash",
            "raw_id",
            "raw_hash",
            "schema_version",
            "target_volume",
            "status",
            "normalizer_version",
            "request_params",
            "source_ingestion_id",
            "endpoint",
            "unit_evidence",
            "source_quantity_unit",
            "source_evidence_hash",
            "raw_candidates",
        )
    )


def apply_plan(
    engine: Engine,
    raw_store: LocalRawStore,
    plan: Mapping[str, Any],
    expected_hash: str,
    *,
    batch_size: int = 500,
) -> dict[str, int]:
    """Resume the exact approved plan. Each batch, facts and audit commit atomically."""
    verify_plan(plan, expected_hash)
    if not 1 <= batch_size <= 500:
        raise ValueError("batch_size must be between 1 and 500")
    _verify_database(engine, plan)
    run_id = str(uuid5(NAMESPACE_URL, "quantity-repair:" + expected_hash))
    actions = [("repair", [item]) for item in plan["items"] if item["status"] == "repair"]
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in plan["items"]:
        grouped.setdefault(item["original_ingestion_id"], []).append(item)
    actions.extend(("certify", group) for group in grouped.values())
    with engine.begin() as connection:
        connection.execute(
            text("""
            insert into audit.quantity_unit_repair_run(repair_run_id, plan_hash, rule_version, plan)
            values (:id, :hash, :rule, cast(:plan as jsonb)) on conflict (plan_hash) do nothing
        """),
            {
                "id": run_id,
                "hash": expected_hash,
                "rule": RULE_VERSION,
                "plan": dumps(canonical(plan)),
            },
        )
    while True:
        with engine.begin() as connection:
            connection.execute(text("set local statement_timeout = '25s'"))
            connection.execute(text("set local lock_timeout = '3s'"))
            run = (
                connection.execute(
                    text("""
                select * from audit.quantity_unit_repair_run where repair_run_id=:id for update
            """),
                    {"id": run_id},
                )
                .mappings()
                .one()
            )
            if run["plan"] != canonical(plan):
                raise ValueError("durable plan differs from approved preview")
            if run["status"] == "rolled_back":
                raise ValueError("a rolled-back plan must be previewed again")
            start = run["next_item"]
            pending: list[tuple[str, list[Mapping[str, Any]]]] = []
            locked_rows = 0
            for action in actions[start:]:
                cost = min(len(action[1]), 500) if action[0] == "certify" else 1
                if pending and locked_rows + cost > batch_size:
                    break
                pending.append(action)
                locked_rows += cost
            if not pending:
                conflicts = connection.execute(
                    text("""
                    select count(*) from audit.quantity_unit_repair_item
                    where repair_run_id=:id and status in ('conflict','rollback_conflict')
                """),
                    {"id": run_id},
                ).scalar_one()
                connection.execute(
                    text("""
                    update audit.quantity_unit_repair_run set status=:status, updated_at=now()
                    where repair_run_id=:id
                """),
                    {"id": run_id, "status": "conflict" if conflicts else "completed"},
                )
                break
            normalization_cache: NormalizationCache = {}
            for kind, group in pending:
                if kind == "repair":
                    _apply_item(connection, raw_store, run_id, group[0], normalization_cache)
                else:
                    _certify_existing_group(connection, raw_store, plan["plan_hash"], group)
            connection.execute(
                text("""
                update audit.quantity_unit_repair_run set next_item=:next, updated_at=now()
                where repair_run_id=:id
            """),
                {"id": run_id, "next": start + len(pending)},
            )
    return repair_status(engine, run_id)


def _apply_item(
    connection: Connection,
    raw_store: LocalRawStore,
    run_id: str,
    item: Mapping[str, Any],
    normalization_cache: NormalizationCache | None = None,
) -> None:
    current = _current(connection, item, lock=True)
    previous = (
        connection.execute(
            text("""
        select * from audit.quantity_unit_repair_item
        where item_key=:key and status in ('applied','already_applied')
        order by created_at limit 1
    """),
            {"key": item["item_key"]},
        )
        .mappings()
        .first()
    )
    already_applied = (
        previous is not None
        and current is not None
        and fingerprint(current) == previous["after_hash"]
        and current["ingestion_id"] == previous["new_ingestion_id"]
    )
    matched = (
        current is not None
        and fingerprint(current) == item["before_hash"]
        and str(current["ingestion_id"]) == item["original_ingestion_id"]
    )
    if matched and current is not None:
        matched = _recheck(connection, raw_store, item, current, normalization_cache)
    new_id: str | None = None
    after: Mapping[str, Any] | None = None
    if already_applied and previous is not None:
        new_id = str(previous["new_ingestion_id"])
        after = dict(previous["after_state"])
    if matched:
        new_id = str(uuid5(UUID(run_id), item["item_key"]))
        params = {
            **item["request_params"],
            **quantity_unit_metadata(source_unit=item["source_quantity_unit"]),
            "quantity_repair_plan": run_id,
        }
        connection.execute(
            text("""
            insert into ingestion.ingestion_run
                (ingestion_id, provider_code, dataset_code, status, requested_at, started_at,
                 finished_at, request_params, fetched_rows, accepted_rows, replayed_from_raw_id)
            values (:id, :provider, 'daily_bar', 'succeeded', now(), now(), now(),
                    cast(:params as jsonb), 1, 1, :raw_id)
        """),
            {
                "id": new_id,
                "provider": item["provider_code"],
                "params": dumps(params),
                "raw_id": item["raw_id"],
            },
        )
        # The row is locked and its complete fingerprint was compared above. The
        # explicit lineage/value predicate is an additional compare-and-set guard.
        after = dict(
            connection.execute(
                text("""
            update core.daily_bar set volume=:target, ingestion_id=:new_id
            where symbol=:symbol and trade_date=:trade_date and ingestion_id=:old_id
                and volume is not distinct from :before_volume
            returning *
        """),
                {
                    "target": item["target_volume"],
                    "new_id": new_id,
                    "symbol": item["symbol"],
                    "trade_date": date.fromisoformat(item["trade_date"]),
                    "old_id": item["original_ingestion_id"],
                    "before_volume": item["before"]["volume"],
                },
            )
            .mappings()
            .one()
        )
        _certificate(
            connection,
            new_id,
            [item["raw_hash"]],
            {"plan": run_id, "item": item["item_key"], "after_hash": fingerprint(after)},
        )
    connection.execute(
        text("""
        insert into audit.quantity_unit_repair_item
            (repair_run_id,item_key,rule_version,symbol,trade_date,original_ingestion_id,
             raw_id,raw_hash,before_state,before_hash,after_state,after_hash,new_ingestion_id,status,reason)
        values (:run,:key,:rule,:symbol,:trade_date,:old,:raw_id,:raw_hash,cast(:before as jsonb),
                :before_hash,cast(:after as jsonb),:after_hash,:new_id,:status,:reason)
    """),
        {
            "run": run_id,
            "key": item["item_key"],
            "rule": RULE_VERSION,
            "symbol": item["symbol"],
            "trade_date": date.fromisoformat(item["trade_date"]),
            "old": item["original_ingestion_id"],
            "raw_id": item["raw_id"],
            "raw_hash": item["raw_hash"],
            "before": dumps(item["before"]),
            "before_hash": item["before_hash"],
            "after": dumps(canonical(after)) if after else None,
            "after_hash": fingerprint(after) if after else None,
            "new_id": new_id,
            "status": "already_applied"
            if already_applied
            else "applied"
            if matched
            else "conflict",
            "reason": None if matched or already_applied else "fact_or_raw_changed_since_preview",
        },
    )


def _certify_existing_group(
    connection: Connection,
    raw_store: LocalRawStore,
    plan_hash: str,
    items: Sequence[Mapping[str, Any]],
) -> None:
    ingestion_id = items[0]["original_ingestion_id"]
    hashes = [item["raw_hash"] for item in items if "raw_hash" in item]
    evidence = {"plan": plan_hash, "facts": sorted(item["before_hash"] for item in items)}
    incomplete_scope = (
        len(items) != items[0]["ingestion_current_row_count"] or len(items) > MAX_CERTIFICATION_ROWS
    )
    complete = certification_eligible(items)
    if complete:
        normalization_cache: NormalizationCache = {}
        # Lock every currently referenced fact, including out-of-scope symbols, so
        # a partial date/symbol preview cannot certify the rest of an ingestion.
        current = (
            connection.execute(
                text("""
            select * from core.daily_bar where ingestion_id=:id
            order by symbol, trade_date limit :bound for update
        """),
                {"id": ingestion_id, "bound": MAX_CERTIFICATION_ROWS + 1},
            )
            .mappings()
            .all()
        )
        by_key = {(item["symbol"], item["trade_date"]): item for item in items}
        incomplete_scope = len(current) != len(items)
        complete = len(current) == len(items) and all(
            (str(row["symbol"]), str(row["trade_date"])) in by_key
            and _recheck(
                connection,
                raw_store,
                by_key[(str(row["symbol"]), str(row["trade_date"]))],
                dict(row),
                normalization_cache,
            )
            for row in current
        )
    _certificate(
        connection,
        ingestion_id,
        hashes,
        evidence,
        status="certified" if complete else "quarantined",
        # An incomplete preview has no evidence about sibling facts. It may
        # record missing certification, but cannot revoke a complete prior proof.
        preserve_certified=incomplete_scope,
    )


def certification_eligible(items: Sequence[Mapping[str, Any]]) -> bool:
    return bool(items) and (
        all(item["status"] == "already_correct" for item in items)
        and len(items) == items[0]["ingestion_current_row_count"]
        and len(items) <= MAX_CERTIFICATION_ROWS
    )


def repair_status(engine: Engine, run_id: str) -> dict[str, int]:
    with engine.connect() as connection:
        rows = connection.execute(
            text("""
            select status,count(*) from audit.quantity_unit_repair_item
            where repair_run_id=:id group by status order by status
        """),
            {"id": run_id},
        ).all()
        return {row[0]: row[1] for row in rows}


def certification_status(engine: Engine, plan: Mapping[str, Any]) -> dict[str, int]:
    """Current field-group certificate counts for this plan's old and new lineage."""
    run_id = str(uuid5(NAMESPACE_URL, "quantity-repair:" + plan["plan_hash"]))
    origins = sorted({item["original_ingestion_id"] for item in plan["items"]})
    with engine.connect() as connection, connection.begin():
        connection.execute(text("set transaction read only"))
        connection.execute(text("set local statement_timeout = '25s'"))
        rows = connection.execute(
            text("""
            with ids as (
                select unnest(cast(:origins as uuid[])) as ingestion_id
                union
                select new_ingestion_id from audit.quantity_unit_repair_item
                where repair_run_id=:id and new_ingestion_id is not null
            )
            select c.status,count(*) from audit.quantity_unit_certification c
            join ids using (ingestion_id)
            where c.unit_contract_version=:contract
                and c.field_group=any(cast(:groups as text[]))
            group by c.status order by c.status
        """),
            {
                "origins": origins,
                "id": run_id,
                "contract": CONTRACT_VERSION,
                "groups": list(CERTIFIED_FIELD_GROUPS),
            },
        ).all()
        return {row[0]: row[1] for row in rows}


def rollback_plan(
    engine: Engine, plan: Mapping[str, Any], expected_hash: str, *, batch_size: int = 500
) -> dict[str, int]:
    """Restore volume and original lineage only while the exact after state matches."""
    verify_plan(plan, expected_hash)
    if not 1 <= batch_size <= 500:
        raise ValueError("batch_size must be between 1 and 500")
    _verify_database(engine, plan)
    run_id = str(uuid5(NAMESPACE_URL, "quantity-repair:" + expected_hash))
    while True:
        with engine.begin() as connection:
            connection.execute(text("set local statement_timeout = '25s'"))
            connection.execute(text("set local lock_timeout = '3s'"))
            run = (
                connection.execute(
                    text("""
                select plan from audit.quantity_unit_repair_run where repair_run_id=:id for update
            """),
                    {"id": run_id},
                )
                .mappings()
                .one()
            )
            if run["plan"] != canonical(plan):
                raise ValueError("durable plan differs from approved preview")
            rows = (
                connection.execute(
                    text("""
                select * from audit.quantity_unit_repair_item
                where repair_run_id=:id and status='applied' order by item_key
                limit :size for update
            """),
                    {"id": run_id, "size": batch_size},
                )
                .mappings()
                .all()
            )
            if not rows:
                conflict = connection.execute(
                    text("""
                    select exists(select 1 from audit.quantity_unit_repair_item
                        where repair_run_id=:id and status in ('conflict','rollback_conflict'))
                """),
                    {"id": run_id},
                ).scalar_one()
                connection.execute(
                    text("""
                    update audit.quantity_unit_repair_run set status=:status,updated_at=now()
                    where repair_run_id=:id
                """),
                    {"id": run_id, "status": "conflict" if conflict else "rolled_back"},
                )
                break
            for item in rows:
                current = _current(connection, dict(item), lock=True)
                matches = (
                    current is not None
                    and fingerprint(current) == item["after_hash"]
                    and current["ingestion_id"] == item["new_ingestion_id"]
                )
                if matches:
                    connection.execute(
                        text("""
                        update core.daily_bar set volume=:volume,ingestion_id=:old
                        where symbol=:symbol and trade_date=:trade_date and ingestion_id=:new
                    """),
                        {
                            "volume": item["before_state"]["volume"],
                            "old": item["original_ingestion_id"],
                            "new": item["new_ingestion_id"],
                            "symbol": item["symbol"],
                            "trade_date": item["trade_date"],
                        },
                    )
                    _certificate(
                        connection,
                        str(item["original_ingestion_id"]),
                        [item["raw_hash"]],
                        {"rollback_plan": expected_hash},
                        status="quarantined",
                    )
                    _certificate(
                        connection,
                        str(item["new_ingestion_id"]),
                        [item["raw_hash"]],
                        {"rollback_plan": expected_hash},
                        status="quarantined",
                    )
                connection.execute(
                    text("""
                    update audit.quantity_unit_repair_item
                    set status=:status,reason=:reason,updated_at=now()
                    where repair_run_id=:id and item_key=:key
                """),
                    {
                        "id": run_id,
                        "key": item["item_key"],
                        "status": "rolled_back" if matches else "rollback_conflict",
                        "reason": None if matches else "after_fingerprint_or_lineage_changed",
                    },
                )
    return repair_status(engine, run_id)


def summary(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Share counts and hash without printing protected per-row facts or Raw paths."""
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in plan["items"]:
        grouped.setdefault(item["original_ingestion_id"], []).append(item)
    eligible = sum(certification_eligible(group) for group in grouped.values())
    return {
        "plan_hash": plan["plan_hash"],
        "scope": plan["scope"],
        "counts": dict(Counter(item["status"] for item in plan["items"])),
        "reasons": dict(Counter(item["reason"] for item in plan["items"])),
        "affected_dependencies": len(plan["dependencies"]),
        "existing_ingestion_certification_candidates": {
            "eligible": eligible,
            "quarantined": len(grouped) - eligible,
        },
        "max_certification_rows_per_ingestion": MAX_CERTIFICATION_ROWS,
    }
