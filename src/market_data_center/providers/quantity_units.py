"""Exact, source-specific quantity contracts at the provider boundary (ADR-0068)."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType

from market_data_center.providers.contracts import ProviderError

UNIT_CONTRACT_VERSION = "stock-quantity.v1"
NORMALIZER_VERSION = "quantity-normalizer.v1"
BIGINT_MAX = 9_223_372_036_854_775_807


@dataclass(frozen=True)
class QuantityUnitRule:
    source_unit: str | None
    evidence: str


type QuantityIdentity = tuple[str, str, str, str, str]


def _rules() -> dict[QuantityIdentity, QuantityUnitRule]:
    rules: dict[QuantityIdentity, QuantityUnitRule] = {}

    def add(
        provider: str,
        endpoint: str,
        schemas: tuple[str, ...],
        fields: tuple[str, ...],
        unit: str | None,
        evidence: str,
        instrument: str = "stock",
    ) -> None:
        for schema in schemas:
            for field in fields:
                rules[provider, endpoint, schema, instrument, field] = QuantityUnitRule(
                    unit, evidence
                )

    add(
        "akshare",
        "stock_zh_a_hist",
        ("akshare.daily_bar.v1", "akshare.daily_bar.v2"),
        ("成交量",),
        "lot",
        "AKShare stock_zh_a_hist field table: volume in hands, amount CNY",
    )
    add(
        "pytdx",
        "get_security_bars",
        ("pytdx.remote_daily_bar.v1", "pytdx.remote_daily_bar.v2"),
        ("volume",),
        "lot",
        "ADR-0024/Issue90 exact daily bar wire decoder: stock lots of 100",
    )
    add(
        "pytdx",
        "local_day_file",
        ("pytdx.local_daily_bar.v1", "pytdx.local_daily_bar.v2", "pytdx.local_daily_bar.v3"),
        ("volume",),
        "share",
        "TDX .day uint32 volume is shares; v3 preserves binary32 amount exactly",
    )
    add(
        "baostock",
        "query_history_k_data_plus",
        ("baostock.daily_bar.v1",),
        ("volume",),
        "share",
        "BaoStock daily stock field table: volume in shares",
    )
    add(
        "tushare",
        "daily",
        ("tushare.daily_bar.v1",),
        ("vol",),
        "lot",
        "Tushare daily field table: volume hands, amount thousand CNY",
    )
    add(
        "akshare",
        "stock_zh_a_hist",
        ("akshare.daily_bar.v1", "akshare.daily_bar.v2"),
        ("成交额",),
        "cny",
        "AKShare stock_zh_a_hist field table: turnover amount in CNY",
    )
    add(
        "pytdx",
        "get_security_bars",
        ("pytdx.remote_daily_bar.v1", "pytdx.remote_daily_bar.v2"),
        ("amount",),
        "cny",
        "Accepted exact TDX daily decoder: source turnover amount in CNY",
    )
    add(
        "pytdx",
        "local_day_file",
        ("pytdx.local_daily_bar.v1", "pytdx.local_daily_bar.v2", "pytdx.local_daily_bar.v3"),
        ("amount",),
        "cny",
        "TDX .day binary32 amount is CNY; v1/v2 Raw may have lost fractional CNY",
    )
    add(
        "baostock",
        "query_history_k_data_plus",
        ("baostock.daily_bar.v1",),
        ("amount",),
        "cny",
        "BaoStock stock daily field table: turnover amount in CNY",
    )
    add(
        "tushare",
        "daily",
        ("tushare.daily_bar.v1",),
        ("amount",),
        "thousand_cny",
        "Tushare daily amount field is thousand CNY; adapter multiplies by 1000",
    )
    add(
        "tushare",
        "daily_basic",
        ("tushare.stock_daily_indicator.v1",),
        ("total_share", "float_share", "free_share"),
        "ten_thousand_share",
        "Tushare daily_basic field table: capital in ten thousand shares",
    )
    add(
        "akshare",
        "stock_zh_a_gbjg_em",
        ("akshare.capital.v1",),
        ("总股本", "流通受限股份", "已流通股份", "已上市流通A股"),
        None,
        "Official stock_zh_a_gbjg_em field table does not establish source quantity units",
    )
    add(
        "akshare",
        "stock_history_dividend_detail",
        ("akshare.capital.v1",),
        ("基准股本",),
        "share",
        "Official AKShare stock_history_dividend_detail rights table: base capital in shares",
    )
    add(
        "akshare",
        "stock_zt_pool_em",
        ("akshare_eastmoney.current_day_limit_up_pool.v1",),
        ("封板资金",),
        None,
        "Official stock_zt_pool_em field table does not establish source sealed-funds units",
    )
    add(
        "akshare",
        "stock_zt_pool_dtgc_em",
        ("akshare_eastmoney.current_day_limit_down_pool.v1",),
        ("封单资金",),
        None,
        "Official stock_zt_pool_dtgc_em field table does not establish source sealed-funds units",
    )
    add(
        "tushare",
        "top10_floatholders",
        ("tushare.top10_float_holder.v1",),
        ("hold_amount", "hold_change"),
        "share",
        "Tushare top10_floatholders output table: holdings and signed changes in shares",
    )
    add(
        "tushare",
        "cb_daily",
        ("tushare.convertible_bond_daily_bar.v1",),
        ("vol",),
        "bond_lot",
        "Accepted ConvertibleBond adapter: ten bonds per lot, independently of stock lots",
        "convertible_bond",
    )
    add(
        "tushare",
        "cb_daily",
        ("tushare.convertible_bond_daily_bar.v1",),
        ("amount",),
        "ten_thousand_cny",
        "Tushare official doc187: cb_daily amount is ten thousand CNY",
        "convertible_bond",
    )
    add(
        "tushare",
        "cb_basic",
        ("tushare.convertible_bond.v1",),
        ("issue_size",),
        "cny",
        "Tushare official doc185: cb_basic issue_size is CNY",
        "convertible_bond",
    )
    add(
        "tushare",
        "cb_daily",
        ("tushare.convertible_bond_daily_bar.v1",),
        ("remain_size",),
        None,
        "cb_daily official field table does not establish remain_size source units",
        "convertible_bond",
    )
    add(
        "eastmoney",
        "stock/details/get",
        ("eastmoney.call_auction_indicative_detail.v1",),
        ("volume_lots",),
        "lot",
        "Accepted Eastmoney auction indicative detail adapter: integer hands of 100 shares",
    )
    add(
        "pytdx_hq",
        "standard_series_archive",
        (
            "market_data_center.call_auction_market_series.raw.v1",
            "market_data_center.call_auction_market_series.raw.v2",
        ),
        (
            "cumulative_volume",
            *(f"{side}_levels.{level}.volume" for side in ("bid", "ask") for level in range(1, 6)),
        ),
        "share",
        "Standard domain record archive; v1/v2 already contain shares, never source lots",
    )
    add(
        "tencent_quote",
        "qt.gtimg.cn",
        ("tencent_quote.qt_gtimg.v1",),
        (
            "cumulative_volume",
            *(f"{side}{level}" for side in ("bid", "ask") for level in range(1, 6)),
        ),
        "lot",
        "Tencent quote integer-lot source protocol; fractions rejected before conversion",
    )
    add(
        "pytdx_hq",
        "standard_snapshot_archive",
        ("market_data_center.call_auction_market_snapshot.raw.v1",),
        (
            "cumulative_volume",
            *(f"{side}_levels.{level}.volume" for side in ("bid", "ask") for level in range(1, 6)),
        ),
        "share",
        "Standard call-auction snapshot archive already contains share quantities",
    )
    add(
        "pytdx_hq",
        "get_security_quotes",
        ("pytdx_hq.security_quotes.v1",),
        (
            "volume_lots",
            *(f"{side}_vol{level}" for side in ("bid", "ask") for level in range(1, 6)),
        ),
        "lot",
        "TDX quote get_price integer-lot wire protocol",
    )
    for provider, endpoint, field, unit in (
        ("eastmoney", "quotes", "f5", "lot"),
        ("eastmoney", "eastmoney_kline", "volume", "lot"),
        ("tencent", "tencent_kline", "volume", "lot"),
        ("sina", "sina_kline", "volume", "share"),
        ("tencent", "minute", "cumulative_volume", "lot"),
    ):
        add(
            provider,
            endpoint,
            ("auction_sources.v1",),
            (field,),
            unit,
            "Existing bounded auction-sources endpoint contract; exact integral shares required",
        )
    add(
        "akshare_ths",
        "board_index_daily_bars",
        ("akshare_ths.board_index_daily_bar.v1",),
        ("成交量",),
        None,
        "THS board aggregate unit unverified; stock units cannot be inferred",
        "board_index",
    )
    add(
        "tushare",
        "index_daily",
        ("tushare.index_daily.v1",),
        ("vol",),
        None,
        "Index quantity semantics require independent certification",
        "index",
    )
    add(
        "baostock",
        "query_history_k_data_plus",
        ("baostock.daily_bar.v1",),
        ("volume",),
        None,
        "Index quantity semantics require independent certification",
        "index",
    )
    add(
        "tushare",
        "stk_factor_pro",
        ("tushare.stock_technical_factor.v1",),
        tuple(
            f"{name}_{basis}" for name in ("obv", "emv", "maemv") for basis in ("bfq", "qfq", "hfq")
        ),
        None,
        "Official factor parameters do not certify output scaling; no OBV x100 assumption",
    )
    return rules


QUANTITY_UNIT_RULES = MappingProxyType(_rules())

# Explicit endpoint/field pairs keep quantity and monetary proof independent.
QUANTITY_FIELD_GROUP_FIELDS: Mapping[str, Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "daily_bar.volume": {
            "stock_zh_a_hist": ("成交量",),
            "get_security_bars": ("volume",),
            "local_day_file": ("volume",),
            "query_history_k_data_plus": ("volume",),
            "daily": ("vol",),
            "index_daily": ("vol",),
        },
        "daily_bar.amount": {
            "stock_zh_a_hist": ("成交额",),
            "get_security_bars": ("amount",),
            "local_day_file": ("amount",),
            "query_history_k_data_plus": ("amount",),
            "daily": ("amount",),
        },
        "five_level_quote.quantities": {
            "get_security_quotes": (
                "volume_lots",
                *(f"{side}_vol{level}" for side in ("bid", "ask") for level in range(1, 6)),
            ),
            "qt.gtimg.cn": (
                "cumulative_volume",
                *(f"{side}{level}" for side in ("bid", "ask") for level in range(1, 6)),
            ),
        },
        "eod_quote_snapshot.quantities": {
            "get_security_quotes": (
                "volume_lots",
                *(f"{side}_vol{level}" for side in ("bid", "ask") for level in range(1, 6)),
            ),
        },
        "stock_quote_snapshot.quantities": {
            "qt.gtimg.cn": (
                "cumulative_volume",
                *(f"{side}{level}" for side in ("bid", "ask") for level in range(1, 6)),
            ),
        },
        "call_auction_indicative_detail.displayed_volume_shares": {
            "stock/details/get": ("volume_lots",)
        },
        "call_auction_market_series.quantities": {
            "standard_series_archive": (
                "cumulative_volume",
                *(
                    f"{side}_levels.{level}.volume"
                    for side in ("bid", "ask")
                    for level in range(1, 6)
                ),
            ),
        },
        "call_auction_market_snapshot.quantities": {
            "standard_snapshot_archive": (
                "cumulative_volume",
                *(
                    f"{side}_levels.{level}.volume"
                    for side in ("bid", "ask")
                    for level in range(1, 6)
                ),
            ),
        },
        "capital.shares": {
            "stock_zh_a_gbjg_em": ("总股本", "流通受限股份", "已流通股份", "已上市流通A股"),
            "stock_history_dividend_detail": ("基准股本",),
        },
        "capital.rights_issue.quantities": {"stock_history_dividend_detail": ("基准股本",)},
        "stock_daily_indicator.shares": {
            "daily_basic": ("total_share", "float_share", "free_share")
        },
        "top10_float_holder.hold_amount": {"top10_floatholders": ("hold_amount", "hold_change")},
        "convertible_bond_daily_bar.volume": {"cb_daily": ("vol",)},
        "convertible_bond_daily_bar.amount": {"cb_daily": ("amount",)},
        "convertible_bond_daily_bar.remain_size": {"cb_daily": ("remain_size",)},
        "convertible_bond.bond.quantities": {"cb_basic": ("issue_size",)},
        "today_limit_up.source_observation.quantities": {"stock_zt_pool_em": ("封板资金",)},
        "today_limit_down.source_observation.quantities": {"stock_zt_pool_dtgc_em": ("封单资金",)},
    }
)


def quantity_group_rules(
    *, provider: str, schema_version: str, field_group: str, instrument_type: str = "stock"
) -> tuple[tuple[QuantityIdentity, QuantityUnitRule], ...]:
    fields_by_endpoint = QUANTITY_FIELD_GROUP_FIELDS.get(field_group, {})
    registered_endpoints = {
        identity[1]
        for identity in QUANTITY_UNIT_RULES
        if identity[0] == provider
        and identity[2] == schema_version
        and identity[3] == instrument_type
    }
    expected = {
        (endpoint, field)
        for endpoint, fields in fields_by_endpoint.items()
        if endpoint in registered_endpoints
        for field in fields
    }
    matches = tuple(
        (identity, rule)
        for identity, rule in QUANTITY_UNIT_RULES.items()
        if identity[0] == provider
        and (identity[1], identity[4]) in expected
        and identity[2] == schema_version
        and identity[3] == instrument_type
    )
    if not matches or {(identity[1], identity[4]) for identity, _ in matches} != expected:
        raise ProviderError(
            "quantity unit contract not registered for "
            f"{provider}/{schema_version}/{instrument_type}/{field_group}"
        )
    for identity, _ in matches:
        quantity_unit_rule(
            provider=identity[0],
            endpoint=identity[1],
            schema_version=identity[2],
            instrument_type=identity[3],
            field=identity[4],
        )
    return matches


def validate_quantity_metadata(
    *,
    provider: str,
    schema_version: str,
    request_params: Mapping[str, object],
    replay: bool = False,
) -> None:
    """Absent legacy metadata is allowed; explicit claims must match source evidence."""
    expected: dict[str, object] = {
        "provider_schema_version": schema_version,
        "quantity_unit_contract_version": UNIT_CONTRACT_VERSION,
    }
    # A source run's normalizer tag records history, not the corrected replay code.
    if not replay:
        expected["quantity_normalizer_version"] = NORMALIZER_VERSION
    scales = {"share": 1, "lot": 100, "ten_thousand_share": 10_000, "bond_lot": 10}
    quantities = {
        (identity[3], rule.source_unit)
        for identity, rule in QUANTITY_UNIT_RULES.items()
        if identity[0] == provider and identity[2] == schema_version and rule.source_unit in scales
    }
    source_units = {unit for _, unit in quantities}
    standard_units = {
        "bond" if instrument == "convertible_bond" else "share" for instrument, _ in quantities
    }
    for key in ("source_quantity_unit", "volume_unit", "standard_quantity_unit", "volume_lot_size"):
        if key not in request_params:
            continue
        units = standard_units if key == "standard_quantity_unit" else source_units
        if len(units) != 1:
            raise ProviderError(f"quantity metadata has no unique registered evidence: {key}")
        unit = next(iter(units))
        if key == "volume_lot_size":
            if type(request_params[key]) is not int:
                raise ProviderError(f"quantity metadata contradicts registered evidence: {key}")
            assert unit is not None
            expected[key] = scales[unit]
        else:
            expected[key] = unit
    amount_keys = ("source_amount_unit", "amount_unit", "standard_amount_unit")
    if any(key in request_params for key in amount_keys):
        amount_group, amount_instrument = {
            "tushare.convertible_bond_daily_bar.v1": (
                "convertible_bond_daily_bar.amount",
                "convertible_bond",
            ),
            "tushare.convertible_bond.v1": ("convertible_bond.bond.quantities", "convertible_bond"),
        }.get(schema_version, ("daily_bar.amount", "stock"))
        try:
            amount_rules = quantity_group_rules(
                provider=provider,
                schema_version=schema_version,
                field_group=amount_group,
                instrument_type=amount_instrument,
            )
        except ProviderError as error:
            raise ProviderError("quantity metadata has no registered amount evidence") from error
        amount_units = {rule.source_unit for _, rule in amount_rules}
        if len(amount_units) != 1:
            raise ProviderError("quantity metadata has no unique registered amount evidence")
        amount_unit = next(iter(amount_units))
        expected.update(
            source_amount_unit=amount_unit, amount_unit=amount_unit, standard_amount_unit="cny"
        )
    for key, value in expected.items():
        if key in request_params and request_params[key] != value:
            raise ProviderError(f"quantity metadata contradicts registered evidence: {key}")


def quantity_unit_rule(
    *, provider: str, endpoint: str, schema_version: str, instrument_type: str, field: str
) -> QuantityUnitRule:
    identity = provider, endpoint, schema_version, instrument_type, field
    rule = QUANTITY_UNIT_RULES.get(identity)
    if rule is None or rule.source_unit is None:
        reason = "not registered" if rule is None else rule.evidence
        raise ProviderError(f"quantity unit contract unverified for {'/'.join(identity)}: {reason}")
    return rule


def normalize_stock_quantity(value: object, *, source_unit: str, context: str) -> int | None:
    scales = {"share": 0, "lot": 2, "ten_thousand_share": 4}
    if source_unit not in scales:
        raise ProviderError(f"{context}: unsupported stock quantity unit {source_unit}")
    return _normalize_integral_quantity(value, decimal_scale=scales[source_unit], context=context)


def normalize_bond_quantity(value: object, *, context: str) -> int | None:
    """Tushare cb_daily lots contain ten bonds, independently of stock lots."""
    quantity_unit_rule(
        provider="tushare",
        endpoint="cb_daily",
        schema_version="tushare.convertible_bond_daily_bar.v1",
        instrument_type="convertible_bond",
        field="vol",
    )
    return _normalize_integral_quantity(value, decimal_scale=1, context=context)


def _normalize_integral_quantity(value: object, *, decimal_scale: int, context: str) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ProviderError(f"{context}: quantity requires an exact decimal scalar")
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ProviderError(f"{context}: invalid quantity") from error
    if not number.is_finite() or number < 0:
        raise ProviderError(f"{context}: quantity must be finite and nonnegative")
    parts = number.as_tuple()
    assert isinstance(parts.exponent, int)
    # Tuple construction scales exactly even if a caller lowered Decimal precision.
    result = Decimal((parts.sign, parts.digits, parts.exponent + decimal_scale))
    if result > BIGINT_MAX or result != result.to_integral_value():
        raise ProviderError(f"{context}: invalid integer volume/quantity or bigint overflow")
    return int(result)


def normalize_source_stock_quantity(
    value: object,
    *,
    provider: str,
    endpoint: str,
    schema_version: str,
    instrument_type: str,
    field: str,
) -> int | None:
    rule = quantity_unit_rule(
        provider=provider,
        endpoint=endpoint,
        schema_version=schema_version,
        instrument_type=instrument_type,
        field=field,
    )
    assert rule.source_unit is not None
    return normalize_stock_quantity(
        value,
        source_unit=rule.source_unit,
        context=f"{provider}/{endpoint}/{schema_version}/{instrument_type}/{field}",
    )


def quantity_unit_metadata(*, source_unit: str) -> dict[str, object]:
    return {
        "quantity_unit_contract_version": UNIT_CONTRACT_VERSION,
        "quantity_normalizer_version": NORMALIZER_VERSION,
        "source_quantity_unit": source_unit,
        "standard_quantity_unit": "share",
    }


def quantity_failure_details(error: Exception) -> dict[str, object]:
    """Record a stable unit failure reason without leaking source values or paths."""
    message = str(error)
    if "quantity metadata" in message:
        reason = "quantity_metadata_conflict"
    elif "quantity unit contract" in message or "unsupported stock quantity unit" in message:
        reason = "quantity_units_unverified"
    else:
        return {}
    return {"quantity_reason": reason, "quantity_unit_contract_version": UNIT_CONTRACT_VERSION}


def stock_instrument_type(symbol: str) -> str:
    exchange, _, code = symbol.partition(":")
    prefixes = {"SSE": ("6",), "SZSE": ("0", "30"), "BSE": ("4", "8", "92")}
    return (
        "stock"
        if len(code) == 6
        and code.isascii()
        and code.isdigit()
        and code.startswith(prefixes.get(exchange, ()))
        and not (exchange == "BSE" and code.startswith("89"))
        else "unverified_instrument"
    )
