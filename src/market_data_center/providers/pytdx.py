"""pytdx adapter for remote unadjusted A-share Daily Bars."""

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from struct import unpack
from types import TracebackType
from typing import Protocol, Self, cast

from market_data_center.domain.classification import (
    ClassificationCatalogSnapshotRecord,
    ClassificationDefinition,
    ClassificationMemberSnapshotRecord,
    ClassificationRecord,
    ClassificationType,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    Market,
    SecurityRecord,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.domain.stock_daily_indicator import StockDailyIndicatorSnapshotRecord
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
    ProviderRecord,
    ProviderRequestUnavailable,
    RawRow,
)
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    endpoints_for,
    load_endpoint_pool,
)
from market_data_center.settings import PytdxDailyBarSettings, PytdxPoolSettings

DAILY_BAR_CATEGORY = 9
CAPABILITY_BY_EXCHANGE = {
    "sh": PytdxCapability.DAILY_BAR_SSE,
    "sz": PytdxCapability.DAILY_BAR_SZSE,
    "bj": PytdxCapability.DAILY_BAR_BSE,
}
TDX_MARKET_BY_EXCHANGE = {"sh": 1, "sz": 0, "bj": 0}


class PytdxDailyBarClient(Protocol):
    def connect(self, host: str, port: int, *, time_out: float) -> bool: ...

    def disconnect(self) -> None: ...

    def get_security_bars(
        self, category: int, market: int, code: str, start: int, count: int
    ) -> object: ...


class PytdxProvider:
    """Read unadjusted Daily Bars from one curated remote TDX endpoint."""

    source_code = "pytdx"

    def __init__(
        self,
        settings: PytdxDailyBarSettings,
        *,
        pool_settings: PytdxPoolSettings | None = None,
        client_factory: Callable[[], PytdxDailyBarClient] | None = None,
    ) -> None:
        self._settings = settings
        self._vipdoc_path = settings.pytdx_vipdoc_path
        self._pool_settings = pool_settings or PytdxPoolSettings()
        self._client_factory = client_factory or _default_client_factory
        self._sessions: dict[
            str, tuple[PytdxDailyBarClient, tuple[str, int]]
        ] = {}
        self._managed = False

    @classmethod
    def default(cls) -> Self:
        return cls(PytdxDailyBarSettings())

    def __enter__(self) -> Self:
        self._managed = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        disconnected: set[int] = set()
        for client, _ in self._sessions.values():
            if id(client) not in disconnected:
                _disconnect(client)
                disconnected.add(id(client))
        self._sessions.clear()
        self._managed = False

    def source_symbol(self, symbol: str) -> str:
        exchange, code, _ = _parse_source_symbol(symbol)
        return f"{exchange}.{code}"

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        raise ProviderError("pytdx does not support the security dataset")

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        raise ProviderError("pytdx does not support the trading calendar dataset")

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        _ensure_date_range(start_date, end_date)
        exchange, code, symbol = _parse_source_symbol(source_symbol)
        # Local .day file takes priority over remote endpoints.
        if self._vipdoc_path:
            local_rows = _read_local_day_file(
                self._vipdoc_path, exchange, code, start_date, end_date
            )
            if local_rows:
                ordered = sorted(local_rows, key=lambda row: _parse_date(row["date"]))
                raw_rows = _rows_with_previous_close(ordered)
                return ProviderBatch(
                    raw_rows=raw_rows,
                    request_params={
                        "source_symbol": f"{exchange}.{code}",
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                        "source": "local_day_file",
                    },
                    schema_version="pytdx.local_daily_bar.v2",
                    record_factory=lambda: _daily_bar_records(raw_rows, symbol),
                )
        client, selected_endpoint = self._session_for(exchange, source_symbol)
        market = TDX_MARKET_BY_EXCHANGE[exchange]
        source_rows: list[RawRow] = []
        try:
            for page in range(self._settings.pytdx_daily_bar_max_pages):
                result = client.get_security_bars(
                    DAILY_BAR_CATEGORY,
                    market,
                    code,
                    page * self._settings.pytdx_daily_bar_page_size,
                    self._settings.pytdx_daily_bar_page_size,
                )
                if not isinstance(result, list):
                    raise ProviderError("pytdx remote Daily Bar response is not a list")
                if not result:
                    break
                page_rows = [_remote_raw_row(row) for row in result]
                source_rows.extend(page_rows)
                oldest = min(_parse_date(row["date"]) for row in page_rows)
                if oldest < start_date or len(page_rows) < self._settings.pytdx_daily_bar_page_size:
                    break
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError("pytdx remote Daily Bar request failed") from error

        if not source_rows:
            raise ProviderRequestUnavailable(
                f"pytdx remote endpoint returned no Daily Bars for {source_symbol}"
            )
        ordered = sorted(source_rows, key=lambda row: _parse_date(row["date"]))
        raw_rows = _rows_with_previous_close(ordered)
        raw_rows = tuple(
            row for row in raw_rows if start_date <= _parse_date(row["date"]) <= end_date
        )
        if not raw_rows:
            raise ProviderRequestUnavailable(
                f"pytdx remote endpoint has no Daily Bars in range for {source_symbol}"
            )
        endpoint = f"{selected_endpoint[0]}:{selected_endpoint[1]}"
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "source_symbol": f"{exchange}.{code}",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "endpoint": endpoint,
                "tdx_market": market,
                "category": DAILY_BAR_CATEGORY,
                "page_size": self._settings.pytdx_daily_bar_page_size,
                "max_pages": self._settings.pytdx_daily_bar_max_pages,
                "adjust": "none",
            },
            schema_version="pytdx.remote_daily_bar.v1",
            record_factory=lambda: _daily_bar_records(raw_rows, symbol),
        )

    def _session_for(
        self, exchange: str, source_symbol: str
    ) -> tuple[PytdxDailyBarClient, tuple[str, int]]:
        if not self._managed:
            raise ProviderError("pytdx remote provider must be used as a managed context")
        existing = self._sessions.get(exchange)
        if existing is not None:
            return existing
        capability = CAPABILITY_BY_EXCHANGE[exchange]
        try:
            pool = load_endpoint_pool(self._pool_settings.pytdx_pool_path)
        except ProviderError as error:
            if self._vipdoc_path:
                raise ProviderRequestUnavailable(
                    f"pytdx local .day file not found for {source_symbol} "
                    "and no usable remote endpoint pool exists"
                ) from error
            raise
        endpoints = endpoints_for(pool, capability)
        if not endpoints:
            raise ProviderRequestUnavailable(
                f"pytdx endpoint pool has no {capability.value} node"
            )
        errors: list[str] = []
        for host, port in endpoints[: self._settings.pytdx_daily_bar_max_attempts]:
            client = self._client_factory()
            try:
                connected = client.connect(
                    host,
                    port,
                    time_out=self._settings.pytdx_daily_bar_timeout_seconds,
                )
            except Exception as error:
                errors.append(f"{host}:{port} ({type(error).__name__})")
                _disconnect(client)
                continue
            if connected:
                session = (client, (host, port))
                self._sessions[exchange] = session
                return session
            errors.append(f"{host}:{port}")
            _disconnect(client)
        raise ProviderError(
            f"pytdx remote connection failed for {exchange}; tried " + ", ".join(errors)
        )

    def fetch_capital(self, source_symbol: str) -> ProviderBatch[CapitalRecord]:
        raise ProviderRequestUnavailable("pytdx does not provide Capital facts")

    def fetch_stock_daily_indicators(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[StockDailyIndicatorSnapshotRecord]:
        raise ProviderRequestUnavailable("pytdx does not provide stock daily indicators")

    def fetch_stock_daily_indicator_snapshot(
        self, trade_date: date
    ) -> ProviderBatch[StockDailyIndicatorSnapshotRecord]:
        raise ProviderRequestUnavailable("pytdx does not provide stock daily indicators")

    def fetch_classification_catalog(
        self, classification_type: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        raise ProviderRequestUnavailable("remote pytdx does not provide Classification facts")

    def fetch_classification_members(
        self, classification_type: str, classification_code: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        raise ProviderRequestUnavailable("remote pytdx does not provide Classification facts")


def normalize_pytdx_raw(
    dataset_code: DatasetCode,
    schema_version: str,
    raw_rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[ProviderRecord, ...]:
    if dataset_code is DatasetCode.CLASSIFICATION_CATALOG:
        if schema_version != "pytdx.local_classification_catalog.v1":
            raise ProviderError(f"unsupported pytdx Raw schema: {schema_version}")
        kind = _replay_classification_type(request_params)
        snapshot_date = _replay_date(request_params, "snapshot_date")
        return (_classification_catalog_record(raw_rows, kind, snapshot_date),)
    if dataset_code is DatasetCode.CLASSIFICATION_MEMBERS:
        if schema_version != "pytdx.local_classification_members.v1":
            raise ProviderError(f"unsupported pytdx Raw schema: {schema_version}")
        kind = _replay_classification_type(request_params)
        snapshot_date = _replay_date(request_params, "snapshot_date")
        code = request_params.get("classification_code")
        if not isinstance(code, str) or not code.strip():
            raise ProviderError("pytdx replay is missing classification_code")
        return (_classification_member_record(raw_rows, kind, code, snapshot_date),)
    if dataset_code is not DatasetCode.DAILY_BAR:
        raise ProviderError(f"pytdx cannot replay dataset: {dataset_code.value}")
    if schema_version not in {
        "pytdx.local_daily_bar.v1",
        "pytdx.local_daily_bar.v2",
        "pytdx.remote_daily_bar.v1",
    }:
        raise ProviderError(f"unsupported pytdx Raw schema: {schema_version}")
    source_symbol = request_params.get("source_symbol")
    if not isinstance(source_symbol, str):
        raise ProviderError("pytdx replay request is missing source_symbol")
    _, _, symbol = _parse_source_symbol(source_symbol)
    normalized_rows = (
        _rows_with_previous_close(raw_rows)
        if schema_version == "pytdx.local_daily_bar.v1"
        else raw_rows
    )
    return tuple(_daily_bar_records(normalized_rows, symbol))


def _default_client_factory() -> PytdxDailyBarClient:
    from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]

    return cast(
        PytdxDailyBarClient,
        TdxHq_API(heartbeat=True, auto_retry=False, raise_exception=True),
    )


def _disconnect(client: PytdxDailyBarClient) -> None:
    try:
        client.disconnect()
    except Exception:
        return


def _read_local_day_file(
    vipdoc_path: str,
    exchange: str,
    code: str,
    start_date: date,
    end_date: date,
) -> tuple[RawRow, ...] | None:
    """Read a local TDX .day file and return raw rows, or None if the file is absent.

    The .day binary format is 32 bytes per record:
    date(uint32 YYYYMMDD) open(uint32) high(uint32) low(uint32) close(uint32)
    amount(float32) volume(uint32) prev_close(uint32).

    Prices are stored as actual_price * 100; the ``price_scale`` empty
    marker tells ``_map_daily_bar`` to apply the /100 conversion.
    """
    day_file = Path(vipdoc_path) / exchange / "lday" / f"{exchange}{code}.day"
    if not day_file.is_file():
        return None
    data = day_file.read_bytes()
    record_count = len(data) // 32
    rows: list[RawRow] = []
    for index in range(record_count):
        offset = index * 32
        raw_date, open_raw, high_raw, low_raw, close_raw, amount_raw, vol_raw, _prev = unpack(
            "<IIIIIfII", data[offset : offset + 32]
        )
        try:
            trade_date = date(raw_date // 10000, raw_date % 10000 // 100, raw_date % 100)
        except ValueError:
            continue
        if trade_date < start_date or trade_date > end_date:
            continue
        rows.append(
            {
                "date": f"{raw_date:08d}",
                "price_scale": "",
                "open": str(open_raw),
                "high": str(high_raw),
                "low": str(low_raw),
                "close": str(close_raw),
                "amount": f"{amount_raw:.0f}",
                "volume": str(vol_raw),
            }
        )
    return tuple(rows)


def _remote_raw_row(row: object) -> RawRow:
    if not isinstance(row, Mapping):
        raise ProviderError("pytdx remote Daily Bar row is not an object")
    raw_date = str(row.get("datetime") or row.get("date") or "").strip()
    date_key = raw_date[:10].replace("-", "")
    required = {
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "amount": row.get("amount"),
        "volume": row.get("vol", row.get("volume")),
    }
    if any(value is None for value in required.values()):
        raise ProviderError("pytdx remote Daily Bar row is missing required fields")
    _parse_date(date_key)
    return {
        "date": date_key,
        "price_scale": "1",
        **{key: str(value) for key, value in required.items()},
    }


def _map_daily_bar(row: Mapping[str, str], symbol: str) -> DailyBarRecord:
    previous_close = row.get("previous_close", "").strip()
    price = _decimal if row.get("price_scale") == "1" else _price
    return DailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["date"]),
        market=Market.CN_A_SHARE,
        open=price(row["open"], "open"),
        high=price(row["high"], "high"),
        low=price(row["low"], "low"),
        close=price(row["close"], "close"),
        previous_close=price(previous_close, "previous_close") if previous_close else None,
        volume=_integer(row["volume"], "volume"),
        amount=_decimal(row["amount"], "amount"),
        trade_status=TradeStatus.UNKNOWN,
        is_st=None,
        source_code="pytdx",
    )


def _daily_bar_records(rows: Iterable[Mapping[str, str]], symbol: str) -> list[DailyBarRecord]:
    records_by_date: dict[date, DailyBarRecord] = {}
    for row in rows:
        record = _map_daily_bar(row, symbol)
        existing = records_by_date.get(record.trade_date)
        if existing is not None and existing != record:
            raise ProviderError(
                f"pytdx contains conflicting daily bars for {symbol} "
                f"on {record.trade_date.isoformat()}"
            )
        records_by_date[record.trade_date] = record
    return [records_by_date[trade_date] for trade_date in sorted(records_by_date)]


def _rows_with_previous_close(
    rows: Sequence[Mapping[str, str]],
) -> tuple[Mapping[str, str], ...]:
    ordered = sorted(rows, key=lambda row: int(row["date"]))
    result: list[Mapping[str, str]] = []
    previous: Mapping[str, str] | None = None
    for row in ordered:
        result.append(
            {
                **row,
                "previous_close": previous["close"] if previous is not None else "",
            }
        )
        previous = row
    return tuple(result)


def _parse_source_symbol(value: str) -> tuple[str, str, str]:
    candidate = value.strip().upper()
    if ":" in candidate:
        exchange_name, code = candidate.split(":", maxsplit=1)
        exchange = {"SSE": "sh", "SZSE": "sz", "BSE": "bj"}.get(exchange_name)
    elif "." in candidate:
        prefix, code = candidate.split(".", maxsplit=1)
        exchange = {
            "1": "sh",
            "0": "sz",
            "SH": "sh",
            "SZ": "sz",
            "BJ": "bj",
        }.get(prefix)
    else:
        exchange = None
        code = candidate
    if exchange is None or not code.isdigit() or len(code) != 6:
        raise ProviderError(f"unsupported pytdx symbol: {value}")
    exchange_name = {"sh": "SSE", "sz": "SZSE", "bj": "BSE"}[exchange]
    return exchange, code, f"{exchange_name}:{code}"


def _parse_date(value: str) -> date:
    if len(value) != 8 or not value.isdigit():
        raise ProviderError(f"invalid pytdx date: {value}")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError as error:
        raise ProviderError(f"invalid pytdx date: {value}") from error


def _price(value: str, field: str) -> Decimal:
    return _decimal(value, field) / Decimal(100)


def _decimal(value: str, field: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ProviderError(f"invalid pytdx {field}: {value}") from error


def _integer(value: str, field: str) -> int:
    number = _decimal(value, field)
    if number != number.to_integral_value():
        raise ProviderError(f"invalid pytdx integer {field}: {value}")
    return int(number)


def _ensure_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")


def _classification_type(value: str) -> ClassificationType:
    try:
        kind = ClassificationType(value.strip().lower())
    except ValueError as error:
        raise ProviderRequestUnavailable(
            f"pytdx does not support classification type: {value}"
        ) from error
    if kind not in {ClassificationType.INDUSTRY, ClassificationType.CONCEPT}:
        raise ProviderRequestUnavailable(f"pytdx does not support classification type: {value}")
    return kind


def _replay_classification_type(request_params: Mapping[str, object]) -> ClassificationType:
    value = request_params.get("classification_type")
    if not isinstance(value, str):
        raise ProviderError("pytdx replay is missing classification_type")
    return _classification_type(value)


def _replay_date(request_params: Mapping[str, object], field: str) -> date:
    value = request_params.get(field)
    if not isinstance(value, str):
        raise ProviderError(f"pytdx replay is missing {field}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ProviderError(f"pytdx replay has invalid {field}: {value}") from error


def _read_local_text(path: Path) -> str:
    if not path.is_file():
        raise ProviderRequestUnavailable(f"pytdx local classification file is missing: {path.name}")
    try:
        return path.read_text(encoding="gb18030")
    except (OSError, UnicodeError) as error:
        raise ProviderError(
            f"pytdx failed to read local classification file: {path.name}"
        ) from error


def _load_industry_classifications(
    hq_cache_path: Path,
) -> tuple[tuple[RawRow, ...], dict[str, tuple[RawRow, ...]]]:
    names: dict[str, str] = {}
    for file_name in ("tdxzs.cfg", "tdxzs3.cfg"):
        for line in _read_local_text(hq_cache_path / file_name).splitlines():
            fields = line.split("|")
            if len(fields) >= 6 and fields[5].startswith(("T", "X")):
                names[fields[5]] = fields[0].strip()

    members: dict[str, list[RawRow]] = {}
    referenced: set[str] = set()
    for line in _read_local_text(hq_cache_path / "tdxhy.cfg").splitlines():
        fields = line.split("|")
        if len(fields) < 6:
            raise ProviderError("pytdx local tdxhy.cfg contains a malformed row")
        market_code, stock_code = fields[0].strip(), fields[1].strip()
        if not _valid_local_stock(market_code, stock_code):
            continue
        for classification_code in (fields[2].strip(), fields[5].strip()):
            if classification_code in names:
                referenced.add(classification_code)
                members.setdefault(classification_code, []).append(
                    {"market_code": market_code, "stock_code": stock_code}
                )

    definitions = tuple(
        {
            "classification_code": code,
            "name": names[code],
            "level": "1",
            "parent_code": "",
        }
        for code in sorted(referenced)
    )
    return definitions, {code: tuple(rows) for code, rows in members.items()}


def _load_concept_classifications(
    hq_cache_path: Path,
) -> tuple[tuple[RawRow, ...], dict[str, tuple[RawRow, ...]]]:
    definitions: list[RawRow] = []
    members: dict[str, list[RawRow]] = {}
    current_code: str | None = None
    expected_count = 0
    for line in _read_local_text(hq_cache_path / "infoharbor_block.dat").splitlines():
        if line.startswith("#"):
            if current_code is not None and len(members[current_code]) != expected_count:
                raise ProviderError(f"pytdx concept member count mismatch for {current_code}")
            header = line[1:].split(",")
            current_code = None
            if len(header) < 3 or not header[0].startswith("GN_"):
                continue
            name = header[0][3:].strip()
            code = header[2].strip().upper()
            try:
                expected_count = int(header[1])
            except ValueError as error:
                raise ProviderError("pytdx concept header has invalid member count") from error
            if not name or not code:
                raise ProviderError("pytdx concept header has blank identity")
            current_code = code
            definitions.append(
                {
                    "classification_code": code,
                    "name": name,
                    "level": "1",
                    "parent_code": "",
                }
            )
            members[code] = []
            continue
        if current_code is None or not line.strip():
            continue
        for value in (item for item in line.split(",") if item):
            market_code, separator, stock_code = value.partition("#")
            if not separator or not _valid_local_stock(market_code, stock_code):
                raise ProviderError(f"pytdx concept member is malformed: {value}")
            members[current_code].append({"market_code": market_code, "stock_code": stock_code})
    if current_code is not None and len(members[current_code]) != expected_count:
        raise ProviderError(f"pytdx concept member count mismatch for {current_code}")
    return tuple(definitions), {code: tuple(rows) for code, rows in members.items()}


def _valid_local_stock(market_code: str, stock_code: str) -> bool:
    return market_code in {"0", "1", "2"} and len(stock_code) == 6 and stock_code.isdigit()


def _classification_catalog_record(
    rows: Sequence[Mapping[str, str]],
    kind: ClassificationType,
    snapshot_date: date,
) -> ClassificationCatalogSnapshotRecord:
    return ClassificationCatalogSnapshotRecord(
        namespace="tdx",
        classification_type=kind,
        snapshot_date=snapshot_date,
        definitions=tuple(
            ClassificationDefinition(
                code=row["classification_code"],
                name=row["name"],
                level=int(row["level"]),
                parent_code=row["parent_code"] or None,
            )
            for row in rows
        ),
        source_code="pytdx",
    )


def _classification_member_record(
    rows: Sequence[Mapping[str, str]],
    kind: ClassificationType,
    classification_code: str,
    snapshot_date: date,
) -> ClassificationMemberSnapshotRecord:
    return ClassificationMemberSnapshotRecord(
        namespace="tdx",
        classification_type=kind,
        classification_code=classification_code.strip().upper(),
        snapshot_date=snapshot_date,
        members=tuple(
            _local_standard_symbol(row["market_code"], row["stock_code"]) for row in rows
        ),
        source_code="pytdx",
    )


def _local_standard_symbol(market_code: str, stock_code: str) -> str:
    exchange = {"0": "SZSE", "1": "SSE", "2": "BSE"}.get(market_code)
    if exchange is None or not _valid_local_stock(market_code, stock_code):
        raise ProviderError(f"invalid pytdx local stock identity: {market_code}#{stock_code}")
    return f"{exchange}:{stock_code}"
