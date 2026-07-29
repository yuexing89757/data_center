"""pytdx adapter for local unadjusted A-share daily-bar files."""

import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
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
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
    ProviderRecord,
    ProviderRequestUnavailable,
    RawRow,
)

type LocalDailyBarRow = tuple[int, int, int, int, int, float, int, int]


class PytdxDailyBarReader(Protocol):
    def parse_data_by_file(self, fname: str) -> Iterable[LocalDailyBarRow]: ...


class PytdxProvider:
    """Read local TDX daily files; other datasets remain unsupported."""

    source_code = "pytdx"

    def __init__(self, reader: PytdxDailyBarReader, *, vipdoc_path: Path) -> None:
        self._reader = reader
        self._vipdoc_path = vipdoc_path
        self._classification_cache: dict[
            ClassificationType, tuple[tuple[RawRow, ...], dict[str, tuple[RawRow, ...]]]
        ] = {}

    @classmethod
    def default(cls) -> Self:
        from pytdx.reader.daily_bar_reader import (  # type: ignore[import-untyped]
            TdxDailyBarReader,
        )

        configured_path = os.getenv("PYTDX_VIPDOC_PATH", "").strip()
        if not configured_path:
            raise ProviderError("PYTDX_VIPDOC_PATH is required for the local pytdx provider")
        vipdoc_path = Path(configured_path)
        if not vipdoc_path.is_dir():
            raise ProviderError(f"pytdx vipdoc directory does not exist: {vipdoc_path}")
        reader = TdxDailyBarReader(str(vipdoc_path))
        return cls(cast(PytdxDailyBarReader, reader), vipdoc_path=vipdoc_path)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

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
        relative_path = Path(exchange) / "lday" / f"{exchange}{code}.day"
        file_path = self._vipdoc_path / relative_path
        if not file_path.is_file():
            raise ProviderRequestUnavailable(
                f"pytdx local daily-bar file does not exist: {relative_path.as_posix()}"
            )

        raw_rows: list[RawRow] = []
        start_key = int(start_date.strftime("%Y%m%d"))
        end_key = int(end_date.strftime("%Y%m%d"))
        try:
            source_rows = sorted(
                (_raw_row(row) for row in self._reader.parse_data_by_file(str(file_path))),
                key=lambda row: int(row["date"]),
            )
            previous_row: RawRow | None = None
            for raw_row in source_rows:
                source_date = int(raw_row["date"])
                if start_key <= source_date <= end_key:
                    raw_rows.append(
                        {
                            **raw_row,
                            "previous_close": previous_row["close"] if previous_row else "",
                        }
                    )
                previous_row = raw_row
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError(
                f"pytdx failed to read local file: {relative_path.as_posix()}"
            ) from error

        if not source_rows:
            raise ProviderRequestUnavailable(
                f"pytdx local daily-bar file is empty: {relative_path.as_posix()}"
            )
        market_latest_date = self._market_latest_date(exchange)
        if market_latest_date < end_date:
            raise ProviderError(
                f"pytdx local {exchange} market data is stale: "
                f"latest {market_latest_date.isoformat()}, requested through {end_date.isoformat()}"
            )
        if not raw_rows:
            raise ProviderRequestUnavailable(
                f"pytdx local file has no daily bars in the requested range for {source_symbol}"
            )
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "source_symbol": f"{exchange}.{code}",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "relative_path": relative_path.as_posix(),
                "adjust": "none",
            },
            schema_version="pytdx.local_daily_bar.v2",
            record_factory=lambda: _daily_bar_records(raw_rows, symbol),
        )

    def fetch_capital(self, source_symbol: str) -> ProviderBatch[CapitalRecord]:
        raise ProviderRequestUnavailable("pytdx local files do not provide Capital facts")

    def fetch_classification_catalog(
        self, classification_type: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        kind = _classification_type(classification_type)
        definitions, _ = self._classification_data(kind)
        return ProviderBatch(
            raw_rows=definitions,
            request_params={
                "classification_type": kind.value,
                "snapshot_date": snapshot_date.isoformat(),
            },
            schema_version="pytdx.local_classification_catalog.v1",
            records=[_classification_catalog_record(definitions, kind, snapshot_date)],
        )

    def fetch_classification_members(
        self, classification_type: str, classification_code: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        kind = _classification_type(classification_type)
        code = classification_code.strip().upper()
        definitions, members_by_code = self._classification_data(kind)
        known_codes = {row["classification_code"] for row in definitions}
        if code not in known_codes:
            raise ProviderRequestUnavailable(f"unknown pytdx classification code: {code}")
        rows = members_by_code.get(code, ())
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "classification_type": kind.value,
                "classification_code": code,
                "snapshot_date": snapshot_date.isoformat(),
            },
            schema_version="pytdx.local_classification_members.v1",
            records=[_classification_member_record(rows, kind, code, snapshot_date)],
        )

    def _classification_data(
        self, kind: ClassificationType
    ) -> tuple[tuple[RawRow, ...], dict[str, tuple[RawRow, ...]]]:
        cached = self._classification_cache.get(kind)
        if cached is not None:
            return cached
        hq_cache_path = self._vipdoc_path.parent / "T0002" / "hq_cache"
        loaded = (
            _load_industry_classifications(hq_cache_path)
            if kind is ClassificationType.INDUSTRY
            else _load_concept_classifications(hq_cache_path)
        )
        self._classification_cache[kind] = loaded
        return loaded

    def _market_latest_date(self, exchange: str) -> date:
        sentinel_code = {"sh": "000001", "sz": "399001", "bj": "899050"}[exchange]
        relative_path = Path(exchange) / "lday" / f"{exchange}{sentinel_code}.day"
        file_path = self._vipdoc_path / relative_path
        if not file_path.is_file():
            raise ProviderError(
                f"pytdx local market sentinel does not exist: {relative_path.as_posix()}"
            )
        try:
            dates = (
                _parse_date(str(row[0])) for row in self._reader.parse_data_by_file(str(file_path))
            )
            return max(dates)
        except ValueError as error:
            raise ProviderError(
                f"pytdx local market sentinel is empty: {relative_path.as_posix()}"
            ) from error
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError(
                f"pytdx failed to read local market sentinel: {relative_path.as_posix()}"
            ) from error


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
    if schema_version not in {"pytdx.local_daily_bar.v1", "pytdx.local_daily_bar.v2"}:
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


def _raw_row(row: LocalDailyBarRow) -> RawRow:
    if len(row) != 8:
        raise ProviderError(f"invalid pytdx local daily-bar record length: {len(row)}")
    trade_date, open_, high, low, close, amount, volume, reserved = row
    return {
        "date": str(trade_date),
        "open": str(open_),
        "high": str(high),
        "low": str(low),
        "close": str(close),
        "amount": str(amount),
        "volume": str(volume),
        "reserved": str(reserved),
    }


def _map_daily_bar(row: Mapping[str, str], symbol: str) -> DailyBarRecord:
    previous_close = row.get("previous_close", "").strip()
    return DailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["date"]),
        market=Market.CN_A_SHARE,
        open=_price(row["open"], "open"),
        high=_price(row["high"], "high"),
        low=_price(row["low"], "low"),
        close=_price(row["close"], "close"),
        previous_close=_price(previous_close, "previous_close") if previous_close else None,
        # The .day binary record already stores stock volume in shares.
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
                f"pytdx local file contains conflicting daily bars for {symbol} "
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
        raise ProviderError(f"invalid pytdx local date: {value}")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError as error:
        raise ProviderError(f"invalid pytdx local date: {value}") from error


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
