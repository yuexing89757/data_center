"""pytdx adapter for local unadjusted A-share daily-bar files."""

import os
from collections.abc import Iterable, Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self, cast

from market_data_center.domain.records import (
    DailyBarRecord,
    Market,
    SecurityRecord,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
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
        source_row_count = 0
        start_key = int(start_date.strftime("%Y%m%d"))
        end_key = int(end_date.strftime("%Y%m%d"))
        try:
            rows = self._reader.parse_data_by_file(str(file_path))
            for source_row in rows:
                source_row_count += 1
                raw_row = _raw_row(source_row)
                source_date = int(raw_row["date"])
                if not start_key <= source_date <= end_key:
                    continue
                raw_rows.append(raw_row)
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError(
                f"pytdx failed to read local file: {relative_path.as_posix()}"
            ) from error

        if source_row_count == 0:
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
            schema_version="pytdx.local_daily_bar.v1",
            record_factory=lambda: _daily_bar_records(raw_rows, symbol),
        )

    def _market_latest_date(self, exchange: str) -> date:
        sentinel_code = {"sh": "000001", "sz": "399001"}[exchange]
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
    return DailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["date"]),
        market=Market.CN_A_SHARE,
        open=_price(row["open"], "open"),
        high=_price(row["high"], "high"),
        low=_price(row["low"], "low"),
        close=_price(row["close"], "close"),
        previous_close=None,
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


def _parse_source_symbol(value: str) -> tuple[str, str, str]:
    candidate = value.strip().upper()
    if ":" in candidate:
        exchange_name, code = candidate.split(":", maxsplit=1)
        if exchange_name == "BSE":
            raise ProviderRequestUnavailable(f"pytdx does not support BSE symbol: {value}")
        exchange = {"SSE": "sh", "SZSE": "sz"}.get(exchange_name)
    elif "." in candidate:
        prefix, code = candidate.split(".", maxsplit=1)
        exchange = {"1": "sh", "0": "sz", "SH": "sh", "SZ": "sz"}.get(prefix)
    else:
        exchange = None
        code = candidate
    if exchange is None or not code.isdigit() or len(code) != 6:
        raise ProviderError(f"unsupported pytdx symbol: {value}")
    exchange_name = "SSE" if exchange == "sh" else "SZSE"
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
