"""Bounded quote reads with explicit regulation index identity; no persistence."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from re import finditer
from time import monotonic as system_monotonic

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tencent_quote import (
    ENDPOINT,
    MAX_RESPONSE_BYTES,
    SHANGHAI,
    _record,
    _request_bytes,
    _source_symbol,
    _source_timestamp,
)

BENCHMARKS = {"SSE:000002": "sh000002", "SZSE:399107": "sz399107", "SZSE:399102": "sz399102"}


@dataclass(frozen=True, slots=True)
class MonitorQuote:
    symbol: str
    observed_at: datetime
    price: Decimal
    reference_price: Decimal
    volume_shares: Decimal | None

    def __post_init__(self) -> None:
        _source_symbol(self.symbol)
        if self.observed_at.utcoffset() is None:
            raise ValueError("monitor quote timestamp must be timezone-aware")
        for value in (self.price, self.reference_price):
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError("monitor price must be a finite positive Decimal")
        if self.volume_shares is not None and (
            not self.volume_shares.is_finite() or self.volume_shares < 0
        ):
            raise ValueError("monitor volume must be finite and nonnegative")


def benchmark_source_symbol(symbol: str) -> str:
    if symbol not in BENCHMARKS:
        raise ValueError("unsupported regulation benchmark")
    return BENCHMARKS[symbol]


def quote_issue(
    stock: MonitorQuote,
    benchmark: MonitorQuote,
    trade_date: date,
    now: datetime,
    active_session: bool,
) -> str | None:
    if now.utcoffset() is None:
        raise ValueError("monitor clock must be timezone-aware")
    times = tuple(q.observed_at.astimezone(SHANGHAI) for q in (stock, benchmark))
    local = now.astimezone(SHANGHAI)
    if local.date() != trade_date or any(t.date() != trade_date for t in times):
        return "quote_date_mismatch"
    if any(t > local for t in times):
        return "future_quote"
    if abs((times[0] - times[1]).total_seconds()) > 60:
        return "unsynchronised_quotes"
    clock = local.time()
    active = time(9, 30) <= clock <= time(11, 30) or time(13) <= clock <= time(15)
    if active_session or active:
        return "stale_quote" if any((local - t).total_seconds() > 60 for t in times) else None
    if time(11, 30) < clock < time(13):
        return (
            None
            if all(time(11, 29) <= t.time() <= time(11, 30) for t in times)
            else "stale_session_quote"
        )
    if clock > time(15):
        return None if all(t.time() >= time(14, 59) for t in times) else "stale_session_quote"
    return "outside_monitor_session"


def fetch_monitor_quotes(
    stock_symbols: tuple[str, ...],
    benchmark_symbols: tuple[str, ...],
    deadline: float,
    *,
    request_bytes: Callable[[str, float], bytes] = _request_bytes,
    monotonic: Callable[[], float] = system_monotonic,
) -> tuple[MonitorQuote, ...]:
    """One batch (<=53 symbols) uses at most one upstream request and remaining budget."""
    if len(stock_symbols) > 50 or len(benchmark_symbols) > 3:
        raise ValueError("monitor quote batch exceeds bound")
    if len(set(stock_symbols)) != len(stock_symbols) or len(set(benchmark_symbols)) != len(
        benchmark_symbols
    ):
        raise ValueError("monitor quote symbols must be unique")
    if any(symbol in BENCHMARKS for symbol in stock_symbols):
        raise ValueError("benchmark cannot be requested as stock")
    sources = {symbol: _source_symbol(symbol) for symbol in stock_symbols}
    sources.update({symbol: benchmark_source_symbol(symbol) for symbol in benchmark_symbols})
    remaining = deadline - monotonic()
    if not sources or remaining <= 0:
        return ()
    try:
        payload = request_bytes(ENDPOINT + ",".join(sources.values()), min(3.0, remaining))
        if len(payload) > MAX_RESPONSE_BYTES or monotonic() > deadline:
            return ()
        body = payload.decode("gbk", errors="strict")
    except (OSError, UnicodeError, ProviderError):
        return ()
    symbol_by_source = {value: key for key, value in sources.items()}
    records: dict[str, MonitorQuote] = {}
    seen: set[str] = set()
    for match in finditer(r'v_((?:sh|sz)[0-9]{6})="([^\"]*)";', body):
        symbol = symbol_by_source.get(match[1])
        if symbol is None:
            continue
        if symbol in seen:
            records.pop(symbol, None)
            continue
        seen.add(symbol)
        try:
            if symbol in BENCHMARKS:
                fields = match[2].split("~")
                if len(fields) < 31 or fields[2] != symbol.split(":")[1]:
                    continue
                quote = MonitorQuote(
                    symbol,
                    _source_timestamp(fields[30]),
                    Decimal(fields[3]),
                    Decimal(fields[4]),
                    None,
                )
            else:
                stock = _record(symbol, match[2], datetime.now(UTC))
                if (
                    stock.source_timestamp is None
                    or stock.last_price is None
                    or stock.previous_close is None
                ):
                    continue
                quote = MonitorQuote(
                    symbol,
                    stock.source_timestamp,
                    stock.last_price,
                    stock.previous_close,
                    Decimal(stock.cumulative_volume)
                    if stock.cumulative_volume is not None
                    else None,
                )
            records[symbol] = quote
        except (ValueError, InvalidOperation, ProviderError):
            continue
    return tuple(records[symbol] for symbol in sources if symbol in records)
