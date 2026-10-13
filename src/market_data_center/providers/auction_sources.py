"""Fixed, bounded source reads extracted from the nine-in-one application.

No persistence, cross-source fallback, JSONP execution, or strategy calculations.
"""

# ruff: noqa: RUF001 - Chinese source purpose descriptions.

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import Any, Literal, cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

MAX_RESPONSE_BYTES = 8_000_000
SHANGHAI = ZoneInfo("Asia/Shanghai")
Endpoint = Literal[
    "quotes",
    "limit_up",
    "limit_down",
    "broken_limit",
    "eastmoney_kline",
    "tencent_kline",
    "sina_kline",
    "minute",
    "index",
    "today",
    "themes",
    "stocks",
]
SOURCES = {
    "eastmoney": ("东方财富", "https://www.eastmoney.com/"),
    "tencent": ("腾讯财经", "https://finance.qq.com/"),
    "sina": ("新浪财经", "https://finance.sina.com.cn/"),
}
MARKETS = {
    "all": "m:1+t:2,m:0+t:6,m:1+t:23,m:0+t:80",
    "mainboard": "m:1+t:2,m:0+t:6",
    "star": "m:1+t:23",
    "growth": "m:0+t:80",
}
FIELDS = "f2,f3,f4,f5,f6,f8,f10,f12,f14,f17,f18,f20,f21,f26,f615,f630,f100,f102,f103"
PURPOSES = {
    "quotes": "全市场单页即时行情，供竞价筛选及实时跟踪；不是固定时点竞价快照。",
    "limit_up": "指定日期来源涨停池及连板信息。",
    "limit_down": "指定日期来源跌停池和来源计数。",
    "broken_limit": "指定日期来源炸板池和来源计数。",
    "eastmoney_kline": "东方财富前复权日K，供历史行情读取。",
    "tencent_kline": "腾讯日/周K，标明实际复权状态。",
    "sina_kline": "新浪日K备用来源，复权口径未明确。",
    "minute": "腾讯当日分时价格及累计成交量额，用于分时图。",
    "index": "腾讯上证指数当前点位及涨跌幅。",
    "today": "东方财富今日题材机会，仅转述来源信息。",
    "themes": "东方财富题材排行单页，区分板块与领涨股涨幅。",
    "stocks": "指定题材成分股单页及来源入选说明。",
}


class AuctionSourcesUpstream(RuntimeError):
    """A fixed upstream returned an invalid or unavailable source response."""


class AuctionSourcesInvalid(ValueError):
    """Request parameters are outside the accepted bounded source contract."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise AuctionSourcesUpstream("invalid upstream response")
        return cast(bytes, body)


def _number(value: object, *, nonnegative: bool = False) -> Decimal | None:
    if value is None or value in ("", "-", "--"):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
        raise ValueError("invalid decimal")
    result = Decimal(value)
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError("invalid finite value")
    return result


def _count(value: object) -> int | None:
    value_decimal = _number(value, nonnegative=True)
    if value_decimal is None:
        return None
    if value_decimal != value_decimal.to_integral_value():
        raise ValueError("invalid count")
    return int(value_decimal)


def _text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("invalid text")
    if len(value) > 20000:
        raise ValueError("text too long")
    return value


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("invalid source object")
    return value


def _rows(value: object, maximum: int) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError("invalid bounded array")
    return value


def _code(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{6}", value):
        raise ValueError("invalid source security code")
    return value


def _symbol(code: str) -> str:
    if code.startswith("6"):
        return "SSE:" + code
    if code.startswith(("0", "3")):
        return "SZSE:" + code
    if code.startswith(("4", "8", "92")):
        return "BSE:" + code
    raise ValueError("unsupported security code")


def _identity(row: dict[str, Any], code_key: str, name_key: str) -> dict[str, Any]:
    code = _code(row[code_key])
    return {"symbol": _symbol(code), "code": code, "name": _text(row.get(name_key))}


def _source_symbol(symbol: str) -> tuple[str, str]:
    if not re.fullmatch(r"(?:SSE:6[0-9]{5}|SZSE:[03][0-9]{5})", symbol):
        raise AuctionSourcesInvalid("unsupported symbol")
    exchange, _, code = symbol.partition(":")
    return ("sh" if exchange == "SSE" else "sz") + code, (
        "1." if exchange == "SSE" else "0."
    ) + code


def _day(value: object) -> date:
    text = str(value)
    if re.fullmatch(r"[0-9]{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", text):
        return date.fromisoformat(text)
    raise ValueError("invalid source date")


def _time(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text == "0":
        return None
    if not re.fullmatch(r"[0-9]{1,6}", text):
        raise ValueError("invalid source time")
    return datetime.strptime(text.zfill(6), "%H%M%S").strftime("%H:%M:%S")


def _quote(row: dict[str, Any]) -> dict[str, Any]:
    volume = _number(row.get("f5"), nonnegative=True)
    return _identity(row, "f12", "f14") | {
        "last_price": _number(row.get("f2"), nonnegative=True),
        "change_pct": _number(row.get("f3")),
        "open": _number(row.get("f17"), nonnegative=True),
        "previous_close": _number(row.get("f18"), nonnegative=True),
        "volume_shares": volume * 100 if volume is not None else None,
        "amount_cny": _number(row.get("f6"), nonnegative=True),
        "turnover_rate_pct": _number(row.get("f8"), nonnegative=True),
        "volume_ratio": _number(row.get("f10"), nonnegative=True),
        "total_market_value_cny": _number(row.get("f20"), nonnegative=True),
        "float_market_value_cny": _number(row.get("f21"), nonnegative=True),
        "listing_date": _day(row["f26"]) if row.get("f26") not in (None, "-", 0) else None,
        "industry": _text(row.get("f100")),
        "concepts": _text(row.get("f103")),
    }


def _pool(row: dict[str, Any], endpoint: Endpoint) -> dict[str, Any]:
    return _identity(row, "c", "n") | {
        "consecutive_limit_up_days": _count(row.get("lbc")) if endpoint == "limit_up" else None,
        "first_limit_time": _time(row.get("fbt")) if endpoint != "limit_down" else None,
        "break_count": _count(row.get("zbc")) if endpoint != "limit_down" else None,
        "sealing_amount_cny": _number(row.get("fund"), nonnegative=True),
        "industry": _text(row.get("hybk")),
    }


def _bar(parts: list[Any], volume_factor: int, *, eastmoney: bool = False) -> dict[str, Any]:
    if len(parts) < 6:
        raise ValueError("short bar")
    prices = [_number(p, nonnegative=True) for p in parts[1:5]]
    if any(p is None for p in prices):
        raise ValueError("missing bar price")
    op, close, high, low = cast(list[Decimal], prices)
    if low > min(op, close) or high < max(op, close) or low > high:
        raise ValueError("invalid OHLC")
    vol = _number(parts[5], nonnegative=True)
    if vol is None:
        raise ValueError("missing bar volume")
    return {
        "trade_date": _day(parts[0]),
        "open": op,
        "close": close,
        "high": high,
        "low": low,
        "volume_shares": vol * volume_factor,
        "amount_cny": _number(parts[6], nonnegative=True) if eastmoney and len(parts) > 6 else None,
    }


def _theme(row: dict[str, Any], endpoint: Endpoint) -> dict[str, Any]:
    code = row.get("themeCode", row.get("code"))
    if not isinstance(code, str) or not re.fullmatch(r"[0-9A-Za-z]{1,32}", code):
        raise ValueError("invalid theme code")
    return {
        "theme_code": code,
        "theme_name": _text(row.get("themeName", row.get("name"))),
        "change_pct": _number(row.get("f3") if endpoint == "today" else row.get("bf3")),
        "leader_change_pct": _number(row.get("f3")) if endpoint == "themes" else None,
        "leader_name": _text(row.get("securityName")),
        "limit_up_count": _count(row.get("fex3")),
        "stock_count": _count(row.get("stockNum", row.get("stockCount"))),
    }


def _theme_stock(row: dict[str, Any]) -> dict[str, Any]:
    keywords = row.get("keywordList")
    reason = None
    if keywords is not None:
        values = _rows(keywords, 100)
        if values:
            reason = _text(_mapping(values[0]).get("introduction"))
    return _identity(row, "securityCode", "securityName") | {
        "last_price": _number(row.get("f2"), nonnegative=True),
        "change_pct": _number(row.get("f3")),
        "main_net_amount_cny": _number(row.get("f62")),
        "reason": reason,
    }


def _invalid_constant(value: str) -> None:
    raise ValueError("invalid JSON numeric constant")


class AuctionSourcesProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_bytes,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("source timeout must be between 0 and 10 seconds")
        self._timeout, self._request, self._clock = timeout_seconds, request_bytes, clock

    def fetch(
        self,
        endpoint: Endpoint,
        *,
        symbol: str = "SSE:600000",
        page: int = 1,
        page_size: int = 500,
        market: str = "all",
        trade_date: date | None = None,
        limit: int = 160,
        period: str = "day",
        server: str = "web",
        theme_code: str = "",
    ) -> dict[str, Any]:
        observed = self._clock()
        today = observed.astimezone(SHANGHAI).date()
        if endpoint not in PURPOSES or not 1 <= page <= 100:
            raise AuctionSourcesInvalid("invalid source page")
        if not 1 <= page_size <= (6000 if endpoint == "quotes" else 1000):
            raise AuctionSourcesInvalid("invalid source page size")
        if endpoint in {"limit_up", "limit_down", "broken_limit"} and (
            trade_date is None or trade_date > today
        ):
            raise AuctionSourcesInvalid("invalid source date")
        if not 1 <= limit <= 640 or period not in {"day", "week"} or server not in {"web", "ifzq"}:
            raise AuctionSourcesInvalid("invalid kline selection")
        params: dict[str, Any]
        provider = "eastmoney"
        milliseconds = int(observed.timestamp() * 1000)
        if endpoint == "quotes":
            if market not in MARKETS:
                raise AuctionSourcesInvalid("unsupported market")
            url = "https://push2dycalc.eastmoney.com/api/qt/clist/get"
            params = {
                "fs": MARKETS[market],
                "fltt": 2,
                "invt": 2,
                "fields": FIELDS,
                "fid": "f3",
                "po": 1,
                "pn": page,
                "pz": page_size,
                "np": 1,
                "ut": "c92c50e6b0fab2c17cd5e276e9a79c42",
                "_": milliseconds,
            }
        elif endpoint in {"limit_up", "limit_down", "broken_limit"}:
            assert trade_date is not None
            name = {
                "limit_up": "getTopicZTPool",
                "limit_down": "getTopicDTPool",
                "broken_limit": "getTopicZBPool",
            }[endpoint]
            url = "https://push2ex.eastmoney.com/" + name
            params = {
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": page - 1,
                "Pagesize": page_size,
                "sort": "fund:asc" if endpoint == "limit_down" else "fbt:asc",
                "date": trade_date.strftime("%Y%m%d"),
                "_": milliseconds,
            }
        elif endpoint in {"eastmoney_kline", "tencent_kline", "sina_kline", "minute"}:
            tcode, secid = _source_symbol(symbol)
            if endpoint == "eastmoney_kline":
                url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
                params = {
                    "secid": secid,
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                    "klt": 101,
                    "fqt": 1,
                    "end": "20500101",
                    "lmt": limit,
                    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                    "_": milliseconds,
                }
            elif endpoint == "tencent_kline":
                provider = "tencent"
                host = "web.ifzq" if server == "web" else "ifzq"
                url = f"https://{host}.gtimg.cn/appstock/app/fqkline/get"
                params = {"param": f"{tcode},{period},,,{limit},qfq"}
            elif endpoint == "minute":
                provider, url, params = (
                    "tencent",
                    "https://web.ifzq.gtimg.cn/appstock/app/minute/query",
                    {"code": tcode},
                )
            else:
                provider, url = (
                    "sina",
                    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
                )
                params = {"symbol": tcode, "scale": 240, "ma": "no", "datalen": limit}
        elif endpoint == "index":
            provider, url, params = "tencent", "https://qt.gtimg.cn/", {"q": "sh000001"}
        else:
            args: dict[str, Any] = {"pageSize": page_size, "pageNum": page, "sort": -1}
            if endpoint == "stocks":
                if not re.fullmatch(r"[0-9A-Za-z]{1,32}", theme_code):
                    raise AuctionSourcesInvalid("invalid theme code")
                args.update(themeCode=theme_code, sortField="f3")
            method_name = {
                "today": "getTodayChance",
                "themes": "getThemeList",
                "stocks": "getStockList",
            }[endpoint]
            url = "https://emcfgdata.eastmoney.com/api/themeInvest/" + method_name
            params = {
                "args": args,
                "client": "web",
                "clientVersion": "8.3",
                "clientType": "cfw",
                "randomCode": "QMvrYSV2BSS4uDmf",
                "timestamp": milliseconds,
            }
        is_post = endpoint in {"today", "themes", "stocks"}
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        if is_post:
            headers.update(
                {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Origin": "https://emrnweb.eastmoney.com",
                    "Referer": "https://emrnweb.eastmoney.com/",
                }
            )
        request = Request(
            url if is_post else url + "?" + urlencode(params),
            data=json.dumps(params).encode("utf-8") if is_post else None,
            headers=headers,
            method="POST" if is_post else "GET",
        )
        try:
            body = self._request(request, self._timeout)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("oversized source response")
            if endpoint == "index":
                # Tencent text quotes use GBK, unlike the JSON data services.
                result = self._index(body.decode("gbk"))
            else:
                data = json.loads(
                    body.decode("utf-8-sig"), parse_float=Decimal, parse_constant=_invalid_constant
                )
                result = self._normalize(
                    endpoint, data, symbol, trade_date, page_size, limit, period
                )
            identities = [
                item.get("symbol", item.get("theme_code"))
                for item in result["items"]
                if "symbol" in item or "theme_code" in item
            ]
            if len(set(identities)) != len(identities):
                raise ValueError("duplicate source identity")
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            IndexError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise AuctionSourcesUpstream("source request failed") from exc
        name, website = SOURCES[provider]
        return {
            "source_code": provider,
            "source_name": name,
            "source_website": website,
            "upstream_url": url,
            "upstream_method": "POST" if is_post else "GET",
            "upstream_parameters": params,
            "purpose": PURPOSES[endpoint],
            "persisted": False,
            "observed_at": self._clock(),
            "page": page,
            "page_size": (
                limit
                if endpoint.endswith("kline")
                else len(result["items"])
                if endpoint in {"minute", "index"}
                else page_size
            ),
            "trade_date": trade_date,
            "symbol": symbol
            if endpoint in {"minute", "eastmoney_kline", "tencent_kline", "sina_kline"}
            else None,
            "adjustment": None,
            "total": None,
        } | result

    def _normalize(
        self,
        endpoint: Endpoint,
        value: object,
        symbol: str,
        requested_day: date | None,
        page_size: int,
        limit: int,
        period: str,
    ) -> dict[str, Any]:
        if endpoint == "sina_kline":
            rows = _rows(value, 1000)
            items = [
                _bar([r["day"], r["open"], r["close"], r["high"], r["low"], r["volume"]], 1)
                for r in map(_mapping, rows)
            ]
            return self._bars(items[-limit:], "unspecified")
        obj = _mapping(value)
        status = obj.get(
            "code" if endpoint in {"today", "themes", "stocks", "minute", "tencent_kline"} else "rc"
        )
        if type(status) is not int or status != 0:
            raise ValueError("source business status failure")
        if endpoint == "today":
            rows = _rows(obj["data"], 100)
            return {"items": [_theme(_mapping(r), endpoint) for r in rows]}
        data = _mapping(obj["data"])
        if endpoint == "quotes":
            rows = _rows(data["diff"], page_size)
            return {
                "items": [_quote(_mapping(r)) for r in rows],
                "total": _count(data.get("total")),
            }
        if endpoint in {"limit_up", "limit_down", "broken_limit"}:
            source_day = _day(data["qdate"])
            if source_day != requested_day:
                raise ValueError("source pool date mismatch")
            rows = _rows(data["pool"], page_size)
            return {
                "items": [_pool(_mapping(r), endpoint) for r in rows],
                "total": _count(data.get("tc")),
                "trade_date": source_day,
            }
        if endpoint == "eastmoney_kline":
            if data.get("code") not in (None, symbol.partition(":")[2]):
                raise ValueError("source security mismatch")
            rows = _rows(data["klines"], 1000)
            if any(not isinstance(row, str) for row in rows):
                raise ValueError("invalid bar string")
            return self._bars(
                [_bar(r.split(","), 100, eastmoney=True) for r in rows][-limit:], "forward"
            )
        if endpoint == "tencent_kline":
            tcode, _ = _source_symbol(symbol)
            stock = _mapping(data[tcode])
            adjusted = "qfq" + period
            key = adjusted if adjusted in stock else period
            rows = _rows(stock[key], 1000)
            return self._bars(
                [_bar(_rows(r, 30), 100) for r in rows][-limit:],
                "forward" if key == adjusted else "none",
            )
        if endpoint == "minute":
            tcode, _ = _source_symbol(symbol)
            minute = _mapping(_mapping(data[tcode])["data"])
            source_day = _day(minute["date"])
            if source_day > self._clock().astimezone(SHANGHAI).date():
                raise ValueError("future minute date")
            items = []
            previous_volume = previous_amount = Decimal(0)
            previous_time = ""
            for raw in _rows(minute["data"], 500):
                if not isinstance(raw, str):
                    raise ValueError("invalid minute row")
                parts = raw.split()
                if len(parts) not in (3, 4) or not re.fullmatch(r"[0-9]{4}", parts[0]):
                    raise ValueError("invalid minute row")
                minute_time = datetime.strptime(parts[0], "%H%M").strftime("%H:%M")
                price = _number(parts[1], nonnegative=True)
                volume = _number(parts[2], nonnegative=True)
                amount = _number(parts[3], nonnegative=True) if len(parts) == 4 else None
                if (
                    price is None
                    or volume is None
                    or volume < previous_volume
                    or (amount is not None and amount < previous_amount)
                    or minute_time <= previous_time
                ):
                    raise ValueError("unordered cumulative minute data")
                items.append(
                    {
                        "time": minute_time,
                        "price": price,
                        "cumulative_volume_shares": volume * 100,
                        "cumulative_amount_cny": amount,
                    }
                )
                previous_volume, previous_time = volume, minute_time
                if amount is not None:
                    previous_amount = amount
            return {"trade_date": source_day, "items": items}
        if endpoint == "themes":
            rows = _rows(data["list"], page_size)
            return {
                "items": [_theme(_mapping(r), endpoint) for r in rows],
                "total": _count(data.get("total")),
            }
        if endpoint == "stocks":
            rows = _rows(data["stockList"] if "stockList" in data else data["list"], page_size)
            return {
                "items": [_theme_stock(_mapping(r)) for r in rows],
                "total": _count(data.get("total", data.get("stockNum"))),
            }
        raise ValueError("unsupported source normalization")

    def _bars(self, items: list[dict[str, Any]], adjustment: str) -> dict[str, Any]:
        dates = [i["trade_date"] for i in items]
        if any(day > self._clock().astimezone(SHANGHAI).date() for day in dates):
            raise ValueError("future source bar")
        if any(a >= b for a, b in pairwise(dates)):
            raise ValueError("duplicate or unordered bars")
        return {"items": items, "adjustment": adjustment}

    @staticmethod
    def _index(text: str) -> dict[str, Any]:
        match = re.fullmatch(r'\s*v_sh000001="([^"\r\n]+)";?\s*', text)
        if match is None:
            raise ValueError("invalid index text")
        parts = match[1].split("~")
        if len(parts) < 36:
            raise ValueError("short index text")
        return {
            "items": [
                {
                    "index_code": "sh000001",
                    "name": _text(parts[1]),
                    "current": _number(parts[3], nonnegative=True),
                    "previous_close": _number(parts[4], nonnegative=True),
                    "change_pct": _number(parts[32]),
                }
            ]
        }
