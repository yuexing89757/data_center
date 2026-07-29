"""Dedicated AKShare/THS adapter for the explicit dynamic board-index catalog."""

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from json import JSONDecodeError, loads
from re import DOTALL, search
from types import TracebackType
from typing import Protocol, Self, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from market_data_center.domain.board_index import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexProviderRecord,
    BoardIndexRecord,
    BoardIndexStatus,
    BoardIndexType,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow

THS_BOARD_ID = "THS:883423"
THS_BOARD_CODE = "883423"
THS_BOARD_NAME = "沪深主板昨日涨停"
SOURCE_CODE = "akshare_ths"
SHANGHAI_TIME_ZONE = ZoneInfo("Asia/Shanghai")
_CONSTITUENT_COLUMNS = (
    "序号",
    "代码",
    "名称",
    "现价",
    "涨跌幅",
    "涨跌",
    "涨速",
    "换手",
    "量比",
    "振幅",
    "成交额",
    "流通股",
    "流通市值",
    "市盈率",
)


class AKShareTHSClient(Protocol):
    def board_index_daily_bars(
        self, board_code: str, start_date: date, end_date: date
    ) -> Sequence[Mapping[str, object]]: ...

    def board_index_constituents(self, board_code: str) -> Sequence[Mapping[str, object]]: ...


class HTTPAKShareTHSClient:
    """THS protocol access isolated behind the AKShare-specific adapter boundary."""

    _user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/89.0.4389.90 Safari/537.36"
    )

    def board_index_daily_bars(
        self, board_code: str, start_date: date, end_date: date
    ) -> Sequence[Mapping[str, object]]:
        rows: list[Mapping[str, object]] = []
        for year in range(start_date.year, end_date.year + 1):
            url = f"https://d.10jqka.com.cn/v4/line/bk_{board_code}/01/{year}.js"
            payload = self._request(
                url,
                headers={
                    "User-Agent": self._user_agent,
                    "Referer": "https://q.10jqka.com.cn/",
                },
                encoding="utf-8",
            )
            rows.extend(_parse_daily_payload(payload, start_date, end_date))
        return rows

    def board_index_constituents(self, board_code: str) -> Sequence[Mapping[str, object]]:
        detail_url = f"https://q.10jqka.com.cn/thshy/detail/code/{board_code}/"
        detail = self._request(
            detail_url,
            headers={"User-Agent": self._user_agent},
            encoding="gb18030",
        )
        rows = list(_parse_constituent_rows(detail))
        page_count = _parse_page_count(detail)
        if page_count <= 1:
            return rows

        headers = {
            "User-Agent": self._user_agent,
            "Referer": detail_url,
        }
        for page in range(2, page_count + 1):
            page_url = (
                f"https://q.10jqka.com.cn/thshy/detail/code/{board_code}/"
                f"field/199112/order/desc/page/{page}/"
            )
            payload = self._request(page_url, headers=headers, encoding="gb18030")
            page_rows = _parse_constituent_rows(payload)
            if not page_rows:
                raise ProviderError(f"THS constituent page {page} returned no parseable rows")
            rows.extend(page_rows)
        return rows

    @staticmethod
    def _request(url: str, *, headers: Mapping[str, str], encoding: str) -> str:
        try:
            with urlopen(Request(url, headers=dict(headers)), timeout=20) as response:
                payload = cast(bytes, response.read())
        except HTTPError as error:
            raise ProviderError(f"THS request failed with HTTP {error.code}") from error
        except (TimeoutError, URLError) as error:
            raise ProviderError("THS request failed") from error
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError as error:
            raise ProviderError("THS response encoding changed") from error


class AKShareTHSProvider(AbstractContextManager["AKShareTHSProvider"]):
    source_code = SOURCE_CODE

    def __init__(
        self,
        client: AKShareTHSClient,
        *,
        today: Callable[[], date] = lambda: datetime.now(SHANGHAI_TIME_ZONE).date(),
    ) -> None:
        self._client = client
        self._today = today

    @classmethod
    def default(cls) -> Self:
        return cls(HTTPAKShareTHSClient())

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def fetch_board_indexes(self) -> ProviderBatch[BoardIndexProviderRecord]:
        raw = {
            "board_id": THS_BOARD_ID,
            "board_code": THS_BOARD_CODE,
            "namespace": "THS",
            "name": THS_BOARD_NAME,
            "board_type": BoardIndexType.DYNAMIC_THEME.value,
            "market": Market.CN_A_SHARE.value,
            "status": BoardIndexStatus.ACTIVE.value,
        }
        return ProviderBatch(
            raw_rows=[raw],
            request_params={"board_id": THS_BOARD_ID},
            schema_version="akshare_ths.board_index.v1",
            records=[_map_board_index(raw)],
        )

    def fetch_board_index_daily_bars(
        self, board_id: str, start_date: date, end_date: date
    ) -> ProviderBatch[BoardIndexProviderRecord]:
        _ensure_date_range(start_date, end_date)
        board_code = _board_code(board_id)
        source_rows = self._client.board_index_daily_bars(board_code, start_date, end_date)
        rows = _raw_rows(source_rows)
        if not rows:
            raise ProviderError("THS board-index daily-bar response is empty")
        required = {"日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额"}
        _require_fields(rows, required, "board-index daily bars")
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "board_id": THS_BOARD_ID,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "adjust": "",
            },
            schema_version="akshare_ths.board_index_daily_bar.v1",
            record_factory=lambda: [_map_daily_bar(row) for row in rows],
        )

    def fetch_board_index_constituents(
        self, board_id: str, snapshot_date: date
    ) -> ProviderBatch[BoardIndexProviderRecord]:
        board_code = _board_code(board_id)
        if snapshot_date != self._today():
            raise ProviderError(
                "THS exposes only the current constituent snapshot; "
                "historical dates must come from previously captured Raw data"
            )
        source_rows = self._client.board_index_constituents(board_code)
        rows = _raw_rows(source_rows)
        if not rows:
            raise ProviderError("THS board-index constituent response is empty")
        _require_fields(rows, {"代码"}, "board-index constituents")
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "board_id": THS_BOARD_ID,
                "snapshot_date": snapshot_date.isoformat(),
            },
            schema_version="akshare_ths.board_index_constituent_snapshot.v1",
            records=[_map_constituents(rows, snapshot_date)],
        )


def normalize_akshare_ths_raw(
    dataset_code: DatasetCode,
    schema_version: str,
    raw_rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[BoardIndexProviderRecord, ...]:
    expected = {
        DatasetCode.BOARD_INDEX: "akshare_ths.board_index.v1",
        DatasetCode.BOARD_INDEX_DAILY_BAR: "akshare_ths.board_index_daily_bar.v1",
        DatasetCode.BOARD_INDEX_CONSTITUENT_SNAPSHOT: (
            "akshare_ths.board_index_constituent_snapshot.v1"
        ),
    }.get(dataset_code)
    if expected is None or schema_version != expected:
        raise ProviderError(f"unsupported AKShare THS Raw schema: {schema_version}")
    board_id = request_params.get("board_id")
    if board_id != THS_BOARD_ID:
        raise ProviderError("AKShare THS replay request has an unsupported board_id")
    if dataset_code is DatasetCode.BOARD_INDEX:
        if len(raw_rows) != 1:
            raise ProviderError("AKShare THS board catalog Raw must contain one row")
        return (_map_board_index(raw_rows[0]),)
    if dataset_code is DatasetCode.BOARD_INDEX_DAILY_BAR:
        return tuple(_map_daily_bar(row) for row in raw_rows)
    snapshot_date = _request_date(request_params, "snapshot_date")
    return (_map_constituents(raw_rows, snapshot_date),)


def _map_board_index(row: Mapping[str, str]) -> BoardIndexRecord:
    if (
        row.get("board_id") != THS_BOARD_ID
        or row.get("board_code") != THS_BOARD_CODE
        or row.get("namespace") != "THS"
        or row.get("name") != THS_BOARD_NAME
    ):
        raise ProviderError("THS explicit board-index directory does not match ADR-0003")
    return BoardIndexRecord(
        board_id=THS_BOARD_ID,
        board_code=THS_BOARD_CODE,
        namespace="THS",
        name=THS_BOARD_NAME,
        board_type=BoardIndexType.DYNAMIC_THEME,
        market=Market.CN_A_SHARE,
        status=BoardIndexStatus.ACTIVE,
        source_code=SOURCE_CODE,
    )


def _map_daily_bar(row: Mapping[str, str]) -> BoardIndexDailyBarRecord:
    return BoardIndexDailyBarRecord(
        board_id=THS_BOARD_ID,
        trade_date=_date_value(row.get("日期"), "日期"),
        market=Market.CN_A_SHARE,
        open=_decimal_value(row.get("开盘价"), "开盘价"),
        high=_decimal_value(row.get("最高价"), "最高价"),
        low=_decimal_value(row.get("最低价"), "最低价"),
        close=_decimal_value(row.get("收盘价"), "收盘价"),
        volume=_integer_value(row.get("成交量"), "成交量"),
        amount=_decimal_value(row.get("成交额"), "成交额"),
        source_code=SOURCE_CODE,
    )


def _map_constituents(
    rows: Sequence[Mapping[str, str]], snapshot_date: date
) -> BoardIndexConstituentSnapshotRecord:
    members = tuple(_standard_symbol(row.get("代码")) for row in rows)
    return BoardIndexConstituentSnapshotRecord(
        board_id=THS_BOARD_ID,
        trade_date=snapshot_date,
        members=members,
        source_code=SOURCE_CODE,
    )


def _parse_daily_payload(
    payload: str, start_date: date, end_date: date
) -> tuple[dict[str, str], ...]:
    start = payload.find("{")
    end = payload.rfind("}")
    if start < 0 or end <= start:
        raise ProviderError("THS daily-bar payload wrapper changed")
    try:
        decoded = loads(payload[start : end + 1])
    except JSONDecodeError as error:
        raise ProviderError("THS daily-bar payload is not valid JSON") from error
    data = decoded.get("data") if isinstance(decoded, dict) else None
    if not isinstance(data, str):
        raise ProviderError("THS daily-bar payload has no data field")
    rows: list[dict[str, str]] = []
    for item in data.split(";"):
        values = item.split(",")
        if len(values) < 7:
            raise ProviderError("THS daily-bar row schema changed")
        trade_date = _date_value(values[0], "日期")
        if start_date <= trade_date <= end_date:
            rows.append(
                {
                    "日期": values[0],
                    "开盘价": values[1],
                    "最高价": values[2],
                    "最低价": values[3],
                    "收盘价": values[4],
                    "成交量": values[5],
                    "成交额": values[6],
                }
            )
    return tuple(rows)


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_tbody = False
        self.in_cell = False
        self.current_row: list[str] | None = None
        self.current_cell: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tbody":
            self.in_tbody = True
        elif self.in_tbody and tag == "tr":
            self.current_row = []
        elif self.current_row is not None and tag == "td":
            self.in_cell = True
            self.current_cell = []

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.in_cell and self.current_row is not None:
            self.current_row.append("".join(self.current_cell).strip())
            self.in_cell = False
        elif tag == "tr" and self.current_row is not None:
            if self.current_row:
                self.rows.append(self.current_row)
            self.current_row = None
        elif tag == "tbody":
            self.in_tbody = False


def _parse_constituent_rows(payload: str) -> tuple[dict[str, str], ...]:
    parser = _TableParser()
    parser.feed(payload)
    rows: list[dict[str, str]] = []
    for values in parser.rows:
        if len(values) < 3 or not values[1].strip().isdigit():
            continue
        rows.append(
            {
                column: values[index] if index < len(values) else ""
                for index, column in enumerate(_CONSTITUENT_COLUMNS)
            }
        )
    return tuple(rows)


def _parse_page_count(payload: str) -> int:
    match = search(
        r'class=["\'][^"\']*page_info[^"\']*["\'][^>]*>\s*\d+\s*/\s*(\d+)\s*<',
        payload,
        DOTALL,
    )
    if match is None:
        return 1
    return int(match.group(1))


def _raw_rows(rows: Sequence[Mapping[str, object]]) -> list[RawRow]:
    return [{str(key): _raw_value(value) for key, value in row.items()} for row in rows]


def _raw_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _require_fields(rows: Sequence[Mapping[str, str]], required: set[str], operation: str) -> None:
    missing = required.difference(rows[0])
    if missing:
        raise ProviderError(f"THS {operation} missing fields: {', '.join(sorted(missing))}")


def _ensure_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")


def _board_code(board_id: str) -> str:
    if board_id.strip().upper() != THS_BOARD_ID:
        raise ProviderError(f"unsupported THS board_id: {board_id}")
    return THS_BOARD_CODE


def _request_date(request_params: Mapping[str, object], key: str) -> date:
    value = request_params.get(key)
    if not isinstance(value, str):
        raise ProviderError(f"AKShare THS replay request is missing {key}")
    return _date_value(value, key)


def _date_value(value: str | None, field: str) -> date:
    if value is None:
        raise ProviderError(f"THS field is missing: {field}")
    compact = value.strip()
    try:
        if len(compact) == 8 and compact.isdigit():
            return datetime.strptime(compact, "%Y%m%d").date()
        return date.fromisoformat(compact[:10])
    except ValueError as error:
        raise ProviderError(f"invalid THS date field: {field}") from error


def _decimal_value(value: str | None, field: str) -> Decimal:
    if value is None or not value.strip():
        raise ProviderError(f"THS field is missing: {field}")
    try:
        return Decimal(value.strip())
    except InvalidOperation as error:
        raise ProviderError(f"invalid THS decimal field: {field}") from error


def _integer_value(value: str | None, field: str) -> int:
    number = _decimal_value(value, field)
    if number != number.to_integral_value():
        raise ProviderError(f"invalid THS integer field: {field}")
    return int(number)


def _standard_symbol(value: str | None) -> str:
    if value is None:
        raise ProviderError("THS constituent code is missing")
    code = value.strip()
    if len(code) != 6 or not code.isdigit():
        raise ProviderError("THS constituent code must contain six digits")
    if code.startswith("6"):
        exchange = "SSE"
    elif code.startswith(("0", "3")):
        exchange = "SZSE"
    elif code.startswith(("4", "8", "9")):
        exchange = "BSE"
    else:
        raise ProviderError(f"unsupported THS constituent code: {code}")
    return f"{exchange}:{code}"
