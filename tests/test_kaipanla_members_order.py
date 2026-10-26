"""Global ordering must happen before public pagination, for either source."""

import json
from datetime import date
from decimal import Decimal
from http.client import HTTPException
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from test_kaipanla_boards import DAY, LABELS, NOW, stock

from market_data_center.providers import kaipanla_boards as boards


def rows():
    result = []
    for index in range(65):
        row = stock(f"{index + 1:06d}")
        row[24] = None
        row[6] = str(index - 30)
        result.append(row)
    for index, label in [(61, "龙一"), (2, "龙二"), (64, "龙十"), (3, "龙十一")]:
        result[index][24] = label
    result[60][6] = None
    result[59][6] = "34.00000000000000000000000000001"
    return result


def setup(source_rows, *, today=False, mutate=None):
    calls = []
    day = date(2026, 9, 29) if today else DAY

    def payload(offset, limit, timeout):
        calls.append((offset, limit, timeout))
        data = dict(errcode=0, Day=str(day), list=source_rows[offset : offset + limit], ZB=LABELS)
        if mutate:
            mutate(data, offset)
        return data

    def http(req, timeout):
        q = parse_qs(urlsplit(req.full_url).query)
        return json.dumps(payload(int(q["Index"][0]), int(q["st"][0]), timeout)).encode()

    provider = boards.KaipanlaBoardsProvider(
        request_bytes=http,
        request_members=lambda board, offset, limit, timeout: payload(offset, limit, timeout),
        clock=lambda: NOW,
    )
    return provider, calls, day


@pytest.mark.parametrize("today", [False, True])
def test_global_leaders_then_gain_before_slice(today):
    provider, calls, day = setup(rows(), today=today)
    first = provider.members("803023", trade_date=day, limit=5)
    assert [i.code for i in first.items] == ["000062", "000003", "000065", "000060", "000064"]
    assert first.items[3].change_pct == Decimal("34.00000000000000000000000000001")
    assert first.next_offset == 5
    assert [(offset, limit) for offset, limit, _ in calls] == [(0, 60), (60, 60)]
    last = provider.members("803023", trade_date=day, offset=64, limit=2)
    assert [i.code for i in last.items] == ["000061"]
    assert last.next_offset is None
    assert provider.members("803023", trade_date=day, offset=10000, limit=2).items == ()


def test_exact_full_page_reads_terminal_empty_page_and_has_no_next():
    provider, calls, day = setup(rows()[:60])
    page = provider.members("803023", trade_date=day, limit=60)
    assert len(page.items) == 60 and page.next_offset is None
    assert [(offset, limit) for offset, limit, _ in calls] == [(0, 60), (60, 60)]


@pytest.mark.parametrize("problem", ["duplicate", "date", "years", "total"])
def test_later_page_failure_never_returns_partial_sorted_success(problem):
    def mutate(data, offset):
        if problem == "total":
            data["_source_total"] = 65 if offset == 0 else 66
        if offset:
            if problem == "duplicate":
                data["list"][0] = stock("000001")
            if problem == "date":
                data["Day"] = "2026-09-27"
            if problem == "years":
                data["ZB"] = []

    provider, _, day = setup(rows(), mutate=mutate)
    with pytest.raises(boards.KaipanlaBoardsUpstream):
        provider.members("803023", trade_date=day, limit=1)


def test_global_deadline_cannot_restart_for_each_page(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(boards, "time", SimpleNamespace(monotonic=lambda: clock[0]), raising=False)

    def mutate(data, offset):
        clock[0] += 5

    provider, calls, day = setup(rows(), mutate=mutate)
    with pytest.raises(boards.KaipanlaBoardsUpstream):
        provider.members("803023", trade_date=day, limit=1)
    assert [t for _, _, t in calls] == [8, 3]


def test_ties_are_by_code_and_zero_precedes_negative_and_missing():
    data = [stock(c) for c in ["000004", "000003", "000002", "000001"]]
    for row, value in zip(data, [None, "-1", "0", "0"], strict=True):
        row[24] = None
        row[6] = value
    p, _, day = setup(data)
    assert [i.code for i in p.members("803023", trade_date=day).items] == [
        "000001",
        "000002",
        "000003",
        "000004",
    ]


@pytest.mark.parametrize("slow_headers", [False, True])
def test_http_deadline_interrupts_slow_headers_and_body(monkeypatch, slow_headers):
    import socket
    import threading
    import time
    from urllib.request import Request

    client, server = socket.socketpair()
    stopped = threading.Event()
    sent = []

    class TLS:
        def __getattr__(self, name):
            return getattr(client, name)

        def do_handshake(self):
            pass

    def serve():
        try:
            server.recv(8192)
            if slow_headers:
                payload = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n"
            else:
                server.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                payload = b"x" * 100
            for byte in payload:
                if stopped.wait(0.02):
                    break
                server.sendall(bytes([byte]))
                sent.append(byte)
        except OSError:
            pass
        finally:
            server.close()

    monkeypatch.setattr(boards, "_connect_http", lambda *a, **k: client)
    import ssl

    monkeypatch.setattr(
        ssl,
        "create_default_context",
        lambda: SimpleNamespace(wrap_socket=lambda *a, **k: TLS()),
    )
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    start = time.monotonic()
    try:
        with pytest.raises((OSError, HTTPException, ValueError, boards.KaipanlaBoardsUpstream)):
            boards._request_bytes(Request(boards.HIS), 0.12)
        assert time.monotonic() - start < 0.7
        assert len(sent) >= 2
    finally:
        stopped.set()
        client.close()
        thread.join(timeout=1)


@pytest.mark.parametrize("bound", ["rows", "bytes"])
def test_collection_resource_bounds_fail_closed(monkeypatch, bound):
    def mutate(data, offset):
        data["_response_bytes"] = 100

    if bound == "rows":
        monkeypatch.setattr(boards, "MAX_MEMBER_ROWS", 60)
    else:
        monkeypatch.setattr(boards, "MAX_MEMBER_BYTES", 150)
    p, calls, day = setup(rows(), today=True, mutate=mutate)
    with pytest.raises(boards.KaipanlaBoardsUpstream):
        p.members("803023", trade_date=day, limit=1)
    assert len(calls) == 2


def test_all_ten_leader_labels_are_numeric_rank_not_lexical():
    data = []
    for index, label in enumerate(reversed("一二三四五六七八九十")):
        row = stock(f"{index + 1:06d}")
        row[24] = "龙" + label
        row[6] = None
        data.append(row)
    p, _, day = setup(data)
    page = p.members("803023", trade_date=day)
    assert [i.leader_label for i in page.items] == ["龙" + n for n in "一二三四五六七八九十"]


def test_http_dns_wait_is_bounded_and_does_not_connect(monkeypatch):
    import time

    def delayed(*args, **kwargs):
        time.sleep(0.3)
        return []

    monkeypatch.setattr(boards, "socket", SimpleNamespace(getaddrinfo=delayed, SOCK_STREAM=1))
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        boards._connect_http("apphis.longhuvip.com", start + 0.05)
    assert time.monotonic() - start < 0.2


def test_http_uses_remaining_budget_and_no_second_address(monkeypatch):
    import socket
    import time
    from unittest.mock import Mock

    raw = Mock()
    raw.connect.side_effect = TimeoutError("connect failed")
    monkeypatch.setattr(
        boards,
        "socket",
        SimpleNamespace(
            SOCK_STREAM=socket.SOCK_STREAM,
            getaddrinfo=lambda *a, **k: [
                (2, 1, 6, "", ("127.0.0.1", 443)),
                (2, 1, 6, "", ("127.0.0.2", 443)),
            ],
            socket=lambda *a: raw,
        ),
    )
    with pytest.raises(TimeoutError):
        boards._connect_http("apphis.longhuvip.com", time.monotonic() + 0.1)
    assert raw.connect.call_count == 1
    assert 0 < raw.settimeout.call_args.args[0] <= 0.10001
    raw.close.assert_called_once()
