"""Synthetic command-2501 frames; no stored market data or live network tests."""

import ssl
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from market_data_center.providers import kaipanla_boards_socket as tls


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, value):
    if isinstance(value, int):
        return varint(number << 3) + varint(value)
    if isinstance(value, str):
        value = value.encode()
    return varint((number << 3) | 2) + varint(len(value)) + value


def page(*, offset=0, limit=2, total=3, day="2026-10-26", board="803023", indices=None):
    count = min(limit, max(0, total - offset))
    values = [""] * 54
    for index, value in {1: "1.23", 3: "0", 9: "-1", 44: "1.1234567890123456789"}.items():
        values[index] = value
    payload = field(1, board) + field(11, total) + field(13, day)
    if offset:
        payload += field(6, offset)
    for i in range(min(total, 100)):
        payload += field(20, f"{i + 1:06d}")
    payload += field(21, b"".join(varint(i) for i in (indices or range(offset, offset + count))))
    for i in range(offset, offset + count):
        stock = field(1, f"{i + 1:06d}") + field(2, "Example") + field(6, "leader")
        stock += b"".join(field(100, v) for v in values)
        payload += field(22, stock)
    for year in (2026, 2027, 2028):
        payload += field(23, f"{year}年预测净利润")
    descriptor = f"hqList|1:20030/2501-0/{board}:6:1:0:0:0:0:0".encode()
    return (
        bytes.fromhex("001709c50000030209c5")
        + b"\x03"
        + len(descriptor).to_bytes(2, "big")
        + descriptor
        + payload
    )


@pytest.mark.parametrize(
    "offset,limit,total", [(0, 1, 3), (1, 2, 3), (2, 60, 3), (10000, 2, 3), (0, 2, 0)]
)
def test_parse_page_preserves_source_date_paging_mapping_and_missing(offset, limit, total):
    result = tls.parse_page(page(offset=offset, limit=limit, total=total), "803023", offset, limit)
    assert result["Day"] == "2026-10-26"
    assert len(result["list"]) == min(limit, max(0, total - offset))
    if result["list"]:
        row = result["list"][0]
        assert row[5] == "1.23" and row[7] == "0" and row[13] == "-1"
        assert row[44] == "1.1234567890123456789"
        assert row[24] == "leader" and row[22] is None


def test_request_encoding_matches_observed_frame_and_variable_page():
    assert (
        tls.query("803023", 0, 60).hex() == "400018001709c50000030209c50a0638303330323310061801383c"
    )
    assert tls.query("803023", 128, 2).endswith(b"\x30\x80\x01\x38\x02")


@pytest.mark.parametrize(
    "payload",
    [b"", b"x" * 30, page()[:-1], page(board="803024"), page(indices=[1, 2]), page(total=6001)],
)
def test_bad_frames_fail_instead_of_empty_success(payload):
    with pytest.raises(ValueError):
        tls.parse_page(payload, "803023", 0, 2)


def test_today_members_api_uses_tls_and_common_decimal_validation():
    from test_kaipanla_boards import BASE, KEY, Transport, client

    calls = []

    def request_members(board, offset, limit, timeout):
        calls.append((board, offset, limit, timeout))
        return tls.parse_page(page(day="2026-09-29", limit=limit), board, offset, limit)

    transport = Transport()
    api = client(transport)
    api.app.state.kaipanla_boards_provider._request_members = request_members
    response = api.get(
        BASE + "/803023/members/history",
        params={"trade_date": "2026-09-29", "limit": 2},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["query_mode"] == "history" and data["requested_date"] == data["trade_date"]
    assert data["total"] is None and data["next_offset"] == 2
    assert data["items"][0]["profit_forecasts"][0]["net_profit_cny"] == "1.1234567890123456789"
    assert data["items"][0]["amount_cny"] == "0"
    assert len(calls) == 1 and transport.calls == []


def test_today_source_date_mismatch_is_502():
    from market_data_center.providers.kaipanla_boards import (
        KaipanlaBoardsProvider,
        KaipanlaBoardsUpstream,
    )

    provider = KaipanlaBoardsProvider(
        request_members=lambda *args: tls.parse_page(page(day="2020-01-01"), "803023", 0, 2),
        clock=lambda: datetime(2026, 10, 26, 1, tzinfo=UTC),
    )
    with pytest.raises(KaipanlaBoardsUpstream):
        provider.members("803023", trade_date=date(2026, 10, 26), limit=2)


@pytest.mark.parametrize(
    "payload",
    [
        page() + field(11, 3),
        page().replace(field(11, 3), field(11, "3")),
        page().replace(field(1, "000002"), field(1, "000001")),
        page().replace(field(20, "000002"), field(20, "000001")),
        page().replace(field(1, "000002"), field(1, "000004")),
        page().replace(field(100, "1.23"), field(100, 123)),
        page() + field(6, 1),
        page().replace(field(13, "2026-10-26"), field(13, 123)),
        page() + b"\x00",
        page() + b"\xff" * 11,
        page() + b"\x0a\xff\xff\x01",
        page() + field(25, 0) * (tls.MAX_FIELDS + 1),
        b"x" * (tls.MAX_FRAME_BYTES + 1),
    ],
    ids=lambda _: "corrupt",
)
def test_corrupt_scalar_directory_columns_and_resource_bounds(payload):
    with pytest.raises(ValueError):
        tls.parse_page(payload, "803023", 0, 2)


class Connection:
    def __init__(self, body, clock, *, fragment=10000, latency=0.01, handshake_error=None):
        self.body = body
        self.clock = clock
        self.fragment = fragment
        self.latency = latency
        self.handshake_error = handshake_error
        self.timeouts = []
        self.sent = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def do_handshake(self):
        self.clock[0] += 1
        if self.handshake_error:
            raise self.handshake_error

    def sendall(self, data):
        self.clock[0] += 1
        self.sent.append(data)

    def recv(self, size):
        self.clock[0] += self.latency
        size = min(size, self.fragment)
        chunk, self.body = self.body[:size], self.body[size:]
        return chunk


def install_transport(monkeypatch, body, **kwargs):
    clock = [100.0]
    conn = Connection(body, clock, **kwargs)
    connects = []

    def connect(endpoint, timeout):
        connects.append((endpoint, timeout))
        clock[0] += 1
        return conn

    class Context:
        def wrap_socket(self, raw, *, server_hostname, do_handshake_on_connect):
            assert raw is conn and server_hostname == tls.SERVER_NAME
            assert do_handshake_on_connect is False
            return conn

    monkeypatch.setattr(tls, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(tls, "socket", SimpleNamespace(create_connection=connect))
    monkeypatch.setattr(tls, "_tls_context", Context)
    return conn, connects


def frame(body):
    return b"\x40" + len(body).to_bytes(2, "big") + body


def test_fragmented_tls_frame_uses_one_connection_and_shared_deadline(monkeypatch):
    conn, connects = install_transport(
        monkeypatch, b"\x11\x00\x00" * 8 + frame(page()), fragment=17
    )
    data = tls.request_members("803023", 0, 2, 8)
    assert len(data["list"]) == 2
    assert connects == [(tls.ENDPOINT, 8)]
    assert conn.sent == [tls.query("803023", 0, 2)] and conn.closed
    assert conn.timeouts[0] == 7 and conn.timeouts[1] == 6
    assert conn.timeouts == sorted(conn.timeouts, reverse=True)


def test_slow_response_cannot_reset_total_timeout(monkeypatch):
    conn, connects = install_transport(monkeypatch, frame(page()), fragment=1, latency=0.6)
    with pytest.raises(TimeoutError):
        tls.request_members("803023", 0, 2, 8)
    assert conn.closed and len(connects) == 1 and len(conn.sent) == 1
    assert conn.body  # Deadline expires while the source is still providing bytes.


@pytest.mark.parametrize(
    "body", [b"\x11\x00\x00" * 9, b"\x41\x00\x10", b"\x40\x00\x01", frame(page())[:-10]]
)
def test_invalid_or_truncated_wire_frames_close_without_retry(monkeypatch, body):
    conn, connects = install_transport(monkeypatch, body)
    with pytest.raises(ValueError):
        tls.request_members("803023", 0, 2, 8)
    assert conn.closed and len(connects) == 1


def test_pinned_certificate_context_keeps_tls_verification_enabled():
    context = tls._tls_context()
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.cert_store_stats()["x509"] == 1


def test_certificate_failure_is_sanitized_at_api_boundary(monkeypatch):
    from test_kaipanla_boards import BASE, KEY, Transport, client

    conn, connects = install_transport(
        monkeypatch, b"", handshake_error=ssl.SSLCertVerificationError("private-certificate-detail")
    )
    transport = Transport()
    api = client(transport)
    response = api.get(
        BASE + "/803023/members/history",
        params={"trade_date": "2026-09-29"},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert "private-certificate-detail" not in response.text
    assert len(connects) == 1 and conn.sent == [] and conn.closed and transport.calls == []


@pytest.mark.parametrize(
    "params,headers,status",
    [
        ({"trade_date": "2026-09-30"}, True, 422),
        ({"trade_date": "2026-09-29", "limit": 61}, True, 422),
        ({"trade_date": "2026-09-29", "offset": -1}, True, 422),
        ({"trade_date": "2026-09-29"}, False, 401),
    ],
)
def test_invalid_or_unauthorized_requests_never_open_tls(monkeypatch, params, headers, status):
    from test_kaipanla_boards import BASE, KEY, Transport, client

    _, connects = install_transport(monkeypatch, b"")
    response = client(Transport()).get(
        BASE + "/803023/members/history",
        params=params,
        headers={"X-API-Key": KEY} if headers else {},
    )
    assert response.status_code == status and connects == []
