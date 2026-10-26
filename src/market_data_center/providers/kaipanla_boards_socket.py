"""Anonymous, single-page Kaipanla command 2501 with a pinned trust certificate."""

import re
import socket
import ssl
import time
from importlib.resources import files

ENDPOINT = ("124.71.166.244", 14000)
SERVER_NAME = "apphwshhq.kaipanla.com"
MAX_FRAME_BYTES = 65535
MAX_FIELDS = 8192


def _varint(value: int) -> bytes:
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def query(board_code: str, offset: int, limit: int) -> bytes:
    if (
        not re.fullmatch(r"[0-9]{6}", board_code)
        or not 0 <= offset <= 10000
        or not 1 <= limit <= 60
    ):
        raise ValueError("invalid member request")
    body = bytes.fromhex("001709c50000030209c5")
    body += b"\x0a\x06" + board_code.encode("ascii") + b"\x10\x06\x18\x01"
    if offset:
        body += b"\x30" + _varint(offset)
    body += b"\x38" + _varint(limit)
    return b"\x40" + len(body).to_bytes(2, "big") + body


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if pos >= len(data):
            raise ValueError("truncated varint")
        byte = data[pos]
        pos += 1
        if shift == 63 and byte > 1:
            raise ValueError("varint overflow")
        value |= (byte & 127) << shift
        if byte < 128:
            return value, pos
    raise ValueError("varint overflow")


def _fields(data: bytes) -> dict[int, list[int | bytes]]:
    result: dict[int, list[int | bytes]] = {}
    pos = 0
    count = 0
    while pos < len(data):
        count += 1
        if count > MAX_FIELDS:
            raise ValueError("too many source fields")
        tag, pos = _read_varint(data, pos)
        number, wire = tag >> 3, tag & 7
        if not 1 <= number < 2**29:
            raise ValueError("invalid field number")
        value: int | bytes
        if wire == 0:
            value, pos = _read_varint(data, pos)
        elif wire in (1, 2, 5):
            if wire == 2:
                size, pos = _read_varint(data, pos)
            else:
                size = 8 if wire == 1 else 4
            if pos + size > len(data):
                raise ValueError("truncated source field")
            value = data[pos : pos + size]
            pos += size
            # Known fields use varint or length-delimited wire types only.
            if wire != 2:
                raise ValueError("unsupported source wire type")
        else:
            raise ValueError("unsupported source wire type")
        result.setdefault(number, []).append(value)
    return result


def _one(
    fields: dict[int, list[int | bytes]], number: int, default: int | bytes | None = None
) -> int | bytes:
    values = fields.get(number, [] if default is None else [default])
    if len(values) != 1:
        raise ValueError("missing or repeated scalar field")
    return values[0]


def _text(value: int | bytes) -> str:
    if not isinstance(value, bytes):
        raise ValueError("invalid string wire type")
    return value.decode("utf-8")


def parse_page(body: bytes, board_code: str, offset: int, limit: int) -> dict[str, object]:
    if not 13 <= len(body) <= MAX_FRAME_BYTES:
        raise ValueError("invalid source frame length")
    if (
        body[:4] != bytes.fromhex("001709c5")
        or body[7:10] != bytes.fromhex("0209c5")
        or body[10] != 3
    ):
        raise ValueError("unexpected source command")
    size = int.from_bytes(body[11:13], "big")
    if size > 256 or 13 + size > len(body):
        raise ValueError("invalid descriptor length")
    descriptor = body[13 : 13 + size].decode("utf-8")
    if not re.fullmatch(
        r"hqList\|[0-9]+:20030/2501-0/" + re.escape(board_code) + r":6:1:0:0:0:0:0", descriptor
    ):
        raise ValueError("mismatched source descriptor")
    fields = _fields(body[13 + size :])
    if _text(_one(fields, 1)) != board_code or _one(fields, 6, 0) != offset:
        raise ValueError("mismatched board or offset")
    total = _one(fields, 11)
    if not isinstance(total, int) or not 0 <= total <= 6000:
        raise ValueError("invalid source total")
    actual = _text(_one(fields, 13))
    codes = [_text(v) for v in fields.get(20, [])]
    if (
        len(codes) != min(total, 100)
        or len(set(codes)) != len(codes)
        or any(not re.fullmatch(r"[0-9]{6}", c) for c in codes)
    ):
        raise ValueError("invalid source code directory")
    expected = min(limit, max(0, total - offset))
    stocks = fields.get(22, [])
    if len(stocks) != expected:
        raise ValueError("ignored page bounds or incomplete page")
    packed_parts = fields.get(21, [])
    if any(not isinstance(p, bytes) for p in packed_parts):
        raise ValueError("invalid packed indices")
    packed = b"".join(p for p in packed_parts if isinstance(p, bytes))
    indices: list[int] = []
    pos = 0
    while pos < len(packed):
        value, pos = _read_varint(packed, pos)
        indices.append(value)
        if len(indices) > limit:
            raise ValueError("too many row indices")
    if indices != list(range(offset, offset + expected)):
        raise ValueError("mismatched row indices")
    rows: list[list[object]] = []
    mapping = {i: i + 4 for i in range(18)}
    mapping.update({18: 23, 19: 25, 21: 28, 22: 29, 24: 33, 28: 37, 29: 38, 33: 42})
    mapping.update({i: i for i in (40, 44, 45, 46, 47, 48, 50, 52, 53)})
    for raw in stocks:
        if not isinstance(raw, bytes):
            raise ValueError("invalid stock wire type")
        stock = _fields(raw)
        values = [_text(v) for v in stock.get(100, [])]
        if not 54 <= len(values) <= 100:
            raise ValueError("invalid stock column count")
        row: list[object] = [None] * 63
        row[0], row[1] = _text(_one(stock, 1)), _text(_one(stock, 2))
        row[24] = _text(_one(stock, 6, b""))
        for source, dest in mapping.items():
            row[dest] = values[source]
        rows.append(row)
    row_codes = [r[0] for r in rows]
    if len(set(row_codes)) != len(rows) or any(
        not isinstance(c, str) or not re.fullmatch(r"[0-9]{6}", c) for c in row_codes
    ):
        raise ValueError("invalid or duplicate stock code")
    overlap = max(0, min(len(codes) - offset, len(rows)))
    if row_codes[:overlap] != codes[offset : offset + overlap]:
        raise ValueError("page differs from source directory")
    labels = [_text(v) for v in fields.get(23, [])]
    if len(labels) > 100:
        raise ValueError("too many forecast labels")
    return {"errcode": 0, "Day": actual, "list": rows, "ZB": labels}


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("member request deadline exceeded")
    return remaining


def _read_exact(connection: ssl.SSLSocket, size: int, deadline: float) -> bytes:
    result = bytearray()
    while len(result) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise ValueError("truncated TLS frame")
        result.extend(chunk)
    _remaining(deadline)
    return bytes(result)


def _tls_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    certificate = (
        files("market_data_center.providers")
        .joinpath("certificates/kpl-quote.pem")
        .read_text(encoding="ascii")
    )
    context.load_verify_locations(cadata=certificate)
    return context


def request_members(board_code: str, offset: int, limit: int, timeout: float) -> dict[str, object]:
    if not 0 < timeout <= 10:
        raise ValueError("invalid member timeout")
    deadline = time.monotonic() + timeout
    request = query(board_code, offset, limit)
    context = _tls_context()
    with (
        socket.create_connection(ENDPOINT, timeout=_remaining(deadline)) as raw,
        context.wrap_socket(
            raw, server_hostname=SERVER_NAME, do_handshake_on_connect=False
        ) as connection,
    ):
        connection.settimeout(_remaining(deadline))
        connection.do_handshake()
        connection.settimeout(_remaining(deadline))
        connection.sendall(request)
        for _ in range(9):
            header = _read_exact(connection, 3, deadline)
            if header == b"\x11\x00\x00":
                continue
            size = int.from_bytes(header[1:], "big")
            if header[0] != 0x40 or not 13 <= size <= MAX_FRAME_BYTES:
                raise ValueError("invalid TLS frame")
            body = _read_exact(connection, size, deadline)
            result = parse_page(body, board_code, offset, limit)
            _remaining(deadline)
            return result
    raise ValueError("too many source heartbeat frames")
