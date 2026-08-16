import base64
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event, Lock
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import Engine

from market_data_center.domain.board_index import BoardIndexDailyBarRecord
from market_data_center.domain.records import Market
from market_data_center.providers.akshare_ths import (
    THSAnnualDailyPayload,
    THSBoardDailyBarLiveBatch,
)
from market_data_center.public_api.board_index_bias_write import (
    BoardIndexApiPersistence,
    BoardIndexPersistenceError,
    BoardIndexPersistenceQueue,
    BoardIndexPersistenceQueueFull,
    PreparedBoardIndexPersistence,
)
from market_data_center.raw_store import LocalRawStore

FETCHED_AT = datetime(2026, 8, 15, 1, 2, 3, tzinfo=UTC)


def _record(day: int = 1) -> BoardIndexDailyBarRecord:
    value = Decimal("100") + day
    return BoardIndexDailyBarRecord(
        board_id="THS:883423",
        trade_date=date(2026, 1, 1) + timedelta(days=day - 1),
        market=Market.CN_A_SHARE,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=day,
        amount=value * day,
        source_code="akshare_ths",
    )


def _batch() -> THSBoardDailyBarLiveBatch:
    payloads = (
        THSAnnualDailyPayload(
            year=2026,
            url="https://d.10jqka.com.cn/v4/line/bk_883423/01/2026.js",
            content=b"current\x00payload",
            fetched_at=FETCHED_AT,
        ),
        THSAnnualDailyPayload(
            year=2025,
            url="https://d.10jqka.com.cn/v4/line/bk_883423/01/2025.js",
            content=b"previous payload",
            fetched_at=FETCHED_AT,
        ),
    )
    return THSBoardDailyBarLiveBatch(
        payloads=payloads,
        records=tuple(_record(day) for day in range(1, 35)),
        fetched_at=FETCHED_AT,
    )


class FakeResult:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = payload

    def mappings(self) -> "FakeResult":
        return self

    def one(self) -> Mapping[str, object]:
        return {"payload": self.payload}


class FakeConnection:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = payload
        self.parameters: Mapping[str, object] | None = None

    def execute(self, statement: object, parameters: Mapping[str, object]) -> FakeResult:
        self.parameters = parameters
        return FakeResult(self.payload)


class FakeBegin:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *args: object) -> None:
        return None


class FakeEngine:
    def __init__(self) -> None:
        self.connection = FakeConnection(
            {
                "outcome": "created",
                "ingestion_id": "11111111-1111-1111-1111-111111111111",
                "raw_id": "22222222-2222-2222-2222-222222222222",
                "input_hash": "unused",
            }
        )

    def begin(self) -> FakeBegin:
        return FakeBegin(self.connection)


def test_prepare_losslessly_captures_payloads_and_serializes_standard_records(
    tmp_path: Path,
) -> None:
    engine = FakeEngine()
    persistence = BoardIndexApiPersistence(cast(Engine, engine), LocalRawStore(tmp_path), tmp_path)

    prepared = persistence.prepare(_batch())

    raw_path = tmp_path.joinpath(*Path(prepared.object_path).parts)
    rows = [json.loads(line) for line in raw_path.read_text(encoding="utf-8").splitlines()]
    assert [base64.b64decode(row["payload_base64"]) for row in rows] == [
        b"current\x00payload",
        b"previous payload",
    ]
    assert prepared.source_years_payload == "[2026,2025]"
    assert prepared.source_row_count == 2
    assert len(json.loads(prepared.records_payload)) == 34
    assert json.loads(prepared.records_payload)[0]["board_id"] == "THS:883423"
    assert len(prepared.input_hash) == 64


def test_commit_calls_only_rpc_with_prepared_manifest_and_records(tmp_path: Path) -> None:
    engine = FakeEngine()
    persistence = BoardIndexApiPersistence(cast(Engine, engine), LocalRawStore(tmp_path), tmp_path)
    prepared = persistence.prepare(_batch())
    engine.connection.payload = {
        "outcome": "created",
        "ingestion_id": str(prepared.ingestion_id),
        "raw_id": str(prepared.raw_id),
        "input_hash": prepared.input_hash,
    }

    result = persistence.commit(prepared)

    assert result["outcome"] == "created"
    assert engine.connection.parameters is not None
    assert engine.connection.parameters["input_hash"] == prepared.input_hash
    assert engine.connection.parameters["records"] == prepared.records_payload
    assert engine.connection.parameters["source_years"] == prepared.source_years_payload


class BlockingPersistence:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.lock = Lock()
        self.calls: list[UUID] = []

    def commit(self, prepared: PreparedBoardIndexPersistence) -> Mapping[str, object]:
        self.started.set()
        self.release.wait(timeout=5)
        with self.lock:
            self.calls.append(prepared.ingestion_id)
        return {
            "outcome": "created",
            "ingestion_id": prepared.ingestion_id,
            "raw_id": prepared.raw_id,
            "input_hash": prepared.input_hash,
        }

    def prepare(self, batch: THSBoardDailyBarLiveBatch) -> PreparedBoardIndexPersistence:
        raise AssertionError("not used")

    def discard(self, prepared: PreparedBoardIndexPersistence) -> None:
        return None


def _prepared(value: int) -> PreparedBoardIndexPersistence:
    return PreparedBoardIndexPersistence(
        ingestion_id=UUID(f"00000000-0000-0000-0000-{value:012d}"),
        raw_id=UUID(f"10000000-0000-0000-0000-{value:012d}"),
        fetched_at=FETCHED_AT,
        input_hash=f"{value:x}".rjust(64, "0"),
        object_path=f"test/{value}.jsonl",
        content_sha256="a" * 64,
        byte_size=1,
        source_row_count=1,
        source_years_payload="[2026]",
        records_payload="[]",
    )


def test_queue_allows_one_running_and_one_waiting_then_drains() -> None:
    persistence = BlockingPersistence()
    queue = BoardIndexPersistenceQueue(persistence)  # type: ignore[arg-type]
    try:
        queue.submit(_prepared(1))
        assert persistence.started.wait(timeout=1)
        queue.submit(_prepared(2))
        with pytest.raises(BoardIndexPersistenceQueueFull):
            queue.submit(_prepared(3))
        persistence.release.set()
        queue.shutdown()
    finally:
        persistence.release.set()

    assert persistence.calls == [
        UUID("00000000-0000-0000-0000-000000000001"),
        UUID("00000000-0000-0000-0000-000000000002"),
    ]


def test_prepare_maps_raw_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = LocalRawStore(tmp_path)
    monkeypatch.setattr(store, "write_jsonl", lambda **kwargs: (_ for _ in ()).throw(OSError()))
    persistence = BoardIndexApiPersistence(cast(Engine, FakeEngine()), store, tmp_path)

    with pytest.raises(BoardIndexPersistenceError, match="Raw"):
        persistence.prepare(_batch())
