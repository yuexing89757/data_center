import json
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.history_archive import read_archive, write_archive


def test_complete_archive_preserves_precision_null_and_corruption_blocks(tmp_path):
    rows = {"core.fact": ('{"id":1,"amount":1.123456789012345678901,"volume":null}',)}
    archive = write_archive(tmp_path, {"cutoff": str(date(2026, 9, 23))}, rows, uuid4())
    header, restored = read_archive(tmp_path, archive)
    assert header["cutoff"] == "2026-09-23"
    assert restored == rows
    assert json.loads(restored["core.fact"][0], parse_float=Decimal)["amount"] == Decimal(
        "1.123456789012345678901"
    )
    (tmp_path / archive.path).write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        read_archive(tmp_path, archive)


def test_archive_cancelled_before_publication_keeps_no_final_file(tmp_path):
    def cancel():
        raise RuntimeError("budget")

    with pytest.raises(RuntimeError, match="budget"):
        write_archive(tmp_path, {}, {"a.b": ('{"id":1}',)}, uuid4(), checkpoint=cancel)
    assert not list(tmp_path.rglob("*.gz"))
