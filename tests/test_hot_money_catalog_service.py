from pathlib import Path

import pytest

from market_data_center.hot_money_catalog_service import (
    HotMoneyCatalogService,
    load_hot_money_catalog,
)


class FakePersistence:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def replace_catalog(self, catalog: object) -> None:
        self.calls.append(catalog)


def test_empty_versioned_catalog_loads_and_dry_run_does_not_write(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text(
        '{"catalog_version":"v1","reviewed_at":null,"actors":[],"mappings":[]}',
        encoding="utf-8",
    )
    persistence = FakePersistence()
    catalog = load_hot_money_catalog(path)

    summary = HotMoneyCatalogService(persistence).sync(catalog, dry_run=True)

    assert summary.catalog_version == "v1"
    assert summary.actor_count == 0
    assert summary.mapping_count == 0
    assert persistence.calls == []


def test_catalog_rejects_overlapping_approved_mappings_for_one_seat(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text(
        """{
          "catalog_version":"v1","reviewed_at":"2026-09-13T09:00:00+08:00",
          "actors":[
            {"actor_code":"A_ONE","canonical_name":"甲","aliases":[]},
            {"actor_code":"A_TWO","canonical_name":"乙","aliases":[]}
          ],
          "mappings":[
            {"actor_code":"A_ONE","seat_id":"00000000-0000-0000-0000-000000000001",
             "source_alias_name":"营业部甲","valid_from":"2026-01-01","valid_to":null,
             "evidence_note":"人工证据","review_status":"APPROVED"},
            {"actor_code":"A_TWO","seat_id":"00000000-0000-0000-0000-000000000001",
             "source_alias_name":"营业部乙","valid_from":"2026-02-01","valid_to":null,
             "evidence_note":"人工证据","review_status":"APPROVED"}
          ]} """,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="overlap"):
        load_hot_money_catalog(path)


def test_catalog_execute_publishes_complete_candidate(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text(
        '{"catalog_version":"v1","reviewed_at":null,"actors":[],"mappings":[]}',
        encoding="utf-8",
    )
    persistence = FakePersistence()
    catalog = load_hot_money_catalog(path)

    HotMoneyCatalogService(persistence).sync(catalog, dry_run=False)

    assert persistence.calls == [catalog]
