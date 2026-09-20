from market_data_center.migrations import MIGRATION_DIR


def test_limit_down_migration_declares_private_immutable_tables() -> None:
    migration = (MIGRATION_DIR / "20260920000100_create_today_limit_down_snapshot.sql").read_text(
        encoding="utf-8"
    )
    for table in ("source_observation", "snapshot", "member", "calculation_quality"):
        assert f"create table today_limit_down.{table}" in migration
        assert f"alter table today_limit_down.{table} enable row level security" in migration
    assert "today_limit_down_source" in migration
    assert "today_limit_down_snapshot" in migration
    assert "closing_ask1_sealing_amount_cny" in migration
    assert "revoke all on all tables in schema today_limit_down from public" in migration
