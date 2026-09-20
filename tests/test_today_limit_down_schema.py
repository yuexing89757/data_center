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


def test_limit_down_rpc_is_bounded_and_api_role_only() -> None:
    migration = (MIGRATION_DIR / "20260920000200_add_daily_limit_down_list_api.sql").read_text(
        encoding="utf-8"
    )
    assert "set statement_timeout = '5s'" in migration
    assert "where s.trade_date = p_trade_date" in migration
    assert "order by m.symbol" in migration
    assert "p_offset > 50000" in migration
    assert "p_limit > 500" in migration
    assert "from today_limit_down.calculation_quality" not in migration
    assert "join today_limit_down.calculation_quality" in migration
    assert "grant execute on function api_v1.query_daily_limit_down_list" in migration
    assert "to market_data_api" in migration
    assert "from public" in migration
