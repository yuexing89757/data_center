from pathlib import Path

MIGRATION = (
    Path(__file__).parents[1]
    / "supabase"
    / "migrations"
    / "20260913000300_add_dragon_tiger_hot_money_profiles.sql"
)
HOT_MONEY_ACTIONS_MIGRATION = (
    Path(__file__).parents[1]
    / "supabase"
    / "migrations"
    / "20260918000100_add_hot_money_actions_api.sql"
)


def test_hot_money_schema_is_worker_only_and_effective_dated() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()
    assert "create table billboard.hot_money_actor" in sql
    assert "create table billboard.hot_money_seat_mapping" in sql
    assert "create table billboard.trading_seat_profile_daily" in sql
    assert "exclude using gist" in sql
    assert sql.count("enable row level security") >= 3
    assert "revoke all on billboard.hot_money_actor from public, anon, authenticated" in sql


def test_profile_schema_preserves_version_and_input_watermark() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()
    assert "primary key (seat_id, as_of_date, algorithm_version)" in sql
    assert "input_watermark_date date not null" in sql
    assert "calculation_id uuid not null references derived.calculation_run" in sql
    assert "'dragon_tiger_seat_profile'" in sql


def test_hot_money_actions_rpc_is_bounded_and_api_only() -> None:
    assert HOT_MONEY_ACTIONS_MIGRATION.exists()
    sql = HOT_MONEY_ACTIONS_MIGRATION.read_text(encoding="utf-8").lower()
    assert "create function api_v1.query_hot_money_actions_by_date" in sql
    assert "review_status = 'approved'" in sql
    assert "statement_timeout = '5s'" in sql
    assert "grant execute on function api_v1.query_hot_money_actions_by_date(date)" in sql
