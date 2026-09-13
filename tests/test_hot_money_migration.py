from pathlib import Path

MIGRATION = (
    Path(__file__).parents[1]
    / "supabase"
    / "migrations"
    / "20260913000300_add_dragon_tiger_hot_money_profiles.sql"
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
