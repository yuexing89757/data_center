-- ADR-0023: convertible bond domain — basic terms, daily bar,
-- convert-price revision history and call/sell-back events.
create schema if not exists convertible_bond;
revoke all on schema convertible_bond from public;
grant usage on schema convertible_bond to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar'
    )
);
alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar'
    )
);

create table convertible_bond.bond (
    symbol text primary key references core.security (symbol),
    bond_code text not null,
    bond_short_name text not null,
    bond_full_name text not null,
    underlying_symbol text not null references core.security (symbol),
    exchange text not null,
    par_value numeric(18, 4) not null,
    issue_size numeric(24, 2),
    issue_date date,
    value_date date,
    maturity_years smallint,
    maturity_date date,
    convert_price_initial numeric(18, 4),
    convert_price numeric(18, 4),
    convert_start_date date,
    convert_end_date date,
    coupon_rate numeric(24, 10),
    redeem_clause text,
    sell_back_clause text,
    lifecycle_status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint cb_bond_code_check check (bond_code ~ '^[0-9]{6}$'),
    constraint cb_bond_exchange_check check (exchange in ('SSE', 'SZSE')),
    constraint cb_bond_par_positive check (par_value > 0),
    constraint cb_bond_issue_size_nonnegative check (issue_size is null or issue_size >= 0),
    constraint cb_bond_maturity_positive check (maturity_years is null or maturity_years > 0),
    constraint cb_bond_convert_price_initial_positive check (
        convert_price_initial is null or convert_price_initial > 0
    ),
    constraint cb_bond_convert_price_positive check (
        convert_price is null or convert_price > 0
    ),
    constraint cb_bond_convert_period_order check (
        convert_start_date is null or convert_end_date is null or convert_end_date >= convert_start_date
    ),
    constraint cb_bond_lifecycle_check check (
        lifecycle_status in (
            'pending_list','listed','in_conversion','called','matured','delisted'
        )
    )
);

create index cb_bond_underlying_idx on convertible_bond.bond (underlying_symbol);
create index cb_bond_ingestion_idx on convertible_bond.bond (ingestion_id);

create table convertible_bond.daily_bar (
    symbol text not null references core.security (symbol),
    trade_date date not null,
    market text not null,
    open numeric(18, 4),
    high numeric(18, 4),
    low numeric(18, 4),
    close numeric(18, 4),
    previous_close numeric(18, 4),
    volume bigint,
    amount numeric(24, 2),
    pct_chg numeric(24, 10),
    convert_value numeric(18, 4),
    convert_premium_pct numeric(24, 10),
    convert_price numeric(18, 4),
    remain_size numeric(24, 2),
    trade_status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    foreign key (market, trade_date) references core.trading_calendar (market, trade_date),
    constraint cb_daily_bar_market_check check (market = 'CN_A_SHARE'),
    constraint cb_daily_bar_nonnegative_check check (
        (open is null or open >= 0)
        and (high is null or high >= 0)
        and (low is null or low >= 0)
        and (close is null or close >= 0)
        and (previous_close is null or previous_close >= 0)
        and (volume is null or volume >= 0)
        and (amount is null or amount >= 0)
        and (remain_size is null or remain_size >= 0)
    ),
    constraint cb_daily_bar_range_check check (
        low is null or high is null or low <= high
    ),
    constraint cb_daily_bar_trade_status_check check (
        trade_status in ('trading','suspended','halted_limit','unknown')
    )
);

create index cb_daily_bar_market_date_idx on convertible_bond.daily_bar (market, trade_date);
create index cb_daily_bar_ingestion_idx on convertible_bond.daily_bar (ingestion_id);

create table convertible_bond.convert_price_revision (
    symbol text not null references core.security (symbol),
    effective_date date not null,
    convert_price_before numeric(18, 4),
    convert_price_after numeric(18, 4) not null,
    revision_reason text not null,
    announcement_date date,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (symbol, effective_date),
    constraint cb_revision_before_positive check (
        convert_price_before is null or convert_price_before > 0
    ),
    constraint cb_revision_after_positive check (convert_price_after > 0),
    constraint cb_revision_reason_check check (
        revision_reason in ('dividend','bonus_share','rights_issue','downward_revision','other')
    )
);

create index cb_convert_price_revision_ingestion_idx
    on convertible_bond.convert_price_revision (ingestion_id);

create table convertible_bond.call_event (
    symbol text not null references core.security (symbol),
    event_type text not null,
    announcement_date date not null,
    trigger_date date,
    record_date date,
    call_price numeric(18, 4),
    status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (symbol, event_type, announcement_date),
    constraint cb_call_event_type_check check (
        event_type in ('forced_redemption','sell_back','maturity_redemption')
    ),
    constraint cb_call_event_price_nonnegative check (
        call_price is null or call_price >= 0
    ),
    constraint cb_call_event_status_check check (status in ('announced','executed','cancelled'))
);

create index cb_call_event_ingestion_idx on convertible_bond.call_event (ingestion_id);

create trigger cb_bond_set_updated_at before update on convertible_bond.bond
for each row execute function ingestion.set_updated_at();
create trigger cb_daily_bar_set_updated_at before update on convertible_bond.daily_bar
for each row execute function ingestion.set_updated_at();

create trigger cb_daily_bar_ensure_trading_day
before insert or update of market, trade_date on convertible_bond.daily_bar
for each row execute function core.ensure_daily_bar_trading_day();

alter table convertible_bond.bond enable row level security;
alter table convertible_bond.daily_bar enable row level security;
alter table convertible_bond.convert_price_revision enable row level security;
alter table convertible_bond.call_event enable row level security;

create policy cb_bond_worker_all on convertible_bond.bond
    for all to market_data_worker using (true) with check (true);
create policy cb_daily_bar_worker_all on convertible_bond.daily_bar
    for all to market_data_worker using (true) with check (true);
create policy cb_convert_price_revision_worker_all on convertible_bond.convert_price_revision
    for all to market_data_worker using (true) with check (true);
create policy cb_call_event_worker_all on convertible_bond.call_event
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on convertible_bond.bond,
    convertible_bond.daily_bar, convertible_bond.convert_price_revision,
    convertible_bond.call_event to market_data_worker;

create view api_v1.convertible_bonds as
select
    symbol, bond_code, bond_short_name, bond_full_name, underlying_symbol, exchange,
    par_value, issue_size, issue_date, value_date, maturity_years, maturity_date,
    convert_price_initial, convert_price, convert_start_date, convert_end_date,
    coupon_rate, redeem_clause, sell_back_clause, lifecycle_status
from convertible_bond.bond;

create view api_v1.convertible_bond_daily_bars as
select
    symbol, trade_date, open, high, low, close, previous_close, volume, amount,
    pct_chg, convert_value, convert_premium_pct, convert_price, remain_size, trade_status
from convertible_bond.daily_bar;

revoke all on api_v1.convertible_bonds, api_v1.convertible_bond_daily_bars from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on api_v1.convertible_bonds, api_v1.convertible_bond_daily_bars to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on api_v1.convertible_bonds, api_v1.convertible_bond_daily_bars to authenticated;
    end if;
end
$$;
