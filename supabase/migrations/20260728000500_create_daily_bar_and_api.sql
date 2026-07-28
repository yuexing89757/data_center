create table core.daily_bar (
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
    trade_status text not null,
    is_st boolean,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    foreign key (market, trade_date)
        references core.trading_calendar (market, trade_date),
    constraint daily_bar_market_check check (market = 'CN_A_SHARE'),
    constraint daily_bar_nonnegative_check check (
        (open is null or open >= 0)
        and (high is null or high >= 0)
        and (low is null or low >= 0)
        and (close is null or close >= 0)
        and (previous_close is null or previous_close >= 0)
        and (volume is null or volume >= 0)
        and (amount is null or amount >= 0)
    ),
    constraint daily_bar_range_check check (
        low is null or high is null or low <= high
    ),
    constraint daily_bar_open_range_check check (
        open is null or low is null or high is null or open between low and high
    ),
    constraint daily_bar_close_range_check check (
        close is null or low is null or high is null or close between low and high
    ),
    constraint daily_bar_trade_status_check check (
        trade_status in ('trading', 'suspended', 'unknown')
    ),
    constraint daily_bar_source_check check (source_code = 'baostock')
);

create index daily_bar_market_date_idx on core.daily_bar (market, trade_date);
create index daily_bar_ingestion_idx on core.daily_bar (ingestion_id);

create or replace function core.ensure_daily_bar_trading_day()
returns trigger
language plpgsql
set search_path = pg_catalog
as $$
begin
    if not exists (
        select 1
        from core.trading_calendar calendar
        where calendar.market = new.market
          and calendar.trade_date = new.trade_date
          and calendar.is_trading_day
    ) then
        raise exception 'daily bar date must be a known trading day'
            using errcode = '23514';
    end if;
    return new;
end
$$;

revoke all on function core.ensure_daily_bar_trading_day() from public;

create trigger daily_bar_ensure_trading_day
before insert or update of market, trade_date on core.daily_bar
for each row execute function core.ensure_daily_bar_trading_day();

create trigger daily_bar_set_updated_at
before update on core.daily_bar
for each row execute function ingestion.set_updated_at();

alter table core.daily_bar enable row level security;
create policy daily_bar_worker_all on core.daily_bar
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on core.daily_bar to market_data_worker;

create view api_v1.securities as
select
    symbol,
    code,
    exchange,
    current_name,
    security_type,
    status,
    ipo_date,
    delisting_date
from core.security;

create view api_v1.trading_calendar as
select
    market,
    trade_date,
    is_trading_day,
    previous_trading_day,
    next_trading_day
from core.trading_calendar;

create view api_v1.daily_bars as
select
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    previous_close,
    volume,
    amount,
    trade_status,
    is_st
from core.daily_bar;

revoke all on api_v1.securities, api_v1.trading_calendar, api_v1.daily_bars from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant usage on schema api_v1 to anon;
        grant select on api_v1.securities, api_v1.trading_calendar, api_v1.daily_bars to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant usage on schema api_v1 to authenticated;
        grant select on api_v1.securities, api_v1.trading_calendar, api_v1.daily_bars
            to authenticated;
    end if;
end
$$;
