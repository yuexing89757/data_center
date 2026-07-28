create table core.trading_calendar (
    market text not null,
    trade_date date not null,
    is_trading_day boolean not null,
    previous_trading_day date,
    next_trading_day date,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (market, trade_date),
    constraint trading_calendar_market_check check (market = 'CN_A_SHARE'),
    constraint trading_calendar_previous_check check (
        previous_trading_day is null or previous_trading_day < trade_date
    ),
    constraint trading_calendar_next_check check (
        next_trading_day is null or next_trading_day > trade_date
    ),
    constraint trading_calendar_source_check check (source_code = 'baostock')
);

create index trading_calendar_open_day_idx
    on core.trading_calendar (market, trade_date)
    where is_trading_day;
create index trading_calendar_ingestion_idx on core.trading_calendar (ingestion_id);

create trigger trading_calendar_set_updated_at
before update on core.trading_calendar
for each row execute function ingestion.set_updated_at();

alter table core.trading_calendar enable row level security;
create policy trading_calendar_worker_all on core.trading_calendar
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on core.trading_calendar to market_data_worker;
