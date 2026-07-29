alter table ingestion.ingestion_run
    drop constraint ingestion_run_provider_check,
    add constraint ingestion_run_provider_check
        check (provider_code in ('baostock', 'akshare', 'pytdx', 'akshare_ths'));

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security',
        'trading_calendar',
        'daily_bar',
        'capital',
        'classification_catalog',
        'classification_members',
        'board_index',
        'board_index_daily_bar',
        'board_index_constituent_snapshot'
    )
);

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security',
        'trading_calendar',
        'daily_bar',
        'capital',
        'classification_catalog',
        'classification_members',
        'board_index',
        'board_index_daily_bar',
        'board_index_constituent_snapshot'
    )
);

create table core.board_index (
    board_id text primary key,
    board_code text not null,
    namespace text not null,
    name text not null,
    board_type text not null,
    market text not null,
    status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (namespace, board_code),
    constraint board_index_identity_check check (
        board_id = namespace || ':' || board_code
        and btrim(namespace) <> ''
        and btrim(board_code) <> ''
        and btrim(name) <> ''
    ),
    constraint board_index_type_check check (board_type = 'dynamic_theme'),
    constraint board_index_market_check check (market = 'CN_A_SHARE'),
    constraint board_index_status_check check (status in ('active', 'inactive', 'unknown')),
    constraint board_index_source_check check (source_code = 'akshare_ths')
);

create table core.board_index_daily_bar (
    board_id text not null references core.board_index (board_id),
    trade_date date not null,
    market text not null,
    open numeric(18, 4) not null,
    high numeric(18, 4) not null,
    low numeric(18, 4) not null,
    close numeric(18, 4) not null,
    volume bigint not null,
    amount numeric(24, 2) not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (board_id, trade_date),
    foreign key (market, trade_date)
        references core.trading_calendar (market, trade_date),
    constraint board_index_daily_bar_market_check check (market = 'CN_A_SHARE'),
    constraint board_index_daily_bar_nonnegative_check check (
        open >= 0
        and high >= 0
        and low >= 0
        and close >= 0
        and volume >= 0
        and amount >= 0
    ),
    constraint board_index_daily_bar_range_check check (
        low <= high and open between low and high and close between low and high
    ),
    constraint board_index_daily_bar_source_check check (source_code = 'akshare_ths')
);

create table core.board_index_constituent_snapshot (
    board_id text not null references core.board_index (board_id),
    trade_date date not null,
    symbol text not null references core.security (symbol),
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (board_id, trade_date, symbol),
    constraint board_index_constituent_source_check check (source_code = 'akshare_ths')
);

create index board_index_ingestion_idx on core.board_index (ingestion_id);
create index board_index_daily_bar_market_date_idx
    on core.board_index_daily_bar (market, trade_date);
create index board_index_daily_bar_ingestion_idx
    on core.board_index_daily_bar (ingestion_id);
create index board_index_constituent_symbol_date_idx
    on core.board_index_constituent_snapshot (symbol, trade_date desc);
create index board_index_constituent_ingestion_idx
    on core.board_index_constituent_snapshot (ingestion_id);

create or replace function core.ensure_board_index_daily_bar_trading_day()
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
        raise exception 'board-index bar date must be a known trading day'
            using errcode = '23514';
    end if;
    return new;
end
$$;

create or replace function core.ensure_board_index_constituent_trading_day()
returns trigger
language plpgsql
set search_path = pg_catalog
as $$
begin
    if not exists (
        select 1
        from core.board_index board
        join core.trading_calendar calendar
          on calendar.market = board.market
         and calendar.trade_date = new.trade_date
         and calendar.is_trading_day
        where board.board_id = new.board_id
    ) then
        raise exception 'board-index constituent date must be a known trading day'
            using errcode = '23514';
    end if;
    return new;
end
$$;

revoke all on function core.ensure_board_index_daily_bar_trading_day() from public;
revoke all on function core.ensure_board_index_constituent_trading_day() from public;

create trigger board_index_daily_bar_ensure_trading_day
before insert or update of market, trade_date on core.board_index_daily_bar
for each row execute function core.ensure_board_index_daily_bar_trading_day();

create trigger board_index_constituent_ensure_trading_day
before insert or update of board_id, trade_date on core.board_index_constituent_snapshot
for each row execute function core.ensure_board_index_constituent_trading_day();

create trigger board_index_set_updated_at
before update on core.board_index
for each row execute function ingestion.set_updated_at();

create trigger board_index_daily_bar_set_updated_at
before update on core.board_index_daily_bar
for each row execute function ingestion.set_updated_at();

alter table core.board_index enable row level security;
alter table core.board_index_daily_bar enable row level security;
alter table core.board_index_constituent_snapshot enable row level security;

create policy board_index_worker_all on core.board_index
    for all to market_data_worker using (true) with check (true);
create policy board_index_daily_bar_worker_all on core.board_index_daily_bar
    for all to market_data_worker using (true) with check (true);
create policy board_index_constituent_worker_all on core.board_index_constituent_snapshot
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on core.board_index to market_data_worker;
grant select, insert, update on core.board_index_daily_bar to market_data_worker;
grant select, insert, delete on core.board_index_constituent_snapshot to market_data_worker;

create view api_v1.board_indexes as
select board_id, board_code, namespace, name, board_type, market, status
from core.board_index;

create view api_v1.board_index_daily_bars as
select board_id, trade_date, market, open, high, low, close, volume, amount
from core.board_index_daily_bar;

create view api_v1.board_index_constituents as
select board_id, trade_date, symbol
from core.board_index_constituent_snapshot;

revoke all on
    api_v1.board_indexes,
    api_v1.board_index_daily_bars,
    api_v1.board_index_constituents
from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on
            api_v1.board_indexes,
            api_v1.board_index_daily_bars,
            api_v1.board_index_constituents
        to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on
            api_v1.board_indexes,
            api_v1.board_index_daily_bars,
            api_v1.board_index_constituents
        to authenticated;
    end if;
end
$$;
