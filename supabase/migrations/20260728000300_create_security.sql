set search_path = pg_catalog, public, extensions;

create table core.security (
    symbol text primary key,
    code text not null,
    exchange text not null,
    current_name text not null,
    security_type text not null,
    status text not null,
    ipo_date date,
    delisting_date date,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint security_exchange_code_unique unique (exchange, code),
    constraint security_exchange_check check (exchange in ('SSE', 'SZSE', 'BSE')),
    constraint security_code_check check (code ~ '^[0-9]+$'),
    constraint security_symbol_check check (symbol = exchange || ':' || code),
    constraint security_name_check check (btrim(current_name) <> ''),
    constraint security_type_check check (security_type in ('stock', 'unknown')),
    constraint security_status_check check (status in ('listed', 'delisted', 'unknown')),
    constraint security_date_order_check check (
        ipo_date is null or delisting_date is null or delisting_date >= ipo_date
    ),
    constraint security_source_check check (source_code = 'baostock')
);

create index security_status_idx on core.security (status);
create index security_ingestion_idx on core.security (ingestion_id);

create trigger security_set_updated_at
before update on core.security
for each row execute function ingestion.set_updated_at();

create table core.security_name_history (
    symbol text not null references core.security (symbol),
    name text not null,
    effective_from date not null,
    effective_to date,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (symbol, effective_from),
    constraint security_name_history_name_check check (btrim(name) <> ''),
    constraint security_name_history_date_check check (
        effective_to is null or effective_to >= effective_from
    ),
    constraint security_name_history_source_check check (source_code = 'baostock'),
    constraint security_name_history_no_overlap exclude using gist (
        symbol with =,
        daterange(effective_from, coalesce(effective_to, 'infinity'::date), '[]') with &&
    )
);

create index security_name_history_ingestion_idx
    on core.security_name_history (ingestion_id);

alter table core.security enable row level security;
alter table core.security_name_history enable row level security;

create policy security_worker_all on core.security
    for all to market_data_worker using (true) with check (true);
create policy security_name_history_worker_all on core.security_name_history
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on core.security to market_data_worker;
grant select, insert, update on core.security_name_history to market_data_worker;

reset search_path;
