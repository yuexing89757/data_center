create schema if not exists stock_pool;
revoke all on schema stock_pool from public;
grant usage on schema stock_pool to market_data_worker;

create table derived.daily_price_limit (
    calculation_id uuid not null references derived.calculation_run (calculation_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    previous_close numeric(18, 4) not null,
    upper_limit numeric(18, 4) not null,
    lower_limit numeric(18, 4) not null,
    limit_ratio numeric(12, 8) not null,
    price_tick numeric(12, 8) not null,
    is_st boolean,
    rule_version text not null,
    algorithm_version text not null,
    primary key (calculation_id, symbol, trade_date),
    constraint daily_price_limit_price_check check (
        previous_close > 0 and upper_limit > previous_close
        and lower_limit <= previous_close and lower_limit > 0
        and limit_ratio > 0 and price_tick > 0
    ),
    constraint daily_price_limit_version_check check (
        btrim(rule_version) <> '' and btrim(algorithm_version) <> ''
    )
);

create table derived.price_limit_event (
    calculation_id uuid not null references derived.calculation_run (calculation_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    direction text not null,
    close numeric(18, 4) not null,
    limit_price numeric(18, 4) not null,
    rule_version text not null,
    algorithm_version text not null,
    primary key (calculation_id, symbol, trade_date, direction),
    constraint price_limit_event_direction_check check (direction in ('up', 'down')),
    constraint price_limit_event_price_check check (close > 0 and close = limit_price),
    constraint price_limit_event_version_check check (
        btrim(rule_version) <> '' and btrim(algorithm_version) <> ''
    )
);

create table stock_pool.snapshot (
    snapshot_id uuid primary key,
    calculation_id uuid not null references derived.calculation_run (calculation_id),
    pool_code text not null,
    basis_trade_date date not null,
    effective_trade_date date not null,
    version integer not null,
    status text not null,
    member_count integer not null,
    candidate_count integer not null,
    rejected_count integer not null,
    content_hash text not null,
    input_hash text not null,
    rule_version text not null,
    algorithm_version text not null,
    generated_at timestamptz not null,
    unique (pool_code, effective_trade_date, version),
    unique (calculation_id, pool_code),
    constraint stock_pool_snapshot_code_check check (pool_code in (
        'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP',
        'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN'
    )),
    constraint stock_pool_snapshot_date_check check (
        effective_trade_date > basis_trade_date
    ),
    constraint stock_pool_snapshot_status_check check (status in ('ready', 'failed')),
    constraint stock_pool_snapshot_count_check check (
        version > 0 and member_count >= 0 and candidate_count >= 0
        and rejected_count >= 0 and member_count <= candidate_count
        and rejected_count <= candidate_count
    ),
    constraint stock_pool_snapshot_hash_check check (
        content_hash ~ '^[0-9a-f]{64}$' and input_hash ~ '^[0-9a-f]{64}$'
    )
);

create table stock_pool.member (
    snapshot_id uuid not null references stock_pool.snapshot (snapshot_id),
    symbol text not null references core.security (symbol),
    direction text not null,
    primary key (snapshot_id, symbol),
    constraint stock_pool_member_direction_check check (direction in ('up', 'down'))
);

create table stock_pool.calculation_quality (
    calculation_id uuid not null references derived.calculation_run (calculation_id),
    rule_code text not null,
    severity text not null,
    symbol text not null,
    message text not null,
    created_at timestamptz not null default now(),
    primary key (calculation_id, rule_code, symbol),
    constraint stock_pool_quality_rule_check check (btrim(rule_code) <> ''),
    constraint stock_pool_quality_severity_check check (severity in ('warning', 'error')),
    constraint stock_pool_quality_message_check check (btrim(message) <> '')
);

create index daily_price_limit_lookup_idx
    on derived.daily_price_limit (symbol, trade_date);
create index price_limit_event_lookup_idx
    on derived.price_limit_event (trade_date, direction, symbol);
create index stock_pool_snapshot_exact_idx
    on stock_pool.snapshot (pool_code, effective_trade_date, version desc)
    where status = 'ready';
create index stock_pool_member_symbol_idx on stock_pool.member (symbol, snapshot_id);

alter table derived.daily_price_limit enable row level security;
alter table derived.price_limit_event enable row level security;
alter table stock_pool.snapshot enable row level security;
alter table stock_pool.member enable row level security;
alter table stock_pool.calculation_quality enable row level security;

create policy daily_price_limit_worker_all on derived.daily_price_limit
    for all to market_data_worker using (true) with check (true);
create policy price_limit_event_worker_all on derived.price_limit_event
    for all to market_data_worker using (true) with check (true);
create policy stock_pool_snapshot_worker_all on stock_pool.snapshot
    for all to market_data_worker using (true) with check (true);
create policy stock_pool_member_worker_all on stock_pool.member
    for all to market_data_worker using (true) with check (true);
create policy stock_pool_quality_worker_all on stock_pool.calculation_quality
    for all to market_data_worker using (true) with check (true);

grant select, insert on derived.daily_price_limit, derived.price_limit_event
    to market_data_worker;
grant select, insert on stock_pool.snapshot, stock_pool.member,
    stock_pool.calculation_quality to market_data_worker;

create or replace function api_v1.query_stock_pool_snapshot(
    p_pool_code text,
    p_effective_trade_date date,
    p_version integer default null,
    p_limit integer default 5000
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, stock_pool, derived, core
set statement_timeout = '5s'
as $$
declare
    selected stock_pool.snapshot%rowtype;
    members jsonb;
begin
    if p_pool_code not in (
        'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP',
        'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN'
    ) or p_effective_trade_date is null or p_limit < 1 or p_limit > 5000
       or (p_version is not null and p_version < 1) then
        raise exception 'invalid stock-pool query boundary' using errcode = '22023';
    end if;

    select * into selected
    from stock_pool.snapshot snapshot
    where snapshot.pool_code = p_pool_code
      and snapshot.effective_trade_date = p_effective_trade_date
      and snapshot.status = 'ready'
      and (p_version is null or snapshot.version = p_version)
    order by snapshot.version desc
    limit 1;

    if not found then
        raise exception 'exact ready stock-pool snapshot does not exist'
            using errcode = 'P0002';
    end if;

    select coalesce(jsonb_agg(to_jsonb(detail) order by detail.symbol), '[]'::jsonb)
    into members
    from (
        select member.symbol, security.current_name, security.exchange,
               member.direction, event.close, event.limit_price,
               price_limit.previous_close, price_limit.upper_limit,
               price_limit.lower_limit, indicator.free_float_turnover_rate_pct,
               indicator.free_float_shares, indicator.circulating_market_value
        from stock_pool.member member
        join core.security security on security.symbol = member.symbol
        join derived.price_limit_event event
          on event.calculation_id = selected.calculation_id
         and event.symbol = member.symbol
         and event.trade_date = selected.basis_trade_date
         and event.direction = member.direction
        join derived.daily_price_limit price_limit
          on price_limit.calculation_id = selected.calculation_id
         and price_limit.symbol = member.symbol
         and price_limit.trade_date = selected.basis_trade_date
        left join core.stock_daily_indicator indicator
          on indicator.symbol = member.symbol
         and indicator.trade_date = selected.basis_trade_date
        where member.snapshot_id = selected.snapshot_id
        order by member.symbol
        limit p_limit
    ) detail;

    return jsonb_build_object(
        'snapshot_id', selected.snapshot_id,
        'calculation_id', selected.calculation_id,
        'pool_code', selected.pool_code,
        'basis_trade_date', selected.basis_trade_date,
        'effective_trade_date', selected.effective_trade_date,
        'version', selected.version,
        'status', selected.status,
        'member_count', selected.member_count,
        'returned_member_count', jsonb_array_length(members),
        'candidate_count', selected.candidate_count,
        'rejected_count', selected.rejected_count,
        'content_hash', selected.content_hash,
        'input_hash', selected.input_hash,
        'rule_version', selected.rule_version,
        'algorithm_version', selected.algorithm_version,
        'generated_at', selected.generated_at,
        'members', members
    );
end
$$;

revoke all on function api_v1.query_stock_pool_snapshot(text, date, integer, integer)
    from public;
grant execute on function api_v1.query_stock_pool_snapshot(text, date, integer, integer)
    to anon, authenticated;
