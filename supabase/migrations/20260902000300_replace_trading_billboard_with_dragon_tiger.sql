alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','dragon_tiger_daily','regulation_daily_calculation'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

drop function if exists api_v1.query_trading_billboard_by_date(date, integer, integer);
drop function if exists api_v1.query_trading_billboard_by_symbol(text, date, date, integer, integer);
drop function if exists api_v1.query_trading_billboard_by_seat(
    text, text, date, date, text, integer, integer
);

drop table if exists billboard.seat;
drop table if exists billboard.entry;

create table billboard.dragon_tiger_reason (
    reason_id uuid primary key default extensions.gen_random_uuid(),
    reason_code text not null unique,
    reason_name text not null,
    reason_type text not null,
    period_type text not null,
    description text,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint dragon_tiger_reason_nonblank_check check (
        btrim(reason_code) <> '' and btrim(reason_name) <> ''
    ),
    constraint dragon_tiger_reason_type_check check (
        reason_type in (
            'PRICE_DEVIATION','TURNOVER','AMPLITUDE','CONTINUOUS_LIMIT','ST','OTHER'
        )
    ),
    constraint dragon_tiger_reason_period_check check (period_type in ('DAY','THREE_DAY'))
);

create trigger dragon_tiger_reason_set_updated_at
before update on billboard.dragon_tiger_reason
for each row execute function ingestion.set_updated_at();

create table billboard.reason_source_alias (
    source_code text not null,
    source_reason_code text not null,
    source_reason_name text not null,
    period_type text not null,
    reason_id uuid not null references billboard.dragon_tiger_reason (reason_id),
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    primary key (source_code, source_reason_code, source_reason_name, period_type),
    constraint reason_source_alias_source_check check (source_code in ('eastmoney','tushare')),
    constraint reason_source_alias_nonblank_check check (
        btrim(source_reason_code) <> '' and btrim(source_reason_name) <> ''
    ),
    constraint reason_source_alias_period_check check (period_type in ('DAY','THREE_DAY'))
);

create table billboard.trading_seat (
    seat_id uuid primary key default extensions.gen_random_uuid(),
    canonical_name text not null,
    broker_name text,
    branch_name text,
    seat_type text not null,
    province text,
    city text,
    first_seen_date date not null,
    last_seen_date date not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint trading_seat_nonblank_check check (btrim(canonical_name) <> ''),
    constraint trading_seat_type_check check (
        seat_type in ('BROKER','INSTITUTION','NORTHBOUND','OTHER','UNKNOWN')
    ),
    constraint trading_seat_date_check check (first_seen_date <= last_seen_date)
);

create trigger trading_seat_set_updated_at
before update on billboard.trading_seat
for each row execute function ingestion.set_updated_at();

create table billboard.trading_seat_alias (
    alias_id uuid primary key default extensions.gen_random_uuid(),
    seat_id uuid not null references billboard.trading_seat (seat_id) on delete cascade,
    source_code text not null,
    source_seat_key text,
    alias_name text not null,
    created_at timestamptz not null default now(),
    constraint trading_seat_alias_source_name_unique unique (source_code, alias_name),
    constraint trading_seat_alias_source_check check (source_code in ('eastmoney','tushare')),
    constraint trading_seat_alias_nonblank_check check (
        btrim(alias_name) <> '' and (source_seat_key is null or btrim(source_seat_key) <> '')
    )
);

create unique index trading_seat_alias_source_key_unique
on billboard.trading_seat_alias (source_code, source_seat_key)
where source_seat_key is not null;

create table billboard.dragon_tiger_event (
    event_id uuid primary key default extensions.gen_random_uuid(),
    symbol text not null references core.security (symbol),
    market text not null default 'CN_A_SHARE',
    trade_date date not null,
    period_type text not null,
    period_start_date date not null,
    period_end_date date not null,
    reason_id uuid not null references billboard.dragon_tiger_reason (reason_id),
    reason_name_raw text not null,
    close_price numeric,
    change_pct numeric,
    turnover_amount numeric,
    turnover_rate numeric,
    amplitude numeric,
    lhb_buy_amount numeric,
    lhb_sell_amount numeric,
    source_code text not null,
    source_record_id text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    content_hash text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint dragon_tiger_event_calendar_fk foreign key (market, trade_date)
        references core.trading_calendar (market, trade_date),
    constraint dragon_tiger_event_source_unique unique (source_code, source_record_id),
    constraint dragon_tiger_event_semantic_unique unique (
        source_code, symbol, trade_date, period_type, reason_id
    ),
    constraint dragon_tiger_event_parent_unique unique (
        event_id, source_code, source_record_id, symbol, trade_date
    ),
    constraint dragon_tiger_event_market_check check (market = 'CN_A_SHARE'),
    constraint dragon_tiger_event_source_check check (source_code in ('eastmoney','tushare')),
    constraint dragon_tiger_event_period_type_check check (period_type in ('DAY','THREE_DAY')),
    constraint dragon_tiger_event_period_check check (
        period_end_date = trade_date
        and (
            (period_type = 'DAY' and period_start_date = trade_date)
            or (period_type = 'THREE_DAY' and period_start_date < trade_date)
        )
    ),
    constraint dragon_tiger_event_nonblank_check check (
        btrim(reason_name_raw) <> '' and btrim(source_record_id) <> ''
    ),
    constraint dragon_tiger_event_nonnegative_check check (
        (close_price is null or close_price >= 0)
        and (turnover_amount is null or turnover_amount >= 0)
        and (turnover_rate is null or turnover_rate >= 0)
        and (amplitude is null or amplitude >= 0)
        and (lhb_buy_amount is null or lhb_buy_amount >= 0)
        and (lhb_sell_amount is null or lhb_sell_amount >= 0)
    ),
    constraint dragon_tiger_event_hash_check check (content_hash ~ '^[0-9a-f]{64}$')
);

create trigger dragon_tiger_event_set_updated_at
before update on billboard.dragon_tiger_event
for each row execute function ingestion.set_updated_at();

create table billboard.seat_trade (
    seat_trade_id uuid primary key default extensions.gen_random_uuid(),
    event_id uuid not null,
    source_code text not null,
    source_event_id text not null,
    source_record_id text not null,
    symbol text not null,
    trade_date date not null,
    seat_id uuid references billboard.trading_seat (seat_id),
    seat_source_key text,
    seat_name_raw text not null,
    buy_amount numeric,
    sell_amount numeric,
    buy_rank integer,
    sell_rank integer,
    is_institution boolean not null,
    is_northbound boolean not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    content_hash text not null,
    created_at timestamptz not null default now(),
    constraint seat_trade_source_unique unique (source_code, source_event_id, source_record_id),
    constraint seat_trade_parent_fk foreign key (
        event_id, source_code, source_event_id, symbol, trade_date
    ) references billboard.dragon_tiger_event (
        event_id, source_code, source_record_id, symbol, trade_date
    ) on update cascade on delete cascade,
    constraint seat_trade_source_check check (source_code in ('eastmoney','tushare')),
    constraint seat_trade_nonblank_check check (
        btrim(source_event_id) <> '' and btrim(source_record_id) <> ''
        and btrim(seat_name_raw) <> ''
        and (seat_source_key is null or btrim(seat_source_key) <> '')
    ),
    constraint seat_trade_amount_check check (
        (buy_amount is not null or sell_amount is not null)
        and not (buy_amount = 0 and sell_amount = 0)
        and (buy_amount is null or buy_amount >= 0)
        and (sell_amount is null or sell_amount >= 0)
    ),
    constraint seat_trade_rank_check check (
        (buy_rank is not null or sell_rank is not null)
        and (buy_rank is null or buy_rank between 1 and 5)
        and (sell_rank is null or sell_rank between 1 and 5)
    ),
    constraint seat_trade_hash_check check (content_hash ~ '^[0-9a-f]{64}$'),
    unique (event_id, buy_rank),
    unique (event_id, sell_rank)
);

create index dragon_tiger_event_date_symbol_idx
    on billboard.dragon_tiger_event (trade_date, period_type, symbol, event_id);
create index dragon_tiger_event_symbol_date_idx
    on billboard.dragon_tiger_event (symbol, trade_date desc, period_type, event_id);
create index seat_trade_event_rank_idx
    on billboard.seat_trade (event_id, buy_rank, sell_rank, seat_trade_id);
create index seat_trade_seat_date_idx
    on billboard.seat_trade (seat_id, trade_date desc, event_id)
    where seat_id is not null;

alter table billboard.dragon_tiger_reason enable row level security;
alter table billboard.reason_source_alias enable row level security;
alter table billboard.trading_seat enable row level security;
alter table billboard.trading_seat_alias enable row level security;
alter table billboard.dragon_tiger_event enable row level security;
alter table billboard.seat_trade enable row level security;

create policy dragon_tiger_reason_worker_all on billboard.dragon_tiger_reason
    for all to market_data_worker using (true) with check (true);
create policy reason_source_alias_worker_all on billboard.reason_source_alias
    for all to market_data_worker using (true) with check (true);
create policy trading_seat_worker_all on billboard.trading_seat
    for all to market_data_worker using (true) with check (true);
create policy trading_seat_alias_worker_all on billboard.trading_seat_alias
    for all to market_data_worker using (true) with check (true);
create policy dragon_tiger_event_worker_all on billboard.dragon_tiger_event
    for all to market_data_worker using (true) with check (true);
create policy seat_trade_worker_all on billboard.seat_trade
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on billboard.dragon_tiger_reason to market_data_worker;
grant select, insert, update on billboard.reason_source_alias to market_data_worker;
grant select, insert, update on billboard.trading_seat to market_data_worker;
grant select, insert, update on billboard.trading_seat_alias to market_data_worker;
grant select, insert, update, delete on billboard.dragon_tiger_event to market_data_worker;
grant select, insert, update, delete on billboard.seat_trade to market_data_worker;
revoke all on all tables in schema billboard from public, anon, authenticated;

create function api_v1._dragon_tiger_event_item(p_event_id uuid) returns jsonb
language sql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
    select jsonb_build_object(
        'event_id', event.event_id,
        'symbol', event.symbol,
        'trade_date', event.trade_date,
        'period_type', event.period_type,
        'period_start_date', event.period_start_date,
        'period_end_date', event.period_end_date,
        'reason_code', reason.reason_code,
        'reason_name', reason.reason_name,
        'reason_type', reason.reason_type,
        'reason_name_raw', event.reason_name_raw,
        'close_price', event.close_price::text,
        'change_pct', event.change_pct::text,
        'turnover_amount', event.turnover_amount::text,
        'turnover_rate', event.turnover_rate::text,
        'amplitude', event.amplitude::text,
        'lhb_buy_amount', event.lhb_buy_amount::text,
        'lhb_sell_amount', event.lhb_sell_amount::text,
        'net_amount', case
            when event.lhb_buy_amount is not null and event.lhb_sell_amount is not null
            then (event.lhb_buy_amount - event.lhb_sell_amount)::text
            else null
        end,
        'source_code', event.source_code,
        'source_record_id', event.source_record_id,
        'seat_trades', coalesce((
            select jsonb_agg(jsonb_build_object(
                'seat_trade_id', trade.seat_trade_id,
                'seat_id', trade.seat_id,
                'seat_name_raw', trade.seat_name_raw,
                'buy_amount', trade.buy_amount::text,
                'sell_amount', trade.sell_amount::text,
                'net_amount', case
                    when trade.buy_amount is not null and trade.sell_amount is not null
                    then (trade.buy_amount - trade.sell_amount)::text
                    else null
                end,
                'buy_rank', trade.buy_rank,
                'sell_rank', trade.sell_rank,
                'is_institution', trade.is_institution,
                'is_northbound', trade.is_northbound
            ) order by coalesce(trade.buy_rank, 99), coalesce(trade.sell_rank, 99), trade.seat_trade_id)
            from billboard.seat_trade trade where trade.event_id = event.event_id
        ), '[]'::jsonb)
    )
    from billboard.dragon_tiger_event event
    join billboard.dragon_tiger_reason reason on reason.reason_id = event.reason_id
    where event.event_id = p_event_id
$$;

create function api_v1.query_dragon_tiger_events_by_date(
    p_trade_date date,
    p_period_type text default null,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_trade_date is null
       or (p_period_type is not null and p_period_type not in ('DAY','THREE_DAY'))
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger date query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count from billboard.dragon_tiger_event event
    where event.trade_date = p_trade_date
      and (p_period_type is null or event.period_type = p_period_type);
    select coalesce(jsonb_agg(api_v1._dragon_tiger_event_item(selected.event_id)
        order by selected.symbol, selected.period_type, selected.event_id), '[]'::jsonb)
    into items from (
        select event.event_id, event.symbol, event.period_type
        from billboard.dragon_tiger_event event
        where event.trade_date = p_trade_date
          and (p_period_type is null or event.period_type = p_period_type)
        order by event.symbol, event.period_type, event.event_id
        offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_events_by_symbol(
    p_symbol text,
    p_start_date date,
    p_end_date date,
    p_period_type text default null,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE|BSE):[0-9]{6}$'
       or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or (p_period_type is not null and p_period_type not in ('DAY','THREE_DAY'))
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger symbol query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count from billboard.dragon_tiger_event event
    where event.symbol = p_symbol and event.trade_date between p_start_date and p_end_date
      and (p_period_type is null or event.period_type = p_period_type);
    select coalesce(jsonb_agg(api_v1._dragon_tiger_event_item(selected.event_id)
        order by selected.trade_date desc, selected.period_type, selected.event_id), '[]'::jsonb)
    into items from (
        select event.event_id, event.trade_date, event.period_type
        from billboard.dragon_tiger_event event
        where event.symbol = p_symbol and event.trade_date between p_start_date and p_end_date
          and (p_period_type is null or event.period_type = p_period_type)
        order by event.trade_date desc, event.period_type, event.event_id
        offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_trades_by_seat(
    p_seat_id uuid,
    p_start_date date,
    p_end_date date,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_seat_id is null or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger seat query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count from billboard.seat_trade trade
    where trade.seat_id = p_seat_id and trade.trade_date between p_start_date and p_end_date;
    select coalesce(jsonb_agg(selected.item order by selected.trade_date desc, selected.event_id), '[]'::jsonb)
    into items from (
        select trade.trade_date, trade.event_id, jsonb_build_object(
            'event_id', trade.event_id, 'seat_trade_id', trade.seat_trade_id,
            'seat_id', trade.seat_id, 'symbol', trade.symbol, 'trade_date', trade.trade_date,
            'seat_name_raw', trade.seat_name_raw, 'buy_amount', trade.buy_amount::text,
            'sell_amount', trade.sell_amount::text,
            'net_amount', case when trade.buy_amount is not null and trade.sell_amount is not null
                then (trade.buy_amount - trade.sell_amount)::text else null end,
            'buy_rank', trade.buy_rank, 'sell_rank', trade.sell_rank,
            'is_institution', trade.is_institution, 'is_northbound', trade.is_northbound,
            'period_type', event.period_type, 'reason_code', reason.reason_code,
            'reason_name', reason.reason_name
        ) item
        from billboard.seat_trade trade
        join billboard.dragon_tiger_event event on event.event_id = trade.event_id
        join billboard.dragon_tiger_reason reason on reason.reason_id = event.reason_id
        where trade.seat_id = p_seat_id and trade.trade_date between p_start_date and p_end_date
        order by trade.trade_date desc, trade.event_id offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_event_metrics(p_event_id uuid) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare event_row billboard.dragon_tiger_event%rowtype;
begin
    if p_event_id is null then
        raise exception 'invalid DragonTiger event id' using errcode = '22023';
    end if;
    select * into event_row from billboard.dragon_tiger_event where event_id = p_event_id;
    if not found then return null; end if;
    return jsonb_build_object(
        'event_id', event_row.event_id,
        'net_amount', case when event_row.lhb_buy_amount is not null and event_row.lhb_sell_amount is not null
            then (event_row.lhb_buy_amount - event_row.lhb_sell_amount)::text else null end,
        'net_buy_strength', case when event_row.turnover_amount is not null and event_row.turnover_amount <> 0
            and event_row.lhb_buy_amount is not null and event_row.lhb_sell_amount is not null
            then ((event_row.lhb_buy_amount - event_row.lhb_sell_amount) / event_row.turnover_amount)::text else null end,
        'buy_seat_count', (select count(*) from billboard.seat_trade where event_id=p_event_id and buy_rank is not null),
        'sell_seat_count', (select count(*) from billboard.seat_trade where event_id=p_event_id and sell_rank is not null),
        'pure_buy_seat_count', (select count(*) from billboard.seat_trade where event_id=p_event_id and buy_amount > 0 and sell_amount = 0),
        'pure_sell_seat_count', (select count(*) from billboard.seat_trade where event_id=p_event_id and sell_amount > 0 and buy_amount = 0),
        'buy_sell_overlap_count', (select count(*) from billboard.seat_trade where event_id=p_event_id and buy_rank is not null and sell_rank is not null),
        'top1_buy_concentration', (select case when event_row.lhb_buy_amount is null or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null else (sum(buy_amount) / event_row.lhb_buy_amount)::text end from billboard.seat_trade where event_id=p_event_id and buy_rank<=1),
        'top3_buy_concentration', (select case when event_row.lhb_buy_amount is null or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null else (sum(buy_amount) / event_row.lhb_buy_amount)::text end from billboard.seat_trade where event_id=p_event_id and buy_rank<=3),
        'top5_buy_concentration', (select case when event_row.lhb_buy_amount is null or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null else (sum(buy_amount) / event_row.lhb_buy_amount)::text end from billboard.seat_trade where event_id=p_event_id and buy_rank<=5),
        'top1_sell_concentration', (select case when event_row.lhb_sell_amount is null or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null else (sum(sell_amount) / event_row.lhb_sell_amount)::text end from billboard.seat_trade where event_id=p_event_id and sell_rank<=1),
        'top3_sell_concentration', (select case when event_row.lhb_sell_amount is null or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null else (sum(sell_amount) / event_row.lhb_sell_amount)::text end from billboard.seat_trade where event_id=p_event_id and sell_rank<=3),
        'top5_sell_concentration', (select case when event_row.lhb_sell_amount is null or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null else (sum(sell_amount) / event_row.lhb_sell_amount)::text end from billboard.seat_trade where event_id=p_event_id and sell_rank<=5),
        'institution_buy_amount', (select case when count(*)=0 or count(*)<>count(buy_amount) then null else sum(buy_amount)::text end from billboard.seat_trade where event_id=p_event_id and is_institution and buy_rank is not null),
        'institution_sell_amount', (select case when count(*)=0 or count(*)<>count(sell_amount) then null else sum(sell_amount)::text end from billboard.seat_trade where event_id=p_event_id and is_institution and sell_rank is not null),
        'institution_net_amount', case
            when (select count(*)=0 or count(*)<>count(buy_amount) from billboard.seat_trade where event_id=p_event_id and is_institution and buy_rank is not null)
              or (select count(*)=0 or count(*)<>count(sell_amount) from billboard.seat_trade where event_id=p_event_id and is_institution and sell_rank is not null)
            then null else (
                (select sum(buy_amount) from billboard.seat_trade where event_id=p_event_id and is_institution and buy_rank is not null)
                - (select sum(sell_amount) from billboard.seat_trade where event_id=p_event_id and is_institution and sell_rank is not null)
            )::text end,
        'northbound_buy_amount', (select case when count(*)=0 or count(*)<>count(buy_amount) then null else sum(buy_amount)::text end from billboard.seat_trade where event_id=p_event_id and is_northbound and buy_rank is not null),
        'northbound_sell_amount', (select case when count(*)=0 or count(*)<>count(sell_amount) then null else sum(sell_amount)::text end from billboard.seat_trade where event_id=p_event_id and is_northbound and sell_rank is not null),
        'northbound_net_amount', case
            when (select count(*)=0 or count(*)<>count(buy_amount) from billboard.seat_trade where event_id=p_event_id and is_northbound and buy_rank is not null)
              or (select count(*)=0 or count(*)<>count(sell_amount) from billboard.seat_trade where event_id=p_event_id and is_northbound and sell_rank is not null)
            then null else (
                (select sum(buy_amount) from billboard.seat_trade where event_id=p_event_id and is_northbound and buy_rank is not null)
                - (select sum(sell_amount) from billboard.seat_trade where event_id=p_event_id and is_northbound and sell_rank is not null)
            )::text end
    );
end
$$;

revoke all on function api_v1._dragon_tiger_event_item(uuid) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_events_by_date(date,text,integer,integer) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_events_by_symbol(text,date,date,text,integer,integer) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_trades_by_seat(uuid,date,date,integer,integer) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_event_metrics(uuid) from public, anon, authenticated;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        execute 'grant execute on function api_v1.query_dragon_tiger_events_by_date(date,text,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_events_by_symbol(text,date,date,text,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_trades_by_seat(uuid,date,date,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_event_metrics(uuid) to market_data_api';
    end if;
end
$$;
