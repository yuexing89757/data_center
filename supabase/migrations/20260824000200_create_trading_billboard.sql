create schema if not exists billboard;
revoke all on schema billboard from public;
grant usage on schema billboard to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'stock_pool','auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table billboard.entry (
    entry_id uuid primary key default extensions.gen_random_uuid(),
    symbol text not null references core.security (symbol),
    market text not null default 'CN_A_SHARE',
    trade_date date not null,
    source_event_id text not null,
    reason_code text not null,
    reason_text text not null,
    close_price numeric,
    change_rate_pct numeric,
    turnover_rate_pct numeric,
    market_amount numeric,
    buy_amount numeric not null,
    sell_amount numeric not null,
    net_amount numeric not null,
    deal_amount numeric not null,
    deal_to_market_pct numeric,
    net_to_market_pct numeric,
    free_float_market_value numeric,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    content_hash text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint trading_billboard_entry_calendar_fk
        foreign key (market, trade_date)
        references core.trading_calendar (market, trade_date),
    constraint trading_billboard_entry_source_event_unique
        unique (source_code, source_event_id),
    constraint trading_billboard_entry_semantic_unique
        unique (symbol, trade_date, reason_code),
    constraint trading_billboard_entry_parent_identity_unique
        unique (entry_id, source_code, source_event_id, symbol, trade_date),
    constraint trading_billboard_entry_market_check check (market = 'CN_A_SHARE'),
    constraint trading_billboard_entry_source_check check (source_code = 'eastmoney'),
    constraint trading_billboard_entry_text_check check (
        btrim(source_event_id) <> '' and btrim(reason_code) <> '' and btrim(reason_text) <> ''
    ),
    constraint trading_billboard_entry_nonnegative_check check (
        (close_price is null or close_price >= 0)
        and (turnover_rate_pct is null or turnover_rate_pct >= 0)
        and (market_amount is null or market_amount >= 0)
        and buy_amount >= 0 and sell_amount >= 0 and deal_amount >= 0
        and (deal_to_market_pct is null or deal_to_market_pct >= 0)
        and (free_float_market_value is null or free_float_market_value >= 0)
    ),
    constraint trading_billboard_entry_amount_check check (
        round(deal_amount, 2) = round(buy_amount + sell_amount, 2)
        and round(net_amount, 2) = round(buy_amount - sell_amount, 2)
    ),
    constraint trading_billboard_entry_hash_check check (
        content_hash ~ '^[0-9a-f]{64}$'
    )
);

create trigger trading_billboard_entry_set_updated_at
before update on billboard.entry
for each row execute function ingestion.set_updated_at();

create table billboard.seat (
    entry_id uuid not null,
    source_code text not null,
    source_event_id text not null,
    symbol text not null,
    trade_date date not null,
    side text not null,
    rank integer not null,
    seat_code text,
    seat_name text not null,
    buy_amount numeric,
    sell_amount numeric,
    net_amount numeric,
    buy_to_market_pct numeric,
    sell_to_market_pct numeric,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (entry_id, side, rank),
    constraint trading_billboard_seat_source_rank_unique
        unique (source_code, source_event_id, side, rank),
    constraint trading_billboard_seat_parent_fk
        foreign key (entry_id, source_code, source_event_id, symbol, trade_date)
        references billboard.entry (
            entry_id, source_code, source_event_id, symbol, trade_date
        ) on update cascade on delete cascade,
    constraint trading_billboard_seat_source_check check (source_code = 'eastmoney'),
    constraint trading_billboard_seat_side_check check (side in ('buy', 'sell')),
    constraint trading_billboard_seat_rank_check check (rank between 1 and 5),
    constraint trading_billboard_seat_text_check check (
        btrim(source_event_id) <> '' and btrim(seat_name) <> ''
        and (seat_code is null or btrim(seat_code) <> '')
    ),
    constraint trading_billboard_seat_nonnegative_check check (
        (buy_amount is null or buy_amount >= 0)
        and (sell_amount is null or sell_amount >= 0)
        and (buy_to_market_pct is null or buy_to_market_pct >= 0)
        and (sell_to_market_pct is null or sell_to_market_pct >= 0)
    ),
    constraint trading_billboard_seat_net_check check (
        buy_amount is null or sell_amount is null
        or (net_amount is not null and round(net_amount, 2) = round(buy_amount - sell_amount, 2))
    )
);

create index trading_billboard_entry_date_symbol_idx
    on billboard.entry (trade_date, symbol, entry_id);
create index trading_billboard_entry_symbol_date_idx
    on billboard.entry (symbol, trade_date desc, entry_id);
create index trading_billboard_seat_entry_side_rank_idx
    on billboard.seat (entry_id, side, rank);
create index trading_billboard_seat_symbol_date_idx
    on billboard.seat (symbol, trade_date desc);
create index trading_billboard_seat_code_date_idx
    on billboard.seat (seat_code, trade_date desc, entry_id, side)
    where seat_code is not null;
create index trading_billboard_seat_name_date_idx
    on billboard.seat (seat_name, trade_date desc, entry_id, side);

alter table billboard.entry enable row level security;
alter table billboard.seat enable row level security;

create policy trading_billboard_entry_worker_select on billboard.entry
    for select to market_data_worker using (true);
create policy trading_billboard_entry_worker_insert on billboard.entry
    for insert to market_data_worker with check (true);
create policy trading_billboard_entry_worker_update on billboard.entry
    for update to market_data_worker using (true) with check (true);
create policy trading_billboard_seat_worker_select on billboard.seat
    for select to market_data_worker using (true);
create policy trading_billboard_seat_worker_insert on billboard.seat
    for insert to market_data_worker with check (true);
create policy trading_billboard_seat_worker_delete on billboard.seat
    for delete to market_data_worker using (true);

grant select, insert, update on billboard.entry to market_data_worker;
grant select, insert, delete on billboard.seat to market_data_worker;
revoke all on billboard.entry, billboard.seat from public;

create function api_v1.query_trading_billboard_by_date(
    p_trade_date date,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare
    items jsonb;
    total_count integer;
begin
    if p_trade_date is null
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid trading billboard date query boundary'
            using errcode = '22023';
    end if;

    select count(*)::integer into total_count
    from billboard.entry entry
    where entry.trade_date = p_trade_date;

    select coalesce(jsonb_agg(selected.item order by selected.symbol, selected.entry_id), '[]'::jsonb)
    into items
    from (
        select entry.symbol, entry.entry_id, jsonb_build_object(
            'symbol', entry.symbol,
            'trade_date', entry.trade_date,
            'source_event_id', entry.source_event_id,
            'reason_code', entry.reason_code,
            'reason_text', entry.reason_text,
            'close_price', entry.close_price::text,
            'change_rate_pct', entry.change_rate_pct::text,
            'turnover_rate_pct', entry.turnover_rate_pct::text,
            'market_amount', entry.market_amount::text,
            'buy_amount', entry.buy_amount::text,
            'sell_amount', entry.sell_amount::text,
            'net_amount', entry.net_amount::text,
            'deal_amount', entry.deal_amount::text,
            'deal_to_market_pct', entry.deal_to_market_pct::text,
            'net_to_market_pct', entry.net_to_market_pct::text,
            'free_float_market_value', entry.free_float_market_value::text,
            'source_code', entry.source_code,
            'buy_seats', coalesce((
                select jsonb_agg(jsonb_build_object(
                    'symbol', seat.symbol,
                    'trade_date', seat.trade_date,
                    'source_event_id', seat.source_event_id,
                    'side', seat.side,
                    'rank', seat.rank,
                    'seat_code', seat.seat_code,
                    'seat_name', seat.seat_name,
                    'buy_amount', seat.buy_amount::text,
                    'sell_amount', seat.sell_amount::text,
                    'net_amount', seat.net_amount::text,
                    'buy_to_market_pct', seat.buy_to_market_pct::text,
                    'sell_to_market_pct', seat.sell_to_market_pct::text
                ) order by seat.rank)
                from billboard.seat seat
                where seat.entry_id = entry.entry_id and seat.side = 'buy'
            ), '[]'::jsonb),
            'sell_seats', coalesce((
                select jsonb_agg(jsonb_build_object(
                    'symbol', seat.symbol,
                    'trade_date', seat.trade_date,
                    'source_event_id', seat.source_event_id,
                    'side', seat.side,
                    'rank', seat.rank,
                    'seat_code', seat.seat_code,
                    'seat_name', seat.seat_name,
                    'buy_amount', seat.buy_amount::text,
                    'sell_amount', seat.sell_amount::text,
                    'net_amount', seat.net_amount::text,
                    'buy_to_market_pct', seat.buy_to_market_pct::text,
                    'sell_to_market_pct', seat.sell_to_market_pct::text
                ) order by seat.rank)
                from billboard.seat seat
                where seat.entry_id = entry.entry_id and seat.side = 'sell'
            ), '[]'::jsonb)
        ) as item
        from billboard.entry entry
        where entry.trade_date = p_trade_date
        order by entry.symbol, entry.entry_id
        offset p_offset limit p_limit
    ) selected;

    return jsonb_build_object(
        'items', items,
        'returned_count', jsonb_array_length(items),
        'total_count', total_count,
        'has_more', p_offset + jsonb_array_length(items) < total_count,
        'limit', p_limit,
        'offset', p_offset
    );
end
$$;

create function api_v1.query_trading_billboard_by_symbol(
    p_symbol text,
    p_start_date date,
    p_end_date date,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare
    items jsonb;
    total_count integer;
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE|BSE):[0-9]{6}$'
       or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid trading billboard symbol query boundary'
            using errcode = '22023';
    end if;

    select count(*)::integer into total_count
    from billboard.entry entry
    where entry.symbol = p_symbol
      and entry.trade_date between p_start_date and p_end_date;

    select coalesce(
        jsonb_agg(selected.item order by selected.trade_date desc, selected.entry_id),
        '[]'::jsonb
    ) into items
    from (
        select entry.trade_date, entry.entry_id, jsonb_build_object(
            'symbol', entry.symbol,
            'trade_date', entry.trade_date,
            'source_event_id', entry.source_event_id,
            'reason_code', entry.reason_code,
            'reason_text', entry.reason_text,
            'close_price', entry.close_price::text,
            'change_rate_pct', entry.change_rate_pct::text,
            'turnover_rate_pct', entry.turnover_rate_pct::text,
            'market_amount', entry.market_amount::text,
            'buy_amount', entry.buy_amount::text,
            'sell_amount', entry.sell_amount::text,
            'net_amount', entry.net_amount::text,
            'deal_amount', entry.deal_amount::text,
            'deal_to_market_pct', entry.deal_to_market_pct::text,
            'net_to_market_pct', entry.net_to_market_pct::text,
            'free_float_market_value', entry.free_float_market_value::text,
            'source_code', entry.source_code,
            'buy_seats', coalesce((
                select jsonb_agg(jsonb_build_object(
                    'symbol', seat.symbol, 'trade_date', seat.trade_date,
                    'source_event_id', seat.source_event_id, 'side', seat.side,
                    'rank', seat.rank, 'seat_code', seat.seat_code,
                    'seat_name', seat.seat_name, 'buy_amount', seat.buy_amount::text,
                    'sell_amount', seat.sell_amount::text, 'net_amount', seat.net_amount::text,
                    'buy_to_market_pct', seat.buy_to_market_pct::text,
                    'sell_to_market_pct', seat.sell_to_market_pct::text
                ) order by seat.rank)
                from billboard.seat seat
                where seat.entry_id = entry.entry_id and seat.side = 'buy'
            ), '[]'::jsonb),
            'sell_seats', coalesce((
                select jsonb_agg(jsonb_build_object(
                    'symbol', seat.symbol, 'trade_date', seat.trade_date,
                    'source_event_id', seat.source_event_id, 'side', seat.side,
                    'rank', seat.rank, 'seat_code', seat.seat_code,
                    'seat_name', seat.seat_name, 'buy_amount', seat.buy_amount::text,
                    'sell_amount', seat.sell_amount::text, 'net_amount', seat.net_amount::text,
                    'buy_to_market_pct', seat.buy_to_market_pct::text,
                    'sell_to_market_pct', seat.sell_to_market_pct::text
                ) order by seat.rank)
                from billboard.seat seat
                where seat.entry_id = entry.entry_id and seat.side = 'sell'
            ), '[]'::jsonb)
        ) as item
        from billboard.entry entry
        where entry.symbol = p_symbol
          and entry.trade_date between p_start_date and p_end_date
        order by entry.trade_date desc, entry.entry_id
        offset p_offset limit p_limit
    ) selected;

    return jsonb_build_object(
        'items', items,
        'returned_count', jsonb_array_length(items),
        'total_count', total_count,
        'has_more', p_offset + jsonb_array_length(items) < total_count,
        'limit', p_limit,
        'offset', p_offset
    );
end
$$;

create function api_v1.query_trading_billboard_by_seat(
    p_seat_code text,
    p_seat_name text,
    p_start_date date,
    p_end_date date,
    p_side text default null,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare
    items jsonb;
    total_count integer;
begin
    if (p_seat_code is null) = (p_seat_name is null)
       or (p_seat_code is not null and btrim(p_seat_code) = '')
       or (p_seat_name is not null and btrim(p_seat_name) = '')
       or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or (p_side is not null and p_side not in ('buy', 'sell'))
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid trading billboard seat query boundary'
            using errcode = '22023';
    end if;

    select count(*)::integer into total_count
    from billboard.seat seat
    where seat.trade_date between p_start_date and p_end_date
      and (p_side is null or seat.side = p_side)
      and (
          (p_seat_code is not null and seat.seat_code = p_seat_code)
          or (p_seat_name is not null and seat.seat_name = p_seat_name)
      );

    select coalesce(jsonb_agg(
        selected.item order by selected.trade_date desc, selected.symbol,
        selected.entry_id, selected.side, selected.rank
    ), '[]'::jsonb) into items
    from (
        select
            seat.trade_date, seat.symbol, seat.entry_id, seat.side, seat.rank,
            jsonb_build_object(
                'symbol', seat.symbol,
                'trade_date', seat.trade_date,
                'source_event_id', seat.source_event_id,
                'side', seat.side,
                'rank', seat.rank,
                'seat_code', seat.seat_code,
                'seat_name', seat.seat_name,
                'buy_amount', seat.buy_amount::text,
                'sell_amount', seat.sell_amount::text,
                'net_amount', seat.net_amount::text,
                'buy_to_market_pct', seat.buy_to_market_pct::text,
                'sell_to_market_pct', seat.sell_to_market_pct::text,
                'reason_code', entry.reason_code,
                'reason_text', entry.reason_text,
                'summary_buy_amount', entry.buy_amount::text,
                'summary_sell_amount', entry.sell_amount::text,
                'summary_net_amount', entry.net_amount::text,
                'summary_deal_amount', entry.deal_amount::text,
                'source_code', seat.source_code
            ) as item
        from billboard.seat seat
        join billboard.entry entry on entry.entry_id = seat.entry_id
        where seat.trade_date between p_start_date and p_end_date
          and (p_side is null or seat.side = p_side)
          and (
              (p_seat_code is not null and seat.seat_code = p_seat_code)
              or (p_seat_name is not null and seat.seat_name = p_seat_name)
          )
        order by seat.trade_date desc, seat.symbol, seat.entry_id, seat.side, seat.rank
        offset p_offset limit p_limit
    ) selected;

    return jsonb_build_object(
        'items', items,
        'returned_count', jsonb_array_length(items),
        'total_count', total_count,
        'has_more', p_offset + jsonb_array_length(items) < total_count,
        'limit', p_limit,
        'offset', p_offset
    );
end
$$;

revoke all on function api_v1.query_trading_billboard_by_date(date, integer, integer)
    from public, anon, authenticated;
revoke all on function api_v1.query_trading_billboard_by_symbol(
    text, date, date, integer, integer
) from public, anon, authenticated;
revoke all on function api_v1.query_trading_billboard_by_seat(
    text, text, date, date, text, integer, integer
) from public, anon, authenticated;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        execute 'grant execute on function api_v1.query_trading_billboard_by_date(date, integer, integer) to market_data_api';
        execute 'grant execute on function api_v1.query_trading_billboard_by_symbol(text, date, date, integer, integer) to market_data_api';
        execute 'grant execute on function api_v1.query_trading_billboard_by_seat(text, text, date, date, text, integer, integer) to market_data_api';
    end if;
end
$$;
