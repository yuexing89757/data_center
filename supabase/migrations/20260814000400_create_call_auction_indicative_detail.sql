alter table ingestion.ingestion_run drop constraint ingestion_run_provider_check;
alter table ingestion.ingestion_run add constraint ingestion_run_provider_check check (
    provider_code in ('baostock','akshare','akshare_ths','pytdx','tushare','pytdx_hq','eastmoney')
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_provider_check;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source','call_auction_indicative_detail'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source','call_auction_indicative_detail'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

create table realtime.call_auction_indicative_snapshot (
    ingestion_id uuid primary key references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    version integer not null check (version > 0),
    status text not null check (status in ('succeeded','partial')),
    source_code text not null check (source_code = 'eastmoney'),
    record_count integer not null check (record_count >= 0),
    created_at timestamptz not null default now(),
    unique (symbol, trade_date, version)
);

create table realtime.call_auction_indicative_detail (
    ingestion_id uuid not null references realtime.call_auction_indicative_snapshot (ingestion_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    source_sequence integer not null check (source_sequence >= 0),
    observed_at timestamptz not null,
    indicative_price numeric(18,4) not null check (indicative_price > 0),
    displayed_volume_shares bigint not null check (
        displayed_volume_shares >= 0 and displayed_volume_shares % 100 = 0
    ),
    source_display_classification text not null check (
        source_display_classification in ('internal','external','unknown')
    ),
    primary key (ingestion_id, source_sequence),
    constraint auction_indicative_window check (
        (observed_at at time zone 'Asia/Shanghai')::date = trade_date
        and (observed_at at time zone 'Asia/Shanghai')::time >= time '09:15:00'
        and (observed_at at time zone 'Asia/Shanghai')::time < time '09:26:00'
    )
);

create index call_auction_indicative_snapshot_lookup_idx
    on realtime.call_auction_indicative_snapshot (symbol, trade_date, version desc);
create index call_auction_indicative_detail_order_idx
    on realtime.call_auction_indicative_detail (ingestion_id, observed_at, source_sequence);

alter table realtime.call_auction_indicative_snapshot enable row level security;
alter table realtime.call_auction_indicative_detail enable row level security;
create policy call_auction_indicative_snapshot_worker
    on realtime.call_auction_indicative_snapshot for all to market_data_worker
    using (true) with check (true);
create policy call_auction_indicative_detail_worker
    on realtime.call_auction_indicative_detail for all to market_data_worker
    using (true) with check (true);
grant select, insert on realtime.call_auction_indicative_snapshot,
    realtime.call_auction_indicative_detail to market_data_worker;
revoke all on realtime.call_auction_indicative_snapshot,
    realtime.call_auction_indicative_detail from public;

create function api_v1.query_call_auction_indicative_details(
    p_symbol text,
    p_trade_date date,
    p_offset integer default 0,
    p_limit integer default 200
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, api_v1, realtime, ingestion, audit
set statement_timeout = '5s'
as $$
declare selected realtime.call_auction_indicative_snapshot%rowtype;
begin
    if p_symbol !~ '^(SSE|SZSE):[0-9]{6}$' or p_trade_date is null
       or p_trade_date <> (now() at time zone 'Asia/Shanghai')::date
       or p_offset < 0 or p_offset > 5000 or p_limit < 1 or p_limit > 500 then
        raise exception 'invalid current-day auction indicative query boundary'
            using errcode = '22023';
    end if;
    select * into selected from realtime.call_auction_indicative_snapshot s
    where s.symbol=p_symbol and s.trade_date=p_trade_date
      and s.status in ('succeeded','partial')
    order by (s.status='succeeded') desc, s.version desc limit 1;
    if not found then raise no_data_found; end if;
    return jsonb_build_object(
        'symbol', selected.symbol, 'trade_date', selected.trade_date,
        'version', selected.version, 'ingestion_id', selected.ingestion_id,
        'raw_id', selected.raw_id, 'status', selected.status,
        'semantics', 'auction_virtual_indicative_matching_detail',
        'is_exchange_trade_tick', false, 'is_order_by_order', false,
        'total_count', selected.record_count, 'offset', p_offset,
        'returned_count', least(greatest(selected.record_count-p_offset,0),p_limit),
        'has_more', p_offset+p_limit < selected.record_count,
        'quality', jsonb_build_object(
            'partial', selected.status='partial',
            'source_display_classification_trusted', false
        ),
        'items', coalesce((select jsonb_agg(jsonb_build_object(
            'observed_at', d.observed_at, 'source_sequence', d.source_sequence,
            'indicative_price', d.indicative_price,
            'displayed_volume_shares', d.displayed_volume_shares,
            'source_display_classification', d.source_display_classification
        ) order by d.observed_at,d.source_sequence)
        from (select * from realtime.call_auction_indicative_detail x
              where x.ingestion_id=selected.ingestion_id
              order by x.observed_at,x.source_sequence offset p_offset limit p_limit) d), '[]'::jsonb)
    );
end $$;

revoke all on function api_v1.query_call_auction_indicative_details(text,date,integer,integer)
    from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_indicative_details(text,date,integer,integer)
    to market_data_api;
