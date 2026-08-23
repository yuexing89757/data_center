alter table ingestion.ingestion_run drop constraint ingestion_run_provider_check;
alter table ingestion.ingestion_run add constraint ingestion_run_provider_check check (
    provider_code in (
        'baostock','akshare','akshare_ths','pytdx','tushare','pytdx_hq','eastmoney',
        'pysnowball','tencent_quote'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_provider_check;

create table realtime.stock_quote_snapshot (
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    symbol text not null references core.security (symbol),
    observed_at timestamptz not null,
    source_timestamp timestamptz not null,
    quote_status text not null check (quote_status in ('trading','suspended','closed','unknown')),
    last_price numeric(18,4),
    previous_close numeric(18,4),
    open numeric(18,4),
    high numeric(18,4),
    low numeric(18,4),
    cumulative_volume bigint,
    cumulative_amount numeric(24,4),
    bid1_price numeric(18,4), bid1_volume bigint,
    bid2_price numeric(18,4), bid2_volume bigint,
    bid3_price numeric(18,4), bid3_volume bigint,
    bid4_price numeric(18,4), bid4_volume bigint,
    bid5_price numeric(18,4), bid5_volume bigint,
    ask1_price numeric(18,4), ask1_volume bigint,
    ask2_price numeric(18,4), ask2_volume bigint,
    ask3_price numeric(18,4), ask3_volume bigint,
    ask4_price numeric(18,4), ask4_volume bigint,
    ask5_price numeric(18,4), ask5_volume bigint,
    source_code text not null check (source_code = 'tencent_quote'),
    created_at timestamptz not null default now(),
    primary key (ingestion_id, symbol),
    unique (symbol, observed_at),
    check (source_timestamp <= observed_at + interval '5 seconds'),
    check (low is null or high is null or low <= high),
    check (open is null or low is null or high is null or open between low and high),
    check (last_price is null or low is null or high is null or last_price between low and high),
    check (
        (last_price is null or last_price > 0)
        and (previous_close is null or previous_close > 0)
        and (open is null or open > 0)
        and (high is null or high > 0)
        and (low is null or low > 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    ),
    check (cumulative_volume is null or cumulative_volume >= 0),
    check (
        (bid1_price is null or bid1_price > 0) and (bid1_volume is null or bid1_volume >= 0)
        and (bid2_price is null or bid2_price > 0) and (bid2_volume is null or bid2_volume >= 0)
        and (bid3_price is null or bid3_price > 0) and (bid3_volume is null or bid3_volume >= 0)
        and (bid4_price is null or bid4_price > 0) and (bid4_volume is null or bid4_volume >= 0)
        and (bid5_price is null or bid5_price > 0) and (bid5_volume is null or bid5_volume >= 0)
        and (ask1_price is null or ask1_price > 0) and (ask1_volume is null or ask1_volume >= 0)
        and (ask2_price is null or ask2_price > 0) and (ask2_volume is null or ask2_volume >= 0)
        and (ask3_price is null or ask3_price > 0) and (ask3_volume is null or ask3_volume >= 0)
        and (ask4_price is null or ask4_price > 0) and (ask4_volume is null or ask4_volume >= 0)
        and (ask5_price is null or ask5_price > 0) and (ask5_volume is null or ask5_volume >= 0)
    )
);

create index stock_quote_snapshot_latest_idx
    on realtime.stock_quote_snapshot (symbol, observed_at desc);
create index stock_quote_snapshot_source_time_idx
    on realtime.stock_quote_snapshot (source_timestamp desc);

alter table realtime.stock_quote_snapshot enable row level security;
create policy stock_quote_snapshot_worker on realtime.stock_quote_snapshot
    for all to market_data_worker using (true) with check (true);
grant select, insert on realtime.stock_quote_snapshot to market_data_worker;

create function api_v1.query_latest_stock_quotes(
    p_codes text[],
    p_max_age_seconds integer default 15
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, core, realtime
set statement_timeout = '5s'
as $$
declare
    v_payload jsonb;
begin
    if p_codes is null
       or cardinality(p_codes) < 1
       or cardinality(p_codes) > 500
       or p_max_age_seconds < 1
       or p_max_age_seconds > 86400
       or exists (
           select 1 from unnest(p_codes) requested(code)
           where requested.code is null or requested.code !~ '^[0-9]{6}$'
       ) then
        raise exception 'invalid latest stock quote query boundary' using errcode = '22023';
    end if;

    if exists (
        select 1
        from (
            select requested.code
            from (select distinct code from unnest(p_codes) input(code)) requested
            join core.security security
              on security.code=requested.code and security.security_type='stock'
            group by requested.code
            having count(*) > 1
        ) ambiguous
    ) then
        raise exception 'stock code is ambiguous across exchanges' using errcode = 'P0003';
    end if;

    with requested_codes as (
        select requested.code, min(requested.ordinality)::integer as request_order
        from unnest(p_codes) with ordinality requested(code, ordinality)
        group by requested.code
    ), resolved as (
        select requested.code, requested.request_order, security.symbol, security.current_name
        from requested_codes requested
        left join core.security security
          on security.code=requested.code and security.security_type='stock'
    ), latest as (
        select resolved.code, resolved.request_order, resolved.symbol, resolved.current_name,
               quote.observed_at, quote.source_timestamp, quote.quote_status,
               quote.last_price, quote.previous_close, quote.open, quote.high, quote.low,
               quote.cumulative_volume, quote.cumulative_amount,
               quote.bid1_price, quote.bid1_volume, quote.bid2_price, quote.bid2_volume,
               quote.bid3_price, quote.bid3_volume, quote.bid4_price, quote.bid4_volume,
               quote.bid5_price, quote.bid5_volume,
               quote.ask1_price, quote.ask1_volume, quote.ask2_price, quote.ask2_volume,
               quote.ask3_price, quote.ask3_volume, quote.ask4_price, quote.ask4_volume,
               quote.ask5_price, quote.ask5_volume
        from resolved
        left join lateral (
            select value.*
            from realtime.stock_quote_snapshot value
            where value.symbol=resolved.symbol
              and value.observed_at >= now() - make_interval(secs => p_max_age_seconds)
              and value.source_timestamp >= now() - make_interval(secs => p_max_age_seconds)
            order by value.observed_at desc
            limit 1
        ) quote on true
    )
    select jsonb_build_object(
        'max_age_seconds', p_max_age_seconds,
        'requested_count', count(*)::integer,
        'found_count', count(*) filter (where latest.observed_at is not null)::integer,
        'missing_codes', coalesce(
            jsonb_agg(latest.code order by latest.request_order)
                filter (where latest.observed_at is null),
            '[]'::jsonb
        ),
        'items', coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'symbol', latest.symbol,
                    'code', latest.code,
                    'name', latest.current_name,
                    'observed_at', latest.observed_at,
                    'source_timestamp', latest.source_timestamp,
                    'quote_status', latest.quote_status,
                    'last_price', latest.last_price,
                    'previous_close', latest.previous_close,
                    'open', latest.open,
                    'high', latest.high,
                    'low', latest.low,
                    'cumulative_volume_shares', latest.cumulative_volume,
                    'cumulative_amount_cny', latest.cumulative_amount,
                    'bid_levels', jsonb_build_array(
                        jsonb_build_object('level',1,'price',latest.bid1_price,'volume_shares',latest.bid1_volume),
                        jsonb_build_object('level',2,'price',latest.bid2_price,'volume_shares',latest.bid2_volume),
                        jsonb_build_object('level',3,'price',latest.bid3_price,'volume_shares',latest.bid3_volume),
                        jsonb_build_object('level',4,'price',latest.bid4_price,'volume_shares',latest.bid4_volume),
                        jsonb_build_object('level',5,'price',latest.bid5_price,'volume_shares',latest.bid5_volume)
                    ),
                    'ask_levels', jsonb_build_array(
                        jsonb_build_object('level',1,'price',latest.ask1_price,'volume_shares',latest.ask1_volume),
                        jsonb_build_object('level',2,'price',latest.ask2_price,'volume_shares',latest.ask2_volume),
                        jsonb_build_object('level',3,'price',latest.ask3_price,'volume_shares',latest.ask3_volume),
                        jsonb_build_object('level',4,'price',latest.ask4_price,'volume_shares',latest.ask4_volume),
                        jsonb_build_object('level',5,'price',latest.ask5_price,'volume_shares',latest.ask5_volume)
                    )
                ) order by latest.request_order
            ) filter (where latest.observed_at is not null),
            '[]'::jsonb
        )
    ) into v_payload
    from latest;
    return v_payload;
end
$$;

comment on function api_v1.query_latest_stock_quotes(text[], integer)
is 'Returns bounded latest persisted stock quote snapshots when both observation and source timestamps are fresh.';

revoke all on function api_v1.query_latest_stock_quotes(text[], integer) from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname='anon') then
        revoke all on function api_v1.query_latest_stock_quotes(text[], integer) from anon;
    end if;
    if exists (select 1 from pg_roles where rolname='authenticated') then
        grant execute on function api_v1.query_latest_stock_quotes(text[], integer) to authenticated;
    end if;
    if exists (select 1 from pg_roles where rolname='market_data_api') then
        grant execute on function api_v1.query_latest_stock_quotes(text[], integer) to market_data_api;
    end if;
end
$$;
