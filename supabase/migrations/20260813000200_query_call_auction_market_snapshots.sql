create function api_v1.query_call_auction_market_snapshots(
    p_trade_date date,
    p_codes text[]
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, ingestion, realtime, core
set statement_timeout = '5s'
as $$
declare
    selected_ingestion_id uuid;
    selected_status text;
    payload jsonb;
begin
    if p_trade_date is null
       or p_codes is null
       or cardinality(p_codes) < 1
       or cardinality(p_codes) > 500
       or exists (
           select 1
           from unnest(p_codes) as requested(code)
           where requested.code is null or requested.code !~ '^[0-9]{6}$'
       ) then
        raise exception 'invalid call-auction market snapshot query boundary'
            using errcode = '22023';
    end if;

    select run.ingestion_id, run.status
      into selected_ingestion_id, selected_status
    from ingestion.ingestion_run run
    where run.dataset_code = 'call_auction_market_snapshot'
      and run.status in ('succeeded', 'partial')
      and exists (
          select 1
          from realtime.call_auction_market_snapshot snapshot
          where snapshot.ingestion_id = run.ingestion_id
            and snapshot.trade_date = p_trade_date
      )
    order by
        case run.status when 'succeeded' then 0 else 1 end,
        run.finished_at desc,
        run.requested_at desc,
        run.ingestion_id desc
    limit 1;

    if selected_ingestion_id is null then
        raise exception 'call-auction market snapshot not found'
            using errcode = 'P0002';
    end if;

    with requested_codes as (
        select distinct requested.code
        from unnest(p_codes) as requested(code)
    ), matched as (
        select
            snapshot.symbol,
            security.code,
            snapshot.observed_at,
            snapshot.last_price,
            snapshot.previous_close,
            snapshot.high_price,
            snapshot.low_price,
            snapshot.cumulative_volume,
            snapshot.cumulative_amount
        from realtime.call_auction_market_snapshot snapshot
        join core.security security on security.symbol = snapshot.symbol
        join requested_codes requested on requested.code = security.code
        where snapshot.ingestion_id = selected_ingestion_id
          and snapshot.trade_date = p_trade_date
          and security.exchange in ('SSE', 'SZSE')
    ), item_payload as (
        select
            count(*)::integer as returned_count,
            coalesce(
                jsonb_agg(
                    jsonb_build_object(
                        'symbol', matched.symbol,
                        'code', matched.code,
                        'observed_at', matched.observed_at,
                        'last_price', matched.last_price,
                        'previous_close', matched.previous_close,
                        'high_price', matched.high_price,
                        'low_price', matched.low_price,
                        'cumulative_volume', matched.cumulative_volume,
                        'cumulative_amount', matched.cumulative_amount
                    ) order by matched.code, matched.symbol
                ),
                '[]'::jsonb
            ) as items
        from matched
    ), missing_payload as (
        select coalesce(
            jsonb_agg(requested.code order by requested.code),
            '[]'::jsonb
        ) as missing_codes
        from requested_codes requested
        where not exists (
            select 1 from matched where matched.code = requested.code
        )
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'ingestion_id', selected_ingestion_id,
        'ingestion_status', selected_status,
        'requested_count', (select count(*) from requested_codes),
        'returned_count', item_payload.returned_count,
        'missing_codes', missing_payload.missing_codes,
        'items', item_payload.items
    )
      into payload
    from item_payload
    cross join missing_payload;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_market_snapshots(date, text[])
    from public;
grant execute on function api_v1.query_call_auction_market_snapshots(date, text[])
    to market_data_api;
