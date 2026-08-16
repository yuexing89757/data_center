create or replace function api_v1.query_call_auction_indicative_details(
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
declare
    selected realtime.call_auction_indicative_snapshot%rowtype;
    source_row_count integer;
begin
    if p_symbol !~ '^(SSE|SZSE):[0-9]{6}$' or p_trade_date is null
       or p_trade_date <> (now() at time zone 'Asia/Shanghai')::date
       or p_offset < 0 or p_offset > 5000 or p_limit < 1 or p_limit > 500 then
        raise exception 'invalid current-day auction indicative query boundary'
            using errcode = '22023';
    end if;

    select * into selected
    from realtime.call_auction_indicative_snapshot snapshot
    where snapshot.symbol = p_symbol
      and snapshot.trade_date = p_trade_date
      and snapshot.status in ('succeeded', 'partial')
    order by (snapshot.status = 'succeeded') desc, snapshot.version desc
    limit 1;

    if not found then
        raise no_data_found;
    end if;

    select manifest.row_count::integer into source_row_count
    from ingestion.raw_manifest manifest
    where manifest.raw_id = selected.raw_id;

    return jsonb_build_object(
        'symbol', selected.symbol,
        'trade_date', selected.trade_date,
        'fetched_at', selected.fetched_at,
        'source', 'eastmoney',
        'live_provider_derived', true,
        'data_origin', 'database',
        'cache_hit', false,
        'persistence_status', 'persisted',
        'version', selected.version,
        'ingestion_status', selected.status,
        'ingestion_id', selected.ingestion_id,
        'raw_id', selected.raw_id,
        'input_hash', selected.input_hash,
        'semantics', 'auction_virtual_indicative_matching_detail',
        'is_exchange_trade_tick', false,
        'is_order_by_order', false,
        'total_count', selected.record_count,
        'offset', p_offset,
        'returned_count', least(greatest(selected.record_count - p_offset, 0), p_limit),
        'has_more', p_offset + p_limit < selected.record_count,
        'quality', jsonb_build_object(
            'status', case when selected.status = 'succeeded' then 'complete' else 'partial' end,
            'source_row_count', source_row_count,
            'accepted_auction_row_count', selected.record_count,
            'source_display_classification_trusted', false,
            'raw_captured', true,
            'database_persistence', 'persisted'
        ),
        'items', coalesce((
            select jsonb_agg(jsonb_build_object(
                'observed_at', detail.observed_at,
                'source_sequence', detail.source_sequence,
                'indicative_price', detail.indicative_price,
                'displayed_volume_shares', detail.displayed_volume_shares,
                'source_display_classification', detail.source_display_classification
            ) order by detail.observed_at, detail.source_sequence)
            from (
                select *
                from realtime.call_auction_indicative_detail item
                where item.ingestion_id = selected.ingestion_id
                order by item.observed_at, item.source_sequence
                offset p_offset limit p_limit
            ) detail
        ), '[]'::jsonb)
    );
end
$$;

revoke all on function api_v1.query_call_auction_indicative_details(
    text, date, integer, integer
) from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_indicative_details(
    text, date, integer, integer
) to market_data_api;
