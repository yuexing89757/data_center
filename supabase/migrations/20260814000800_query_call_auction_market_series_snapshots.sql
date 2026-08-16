create function api_v1.query_call_auction_market_series_snapshots(
    p_trade_date date,
    p_codes text[]
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, realtime, core
set statement_timeout = '5s'
as $$
declare
    selected_session_id uuid;
    selected_session_status text;
    selected_expected_rounds integer;
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
        raise exception 'invalid call-auction market series snapshot query boundary'
            using errcode = '22023';
    end if;

    select session.session_id, session.status, session.expected_rounds
      into selected_session_id, selected_session_status, selected_expected_rounds
    from realtime.call_auction_market_series_session session
    where session.trade_date = p_trade_date
      and session.status in ('succeeded', 'partial')
    order by
        case session.status when 'succeeded' then 0 else 1 end,
        session.finished_at desc,
        session.started_at desc,
        session.session_id desc
    limit 1;

    if selected_session_id is null then
        raise exception 'call-auction market series snapshot not found'
            using errcode = 'P0002';
    end if;

    with requested_codes as (
        select distinct requested.code
        from unnest(p_codes) as requested(code)
    ), selected_rounds as (
        select round.*
        from realtime.call_auction_market_series_round round
        where round.session_id = selected_session_id
    ), round_payloads as (
        select
            round.sample_seq,
            jsonb_build_object(
                'sample_seq', round.sample_seq,
                'scheduled_at', round.scheduled_at,
                'collected_at', round.collected_at,
                'round_status', round.status,
                'selected_ingestion_id', round.selected_ingestion_id,
                'requested_count', (select count(*) from requested_codes),
                'returned_count', items.returned_count,
                'missing_codes', missing.missing_codes,
                'items', items.items
            ) as payload
        from selected_rounds round
        cross join lateral (
            select
                count(matched.symbol)::integer as returned_count,
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
                    ) filter (where matched.symbol is not null),
                    '[]'::jsonb
                ) as items
            from (
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
                from realtime.call_auction_market_series_snapshot snapshot
                join core.security security on security.symbol = snapshot.symbol
                join requested_codes requested on requested.code = security.code
                where snapshot.session_id = selected_session_id
                  and snapshot.sample_seq = round.sample_seq
                  and snapshot.ingestion_id = round.selected_ingestion_id
                  and snapshot.trade_date = p_trade_date
                  and security.exchange in ('SSE', 'SZSE')
            ) matched
        ) items
        cross join lateral (
            select coalesce(
                jsonb_agg(requested.code order by requested.code),
                '[]'::jsonb
            ) as missing_codes
            from requested_codes requested
            where not exists (
                select 1
                from realtime.call_auction_market_series_snapshot snapshot
                join core.security security on security.symbol = snapshot.symbol
                where snapshot.session_id = selected_session_id
                  and snapshot.sample_seq = round.sample_seq
                  and snapshot.ingestion_id = round.selected_ingestion_id
                  and snapshot.trade_date = p_trade_date
                  and security.exchange in ('SSE', 'SZSE')
                  and security.code = requested.code
            )
        ) missing
    ), rounds_payload as (
        select
            count(*)::integer as returned_rounds,
            coalesce(
                jsonb_agg(round_payloads.payload order by round_payloads.sample_seq),
                '[]'::jsonb
            ) as rounds
        from round_payloads
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'session_id', selected_session_id,
        'session_status', selected_session_status,
        'expected_rounds', selected_expected_rounds,
        'returned_rounds', rounds_payload.returned_rounds,
        'requested_count', (select count(*) from requested_codes),
        'rounds', rounds_payload.rounds
    )
      into payload
    from rounds_payload;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_market_series_snapshots(date, text[])
    from public;
grant execute on function api_v1.query_call_auction_market_series_snapshots(date, text[])
    to market_data_api;
