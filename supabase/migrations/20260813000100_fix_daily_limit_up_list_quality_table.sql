-- Fix api_v1.query_daily_limit_up_list: the quality CTE referenced
-- today_limit_up.member_quality, which does not exist. The real table is
-- today_limit_up.calculation_quality (created by migration 20260811000500,
-- written to by persistence/today_limit_up_postgres.py). The wrong name made
-- the RPC raise sqlstate 42P01 on every call, which the public API surfaces as
-- HTTP 503 service_unavailable. Rebuild the function with the correct table.
-- Forward-only ordered migration; safe whether or not 20260812000200 has been
-- applied, since the DROP IF EXISTS matches the current signature.

drop function if exists api_v1.query_daily_limit_up_list(date, integer, integer, integer);

create function api_v1.query_daily_limit_up_list(
    p_trade_date date,
    p_version integer default null,
    p_offset integer default 0,
    p_limit integer default 500
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, today_limit_up
set statement_timeout = '5s'
as $$
declare
    payload jsonb;
begin
    if p_limit < 1 or p_limit > 500 or p_offset < 0 then
        raise exception 'invalid pagination boundary' using errcode = '22023';
    end if;

    with selected_snapshot as (
        select *
        from today_limit_up.snapshot
        where trade_date = p_trade_date
          and (p_version is null or version = p_version)
        order by version desc
        limit 1
    ),
    quality_payload as (
        select
            count(*)::integer as total_findings,
            coalesce(
                jsonb_object_agg(rule_code, cnt order by rule_code),
                '{}'::jsonb
            ) as by_rule
        from (
            select rule_code, count(*)::integer as cnt
            from today_limit_up.calculation_quality
            where snapshot_id = (select snapshot_id from selected_snapshot)
            group by rule_code
        ) q
    ),
    page as (
        select m.*
        from selected_snapshot s
        join today_limit_up.member m on m.snapshot_id = s.snapshot_id
        order by m.symbol
        offset p_offset
        limit p_limit
    ),
    item_payload as (
        select
            count(*)::integer as returned_count,
            coalesce(
                jsonb_agg(
                    jsonb_build_object(
                        'symbol', p.symbol,
                        'code', p.code,
                        'name', p.historical_name,
                        'previous_close', p.previous_close,
                        'close', p.close,
                        'limit_price', p.limit_price,
                        'change_percent', p.change_percent,
                        'free_float_shares', p.free_float_shares,
                        'free_float_market_cap_cny', p.free_float_market_cap_cny,
                        'first_limit_up_at', p.first_limit_up_at,
                        'last_limit_up_at', p.last_limit_up_at,
                        'open_count', p.open_count,
                        'limit_up_duration_seconds', p.limit_up_duration_seconds,
                        'duration_semantics', p.duration_semantics,
                        'source_reported_sealed_funds_cny',
                            p.source_reported_sealed_funds_cny,
                        'closing_bid1_price', p.closing_bid1_price,
                        'closing_bid1_volume_shares', p.closing_bid1_volume_shares,
                        'closing_bid2_price', p.closing_bid2_price,
                        'closing_bid2_volume_shares', p.closing_bid2_volume_shares,
                        'closing_bid3_price', p.closing_bid3_price,
                        'closing_bid3_volume_shares', p.closing_bid3_volume_shares,
                        'closing_bid4_price', p.closing_bid4_price,
                        'closing_bid4_volume_shares', p.closing_bid4_volume_shares,
                        'closing_bid5_price', p.closing_bid5_price,
                        'closing_bid5_volume_shares', p.closing_bid5_volume_shares,
                        'closing_bid1_sealing_amount_cny',
                            p.closing_bid1_sealing_amount_cny,
                        'daily_bar_ingestion_id', p.daily_bar_ingestion_id,
                        'indicator_ingestion_id', p.indicator_ingestion_id,
                        'name_ingestion_id', p.name_ingestion_id,
                        'pool_calculation_id', p.pool_calculation_id,
                        'source_observation_ingestion_id',
                            p.source_observation_ingestion_id,
                        'source_observation_raw_id', p.source_observation_raw_id,
                        'order_book_ingestion_id', p.order_book_ingestion_id,
                        'volume', p.volume,
                        'amount_cny', p.amount_cny,
                        'free_float_turnover_rate_pct', p.free_float_turnover_rate_pct,
                        'consecutive_limit_up_days', p.consecutive_limit_up_days
                    ) order by p.symbol
                ),
                '[]'::jsonb
            ) as items
        from page p
    )
    select jsonb_build_object(
        'snapshot_id', s.snapshot_id,
        'calculation_id', s.calculation_id,
        'trade_date', s.trade_date,
        'version', s.version,
        'status', s.status,
        'rule_version', s.rule_version,
        'algorithm_version', s.algorithm_version,
        'input_hash', s.input_hash,
        'source_ingestion_id', s.source_ingestion_id,
        'generated_at', s.generated_at,
        'candidate_count', s.candidate_count,
        'member_count', s.member_count,
        'rejected_count', s.rejected_count,
        'offset', p_offset,
        'returned_count', i.returned_count,
        'has_more', p_offset + i.returned_count < s.member_count,
        'quality', jsonb_build_object(
            'total_findings', q.total_findings,
            'by_rule', q.by_rule
        ),
        'items', i.items
    )
    into payload
    from selected_snapshot s
    cross join quality_payload q
    cross join item_payload i;

    if payload is null then
        raise exception using errcode = 'P0002', message = 'daily limit-up snapshot not found';
    end if;
    return payload;
end
$$;

revoke all on function api_v1.query_daily_limit_up_list(date, integer, integer, integer)
    from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) to market_data_api;
    end if;
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant execute on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant execute on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) to authenticated;
    end if;
end
$$;
