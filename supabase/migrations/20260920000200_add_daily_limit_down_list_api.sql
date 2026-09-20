-- ADR-0056: bounded, exact-date read of immutable today_limit_down snapshots.
create function api_v1.query_daily_limit_down_list(
    p_trade_date date,
    p_version integer default null,
    p_offset integer default 0,
    p_limit integer default 200
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, today_limit_down
set statement_timeout = '5s'
as $$
declare
    payload jsonb;
begin
    if p_trade_date is null then
        raise exception using errcode = '22023', message = 'trade_date is required';
    end if;
    if p_version is not null and p_version < 1 then
        raise exception using errcode = '22023', message = 'version must be positive';
    end if;
    if p_offset < 0 or p_offset > 50000 then
        raise exception using errcode = '22023', message = 'offset is out of bounds';
    end if;
    if p_limit < 1 or p_limit > 500 then
        raise exception using errcode = '22023', message = 'limit is out of bounds';
    end if;

    with selected_snapshot as (
        select s.*
        from today_limit_down.snapshot s
        where s.trade_date = p_trade_date
          and (p_version is null or s.version = p_version)
        order by s.version desc
        limit 1
    ),
    quality_counts as (
        select q.rule_code, count(*)::integer as finding_count
        from selected_snapshot s
        join today_limit_down.calculation_quality q on q.snapshot_id = s.snapshot_id
        group by q.rule_code
    ),
    quality_payload as (
        select
            coalesce(sum(finding_count), 0)::integer as total_findings,
            coalesce(
                jsonb_object_agg(rule_code, finding_count order by rule_code),
                '{}'::jsonb
            ) as by_rule
        from quality_counts
    ),
    page as (
        select m.*
        from selected_snapshot s
        join today_limit_down.member m on m.snapshot_id = s.snapshot_id
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
                        'first_limit_down_at', p.first_limit_down_at,
                        'last_limit_down_at', p.last_limit_down_at,
                        'open_count', p.open_count,
                        'consecutive_limit_down_days', p.consecutive_limit_down_days,
                        'limit_down_duration_seconds', p.limit_down_duration_seconds,
                        'duration_semantics', p.duration_semantics,
                        'source_reported_sealed_funds_cny',
                            p.source_reported_sealed_funds_cny,
                        'closing_ask1_price', p.closing_ask1_price,
                        'closing_ask1_volume_shares', p.closing_ask1_volume_shares,
                        'closing_ask2_price', p.closing_ask2_price,
                        'closing_ask2_volume_shares', p.closing_ask2_volume_shares,
                        'closing_ask3_price', p.closing_ask3_price,
                        'closing_ask3_volume_shares', p.closing_ask3_volume_shares,
                        'closing_ask4_price', p.closing_ask4_price,
                        'closing_ask4_volume_shares', p.closing_ask4_volume_shares,
                        'closing_ask5_price', p.closing_ask5_price,
                        'closing_ask5_volume_shares', p.closing_ask5_volume_shares,
                        'closing_ask1_sealing_amount_cny',
                            p.closing_ask1_sealing_amount_cny,
                        'daily_bar_ingestion_id', p.daily_bar_ingestion_id,
                        'indicator_ingestion_id', p.indicator_ingestion_id,
                        'name_ingestion_id', p.name_ingestion_id,
                        'pool_calculation_id', p.pool_calculation_id,
                        'source_observation_ingestion_id',
                            p.source_observation_ingestion_id,
                        'source_observation_raw_id', p.source_observation_raw_id,
                        'order_book_ingestion_id', p.order_book_ingestion_id
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
        raise exception using errcode = 'P0002', message = 'daily limit-down snapshot not found';
    end if;
    return payload;
end
$$;

revoke all on function api_v1.query_daily_limit_down_list(date, integer, integer, integer)
    from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_daily_limit_down_list(
            date, integer, integer, integer
        ) to market_data_api;
    end if;
end
$$;
