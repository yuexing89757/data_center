-- ADR-0048 clarification: single-symbol exact-date trigger status read.
-- Reuses the published-version selection contract: no date fallback, one
-- calculation_id per response, no fabricated next-session prices.
create function api_v1.query_regulation_symbol_trigger(
    p_trade_date date, p_symbol text
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, pg_temp
set statement_timeout = '5s'
as $$
declare
    v_context jsonb := regulation.query_context(p_trade_date, null, 1, 'triggers');
    v_meta jsonb := v_context->'metadata';
    v_id uuid := (v_meta->>'calculation_id')::uuid;
    v_watermark timestamptz := (v_meta->>'event_watermark')::timestamptz;
    v_status regulation.status%rowtype;
    v_event_keys jsonb;
    v_dates date[];
    v_official_count integer;
    v_direction text;
    v_triggers jsonb;
    v_next jsonb;
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE):[0-9]{6}$' then
        raise exception using errcode = '22023', message = 'invalid regulation query parameters';
    end if;

    select * into v_status from regulation.status s
    where s.calculation_id = v_id and s.symbol = p_symbol;
    if not found then
        raise exception using errcode = 'P0002', message = 'symbol is outside calculation coverage';
    end if;

    -- 30-trading-session official event count, same membership rule as recent-next.
    select input_event_keys into v_event_keys
    from regulation.calculation_run where calculation_id = v_id;
    if v_event_keys is null then
        raise exception using errcode = 'P0002', message = 'calculation event membership is unavailable';
    end if;
    select array_agg(trade_date order by trade_date) into v_dates
    from (
        select trade_date from core.trading_calendar
        where market = 'CN_A_SHARE' and is_trading_day and trade_date <= p_trade_date
        order by trade_date desc limit 30
    ) days;
    if coalesce(cardinality(v_dates), 0) <> 30 then
        raise exception using errcode = 'P0002', message = '30-session calendar is unavailable';
    end if;

    select count(*) into v_official_count
    from jsonb_to_recordset(v_event_keys)
        as k(source_code text, source_event_id text, source_content_hash text)
    join regulation.event e using (source_code, source_event_id, source_content_hash)
    where e.symbol = p_symbol and e.period_end_date = any(v_dates)
      and e.observed_at <= v_watermark
      and e.source_code in ('sse_official', 'szse_official')
      and e.event_type in ('ABNORMAL_VOLATILITY', 'SERIOUS_ABNORMAL_VOLATILITY');

    select e.direction into v_direction
    from jsonb_to_recordset(v_event_keys)
        as k(source_code text, source_event_id text, source_content_hash text)
    join regulation.event e using (source_code, source_event_id, source_content_hash)
    where e.symbol = p_symbol and e.period_end_date = any(v_dates)
      and e.observed_at <= v_watermark
      and e.source_code in ('sse_official', 'szse_official')
      and e.event_type in ('ABNORMAL_VOLATILITY', 'SERIOUS_ABNORMAL_VOLATILITY')
    order by e.period_end_date desc, e.published_at desc, e.source_code, e.source_event_id
    limit 1;

    select coalesce(jsonb_agg(jsonb_build_object(
        'rule_code', r.rule_code, 'level', r.level, 'direction', r.direction,
        'kind', r.kind, 'window_start_date', rr.window_start_date,
        'window_end_date', rr.window_end_date,
        'observed_window_days', rr.observed_window_days,
        'current_value', rr.current_value::text, 'threshold', rr.threshold::text,
        'secondary_current_value', rr.secondary_current_value::text,
        'secondary_threshold', rr.secondary_threshold::text,
        'event_count', rr.event_count, 'required_count', rr.required_count,
        'selected_reset_date', rr.selected_reset_date
    ) order by (r.level = 'SERIOUS_ABNORMAL') desc, r.rule_code), '[]'::jsonb)
    into v_triggers
    from regulation.rule_result rr join regulation.rule r using (rule_id)
    where rr.calculation_id = v_id and rr.symbol = p_symbol
      and rr.triggered and rr.data_completeness = 'COMPLETE';

    select coalesce(jsonb_agg(jsonb_build_object(
        'level', w.level, 'direction', w.direction, 'rule_code', r.rule_code,
        'benchmark_symbol', coalesce(r.benchmark_symbol, v_status.benchmark_symbol),
        'scenario_code', w.scenario_code, 'scenario_index_pct', w.scenario_index_pct::text,
        'next_day_reference_price', w.next_day_reference_price::text,
        'raw_trigger_price', w.raw_trigger_price::text,
        'trigger_price', w.next_day_trigger_price::text,
        'trigger_change_pct', w.next_day_trigger_pct::text,
        'lower_limit_price', w.lower_limit_price::text,
        'upper_limit_price', w.upper_limit_price::text,
        'reachability', case w.scenario_code
            when 'CURRENT' then 'CURRENTLY_TRIGGERED'
            when 'NONE' then 'NOT_PRICE_CALCULABLE' else w.reachability end,
        'window_start_date', w.window_start_date, 'window_end_date', w.window_end_date,
        'requires_official_event_confirmation', w.requires_official_event_confirmation
    ) order by (w.level = 'SERIOUS_ABNORMAL') desc, r.rule_code,
               case w.scenario_code when 'CURRENT' then 0 when 'NONE' then 1
                   when 'INDEX_DOWN_2' then 2 when 'INDEX_FLAT' then 3 else 4 end), '[]'::jsonb)
    into v_next
    from regulation.warning w join regulation.rule r using (rule_id)
    join regulation.rule_result rr
      on rr.calculation_id = w.calculation_id and rr.symbol = w.symbol and rr.rule_id = w.rule_id
    where w.calculation_id = v_id and w.symbol = p_symbol
      and v_status.applicability = 'APPLICABLE' and v_status.data_completeness = 'COMPLETE'
      and rr.data_completeness = 'COMPLETE';

    return v_meta || jsonb_build_object(
        'next_trade_date', v_context->'next_trade_date',
        'lookback_trading_days', 30, 'lookback_start_date', v_dates[1],
        'returned_count', 1, 'has_more', false, 'next_cursor', null,
        'code', split_part(p_symbol, ':', 2), 'symbol', p_symbol,
        'name', (
            select nh.name from core.security_name_history nh
            where nh.symbol = p_symbol and nh.effective_from <= p_trade_date
              and (nh.effective_to is null or nh.effective_to >= p_trade_date)
        ),
        'exchange', v_status.exchange, 'segment', v_status.segment,
        'applicability', v_status.applicability,
        'data_completeness', v_status.data_completeness,
        'calculated_state', v_status.calculated_state,
        'announced_state', v_status.announced_state,
        'is_triggered_today', v_status.calculated_state <> 'NORMAL',
        'close', v_status.close::text,
        'stock_daily_return_pct', v_status.stock_daily_return_pct::text,
        'benchmark_symbol', v_status.benchmark_symbol,
        'benchmark_close', v_status.benchmark_close::text,
        'benchmark_daily_return_pct', v_status.benchmark_daily_return_pct::text,
        'daily_deviation_pct', v_status.daily_deviation_pct::text,
        'official_event_count_30d', v_official_count,
        'official_event_direction_30d', v_direction,
        'abnormal_count_10d', v_status.abnormal_count_10d,
        'abnormal_count_10d_up', v_status.abnormal_count_10d_up,
        'abnormal_count_10d_down', v_status.abnormal_count_10d_down,
        'triggered_rules', v_triggers,
        'next_triggers', v_next
    );
end;
$$;

revoke all on function api_v1.query_regulation_symbol_trigger(date, text)
    from public, anon, authenticated;
grant execute on function api_v1.query_regulation_symbol_trigger(date, text)
    to market_data_api;

comment on function api_v1.query_regulation_symbol_trigger(date, text)
    is 'Exact-date single-symbol trigger status with dual-count windows and next-session conditions. Not a price prediction.';
