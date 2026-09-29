-- ADR-0060 / Issue #69. Independent calculated events; no official event mutations.
create table regulation.monitor_context (
    calculation_id uuid primary key references regulation.calculation_run(calculation_id),
    parent_calculation_id uuid references regulation.monitor_context(calculation_id),
    source jsonb not null check (jsonb_typeof(source) = 'object'),
    official_coverage text not null default 'UNKNOWN' check (official_coverage in ('UNKNOWN', 'COMPLETE')),
    check (parent_calculation_id is distinct from calculation_id)
);
create table regulation.monitor_input (
    calculation_id uuid not null references regulation.monitor_context(calculation_id),
    symbol text not null references core.security(symbol),
    name text,
    candidate_reasons text[] not null,
    payload jsonb not null check (jsonb_typeof(payload) = 'object'),
    primary key (calculation_id, symbol),
    check (symbol ~ '^(SSE|SZSE):[0-9]{6}$')
);
create table regulation.calculated_event (
    calculation_id uuid not null references regulation.monitor_context(calculation_id),
    symbol text not null,
    trade_date date not null check (trade_date >= date '2026-07-06'),
    level text not null check (level in ('ABNORMAL', 'SERIOUS_ABNORMAL')),
    direction text not null check (direction in ('UP', 'DOWN', 'NONE')),
    kind text not null check (kind in ('CUMULATIVE_DEVIATION', 'TURNOVER_COMPOSITE')),
    rule_codes text[] not null check (cardinality(rule_codes) > 0),
    windows jsonb not null check (jsonb_typeof(windows) = 'array'),
    primary key (calculation_id, symbol, trade_date, level, direction, kind),
    foreign key (calculation_id, symbol) references regulation.monitor_input(calculation_id, symbol),
    check ((kind = 'TURNOVER_COMPOSITE' and direction = 'NONE' and level = 'ABNORMAL')
        or (kind = 'CUMULATIVE_DEVIATION' and direction in ('UP', 'DOWN')))
);
create index calculated_event_symbol_date on regulation.calculated_event(symbol, trade_date);
create index monitor_context_parent on regulation.monitor_context(parent_calculation_id);
alter table regulation.monitor_context enable row level security;
alter table regulation.monitor_input enable row level security;
alter table regulation.calculated_event enable row level security;
create policy monitor_context_worker_read on regulation.monitor_context for select to market_data_worker using (true);
create policy monitor_context_worker_insert on regulation.monitor_context for insert to market_data_worker with check (true);
create policy monitor_input_worker_read on regulation.monitor_input for select to market_data_worker using (true);
create policy monitor_input_worker_insert on regulation.monitor_input for insert to market_data_worker with check (true);
create policy calculated_event_worker_read on regulation.calculated_event for select to market_data_worker using (true);
create policy calculated_event_worker_insert on regulation.calculated_event for insert to market_data_worker with check (true);
grant select, insert on regulation.monitor_context, regulation.monitor_input, regulation.calculated_event to market_data_worker;
revoke all on regulation.monitor_context, regulation.monitor_input, regulation.calculated_event from public, anon, authenticated, market_data_api;

-- Legacy APIs must never select a monitor batch just because it finished later.
-- Keep the existing function body/validation unchanged apart from explicit version routing.
do $$
declare
    definition text := pg_get_functiondef('regulation.query_context(date,text,integer,text)'::regprocedure);
    needle text := 'and r.completed_at is not null and (v_id is null or r.calculation_id = v_id)';
begin
    if position(needle in definition) = 0 then
        raise exception 'unexpected legacy regulation query_context definition';
    end if;
    execute replace(definition, needle,
        'and r.algorithm_version not like ''regulation-monitor.%'' ' || needle);
end;
$$;

-- A correction invalidates all descendants until they are recomputed in order.
create function regulation.monitor_chain_current(p_id uuid)
returns boolean language sql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
    with recursive chain as (
        select m.calculation_id,m.parent_calculation_id,r.trade_date
        from regulation.monitor_context m join regulation.calculation_run r using(calculation_id)
        where m.calculation_id=p_id
        union all
        select m.calculation_id,m.parent_calculation_id,r.trade_date
        from chain c join regulation.monitor_context m on m.calculation_id=c.parent_calculation_id
        join regulation.calculation_run r on r.calculation_id=m.calculation_id
    )
    select exists(select 1 from chain) and not exists (
        select 1 from chain c where c.parent_calculation_id is distinct from (
            select r.calculation_id from regulation.calculation_run r
            join regulation.monitor_context m using(calculation_id)
            where r.algorithm_version='regulation-monitor.v1'
              and r.status in ('SUCCEEDED','PARTIAL') and r.completed_at is not null
              and r.next_trade_date=c.trade_date
              and r.trade_date=(select max(trade_date) from core.trading_calendar
                  where market='CN_A_SHARE' and is_trading_day and trade_date<c.trade_date)
            order by r.completed_at desc,r.calculation_id desc limit 1
        )
    );
$$;
revoke all on function regulation.monitor_chain_current(uuid) from public,anon,authenticated,market_data_api;
grant execute on function regulation.monitor_chain_current(uuid) to market_data_worker;

create function regulation.monitor_query_context(p_trade_date date, p_calculation_id uuid default null)
returns jsonb language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
declare
    v_previous date;
    v_next date;
    v_run regulation.calculation_run%rowtype;
    v_source jsonb;
    v_coverage text;
begin
    if p_trade_date is null or p_trade_date < date '2026-07-06' or not exists (
        select 1 from core.trading_calendar where market = 'CN_A_SHARE'
        and trade_date = p_trade_date and is_trading_day
    ) then
        raise exception using errcode = '22023', message = 'monitor date is not an applicable trading day';
    end if;
    select max(trade_date) into v_previous from core.trading_calendar
    where market = 'CN_A_SHARE' and is_trading_day and trade_date < p_trade_date;
    select min(trade_date) into v_next from core.trading_calendar
    where market = 'CN_A_SHARE' and is_trading_day and trade_date > p_trade_date;
    if v_next is null then
        raise exception using errcode = 'P0002', message = 'monitor next trading day is unavailable';
    end if;
    select r.* into v_run from regulation.calculation_run r
    join regulation.monitor_context m using (calculation_id)
    where r.algorithm_version = 'regulation-monitor.v1'
      and r.status in ('SUCCEEDED','PARTIAL') and r.completed_at is not null
      and (r.trade_date = p_trade_date or
           (r.trade_date = v_previous and r.next_trade_date = p_trade_date))
    order by r.trade_date desc, r.completed_at desc, r.calculation_id desc limit 1;
    if not found then
        raise exception using errcode = 'P0002', message = 'monitor base batch is not published';
    end if;
    if (p_calculation_id is not null and p_calculation_id <> v_run.calculation_id)
       or not regulation.monitor_chain_current(v_run.calculation_id) then
        raise exception using errcode = 'P0004', message = 'monitor batch/date conflict or superseded chain';
    end if;
    select source, official_coverage into v_source, v_coverage from regulation.monitor_context
    where calculation_id = v_run.calculation_id;
    return jsonb_build_object(
        'schema_version', 'regulation-monitor.v1', 'trade_date', p_trade_date,
        'base_trade_date', v_run.trade_date, 'next_trade_date', v_next,
        'calculation_id', v_run.calculation_id, 'algorithm_version', v_run.algorithm_version,
        'rule_set_version', v_run.rule_set_version, 'completed_at', v_run.completed_at,
        'count_cutoff_date', v_run.trade_date, 'official_coverage', v_coverage,
        'official_watermark', v_run.event_watermark, 'source', v_source,
        'is_confirmed_close', v_run.trade_date = p_trade_date,
        'coverage', jsonb_build_object('expected_count', v_run.expected_count,
            'complete_count', v_run.complete_count, 'incomplete_count', v_run.incomplete_count,
            'not_applicable_count', v_run.not_applicable_count));
end;
$$;
revoke all on function regulation.monitor_query_context(date,uuid) from public, anon, authenticated, market_data_api;

create function api_v1.query_regulation_monitor_candidates(
    p_trade_date date, p_limit integer default 50, p_cursor text default null, p_query text default null
) returns jsonb language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
declare
    v_context jsonb;
    v_cursor jsonb;
    v_id uuid;
    v_query text := nullif(btrim(p_query), '');
    v_items jsonb;
    v_total integer;
    v_more boolean;
    v_next text;
begin
    if p_limit is null or p_limit not between 1 and 100 or length(v_query) > 80 then
        raise exception using errcode = '22023', message = 'invalid monitor candidate bounds';
    end if;
    if p_cursor is not null then
        begin
            if length(p_cursor) not between 1 and 2048 or p_cursor !~ '^[A-Za-z0-9_-]+$' then
                raise exception 'invalid cursor encoding';
            end if;
            v_cursor := convert_from(decode(translate(p_cursor, '-_', '+/') ||
                repeat('=', (4-length(p_cursor)%4)%4), 'base64'), 'UTF8')::jsonb;
            if v_cursor->>'trade_date' is distinct from p_trade_date::text
               or v_cursor->>'query' is distinct from v_query
               or v_cursor->'limit' is distinct from to_jsonb(p_limit)
               or coalesce(v_cursor->>'symbol', '') !~ '^(SSE|SZSE):[0-9]{6}$'
               or jsonb_typeof(v_cursor->'calculation_id') is distinct from 'string'
               or v_cursor->>'endpoint' is distinct from 'monitor-candidates'
               or not (v_cursor ?& array['trade_date','query','limit','symbol','endpoint','calculation_id']) then
                raise exception 'invalid cursor identity';
            end if;
            v_id := (v_cursor->>'calculation_id')::uuid;
        exception when others then
            raise exception using errcode = '22023', message = 'invalid monitor cursor';
        end;
    end if;
    v_context := regulation.monitor_query_context(p_trade_date, v_id);
    v_id := (v_context->>'calculation_id')::uuid;
    with filtered as materialized (
        select symbol, name, candidate_reasons from regulation.monitor_input
        where calculation_id = v_id
          and (cardinality(candidate_reasons) > 0 or split_part(symbol, ':', 2) = v_query)
          and (v_query is null or split_part(symbol, ':', 2) = v_query
               or position(lower(v_query) in lower(name)) > 0)
    ), bounded as (
        select * from filtered where v_cursor is null or symbol > v_cursor->>'symbol'
        order by symbol limit p_limit + 1
    )
    select (select count(*) from filtered),
           coalesce(jsonb_agg(jsonb_build_object('symbol', symbol, 'code', split_part(symbol, ':', 2),
               'name', name, 'candidate_basis', candidate_reasons) order by symbol), '[]'::jsonb)
    into v_total, v_items from bounded;
    v_more := jsonb_array_length(v_items) > p_limit;
    if v_more then
        v_items := v_items - p_limit;
        v_next := rtrim(translate(replace(encode(convert_to(jsonb_build_object(
            'trade_date', p_trade_date, 'query', v_query, 'limit', p_limit,
            'symbol', v_items->-1->>'symbol', 'calculation_id', v_id,
            'endpoint', 'monitor-candidates')::text, 'UTF8'), 'base64'), E'\n', ''), '+/', '-_'), '=');
    end if;
    -- Exact lookup outside published coverage remains explicitly queryable.
    if v_total = 0 and v_query ~ '^[0-9]{6}$' then
        v_items := jsonb_build_array(jsonb_build_object('code', v_query, 'symbol', null,
            'name', null, 'candidate_basis', jsonb_build_array('EXACT_LOOKUP_OUTSIDE_COVERAGE')));
        v_total := 1;
    end if;
    return (v_context - 'source') || jsonb_build_object('items', v_items, 'total', v_total,
        'next_cursor', v_next, 'requested_count', p_limit, 'found_count', jsonb_array_length(v_items),
        'missing_count', 0, 'candidate_basis', 'PUBLISHED_CLOSE_OBSERVATION_LIST');
end;
$$;

create function api_v1.query_regulation_monitor_inputs(p_trade_date date, p_calculation_id uuid, p_codes text[])
returns jsonb language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
declare
    v_context jsonb;
    v_items jsonb;
begin
    if p_calculation_id is null or p_codes is null or cardinality(p_codes) not between 1 and 50
       or exists(select 1 from unnest(p_codes) c where c is null or c !~ '^[0-9]{6}$') then
        raise exception using errcode = '22023', message = 'invalid monitor input batch';
    end if;
    v_context := regulation.monitor_query_context(p_trade_date, p_calculation_id);
    if exists(select s.code from core.security s where s.code = any(p_codes)
        and s.security_type = 'stock' group by s.code having count(*) > 1) then
        raise exception using errcode = 'P0003', message = 'ambiguous stock identity';
    end if;
    select coalesce(jsonb_agg(jsonb_build_object('code', requested.code, 'symbol', s.symbol,
        'name', m.name, 'payload', m.payload,
        'target_applicability', case
            when facts.reason is null then 'APPLICABLE'
            when facts.reason in ('security_not_listed','st_security_excluded','no_limit_initial_listing_stage') then 'NOT_APPLICABLE'
            else 'INSUFFICIENT_DATA' end,
        'target_applicability_reason', facts.reason,
        -- A conditional reference, checked against currently known facts, not a promise
        -- that no future announcement can change tomorrow's reference price.
        'next_day_reference_safe', coalesce(
            m.payload->'candidate'->>'applicability' = 'APPLICABLE'
            and m.payload->'candidate'->>'next_day_reference_price' is not null
            and s.status = 'listed'
            and (s.delisting_date is null or s.delisting_date > (v_context->>'next_trade_date')::date)
            and not exists(select 1 from core.security_name_history n where n.symbol=s.symbol
                and n.effective_from <= (v_context->>'next_trade_date')::date
                and (n.effective_to is null or n.effective_to >= p_trade_date)
                and (upper(replace(n.name, ' ', '')) ~ '^\*?ST' or n.name like '%退%'))
            and not exists(select 1 from capital.distribution d where d.symbol=s.symbol
                and d.status <> 'cancelled'
                and (d.ex_date between p_trade_date and (v_context->>'next_trade_date')::date
                     or (d.ex_date is null and d.status in ('approved','unknown'))))
            and not exists(select 1 from capital.rights_issue r where r.symbol=s.symbol
                and (r.ex_date between p_trade_date and (v_context->>'next_trade_date')::date
                     or (r.ex_date is null and r.record_date >= (v_context->>'base_trade_date')::date))),
            false),
        'unrestricted_shares', null) order by requested.code), '[]'::jsonb)
    into v_items from (select distinct unnest(p_codes) as code) requested
    left join core.security s on s.code = requested.code and s.security_type = 'stock'
    left join regulation.monitor_input m on m.symbol = s.symbol and m.calculation_id = p_calculation_id
    left join lateral (
        select n.name from core.security_name_history n where n.symbol=s.symbol
          and n.effective_from <= p_trade_date and (n.effective_to is null or n.effective_to >= p_trade_date)
        order by n.effective_from desc limit 1
    ) target_name on true
    cross join lateral (
        select case
            when s.symbol is null then 'missing_security'
            when s.status <> 'listed' or s.delisting_date <= p_trade_date then 'security_not_listed'
            when target_name.name is null then 'missing_security_name_history'
            when upper(replace(target_name.name,' ','')) ~ '^\*?ST' or target_name.name like '%退%' then 'st_security_excluded'
            when s.ipo_date is null then 'missing_ipo_date'
            when (select count(*) from core.trading_calendar c where c.market='CN_A_SHARE'
                  and c.is_trading_day and c.trade_date between s.ipo_date and p_trade_date) >= 6 then null
            when s.ipo_date >= (select min(c.trade_date) from core.trading_calendar c where c.market='CN_A_SHARE')
                then 'no_limit_initial_listing_stage'
            else 'listing_calendar_unverified' end as reason
    ) facts;
    return v_context || jsonb_build_object('items', v_items,
        'reference_checked_at', statement_timestamp(),
        'requested_count', cardinality(p_codes),
        'found_count', (select count(*) from jsonb_array_elements(v_items) i where i->>'payload' is not null),
        'missing_count', (select count(*) from jsonb_array_elements(v_items) i where i->>'payload' is null));
end;
$$;
revoke all on function api_v1.query_regulation_monitor_candidates(date,integer,text,text) from public, anon, authenticated;
revoke all on function api_v1.query_regulation_monitor_inputs(date,uuid,text[]) from public, anon, authenticated;
grant execute on function api_v1.query_regulation_monitor_candidates(date,integer,text,text) to market_data_api;
grant execute on function api_v1.query_regulation_monitor_inputs(date,uuid,text[]) to market_data_api;
