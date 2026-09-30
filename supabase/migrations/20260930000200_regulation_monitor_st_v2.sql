-- ADR-0061 / Issue #85: route monitor publications to v2 and use dated ST facts.
alter table regulation.monitor_context
    add column st_watermark text not null default 'none';

-- A changed ST snapshot immediately supersedes the old day and all descendants.
create or replace function regulation.monitor_chain_current(p_id uuid)
returns boolean language sql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
    with recursive chain as (
        select m.calculation_id,m.parent_calculation_id,m.st_watermark,r.trade_date
        from regulation.monitor_context m join regulation.calculation_run r using(calculation_id)
        where m.calculation_id=p_id
        union all
        select m.calculation_id,m.parent_calculation_id,m.st_watermark,r.trade_date
        from chain c join regulation.monitor_context m on m.calculation_id=c.parent_calculation_id
        join regulation.calculation_run r on r.calculation_id=m.calculation_id
    )
    select exists(select 1 from chain) and not exists (
        select 1 from chain c where
            c.st_watermark is distinct from coalesce((
                select encode(extensions.digest(st.ingestion_id::text, 'sha256'), 'hex')
                from regulation.st_day_snapshot st where st.trade_date=c.trade_date
            ), 'none')
            or c.parent_calculation_id is distinct from (
                select r.calculation_id from regulation.calculation_run r
                join regulation.monitor_context m using(calculation_id)
                where r.algorithm_version='regulation-monitor.v2'
                  and r.status in ('SUCCEEDED','PARTIAL') and r.completed_at is not null
                  and r.next_trade_date=c.trade_date
                  and r.trade_date=(select max(trade_date) from core.trading_calendar
                      where market='CN_A_SHARE' and is_trading_day and trade_date<c.trade_date)
                order by r.completed_at desc,r.calculation_id desc limit 1
            )
    );
$$;

do $$
declare
    definition text;
begin
    definition := pg_get_functiondef('regulation.monitor_query_context(date,uuid)'::regprocedure);
    if position('where r.algorithm_version = ''regulation-monitor.v1''' in definition) = 0 then
        raise exception 'unexpected monitor_query_context definition';
    end if;
    execute replace(
        definition,
        'where r.algorithm_version = ''regulation-monitor.v1''',
        'where r.algorithm_version = ''regulation-monitor.v2'''
    );
end;
$$;

create or replace function api_v1.query_regulation_monitor_inputs(
    p_trade_date date, p_calculation_id uuid, p_codes text[]
)
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
        -- The next-day range is conditional until that day's ST fact exists.
        'next_day_st_verified', exists(select 1 from regulation.st_day_snapshot next_st
            where next_st.trade_date = (v_context->>'next_trade_date')::date),
        'next_day_reference_safe', coalesce(
            m.payload->'candidate'->>'applicability' = 'APPLICABLE'
            and m.payload->'candidate'->>'next_day_reference_price' is not null
            and s.status = 'listed'
            and (s.delisting_date is null or s.delisting_date > (v_context->>'next_trade_date')::date)
            and not exists(select 1 from regulation.st_day_snapshot next_st
                where next_st.trade_date = (v_context->>'next_trade_date')::date
                  and next_st.symbols ? s.symbol)
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
    left join regulation.st_day_snapshot target_st on target_st.trade_date = p_trade_date
    cross join lateral (
        select case
            when s.symbol is null then 'missing_security'
            when s.status <> 'listed' or s.delisting_date <= p_trade_date then 'security_not_listed'
            when target_st.ingestion_id is null then 'missing_regulation_st_snapshot'
            when target_st.symbols ? s.symbol then 'st_security_excluded'
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
