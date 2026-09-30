-- ADR-0063 / Issue #87. An explicit period is part of the immutable v3 input.
-- v2 source JSON has no monitor_start_date and retains its 2026-07-06 origin.
create or replace function regulation.monitor_chain_current(p_id uuid)
returns boolean language sql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
    with recursive chain as (
        select m.calculation_id,m.parent_calculation_id,m.st_watermark,r.trade_date,
               r.algorithm_version,
               coalesce(m.source->>'monitor_start_date','2026-07-06')::date as period_start
        from regulation.monitor_context m join regulation.calculation_run r using(calculation_id)
        where m.calculation_id=p_id
        union all
        select m.calculation_id,m.parent_calculation_id,m.st_watermark,r.trade_date,
               r.algorithm_version,
               coalesce(m.source->>'monitor_start_date','2026-07-06')::date
        from chain c join regulation.monitor_context m on m.calculation_id=c.parent_calculation_id
        join regulation.calculation_run r on r.calculation_id=m.calculation_id
    )
    select exists(select 1 from chain) and not exists (
        select 1 from chain c where
            c.algorithm_version not in ('regulation-monitor.v2','regulation-monitor.v3')
            or c.trade_date<c.period_start
            or (c.trade_date>c.period_start and c.parent_calculation_id is null)
            or c.st_watermark is distinct from coalesce((
                select encode(extensions.digest(st.ingestion_id::text, 'sha256'), 'hex')
                from regulation.st_day_snapshot st where st.trade_date=c.trade_date
            ), 'none')
            or c.parent_calculation_id is distinct from (
                select r.calculation_id from regulation.calculation_run r
                join regulation.monitor_context m using(calculation_id)
                where r.algorithm_version=c.algorithm_version
                  and coalesce(m.source->>'monitor_start_date','2026-07-06')::date=c.period_start
                  and r.trade_date>=c.period_start
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
    version_needle text := 'where r.algorithm_version = ''regulation-monitor.v2''';
    metadata_needle text := '''count_cutoff_date'', v_run.trade_date, ''official_coverage'', v_coverage,';
begin
    definition := pg_get_functiondef('regulation.monitor_query_context(date,uuid)'::regprocedure);
    if position(version_needle in definition)=0 or position(metadata_needle in definition)=0 then
        raise exception 'unexpected monitor_query_context definition';
    end if;
    definition := replace(definition, version_needle,
        'where r.algorithm_version in (''regulation-monitor.v2'',''regulation-monitor.v3'')');
    definition := replace(definition, metadata_needle,
        '''count_cutoff_date'', v_run.trade_date, '
        || '''monitor_start_date'', coalesce((v_source->>''monitor_start_date'')::date,date ''2026-07-06''), '
        || '''count_scope_label'', ''本期内次数(最近10个交易日)'', '
        || '''official_coverage'', v_coverage,');
    execute definition;
end;
$$;
