-- ADR-0078 / owner request 2026-11-10. Policy/schema only: no historical DELETE here.
set local lock_timeout = '2s';
set local statement_timeout = '10min';

create index quality_result_retention_created_idx
    on audit.quality_result(created_at, quality_result_id);

drop policy quality_result_worker_archived_delete on audit.quality_result;
create policy quality_result_worker_expired_delete on audit.quality_result
    for delete to market_data_worker using (
        created_at < (((current_timestamp at time zone 'Asia/Shanghai')::date - 15)::timestamp
                      at time zone 'Asia/Shanghai')
    );

alter table regulation.monitor_context add column inputs_expired_at timestamptz;
comment on column regulation.monitor_context.inputs_expired_at is
    'ADR-0078: input snapshot expired; first purge transaction time; queries/checkpoints must fail closed';
grant update(inputs_expired_at) on regulation.monitor_context to market_data_worker;
create policy monitor_context_worker_expire on regulation.monitor_context
    for update to market_data_worker using (
        exists(select 1 from regulation.calculation_run r
               where r.calculation_id=monitor_context.calculation_id
                 and r.trade_date < (current_timestamp at time zone 'Asia/Shanghai')::date - 15
                 and r.status <> 'RUNNING')
    ) with check (
        inputs_expired_at is not null and exists(
            select 1 from regulation.calculation_run r
            where r.calculation_id=monitor_context.calculation_id
              and r.trade_date < (current_timestamp at time zone 'Asia/Shanghai')::date - 15
              and r.status <> 'RUNNING')
    );
grant delete on regulation.monitor_input to market_data_worker;
create policy monitor_input_worker_expired_delete on regulation.monitor_input
    for delete to market_data_worker using (
        exists(select 1 from regulation.calculation_run r
               join regulation.monitor_context m using(calculation_id)
               where r.calculation_id=monitor_input.calculation_id
                 and r.trade_date < (current_timestamp at time zone 'Asia/Shanghai')::date - 15
                 and r.status <> 'RUNNING' and m.inputs_expired_at is not null)
    );
alter policy monitor_input_worker_insert on regulation.monitor_input with check (
    exists(select 1 from regulation.monitor_context m
           where m.calculation_id=monitor_input.calculation_id and m.inputs_expired_at is null)
);

-- Keep derived events/context after their bulky input expires. Enforce the same
-- input prerequisite at event publication, rather than a lifetime dependency.
alter table regulation.calculated_event
    drop constraint calculated_event_calculation_id_symbol_fkey;
alter table regulation.calculated_event add constraint calculated_event_symbol_fkey
    foreign key(symbol) references core.security(symbol);
create function regulation.require_monitor_event_input() returns trigger
language plpgsql set search_path=pg_catalog,pg_temp as $$
begin
    if not exists(select 1 from regulation.monitor_input q
                  join regulation.monitor_context m using(calculation_id)
                  where q.calculation_id=new.calculation_id and q.symbol=new.symbol
                    and m.inputs_expired_at is null) then
        raise exception using errcode='23503', message='monitor event requires retained input';
    end if;
    return new;
end;
$$;
revoke all on function regulation.require_monitor_event_input() from public;
create trigger calculated_event_input_required
    before insert or update of calculation_id,symbol on regulation.calculated_event
    for each row execute function regulation.require_monitor_event_input();

do $$
declare
    definition text := pg_get_functiondef('regulation.monitor_query_context(date,uuid)'::regprocedure);
    needle text := 'select source, official_coverage into v_source, v_coverage from regulation.monitor_context';
begin
    if position(needle in definition)=0 then
        raise exception 'unexpected monitor query definition';
    end if;
    execute replace(definition,needle,
        'if exists(select 1 from regulation.monitor_context m '
        || 'where m.calculation_id=v_run.calculation_id and m.inputs_expired_at is not null) then '
        || 'raise exception using errcode=''P0002'', message=''monitor inputs expired under 15-day retention''; '
        || 'end if; ' || needle);
end;
$$;
