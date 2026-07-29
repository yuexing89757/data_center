create view api_v1.classification_member_snapshot_status as
select namespace, classification_type, classification_code,
       snapshot_date, member_count
from classification.member_snapshot;

revoke all on api_v1.classification_member_snapshot_status from public;

create function api_v1.query_securities(
    p_query text,
    p_limit integer default 20
)
returns table (
    symbol text,
    code text,
    exchange text,
    current_name text,
    security_type text,
    status text,
    ipo_date date,
    delisting_date date
)
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
declare
    v_query text := lower(btrim(p_query));
begin
    if p_query is null or v_query = '' then
        raise exception 'p_query must not be blank' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 100 then
        raise exception 'p_limit must be between 1 and 100' using errcode = '22023';
    end if;
    return query
    select security.symbol, security.code, security.exchange,
           security.current_name, security.security_type, security.status,
           security.ipo_date, security.delisting_date
    from api_v1.securities security
    where position(v_query in lower(security.symbol)) > 0
       or position(v_query in lower(security.code)) > 0
       or position(v_query in lower(security.current_name)) > 0
    order by
        case
            when lower(security.symbol) = v_query or lower(security.code) = v_query then 0
            when lower(security.current_name) = v_query then 1
            else 2
        end,
        security.symbol
    limit p_limit;
end
$$;

create function api_v1.query_daily_bars(
    p_symbol text,
    p_start_date date,
    p_end_date date,
    p_limit integer default 1000
)
returns table (
    symbol text,
    trade_date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    previous_close numeric,
    volume bigint,
    amount numeric,
    trade_status text,
    is_st boolean
)
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE|BSE):[0-9]{6}$' then
        raise exception 'p_symbol must be a standard symbol' using errcode = '22023';
    end if;
    if p_start_date is null or p_end_date is null or p_end_date < p_start_date then
        raise exception 'date range is invalid' using errcode = '22023';
    end if;
    if p_end_date - p_start_date > 3660 then
        raise exception 'date range must not exceed 3661 days' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 5000 then
        raise exception 'p_limit must be between 1 and 5000' using errcode = '22023';
    end if;
    return query
    select bar.symbol, bar.trade_date, bar.open, bar.high, bar.low, bar.close,
           bar.previous_close, bar.volume, bar.amount, bar.trade_status, bar.is_st
    from api_v1.daily_bars bar
    where bar.symbol = p_symbol
      and bar.trade_date between p_start_date and p_end_date
    order by bar.trade_date
    limit p_limit;
end
$$;

create function api_v1.query_adjusted_daily_bars(
    p_symbol text,
    p_start_date date,
    p_end_date date,
    p_adjustment_type text default 'forward',
    p_algorithm_version text default '1.0.0',
    p_calculation_id uuid default null,
    p_limit integer default 1000
)
returns table (
    symbol text,
    trade_date date,
    adjustment_type text,
    adjustment_factor numeric,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    previous_close numeric,
    calculation_id uuid,
    algorithm_version text,
    calculation_start_date date,
    calculation_end_date date,
    input_hash text,
    calculated_at timestamptz
)
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
declare
    v_calculation_id uuid;
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE|BSE):[0-9]{6}$' then
        raise exception 'p_symbol must be a standard symbol' using errcode = '22023';
    end if;
    if p_start_date is null or p_end_date is null or p_end_date < p_start_date then
        raise exception 'date range is invalid' using errcode = '22023';
    end if;
    if p_end_date - p_start_date > 3660 then
        raise exception 'date range must not exceed 3661 days' using errcode = '22023';
    end if;
    if p_adjustment_type is null or p_adjustment_type not in ('forward', 'backward') then
        raise exception 'p_adjustment_type must be forward or backward'
            using errcode = '22023';
    end if;
    if p_algorithm_version is null or btrim(p_algorithm_version) = '' then
        raise exception 'p_algorithm_version must not be blank' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 5000 then
        raise exception 'p_limit must be between 1 and 5000' using errcode = '22023';
    end if;

    if p_calculation_id is null then
        select run.calculation_id
        into v_calculation_id
        from api_v1.calculation_runs run
        where run.calculation_code = 'cn_a_share_daily_derived'
          and run.algorithm_version = p_algorithm_version
          and run.status = 'succeeded'
          and run.start_date <= p_start_date
          and run.end_date >= p_end_date
        order by run.calculated_at desc, run.calculation_id desc
        limit 1;
    else
        select run.calculation_id
        into v_calculation_id
        from api_v1.calculation_runs run
        where run.calculation_id = p_calculation_id
          and run.algorithm_version = p_algorithm_version
          and run.status = 'succeeded'
          and run.start_date <= p_start_date
          and run.end_date >= p_end_date;
    end if;
    if v_calculation_id is null then
        raise exception 'no compatible calculation version exists' using errcode = 'P0002';
    end if;

    return query
    select bar.symbol, bar.trade_date, bar.adjustment_type,
           bar.adjustment_factor, bar.open, bar.high, bar.low, bar.close,
           bar.previous_close, bar.calculation_id, bar.algorithm_version,
           bar.calculation_start_date, bar.calculation_end_date,
           bar.input_hash, bar.calculated_at
    from api_v1.adjusted_daily_bars bar
    where bar.calculation_id = v_calculation_id
      and bar.symbol = p_symbol
      and bar.trade_date between p_start_date and p_end_date
      and bar.adjustment_type = p_adjustment_type
    order by bar.trade_date
    limit p_limit;
end
$$;

create function api_v1.query_market_snapshot(
    p_trade_date date,
    p_algorithm_version text default '1.0.0',
    p_calculation_id uuid default null,
    p_limit integer default 5000
)
returns table (
    symbol text,
    trade_date date,
    raw_close numeric,
    total_return_1d numeric,
    moving_average_5 numeric,
    moving_average_10 numeric,
    moving_average_20 numeric,
    total_market_cap numeric,
    circulating_market_cap numeric,
    calculation_id uuid,
    algorithm_version text,
    input_hash text,
    calculated_at timestamptz
)
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
declare
    v_calculation_id uuid;
begin
    if p_trade_date is null then
        raise exception 'p_trade_date must not be null' using errcode = '22023';
    end if;
    if p_algorithm_version is null or btrim(p_algorithm_version) = '' then
        raise exception 'p_algorithm_version must not be blank' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 5000 then
        raise exception 'p_limit must be between 1 and 5000' using errcode = '22023';
    end if;

    if p_calculation_id is null then
        select run.calculation_id
        into v_calculation_id
        from api_v1.calculation_runs run
        where run.calculation_code = 'cn_a_share_daily_derived'
          and run.algorithm_version = p_algorithm_version
          and run.status = 'succeeded'
          and run.start_date <= p_trade_date
          and run.end_date >= p_trade_date
        order by run.calculated_at desc, run.calculation_id desc
        limit 1;
    else
        select run.calculation_id
        into v_calculation_id
        from api_v1.calculation_runs run
        where run.calculation_id = p_calculation_id
          and run.algorithm_version = p_algorithm_version
          and run.status = 'succeeded'
          and run.start_date <= p_trade_date
          and run.end_date >= p_trade_date;
    end if;
    if v_calculation_id is null then
        raise exception 'no compatible calculation version exists' using errcode = 'P0002';
    end if;

    return query
    select raw.symbol, raw.trade_date, raw.close,
           daily.total_return_1d, daily.moving_average_5,
           daily.moving_average_10, daily.moving_average_20,
           market.total_market_cap, market.circulating_market_cap,
           daily.calculation_id, daily.algorithm_version,
           daily.input_hash, daily.calculated_at
    from api_v1.daily_bars raw
    join api_v1.daily_metrics daily
      on daily.symbol = raw.symbol
     and daily.trade_date = raw.trade_date
     and daily.calculation_id = v_calculation_id
    left join api_v1.market_capitalizations market
      on market.symbol = raw.symbol
     and market.trade_date = raw.trade_date
     and market.calculation_id = v_calculation_id
    where raw.trade_date = p_trade_date
    order by raw.symbol
    limit p_limit;
end
$$;

create function api_v1.query_classification_members_as_of(
    p_namespace text,
    p_classification_type text,
    p_classification_code text,
    p_as_of_date date,
    p_limit integer default 5000
)
returns table (
    snapshot_date date,
    member_count integer,
    returned_count integer,
    members jsonb
)
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
declare
    v_snapshot_date date;
    v_member_count integer;
    v_members jsonb;
begin
    if p_namespace is null or btrim(p_namespace) = ''
       or p_classification_code is null or btrim(p_classification_code) = '' then
        raise exception 'classification identity must not be blank' using errcode = '22023';
    end if;
    if p_classification_type is null
       or p_classification_type not in ('industry', 'concept', 'index') then
        raise exception 'p_classification_type is invalid' using errcode = '22023';
    end if;
    if p_as_of_date is null then
        raise exception 'p_as_of_date must not be null' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 5000 then
        raise exception 'p_limit must be between 1 and 5000' using errcode = '22023';
    end if;

    select snapshot.snapshot_date, snapshot.member_count
    into v_snapshot_date, v_member_count
    from api_v1.classification_member_snapshot_status snapshot
    where snapshot.namespace = p_namespace
      and snapshot.classification_type = p_classification_type
      and snapshot.classification_code = p_classification_code
      and snapshot.snapshot_date <= p_as_of_date
    order by snapshot.snapshot_date desc
    limit 1;
    if v_snapshot_date is null then
        raise exception 'no classification snapshot exists as of the requested date'
            using errcode = 'P0002';
    end if;

    select coalesce(jsonb_agg(member.symbol order by member.symbol), '[]'::jsonb)
    into v_members
    from (
        select item.symbol
        from api_v1.classification_member_snapshots item
        where item.namespace = p_namespace
          and item.classification_type = p_classification_type
          and item.classification_code = p_classification_code
          and item.snapshot_date = v_snapshot_date
        order by item.symbol
        limit p_limit
    ) member;

    return query
    select v_snapshot_date, v_member_count,
           jsonb_array_length(v_members), v_members;
end
$$;

comment on function api_v1.query_securities(text, integer)
    is 'Bounded Security lookup for scripts, dashboards and agents.';
comment on function api_v1.query_daily_bars(text, date, date, integer)
    is 'Bounded unadjusted Daily Bar query using a closed date range.';
comment on function api_v1.query_adjusted_daily_bars(
    text, date, date, text, text, uuid, integer
) is 'Version-coherent adjusted Daily Bar query.';
comment on function api_v1.query_market_snapshot(date, text, uuid, integer)
    is 'Version-coherent raw price, return, moving-average and market-cap snapshot.';
comment on function api_v1.query_classification_members_as_of(
    text, text, text, date, integer
) is 'Latest complete classification membership snapshot not later than a date.';

revoke all on function api_v1.query_securities(text, integer) from public;
revoke all on function api_v1.query_daily_bars(text, date, date, integer) from public;
revoke all on function api_v1.query_adjusted_daily_bars(
    text, date, date, text, text, uuid, integer
) from public;
revoke all on function api_v1.query_market_snapshot(date, text, uuid, integer) from public;
revoke all on function api_v1.query_classification_members_as_of(
    text, text, text, date, integer
) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on api_v1.classification_member_snapshot_status to anon;
        grant execute on function api_v1.query_securities(text, integer) to anon;
        grant execute on function api_v1.query_daily_bars(text, date, date, integer) to anon;
        grant execute on function api_v1.query_adjusted_daily_bars(
            text, date, date, text, text, uuid, integer
        ) to anon;
        grant execute on function api_v1.query_market_snapshot(
            date, text, uuid, integer
        ) to anon;
        grant execute on function api_v1.query_classification_members_as_of(
            text, text, text, date, integer
        ) to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on api_v1.classification_member_snapshot_status to authenticated;
        grant execute on function api_v1.query_securities(text, integer) to authenticated;
        grant execute on function api_v1.query_daily_bars(
            text, date, date, integer
        ) to authenticated;
        grant execute on function api_v1.query_adjusted_daily_bars(
            text, date, date, text, text, uuid, integer
        ) to authenticated;
        grant execute on function api_v1.query_market_snapshot(
            date, text, uuid, integer
        ) to authenticated;
        grant execute on function api_v1.query_classification_members_as_of(
            text, text, text, date, integer
        ) to authenticated;
    end if;
end
$$;
