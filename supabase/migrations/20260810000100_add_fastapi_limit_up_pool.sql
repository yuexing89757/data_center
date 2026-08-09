create or replace function api_v1.query_limit_up_pool(
    p_trade_date date,
    p_version integer default null,
    p_limit integer default 5000
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, stock_pool, derived, core
set statement_timeout = '5s'
as $$
declare
    selected stock_pool.snapshot%rowtype;
    items jsonb;
    valid_count integer;
    omitted_count integer;
    missing_name_count integer;
    missing_close_count integer;
    missing_free_float_shares_count integer;
begin
    if p_trade_date is null or p_limit < 1 or p_limit > 5000
       or (p_version is not null and p_version < 1) then
        raise exception 'invalid limit-up pool query boundary' using errcode = '22023';
    end if;

    select * into selected
    from stock_pool.snapshot snapshot
    where snapshot.pool_code = 'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP'
      and snapshot.basis_trade_date = p_trade_date
      and snapshot.status = 'ready'
      and (p_version is null or snapshot.version = p_version)
    order by snapshot.version desc
    limit 1;

    if not found then
        raise exception 'exact ready limit-up pool snapshot does not exist'
            using errcode = 'P0002';
    end if;

    with candidates as (
        select member.symbol, member.direction
        from stock_pool.member member
        where member.snapshot_id = selected.snapshot_id
    ), detail as (
        select candidates.symbol,
               security.code,
               name_history.name,
               event.close,
               indicator.free_float_shares,
               event.close * indicator.free_float_shares as free_float_market_cap_cny
        from candidates
        join core.security security on security.symbol = candidates.symbol
        left join core.security_name_history name_history
          on name_history.symbol = candidates.symbol
         and name_history.effective_from <= selected.basis_trade_date
         and (name_history.effective_to is null
              or name_history.effective_to >= selected.basis_trade_date)
        left join derived.price_limit_event event
          on event.calculation_id = selected.calculation_id
         and event.symbol = candidates.symbol
         and event.trade_date = selected.basis_trade_date
         and event.direction = 'up'
        left join core.stock_daily_indicator indicator
          on indicator.symbol = candidates.symbol
         and indicator.trade_date = selected.basis_trade_date
    ), valid_page as (
        select symbol, code, name, free_float_market_cap_cny
        from detail
        where name is not null and close is not null and free_float_shares is not null
        order by symbol
        limit p_limit
    )
    select
        count(*) filter (
            where name is not null and close is not null and free_float_shares is not null
        ),
        count(*) filter (
            where name is null or close is null or free_float_shares is null
        ),
        count(*) filter (where name is null),
        count(*) filter (where close is null),
        count(*) filter (where free_float_shares is null),
        (select coalesce(jsonb_agg(to_jsonb(valid_page) order by valid_page.symbol), '[]'::jsonb)
         from valid_page)
      into valid_count, omitted_count, missing_name_count, missing_close_count,
           missing_free_float_shares_count, items
    from detail;

    return jsonb_build_object(
        'snapshot_id', selected.snapshot_id,
        'calculation_id', selected.calculation_id,
        'trade_date', selected.basis_trade_date,
        'effective_trade_date', selected.effective_trade_date,
        'version', selected.version,
        'rule_version', selected.rule_version,
        'algorithm_version', selected.algorithm_version,
        'input_hash', selected.input_hash,
        'generated_at', selected.generated_at,
        'total_candidate_count', selected.member_count,
        'valid_count', valid_count,
        'returned_count', jsonb_array_length(items),
        'omitted_count', omitted_count,
        'has_more', valid_count > jsonb_array_length(items),
        'omission_reasons', jsonb_build_object(
            'missing_name', missing_name_count,
            'missing_close', missing_close_count,
            'missing_free_float_shares', missing_free_float_shares_count
        ),
        'items', items
    );
end
$$;

revoke all on function api_v1.query_limit_up_pool(date, integer, integer) from public;
grant execute on function api_v1.query_limit_up_pool(date, integer, integer)
    to market_data_api;
