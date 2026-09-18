create function api_v1.query_hot_money_actions_by_date(p_trade_date date)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, billboard, core
set statement_timeout = '5s'
as $$
declare
    result_items jsonb;
    result_count integer;
begin
    if p_trade_date is null then
        raise exception 'trade date is required' using errcode = '22023';
    end if;

    if not exists (
        select 1
        from billboard.dragon_tiger_event event
        where event.trade_date = p_trade_date
    ) then
        raise exception 'DragonTiger data was not found for the requested date'
            using errcode = 'P0002';
    end if;

    with distinct_actions as (
        select distinct
            actor.canonical_name as hot_money_name,
            security.code,
            security.current_name as stock_name,
            seat.canonical_name as seat_name,
            trade.buy_amount,
            trade.sell_amount
        from billboard.hot_money_actor actor
        join billboard.hot_money_seat_mapping mapping
          on mapping.actor_id = actor.actor_id
         and mapping.review_status = 'APPROVED'
         and (mapping.valid_from is null or mapping.valid_from <= p_trade_date)
         and (mapping.valid_to is null or mapping.valid_to >= p_trade_date)
        join billboard.trading_seat seat on seat.seat_id = mapping.seat_id
        join billboard.seat_trade trade
          on trade.seat_id = mapping.seat_id
         and trade.trade_date = p_trade_date
        join core.security security on security.symbol = trade.symbol
        where actor.is_active
          and actor.canonical_name in (
              '温州帮', '欢乐海岸', '鑫多多', '歌神',
              '小棉袄', '炒股养家', '方新侠'
          )
    )
    select
        coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'hot_money_name', hot_money_name,
                    'code', code,
                    'stock_name', stock_name,
                    'seat_name', seat_name,
                    'buy_amount', buy_amount::text,
                    'sell_amount', sell_amount::text,
                    'net_amount', case
                        when buy_amount is not null and sell_amount is not null
                        then (buy_amount - sell_amount)::text
                        else null
                    end
                )
                order by hot_money_name, code, seat_name,
                         buy_amount nulls first, sell_amount nulls first
            ),
            '[]'::jsonb
        ),
        count(*)::integer
    into result_items, result_count
    from distinct_actions;

    return jsonb_build_object(
        'trade_date', p_trade_date,
        'returned_count', result_count,
        'items', result_items
    );
end
$$;

revoke all on function api_v1.query_hot_money_actions_by_date(date)
from public, anon, authenticated;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        execute 'grant execute on function api_v1.query_hot_money_actions_by_date(date) to market_data_api';
    end if;
end
$$;

comment on function api_v1.query_hot_money_actions_by_date(date) is
    'Returns exact-date disclosed actions for the fixed reviewed hot-money actor catalog.';
