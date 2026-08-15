create function api_v1.query_board_index_bias_latest()
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
declare
    payload jsonb;
begin
    if not exists (
        select 1
        from core.board_index_daily_bar
        where board_id = 'THS:883423'
    ) then
        raise exception 'board index daily bars not found' using errcode = 'P0002';
    end if;

    with recent_desc as (
        select
            limited.trade_date,
            limited.close,
            row_number() over (order by limited.trade_date desc)::integer as recent_rank
        from (
            select bar.trade_date, bar.close
            from core.board_index_daily_bar bar
            where bar.board_id = 'THS:883423'
            order by bar.trade_date desc
            limit 34
        ) limited
    ), rolling as (
        select
            recent_desc.*,
            count(*) filter (where close > 0) over (
                order by trade_date
                rows between 4 preceding and current row
            ) as positive_close_count,
            avg(close) filter (where close > 0) over (
                order by trade_date
                rows between 4 preceding and current row
            ) as moving_average_5
        from recent_desc
    ), scored as (
        select
            rolling.*,
            case
                when positive_close_count = 5 and moving_average_5 > 0
                then (close - moving_average_5) / moving_average_5 * 100
                else null
            end as bias_5_pct
        from rolling
    ), latest as (
        select * from scored where recent_rank = 1
    ), previous as (
        select * from scored where recent_rank = 2
    ), observation_window as (
        select * from scored where recent_rank <= 30
    )
    select jsonb_build_object(
        'board_id', board.board_id,
        'board_code', board.board_code,
        'board_name', board.name,
        'trade_date', latest.trade_date,
        'close', latest.close::text,
        'moving_average_5', case
            when latest.bias_5_pct is not null
            then round(latest.moving_average_5, 6)::text
            else null
        end,
        'bias_5_pct', case
            when latest.bias_5_pct is not null
            then round(latest.bias_5_pct, 6)::text
            else null
        end,
        'previous_trade_date', previous.trade_date,
        'previous_bias_5_pct', case
            when previous.bias_5_pct is not null
            then round(previous.bias_5_pct, 6)::text
            else null
        end,
        'bias_direction', case
            when latest.bias_5_pct is null or previous.bias_5_pct is null then null
            when latest.bias_5_pct > previous.bias_5_pct then 'up'
            when latest.bias_5_pct < previous.bias_5_pct then 'down'
            else 'flat'
        end,
        'window_trading_days', 30,
        'bias_sample_count', (
            select count(*) from observation_window where bias_5_pct is not null
        ),
        'highest_bias_5_pct', (
            select round(bias_5_pct, 6)::text
            from observation_window
            where bias_5_pct is not null
            order by bias_5_pct desc, trade_date desc
            limit 1
        ),
        'highest_bias_trade_date', (
            select trade_date
            from observation_window
            where bias_5_pct is not null
            order by bias_5_pct desc, trade_date desc
            limit 1
        ),
        'lowest_bias_5_pct', (
            select round(bias_5_pct, 6)::text
            from observation_window
            where bias_5_pct is not null
            order by bias_5_pct, trade_date desc
            limit 1
        ),
        'lowest_bias_trade_date', (
            select trade_date
            from observation_window
            where bias_5_pct is not null
            order by bias_5_pct, trade_date desc
            limit 1
        ),
        'algorithm_version', 'board_index_bias_v1'
    ) into payload
    from latest
    left join previous on true
    join core.board_index board on board.board_id = 'THS:883423';

    return payload;
end
$$;

revoke all on function api_v1.query_board_index_bias_latest()
    from public, anon, authenticated;
grant execute on function api_v1.query_board_index_bias_latest()
    to market_data_api;
