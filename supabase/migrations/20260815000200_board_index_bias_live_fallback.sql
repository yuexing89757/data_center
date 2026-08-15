create or replace function api_v1.query_board_index_bias_latest()
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
declare
    payload jsonb;
    ready_count integer;
    latest_bar_date date;
    expected_trade_date date;
begin
    select count(*)::integer, max(recent.trade_date)
    into ready_count, latest_bar_date
    from (
        select bar.trade_date
        from core.board_index_daily_bar bar
        where bar.board_id = 'THS:883423'
        order by bar.trade_date desc
        limit 34
    ) recent;

    select max(calendar.trade_date)
    into expected_trade_date
    from core.trading_calendar calendar
    where calendar.market = 'CN_A_SHARE'
      and calendar.is_trading_day
      and calendar.trade_date <= (now() at time zone 'Asia/Shanghai')::date;

    if ready_count < 34
       or expected_trade_date is null
       or latest_bar_date < expected_trade_date then
        raise exception 'board index daily bars are missing, insufficient, or stale'
            using errcode = 'P0002';
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
                order by trade_date rows between 4 preceding and current row
            ) as positive_close_count,
            avg(close) filter (where close > 0) over (
                order by trade_date rows between 4 preceding and current row
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
        'moving_average_5', case when latest.bias_5_pct is not null
            then round(latest.moving_average_5, 6)::text else null end,
        'bias_5_pct', case when latest.bias_5_pct is not null
            then round(latest.bias_5_pct, 6)::text else null end,
        'previous_trade_date', previous.trade_date,
        'previous_bias_5_pct', case when previous.bias_5_pct is not null
            then round(previous.bias_5_pct, 6)::text else null end,
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
            select round(bias_5_pct, 6)::text from observation_window
            where bias_5_pct is not null
            order by bias_5_pct desc, trade_date desc limit 1
        ),
        'highest_bias_trade_date', (
            select trade_date from observation_window where bias_5_pct is not null
            order by bias_5_pct desc, trade_date desc limit 1
        ),
        'lowest_bias_5_pct', (
            select round(bias_5_pct, 6)::text from observation_window
            where bias_5_pct is not null
            order by bias_5_pct, trade_date desc limit 1
        ),
        'lowest_bias_trade_date', (
            select trade_date from observation_window where bias_5_pct is not null
            order by bias_5_pct, trade_date desc limit 1
        ),
        'algorithm_version', 'board_index_bias_v1',
        'data_origin', 'database',
        'persistence_status', 'persisted',
        'fetched_at', now()
    ) into payload
    from latest
    left join previous on true
    join core.board_index board on board.board_id = 'THS:883423';

    return payload;
end
$$;

create index ingestion_run_board_live_input_hash_idx
on ingestion.ingestion_run ((request_params->>'input_hash'))
where provider_code = 'akshare_ths'
  and dataset_code = 'board_index_daily_bar'
  and request_params->>'mode' = 'fastapi_live_board_index';

create function api_v1.persist_board_index_daily_bars_live(
    p_ingestion_id uuid,
    p_raw_id uuid,
    p_fetched_at timestamptz,
    p_input_hash text,
    p_object_path text,
    p_content_sha256 text,
    p_byte_size bigint,
    p_source_row_count integer,
    p_source_years jsonb,
    p_records jsonb
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, api_v1, ingestion, audit, core
set statement_timeout = '5s'
as $$
declare
    existing_ingestion_id uuid;
    existing_raw_id uuid;
    accepted_count integer;
    distinct_date_count integer;
    invalid_count integer;
    fetched_year integer := extract(year from p_fetched_at at time zone 'Asia/Shanghai');
    fetched_month integer := extract(month from p_fetched_at at time zone 'Asia/Shanghai');
    fetched_day integer := extract(day from p_fetched_at at time zone 'Asia/Shanghai');
begin
    if p_input_hash !~ '^[0-9a-f]{64}$'
       or p_content_sha256 !~ '^[0-9a-f]{64}$'
       or p_byte_size < 1 or p_byte_size > 5000000
       or p_source_row_count < 1 or p_source_row_count > 2
       or jsonb_typeof(p_source_years) <> 'array'
       or jsonb_array_length(p_source_years) <> p_source_row_count
       or jsonb_typeof(p_records) <> 'array'
       or jsonb_array_length(p_records) < 34
       or jsonb_array_length(p_records) > 600
       or (p_fetched_at at time zone 'Asia/Shanghai')::date
            <> (now() at time zone 'Asia/Shanghai')::date
       or p_object_path !~ '^akshare_ths/board_index_daily_bar/year=[0-9]{4}/month=[0-9]{2}/day=[0-9]{2}/[0-9a-f-]{36}[.]jsonl$'
       or not starts_with(
            p_object_path,
            format(
                'akshare_ths/board_index_daily_bar/year=%s/month=%s/day=%s/',
                fetched_year,
                lpad(fetched_month::text, 2, '0'),
                lpad(fetched_day::text, 2, '0')
            )
       ) then
        raise exception 'invalid live board-index persistence boundary'
            using errcode = '22023';
    end if;

    if (select count(distinct value::integer) from jsonb_array_elements_text(p_source_years))
            <> p_source_row_count
       or exists (
            select 1 from jsonb_array_elements_text(p_source_years) year_value
            where year_value.value::integer not in (fetched_year, fetched_year - 1)
       ) then
        raise exception 'invalid live board-index source years' using errcode = '22023';
    end if;

    if not exists (
        select 1 from core.board_index board where board.board_id = 'THS:883423'
    ) then
        raise exception 'unknown board index' using errcode = '22023';
    end if;

    select
        count(*)::integer,
        count(distinct item.trade_date)::integer,
        count(*) filter (
            where item.board_id <> 'THS:883423'
               or item.market <> 'CN_A_SHARE'
               or item.source_code <> 'akshare_ths'
               or item.trade_date > (p_fetched_at at time zone 'Asia/Shanghai')::date
               or item."open" <= 0 or item.high <= 0 or item.low <= 0 or item.close <= 0
               or item.low > item.high
               or item."open" not between item.low and item.high
               or item.close not between item.low and item.high
               or item.volume < 0 or item.amount < 0
               or not exists (
                    select 1 from core.trading_calendar calendar
                    where calendar.market = 'CN_A_SHARE'
                      and calendar.trade_date = item.trade_date
                      and calendar.is_trading_day
               )
               or not exists (
                    select 1 from jsonb_array_elements_text(p_source_years) source_year
                    where source_year.value::integer = extract(year from item.trade_date)
               )
        )::integer
    into accepted_count, distinct_date_count, invalid_count
    from jsonb_to_recordset(p_records) as item(
        board_id text,
        trade_date date,
        market text,
        "open" numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        amount numeric,
        source_code text
    );

    if accepted_count <> jsonb_array_length(p_records)
       or distinct_date_count <> accepted_count
       or invalid_count <> 0 then
        raise exception 'invalid live board-index record' using errcode = '22023';
    end if;

    perform pg_advisory_xact_lock(
        hashtextextended('board_index_daily_bar:' || p_input_hash, 0)
    );

    select run.ingestion_id, manifest.raw_id
    into existing_ingestion_id, existing_raw_id
    from ingestion.ingestion_run run
    join ingestion.raw_manifest manifest on manifest.ingestion_id = run.ingestion_id
    where run.provider_code = 'akshare_ths'
      and run.dataset_code = 'board_index_daily_bar'
      and run.status = 'succeeded'
      and run.request_params->>'mode' = 'fastapi_live_board_index'
      and run.request_params->>'input_hash' = p_input_hash
    order by run.finished_at desc
    limit 1;

    if found then
        return jsonb_build_object(
            'outcome', 'reused',
            'ingestion_id', existing_ingestion_id,
            'raw_id', existing_raw_id,
            'input_hash', p_input_hash
        );
    end if;

    insert into ingestion.ingestion_run (
        ingestion_id, provider_code, dataset_code, status,
        requested_at, started_at, finished_at, request_params,
        fetched_rows, accepted_rows, rejected_rows
    ) values (
        p_ingestion_id, 'akshare_ths', 'board_index_daily_bar', 'succeeded',
        p_fetched_at, p_fetched_at, now(),
        jsonb_build_object(
            'board_id', 'THS:883423',
            'mode', 'fastapi_live_board_index',
            'input_hash', p_input_hash,
            'source_years', p_source_years
        ),
        accepted_count, accepted_count, 0
    );

    insert into ingestion.raw_manifest (
        raw_id, ingestion_id, storage_backend, object_path, file_format,
        content_sha256, byte_size, row_count, schema_version
    ) values (
        p_raw_id, p_ingestion_id, 'local', p_object_path, 'jsonl',
        p_content_sha256, p_byte_size, p_source_row_count,
        'akshare_ths.board_index_daily_bar.live.v1'
    );

    insert into core.board_index_daily_bar (
        board_id, trade_date, market, "open", high, low, close,
        volume, amount, source_code, ingestion_id
    )
    select
        item.board_id, item.trade_date, item.market, item."open", item.high,
        item.low, item.close, item.volume, item.amount, item.source_code,
        p_ingestion_id
    from jsonb_to_recordset(p_records) as item(
        board_id text,
        trade_date date,
        market text,
        "open" numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        amount numeric,
        source_code text
    )
    on conflict (board_id, trade_date) do update set
        market = excluded.market,
        "open" = excluded."open",
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume,
        amount = excluded.amount,
        source_code = excluded.source_code,
        ingestion_id = excluded.ingestion_id,
        updated_at = now();

    insert into audit.quality_result (
        quality_result_id, ingestion_id, dataset_code, rule_code,
        severity, status, message, details
    ) values (
        extensions.gen_random_uuid(), p_ingestion_id, 'board_index_daily_bar',
        'board_index_daily_bar.live_response_boundary', 'info', 'passed',
        'bounded THS live board-index history persisted',
        jsonb_build_object(
            'source_payloads', p_source_row_count,
            'accepted_rows', accepted_count,
            'input_hash', p_input_hash
        )
    );

    return jsonb_build_object(
        'outcome', 'created',
        'ingestion_id', p_ingestion_id,
        'raw_id', p_raw_id,
        'input_hash', p_input_hash
    );
exception
    when invalid_text_representation or numeric_value_out_of_range
         or datetime_field_overflow then
        raise exception 'invalid live board-index typed value' using errcode = '22023';
end
$$;

revoke all on function api_v1.query_board_index_bias_latest()
    from public,anon,authenticated;
grant execute on function api_v1.query_board_index_bias_latest()
    to market_data_api;

revoke all on function api_v1.persist_board_index_daily_bars_live(
    uuid,uuid,timestamptz,text,text,text,bigint,integer,jsonb,jsonb)
    from public,anon,authenticated;
grant execute on function api_v1.persist_board_index_daily_bars_live(
    uuid,uuid,timestamptz,text,text,text,bigint,integer,jsonb,jsonb)
    to market_data_api;
