alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','convertible_bond',
        'convertible_bond_daily_bar','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','today_limit_up_source',
        'call_auction_indicative_detail','call_auction_market_series'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','convertible_bond',
        'convertible_bond_daily_bar','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','today_limit_up_source',
        'call_auction_indicative_detail','call_auction_market_series'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','pytdx_pool_refresh','today_limit_up_snapshot',
        'call_auction_market_series','close_price_new_highs_120d','board_index_daily_bar'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table core.shareholder_count (
    symbol text not null references core.security (symbol),
    statistics_date date not null,
    announcement_date date not null,
    shareholder_count bigint not null,
    revision_key text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (symbol, statistics_date, revision_key),
    constraint shareholder_count_date_order check (statistics_date <= announcement_date),
    constraint shareholder_count_positive check (shareholder_count > 0),
    constraint shareholder_count_revision_key_check check (revision_key ~ '^[0-9a-f]{64}$'),
    constraint shareholder_count_source_check check (source_code = 'tushare')
);

create index shareholder_count_history_idx on core.shareholder_count (
    symbol, statistics_date, announcement_date desc, first_observed_at desc
);
create index shareholder_count_announcement_idx
    on core.shareholder_count (announcement_date);
create index shareholder_count_ingestion_idx
    on core.shareholder_count (ingestion_id);

alter table core.shareholder_count enable row level security;
create policy shareholder_count_worker_select on core.shareholder_count
    for select to market_data_worker using (true);
create policy shareholder_count_worker_insert on core.shareholder_count
    for insert to market_data_worker with check (true);
revoke all on core.shareholder_count from public;
grant select, insert on core.shareholder_count to market_data_worker;

create function api_v1.query_shareholder_counts_as_of(
    p_as_of_date date,
    p_symbols text[] default null,
    p_limit integer default 500
)
returns table (
    symbol text,
    statistics_date date,
    announcement_date date,
    shareholder_count bigint,
    previous_statistics_date date,
    previous_shareholder_count bigint,
    change_count bigint,
    change_ratio numeric
)
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
begin
    if p_as_of_date is null then
        raise exception 'p_as_of_date is required' using errcode = '22023';
    end if;
    if p_symbols is not null and (
        cardinality(array(
            select distinct input.symbol from unnest(p_symbols) as input(symbol)
        )) > 500
        or exists (
            select 1 from unnest(p_symbols) as input(symbol) where input.symbol is null
        )
    ) then
        raise exception 'p_symbols must contain at most 500 non-null symbols'
            using errcode = '22023';
    end if;

    return query
    with visible_revisions as (
        select
            fact.*,
            row_number() over (
                partition by fact.symbol, fact.statistics_date
                order by fact.announcement_date desc,
                         fact.first_observed_at desc,
                         fact.revision_key desc
            ) as revision_rank
        from core.shareholder_count fact
        where fact.announcement_date <= p_as_of_date
          and fact.first_observed_at < (
              (p_as_of_date + 1)::timestamp at time zone 'Asia/Shanghai'
          )
          and (p_symbols is null or fact.symbol = any(p_symbols))
    ), selected_revisions as (
        select visible.*
        from visible_revisions visible
        where visible.revision_rank = 1
    ), sequenced as (
        select
            selected.*,
            lag(selected.statistics_date) over (
                partition by selected.symbol order by selected.statistics_date
            ) as previous_statistics_date_value,
            lag(selected.shareholder_count) over (
                partition by selected.symbol order by selected.statistics_date
            ) as previous_shareholder_count_value
        from selected_revisions selected
    ), latest as (
        select
            sequenced.*,
            row_number() over (
                partition by sequenced.symbol
                order by sequenced.statistics_date desc,
                         sequenced.announcement_date desc,
                         sequenced.revision_key desc
            ) as latest_rank
        from sequenced
    )
    select
        latest.symbol,
        latest.statistics_date,
        latest.announcement_date,
        latest.shareholder_count,
        latest.previous_statistics_date_value,
        latest.previous_shareholder_count_value,
        latest.shareholder_count - latest.previous_shareholder_count_value,
        (latest.shareholder_count - latest.previous_shareholder_count_value)::numeric
            / nullif(latest.previous_shareholder_count_value, 0)
    from latest
    where latest.latest_rank = 1
    order by latest.symbol
    limit greatest(1, least(coalesce(p_limit, 500), 2000));
end
$$;

create function api_v1.query_shareholder_count_history_as_of(
    p_symbol text,
    p_as_of_date date,
    p_start_statistics_date date,
    p_end_statistics_date date,
    p_limit integer default 500
)
returns table (
    symbol text,
    statistics_date date,
    announcement_date date,
    shareholder_count bigint,
    previous_statistics_date date,
    previous_shareholder_count bigint,
    change_count bigint,
    change_ratio numeric
)
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
begin
    if p_symbol is null or btrim(p_symbol) = '' or p_as_of_date is null then
        raise exception 'p_symbol and p_as_of_date are required' using errcode = '22023';
    end if;
    if p_start_statistics_date is null
       or p_end_statistics_date is null
       or p_start_statistics_date > p_end_statistics_date then
        raise exception 'statistics date range is invalid' using errcode = '22023';
    end if;

    return query
    with visible_revisions as (
        select
            fact.*,
            row_number() over (
                partition by fact.symbol, fact.statistics_date
                order by fact.announcement_date desc,
                         fact.first_observed_at desc,
                         fact.revision_key desc
            ) as revision_rank
        from core.shareholder_count fact
        where fact.symbol = p_symbol
          and fact.statistics_date between p_start_statistics_date and p_end_statistics_date
          and fact.announcement_date <= p_as_of_date
          and fact.first_observed_at < (
              (p_as_of_date + 1)::timestamp at time zone 'Asia/Shanghai'
          )
    ), selected_revisions as (
        select visible.*
        from visible_revisions visible
        where visible.revision_rank = 1
    ), sequenced as (
        select
            selected.*,
            lag(selected.statistics_date) over (
                order by selected.statistics_date, selected.announcement_date
            ) as previous_statistics_date_value,
            lag(selected.shareholder_count) over (
                order by selected.statistics_date, selected.announcement_date
            ) as previous_shareholder_count_value
        from selected_revisions selected
    )
    select
        sequenced.symbol,
        sequenced.statistics_date,
        sequenced.announcement_date,
        sequenced.shareholder_count,
        sequenced.previous_statistics_date_value,
        sequenced.previous_shareholder_count_value,
        sequenced.shareholder_count - sequenced.previous_shareholder_count_value,
        (sequenced.shareholder_count - sequenced.previous_shareholder_count_value)::numeric
            / nullif(sequenced.previous_shareholder_count_value, 0)
    from sequenced
    order by sequenced.statistics_date, sequenced.announcement_date
    limit greatest(1, least(coalesce(p_limit, 500), 2000));
end
$$;

create function api_v1.query_shareholder_count_history_latest(
    p_symbol text,
    p_start_statistics_date date,
    p_end_statistics_date date,
    p_limit integer default 500
)
returns table (
    symbol text,
    statistics_date date,
    announcement_date date,
    shareholder_count bigint,
    previous_statistics_date date,
    previous_shareholder_count bigint,
    change_count bigint,
    change_ratio numeric
)
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
begin
    if p_symbol is null or btrim(p_symbol) = '' then
        raise exception 'p_symbol is required' using errcode = '22023';
    end if;
    if p_start_statistics_date is null
       or p_end_statistics_date is null
       or p_start_statistics_date > p_end_statistics_date then
        raise exception 'statistics date range is invalid' using errcode = '22023';
    end if;

    return query
    with current_revisions as (
        select
            fact.*,
            row_number() over (
                partition by fact.symbol, fact.statistics_date
                order by fact.announcement_date desc,
                         fact.first_observed_at desc,
                         fact.revision_key desc
            ) as revision_rank
        from core.shareholder_count fact
        where fact.symbol = p_symbol
          and fact.statistics_date between p_start_statistics_date and p_end_statistics_date
    ), selected_revisions as (
        select current_value.*
        from current_revisions current_value
        where current_value.revision_rank = 1
    ), sequenced as (
        select
            selected.*,
            lag(selected.statistics_date) over (
                order by selected.statistics_date, selected.announcement_date
            ) as previous_statistics_date_value,
            lag(selected.shareholder_count) over (
                order by selected.statistics_date, selected.announcement_date
            ) as previous_shareholder_count_value
        from selected_revisions selected
    )
    select
        sequenced.symbol,
        sequenced.statistics_date,
        sequenced.announcement_date,
        sequenced.shareholder_count,
        sequenced.previous_statistics_date_value,
        sequenced.previous_shareholder_count_value,
        sequenced.shareholder_count - sequenced.previous_shareholder_count_value,
        (sequenced.shareholder_count - sequenced.previous_shareholder_count_value)::numeric
            / nullif(sequenced.previous_shareholder_count_value, 0)
    from sequenced
    order by sequenced.statistics_date, sequenced.announcement_date
    limit greatest(1, least(coalesce(p_limit, 500), 2000));
end
$$;

comment on function api_v1.query_shareholder_counts_as_of(date, text[], integer)
is 'Returns each requested stock latest shareholder count strictly visible at the knowledge date.';
comment on function api_v1.query_shareholder_count_history_as_of(text, date, date, date, integer)
is 'Returns one stock shareholder-count history strictly visible at the knowledge date.';
comment on function api_v1.query_shareholder_count_history_latest(text, date, date, integer)
is 'Returns current-known shareholder-count history; this is not a strict no-future-data view.';

revoke all on function api_v1.query_shareholder_counts_as_of(date, text[], integer) from public;
revoke all on function api_v1.query_shareholder_count_history_as_of(
    text, date, date, date, integer
) from public;
revoke all on function api_v1.query_shareholder_count_history_latest(
    text, date, date, integer
) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant execute on function api_v1.query_shareholder_counts_as_of(date, text[], integer)
            to anon;
        grant execute on function api_v1.query_shareholder_count_history_as_of(
            text, date, date, date, integer
        ) to anon;
        grant execute on function api_v1.query_shareholder_count_history_latest(
            text, date, date, integer
        ) to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant execute on function api_v1.query_shareholder_counts_as_of(date, text[], integer)
            to authenticated;
        grant execute on function api_v1.query_shareholder_count_history_as_of(
            text, date, date, date, integer
        ) to authenticated;
        grant execute on function api_v1.query_shareholder_count_history_latest(
            text, date, date, integer
        ) to authenticated;
    end if;
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_shareholder_counts_as_of(date, text[], integer)
            to market_data_api;
        grant execute on function api_v1.query_shareholder_count_history_as_of(
            text, date, date, date, integer
        ) to market_data_api;
        grant execute on function api_v1.query_shareholder_count_history_latest(
            text, date, date, integer
        ) to market_data_api;
    end if;
end
$$;
