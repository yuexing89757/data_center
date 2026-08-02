alter table ingestion.ingestion_run drop constraint ingestion_run_provider_check;
alter table ingestion.ingestion_run add constraint ingestion_run_provider_check check (
    provider_code in ('baostock','akshare','pytdx','akshare_ths','tushare','pytdx_hq')
);
alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote'
    )
);
alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote'
    )
);

create schema if not exists realtime;
revoke all on schema realtime from public;
grant usage on schema realtime to market_data_worker;

create table realtime.auction_collection_session (
    session_id uuid primary key,
    pool_snapshot_id uuid not null references stock_pool.snapshot (snapshot_id),
    pool_snapshot_version integer not null check (pool_snapshot_version > 0),
    basis_trade_date date not null,
    effective_trade_date date not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    cadence_seconds integer not null check (cadence_seconds between 1 and 60),
    expected_rounds integer not null check (expected_rounds > 0),
    expected_quotes bigint not null check (expected_quotes > 0),
    provider_code text not null check (provider_code = 'pytdx_hq'),
    status text not null check (status in ('running','succeeded','partial','failed')),
    started_at timestamptz not null,
    finished_at timestamptz,
    successful_rounds integer not null default 0 check (successful_rounds >= 0),
    partial_rounds integer not null default 0 check (partial_rounds >= 0),
    failed_rounds integer not null default 0 check (failed_rounds >= 0),
    successful_quotes bigint not null default 0 check (successful_quotes >= 0),
    failed_quotes bigint not null default 0 check (failed_quotes >= 0),
    error_summary text check (char_length(error_summary) <= 200),
    created_at timestamptz not null default now(),
    unique (pool_snapshot_id, effective_trade_date, cadence_seconds, provider_code),
    constraint auction_session_date_check check (effective_trade_date > basis_trade_date),
    constraint auction_session_window_check check (window_end > window_start),
    constraint auction_session_terminal_check check (
        (status = 'running' and finished_at is null)
        or (status <> 'running' and finished_at is not null)
    )
);

create table realtime.auction_collection_round (
    session_id uuid not null references realtime.auction_collection_session (session_id),
    sample_seq integer not null check (sample_seq >= 0),
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    scheduled_at timestamptz not null,
    collected_at timestamptz not null,
    phase text not null check (phase in ('cancellable','non_cancellable','final_match')),
    status text not null check (status in ('succeeded','partial','failed')),
    expected_quotes integer not null check (expected_quotes > 0),
    successful_quotes integer not null check (successful_quotes >= 0),
    failed_quotes integer not null check (failed_quotes >= 0),
    latency_ms integer not null check (latency_ms >= 0),
    error_summary text check (char_length(error_summary) <= 200),
    primary key (session_id, sample_seq),
    unique (ingestion_id),
    constraint auction_round_count_check check (
        successful_quotes + failed_quotes = expected_quotes
    )
);

create table realtime.five_level_quote_snapshot (
    session_id uuid not null references realtime.auction_collection_session (session_id),
    pool_snapshot_id uuid not null references stock_pool.snapshot (snapshot_id),
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    symbol text not null references core.security (symbol),
    sample_seq integer not null check (sample_seq >= 0),
    scheduled_at timestamptz not null,
    collected_at timestamptz not null,
    source_timestamp timestamptz,
    phase text not null check (phase in ('cancellable','non_cancellable','final_match')),
    quote_semantics text not null check (
        quote_semantics in ('auction_indicative','verified_order_book')
    ),
    quote_status text not null check (quote_status in ('trading','suspended','closed','unknown')),
    last_price numeric(18,4), previous_close numeric(18,4),
    open numeric(18,4), high numeric(18,4), low numeric(18,4),
    cumulative_volume bigint, cumulative_amount numeric(30,4),
    bid1_price numeric(18,4), bid1_volume bigint, ask1_price numeric(18,4), ask1_volume bigint,
    bid2_price numeric(18,4), bid2_volume bigint, ask2_price numeric(18,4), ask2_volume bigint,
    bid3_price numeric(18,4), bid3_volume bigint, ask3_price numeric(18,4), ask3_volume bigint,
    bid4_price numeric(18,4), bid4_volume bigint, ask4_price numeric(18,4), ask4_volume bigint,
    bid5_price numeric(18,4), bid5_volume bigint, ask5_price numeric(18,4), ask5_volume bigint,
    source_code text not null check (source_code = 'pytdx_hq'),
    created_at timestamptz not null default now(),
    primary key (session_id, symbol, sample_seq),
    constraint auction_quote_time_check check (collected_at >= scheduled_at - interval '5 seconds'),
    constraint auction_quote_nonnegative_check check (
        (last_price is null or last_price >= 0)
        and (previous_close is null or previous_close >= 0)
        and (cumulative_volume is null or cumulative_volume >= 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    )
);

create table derived.auction_quote_metric (
    session_id uuid not null,
    symbol text not null,
    sample_seq integer not null,
    spread numeric(18,4), mid_price numeric(18,4),
    bid_depth_5 bigint, ask_depth_5 bigint, imbalance_5 numeric(24,12),
    seal_amount numeric(30,4),
    calculated_at timestamptz not null,
    algorithm_version text not null,
    price_limit_rule_version text,
    primary key (session_id, symbol, sample_seq),
    foreign key (session_id, symbol, sample_seq)
        references realtime.five_level_quote_snapshot (session_id, symbol, sample_seq),
    constraint auction_metric_nonnegative_check check (
        (bid_depth_5 is null or bid_depth_5 >= 0)
        and (ask_depth_5 is null or ask_depth_5 >= 0)
        and (seal_amount is null or seal_amount >= 0)
    )
);

create index auction_session_trade_date_idx
    on realtime.auction_collection_session (effective_trade_date, started_at desc);
create index auction_round_schedule_idx
    on realtime.auction_collection_round (session_id, scheduled_at);
create index auction_quote_symbol_time_idx
    on realtime.five_level_quote_snapshot (symbol, collected_at desc);
create index auction_quote_session_time_idx
    on realtime.five_level_quote_snapshot (session_id, collected_at, symbol);

alter table realtime.auction_collection_session enable row level security;
alter table realtime.auction_collection_round enable row level security;
alter table realtime.five_level_quote_snapshot enable row level security;
alter table derived.auction_quote_metric enable row level security;
create policy auction_session_worker_all on realtime.auction_collection_session
    for all to market_data_worker using (true) with check (true);
create policy auction_round_worker_all on realtime.auction_collection_round
    for all to market_data_worker using (true) with check (true);
create policy auction_quote_worker_all on realtime.five_level_quote_snapshot
    for all to market_data_worker using (true) with check (true);
create policy auction_metric_worker_all on derived.auction_quote_metric
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on realtime.auction_collection_session to market_data_worker;
grant select, insert on realtime.auction_collection_round,
    realtime.five_level_quote_snapshot, derived.auction_quote_metric to market_data_worker;

create or replace function api_v1.query_auction_quotes(
    p_trade_date date default null,
    p_session_id uuid default null,
    p_symbol text default null,
    p_start timestamptz default null,
    p_end timestamptz default null,
    p_limit integer default 1000
)
returns table (
    session_id uuid, pool_snapshot_id uuid, trade_date date, symbol text,
    sample_seq integer, scheduled_at timestamptz, collected_at timestamptz,
    source_timestamp timestamptz, phase text, quote_semantics text, quote_status text,
    last_price numeric, previous_close numeric, cumulative_volume bigint,
    cumulative_amount numeric, bid_levels jsonb, ask_levels jsonb,
    spread numeric, mid_price numeric, bid_depth_5 bigint, ask_depth_5 bigint,
    imbalance_5 numeric, seal_amount numeric, metric_algorithm_version text
)
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, realtime, derived
set statement_timeout = '5s'
as $$
begin
    if (p_trade_date is null and p_session_id is null)
       or p_limit < 1 or p_limit > 5000
       or (p_symbol is not null and p_symbol !~ '^(SSE|SZSE):[0-9]{6}$')
       or (p_start is not null and p_end is not null and (
           p_end < p_start or p_end - p_start > interval '1 day'
       )) then
        raise exception 'invalid auction quote query boundary' using errcode = '22023';
    end if;
    return query
    select quote.session_id, quote.pool_snapshot_id, session.effective_trade_date,
           quote.symbol, quote.sample_seq, quote.scheduled_at, quote.collected_at,
           quote.source_timestamp, quote.phase, quote.quote_semantics, quote.quote_status,
           quote.last_price, quote.previous_close, quote.cumulative_volume,
           quote.cumulative_amount,
           jsonb_build_array(
             jsonb_build_object('level',1,'price',quote.bid1_price,'volume',quote.bid1_volume),
             jsonb_build_object('level',2,'price',quote.bid2_price,'volume',quote.bid2_volume),
             jsonb_build_object('level',3,'price',quote.bid3_price,'volume',quote.bid3_volume),
             jsonb_build_object('level',4,'price',quote.bid4_price,'volume',quote.bid4_volume),
             jsonb_build_object('level',5,'price',quote.bid5_price,'volume',quote.bid5_volume)
           ),
           jsonb_build_array(
             jsonb_build_object('level',1,'price',quote.ask1_price,'volume',quote.ask1_volume),
             jsonb_build_object('level',2,'price',quote.ask2_price,'volume',quote.ask2_volume),
             jsonb_build_object('level',3,'price',quote.ask3_price,'volume',quote.ask3_volume),
             jsonb_build_object('level',4,'price',quote.ask4_price,'volume',quote.ask4_volume),
             jsonb_build_object('level',5,'price',quote.ask5_price,'volume',quote.ask5_volume)
           ),
           metric.spread, metric.mid_price, metric.bid_depth_5, metric.ask_depth_5,
           metric.imbalance_5, metric.seal_amount, metric.algorithm_version
    from realtime.five_level_quote_snapshot quote
    join realtime.auction_collection_session session using (session_id)
    left join derived.auction_quote_metric metric
      using (session_id, symbol, sample_seq)
    where (p_trade_date is null or session.effective_trade_date = p_trade_date)
      and (p_session_id is null or quote.session_id = p_session_id)
      and (p_symbol is null or quote.symbol = p_symbol)
      and (p_start is null or quote.collected_at >= p_start)
      and (p_end is null or quote.collected_at <= p_end)
    order by quote.collected_at, quote.symbol
    limit p_limit;
end
$$;

revoke all on function api_v1.query_auction_quotes(
    date, uuid, text, timestamptz, timestamptz, integer
) from public;
grant execute on function api_v1.query_auction_quotes(
    date, uuid, text, timestamptz, timestamptz, integer
) to authenticated;
