alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source','call_auction_indicative_detail',
        'call_auction_market_series'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source','call_auction_indicative_detail',
        'call_auction_market_series'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'stock_pool','auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','pytdx_pool_refresh','today_limit_up_snapshot',
        'call_auction_market_series'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table realtime.call_auction_market_series_session (
    session_id uuid primary key,
    workflow_run_id uuid not null unique
        references operations.workflow_run (workflow_run_id),
    trade_date date not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    cadence_seconds integer not null check (cadence_seconds = 20),
    expected_rounds integer not null check (expected_rounds = 32),
    universe_symbols text[] not null,
    universe_count integer not null check (universe_count > 0),
    universe_hash text not null check (universe_hash ~ '^[0-9a-f]{64}$'),
    status text not null check (status in ('running','succeeded','partial','failed')),
    started_at timestamptz not null,
    finished_at timestamptz,
    successful_rounds integer not null default 0 check (successful_rounds >= 0),
    partial_rounds integer not null default 0 check (partial_rounds >= 0),
    failed_rounds integer not null default 0 check (failed_rounds >= 0),
    successful_quotes bigint not null default 0 check (successful_quotes >= 0),
    failed_quotes bigint not null default 0 check (failed_quotes >= 0),
    error_summary varchar(500),
    check (cardinality(universe_symbols) = universe_count),
    check (successful_rounds + partial_rounds + failed_rounds <= expected_rounds),
    check (successful_quotes + failed_quotes <= universe_count::bigint * expected_rounds),
    check ((window_start at time zone 'Asia/Shanghai')::date = trade_date),
    check ((window_start at time zone 'Asia/Shanghai')::time = time '09:15:00'),
    check ((window_end at time zone 'Asia/Shanghai')::date = trade_date),
    check ((window_end at time zone 'Asia/Shanghai')::time = time '09:25:40'),
    check ((status = 'running' and finished_at is null)
        or (status <> 'running' and finished_at is not null))
);

create index call_auction_market_series_session_trade_date_idx
    on realtime.call_auction_market_series_session (trade_date, started_at desc);

create table realtime.call_auction_market_series_round (
    session_id uuid not null
        references realtime.call_auction_market_series_session (session_id),
    sample_seq integer not null check (sample_seq between 0 and 31),
    scheduled_at timestamptz not null,
    collected_at timestamptz,
    status text not null check (status in ('running','succeeded','partial','failed')),
    attempt_count integer not null check (attempt_count between 0 and 2),
    expected_quotes integer not null check (expected_quotes > 0),
    successful_quotes integer not null check (successful_quotes >= 0),
    failed_quotes integer not null check (failed_quotes >= 0),
    selected_ingestion_id uuid references ingestion.ingestion_run (ingestion_id),
    error_summary varchar(500),
    primary key (session_id, sample_seq),
    check ((status = 'running' and collected_at is null)
        or (status <> 'running' and collected_at is not null)),
    check ((status = 'running' and successful_quotes = 0 and failed_quotes = 0)
        or (status <> 'running' and successful_quotes + failed_quotes = expected_quotes))
);

create index call_auction_market_series_round_selected_ingestion_idx
    on realtime.call_auction_market_series_round (selected_ingestion_id)
    where selected_ingestion_id is not null;

create table realtime.call_auction_market_series_snapshot (
    trade_date date not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    session_id uuid not null,
    sample_seq integer not null,
    scheduled_at timestamptz not null,
    symbol text not null references core.security (symbol),
    observed_at timestamptz not null,
    last_price numeric(18, 4),
    previous_close numeric(18, 4),
    high_price numeric(18, 4),
    low_price numeric(18, 4),
    cumulative_volume bigint,
    cumulative_amount numeric(30, 4),
    source_code text not null check (source_code = 'pytdx_hq'),
    created_at timestamptz not null default now(),
    primary key (trade_date, ingestion_id, symbol),
    foreign key (session_id, sample_seq)
        references realtime.call_auction_market_series_round (session_id, sample_seq),
    check (scheduled_at = (
        (trade_date + time '09:15:00') at time zone 'Asia/Shanghai'
        + make_interval(secs => sample_seq * 20)
    )),
    check ((observed_at at time zone 'Asia/Shanghai')::date = trade_date),
    check (observed_at >= scheduled_at and observed_at < scheduled_at + interval '20 seconds'),
    check (
        (high_price is null or high_price >= 0)
        and (low_price is null or low_price >= 0)
        and (high_price is null or low_price is null or high_price >= low_price)
        and (last_price is null or low_price is null or last_price >= low_price)
        and (last_price is null or high_price is null or last_price <= high_price)
    ),
    check (
        (last_price is null or last_price >= 0)
        and (previous_close is null or previous_close >= 0)
        and (cumulative_volume is null or cumulative_volume >= 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    )
) partition by range (trade_date);

create index call_auction_market_series_snapshot_slot_symbol_idx
    on realtime.call_auction_market_series_snapshot (trade_date, sample_seq, symbol);
create index call_auction_market_series_snapshot_ingestion_symbol_idx
    on realtime.call_auction_market_series_snapshot (ingestion_id, symbol);

create table realtime.call_auction_market_series_snapshot_202608
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2026-08-01') to ('2026-09-01');
create table realtime.call_auction_market_series_snapshot_202609
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2026-09-01') to ('2026-10-01');
create table realtime.call_auction_market_series_snapshot_202610
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2026-10-01') to ('2026-11-01');
create table realtime.call_auction_market_series_snapshot_202611
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2026-11-01') to ('2026-12-01');
create table realtime.call_auction_market_series_snapshot_202612
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2026-12-01') to ('2027-01-01');
create table realtime.call_auction_market_series_snapshot_202701
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-01-01') to ('2027-02-01');
create table realtime.call_auction_market_series_snapshot_202702
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-02-01') to ('2027-03-01');
create table realtime.call_auction_market_series_snapshot_202703
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-03-01') to ('2027-04-01');
create table realtime.call_auction_market_series_snapshot_202704
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-04-01') to ('2027-05-01');
create table realtime.call_auction_market_series_snapshot_202705
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-05-01') to ('2027-06-01');
create table realtime.call_auction_market_series_snapshot_202706
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-06-01') to ('2027-07-01');
create table realtime.call_auction_market_series_snapshot_202707
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-07-01') to ('2027-08-01');
create table realtime.call_auction_market_series_snapshot_202708
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-08-01') to ('2027-09-01');
create table realtime.call_auction_market_series_snapshot_202709
    partition of realtime.call_auction_market_series_snapshot
    for values from ('2027-09-01') to ('2027-10-01');

alter table realtime.call_auction_market_series_session enable row level security;
alter table realtime.call_auction_market_series_round enable row level security;
alter table realtime.call_auction_market_series_snapshot enable row level security;
alter table realtime.call_auction_market_series_snapshot_202608 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202609 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202610 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202611 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202612 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202701 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202702 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202703 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202704 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202705 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202706 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202707 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202708 enable row level security;
alter table realtime.call_auction_market_series_snapshot_202709 enable row level security;

create policy call_auction_market_series_session_worker_select
    on realtime.call_auction_market_series_session
    for select to market_data_worker using (true);
create policy call_auction_market_series_session_worker_insert
    on realtime.call_auction_market_series_session
    for insert to market_data_worker with check (true);
create policy call_auction_market_series_session_worker_update
    on realtime.call_auction_market_series_session
    for update to market_data_worker using (true) with check (true);

create policy call_auction_market_series_round_worker_select
    on realtime.call_auction_market_series_round
    for select to market_data_worker using (true);
create policy call_auction_market_series_round_worker_insert
    on realtime.call_auction_market_series_round
    for insert to market_data_worker with check (true);
create policy call_auction_market_series_round_worker_update
    on realtime.call_auction_market_series_round
    for update to market_data_worker using (true) with check (true);

create policy call_auction_market_series_snapshot_worker_select
    on realtime.call_auction_market_series_snapshot
    for select to market_data_worker using (true);
create policy call_auction_market_series_snapshot_worker_insert
    on realtime.call_auction_market_series_snapshot
    for insert to market_data_worker with check (true);

grant select, insert, update on realtime.call_auction_market_series_session
    to market_data_worker;
grant select, insert, update on realtime.call_auction_market_series_round
    to market_data_worker;
grant select, insert on realtime.call_auction_market_series_snapshot
    to market_data_worker;
