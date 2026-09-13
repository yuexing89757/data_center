create table realtime.call_auction_market_series_snapshot_history (
    like realtime.call_auction_market_series_snapshot
        including defaults including constraints including storage
) partition by range (trade_date);

alter table realtime.call_auction_market_series_snapshot_history
    add primary key (trade_date, ingestion_id, symbol),
    add foreign key (ingestion_id)
        references ingestion.ingestion_run (ingestion_id),
    add foreign key (symbol)
        references core.security (symbol),
    add foreign key (session_id, sample_seq)
        references realtime.call_auction_market_series_round (session_id, sample_seq);

create index call_auction_market_series_snapshot_history_date_symbol_seq_idx
    on realtime.call_auction_market_series_snapshot_history
        (trade_date, symbol, sample_seq);

create table realtime.call_auction_market_series_snapshot_history_202603
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-03-01') to ('2026-04-01');
create table realtime.call_auction_market_series_snapshot_history_202604
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-04-01') to ('2026-05-01');
create table realtime.call_auction_market_series_snapshot_history_202605
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-05-01') to ('2026-06-01');
create table realtime.call_auction_market_series_snapshot_history_202606
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-06-01') to ('2026-07-01');
create table realtime.call_auction_market_series_snapshot_history_202607
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-07-01') to ('2026-08-01');
create table realtime.call_auction_market_series_snapshot_history_202608
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-08-01') to ('2026-09-01');
create table realtime.call_auction_market_series_snapshot_history_202609
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-09-01') to ('2026-10-01');
create table realtime.call_auction_market_series_snapshot_history_202610
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-10-01') to ('2026-11-01');
create table realtime.call_auction_market_series_snapshot_history_202611
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-11-01') to ('2026-12-01');
create table realtime.call_auction_market_series_snapshot_history_202612
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2026-12-01') to ('2027-01-01');
create table realtime.call_auction_market_series_snapshot_history_202701
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-01-01') to ('2027-02-01');
create table realtime.call_auction_market_series_snapshot_history_202702
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-02-01') to ('2027-03-01');
create table realtime.call_auction_market_series_snapshot_history_202703
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-03-01') to ('2027-04-01');
create table realtime.call_auction_market_series_snapshot_history_202704
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-04-01') to ('2027-05-01');
create table realtime.call_auction_market_series_snapshot_history_202705
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-05-01') to ('2027-06-01');
create table realtime.call_auction_market_series_snapshot_history_202706
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-06-01') to ('2027-07-01');
create table realtime.call_auction_market_series_snapshot_history_202707
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-07-01') to ('2027-08-01');
create table realtime.call_auction_market_series_snapshot_history_202708
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-08-01') to ('2027-09-01');
create table realtime.call_auction_market_series_snapshot_history_202709
    partition of realtime.call_auction_market_series_snapshot_history
    for values from ('2027-09-01') to ('2027-10-01');

alter table realtime.call_auction_market_series_snapshot_history enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202603 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202604 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202605 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202606 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202607 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202608 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202609 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202610 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202611 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202612 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202701 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202702 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202703 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202704 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202705 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202706 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202707 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202708 enable row level security;
alter table realtime.call_auction_market_series_snapshot_history_202709 enable row level security;

create policy call_auction_market_series_snapshot_history_worker_select
    on realtime.call_auction_market_series_snapshot_history
    for select to market_data_worker using (true);
create policy call_auction_market_series_snapshot_history_worker_insert
    on realtime.call_auction_market_series_snapshot_history
    for insert to market_data_worker with check (true);
create policy call_auction_market_series_snapshot_history_worker_delete
    on realtime.call_auction_market_series_snapshot_history
    for delete to market_data_worker using (true);

grant select, insert, delete
    on realtime.call_auction_market_series_snapshot_history
    to market_data_worker;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','security_bse_daily','dragon_tiger_daily',
        'regulation_daily_calculation','data_cleanup','call_auction_market_series_archive'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;
