create schema if not exists today_limit_up;
revoke all on schema today_limit_up from public;
grant usage on schema today_limit_up to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source'
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
        'today_limit_up_source'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'stock_pool','auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','pytdx_pool_refresh','today_limit_up_snapshot'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table today_limit_up.source_observation (
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    source_code text not null check (source_code = 'akshare'),
    observed_at timestamptz not null,
    source_name text,
    first_limit_up_at timestamptz,
    last_limit_up_at timestamptz,
    open_count integer,
    source_reported_sealed_funds_cny numeric(30, 4),
    created_at timestamptz not null default now(),
    primary key (ingestion_id, symbol),
    constraint today_limit_up_observation_nonnegative check (
        (open_count is null or open_count >= 0)
        and (source_reported_sealed_funds_cny is null
             or source_reported_sealed_funds_cny >= 0)
    ),
    constraint today_limit_up_observation_times check (
        (first_limit_up_at is null or
         (first_limit_up_at at time zone 'Asia/Shanghai')::date = trade_date)
        and (last_limit_up_at is null or
             (last_limit_up_at at time zone 'Asia/Shanghai')::date = trade_date)
        and (first_limit_up_at is null or last_limit_up_at is null
             or first_limit_up_at <= last_limit_up_at)
    )
);

create table today_limit_up.snapshot (
    snapshot_id uuid primary key,
    calculation_id uuid references derived.calculation_run (calculation_id),
    trade_date date not null,
    version integer not null,
    status text not null,
    member_count integer not null,
    candidate_count integer not null,
    rejected_count integer not null,
    content_hash text not null,
    input_hash text not null,
    rule_version text not null,
    algorithm_version text not null,
    source_ingestion_id uuid references ingestion.ingestion_run (ingestion_id),
    generated_at timestamptz not null,
    unique (trade_date, version),
    unique (trade_date, input_hash),
    constraint today_limit_up_snapshot_status_check
        check (status in ('ready','partial','deferred','failed')),
    constraint today_limit_up_snapshot_count_check check (
        version > 0 and member_count >= 0 and candidate_count >= 0
        and rejected_count >= 0 and member_count + rejected_count <= candidate_count
    ),
    constraint today_limit_up_snapshot_hash_check check (
        content_hash ~ '^[0-9a-f]{64}$' and input_hash ~ '^[0-9a-f]{64}$'
    ),
    constraint today_limit_up_snapshot_deferred_check check (
        (status = 'deferred' and calculation_id is null and member_count = 0)
        or (status <> 'deferred' and calculation_id is not null)
    )
);

create table today_limit_up.member (
    snapshot_id uuid not null references today_limit_up.snapshot (snapshot_id),
    symbol text not null references core.security (symbol),
    code text not null,
    historical_name text not null,
    previous_close numeric(18, 4) not null,
    close numeric(18, 4) not null,
    limit_price numeric(18, 4) not null,
    change_percent numeric(24, 10) not null,
    free_float_shares bigint not null,
    free_float_market_cap_cny numeric(30, 4) not null,
    first_limit_up_at timestamptz,
    last_limit_up_at timestamptz,
    open_count integer,
    limit_up_duration_seconds integer,
    duration_semantics text not null default 'unavailable_without_event_stream',
    source_reported_sealed_funds_cny numeric(30, 4),
    closing_bid1_price numeric(18, 4), closing_bid1_volume_shares bigint,
    closing_bid2_price numeric(18, 4), closing_bid2_volume_shares bigint,
    closing_bid3_price numeric(18, 4), closing_bid3_volume_shares bigint,
    closing_bid4_price numeric(18, 4), closing_bid4_volume_shares bigint,
    closing_bid5_price numeric(18, 4), closing_bid5_volume_shares bigint,
    closing_bid1_sealing_amount_cny numeric(30, 4),
    daily_bar_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    indicator_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    name_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    pool_calculation_id uuid not null references derived.calculation_run (calculation_id),
    source_observation_ingestion_id uuid references ingestion.ingestion_run (ingestion_id),
    source_observation_raw_id uuid references ingestion.raw_manifest (raw_id),
    order_book_ingestion_id uuid references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (snapshot_id, symbol),
    constraint today_limit_up_member_code_check check (code ~ '^[0-9]{6}$'),
    constraint today_limit_up_member_fact_check check (
        btrim(historical_name) <> '' and previous_close > 0 and close > 0
        and close = limit_price and free_float_shares > 0
        and free_float_market_cap_cny = close * free_float_shares
        and change_percent = round((close / previous_close - 1) * 100, 10)
    ),
    constraint today_limit_up_member_optional_nonnegative check (
        (open_count is null or open_count >= 0)
        and (limit_up_duration_seconds is null or limit_up_duration_seconds >= 0)
        and (source_reported_sealed_funds_cny is null or source_reported_sealed_funds_cny >= 0)
    ),
    constraint today_limit_up_member_duration_check check (
        (limit_up_duration_seconds is null
         and duration_semantics = 'unavailable_without_event_stream')
        or (limit_up_duration_seconds is not null
            and duration_semantics = 'source_reported_cumulative')
    ),
    constraint today_limit_up_member_bid1_pair_check check (
        (closing_bid1_price is null) = (closing_bid1_volume_shares is null)
        and (closing_bid1_sealing_amount_cny is null or (
            closing_bid1_price = limit_price
            and closing_bid1_sealing_amount_cny = closing_bid1_price * closing_bid1_volume_shares
        ))
    ),
    constraint today_limit_up_member_order_book_pairs_check check (
        (closing_bid2_price is null) = (closing_bid2_volume_shares is null)
        and (closing_bid3_price is null) = (closing_bid3_volume_shares is null)
        and (closing_bid4_price is null) = (closing_bid4_volume_shares is null)
        and (closing_bid5_price is null) = (closing_bid5_volume_shares is null)
        and (closing_bid1_volume_shares is null or closing_bid1_volume_shares >= 0)
        and (closing_bid2_volume_shares is null or closing_bid2_volume_shares >= 0)
        and (closing_bid3_volume_shares is null or closing_bid3_volume_shares >= 0)
        and (closing_bid4_volume_shares is null or closing_bid4_volume_shares >= 0)
        and (closing_bid5_volume_shares is null or closing_bid5_volume_shares >= 0)
    ),
    constraint today_limit_up_member_source_lineage_check check (
        (source_observation_ingestion_id is null) = (source_observation_raw_id is null)
    )
);

create table today_limit_up.calculation_quality (
    snapshot_id uuid not null references today_limit_up.snapshot (snapshot_id),
    rule_code text not null,
    severity text not null check (severity in ('warning','error')),
    symbol text not null default '',
    message text not null,
    created_at timestamptz not null default now(),
    primary key (snapshot_id, rule_code, symbol),
    constraint today_limit_up_quality_text_check
        check (btrim(rule_code) <> '' and btrim(message) <> '')
);

create index today_limit_up_snapshot_date_idx
    on today_limit_up.snapshot (trade_date, version desc);
create index today_limit_up_member_symbol_idx
    on today_limit_up.member (symbol, snapshot_id);
create index today_limit_up_observation_date_idx
    on today_limit_up.source_observation (trade_date, symbol);

alter table today_limit_up.source_observation enable row level security;
alter table today_limit_up.snapshot enable row level security;
alter table today_limit_up.member enable row level security;
alter table today_limit_up.calculation_quality enable row level security;
create policy today_limit_up_observation_worker on today_limit_up.source_observation
    for all to market_data_worker using (true) with check (true);
create policy today_limit_up_snapshot_worker on today_limit_up.snapshot
    for all to market_data_worker using (true) with check (true);
create policy today_limit_up_member_worker on today_limit_up.member
    for all to market_data_worker using (true) with check (true);
create policy today_limit_up_quality_worker on today_limit_up.calculation_quality
    for all to market_data_worker using (true) with check (true);
grant select, insert on today_limit_up.source_observation, today_limit_up.snapshot,
    today_limit_up.member, today_limit_up.calculation_quality to market_data_worker;
revoke all on all tables in schema today_limit_up from public;
