create schema if not exists dragon_tiger;
revoke all on schema dragon_tiger from public, market_data_api;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on schema dragon_tiger from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on schema dragon_tiger from authenticated;
    end if;
end
$$;
grant usage on schema dragon_tiger to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot',
        'today_limit_up_source','call_auction_indicative_detail','dragon_tiger'
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
        'today_limit_up_source','call_auction_indicative_detail','dragon_tiger'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

create table dragon_tiger.source_snapshot (
    snapshot_id uuid primary key,
    ingestion_id uuid not null unique references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null unique references ingestion.raw_manifest (raw_id),
    trade_date date not null,
    version integer not null check (version > 0),
    source_code text not null check (source_code = 'eastmoney'),
    status text not null check (status in ('complete','partial')),
    observed_at timestamptz not null,
    input_hash text not null check (input_hash ~ '^[0-9a-f]{64}$'),
    content_hash text not null check (content_hash ~ '^[0-9a-f]{64}$'),
    observation_count integer not null check (observation_count >= 0),
    event_count integer not null check (event_count >= 0),
    partial_reasons jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    unique (trade_date, version),
    unique (trade_date, input_hash),
    constraint dragon_tiger_snapshot_observed_date_check check (
        (observed_at at time zone 'Asia/Shanghai')::date >= trade_date
    ),
    constraint dragon_tiger_snapshot_partial_check check (
        jsonb_typeof(partial_reasons) = 'array'
        and ((status = 'complete' and jsonb_array_length(partial_reasons) = 0)
             or (status = 'partial' and jsonb_array_length(partial_reasons) > 0))
    )
);

create table dragon_tiger.source_observation (
    observation_id uuid primary key,
    snapshot_id uuid not null references dragon_tiger.source_snapshot (snapshot_id),
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    source_event_key text not null check (btrim(source_event_key) <> ''),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    source_code text not null check (source_code = 'eastmoney'),
    observed_at timestamptz not null,
    source_name text not null check (btrim(source_name) <> ''),
    source_status_text text,
    created_at timestamptz not null default now(),
    unique (snapshot_id, source_event_key),
    unique (snapshot_id, symbol),
    constraint dragon_tiger_observation_date_check check (
        (observed_at at time zone 'Asia/Shanghai')::date >= trade_date
    )
);

create table dragon_tiger.event (
    event_id uuid primary key,
    snapshot_id uuid not null references dragon_tiger.source_snapshot (snapshot_id),
    observation_id uuid not null unique references dragon_tiger.source_observation (observation_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    revision integer not null check (revision > 0),
    historical_name text not null check (btrim(historical_name) <> ''),
    market text not null check (market = 'CN_A_SHARE'),
    unadjusted_close numeric(18,4) not null check (unadjusted_close > 0),
    change_percent numeric(24,10) not null,
    turnover_amount_cny numeric(30,4) not null check (turnover_amount_cny >= 0),
    turnover_rate_percent numeric(24,10) check (turnover_rate_percent >= 0),
    event_status text not null check (event_status in ('observed','partial','retracted')),
    source_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    source_raw_id uuid not null references ingestion.raw_manifest (raw_id),
    created_at timestamptz not null default now(),
    unique (snapshot_id, symbol),
    unique (symbol, trade_date, revision)
);

create table dragon_tiger.reason (
    reason_id uuid primary key,
    event_id uuid not null references dragon_tiger.event (event_id),
    observation_id uuid not null references dragon_tiger.source_observation (observation_id),
    reason_code text not null check (reason_code ~ '^[a-z][a-z0-9_]{1,63}$'),
    reason_name text not null check (btrim(reason_name) <> ''),
    source_original_text text not null check (btrim(source_original_text) <> ''),
    display_order integer not null check (display_order >= 0),
    source_numeric_value numeric(30,10),
    source_numeric_unit text,
    source_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    source_raw_id uuid not null references ingestion.raw_manifest (raw_id),
    created_at timestamptz not null default now(),
    unique (event_id, reason_code),
    unique (event_id, display_order),
    constraint dragon_tiger_reason_numeric_pair_check check (
        (source_numeric_value is null) = (source_numeric_unit is null)
        and (source_numeric_unit is null or btrim(source_numeric_unit) <> '')
    )
);

create table dragon_tiger.seat (
    seat_id uuid primary key,
    identity_key text not null check (identity_key ~ '^[a-z0-9][a-z0-9:_-]{2,127}$'),
    canonical_name text not null check (btrim(canonical_name) <> ''),
    seat_type text not null check (seat_type in ('institution','broker_branch','other','unknown')),
    broker_name text,
    branch_name text,
    region text,
    valid_from date not null,
    valid_to date,
    source_name text not null check (btrim(source_name) <> ''),
    normalization_status text not null
        check (normalization_status in ('matched','provisional','unmatched')),
    source_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    unique (identity_key, valid_from),
    exclude using gist (
        identity_key with =,
        daterange(valid_from, coalesce(valid_to, 'infinity'::date), '[]') with &&
    ),
    constraint dragon_tiger_seat_validity_check check (
        valid_to is null or valid_to >= valid_from
    ),
    constraint dragon_tiger_seat_broker_check check (
        seat_type <> 'broker_branch' or (broker_name is not null and btrim(broker_name) <> '')
    )
);

create table dragon_tiger.seat_activity (
    activity_id uuid primary key,
    event_id uuid not null references dragon_tiger.event (event_id),
    seat_id uuid not null references dragon_tiger.seat (seat_id),
    observation_id uuid not null references dragon_tiger.source_observation (observation_id),
    side text not null check (side in ('buy','sell','both')),
    buy_amount_cny numeric(30,4) not null check (buy_amount_cny >= 0),
    sell_amount_cny numeric(30,4) not null check (sell_amount_cny >= 0),
    net_amount_cny numeric(30,4) not null,
    buy_rank integer check (buy_rank > 0),
    sell_rank integer check (sell_rank > 0),
    source_seat_name text not null check (btrim(source_seat_name) <> ''),
    source_order integer not null check (source_order >= 0),
    source_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    source_raw_id uuid not null references ingestion.raw_manifest (raw_id),
    created_at timestamptz not null default now(),
    unique (event_id, seat_id),
    unique (event_id, source_order),
    constraint dragon_tiger_activity_net_check check (
        net_amount_cny = buy_amount_cny - sell_amount_cny
    ),
    constraint dragon_tiger_activity_side_check check (
        (side = 'buy' and buy_amount_cny > 0 and sell_amount_cny = 0)
        or (side = 'sell' and sell_amount_cny > 0 and buy_amount_cny = 0)
        or (side = 'both' and buy_amount_cny > 0 and sell_amount_cny > 0)
    )
);

create table dragon_tiger.event_summary (
    event_id uuid not null references dragon_tiger.event (event_id),
    calculation_version text not null check (btrim(calculation_version) <> ''),
    calculated_at timestamptz not null,
    total_buy_amount_cny numeric(30,4) not null check (total_buy_amount_cny >= 0),
    total_sell_amount_cny numeric(30,4) not null check (total_sell_amount_cny >= 0),
    total_net_amount_cny numeric(30,4) not null,
    institution_buy_amount_cny numeric(30,4) not null check (institution_buy_amount_cny >= 0),
    institution_sell_amount_cny numeric(30,4) not null check (institution_sell_amount_cny >= 0),
    institution_net_amount_cny numeric(30,4) not null,
    top5_buy_amount_cny numeric(30,4) not null check (top5_buy_amount_cny >= 0),
    top5_sell_amount_cny numeric(30,4) not null check (top5_sell_amount_cny >= 0),
    top5_buy_concentration_ratio numeric(24,12)
        check (top5_buy_concentration_ratio between 0 and 1),
    top5_sell_concentration_ratio numeric(24,12)
        check (top5_sell_concentration_ratio between 0 and 1),
    activity_count integer not null check (activity_count >= 0),
    institution_activity_count integer not null check (
        institution_activity_count >= 0 and institution_activity_count <= activity_count
    ),
    created_at timestamptz not null default now(),
    primary key (event_id, calculation_version),
    constraint dragon_tiger_summary_net_check check (
        total_net_amount_cny = total_buy_amount_cny - total_sell_amount_cny
        and institution_net_amount_cny =
            institution_buy_amount_cny - institution_sell_amount_cny
    ),
    constraint dragon_tiger_summary_concentration_check check (
        ((total_buy_amount_cny = 0 and top5_buy_amount_cny = 0
          and top5_buy_concentration_ratio is null)
         or (total_buy_amount_cny > 0 and top5_buy_amount_cny <= total_buy_amount_cny
             and top5_buy_concentration_ratio =
                 round(top5_buy_amount_cny / total_buy_amount_cny, 12)))
        and (
            (total_sell_amount_cny = 0 and top5_sell_amount_cny = 0
             and top5_sell_concentration_ratio is null)
            or (total_sell_amount_cny > 0
                and top5_sell_amount_cny <= total_sell_amount_cny
                and top5_sell_concentration_ratio =
                    round(top5_sell_amount_cny / total_sell_amount_cny, 12))
        )
    )
);

create table dragon_tiger.snapshot_quality (
    snapshot_id uuid not null references dragon_tiger.source_snapshot (snapshot_id),
    rule_code text not null check (btrim(rule_code) <> ''),
    severity text not null check (severity in ('warning','error')),
    identity text not null default '',
    message text not null check (btrim(message) <> ''),
    created_at timestamptz not null default now(),
    primary key (snapshot_id, rule_code, identity)
);

create index dragon_tiger_snapshot_date_idx
    on dragon_tiger.source_snapshot (trade_date, version desc);
create index dragon_tiger_event_symbol_date_idx
    on dragon_tiger.event (symbol, trade_date desc, revision desc);
create index dragon_tiger_activity_event_idx
    on dragon_tiger.seat_activity (event_id, source_order);
create unique index dragon_tiger_activity_buy_rank_idx
    on dragon_tiger.seat_activity (event_id, buy_rank) where buy_rank is not null;
create unique index dragon_tiger_activity_sell_rank_idx
    on dragon_tiger.seat_activity (event_id, sell_rank) where sell_rank is not null;
create index dragon_tiger_seat_identity_idx
    on dragon_tiger.seat (identity_key, valid_from desc);

alter table dragon_tiger.source_snapshot enable row level security;
alter table dragon_tiger.source_observation enable row level security;
alter table dragon_tiger.event enable row level security;
alter table dragon_tiger.reason enable row level security;
alter table dragon_tiger.seat enable row level security;
alter table dragon_tiger.seat_activity enable row level security;
alter table dragon_tiger.event_summary enable row level security;
alter table dragon_tiger.snapshot_quality enable row level security;

create policy dragon_tiger_snapshot_worker_select on dragon_tiger.source_snapshot
    for select to market_data_worker using (true);
create policy dragon_tiger_snapshot_worker_insert on dragon_tiger.source_snapshot
    for insert to market_data_worker with check (true);
create policy dragon_tiger_observation_worker_select on dragon_tiger.source_observation
    for select to market_data_worker using (true);
create policy dragon_tiger_observation_worker_insert on dragon_tiger.source_observation
    for insert to market_data_worker with check (true);
create policy dragon_tiger_event_worker_select on dragon_tiger.event
    for select to market_data_worker using (true);
create policy dragon_tiger_event_worker_insert on dragon_tiger.event
    for insert to market_data_worker with check (true);
create policy dragon_tiger_reason_worker_select on dragon_tiger.reason
    for select to market_data_worker using (true);
create policy dragon_tiger_reason_worker_insert on dragon_tiger.reason
    for insert to market_data_worker with check (true);
create policy dragon_tiger_seat_worker_select on dragon_tiger.seat
    for select to market_data_worker using (true);
create policy dragon_tiger_seat_worker_insert on dragon_tiger.seat
    for insert to market_data_worker with check (true);
create policy dragon_tiger_activity_worker_select on dragon_tiger.seat_activity
    for select to market_data_worker using (true);
create policy dragon_tiger_activity_worker_insert on dragon_tiger.seat_activity
    for insert to market_data_worker with check (true);
create policy dragon_tiger_summary_worker_select on dragon_tiger.event_summary
    for select to market_data_worker using (true);
create policy dragon_tiger_summary_worker_insert on dragon_tiger.event_summary
    for insert to market_data_worker with check (true);
create policy dragon_tiger_quality_worker_select on dragon_tiger.snapshot_quality
    for select to market_data_worker using (true);
create policy dragon_tiger_quality_worker_insert on dragon_tiger.snapshot_quality
    for insert to market_data_worker with check (true);

grant select, insert on all tables in schema dragon_tiger to market_data_worker;
revoke update, delete, truncate, references, trigger on all tables in schema dragon_tiger
    from market_data_worker;
revoke all on all tables in schema dragon_tiger from public, market_data_api;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on all tables in schema dragon_tiger from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on all tables in schema dragon_tiger from authenticated;
    end if;
end
$$;
