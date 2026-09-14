create extension if not exists btree_gist with schema extensions;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','security_bse_daily','dragon_tiger_daily',
        'dragon_tiger_seat_profile','regulation_daily_calculation','data_cleanup',
        'call_auction_market_series_archive'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table billboard.hot_money_actor (
    actor_id uuid primary key default extensions.gen_random_uuid(),
    actor_code text not null unique,
    canonical_name text not null,
    aliases text[] not null default '{}',
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint hot_money_actor_nonblank_check check (
        btrim(actor_code) <> '' and btrim(canonical_name) <> ''
    )
);

create trigger hot_money_actor_set_updated_at
before update on billboard.hot_money_actor
for each row execute function ingestion.set_updated_at();

create table billboard.hot_money_seat_mapping (
    mapping_id uuid primary key default extensions.gen_random_uuid(),
    actor_id uuid not null references billboard.hot_money_actor (actor_id),
    seat_id uuid not null references billboard.trading_seat (seat_id),
    valid_from date,
    valid_to date,
    source_alias_name text not null,
    evidence_note text not null,
    review_status text not null,
    reviewed_at timestamptz,
    catalog_version text not null,
    created_at timestamptz not null default now(),
    constraint hot_money_mapping_nonblank_check check (
        btrim(source_alias_name) <> '' and btrim(evidence_note) <> ''
        and btrim(catalog_version) <> ''
    ),
    constraint hot_money_mapping_range_check check (
        valid_to is null or valid_from is null or valid_from <= valid_to
    ),
    constraint hot_money_mapping_review_check check (
        (review_status = 'PENDING' and reviewed_at is null)
        or (review_status in ('APPROVED','REJECTED') and reviewed_at is not null)
    ),
    constraint hot_money_mapping_generic_alias_check check (
        source_alias_name not in ('机构专用','沪股通专用','深股通专用','北向资金专用')
    ),
    constraint hot_money_mapping_approved_seat_overlap exclude using gist (
        seat_id with =,
        daterange(coalesce(valid_from, '-infinity'::date),
                  coalesce(valid_to, 'infinity'::date), '[]') with &&
    ) where (review_status = 'APPROVED')
);

create table billboard.trading_seat_profile_daily (
    seat_id uuid not null references billboard.trading_seat (seat_id),
    as_of_date date not null,
    algorithm_version text not null,
    metric_definition text not null,
    return_definition text not null,
    participation_definition text not null,
    total_lhb_count integer not null check (total_lhb_count >= 0),
    total_buy_amount numeric check (total_buy_amount is null or total_buy_amount >= 0),
    total_sell_amount numeric check (total_sell_amount is null or total_sell_amount >= 0),
    t1_sample_count integer not null check (t1_sample_count >= 0),
    t1_win_rate numeric check (t1_win_rate between 0 and 1),
    t1_avg_return numeric,
    t3_sample_count integer not null check (t3_sample_count >= 0),
    t3_win_rate numeric check (t3_win_rate between 0 and 1),
    t3_avg_return numeric,
    t5_sample_count integer not null check (t5_sample_count >= 0),
    t5_win_rate numeric check (t5_win_rate between 0 and 1),
    t5_avg_return numeric,
    consecutive_participation_sample_count integer not null
        check (consecutive_participation_sample_count >= 0),
    consecutive_participation_rate numeric
        check (consecutive_participation_rate between 0 and 1),
    input_watermark_date date not null,
    calculation_id uuid not null references derived.calculation_run (calculation_id),
    created_at timestamptz not null default now(),
    primary key (seat_id, as_of_date, algorithm_version),
    constraint trading_seat_profile_text_check check (
        btrim(algorithm_version) <> '' and btrim(metric_definition) <> ''
        and btrim(return_definition) <> '' and btrim(participation_definition) <> ''
    ),
    constraint trading_seat_profile_watermark_check check (input_watermark_date <= as_of_date),
    constraint trading_seat_profile_sample_value_check check (
        (t1_sample_count > 0 or (t1_win_rate is null and t1_avg_return is null))
        and (t3_sample_count > 0 or (t3_win_rate is null and t3_avg_return is null))
        and (t5_sample_count > 0 or (t5_win_rate is null and t5_avg_return is null))
    )
);

create index hot_money_mapping_actor_date_idx
    on billboard.hot_money_seat_mapping (actor_id, valid_from, valid_to);
create index hot_money_mapping_seat_date_idx
    on billboard.hot_money_seat_mapping (seat_id, valid_from, valid_to)
    where review_status = 'APPROVED';
create index trading_seat_profile_date_idx
    on billboard.trading_seat_profile_daily (as_of_date desc, seat_id);

alter table billboard.hot_money_actor enable row level security;
alter table billboard.hot_money_seat_mapping enable row level security;
alter table billboard.trading_seat_profile_daily enable row level security;

create policy hot_money_actor_worker_all on billboard.hot_money_actor
    for all to market_data_worker using (true) with check (true);
create policy hot_money_mapping_worker_all on billboard.hot_money_seat_mapping
    for all to market_data_worker using (true) with check (true);
create policy trading_seat_profile_worker_all on billboard.trading_seat_profile_daily
    for all to market_data_worker using (true) with check (true);

grant select, insert, update, delete on billboard.hot_money_actor to market_data_worker;
grant select, insert, update, delete on billboard.hot_money_seat_mapping to market_data_worker;
grant select, insert, update, delete on billboard.trading_seat_profile_daily
    to market_data_worker;
revoke all on billboard.hot_money_actor from public, anon, authenticated;
revoke all on billboard.hot_money_seat_mapping from public, anon, authenticated;
revoke all on billboard.trading_seat_profile_daily from public, anon, authenticated;

comment on table billboard.hot_money_actor is
    'Versioned manually reviewed hot-money actor catalog; not an inferred trading label.';
comment on table billboard.hot_money_seat_mapping is
    'Effective-dated reviewed mapping from a hot-money actor to a stable trading seat.';
comment on table billboard.trading_seat_profile_daily is
    'Versioned point-in-time objective T+1/T+3/T+5 stable-seat performance profile.';
