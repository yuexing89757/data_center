create table derived.close_price_new_high_120d_snapshot (
    snapshot_id uuid primary key,
    calculation_id uuid not null unique references derived.calculation_run (calculation_id),
    trade_date date not null,
    version integer not null,
    status text not null,
    candidate_count integer not null,
    eligible_history_count integer not null,
    omitted_count integer not null,
    member_count integer not null,
    incomplete_history_count integer not null,
    non_trading_bar_count integer not null,
    nonpositive_price_count integer not null,
    missing_name_count integer not null,
    input_hash text not null,
    content_hash text not null,
    algorithm_version text not null,
    generated_at timestamptz not null,
    unique (trade_date, version),
    unique (trade_date, input_hash),
    constraint close_price_new_high_snapshot_status_check check (status='ready'),
    constraint close_price_new_high_snapshot_count_check check (
        version > 0 and candidate_count >= 0 and eligible_history_count >= 0
        and omitted_count >= 0 and member_count >= 0
        and eligible_history_count + omitted_count = candidate_count
        and member_count <= eligible_history_count
        and incomplete_history_count >= 0 and non_trading_bar_count >= 0
        and nonpositive_price_count >= 0 and missing_name_count >= 0
    ),
    constraint close_price_new_high_snapshot_hash_check check (
        input_hash ~ '^[0-9a-f]{64}$' and content_hash ~ '^[0-9a-f]{64}$'
    ),
    constraint close_price_new_high_snapshot_algorithm_check check (
        btrim(algorithm_version) <> ''
    )
);

create table derived.close_price_new_high_120d_member (
    snapshot_id uuid not null
        references derived.close_price_new_high_120d_snapshot (snapshot_id),
    symbol text not null references core.security (symbol),
    display_name text not null,
    close numeric(18,4) not null,
    previous_119d_high numeric(18,4) not null,
    breakout_pct numeric(24,10) not null,
    primary key (snapshot_id, symbol),
    constraint close_price_new_high_member_name_check check (btrim(display_name) <> ''),
    constraint close_price_new_high_member_price_check check (
        close > previous_119d_high and previous_119d_high > 0 and breakout_pct > 0
    )
);

create index close_price_new_high_snapshot_ready_idx
    on derived.close_price_new_high_120d_snapshot (trade_date desc, version desc)
    where status='ready';
create index close_price_new_high_member_order_idx
    on derived.close_price_new_high_120d_member
    (snapshot_id, breakout_pct desc, symbol);

alter table derived.close_price_new_high_120d_snapshot enable row level security;
alter table derived.close_price_new_high_120d_member enable row level security;

create policy close_price_new_high_snapshot_worker_all
    on derived.close_price_new_high_120d_snapshot
    for all to market_data_worker using (true) with check (true);
create policy close_price_new_high_member_worker_all
    on derived.close_price_new_high_120d_member
    for all to market_data_worker using (true) with check (true);

grant select, insert on derived.close_price_new_high_120d_snapshot,
    derived.close_price_new_high_120d_member to market_data_worker;

create or replace function api_v1.query_close_price_new_highs_120d()
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, derived
set statement_timeout = '10s'
as $$
declare
    selected derived.close_price_new_high_120d_snapshot%rowtype;
    items jsonb;
begin
    select * into selected
    from derived.close_price_new_high_120d_snapshot snapshot
    where snapshot.status='ready'
    order by snapshot.trade_date desc, snapshot.version desc
    limit 1;

    if not found then
        raise exception 'ready closing-high snapshot does not exist' using errcode='P0002';
    end if;

    select coalesce(
        jsonb_agg(
            jsonb_build_object(
                'symbol', member.symbol,
                'code', split_part(member.symbol, ':', 2),
                'name', member.display_name,
                'close', member.close,
                'previous_119d_high', member.previous_119d_high,
                'breakout_pct', member.breakout_pct
            ) order by member.breakout_pct desc, member.symbol
        ),
        '[]'::jsonb
    ) into items
    from derived.close_price_new_high_120d_member member
    where member.snapshot_id=selected.snapshot_id;

    return jsonb_build_object(
        'trade_date', selected.trade_date,
        'window_trading_session_count', 120,
        'comparison_session_count', 119,
        'total_candidate_count', selected.candidate_count,
        'eligible_history_count', selected.eligible_history_count,
        'omitted_count', selected.omitted_count,
        'returned_count', selected.member_count,
        'omissions', jsonb_build_object(
            'incomplete_history', selected.incomplete_history_count,
            'non_trading_bar', selected.non_trading_bar_count,
            'nonpositive_price', selected.nonpositive_price_count,
            'missing_name', selected.missing_name_count
        ),
        'items', items
    );
end
$$;

revoke all on function api_v1.query_close_price_new_highs_120d()
    from public, anon, authenticated;
grant execute on function api_v1.query_close_price_new_highs_120d()
    to market_data_api;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'stock_pool','auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','pytdx_pool_refresh','today_limit_up_snapshot',
        'call_auction_market_series','close_price_new_highs_120d'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;
