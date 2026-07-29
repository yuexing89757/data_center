create schema if not exists derived;
create schema if not exists metrics;
revoke all on schema derived, metrics from public;
grant usage on schema derived, metrics to market_data_worker;

create table derived.calculation_run (
    calculation_id uuid primary key,
    calculation_code text not null,
    algorithm_version text not null,
    mode text not null,
    start_date date not null,
    end_date date not null,
    status text not null,
    input_watermark jsonb not null,
    input_hash text not null,
    requested_at timestamptz not null,
    calculated_at timestamptz,
    finished_at timestamptz,
    output_rows bigint not null default 0,
    error_summary text,
    constraint calculation_run_code_check check (btrim(calculation_code) <> ''),
    constraint calculation_run_version_check check (btrim(algorithm_version) <> ''),
    constraint calculation_run_mode_check check (mode in ('full', 'incremental')),
    constraint calculation_run_date_check check (end_date >= start_date),
    constraint calculation_run_status_check check (
        status in ('running', 'succeeded', 'failed')
    ),
    constraint calculation_run_hash_check check (input_hash ~ '^[0-9a-f]{64}$'),
    constraint calculation_run_count_check check (output_rows >= 0)
);

create unique index calculation_run_succeeded_input_idx
    on derived.calculation_run (
        calculation_code, algorithm_version, start_date, end_date, input_hash
    )
    where status = 'succeeded';
create index calculation_run_latest_idx
    on derived.calculation_run (
        calculation_code, algorithm_version, start_date, end_date, calculated_at desc
    ) where status = 'succeeded';

create table derived.adjusted_daily_bar (
    calculation_id uuid not null references derived.calculation_run (calculation_id)
        on delete cascade,
    symbol text not null references core.security (symbol),
    trade_date date not null,
    adjustment_type text not null,
    adjustment_factor numeric(38, 18) not null,
    open numeric(28, 10),
    high numeric(28, 10),
    low numeric(28, 10),
    close numeric(28, 10),
    previous_close numeric(28, 10),
    primary key (calculation_id, symbol, trade_date, adjustment_type),
    constraint adjusted_daily_bar_type_check check (
        adjustment_type in ('forward', 'backward')
    ),
    constraint adjusted_daily_bar_factor_check check (adjustment_factor > 0),
    constraint adjusted_daily_bar_price_check check (
        (open is null or open >= 0)
        and (high is null or high >= 0)
        and (low is null or low >= 0)
        and (close is null or close >= 0)
        and (previous_close is null or previous_close >= 0)
    ),
    constraint adjusted_daily_bar_range_check check (
        low is null or high is null or low <= high
    )
);

create table derived.daily_metric (
    calculation_id uuid not null references derived.calculation_run (calculation_id)
        on delete cascade,
    symbol text not null references core.security (symbol),
    trade_date date not null,
    total_return_1d numeric(28, 12),
    moving_average_5 numeric(28, 10),
    moving_average_10 numeric(28, 10),
    moving_average_20 numeric(28, 10),
    primary key (calculation_id, symbol, trade_date)
);

create table derived.market_capitalization (
    calculation_id uuid not null references derived.calculation_run (calculation_id)
        on delete cascade,
    symbol text not null references core.security (symbol),
    trade_date date not null,
    total_market_cap numeric(38, 4) not null,
    circulating_market_cap numeric(38, 4),
    primary key (calculation_id, symbol, trade_date),
    constraint market_cap_nonnegative_check check (
        total_market_cap >= 0
        and (circulating_market_cap is null or circulating_market_cap >= 0)
    )
);

create table metrics.classification_daily_metric (
    calculation_id uuid not null references derived.calculation_run (calculation_id)
        on delete cascade,
    namespace text not null,
    classification_type text not null,
    classification_code text not null,
    membership_snapshot_date date not null,
    trade_date date not null,
    member_count integer not null,
    priced_member_count integer not null,
    advancing_count integer not null,
    declining_count integer not null,
    unchanged_count integer not null,
    total_volume bigint not null,
    total_amount numeric(38, 4) not null,
    equal_weight_return numeric(28, 12),
    total_market_cap numeric(38, 4),
    market_cap_member_count integer not null,
    primary key (
        calculation_id, namespace, classification_type,
        classification_code, trade_date
    ),
    constraint classification_metric_type_check check (
        classification_type in ('industry', 'concept', 'index')
    ),
    constraint classification_metric_counts_check check (
        member_count >= 0
        and priced_member_count between 0 and member_count
        and advancing_count >= 0
        and declining_count >= 0
        and unchanged_count >= 0
        and advancing_count + declining_count + unchanged_count = priced_member_count
        and total_volume >= 0
        and total_amount >= 0
        and market_cap_member_count between 0 and member_count
        and (total_market_cap is null or total_market_cap >= 0)
    )
);

create index adjusted_daily_bar_lookup_idx
    on derived.adjusted_daily_bar (symbol, trade_date, adjustment_type);
create index daily_metric_lookup_idx
    on derived.daily_metric (symbol, trade_date);
create index market_cap_lookup_idx
    on derived.market_capitalization (symbol, trade_date);
create index classification_daily_metric_lookup_idx
    on metrics.classification_daily_metric (
        namespace, classification_type, classification_code, trade_date
    );

alter table derived.calculation_run enable row level security;
alter table derived.adjusted_daily_bar enable row level security;
alter table derived.daily_metric enable row level security;
alter table derived.market_capitalization enable row level security;
alter table metrics.classification_daily_metric enable row level security;

create policy calculation_run_worker_all on derived.calculation_run
    for all to market_data_worker using (true) with check (true);
create policy adjusted_daily_bar_worker_all on derived.adjusted_daily_bar
    for all to market_data_worker using (true) with check (true);
create policy daily_metric_worker_all on derived.daily_metric
    for all to market_data_worker using (true) with check (true);
create policy market_capitalization_worker_all on derived.market_capitalization
    for all to market_data_worker using (true) with check (true);
create policy classification_daily_metric_worker_all on metrics.classification_daily_metric
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on derived.calculation_run to market_data_worker;
grant select, insert on derived.adjusted_daily_bar to market_data_worker;
grant select, insert on derived.daily_metric to market_data_worker;
grant select, insert on derived.market_capitalization to market_data_worker;
grant select, insert on metrics.classification_daily_metric to market_data_worker;

create view api_v1.calculation_runs as
select calculation_id, calculation_code, algorithm_version, mode,
       start_date, end_date, status, input_watermark, input_hash,
       calculated_at, output_rows
from derived.calculation_run;

create view api_v1.adjusted_daily_bars as
select bar.symbol, bar.trade_date, bar.adjustment_type, bar.adjustment_factor,
       bar.open, bar.high, bar.low, bar.close, bar.previous_close,
       run.calculation_id, run.algorithm_version, run.start_date as calculation_start_date,
       run.end_date as calculation_end_date, run.input_hash, run.calculated_at
from derived.adjusted_daily_bar bar
join derived.calculation_run run using (calculation_id)
where run.status = 'succeeded';

create view api_v1.daily_metrics as
select metric.symbol, metric.trade_date, metric.total_return_1d,
       metric.moving_average_5, metric.moving_average_10, metric.moving_average_20,
       run.calculation_id, run.algorithm_version, run.start_date as calculation_start_date,
       run.end_date as calculation_end_date, run.input_hash, run.calculated_at
from derived.daily_metric metric
join derived.calculation_run run using (calculation_id)
where run.status = 'succeeded';

create view api_v1.market_capitalizations as
select metric.symbol, metric.trade_date, metric.total_market_cap,
       metric.circulating_market_cap, run.calculation_id,
       run.algorithm_version, run.start_date as calculation_start_date,
       run.end_date as calculation_end_date, run.input_hash, run.calculated_at
from derived.market_capitalization metric
join derived.calculation_run run using (calculation_id)
where run.status = 'succeeded';

create view api_v1.classification_daily_metrics as
select metric.namespace, metric.classification_type, metric.classification_code,
       metric.membership_snapshot_date, metric.trade_date, metric.member_count,
       metric.priced_member_count, metric.advancing_count, metric.declining_count,
       metric.unchanged_count, metric.total_volume, metric.total_amount,
       metric.equal_weight_return, metric.total_market_cap,
       metric.market_cap_member_count, run.calculation_id,
       run.algorithm_version, run.start_date as calculation_start_date,
       run.end_date as calculation_end_date, run.input_hash, run.calculated_at
from metrics.classification_daily_metric metric
join derived.calculation_run run using (calculation_id)
where run.status = 'succeeded';

revoke all on
    api_v1.calculation_runs,
    api_v1.adjusted_daily_bars,
    api_v1.daily_metrics,
    api_v1.market_capitalizations,
    api_v1.classification_daily_metrics
from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on
            api_v1.calculation_runs,
            api_v1.adjusted_daily_bars,
            api_v1.daily_metrics,
            api_v1.market_capitalizations,
            api_v1.classification_daily_metrics
        to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on
            api_v1.calculation_runs,
            api_v1.adjusted_daily_bars,
            api_v1.daily_metrics,
            api_v1.market_capitalizations,
            api_v1.classification_daily_metrics
        to authenticated;
    end if;
end
$$;
