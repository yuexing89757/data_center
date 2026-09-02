create schema if not exists regulation;
revoke all on schema regulation from public;
grant usage on schema regulation to market_data_worker;

create table regulation.rule (
    rule_id uuid primary key default extensions.gen_random_uuid(),
    rule_code text not null unique,
    exchange text not null,
    segment text not null,
    rule_name text not null,
    level text not null,
    kind text not null,
    direction text not null,
    window_days integer,
    threshold_pct numeric(20, 8),
    comparison_window_days integer,
    ratio_threshold numeric(20, 8),
    secondary_threshold_pct numeric(20, 8),
    count_window_days integer,
    required_count integer,
    counted_event_kind text,
    reset_level text not null,
    benchmark_symbol text,
    rule_set_version text not null,
    effective_date date not null,
    expire_date date,
    source_document text not null,
    source_clause text not null,
    source_url text not null,
    enabled boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint regulation_rule_exchange_segment_check check (
        (exchange = 'SSE' and segment = 'SSE_MAIN')
        or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM'))
    ),
    constraint regulation_rule_level_check check (
        level in ('ABNORMAL', 'SERIOUS_ABNORMAL')
    ),
    constraint regulation_rule_kind_check check (
        kind in ('CUMULATIVE_DEVIATION', 'TURNOVER_COMPOSITE', 'EVENT_COUNT')
    ),
    constraint regulation_rule_direction_check check (direction in ('UP', 'DOWN', 'NONE')),
    constraint regulation_rule_reset_level_check check (
        reset_level in ('ABNORMAL', 'SERIOUS_ABNORMAL')
    ),
    constraint regulation_rule_nonblank_check check (
        btrim(rule_code) <> '' and btrim(rule_name) <> ''
        and btrim(rule_set_version) <> '' and btrim(source_document) <> ''
        and btrim(source_clause) <> '' and btrim(source_url) <> ''
    ),
    constraint regulation_rule_date_check check (
        effective_date >= date '2026-07-06'
        and (expire_date is null or expire_date >= effective_date)
    ),
    constraint regulation_rule_sign_check check (
        (direction = 'UP' and threshold_pct > 0)
        or (direction = 'DOWN' and (threshold_pct < 0 or kind = 'EVENT_COUNT'))
        or (direction = 'NONE' and threshold_pct is null)
    ),
    constraint regulation_rule_kind_parameters_check check (
        (
            kind = 'CUMULATIVE_DEVIATION'
            and direction in ('UP', 'DOWN')
            and window_days > 0 and threshold_pct is not null
            and comparison_window_days is null and ratio_threshold is null
            and secondary_threshold_pct is null and count_window_days is null
            and required_count is null and counted_event_kind is null
            and benchmark_symbol is not null and btrim(benchmark_symbol) <> ''
        )
        or (
            kind = 'TURNOVER_COMPOSITE'
            and direction = 'NONE' and window_days > 0
            and threshold_pct is null and comparison_window_days > 0
            and ratio_threshold > 0 and secondary_threshold_pct > 0
            and count_window_days is null and required_count is null
            and counted_event_kind is null and benchmark_symbol is null
            and segment in ('SSE_MAIN', 'SZSE_MAIN')
        )
        or (
            kind = 'EVENT_COUNT'
            and direction in ('UP', 'DOWN') and window_days is null
            and threshold_pct is null and comparison_window_days is null
            and ratio_threshold is null and secondary_threshold_pct is null
            and count_window_days > 0 and required_count > 0
            and counted_event_kind = 'PRICE_DEVIATION_ABNORMAL'
            and benchmark_symbol is null
        )
    ),
    constraint regulation_rule_active_dimension_exclusion exclude using gist (
        segment extensions.gist_text_ops with =,
        level extensions.gist_text_ops with =,
        kind extensions.gist_text_ops with =,
        direction extensions.gist_text_ops with =,
        daterange(effective_date, expire_date, '[]') with &&
    ) where (enabled)
);

create table regulation.event (
    event_id uuid primary key default extensions.gen_random_uuid(),
    symbol text not null references core.security (symbol),
    exchange text not null,
    segment text not null,
    event_type text not null,
    event_level text not null,
    direction text,
    period_start_date date not null,
    period_end_date date not null,
    published_at timestamptz not null,
    effective_reset_date date,
    source_event_id text not null,
    source_title text not null,
    source_url text not null,
    source_content_hash text not null,
    source_code text not null,
    explicit_rule_codes text[] not null default '{}',
    observed_at timestamptz not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    unique (source_code, source_event_id),
    unique nulls not distinct (
        source_code, symbol, period_start_date, period_end_date, event_level, direction
    ),
    constraint regulation_event_identity_check check (
        symbol ~ '^(SSE|SZSE):[0-9]{6}$'
        and ((exchange = 'SSE' and segment = 'SSE_MAIN' and source_code = 'sse_official')
          or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM')
              and source_code = 'szse_official'))
        and btrim(source_event_id) <> '' and btrim(source_title) <> ''
        and btrim(source_url) <> '' and source_content_hash ~ '^[0-9a-f]{64}$'
    ),
    constraint regulation_event_type_level_check check (
        (event_type = 'ABNORMAL_VOLATILITY' and event_level = 'ABNORMAL')
        or (event_type = 'SERIOUS_ABNORMAL_VOLATILITY'
            and event_level = 'SERIOUS_ABNORMAL')
    ),
    constraint regulation_event_direction_check check (direction in ('UP', 'DOWN')),
    constraint regulation_event_period_check check (
        period_start_date <= period_end_date
        and (effective_reset_date is null or effective_reset_date > period_end_date)
        and observed_at >= published_at
    )
);

create table regulation.calculation_run (
    calculation_id uuid primary key default extensions.gen_random_uuid(),
    trade_date date not null,
    next_trade_date date not null,
    status text not null,
    algorithm_version text not null,
    rule_set_version text not null,
    rule_set_hash text not null,
    scenario_config_version text not null,
    input_hash text not null,
    market_watermark text not null,
    capital_watermark text not null,
    event_watermark timestamptz not null,
    expected_count integer not null,
    complete_count integer not null,
    incomplete_count integer not null,
    not_applicable_count integer not null,
    started_at timestamptz not null,
    completed_at timestamptz,
    unique (trade_date, input_hash),
    unique (calculation_id, trade_date),
    constraint regulation_calculation_run_status_check check (
        status in ('RUNNING', 'SUCCEEDED', 'PARTIAL', 'FAILED')
    ),
    constraint regulation_calculation_run_date_check check (next_trade_date > trade_date),
    constraint regulation_calculation_run_version_hash_check check (
        btrim(algorithm_version) <> '' and btrim(rule_set_version) <> ''
        and btrim(scenario_config_version) <> ''
        and rule_set_hash ~ '^[0-9a-f]{64}$' and input_hash ~ '^[0-9a-f]{64}$'
        and btrim(market_watermark) <> '' and btrim(capital_watermark) <> ''
    ),
    constraint regulation_calculation_run_coverage_check check (
        expected_count >= 0 and complete_count >= 0 and incomplete_count >= 0
        and not_applicable_count >= 0
        and complete_count + incomplete_count + not_applicable_count = expected_count
    ),
    constraint regulation_calculation_run_completion_check check (
        (status = 'RUNNING' and completed_at is null)
        or (status in ('SUCCEEDED', 'PARTIAL', 'FAILED') and completed_at is not null)
    )
);

create table regulation.status (
    calculation_id uuid not null,
    trade_date date not null,
    symbol text not null references core.security (symbol),
    exchange text not null,
    segment text not null,
    applicability text not null,
    applicability_reason text,
    data_completeness text not null,
    calculated_state text not null,
    announced_state text not null,
    close numeric(20, 8),
    stock_daily_return_pct numeric(20, 8),
    benchmark_symbol text,
    benchmark_close numeric(20, 8),
    benchmark_daily_return_pct numeric(20, 8),
    daily_deviation_pct numeric(20, 8),
    abnormal_count_10d integer not null,
    abnormal_count_10d_up integer not null,
    abnormal_count_10d_down integer not null,
    abnormal_reset_date date,
    serious_reset_date date,
    created_at timestamptz not null default now(),
    primary key (calculation_id, symbol),
    unique (calculation_id, symbol, trade_date),
    foreign key (calculation_id, trade_date)
        references regulation.calculation_run (calculation_id, trade_date),
    constraint regulation_status_identity_check check (
        symbol ~ '^(SSE|SZSE):[0-9]{6}$'
        and ((exchange = 'SSE' and segment = 'SSE_MAIN')
          or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM')))
    ),
    constraint regulation_status_applicability_check check (
        applicability in ('APPLICABLE', 'NOT_APPLICABLE', 'INSUFFICIENT_DATA')
        and data_completeness in ('COMPLETE', 'INCOMPLETE', 'NOT_APPLICABLE')
        and ((applicability = 'APPLICABLE' and data_completeness = 'COMPLETE')
          or (applicability = 'INSUFFICIENT_DATA' and data_completeness = 'INCOMPLETE')
          or (applicability = 'NOT_APPLICABLE' and data_completeness = 'NOT_APPLICABLE'))
    ),
    constraint regulation_status_state_check check (
        calculated_state in ('NORMAL', 'ABNORMAL_TRIGGERED', 'SERIOUS_TRIGGERED')
        and announced_state in ('NONE', 'ABNORMAL', 'SERIOUS_ABNORMAL')
    ),
    constraint regulation_status_price_count_check check (
        (close is null or close > 0) and (benchmark_close is null or benchmark_close > 0)
        and abnormal_count_10d >= 0 and abnormal_count_10d_up >= 0
        and abnormal_count_10d_down >= 0
        and abnormal_count_10d = abnormal_count_10d_up + abnormal_count_10d_down
    )
);

create table regulation.rule_result (
    calculation_id uuid not null,
    symbol text not null,
    rule_id uuid not null references regulation.rule (rule_id),
    evaluation_state text not null,
    triggered boolean not null,
    window_start_date date,
    window_end_date date,
    observed_window_days integer,
    current_value numeric(20, 8),
    threshold numeric(20, 8),
    distance numeric(20, 8),
    secondary_current_value numeric(20, 8),
    secondary_threshold numeric(20, 8),
    event_count integer,
    required_count integer,
    selected_reset_date date,
    data_completeness text not null,
    incomplete_reason text,
    created_at timestamptz not null default now(),
    primary key (calculation_id, symbol, rule_id),
    foreign key (calculation_id, symbol)
        references regulation.status (calculation_id, symbol),
    constraint regulation_rule_result_evaluation_check check (
        evaluation_state in ('NOT_TRIGGERED', 'TRIGGERED_CALCULATED', 'ANNOUNCED_BY_EXCHANGE')
        and triggered = (evaluation_state <> 'NOT_TRIGGERED')
    ),
    constraint regulation_rule_result_window_check check (
        (window_start_date is null and window_end_date is null and observed_window_days is null)
        or (window_start_date <= window_end_date and observed_window_days > 0)
    ),
    constraint regulation_rule_result_distance_check check (
        distance is null or (distance >= 0 and (not triggered or distance = 0))
    ),
    constraint regulation_rule_result_count_check check (
        (event_count is null or event_count >= 0)
        and (required_count is null or required_count > 0)
    ),
    constraint regulation_rule_result_completeness_check check (
        data_completeness in ('COMPLETE', 'INCOMPLETE', 'NOT_APPLICABLE')
    )
);

create table regulation.warning (
    calculation_id uuid not null,
    trade_date date not null,
    next_trade_date date not null,
    symbol text not null,
    rule_id uuid not null,
    warning_type text not null,
    level text not null,
    direction text not null,
    current_value numeric(20, 8),
    threshold numeric(20, 8),
    distance numeric(20, 8),
    scenario_code text not null,
    scenario_index_pct numeric(20, 8),
    next_day_reference_price numeric(20, 8),
    raw_trigger_price numeric(20, 8),
    next_day_trigger_price numeric(20, 8),
    next_day_trigger_pct numeric(20, 8),
    price_limit_ratio numeric(20, 8),
    lower_limit_price numeric(20, 8),
    upper_limit_price numeric(20, 8),
    reachability text not null,
    window_start_date date,
    window_end_date date,
    requires_official_event_confirmation boolean not null,
    message_template_code text not null,
    message text not null,
    created_at timestamptz not null default now(),
    primary key (calculation_id, symbol, rule_id, scenario_code),
    foreign key (calculation_id, symbol, rule_id)
        references regulation.rule_result (calculation_id, symbol, rule_id),
    foreign key (calculation_id, symbol, trade_date)
        references regulation.status (calculation_id, symbol, trade_date),
    constraint regulation_warning_identity_check check (
        btrim(warning_type) <> '' and btrim(message_template_code) <> '' and btrim(message) <> ''
        and level in ('ABNORMAL', 'SERIOUS_ABNORMAL')
        and direction in ('UP', 'DOWN', 'NONE')
    ),
    constraint regulation_warning_distance_price_check check (
        (distance is null or distance >= 0)
        and (next_day_reference_price is null or next_day_reference_price > 0)
        and (raw_trigger_price is null or raw_trigger_price > 0)
        and (next_day_trigger_price is null or next_day_trigger_price > 0)
        and (price_limit_ratio is null or price_limit_ratio > 0)
        and (lower_limit_price is null or lower_limit_price > 0)
        and (upper_limit_price is null or upper_limit_price > 0)
        and (lower_limit_price is null or upper_limit_price is null
             or lower_limit_price <= upper_limit_price)
    ),
    constraint regulation_warning_scenario_reachability_check check (
        (
            scenario_code = 'CURRENT' and scenario_index_pct is null
            and reachability = 'CURRENT' and next_day_trigger_price is null
        )
        or (
            scenario_code = 'NONE' and scenario_index_pct is null
            and reachability = 'NOT_PRICE_CALCULABLE'
            and next_day_trigger_price is null
        )
        or (
            scenario_code in ('INDEX_DOWN_2', 'INDEX_FLAT', 'INDEX_UP_2')
            and scenario_index_pct is not null
            and reachability in ('REACHABLE_NEXT_SESSION', 'NOT_REACHABLE_NEXT_SESSION')
            and next_day_reference_price is not null and raw_trigger_price is not null
            and next_day_trigger_price is not null and next_day_trigger_pct is not null
            and price_limit_ratio is not null and lower_limit_price is not null
            and upper_limit_price is not null
        )
    ),
    constraint regulation_warning_window_check check (
        (window_start_date is null and window_end_date is null)
        or window_start_date <= window_end_date
    )
);

create index regulation_rule_lookup_idx
    on regulation.rule (exchange, segment, effective_date, expire_date);
create index regulation_event_lookup_idx
    on regulation.event (symbol, period_end_date desc, event_level, direction);
create index regulation_calculation_run_lookup_idx
    on regulation.calculation_run (trade_date, completed_at desc);
create index regulation_status_lookup_idx on regulation.status (calculation_id, symbol);
create index regulation_rule_result_lookup_idx
    on regulation.rule_result (calculation_id, symbol, rule_id);
create index regulation_warning_lookup_idx
    on regulation.warning (calculation_id, symbol, rule_id, scenario_code);

alter table regulation.rule enable row level security;
alter table regulation.event enable row level security;
alter table regulation.calculation_run enable row level security;
alter table regulation.status enable row level security;
alter table regulation.rule_result enable row level security;
alter table regulation.warning enable row level security;

create policy regulation_rule_worker_select on regulation.rule
    for select to market_data_worker using (true);
create policy regulation_event_worker_select on regulation.event
    for select to market_data_worker using (true);
create policy regulation_event_worker_insert on regulation.event
    for insert to market_data_worker with check (true);
create policy regulation_calculation_run_worker_all on regulation.calculation_run
    for all to market_data_worker using (true) with check (true);
create policy regulation_status_worker_select on regulation.status
    for select to market_data_worker using (true);
create policy regulation_status_worker_insert on regulation.status
    for insert to market_data_worker with check (true);
create policy regulation_rule_result_worker_select on regulation.rule_result
    for select to market_data_worker using (true);
create policy regulation_rule_result_worker_insert on regulation.rule_result
    for insert to market_data_worker with check (true);
create policy regulation_warning_worker_select on regulation.warning
    for select to market_data_worker using (true);
create policy regulation_warning_worker_insert on regulation.warning
    for insert to market_data_worker with check (true);

revoke all on all tables in schema regulation from public;
grant select on regulation.rule to market_data_worker;
grant select, insert on regulation.event to market_data_worker;
grant select, insert, update on regulation.calculation_run to market_data_worker;
grant select, insert on regulation.status, regulation.rule_result, regulation.warning
    to market_data_worker;

insert into regulation.rule (
    rule_code, exchange, segment, rule_name, level, kind, direction,
    window_days, threshold_pct, comparison_window_days, ratio_threshold,
    secondary_threshold_pct, count_window_days, required_count, counted_event_kind,
    reset_level, benchmark_symbol, rule_set_version, effective_date, expire_date,
    source_document, source_clause, source_url, enabled
) values
-- official-rule: 01
('SSE_MAIN_ABNORMAL_3D_DEV_UP', 'SSE', 'SSE_MAIN', '主板3日向上价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 3, 20, null, null, null, null, null, null, 'ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 02
('SSE_MAIN_ABNORMAL_3D_DEV_DOWN', 'SSE', 'SSE_MAIN', '主板3日向下价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 3, -20, null, null, null, null, null, null, 'ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 03
('SSE_MAIN_ABNORMAL_TURNOVER', 'SSE', 'SSE_MAIN', '主板3日换手率复合异常波动', 'ABNORMAL', 'TURNOVER_COMPOSITE', 'NONE', 3, null, 5, 30, 20, null, null, null, 'ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 04
('SSE_MAIN_SERIOUS_10D_COUNT_UP', 'SSE', 'SSE_MAIN', '主板10日向上多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'UP', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 05
('SSE_MAIN_SERIOUS_10D_COUNT_DOWN', 'SSE', 'SSE_MAIN', '主板10日向下多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'DOWN', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 06
('SSE_MAIN_SERIOUS_10D_DEV_UP', 'SSE', 'SSE_MAIN', '主板10日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 10, 100, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 07
('SSE_MAIN_SERIOUS_10D_DEV_DOWN', 'SSE', 'SSE_MAIN', '主板10日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 10, -50, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 08
('SSE_MAIN_SERIOUS_30D_DEV_UP', 'SSE', 'SSE_MAIN', '主板30日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 30, 200, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(3)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 09
('SSE_MAIN_SERIOUS_30D_DEV_DOWN', 'SSE', 'SSE_MAIN', '主板30日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 30, -70, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000002', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(3)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 10
('SZSE_MAIN_ABNORMAL_3D_DEV_UP', 'SZSE', 'SZSE_MAIN', '主板3日向上价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 3, 20, null, null, null, null, null, null, 'ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 11
('SZSE_MAIN_ABNORMAL_3D_DEV_DOWN', 'SZSE', 'SZSE_MAIN', '主板3日向下价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 3, -20, null, null, null, null, null, null, 'ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 12
('SZSE_MAIN_ABNORMAL_TURNOVER', 'SZSE', 'SZSE_MAIN', '主板3日换手率复合异常波动', 'ABNORMAL', 'TURNOVER_COMPOSITE', 'NONE', 3, null, 5, 30, 20, null, null, null, 'ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.3(2)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 13
('SZSE_MAIN_SERIOUS_10D_COUNT_UP', 'SZSE', 'SZSE_MAIN', '主板10日向上多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'UP', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 14
('SZSE_MAIN_SERIOUS_10D_COUNT_DOWN', 'SZSE', 'SZSE_MAIN', '主板10日向下多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'DOWN', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 15
('SZSE_MAIN_SERIOUS_10D_DEV_UP', 'SZSE', 'SZSE_MAIN', '主板10日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 10, 100, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(2)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 16
('SZSE_MAIN_SERIOUS_10D_DEV_DOWN', 'SZSE', 'SZSE_MAIN', '主板10日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 10, -50, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(2)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 17
('SZSE_MAIN_SERIOUS_30D_DEV_UP', 'SZSE', 'SZSE_MAIN', '主板30日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 30, 200, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(3)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 18
('SZSE_MAIN_SERIOUS_30D_DEV_DOWN', 'SZSE', 'SZSE_MAIN', '主板30日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 30, -70, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399107', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(3)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 19
('GEM_ABNORMAL_3D_DEV_UP', 'SZSE', 'GEM', '创业板3日向上价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 3, 30, null, null, null, null, null, null, 'ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 20
('GEM_ABNORMAL_3D_DEV_DOWN', 'SZSE', 'GEM', '创业板3日向下价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 3, -30, null, null, null, null, null, null, 'ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 21
('GEM_SERIOUS_10D_COUNT_UP', 'SZSE', 'GEM', '创业板10日向上多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'UP', null, null, null, null, null, 10, 3, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 22
('GEM_SERIOUS_10D_COUNT_DOWN', 'SZSE', 'GEM', '创业板10日向下多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'DOWN', null, null, null, null, null, 10, 3, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(1)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 23
('GEM_SERIOUS_10D_DEV_UP', 'SZSE', 'GEM', '创业板10日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 10, 100, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(2)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 24
('GEM_SERIOUS_10D_DEV_DOWN', 'SZSE', 'GEM', '创业板10日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 10, -50, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(2)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 25
('GEM_SERIOUS_30D_DEV_UP', 'SZSE', 'GEM', '创业板30日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 30, 200, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(3)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true),
-- official-rule: 26
('GEM_SERIOUS_30D_DEV_DOWN', 'SZSE', 'GEM', '创业板30日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 30, -70, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SZSE:399102', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '深圳证券交易所交易规则（2026年修订）', '5.4.4(3)', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf', true);
