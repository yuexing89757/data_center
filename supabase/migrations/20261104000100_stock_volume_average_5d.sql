-- ADR-0076 / Issue #103. Schema only: no production calculation or data repair.
alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','today_limit_down_snapshot',
        'close_price_new_highs_120d','board_index_daily_bar','trading_billboard_daily',
        'security_bse_daily','dragon_tiger_daily','dragon_tiger_seat_profile',
        'regulation_daily_calculation','regulation_event_reconciliation','data_cleanup',
        'call_auction_market_series_archive','top10_float_holders_weekly','board_moneyflow_daily',
        'stock_technical_factor','stock_volume_average_5d'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table derived.stock_volume_average_5d (
    calculation_id uuid not null references derived.calculation_run(calculation_id),
    symbol text not null references core.security(symbol),
    trade_date date not null,
    window_start_date date not null check (window_start_date < trade_date),
    volume_average_5d numeric(28,1) check (
        volume_average_5d >= 0 and volume_average_5d < 1e27
    ),
    covered_days integer not null check (covered_days between 0 and 5),
    quality_reasons text[] not null,
    input_evidence jsonb not null check (
        jsonb_typeof(input_evidence) = 'array' and jsonb_array_length(input_evidence) = 5
    ),
    primary key (calculation_id, symbol, trade_date),
    check (
        (covered_days = 5 and volume_average_5d is not null and cardinality(quality_reasons) = 0)
        or (covered_days < 5 and volume_average_5d is null and cardinality(quality_reasons) > 0)
    ),
    check (array_position(quality_reasons, null) is null),
    check (quality_reasons <@ array[
        'insufficient_listing_history','missing_daily_bar','missing_volume',
        'quantity_units_unverified','suspended'
    ]::text[])
);
create index stock_volume_average_5d_date_symbol_idx
    on derived.stock_volume_average_5d(trade_date, symbol);

create table derived.stock_volume_average_5d_snapshot (
    trade_date date not null,
    version integer not null check (version > 0),
    calculation_id uuid not null references derived.calculation_run(calculation_id),
    published_at timestamptz not null,
    candidate_count integer not null check (candidate_count between 1 and 10000),
    complete_count integer not null check (complete_count between 0 and candidate_count),
    input_evidence jsonb not null check (jsonb_typeof(input_evidence) = 'object'),
    input_watermark jsonb not null check (jsonb_typeof(input_watermark) = 'object'),
    evidence_hash text not null check (evidence_hash ~ '^[0-9a-f]{64}$'),
    primary key (trade_date, version)
);
create index stock_volume_average_5d_snapshot_calculation_idx
    on derived.stock_volume_average_5d_snapshot(calculation_id);

alter table derived.stock_volume_average_5d enable row level security;
alter table derived.stock_volume_average_5d_snapshot enable row level security;
create policy stock_volume_average_5d_worker on derived.stock_volume_average_5d
    for all to market_data_worker using (true) with check (true);
create policy stock_volume_average_5d_snapshot_worker on derived.stock_volume_average_5d_snapshot
    for all to market_data_worker using (true) with check (true);
revoke all on derived.stock_volume_average_5d, derived.stock_volume_average_5d_snapshot
    from public, anon, authenticated, market_data_api;
grant select, insert on derived.stock_volume_average_5d, derived.stock_volume_average_5d_snapshot
    to market_data_worker;
grant select on derived.stock_volume_average_5d, derived.stock_volume_average_5d_snapshot
    to market_data_backup;

comment on table derived.stock_volume_average_5d is '包含当日的固定五个市场交易日成交均量，不可变版本化结果；缺失不补零。';
comment on column derived.stock_volume_average_5d.calculation_id is '所属计算批次，算法及输入哈希见derived.calculation_run；禁止逐股票混选批次。';
comment on column derived.stock_volume_average_5d.symbol is '标准股票代码，包含SSE/SZSE/BSE交易所及保留前导零的六位代码。';
comment on column derived.stock_volume_average_5d.trade_date is '包含在五日窗口中的目标交易日，上海交易日期。';
comment on column derived.stock_volume_average_5d.window_start_date is '固定五个市场交易日窗口的第一日。';
comment on column derived.stock_volume_average_5d.volume_average_5d is '五日已认证整数成交股数之和除以5，单位股，允许一位小数；窗口不完整为NULL。';
comment on column derived.stock_volume_average_5d.covered_days is '五个固定日期中具有已认证非负整数股数且非停牌的天数；真实零计入。';
comment on column derived.stock_volume_average_5d.quality_reasons is '排序去重的缺失原因码；完整结果为空数组，不完整结果至少一个原因。';
comment on column derived.stock_volume_average_5d.input_evidence is '受限五日输入股数、缺失状态、来源批次及数量认证引用；不存未认证量值或Raw路径。';
comment on table derived.stock_volume_average_5d_snapshot is '每日均量不可变发布序列；同交易日最大version指向当前单一计算批次。';
comment on column derived.stock_volume_average_5d_snapshot.trade_date is '精确发布交易日；不回退其他日期。';
comment on column derived.stock_volume_average_5d_snapshot.version is '同交易日递增发布版本；输入A到B再到A复用旧计算但追加新发布版本。';
comment on column derived.stock_volume_average_5d_snapshot.calculation_id is '本次发布的完整市场计算批次，所有股票结果共用。';
comment on column derived.stock_volume_average_5d_snapshot.published_at is '本次发布时刻，不篡改被复用计算的原calculated_at。';
comment on column derived.stock_volume_average_5d_snapshot.candidate_count is '目标日有效的三交易所普通股票候选数。';
comment on column derived.stock_volume_average_5d_snapshot.complete_count is '五日数量完整且产生非NULL均量的股票数。';
comment on column derived.stock_volume_average_5d_snapshot.input_evidence is '本次发布的受限完整市场输入证据；复用旧数值计算时仍保存当次来源及认证引用。';
comment on column derived.stock_volume_average_5d_snapshot.input_watermark is '本次发布实际读取的工作流、水位及单位契约版本；不覆盖原计算批次水位。';
comment on column derived.stock_volume_average_5d_snapshot.evidence_hash is '排序规范化后的本次输入证据SHA256；数值与证据均相同才返回unchanged。';
