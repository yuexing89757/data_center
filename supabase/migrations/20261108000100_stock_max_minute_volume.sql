-- ADR-0077 / Issue #105: bounded minute-quantity peak; no minute Core or public details.
alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source','today_limit_down_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger',
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow',
        'stock_technical_factor','stock_minute_volume'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;
alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source','today_limit_down_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger',
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow',
        'stock_technical_factor','stock_minute_volume'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

create table derived.stock_max_minute_volume (
    calculation_id uuid not null references derived.calculation_run(calculation_id),
    symbol text not null references core.security(symbol),
    trade_date date not null,
    max_minute_volume_shares bigint check (max_minute_volume_shares >= 0),
    peak_at timestamptz,
    expected_count integer not null check (expected_count = 240),
    observed_count integer not null check (observed_count between 0 and 4800),
    quality_reasons text[] not null check (array_position(quality_reasons,null) is null),
    input_evidence jsonb not null check (jsonb_typeof(input_evidence) = 'object'),
    primary key (calculation_id,symbol,trade_date),
    check (
        (max_minute_volume_shares is not null and peak_at is not null
         and observed_count=240 and cardinality(quality_reasons)=0)
        or (max_minute_volume_shares is null and peak_at is null
            and cardinality(quality_reasons)>0)
    ),
    check (peak_at is null or (
        (peak_at at time zone 'Asia/Shanghai')::date=trade_date
        and extract(second from peak_at)=0
        and ((peak_at at time zone 'Asia/Shanghai')::time between time '09:31' and time '11:30'
             or (peak_at at time zone 'Asia/Shanghai')::time between time '13:01' and time '15:00')
    ))
);
create index stock_max_minute_volume_date_symbol_idx
    on derived.stock_max_minute_volume(trade_date,symbol);

create table derived.stock_max_minute_volume_snapshot (
    trade_date date not null,
    version integer not null check (version>0),
    calculation_id uuid not null references derived.calculation_run(calculation_id),
    published_at timestamptz not null,
    candidate_count integer not null check (candidate_count between 1 and 10000),
    complete_count integer not null check (complete_count between 1 and candidate_count),
    input_evidence jsonb not null check (
        jsonb_typeof(input_evidence)='object'
        and input_evidence ? 'coverage_scope' and input_evidence ? 'stocks'
        and input_evidence->>'coverage_scope' in ('full_market','requested_symbols')
        and jsonb_typeof(input_evidence->'stocks')='object'
    ),
    evidence_hash text not null check (evidence_hash ~ '^[0-9a-f]{64}$'),
    primary key (trade_date,version)
);
create index stock_max_minute_volume_snapshot_calculation_idx
    on derived.stock_max_minute_volume_snapshot(calculation_id);

alter table derived.stock_max_minute_volume enable row level security;
alter table derived.stock_max_minute_volume_snapshot enable row level security;
create policy stock_max_minute_volume_worker on derived.stock_max_minute_volume
    for all to market_data_worker using(true) with check(true);
create policy stock_max_minute_volume_snapshot_worker on derived.stock_max_minute_volume_snapshot
    for all to market_data_worker using(true) with check(true);
revoke all on derived.stock_max_minute_volume,derived.stock_max_minute_volume_snapshot
    from public,anon,authenticated,market_data_api;
grant select,insert on derived.stock_max_minute_volume,derived.stock_max_minute_volume_snapshot
    to market_data_worker;
grant select on derived.stock_max_minute_volume,derived.stock_max_minute_volume_snapshot
    to market_data_backup;

comment on table derived.stock_max_minute_volume is '指定交易日来源原生240根一分钟数量柱的最大整数股数；不可变单一计算版本，不公开分钟明细。';
comment on column derived.stock_max_minute_volume.peak_at is '并列峰值取最早来源结束标签；按Asia/Shanghai解释，不剥离来源首尾柱内竞价量。';
comment on column derived.stock_max_minute_volume.input_evidence is '受限Raw及数量认证引用；无分钟OHLC，不对外暴露路径或协议凭证。';
comment on table derived.stock_max_minute_volume_snapshot is '精确日期递增发布版本；同日新有效集不得缩小旧有效集，零有效结果不发布。';
comment on column derived.stock_max_minute_volume_snapshot.input_evidence is '当次发布证据及full_market/requested_symbols覆盖范围；数值计算复用时仍保留当次证据。';
comment on column derived.stock_max_minute_volume.calculation_id is '不可变计算批次；算法版本、输入哈希与计算时刻见derived.calculation_run。';
comment on column derived.stock_max_minute_volume.symbol is 'SSE/SZSE/BSE标准股票代码，保留六位代码前导零。';
comment on column derived.stock_max_minute_volume.trade_date is '准确的上海市场交易日期，不回退其他日期。';
comment on column derived.stock_max_minute_volume.max_minute_volume_shares is '来源原生一分钟数量柱最大整数股数；完整全零为0，不完整或未认证为NULL。';
comment on column derived.stock_max_minute_volume.expected_count is '本算法完整交易日固定需要240个合法结束时间标签。';
comment on column derived.stock_max_minute_volume.observed_count is '实际观察样本数量；不以补零或前值填充构造完整时段。';
comment on column derived.stock_max_minute_volume.quality_reasons is '缺口或不准入原因码；完整标准值为空数组，NULL结果至少一个原因。';
comment on column derived.stock_max_minute_volume_snapshot.trade_date is '准确发布交易日；读取只选该日版本。';
comment on column derived.stock_max_minute_volume_snapshot.version is '同日严格递增的不可变发布序号；数值复用但证据变化时仍新增发布。';
comment on column derived.stock_max_minute_volume_snapshot.calculation_id is '当次发布对应的单一完整计算批次；禁止逐股票混选其他批次。';
comment on column derived.stock_max_minute_volume_snapshot.published_at is '带时区的实际发布时刻，独立于可复用旧计算批次的计算时刻。';
comment on column derived.stock_max_minute_volume_snapshot.candidate_count is '该次计算的目标日普通股票候选数；覆盖范围另见input_evidence。';
comment on column derived.stock_max_minute_volume_snapshot.complete_count is '该次发布具有完整认证结果的证券数量；至少一个有效结果才可发布。';
comment on column derived.stock_max_minute_volume_snapshot.evidence_hash is '本次完整输入证据规范化JSON的SHA256；与数值输入哈希分别维护。';

create function api_v1.query_stock_max_minute_volume(p_symbols text[],p_trade_date date)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,pg_temp set statement_timeout='5s'
as $$
declare v_snapshot derived.stock_max_minute_volume_snapshot%rowtype;
        v_calculation derived.calculation_run%rowtype;
        v_items jsonb;
        v_revoked boolean := false;
begin
    if p_trade_date is null or p_trade_date>(current_timestamp at time zone 'Asia/Shanghai')::date
        or p_symbols is null or cardinality(p_symbols) not between 1 and 500
        or exists(select 1 from unnest(p_symbols) s where s is null or s !~ '^(SSE|SZSE|BSE):[0-9]{6}$') then
        raise exception 'Exact nonfuture trade date and 1 to 500 standard stock symbols required' using errcode='22023';
    end if;
    select * into v_snapshot from derived.stock_max_minute_volume_snapshot
        where trade_date=p_trade_date order by version desc limit 1;
    if v_snapshot.calculation_id is not null then
        select * into v_calculation from derived.calculation_run
            where calculation_id=v_snapshot.calculation_id and status='succeeded';
        select exists (
            select 1 from derived.stock_max_minute_volume m
            where m.calculation_id=v_snapshot.calculation_id and m.trade_date=p_trade_date
              and m.max_minute_volume_shares is not null and not exists (
                select 1 from audit.quantity_unit_certification q
                where q.ingestion_id=(v_snapshot.input_evidence->'stocks'->m.symbol->>'ingestion_id')::uuid
                  and q.unit_contract_version='stock-quantity.v1'
                  and q.field_group='stock_minute_volume.volume' and q.status='certified'
                  and q.evidence_hash=v_snapshot.input_evidence->'stocks'->m.symbol->>'evidence_hash'
                  and q.raw_hashes @> jsonb_build_array(v_snapshot.input_evidence->'stocks'->m.symbol->>'raw_hash')
              )
        ) into v_revoked;
    end if;
    with requested as (
        select symbol,min(ordinality) request_order
        from unnest(p_symbols) with ordinality input(symbol,ordinality) group by symbol
    ) select jsonb_agg(jsonb_build_object(
        'symbol',r.symbol,'max_minute_volume_shares',case when q.status='certified' then m.max_minute_volume_shares else null end,
        'peak_at',case when q.status='certified' then m.peak_at else null end,'expected_count',coalesce(m.expected_count,240),
        'observed_count',coalesce(m.observed_count,0),
        'quality_status',case when m.max_minute_volume_shares is not null and q.status='certified' then 'complete' else 'missing' end,
        'quality_reasons',case when m.max_minute_volume_shares is not null and q.status is distinct from 'certified'
            then '["quantity_units_unverified"]'::jsonb else coalesce(to_jsonb(m.quality_reasons),
            case when v_snapshot.calculation_id is null then '["not_ready"]'::jsonb else '["not_in_snapshot"]'::jsonb end) end
    ) order by r.request_order) into v_items
    from requested r left join derived.stock_max_minute_volume m
      on m.symbol=r.symbol and m.trade_date=p_trade_date and m.calculation_id=v_snapshot.calculation_id
    left join audit.quantity_unit_certification q
      on q.ingestion_id=(v_snapshot.input_evidence->'stocks'->r.symbol->>'ingestion_id')::uuid
     and q.unit_contract_version='stock-quantity.v1' and q.field_group='stock_minute_volume.volume'
     and q.status='certified'
     and q.evidence_hash=v_snapshot.input_evidence->'stocks'->r.symbol->>'evidence_hash'
     and q.raw_hashes @> jsonb_build_array(v_snapshot.input_evidence->'stocks'->r.symbol->>'raw_hash');
    -- Current publication evidence, rather than a reused calculation's original lineage, gates reads.
    return jsonb_build_object(
        'trade_date',p_trade_date,
        'status',case when v_snapshot.calculation_id is null then 'not_ready'
            when v_snapshot.complete_count=v_snapshot.candidate_count
                 and v_snapshot.input_evidence->>'coverage_scope'='full_market' and not v_revoked then 'ready'
            else 'partial' end,
        'version',v_snapshot.version,'calculation_id',v_snapshot.calculation_id,
        'algorithm_version',v_calculation.algorithm_version,'calculated_at',v_calculation.calculated_at,
        'published_at',v_snapshot.published_at,'items',v_items
    );
end;
$$;
revoke all on function api_v1.query_stock_max_minute_volume(text[],date) from public,anon,authenticated;
grant execute on function api_v1.query_stock_max_minute_volume(text[],date) to authenticated,market_data_api;
comment on function api_v1.query_stock_max_minute_volume(text[],date) is '准确日期只读最大一分钟量摘要，1–500标准股票，单一发布版本，缺失不回退；禁止触发采集。';
