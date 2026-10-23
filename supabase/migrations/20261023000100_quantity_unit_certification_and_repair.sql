-- ADR-0068: restricted evidence and repair ledgers; no market facts are changed here.
create table audit.quantity_unit_certification (
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    unit_contract_version text not null default 'stock-quantity.v1',
    field_group text not null,
    status text not null check (status in ('certified', 'quarantined')),
    raw_hashes jsonb not null check (jsonb_typeof(raw_hashes) = 'array'),
    normalizer_version text not null check (btrim(normalizer_version) <> ''),
    evidence_hash text not null check (evidence_hash ~ '^[0-9a-f]{64}$'),
    certified_at timestamptz not null default now(),
    primary key (ingestion_id, unit_contract_version, field_group),
    check (btrim(unit_contract_version) <> '' and btrim(field_group) <> ''),
    check (status <> 'certified' or jsonb_array_length(raw_hashes) > 0)
);

create table audit.quantity_unit_repair_run (
    repair_run_id uuid primary key,
    plan_hash text not null unique check (plan_hash ~ '^[0-9a-f]{64}$'),
    rule_version text not null,
    plan jsonb not null check (jsonb_typeof(plan) = 'object'),
    status text not null default 'running'
        check (status in ('running', 'completed', 'conflict', 'rolled_back')),
    next_item bigint not null default 0 check (next_item >= 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table audit.quantity_unit_repair_item (
    repair_run_id uuid not null references audit.quantity_unit_repair_run (repair_run_id),
    item_key text not null check (item_key ~ '^[0-9a-f]{64}$'),
    rule_version text not null,
    table_name text not null default 'core.daily_bar' check (table_name = 'core.daily_bar'),
    field_name text not null default 'volume' check (field_name = 'volume'),
    symbol text not null,
    trade_date date not null,
    original_ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    raw_id uuid not null references ingestion.raw_manifest (raw_id),
    raw_hash text not null check (raw_hash ~ '^[0-9a-f]{64}$'),
    before_state jsonb not null,
    after_state jsonb,
    before_hash text not null check (before_hash ~ '^[0-9a-f]{64}$'),
    after_hash text check (after_hash ~ '^[0-9a-f]{64}$'),
    new_ingestion_id uuid references ingestion.ingestion_run (ingestion_id),
    status text not null check (
        status in ('applied', 'already_applied', 'conflict', 'rolled_back', 'rollback_conflict')
    ),
    reason text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (repair_run_id, item_key),
    unique (repair_run_id, rule_version, table_name, field_name, symbol, trade_date,
            original_ingestion_id, raw_hash, before_hash),
    check (status not in ('applied', 'already_applied', 'rolled_back', 'rollback_conflict')
           or (after_state is not null and after_hash is not null and new_ingestion_id is not null))
);
create index quantity_unit_repair_item_identity_idx on audit.quantity_unit_repair_item (item_key);

alter table audit.quantity_unit_certification enable row level security;
alter table audit.quantity_unit_repair_run enable row level security;
alter table audit.quantity_unit_repair_item enable row level security;
revoke all on audit.quantity_unit_certification, audit.quantity_unit_repair_run,
    audit.quantity_unit_repair_item from public, market_data_api;
grant select, insert, update on audit.quantity_unit_certification,
    audit.quantity_unit_repair_run, audit.quantity_unit_repair_item to market_data_worker;
grant select on audit.quantity_unit_certification, audit.quantity_unit_repair_run,
    audit.quantity_unit_repair_item to market_data_backup;
create policy quantity_unit_certification_worker on audit.quantity_unit_certification
    for all to market_data_worker using (true) with check (true);
create policy quantity_unit_repair_run_worker on audit.quantity_unit_repair_run
    for all to market_data_worker using (true) with check (true);
create policy quantity_unit_repair_item_worker on audit.quantity_unit_repair_item
    for all to market_data_worker using (true) with check (true);

comment on table audit.quantity_unit_certification is
    '按采集批次和业务字段组保存数量单位认证；不能从数值量级或字段名称推断单位。';
comment on table audit.quantity_unit_repair_run is
    '受控历史股数量修复计划与断点；计划默认预览，写入必须核对目标库和计划hash。';
comment on table audit.quantity_unit_repair_item is
    '股票日K成交股数修复的不可丢失审计依据；保留前后记录、Raw证明和回滚所需血缘，不修改Raw。';
do $comments$
declare item record;
begin
    for item in select * from (values
        ('quantity_unit_certification','ingestion_id','被认证标准事实的采集批次UUID。'),
        ('quantity_unit_certification','unit_contract_version','数量业务单位契约版本；股票股数契约为stock-quantity.v1。'),
        ('quantity_unit_certification','field_group','独立业务字段组；债券张与股票股使用不同字段组。'),
        ('quantity_unit_certification','status','认证状态：certified已证明，quarantined隔离。'),
        ('quantity_unit_certification','raw_hashes','证明所绑定不可变Raw对象SHA256集合，不含对象内容或路径。'),
        ('quantity_unit_certification','normalizer_version','实际执行的精确标准化实现版本。'),
        ('quantity_unit_certification','evidence_hash','采集身份、来源单位及Raw证据的SHA256摘要。'),
        ('quantity_unit_certification','certified_at','认证事务记录时间，带时区。'),
        ('quantity_unit_repair_run','repair_run_id','修复计划执行UUID；重试沿用同一封存计划。'),
        ('quantity_unit_repair_run','plan_hash','封存预览计划的SHA256，唯一且绑定目标数据库身份。'),
        ('quantity_unit_repair_run','rule_version','历史修复规则版本，不根据数值大小选规则。'),
        ('quantity_unit_repair_run','plan','私有修复计划，含精确范围与前后指纹；禁止对外提供。'),
        ('quantity_unit_repair_run','status','执行状态：running、completed、conflict或rolled_back。'),
        ('quantity_unit_repair_run','next_item','已提交计划的恢复断点，非行情数量。'),
        ('quantity_unit_repair_run','created_at','计划首次落账时间，带时区。'),
        ('quantity_unit_repair_run','updated_at','计划最后事务更新时间，带时区。'),
        ('quantity_unit_repair_item','repair_run_id','所属受控修复计划UUID。'),
        ('quantity_unit_repair_item','item_key','绑定规则、自然键、原血缘、Raw及修复前指纹的幂等键。'),
        ('quantity_unit_repair_item','rule_version','本条执行的历史数量修复规则版本。'),
        ('quantity_unit_repair_item','table_name','允许修复的精确表名，当前仅core.daily_bar。'),
        ('quantity_unit_repair_item','field_name','允许修复的字段，当前仅volume，目标单位股。'),
        ('quantity_unit_repair_item','symbol','股票标准证券代码，保留前导零。'),
        ('quantity_unit_repair_item','trade_date','股票日K自然键的交易所本地交易日。'),
        ('quantity_unit_repair_item','original_ingestion_id','修复前事实采集UUID；旧快照隔离不能被后续认证绕过。'),
        ('quantity_unit_repair_item','raw_id','不可变来源Raw manifest UUID。'),
        ('quantity_unit_repair_item','raw_hash','执行前重新校验的Raw内容SHA256。'),
        ('quantity_unit_repair_item','before_state','完整修复前日K及血缘，保留原错误单位数值作为回滚依据。'),
        ('quantity_unit_repair_item','after_state','从经验证Raw重新标准化的目标日K及新血缘；volume为股，amount为元。'),
        ('quantity_unit_repair_item','before_hash','完整修复前状态SHA256；用于并发CAS和回滚审计。'),
        ('quantity_unit_repair_item','after_hash','完整修复后状态SHA256；防止回滚覆盖后续事实。'),
        ('quantity_unit_repair_item','new_ingestion_id','修复产生且关联原Raw的新回放采集UUID。'),
        ('quantity_unit_repair_item','status','逐条执行、已应用、冲突或回滚状态；重复执行不得重复换算。'),
        ('quantity_unit_repair_item','reason','明确冲突或隔离原因，不含凭据和Raw路径。'),
        ('quantity_unit_repair_item','created_at','审计首次落账时间，带时区。'),
        ('quantity_unit_repair_item','updated_at','逐条审计最后事务更新时间，带时区。')
    ) comments(table_name,column_name,description)
    loop
        execute format('comment on column audit.%I.%I is %L',
            item.table_name,item.column_name,item.description);
    end loop;
end;
$comments$;
