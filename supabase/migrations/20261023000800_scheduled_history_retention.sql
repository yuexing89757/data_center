-- ADR-0070. No historical rows are deleted by this migration.
create table audit.history_retention_catalog (
    table_name text primary key,
    predicate text not null,
    parent_table text,
    parent_join text,
    delete_order integer not null
);
comment on table audit.history_retention_catalog is 'ADR-0070 fixed migration-owned retention scope; no user SQL';
comment on column audit.history_retention_catalog.table_name is 'Qualified internal fact table';
comment on column audit.history_retention_catalog.parent_table is 'Atomic group root or null for roots';
comment on column audit.history_retention_catalog.parent_join is 'Fixed child-to-root SQL join';
comment on column audit.history_retention_catalog.delete_order is 'Child-before-parent ordinal within group';
comment on column audit.history_retention_catalog.predicate is 'Fixed eligibility SQL with bound exclusive Shanghai cutoff';
alter table audit.history_retention_catalog enable row level security;
grant select on audit.history_retention_catalog to market_data_worker, market_data_backup;
create policy history_catalog_read on audit.history_retention_catalog for select to market_data_worker using (true);

create table audit.history_archive (
    archive_id uuid primary key,
    workflow_run_id uuid not null references operations.workflow_run(workflow_run_id),
    cutoff date not null,
    policy_table text not null references audit.history_retention_catalog(table_name),
    object_path text not null unique,
    compressed_sha256 text not null check (compressed_sha256 ~ '^[0-9a-f]{64}$'),
    content_sha256 text not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
    compressed_bytes bigint not null check (compressed_bytes between 1 and 34603008),
    row_count integer not null check (row_count between 1 and 10000),
    payload_hashes jsonb not null check (jsonb_typeof(payload_hashes)='object'),
    archived_at timestamptz not null default now(),
    check (object_path = '_history_archive/' || archive_id::text || '.jsonl.gz')
);
comment on table audit.history_archive is 'ADR-0070 immutable full-row archive receipt committed atomically with exact deletion';
comment on column audit.history_archive.archive_id is 'Immutable archive UUID';
comment on column audit.history_archive.workflow_run_id is 'Owning cleanup workflow';
comment on column audit.history_archive.cutoff is 'Exclusive Shanghai business date boundary';
comment on column audit.history_archive.policy_table is 'Root of atomic parent and children group';
comment on column audit.history_archive.object_path is 'Restricted path relative to Worker Raw root; backup required';
comment on column audit.history_archive.compressed_sha256 is 'SHA256 of verified published gzip bytes';
comment on column audit.history_archive.content_sha256 is 'SHA256 of complete original JSON text and recovery header';
comment on column audit.history_archive.compressed_bytes is 'Compressed file size in bytes';
comment on column audit.history_archive.row_count is 'Total archived and deleted records';
comment on column audit.history_archive.payload_hashes is 'Per-table canonical JSON payload equality hashes; file integrity uses SHA256';
comment on column audit.history_archive.archived_at is 'Archive registration transaction time';
alter table audit.history_archive enable row level security;
grant select, insert on audit.history_archive to market_data_worker;
grant select on audit.history_archive to market_data_backup;
create policy history_archive_read on audit.history_archive for select to market_data_worker using (true);
create policy history_archive_insert on audit.history_archive for insert to market_data_worker with check (
    cutoff <= ((current_timestamp at time zone 'Asia/Shanghai')::date - interval '1 month')::date
);

create function audit.history_database_identity() returns text
language sql security definer set search_path=pg_catalog as $$
    select system_identifier::text || ':' || (select oid::text from pg_database where datname=current_database())
    from pg_control_system()
$$;
revoke all on function audit.history_database_identity() from public;
grant execute on function audit.history_database_identity() to market_data_worker;
comment on function audit.history_database_identity() is 'Internal archive recovery binding to cluster and database identity';

create function audit.history_batch(action text, target text, payload jsonb, cutoff date, archive uuid)
returns bigint language plpgsql security definer set search_path=pg_catalog as $$
declare
    predicate_sql text;
    relation regclass;
    matches text;
    n bigint;
    child record;
    child_matches text;
    referenced boolean;
begin
    select c.predicate into predicate_sql from audit.history_retention_catalog c where c.table_name=target;
    if predicate_sql is null or action not in ('lock','delete') or cutoff is null
       or cutoff > ((current_timestamp at time zone 'Asia/Shanghai')::date - interval '1 month')::date
       or jsonb_typeof(payload) is distinct from 'array'
       or jsonb_array_length(payload) not between 1 and 10000
       or octet_length(payload::text) > 33554432 then
        raise exception 'invalid_history_batch_scope';
    end if;
    relation := target::regclass;
    select string_agg(format('t.%I=p.%I',a.attname,a.attname),' and ' order by k.pos)
      into matches from pg_index i
      cross join lateral unnest(i.indkey) with ordinality k(attnum,pos)
      join pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.attnum
      where i.indrelid=relation and i.indisprimary;
    if matches is null then raise exception 'history_primary_key_required'; end if;
    predicate_sql := replace(predicate_sql, ':cutoff', '$2');
    execute format('select count(*) from (select t.* from %s t
        join jsonb_populate_recordset(null::%s,$1) p on %s
        where to_jsonb(t)=to_jsonb(p) and (%s) for update of t) locked',
        relation,relation,matches,predicate_sql) into n using payload,cutoff;
    if n <> jsonb_array_length(payload) then raise exception 'history_rows_changed_or_protected'; end if;
    if action='lock' then return n; end if;
    if not exists(select 1 from audit.history_archive a where a.archive_id=archive
        and a.cutoff=history_batch.cutoff and a.payload_hashes->>target=md5(payload::text)) then
        raise exception 'history_archive_receipt_required';
    end if;
    -- Check every incoming FK, including CASCADE and SET NULL. Never implicitly
    -- remove or mutate a row that was not itself archived by this transaction.
    for child in select conrelid,conkey,confkey from pg_constraint
        where confrelid=relation and contype='f' and conparentid=0 loop
        select string_agg(format('c.%I=p.%I',a.attname,b.attname),' and ' order by k.pos)
        into child_matches from unnest(child.conkey,child.confkey) with ordinality k(f,p,pos)
        join pg_attribute a on a.attrelid=child.conrelid and a.attnum=k.f
        join pg_attribute b on b.attrelid=relation and b.attnum=k.p;
        execute format('select exists(select 1 from %s c
            join jsonb_populate_recordset(null::%s,$1) p on %s)',
            child.conrelid::regclass,relation,child_matches) into referenced using payload;
        if referenced then raise exception 'history_referenced_rows_remain'; end if;
    end loop;
    execute format('delete from %s t using jsonb_populate_recordset(null::%s,$1) p
        where %s and to_jsonb(t)=to_jsonb(p) and (%s)',
        relation,relation,matches,predicate_sql) using payload,cutoff;
    get diagnostics n = row_count;
    if n <> jsonb_array_length(payload) then raise exception 'history_delete_count_mismatch'; end if;
    return n;
end;
$$;
revoke all on function audit.history_batch(text,text,jsonb,date,uuid) from public;

comment on function audit.history_batch(text,text,jsonb,date,uuid) is 'ADR-0070 bounded exact old-row locks/deletes only; fixed catalog, archive receipt and no implicit FK actions';

create function audit.history_lock(target text, payload jsonb, cutoff date) returns bigint
language sql security definer set search_path=pg_catalog as $$
    select audit.history_batch('lock',target,payload,cutoff,null)
$$;
revoke all on function audit.history_lock(text,jsonb,date) from public;
grant execute on function audit.history_lock(text,jsonb,date) to market_data_worker;
comment on function audit.history_lock(text,jsonb,date) is 'Lock-only bounded history helper; cannot delete or change facts';

create function audit.history_delete_group(root_table text, payloads jsonb, cutoff date, archive uuid)
returns bigint language plpgsql security definer set search_path=pg_catalog as $$
declare item record; n bigint; total bigint := 0; root_rows jsonb; child_rows jsonb; missing bigint;
begin
    if not exists(select 1 from audit.history_retention_catalog c
        where c.table_name=root_table and c.parent_table is null)
        or jsonb_typeof(payloads) is distinct from 'object'
        or octet_length(payloads::text)>33554432 then
        raise exception 'history_group_scope';
    end if;
    root_rows := payloads->root_table;
    if root_rows is null or jsonb_array_length(root_rows) not between 1 and 200
        or (jsonb_array_length(root_rows)>1 and exists(select 1 from audit.history_retention_catalog
            where parent_table=root_table)) then raise exception 'history_group_root_required'; end if;
    if not exists(select 1 from audit.history_archive a where a.archive_id=archive
        and a.policy_table=root_table and a.cutoff=history_delete_group.cutoff) then
        raise exception 'history_group_receipt_required';
    end if;
    if exists(select 1 from jsonb_object_keys(payloads) k where not exists(
        select 1 from audit.history_retention_catalog c where c.table_name=k
        and (c.table_name=root_table or c.parent_table=root_table))) then
        raise exception 'history_group_unexpected_table';
    end if;
    perform audit.history_batch('lock',root_table,root_rows,cutoff,null);
    -- Check complete child membership, not just a caller's list of rows to delete.
    for item in select * from audit.history_retention_catalog
        where parent_table=root_table order by delete_order loop
        child_rows := coalesce(payloads->item.table_name,'[]'::jsonb);
        execute format('select count(*),count(*) filter(where to_jsonb(t) not in
            (select value from jsonb_array_elements($2))) from %s t where exists(
            select 1 from jsonb_populate_recordset(null::%s,$1) p where %s)',
            item.table_name::regclass,root_table::regclass,item.parent_join)
            into n,missing using root_rows,child_rows;
        if n<>jsonb_array_length(child_rows) or missing<>0 then
            raise exception 'history_group_incomplete_children';
        end if;
        if n>0 then perform audit.history_batch('lock',item.table_name,child_rows,cutoff,null); end if;
    end loop;
    for item in select * from audit.history_retention_catalog
        where table_name=root_table or parent_table=root_table order by delete_order loop
        child_rows := coalesce(payloads->item.table_name,'[]'::jsonb);
        if jsonb_array_length(child_rows)>0 then
            total := total + audit.history_batch('delete',item.table_name,child_rows,cutoff,archive);
        end if;
    end loop;
    if total<>(select row_count from audit.history_archive where archive_id=archive) then
        raise exception 'history_group_count_mismatch';
    end if;
    return total;
end;
$$;
revoke all on function audit.history_delete_group(text,jsonb,date,uuid) from public;
grant execute on function audit.history_delete_group(text,jsonb,date,uuid) to market_data_worker;
comment on function audit.history_delete_group(text,jsonb,date,uuid) is 'Complete fixed parent-child unit deletion only; all children archived, parent required, one transaction';

insert into audit.history_retention_catalog(table_name,predicate,parent_table,parent_join,delete_order) values
('realtime.call_auction_indicative_snapshot','t.trade_date < :cutoff',null,null,9999),
('realtime.call_auction_indicative_detail','exists(select 1 from realtime.call_auction_indicative_snapshot p where t.ingestion_id=p.ingestion_id and (p.trade_date < :cutoff))','realtime.call_auction_indicative_snapshot','t.ingestion_id=p.ingestion_id',0),
('realtime.call_auction_market_snapshot','t.trade_date < :cutoff',null,null,9999),
('realtime.call_auction_snapshot','t.trade_date < :cutoff',null,null,9999),
('realtime.eod_quote_snapshot','t.trade_date < :cutoff',null,null,9999),
('realtime.auction_collection_session','t.effective_trade_date < :cutoff and t.finished_at is not null',null,null,9999),
('derived.auction_quote_metric','exists(select 1 from realtime.auction_collection_session p where t.session_id=p.session_id and (p.effective_trade_date < :cutoff and p.finished_at is not null))','realtime.auction_collection_session','t.session_id=p.session_id',0),
('realtime.five_level_quote_snapshot','exists(select 1 from realtime.auction_collection_session p where t.session_id=p.session_id and (p.effective_trade_date < :cutoff and p.finished_at is not null))','realtime.auction_collection_session','t.session_id=p.session_id',1),
('realtime.auction_collection_round','exists(select 1 from realtime.auction_collection_session p where t.session_id=p.session_id and (p.effective_trade_date < :cutoff and p.finished_at is not null))','realtime.auction_collection_session','t.session_id=p.session_id',2),
('realtime.call_auction_market_series_session','t.trade_date < :cutoff and t.finished_at is not null and not exists(select 1 from realtime.call_auction_market_series_snapshot s where s.session_id=t.session_id) and not exists(select 1 from realtime.call_auction_market_series_snapshot_history s where s.session_id=t.session_id)',null,null,9999),
('realtime.call_auction_market_series_round','exists(select 1 from realtime.call_auction_market_series_session p where t.session_id=p.session_id and (p.trade_date < :cutoff and p.finished_at is not null and not exists(select 1 from realtime.call_auction_market_series_snapshot s where s.session_id=p.session_id) and not exists(select 1 from realtime.call_auction_market_series_snapshot_history s where s.session_id=p.session_id)))','realtime.call_auction_market_series_session','t.session_id=p.session_id',0),
('core.stock_technical_factor_snapshot','t.trade_date < :cutoff',null,null,9999),
('core.board_moneyflow','t.trade_date < :cutoff',null,null,9999),
('billboard.dragon_tiger_event','t.trade_date < :cutoff',null,null,9999),
('billboard.seat_trade','exists(select 1 from billboard.dragon_tiger_event p where t.event_id=p.event_id and (p.trade_date < :cutoff))','billboard.dragon_tiger_event','t.event_id=p.event_id',0),
('billboard.trading_seat_profile_daily','t.as_of_date < :cutoff',null,null,9999),
('regulation.status','t.trade_date < :cutoff and not exists(select 1 from regulation.monitor_context m where m.calculation_id=t.calculation_id or m.parent_calculation_id=t.calculation_id)',null,null,9999),
('regulation.warning','exists(select 1 from regulation.status p where t.calculation_id=p.calculation_id and t.symbol=p.symbol and (p.trade_date < :cutoff and not exists(select 1 from regulation.monitor_context m where m.calculation_id=p.calculation_id or m.parent_calculation_id=p.calculation_id)))','regulation.status','t.calculation_id=p.calculation_id and t.symbol=p.symbol',0),
('regulation.rule_result','exists(select 1 from regulation.status p where t.calculation_id=p.calculation_id and t.symbol=p.symbol and (p.trade_date < :cutoff and not exists(select 1 from regulation.monitor_context m where m.calculation_id=p.calculation_id or m.parent_calculation_id=p.calculation_id)))','regulation.status','t.calculation_id=p.calculation_id and t.symbol=p.symbol',1),
('stock_pool.snapshot','t.effective_trade_date < :cutoff and not exists(select 1 from realtime.auction_collection_session a where a.pool_snapshot_id=t.snapshot_id) and not exists(select 1 from realtime.five_level_quote_snapshot a where a.pool_snapshot_id=t.snapshot_id)',null,null,9999),
('stock_pool.member','exists(select 1 from stock_pool.snapshot p where t.snapshot_id=p.snapshot_id and (p.effective_trade_date < :cutoff and not exists(select 1 from realtime.auction_collection_session a where a.pool_snapshot_id=p.snapshot_id) and not exists(select 1 from realtime.five_level_quote_snapshot a where a.pool_snapshot_id=p.snapshot_id)))','stock_pool.snapshot','t.snapshot_id=p.snapshot_id',0),
('stock_pool.calculation_quality','t.created_at < cast(:cutoff as date)::timestamp at time zone ''Asia/Shanghai'' and not exists(select 1 from stock_pool.snapshot s where s.calculation_id=t.calculation_id)',null,null,9999),
('today_limit_up.snapshot','t.trade_date < :cutoff',null,null,9999),
('today_limit_up.member','exists(select 1 from today_limit_up.snapshot p where t.snapshot_id=p.snapshot_id and (p.trade_date < :cutoff))','today_limit_up.snapshot','t.snapshot_id=p.snapshot_id',0),
('today_limit_up.calculation_quality','exists(select 1 from today_limit_up.snapshot p where t.snapshot_id=p.snapshot_id and (p.trade_date < :cutoff))','today_limit_up.snapshot','t.snapshot_id=p.snapshot_id',1),
('today_limit_down.snapshot','t.trade_date < :cutoff',null,null,9999),
('today_limit_down.member','exists(select 1 from today_limit_down.snapshot p where t.snapshot_id=p.snapshot_id and (p.trade_date < :cutoff))','today_limit_down.snapshot','t.snapshot_id=p.snapshot_id',0),
('today_limit_down.calculation_quality','exists(select 1 from today_limit_down.snapshot p where t.snapshot_id=p.snapshot_id and (p.trade_date < :cutoff))','today_limit_down.snapshot','t.snapshot_id=p.snapshot_id',1),
('today_limit_up.source_observation','t.trade_date < :cutoff',null,null,9999),
('today_limit_down.source_observation','t.trade_date < :cutoff',null,null,9999),
('derived.close_price_new_high_120d_snapshot','t.trade_date < :cutoff',null,null,9999),
('derived.close_price_new_high_120d_member','exists(select 1 from derived.close_price_new_high_120d_snapshot p where t.snapshot_id=p.snapshot_id and (p.trade_date < :cutoff))','derived.close_price_new_high_120d_snapshot','t.snapshot_id=p.snapshot_id',0);
