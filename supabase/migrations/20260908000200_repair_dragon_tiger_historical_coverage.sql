drop function if exists api_v1.query_dragon_tiger_events_by_date(date, text, integer, integer);
drop function if exists api_v1.query_dragon_tiger_events_by_symbol(
    text, date, date, text, integer, integer
);
drop function if exists api_v1.query_dragon_tiger_trades_by_seat(
    uuid, date, date, integer, integer
);
drop function if exists api_v1.query_dragon_tiger_event_metrics(uuid);
drop function if exists api_v1._dragon_tiger_event_item(uuid);

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','security_bse_daily','dragon_tiger_daily',
        'regulation_daily_calculation','data_cleanup'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

do $$
begin
    if exists (
        select 1
        from billboard.reason_source_alias
        group by source_code, source_reason_code, source_reason_name
        having count(distinct reason_id) > 1
    ) then
        raise exception 'DT_LEGACY_REASON_MAPPING_AMBIGUOUS' using errcode = '23514';
    end if;
    if exists (
        select 1 from billboard.dragon_tiger_event
        where period_type not in ('DAY', 'THREE_DAY')
           or period_start_date is null or period_end_date is null
    ) then
        raise exception 'DT_LEGACY_PERIOD_MAPPING_UNSUPPORTED' using errcode = '23514';
    end if;
    if exists (
        select 1
        from billboard.dragon_tiger_event event
        where event.lhb_buy_amount is null
          and event.lhb_sell_amount is null
          and not exists (
              select 1
              from billboard.seat_trade trade
              where trade.event_id = event.event_id
                and (trade.buy_rank is not null or trade.sell_rank is not null)
          )
    ) then
        raise exception 'DT_LEGACY_DISCLOSURE_MAPPING_UNSUPPORTED' using errcode = '23514';
    end if;
end
$$;

alter table billboard.dragon_tiger_reason
    drop constraint dragon_tiger_reason_period_check;
alter table billboard.dragon_tiger_reason drop column period_type;

delete from billboard.reason_source_alias duplicate
using billboard.reason_source_alias kept
where duplicate.source_code = kept.source_code
  and duplicate.source_reason_code = kept.source_reason_code
  and duplicate.source_reason_name = kept.source_reason_name
  and duplicate.ctid > kept.ctid;

alter table billboard.reason_source_alias drop constraint reason_source_alias_pkey;
alter table billboard.reason_source_alias drop constraint reason_source_alias_period_check;
alter table billboard.reason_source_alias drop column period_type;
alter table billboard.reason_source_alias add primary key (
    source_code, source_reason_code, source_reason_name
);

create table billboard.trading_seat_source_identity (
    identity_id uuid primary key default extensions.gen_random_uuid(),
    seat_id uuid not null references billboard.trading_seat (seat_id) on delete cascade,
    source_code text not null,
    source_seat_key text not null,
    first_seen_date date not null,
    last_seen_date date not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint trading_seat_source_identity_unique unique (source_code, source_seat_key),
    constraint trading_seat_source_identity_source_check
        check (source_code in ('eastmoney', 'tushare')),
    constraint trading_seat_source_identity_nonblank_check
        check (btrim(source_code) <> '' and btrim(source_seat_key) <> ''),
    constraint trading_seat_source_identity_date_check
        check (first_seen_date <= last_seen_date)
);

create trigger trading_seat_source_identity_set_updated_at
before update on billboard.trading_seat_source_identity
for each row execute function ingestion.set_updated_at();

insert into billboard.trading_seat_source_identity (
    seat_id, source_code, source_seat_key, first_seen_date, last_seen_date
)
select alias.seat_id, alias.source_code, alias.source_seat_key,
       seat.first_seen_date, seat.last_seen_date
from billboard.trading_seat_alias alias
join billboard.trading_seat seat on seat.seat_id = alias.seat_id
where alias.source_seat_key is not null;

update billboard.seat_trade
set seat_id = null
where seat_source_key is null;

alter table billboard.trading_seat_alias
    add column identity_id uuid,
    add column first_seen_date date,
    add column last_seen_date date;

update billboard.trading_seat_alias alias
set identity_id = identity.identity_id,
    first_seen_date = identity.first_seen_date,
    last_seen_date = identity.last_seen_date
from billboard.trading_seat_source_identity identity
where alias.source_code = identity.source_code
  and alias.source_seat_key = identity.source_seat_key;

delete from billboard.trading_seat_alias where identity_id is null;

drop index billboard.trading_seat_alias_source_key_unique;
alter table billboard.trading_seat_alias
    drop constraint trading_seat_alias_source_name_unique,
    drop constraint trading_seat_alias_source_check,
    drop constraint trading_seat_alias_nonblank_check,
    drop column seat_id,
    drop column source_code,
    drop column source_seat_key,
    alter column identity_id set not null,
    alter column first_seen_date set not null,
    alter column last_seen_date set not null,
    add constraint trading_seat_alias_identity_fk foreign key (identity_id)
        references billboard.trading_seat_source_identity (identity_id) on delete cascade,
    add constraint trading_seat_alias_identity_name_unique unique (identity_id, alias_name),
    add constraint trading_seat_alias_nonblank_check check (btrim(alias_name) <> ''),
    add constraint trading_seat_alias_date_check check (first_seen_date <= last_seen_date);

drop index billboard.dragon_tiger_event_date_symbol_idx;
drop index billboard.dragon_tiger_event_symbol_date_idx;
alter table billboard.dragon_tiger_event
    drop constraint dragon_tiger_event_semantic_unique,
    drop constraint dragon_tiger_event_period_type_check,
    drop constraint dragon_tiger_event_period_check,
    add column trigger_window_basis text,
    add column trigger_window_sessions integer,
    add column trigger_occurrence_count integer,
    add column trigger_start_date date,
    add column trigger_end_date date,
    add column amount_period_basis text,
    add column amount_period_sessions integer,
    add column amount_period_start_date date,
    add column amount_period_end_date date,
    add column buy_disclosure_present boolean,
    add column sell_disclosure_present boolean;

update billboard.dragon_tiger_event
set trigger_window_basis = 'MARKET_SESSIONS',
    trigger_window_sessions = case period_type when 'DAY' then 1 else 3 end,
    trigger_occurrence_count = null,
    trigger_start_date = period_start_date,
    trigger_end_date = period_end_date,
    amount_period_basis = 'SOURCE_UNSPECIFIED',
    amount_period_sessions = null,
    amount_period_start_date = null,
    amount_period_end_date = null,
    buy_disclosure_present = (
        lhb_buy_amount is not null or exists (
            select 1 from billboard.seat_trade trade
            where trade.event_id = billboard.dragon_tiger_event.event_id
              and trade.buy_rank is not null
        )
    ),
    sell_disclosure_present = (
        lhb_sell_amount is not null or exists (
            select 1 from billboard.seat_trade trade
            where trade.event_id = billboard.dragon_tiger_event.event_id
              and trade.sell_rank is not null
        )
    );

alter table billboard.dragon_tiger_event
    alter column trigger_window_basis set not null,
    alter column trigger_window_sessions set not null,
    alter column trigger_end_date set not null,
    alter column amount_period_basis set not null,
    alter column buy_disclosure_present set not null,
    alter column sell_disclosure_present set not null,
    drop column period_type,
    drop column period_start_date,
    drop column period_end_date,
    add constraint dragon_tiger_event_trigger_basis_check check (
        trigger_window_basis in ('MARKET_SESSIONS', 'SECURITY_TRADED_SESSIONS')
    ),
    add constraint dragon_tiger_event_trigger_window_check check (
        trigger_window_sessions > 0
        and (trigger_occurrence_count is null or trigger_occurrence_count > 0)
        and trigger_end_date = trade_date
        and (trigger_start_date is null or trigger_start_date <= trigger_end_date)
    ),
    add constraint dragon_tiger_event_amount_basis_check check (
        amount_period_basis in (
            'MARKET_SESSIONS', 'SECURITY_TRADED_SESSIONS', 'SOURCE_UNSPECIFIED'
        )
    ),
    add constraint dragon_tiger_event_amount_period_check check (
        (
            amount_period_basis = 'SOURCE_UNSPECIFIED'
            and amount_period_sessions is null
            and amount_period_start_date is null
            and amount_period_end_date is null
        ) or (
            amount_period_basis in ('MARKET_SESSIONS', 'SECURITY_TRADED_SESSIONS')
            and amount_period_sessions > 0
            and amount_period_end_date = trade_date
            and (
                amount_period_start_date is null
                or amount_period_start_date <= amount_period_end_date
            )
        )
    ),
    add constraint dragon_tiger_event_disclosure_check check (
        buy_disclosure_present or sell_disclosure_present
    );

create unique index dragon_tiger_event_semantic_unique_v2
on billboard.dragon_tiger_event (
    source_code, symbol, trade_date, trigger_window_basis,
    trigger_window_sessions, coalesce(trigger_occurrence_count, 0), reason_id
);
create index dragon_tiger_event_date_symbol_idx
    on billboard.dragon_tiger_event (
        trade_date, trigger_window_basis, trigger_window_sessions, symbol, event_id
    );
create index dragon_tiger_event_symbol_date_idx
    on billboard.dragon_tiger_event (
        symbol, trade_date desc, trigger_window_basis, trigger_window_sessions, event_id
    );

alter table billboard.trading_seat_source_identity enable row level security;
create policy trading_seat_source_identity_worker_all
on billboard.trading_seat_source_identity
for all to market_data_worker using (true) with check (true);

grant select, insert, update on billboard.trading_seat_source_identity to market_data_worker;
revoke all on billboard.trading_seat_source_identity from public, anon, authenticated;

comment on table billboard.trading_seat_source_identity is
    'Stable provider seat identity; names are temporal observations in trading_seat_alias.';

create function api_v1._dragon_tiger_quality_codes(
    p_ingestion_id uuid,
    p_source_record_id text
) returns jsonb
language sql stable security definer
set search_path = pg_catalog, audit
set statement_timeout = '5s'
as $$
    select coalesce(
        jsonb_agg(distinct quality.rule_code order by quality.rule_code),
        '[]'::jsonb
    )
    from audit.quality_result quality
    where quality.ingestion_id = p_ingestion_id
      and quality.dataset_code = 'dragon_tiger'
      and quality.status = 'failed'
      and left(quality.rule_code, 3) = 'DT_'
      and quality.natural_key ->> 'source_event_id' = p_source_record_id
$$;

create function api_v1._dragon_tiger_event_item(p_event_id uuid) returns jsonb
language sql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
    select jsonb_build_object(
        'event_id', event.event_id,
        'symbol', event.symbol,
        'trade_date', event.trade_date,
        'trigger_window_basis', event.trigger_window_basis,
        'trigger_window_sessions', event.trigger_window_sessions,
        'trigger_occurrence_count', event.trigger_occurrence_count,
        'trigger_start_date', event.trigger_start_date,
        'trigger_end_date', event.trigger_end_date,
        'amount_period_basis', event.amount_period_basis,
        'amount_period_sessions', event.amount_period_sessions,
        'amount_period_start_date', event.amount_period_start_date,
        'amount_period_end_date', event.amount_period_end_date,
        'reason_code', reason.reason_code,
        'reason_name', reason.reason_name,
        'reason_type', reason.reason_type,
        'reason_name_raw', event.reason_name_raw,
        'close_price', event.close_price::text,
        'change_pct', event.change_pct::text,
        'turnover_amount', event.turnover_amount::text,
        'turnover_rate', event.turnover_rate::text,
        'amplitude', event.amplitude::text,
        'lhb_buy_amount', event.lhb_buy_amount::text,
        'lhb_sell_amount', event.lhb_sell_amount::text,
        'net_amount', case
            when event.lhb_buy_amount is not null and event.lhb_sell_amount is not null
            then (event.lhb_buy_amount - event.lhb_sell_amount)::text
            else null
        end,
        'buy_disclosure_present', event.buy_disclosure_present,
        'sell_disclosure_present', event.sell_disclosure_present,
        'data_quality_codes', api_v1._dragon_tiger_quality_codes(
            event.ingestion_id, event.source_record_id
        ),
        'source_code', event.source_code,
        'source_record_id', event.source_record_id,
        'seat_trades', coalesce((
            select jsonb_agg(jsonb_build_object(
                'seat_trade_id', trade.seat_trade_id,
                'seat_id', trade.seat_id,
                'seat_name_raw', trade.seat_name_raw,
                'buy_amount', trade.buy_amount::text,
                'sell_amount', trade.sell_amount::text,
                'net_amount', case
                    when trade.buy_amount is not null and trade.sell_amount is not null
                    then (trade.buy_amount - trade.sell_amount)::text
                    else null
                end,
                'buy_rank', trade.buy_rank,
                'sell_rank', trade.sell_rank,
                'is_institution', trade.is_institution,
                'is_northbound', trade.is_northbound
            ) order by coalesce(trade.buy_rank, 99), coalesce(trade.sell_rank, 99),
                trade.seat_trade_id)
            from billboard.seat_trade trade where trade.event_id = event.event_id
        ), '[]'::jsonb)
    )
    from billboard.dragon_tiger_event event
    join billboard.dragon_tiger_reason reason on reason.reason_id = event.reason_id
    where event.event_id = p_event_id
$$;

create function api_v1.query_dragon_tiger_events_by_date(
    p_trade_date date,
    p_trigger_window_basis text default null,
    p_trigger_window_sessions integer default null,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_trade_date is null
       or (p_trigger_window_basis is not null and p_trigger_window_basis not in (
           'MARKET_SESSIONS', 'SECURITY_TRADED_SESSIONS'
       ))
       or (p_trigger_window_sessions is not null and p_trigger_window_sessions < 1)
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger date query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count
    from billboard.dragon_tiger_event event
    where event.trade_date = p_trade_date
      and (p_trigger_window_basis is null
           or event.trigger_window_basis = p_trigger_window_basis)
      and (p_trigger_window_sessions is null
           or event.trigger_window_sessions = p_trigger_window_sessions);
    select coalesce(jsonb_agg(api_v1._dragon_tiger_event_item(selected.event_id)
        order by selected.symbol, selected.trigger_window_basis,
            selected.trigger_window_sessions, selected.event_id), '[]'::jsonb)
    into items from (
        select event.event_id, event.symbol, event.trigger_window_basis,
            event.trigger_window_sessions
        from billboard.dragon_tiger_event event
        where event.trade_date = p_trade_date
          and (p_trigger_window_basis is null
               or event.trigger_window_basis = p_trigger_window_basis)
          and (p_trigger_window_sessions is null
               or event.trigger_window_sessions = p_trigger_window_sessions)
        order by event.symbol, event.trigger_window_basis,
            event.trigger_window_sessions, event.event_id
        offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_events_by_symbol(
    p_symbol text,
    p_start_date date,
    p_end_date date,
    p_trigger_window_basis text default null,
    p_trigger_window_sessions integer default null,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_symbol is null or p_symbol !~ '^(SSE|SZSE|BSE):[0-9]{6}$'
       or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or (p_trigger_window_basis is not null and p_trigger_window_basis not in (
           'MARKET_SESSIONS', 'SECURITY_TRADED_SESSIONS'
       ))
       or (p_trigger_window_sessions is not null and p_trigger_window_sessions < 1)
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger symbol query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count
    from billboard.dragon_tiger_event event
    where event.symbol = p_symbol
      and event.trade_date between p_start_date and p_end_date
      and (p_trigger_window_basis is null
           or event.trigger_window_basis = p_trigger_window_basis)
      and (p_trigger_window_sessions is null
           or event.trigger_window_sessions = p_trigger_window_sessions);
    select coalesce(jsonb_agg(api_v1._dragon_tiger_event_item(selected.event_id)
        order by selected.trade_date desc, selected.trigger_window_basis,
            selected.trigger_window_sessions, selected.event_id), '[]'::jsonb)
    into items from (
        select event.event_id, event.trade_date, event.trigger_window_basis,
            event.trigger_window_sessions
        from billboard.dragon_tiger_event event
        where event.symbol = p_symbol
          and event.trade_date between p_start_date and p_end_date
          and (p_trigger_window_basis is null
               or event.trigger_window_basis = p_trigger_window_basis)
          and (p_trigger_window_sessions is null
               or event.trigger_window_sessions = p_trigger_window_sessions)
        order by event.trade_date desc, event.trigger_window_basis,
            event.trigger_window_sessions, event.event_id
        offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_trades_by_seat(
    p_seat_id uuid,
    p_start_date date,
    p_end_date date,
    p_limit integer default 100,
    p_offset integer default 0
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare items jsonb; total_count integer;
begin
    if p_seat_id is null or p_start_date is null or p_end_date is null
       or p_start_date > p_end_date or p_end_date - p_start_date > 365
       or p_limit is null or p_limit < 1 or p_limit > 500
       or p_offset is null or p_offset < 0 or p_offset > 10000 then
        raise exception 'invalid DragonTiger seat query boundary' using errcode = '22023';
    end if;
    select count(*)::integer into total_count from billboard.seat_trade trade
    where trade.seat_id = p_seat_id
      and trade.trade_date between p_start_date and p_end_date;
    select coalesce(jsonb_agg(selected.item
        order by selected.trade_date desc, selected.event_id), '[]'::jsonb)
    into items from (
        select trade.trade_date, trade.event_id, jsonb_build_object(
            'event_id', trade.event_id, 'seat_trade_id', trade.seat_trade_id,
            'seat_id', trade.seat_id, 'symbol', trade.symbol,
            'trade_date', trade.trade_date, 'seat_name_raw', trade.seat_name_raw,
            'buy_amount', trade.buy_amount::text, 'sell_amount', trade.sell_amount::text,
            'net_amount', case
                when trade.buy_amount is not null and trade.sell_amount is not null
                then (trade.buy_amount - trade.sell_amount)::text else null end,
            'buy_rank', trade.buy_rank, 'sell_rank', trade.sell_rank,
            'is_institution', trade.is_institution, 'is_northbound', trade.is_northbound,
            'trigger_window_basis', event.trigger_window_basis,
            'trigger_window_sessions', event.trigger_window_sessions,
            'trigger_occurrence_count', event.trigger_occurrence_count,
            'trigger_start_date', event.trigger_start_date,
            'trigger_end_date', event.trigger_end_date,
            'amount_period_basis', event.amount_period_basis,
            'amount_period_sessions', event.amount_period_sessions,
            'amount_period_start_date', event.amount_period_start_date,
            'amount_period_end_date', event.amount_period_end_date,
            'buy_disclosure_present', event.buy_disclosure_present,
            'sell_disclosure_present', event.sell_disclosure_present,
            'data_quality_codes', api_v1._dragon_tiger_quality_codes(
                event.ingestion_id, event.source_record_id
            ),
            'reason_code', reason.reason_code, 'reason_name', reason.reason_name
        ) item
        from billboard.seat_trade trade
        join billboard.dragon_tiger_event event on event.event_id = trade.event_id
        join billboard.dragon_tiger_reason reason on reason.reason_id = event.reason_id
        where trade.seat_id = p_seat_id
          and trade.trade_date between p_start_date and p_end_date
        order by trade.trade_date desc, trade.event_id
        offset p_offset limit p_limit
    ) selected;
    return jsonb_build_object('items',items,'returned_count',jsonb_array_length(items),
        'total_count',total_count,'has_more',p_offset + jsonb_array_length(items) < total_count,
        'limit',p_limit,'offset',p_offset);
end
$$;

create function api_v1.query_dragon_tiger_event_metrics(p_event_id uuid) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, billboard
set statement_timeout = '5s'
as $$
declare event_row billboard.dragon_tiger_event%rowtype;
begin
    if p_event_id is null then
        raise exception 'invalid DragonTiger event id' using errcode = '22023';
    end if;
    select * into event_row
    from billboard.dragon_tiger_event where event_id = p_event_id;
    if not found then return null; end if;
    return jsonb_build_object(
        'event_id', event_row.event_id,
        'net_amount', case
            when event_row.lhb_buy_amount is not null and event_row.lhb_sell_amount is not null
            then (event_row.lhb_buy_amount - event_row.lhb_sell_amount)::text else null end,
        'net_buy_strength', case
            when event_row.turnover_amount is not null and event_row.turnover_amount <> 0
             and event_row.lhb_buy_amount is not null and event_row.lhb_sell_amount is not null
            then ((event_row.lhb_buy_amount - event_row.lhb_sell_amount)
                / event_row.turnover_amount)::text else null end,
        'buy_seat_count', (select count(*) from billboard.seat_trade
            where event_id=p_event_id and buy_rank is not null),
        'sell_seat_count', (select count(*) from billboard.seat_trade
            where event_id=p_event_id and sell_rank is not null),
        'pure_buy_seat_count', (select count(*) from billboard.seat_trade
            where event_id=p_event_id and buy_amount > 0 and sell_amount = 0),
        'pure_sell_seat_count', (select count(*) from billboard.seat_trade
            where event_id=p_event_id and sell_amount > 0 and buy_amount = 0),
        'buy_sell_overlap_count', (select count(*) from billboard.seat_trade
            where event_id=p_event_id and buy_rank is not null and sell_rank is not null),
        'top1_buy_concentration', (select case when event_row.lhb_buy_amount is null
            or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null
            else (sum(buy_amount) / event_row.lhb_buy_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and buy_rank<=1),
        'top3_buy_concentration', (select case when event_row.lhb_buy_amount is null
            or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null
            else (sum(buy_amount) / event_row.lhb_buy_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and buy_rank<=3),
        'top5_buy_concentration', (select case when event_row.lhb_buy_amount is null
            or event_row.lhb_buy_amount=0 or count(*)<>count(buy_amount) then null
            else (sum(buy_amount) / event_row.lhb_buy_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and buy_rank<=5),
        'top1_sell_concentration', (select case when event_row.lhb_sell_amount is null
            or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null
            else (sum(sell_amount) / event_row.lhb_sell_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and sell_rank<=1),
        'top3_sell_concentration', (select case when event_row.lhb_sell_amount is null
            or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null
            else (sum(sell_amount) / event_row.lhb_sell_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and sell_rank<=3),
        'top5_sell_concentration', (select case when event_row.lhb_sell_amount is null
            or event_row.lhb_sell_amount=0 or count(*)<>count(sell_amount) then null
            else (sum(sell_amount) / event_row.lhb_sell_amount)::text end
            from billboard.seat_trade where event_id=p_event_id and sell_rank<=5),
        'institution_buy_amount', (select case when count(*)=0
            or count(*)<>count(buy_amount) then null else sum(buy_amount)::text end
            from billboard.seat_trade where event_id=p_event_id
              and is_institution and buy_rank is not null),
        'institution_sell_amount', (select case when count(*)=0
            or count(*)<>count(sell_amount) then null else sum(sell_amount)::text end
            from billboard.seat_trade where event_id=p_event_id
              and is_institution and sell_rank is not null),
        'institution_net_amount', case
            when (select count(*)=0 or count(*)<>count(buy_amount)
                from billboard.seat_trade where event_id=p_event_id
                  and is_institution and buy_rank is not null)
              or (select count(*)=0 or count(*)<>count(sell_amount)
                from billboard.seat_trade where event_id=p_event_id
                  and is_institution and sell_rank is not null)
            then null else (
                (select sum(buy_amount) from billboard.seat_trade
                    where event_id=p_event_id and is_institution and buy_rank is not null)
                - (select sum(sell_amount) from billboard.seat_trade
                    where event_id=p_event_id and is_institution and sell_rank is not null)
            )::text end,
        'northbound_buy_amount', (select case when count(*)=0
            or count(*)<>count(buy_amount) then null else sum(buy_amount)::text end
            from billboard.seat_trade where event_id=p_event_id
              and is_northbound and buy_rank is not null),
        'northbound_sell_amount', (select case when count(*)=0
            or count(*)<>count(sell_amount) then null else sum(sell_amount)::text end
            from billboard.seat_trade where event_id=p_event_id
              and is_northbound and sell_rank is not null),
        'northbound_net_amount', case
            when (select count(*)=0 or count(*)<>count(buy_amount)
                from billboard.seat_trade where event_id=p_event_id
                  and is_northbound and buy_rank is not null)
              or (select count(*)=0 or count(*)<>count(sell_amount)
                from billboard.seat_trade where event_id=p_event_id
                  and is_northbound and sell_rank is not null)
            then null else (
                (select sum(buy_amount) from billboard.seat_trade
                    where event_id=p_event_id and is_northbound and buy_rank is not null)
                - (select sum(sell_amount) from billboard.seat_trade
                    where event_id=p_event_id and is_northbound and sell_rank is not null)
            )::text end,
        'buy_disclosure_present', event_row.buy_disclosure_present,
        'sell_disclosure_present', event_row.sell_disclosure_present,
        'amount_period_verified', event_row.amount_period_basis <> 'SOURCE_UNSPECIFIED',
        'data_quality_codes', api_v1._dragon_tiger_quality_codes(
            event_row.ingestion_id, event_row.source_record_id
        )
    );
end
$$;

revoke all on function api_v1._dragon_tiger_quality_codes(uuid,text)
    from public, anon, authenticated;
revoke all on function api_v1._dragon_tiger_event_item(uuid)
    from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_events_by_date(
    date,text,integer,integer,integer
) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_events_by_symbol(
    text,date,date,text,integer,integer,integer
) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_trades_by_seat(
    uuid,date,date,integer,integer
) from public, anon, authenticated;
revoke all on function api_v1.query_dragon_tiger_event_metrics(uuid)
    from public, anon, authenticated;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        execute 'grant execute on function api_v1.query_dragon_tiger_events_by_date(date,text,integer,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_events_by_symbol(text,date,date,text,integer,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_trades_by_seat(uuid,date,date,integer,integer) to market_data_api';
        execute 'grant execute on function api_v1.query_dragon_tiger_event_metrics(uuid) to market_data_api';
    end if;
end
$$;
