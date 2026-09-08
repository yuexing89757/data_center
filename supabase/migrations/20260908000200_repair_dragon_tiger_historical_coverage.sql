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
    amount_period_basis = 'MARKET_SESSIONS',
    amount_period_sessions = case period_type when 'DAY' then 1 else 3 end,
    amount_period_start_date = period_start_date,
    amount_period_end_date = period_end_date,
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
