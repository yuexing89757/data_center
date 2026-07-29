create schema if not exists capital;
revoke all on schema capital from public;
grant usage on schema capital to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in ('security', 'trading_calendar', 'daily_bar', 'capital')
);
alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in ('security', 'trading_calendar', 'daily_bar', 'capital')
);

create table capital.share_capital (
    symbol text not null references core.security (symbol),
    effective_date date not null,
    total_shares bigint not null,
    restricted_shares bigint,
    circulating_shares bigint,
    listed_a_shares bigint,
    change_reason text,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, effective_date),
    constraint share_capital_total_positive check (total_shares > 0),
    constraint share_capital_components_valid check (
        (restricted_shares is null or restricted_shares between 0 and total_shares)
        and (circulating_shares is null or circulating_shares between 0 and total_shares)
        and (listed_a_shares is null or listed_a_shares between 0 and total_shares)
    )
);

create table capital.distribution (
    symbol text not null references core.security (symbol),
    report_period date not null,
    announcement_date date,
    record_date date,
    ex_date date,
    cash_dividend_per_share numeric(24, 10),
    bonus_share_ratio numeric(24, 10),
    transfer_share_ratio numeric(24, 10),
    status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, report_period),
    constraint distribution_values_nonnegative check (
        (cash_dividend_per_share is null or cash_dividend_per_share >= 0)
        and (bonus_share_ratio is null or bonus_share_ratio >= 0)
        and (transfer_share_ratio is null or transfer_share_ratio >= 0)
    ),
    constraint distribution_has_value check (
        coalesce(cash_dividend_per_share, 0) > 0
        or coalesce(bonus_share_ratio, 0) > 0
        or coalesce(transfer_share_ratio, 0) > 0
    ),
    constraint distribution_status_check check (
        status in ('planned', 'approved', 'implemented', 'cancelled', 'unknown')
    )
);

create table capital.rights_issue (
    symbol text not null references core.security (symbol),
    record_date date not null,
    announcement_date date,
    ex_date date,
    payment_start_date date,
    payment_end_date date,
    listing_date date,
    rights_ratio numeric(24, 10) not null,
    rights_price numeric(24, 10) not null,
    base_shares bigint,
    proceeds numeric(30, 4),
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, record_date),
    constraint rights_issue_ratio_positive check (rights_ratio > 0),
    constraint rights_issue_price_nonnegative check (rights_price >= 0),
    constraint rights_issue_base_positive check (base_shares is null or base_shares > 0),
    constraint rights_issue_proceeds_nonnegative check (proceeds is null or proceeds >= 0),
    constraint rights_issue_payment_order check (
        payment_start_date is null or payment_end_date is null
        or payment_end_date >= payment_start_date
    )
);

create index share_capital_ingestion_idx on capital.share_capital (ingestion_id);
create index distribution_ex_date_idx on capital.distribution (ex_date, symbol);
create index distribution_ingestion_idx on capital.distribution (ingestion_id);
create index rights_issue_ex_date_idx on capital.rights_issue (ex_date, symbol);
create index rights_issue_ingestion_idx on capital.rights_issue (ingestion_id);

create trigger share_capital_set_updated_at before update on capital.share_capital
for each row execute function ingestion.set_updated_at();
create trigger distribution_set_updated_at before update on capital.distribution
for each row execute function ingestion.set_updated_at();
create trigger rights_issue_set_updated_at before update on capital.rights_issue
for each row execute function ingestion.set_updated_at();

alter table capital.share_capital enable row level security;
alter table capital.distribution enable row level security;
alter table capital.rights_issue enable row level security;
create policy share_capital_worker_all on capital.share_capital
    for all to market_data_worker using (true) with check (true);
create policy distribution_worker_all on capital.distribution
    for all to market_data_worker using (true) with check (true);
create policy rights_issue_worker_all on capital.rights_issue
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on capital.share_capital to market_data_worker;
grant select, insert, update on capital.distribution to market_data_worker;
grant select, insert, update on capital.rights_issue to market_data_worker;

create view api_v1.share_capital as
select symbol, effective_date, total_shares, restricted_shares, circulating_shares,
       listed_a_shares, change_reason
from capital.share_capital;
create view api_v1.distributions as
select symbol, report_period, announcement_date, record_date, ex_date,
       cash_dividend_per_share, bonus_share_ratio, transfer_share_ratio, status
from capital.distribution;
create view api_v1.rights_issues as
select symbol, record_date, announcement_date, ex_date, payment_start_date,
       payment_end_date, listing_date, rights_ratio, rights_price, base_shares, proceeds
from capital.rights_issue;

revoke all on api_v1.share_capital, api_v1.distributions, api_v1.rights_issues from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on api_v1.share_capital, api_v1.distributions, api_v1.rights_issues to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on api_v1.share_capital, api_v1.distributions, api_v1.rights_issues
            to authenticated;
    end if;
end
$$;
