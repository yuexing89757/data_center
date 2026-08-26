alter table core.shareholder_count
    drop constraint shareholder_count_date_order;

alter table core.shareholder_count
    add constraint shareholder_count_announcement_lag_check check (
        announcement_date >= statistics_date - 2
    ) not valid;

alter table core.shareholder_count
    validate constraint shareholder_count_announcement_lag_check;
