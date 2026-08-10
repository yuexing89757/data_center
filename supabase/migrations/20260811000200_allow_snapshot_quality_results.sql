-- Keep audit quality dataset codes aligned with the snapshot ingestion datasets
-- introduced by 20260810000200.

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot'
    )
);
