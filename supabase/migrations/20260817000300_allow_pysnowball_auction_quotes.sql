alter table ingestion.ingestion_run drop constraint ingestion_run_provider_check;
alter table ingestion.ingestion_run add constraint ingestion_run_provider_check check (
    provider_code in (
        'baostock','akshare','akshare_ths','pytdx','tushare','pytdx_hq','eastmoney',
        'pysnowball'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_provider_check;

alter table realtime.auction_collection_session
    drop constraint auction_collection_session_provider_code_check;
alter table realtime.auction_collection_session
    add constraint auction_collection_session_provider_code_check check (
        provider_code in ('pytdx_hq','pysnowball')
    ) not valid;
alter table realtime.auction_collection_session
    validate constraint auction_collection_session_provider_code_check;

alter table realtime.five_level_quote_snapshot
    drop constraint five_level_quote_snapshot_source_code_check;
alter table realtime.five_level_quote_snapshot
    add constraint five_level_quote_snapshot_source_code_check check (
        source_code in ('pytdx_hq','pysnowball')
    ) not valid;
alter table realtime.five_level_quote_snapshot
    validate constraint five_level_quote_snapshot_source_code_check;
