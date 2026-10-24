-- Backup already has BYPASSRLS plus SELECT (ADR-0059); keep internal policies Worker-only.
alter policy raw_purge_read on ingestion.raw_purge to market_data_worker;
alter policy raw_orphan_purge_read on ingestion.raw_orphan_purge to market_data_worker;
