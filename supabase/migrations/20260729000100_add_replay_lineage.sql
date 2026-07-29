alter table ingestion.ingestion_run
    add column replayed_from_raw_id uuid
        references ingestion.raw_manifest (raw_id);

create index ingestion_run_replayed_from_raw_idx
    on ingestion.ingestion_run (replayed_from_raw_id)
    where replayed_from_raw_id is not null;

comment on column ingestion.ingestion_run.replayed_from_raw_id is
    'Original immutable Raw object used by a replay run; null for live ingestion.';
