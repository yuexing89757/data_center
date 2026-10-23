-- ADR-0069: bound foreign-key checks when retiring old auction rounds/sessions.
-- Date-leading indexes cannot serve RI checks keyed only by session and sequence.
create index call_auction_series_history_round_ref_idx
    on realtime.call_auction_market_series_snapshot_history(session_id, sample_seq);
create index call_auction_series_snapshot_round_ref_idx
    on realtime.call_auction_market_series_snapshot(session_id, sample_seq);
comment on index realtime.call_auction_series_history_round_ref_idx is 'Supports exact auction round FK checks across retained history partitions';
comment on index realtime.call_auction_series_snapshot_round_ref_idx is 'Supports exact auction round FK checks across current snapshot partitions';
