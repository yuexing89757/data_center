alter table realtime.call_auction_market_series_snapshot
    drop constraint call_auction_market_series_snapshot_check,
    drop constraint call_auction_market_series_snapshot_check2;

alter table realtime.call_auction_market_series_snapshot
    add constraint call_auction_market_series_snapshot_schedule_check
    check (
        scheduled_at = case
            when trade_date >= date '2026-09-07' and sample_seq = 30 then
                (trade_date + time '09:24:53') at time zone 'Asia/Shanghai'
            else
                (trade_date + time '09:15:00') at time zone 'Asia/Shanghai'
                + make_interval(secs => sample_seq * 20)
        end
    ) not valid,
    add constraint call_auction_market_series_snapshot_observation_window_check
    check (
        observed_at >= scheduled_at
        and observed_at < case
            when trade_date >= date '2026-09-07' and sample_seq = 30 then
                (trade_date + time '09:25:20') at time zone 'Asia/Shanghai'
            else scheduled_at + interval '20 seconds'
        end
    ) not valid;

alter table realtime.call_auction_market_series_snapshot
    validate constraint call_auction_market_series_snapshot_schedule_check;

alter table realtime.call_auction_market_series_snapshot
    validate constraint call_auction_market_series_snapshot_observation_window_check;
