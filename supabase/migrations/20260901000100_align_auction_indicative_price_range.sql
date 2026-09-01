alter table realtime.call_auction_market_series_snapshot
    drop constraint call_auction_market_series_snapshot_check3;

alter table realtime.call_auction_market_series_snapshot
    add constraint call_auction_market_series_snapshot_price_range_check
    check (
        (high_price is null or high_price >= 0)
        and (low_price is null or low_price >= 0)
        and (high_price is null or low_price is null or high_price >= low_price)
        and (
            value_semantics = 'auction_indicative'
            or (
                (last_price is null or low_price is null or last_price >= low_price)
                and (last_price is null or high_price is null or last_price <= high_price)
            )
        )
    ) not valid;

alter table realtime.call_auction_market_series_snapshot
    validate constraint call_auction_market_series_snapshot_price_range_check;
