"""Pure peak calculation for exact, complete native one-minute quantity labels."""

from collections import Counter
from datetime import datetime, time, timedelta

from market_data_center.domain.stock_max_minute_volume import MinuteVolumePeak, MinuteVolumeSource


def calculate_minute_volume_peak(source: MinuteVolumeSource) -> MinuteVolumePeak:
    expected = {
        datetime.combine(source.trade_date, time(hour, minute)) + timedelta(minutes=offset)
        for hour, minute in ((9, 31), (13, 1))
        for offset in range(120)
    }
    reasons = list(source.quality_reasons)
    if source.metadata.get("history_boundary_reached") is not True:
        reasons.append("history_boundary_not_reached")
    counts = Counter(bar.timestamp for bar in source.bars)
    if expected - counts.keys():
        reasons.append("missing_minute_bar")
    if counts.keys() - expected:
        reasons.append("unexpected_minute_bar")
    if any(count > 1 for count in counts.values()):
        reasons.append("duplicate_minute_bar")
    if any(bar.timestamp.date() != source.trade_date for bar in source.bars):
        reasons.append("source_date_mismatch")
    if any(
        type(bar.volume_shares) is not int or not 0 <= bar.volume_shares <= 2**63 - 1
        for bar in source.bars
    ):
        reasons.append("invalid_minute_volume")
    peak = (
        min(source.bars, key=lambda bar: (-bar.volume_shares, bar.timestamp))
        if not reasons
        else None
    )
    return MinuteVolumePeak(
        symbol=source.symbol,
        trade_date=source.trade_date,
        max_minute_volume_shares=peak.volume_shares if peak else None,
        peak_at=peak.timestamp if peak else None,
        expected_count=240,
        observed_count=len(source.bars),
        quality_reasons=tuple(dict.fromkeys(reasons)),
    )
