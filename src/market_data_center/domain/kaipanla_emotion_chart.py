"""Ephemeral source observations for the Kaipanla market-emotion chart."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class EmotionChartItem:
    trade_date: date
    emotion_index: int
    large_drawdown_count: int
    limit_up_count: int
    consecutive_limit_up_height: int


@dataclass(frozen=True)
class EmotionChartPage:
    observed_at: datetime
    offset: int
    limit: int
    returned_count: int
    next_offset: int | None
    items: tuple[EmotionChartItem, ...]
    source_code: str = "kaipanla"
    source_layout: str = "kaipanla.market_emotion.chart.v1"
    persisted: bool = False
    total: None = None
