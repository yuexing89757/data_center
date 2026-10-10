"""Ephemeral published topic-list observations."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class HotTalkItem:
    topic_id: str
    rank: int
    title: str
    heat_value: int | None
    published_at: datetime | None
    has_vote: bool


@dataclass(frozen=True)
class HotTalkPage:
    observed_at: datetime
    topic_date: date | None
    returned_count: int
    items: tuple[HotTalkItem, ...]
    source_code: str = "kaipanla"
    source_layout: str = "kaipanla.hot_talks.v1"
    persisted: bool = False
