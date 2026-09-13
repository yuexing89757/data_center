from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from market_data_center.domain.hot_money import (
    HotMoneyActor,
    HotMoneyReviewStatus,
    HotMoneySeatMapping,
)

SEAT_ID = UUID("00000000-0000-0000-0000-000000000001")


def test_actor_requires_unique_nonblank_aliases() -> None:
    with pytest.raises(ValueError, match="aliases"):
        HotMoneyActor("FO_SHAN", "佛山系", ("佛山", " 佛山 "))


def test_mapping_requires_a_valid_effective_range() -> None:
    with pytest.raises(ValueError, match="valid range"):
        HotMoneySeatMapping(
            actor_code="FO_SHAN",
            seat_id=SEAT_ID,
            source_alias_name="某证券营业部",
            valid_from=date(2026, 1, 2),
            valid_to=date(2026, 1, 1),
            evidence_note="人工审核的公开披露",
            review_status=HotMoneyReviewStatus.APPROVED,
            reviewed_at=datetime(2026, 1, 3, tzinfo=UTC),
            catalog_version="v1",
        )


@pytest.mark.parametrize(
    ("status", "reviewed_at"),
    [
        (HotMoneyReviewStatus.PENDING, datetime(2026, 1, 3, tzinfo=UTC)),
        (HotMoneyReviewStatus.APPROVED, None),
        (HotMoneyReviewStatus.REJECTED, None),
    ],
)
def test_mapping_review_status_matches_review_timestamp(
    status: HotMoneyReviewStatus, reviewed_at: datetime | None
) -> None:
    with pytest.raises(ValueError, match="review timestamp"):
        HotMoneySeatMapping(
            actor_code="FO_SHAN",
            seat_id=SEAT_ID,
            source_alias_name="某证券营业部",
            valid_from=None,
            valid_to=None,
            evidence_note="人工审核的公开披露",
            review_status=status,
            reviewed_at=reviewed_at,
            catalog_version="v1",
        )


def test_generic_seat_alias_cannot_be_approved_as_hot_money() -> None:
    with pytest.raises(ValueError, match="generic seat alias"):
        HotMoneySeatMapping(
            actor_code="FO_SHAN",
            seat_id=SEAT_ID,
            source_alias_name="机构专用",
            valid_from=None,
            valid_to=None,
            evidence_note="人工审核的公开披露",
            review_status=HotMoneyReviewStatus.APPROVED,
            reviewed_at=datetime(2026, 1, 3, tzinfo=UTC),
            catalog_version="v1",
        )
