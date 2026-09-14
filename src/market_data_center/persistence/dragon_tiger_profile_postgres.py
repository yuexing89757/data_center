"""PostgreSQL loading and atomic publication for DragonTiger seat profiles."""

from collections.abc import Sequence
from datetime import UTC, date, datetime
from json import dumps
from uuid import UUID

from sqlalchemy import Engine, RowMapping, text

from market_data_center.dragon_tiger_analytics import SeatParticipation, TradingSeatProfile
from market_data_center.dragon_tiger_profile_service import (
    PROFILE_ALGORITHM_VERSION,
    DragonTigerProfileInputs,
    FutureSessionClose,
    SeatProfileObservation,
)

CALCULATION_CODE = "dragon_tiger_seat_profile"


class PostgreSQLDragonTigerProfilePersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool:
        with self._engine.connect() as connection:
            value = connection.scalar(
                text("""
select is_trading_day from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:trade_date
"""),
                {"trade_date": trade_date},
            )
        return value is True

    def load_profile_inputs(self, as_of_date: date) -> DragonTigerProfileInputs:
        with (
            self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as connection,
            connection.begin(),
        ):
            rows = tuple(
                connection.execute(_SELECT_OBSERVATIONS, {"as_of_date": as_of_date}).mappings()
            )
            first_date = min((row["event_date"] for row in rows), default=as_of_date)
            trading_dates = tuple(
                connection.scalars(
                    text("""
select trade_date from core.trading_calendar
where market='CN_A_SHARE' and is_trading_day
  and trade_date between :first_date and :as_of_date
order by trade_date
"""),
                    {"first_date": first_date, "as_of_date": as_of_date},
                )
            )
            watermark = connection.scalar(
                text("select max(trade_date) from core.daily_bar where trade_date<=:as_of_date"),
                {"as_of_date": as_of_date},
            )
        if watermark is None or watermark < as_of_date:
            raise RuntimeError("DT_PROFILE_DAILY_BAR_NOT_READY")
        return DragonTigerProfileInputs(
            as_of_date=as_of_date,
            input_watermark_date=watermark,
            trading_dates=trading_dates,
            observations=tuple(_observation(row) for row in rows),
        )

    def replace_profiles(
        self,
        profiles: Sequence[TradingSeatProfile],
        *,
        calculation_id: UUID,
        input_watermark_date: date,
        input_hash: str,
    ) -> int:
        if any(profile.algorithm_version != PROFILE_ALGORITHM_VERSION for profile in profiles):
            raise ValueError("DragonTiger profile algorithm version mismatch")
        as_of_dates = {profile.as_of_date for profile in profiles}
        as_of_date = next(iter(as_of_dates), input_watermark_date)
        if len(as_of_dates) > 1 or input_watermark_date > as_of_date:
            raise ValueError("DragonTiger profile publication dates are inconsistent")
        with self._engine.begin() as connection:
            existing = connection.scalar(
                text("""
select calculation_id from derived.calculation_run
where calculation_code=:code and algorithm_version=:version
  and start_date=:as_of_date and end_date=:as_of_date
  and input_hash=:input_hash and status='succeeded'
"""),
                {
                    "code": CALCULATION_CODE,
                    "version": PROFILE_ALGORITHM_VERSION,
                    "as_of_date": as_of_date,
                    "input_hash": input_hash,
                },
            )
            if existing is not None:
                count = connection.scalar(
                    text("""
select count(*) from billboard.trading_seat_profile_daily
where as_of_date=:as_of_date and algorithm_version=:version
  and calculation_id=:calculation_id
"""),
                    {
                        "as_of_date": as_of_date,
                        "version": PROFILE_ALGORITHM_VERSION,
                        "calculation_id": existing,
                    },
                )
                if int(count or 0) != len(profiles):
                    raise RuntimeError("DT_PROFILE_IDEMPOTENCY_CONFLICT")
                return len(profiles)
            now = datetime.now(UTC)
            connection.execute(
                text("""
insert into derived.calculation_run (
 calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,
 input_watermark,input_hash,requested_at,calculated_at,finished_at,output_rows
) values (
 :calculation_id,:code,:version,'incremental',:as_of_date,:as_of_date,'succeeded',
 cast(:watermark as jsonb),:input_hash,:now,:now,:now,:output_rows
)
"""),
                {
                    "calculation_id": calculation_id,
                    "code": CALCULATION_CODE,
                    "version": PROFILE_ALGORITHM_VERSION,
                    "as_of_date": as_of_date,
                    "watermark": dumps({"daily_bar_through": input_watermark_date.isoformat()}),
                    "input_hash": input_hash,
                    "now": now,
                    "output_rows": len(profiles),
                },
            )
            connection.execute(
                text("""
delete from billboard.trading_seat_profile_daily
where as_of_date=:as_of_date and algorithm_version=:version
"""),
                {"as_of_date": as_of_date, "version": PROFILE_ALGORITHM_VERSION},
            )
            if profiles:
                connection.execute(
                    _INSERT_PROFILE,
                    [
                        _profile_params(item, calculation_id, input_watermark_date)
                        for item in profiles
                    ],
                )
        return len(profiles)


def _observation(row: RowMapping) -> SeatProfileObservation:
    futures = tuple(
        FutureSessionClose(horizon, row[f"t{horizon}_date"], row[f"t{horizon}_close"])
        for horizon in (1, 3, 5)
        if row[f"t{horizon}_date"] is not None
    )
    return SeatProfileObservation(
        participation=SeatParticipation(
            row["source_record_id"],
            row["seat_id"],
            row["event_date"],
            row["buy_amount"],
            row["sell_amount"],
        ),
        event_close=row["event_close"],
        future_closes=futures,
    )


def _profile_params(
    profile: TradingSeatProfile, calculation_id: UUID, input_watermark_date: date
) -> dict[str, object]:
    return {
        **{name: getattr(profile, name) for name in profile.__dataclass_fields__},
        "calculation_id": calculation_id,
        "input_watermark_date": input_watermark_date,
    }


_SELECT_OBSERVATIONS = text("""
select st.seat_id, e.source_record_id, e.trade_date event_date,
       st.buy_amount, st.sell_amount, e.close_price event_close,
       sessions.t1_date, b1.close t1_close,
       sessions.t3_date, b3.close t3_close,
       sessions.t5_date, b5.close t5_close
from billboard.seat_trade st
join billboard.dragon_tiger_event e on e.event_id=st.event_id
left join lateral (
  select max(trade_date) filter (where rn=1) t1_date,
         max(trade_date) filter (where rn=3) t3_date,
         max(trade_date) filter (where rn=5) t5_date
  from (
    select trade_date, row_number() over (order by trade_date) rn
    from core.trading_calendar
    where market='CN_A_SHARE' and is_trading_day
      and trade_date>e.trade_date and trade_date<=:as_of_date
    order by trade_date limit 5
  ) future_sessions
) sessions on true
left join core.daily_bar b1 on b1.symbol=e.symbol and b1.trade_date=sessions.t1_date
left join core.daily_bar b3 on b3.symbol=e.symbol and b3.trade_date=sessions.t3_date
left join core.daily_bar b5 on b5.symbol=e.symbol and b5.trade_date=sessions.t5_date
where st.seat_id is not null and e.trade_date<=:as_of_date
order by st.seat_id,e.trade_date,e.source_record_id
""")

_INSERT_PROFILE = text("""
insert into billboard.trading_seat_profile_daily (
 seat_id,as_of_date,algorithm_version,metric_definition,return_definition,
 participation_definition,total_lhb_count,total_buy_amount,total_sell_amount,
 t1_sample_count,t1_win_rate,t1_avg_return,t3_sample_count,t3_win_rate,t3_avg_return,
 t5_sample_count,t5_win_rate,t5_avg_return,consecutive_participation_sample_count,
 consecutive_participation_rate,input_watermark_date,calculation_id
) values (
 :seat_id,:as_of_date,:algorithm_version,:metric_definition,:return_definition,
 :participation_definition,:total_lhb_count,:total_buy_amount,:total_sell_amount,
 :t1_sample_count,:t1_win_rate,:t1_avg_return,:t3_sample_count,:t3_win_rate,:t3_avg_return,
 :t5_sample_count,:t5_win_rate,:t5_avg_return,:consecutive_participation_sample_count,
 :consecutive_participation_rate,:input_watermark_date,:calculation_id
)
""")
