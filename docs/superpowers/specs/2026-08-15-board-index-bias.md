# THS 883423 Latest MA5 Bias API Design

## Goal

Expose one authenticated, read-only API response for the latest stored
`THS:883423` board-index daily bar. The response reports the current close,
five-session simple moving average and bias, comparison with the previous
available board trading session, and extrema over the latest 30 board trading
sessions.

## Public contract

The FastAPI route is `GET /api/v1/board-indexes/883423/bias`. It accepts no
trade-date or board-code input. The fixed path deliberately exposes only the
board already present in the explicit BoardIndex catalog.

The response contains:

- `board_id`, `board_code`, `board_name`, and latest `trade_date`;
- latest `close`, `moving_average_5`, and `bias_5_pct`;
- `previous_trade_date`, `previous_bias_5_pct`, and `bias_direction`;
- `window_trading_days`, `bias_sample_count`, 30-session high/low BIAS5 values
  and their trading dates;
- the fixed `algorithm_version` value `board_index_bias_v1`.

Prices, averages, and percentages are PostgreSQL `numeric` values exposed by
FastAPI as decimal strings. Dates use `YYYY-MM-DD`. If the board has no stored
daily bars the API returns 404.

## Calculation semantics

For each available board trading session, including the current session:

```text
MA5 = arithmetic mean of that close and the preceding four available closes
BIAS5 = (close - MA5) / MA5 * 100
```

No calendar-day filling, forward filling, provider fallback, or live fetch is
allowed. A session has a valid BIAS5 only when exactly five positive closes are
available in its rolling window. The latest and previous valid BIAS5 values are
compared numerically: greater is `up`, smaller is `down`, and equal is `flat`.
The direction is null when either comparison value is unavailable.

The extrema observation window is the latest 30 stored board trading sessions,
including the latest session. Extrema ignore sessions without a valid BIAS5;
`bias_sample_count` reports the number used. When fewer than 30 sessions exist,
all available valid samples are used. Tied extrema select the most recent
trading date. A zero or nonpositive close/MA5 produces no valid bias sample.

The database query reads at most the latest 34 rows: 30 observation sessions
plus four earlier closes needed for the oldest observation's MA5.

## Architecture and security

An ordered migration adds stable, security-definer RPC
`api_v1.query_board_index_bias_latest()`. It fixes `THS:883423` inside the
function, has a five-second statement timeout, revokes public execution, and
grants execution only to `market_data_api`. The FastAPI query service calls only
this bounded RPC and never queries `core` directly.

The calculation is performed at query time and is not persisted. This avoids a
new scheduler, calculation table, stale-result lifecycle, and ingestion
lineage that would duplicate the immutable source bars. The explicit algorithm
version makes the returned semantics identifiable; a future semantic change
must use a new version and ADR.

## Verification and documentation

Focused tests cover the HTTP contract, API-key protection, fixed no-input
route, Decimal serialization, insufficient history, prior-session direction,
30-session extrema, tie-breaking, permissions, and query bounds. The three
checked-in public contracts and BoardIndex/FastAPI documentation are updated in
the same change.

