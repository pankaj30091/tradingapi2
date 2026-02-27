# Attribution Refactor – Design Document (Iteration 1)

## 1. Overview

Refactor `calculate_attribution_for_trade` and related APIs in `tradingapi2/tradingapi/attribution.py` so that:

- **Row** is the single input shape for trade data.
- **Spot and leg prices** can be supplied by callers or derived internally (with broker when needed).
- **`current_time`** has a clear default and no separate "attribution mode" flag.
- A small **helper** builds a row from individual fields for tests and one-off use.

**Day-level attribution** (prior day close, current day close/mark for rollovers) is **implemented** via the `day=True` parameter and `mtm_entry_price` / `mtm_exit_price` (see §4.1 and §7).

---

## 2. Goals

- **Single input shape:** All trade data is passed as a `row` (e.g. `pd.Series` or dict).
- **Optional spot/leg data:** If the caller does not pass `spot_prices` or `leg_prices`, attribution derives what it needs from the row and broker (and historical data where needed). When provided, these can be dicts (or lists) with a defined shape, or callables for backward compatibility.
- **Explicit default for time:** If `current_time` is not set, use `row["exit_time"]`; if that is missing or empty, use "now".
- **Easier testing:** Helper to build a row from individual fields; no need to construct a full DataFrame.
- **Internal IV:** Caller does not pass `get_iv_fn`; attribution uses internal IV (e.g. chameli + cached real-time path).
- **Broker when needed:** When spot/leg are not provided by callers, a `broker` is required for FUT/OPT (historical spot) and for optional Redis-based leg prices.

---

## 3. Current State (Implemented)

- **`calculate_attribution_for_trade(row, broker=None, *, spot_prices=None, leg_prices=None, current_time=None, day=True)`**  
  Single entry point. Caller may pass `spot_prices` and `leg_prices` as dict or callable; when None, spot/leg are derived from row and broker. `day=True` (default) uses day-level MTM (prior day close → current close/mark); `day=False` uses trade-level entry/exit. Internal IV via `get_iv_for_symbol_cached`; no `get_iv_fn` in the API.
- **`row_from_attribution_data(...)`**  
  Builds a `pd.Series` row from symbol, entry/exit times and prices, quantity, optional entry_keys/exit_keys/mtm.
- **No `calculate_attribution_realtime`**  
  Callers use `calculate_attribution_for_trade(row, broker=..., current_time=..., day=...)` directly; spot/leg are derived internally when not provided.
- **MTM helpers**  
  `mtm_entry_price(row, mtm_date, broker, historical_cache)` and `mtm_exit_price(...)` support an optional `historical_cache`; the public API does not yet pass a cache (always None), so each call can trigger `get_historical_close_price`.

---

## 4. Proposed API

### 4.1 `calculate_attribution_for_trade`

```
calculate_attribution_for_trade(
    row,
    broker=None,
    *,
    spot_prices=None,
    leg_prices=None,
    current_time=None,
    day=True,
) -> Dict[str, Any]
```

- **`row`** (required): One trade. Dict or `pd.Series` with at least:  
  `symbol`, `entry_time`, `exit_time`, `entry_price`, `exit_price` (or `mtm` for open), `entry_quantity`,  
  and for multi-leg options: `entry_keys`, `exit_keys` (when leg prices are to be read from Redis).
- **`broker`** (optional but required when spot/leg are not provided):  
  Used when `spot_prices` is None (FUT/OPT: historical spot) and when `leg_prices` is None (Redis leg prices). Can be omitted if both are provided.
- **`spot_prices`** (optional):  
  Spot price(s) for the underlying at entry and exit. One of:
  - **Dict:** `{(underlying_symbol, exchange, time): price}`. Keys are `(str, str, datetime)` (or date); implementation looks up exact and date fallback via `_normalize_spot_prices`.
  - **Callable:** `(underlying_symbol, exchange, time) -> Optional[float]`, for backward compatibility or when caller prefers a function.
  - **None:** Spot is derived as in §5.1 (STK from trade price; FUT/OPT from broker via `get_spot_price_at_time` / `get_option_underlying_price`).
- **`leg_prices`** (optional):  
  Per-leg option prices at entry and exit. One of:
  - **Dict:** `{leg_symbol: {"entry": float, "exit": float}}` or `{leg_symbol: (entry_price, exit_price)}` (tuple/list of two). Implementation uses `_normalize_leg_prices` to get a callable.
  - **Callable:** `(leg_symbol, "entry" | "exit") -> Optional[float]`, for backward compatibility.
  - **None:** Leg prices are derived from row (Redis via `entry_keys`/`exit_keys` and broker; open trades: current mark for exit leg).
- **`current_time`** (optional):  
  If None: use `row["exit_time"]`; if that is missing or empty, use `datetime.now()`.  
  If provided: used as the "exit" time (e.g. for open positions or day-level attribution).
- **`day`** (optional, default True):  
  When True: day-level attribution using MTM — `mtm_entry_price` (prior day close) and `mtm_exit_price` (current day close or current mark for open). When False: trade-level attribution from row entry/exit prices and times.
- **Return:** `{"spot_attrib", "vol_attrib", "timedecay_attrib", "spread_attrib", "per_leg"}`. `per_leg` is a dict of leg symbol → `{spot_attrib, vol_attrib, timedecay_attrib, spread_attrib}` (per-leg breakdown from chameli).

Internal IV is used by the implementation (`get_iv_for_symbol_cached`); no `get_iv_fn` in the public signature.

### 4.2 Helper: Build Row from Data Points

```
row_from_attribution_data(
    symbol,
    entry_time,
    exit_time,
    entry_price,
    exit_price,
    entry_quantity,
    *,
    entry_keys=None,
    exit_keys=None,
    mtm=None,
) -> pd.Series
```

- Returns a `pd.Series` suitable to pass as `row` to `calculate_attribution_for_trade`.
- Single place to document and enforce the minimal "row" shape for attribution.

### 4.3 `calculate_attribution_realtime`

- **Not implemented.** Callers use `calculate_attribution_for_trade(row, broker=..., spot_prices=None, leg_prices=None, current_time=..., day=...)` directly; when `spot_prices` and `leg_prices` are None, the implementation builds spot via `_build_spot_fn_from_row_broker` (using `get_spot_price_at_time` / `get_option_underlying_price`) and leg prices via `_get_leg_price_from_broker_row` (Redis / `get_mid_price`). A future convenience wrapper could be added if desired.

---

## 5. Behaviour When Callers Do Not Provide Spot/Leg Data

### 5.1 Spot (when `spot_prices` is None)

- **STK:** Use trade price as spot: at t0 use `row["entry_price"]`, at t1 use `row["exit_price"]` (or `row["mtm"]` if exit not set). No broker needed for STK.
- **FUT / OPT:** Need underlying spot at t0 and t1. Use **broker** (required when `spot_prices` is None for FUT/OPT): `get_spot_price_at_time(broker, underlying_symbol, exchange, time)` and for options `get_option_underlying_price(..., as_of=time)`. Day-level MTM (prior day close / current close) is handled by `day=True` and `mtm_entry_price` / `mtm_exit_price` (§7).

So: **if no spot data is provided, we get spot from entry/exit price for STK; for FUT and OPT we query historical data (via broker).** Broker is required when `spot_prices` is None and symbol type is FUT or OPT.

### 5.2 Leg Prices (when `leg_prices` is None)

- **Single-leg option:** Use `row["entry_price"]` and `row["exit_price"]` (or `mtm`) as leg entry/exit prices.
- **Multi-leg option:** Derive from row:
  - If `entry_keys` / `exit_keys` present and broker has Redis: read per-leg entry/exit price from Redis (same logic as today's leg-price callable in pnl_publisher).
  - Open trades (no exit_keys): use current mark for exit leg (e.g. `get_mid_price` for that leg).

So when `leg_prices` is None we need **broker** for multi-leg (Redis + optional current mark). Single-leg can work from row only.

### 5.3 IV

- Not exposed in the public API. Implementation uses internal IV (chameli / `get_iv_for_symbol` and cached real-time path where applicable).

### 5.4 `current_time`

- If **not** provided: use `row["exit_time"]`; if missing or empty, use `datetime.now()`.
- If **provided**: used as the "exit" time (and later for day-level attribution). No extra parameter like `attribution_mode="day"`.

---

## 6. Dependencies and Callers

- **pnl_publisher / scalping / notebooks:** Use `calculate_attribution_for_trade(row, broker=..., current_time=..., day=..., leg_prices=...)` directly. Can pass `leg_prices` as dict or callable when per-leg prices are known.
- **pnl_redis_update / pnl_common:** Use attribution with historical spot (and possibly different leg paths). They pass row + spot data (callable or dict) and optionally broker.
- **Tests:** Use `row_from_attribution_data(...)` then `calculate_attribution_for_trade(row, broker=sh, spot_prices={...}, leg_prices={...})` with dicts, or pass broker and let attribution derive spot/leg.

---

## 7. Day-Level Attribution (Implemented)

- **Day-level attribution** is implemented when `day=True` (default):
  - **Entry MTM:** `mtm_entry_price(row, attribution_date, broker, historical_cache)` — for positions entered before attribution date, uses EOD close of prior business day via `get_historical_close_price` (ohlcutils first, then broker); otherwise uses `row["entry_price"]`.
  - **Exit MTM:** `mtm_exit_price(row, attribution_date, broker, historical_cache)` — for open positions on attribution date uses current mark or EOD close; for closed positions uses `row["exit_price"]` or historical close as appropriate.
- **Data sources:** `get_historical_close_price` in attribution.py (module-level cache by (symbol, date_str)); ohlcutils when available, else broker (Shoonya → FivePaisa for historical). `mtm_entry_price` / `mtm_exit_price` accept an optional `historical_cache` dict; the single-trade API does not yet pass it (see §9).

---

## 8. Open Points / Follow-ups

- **Time matching for `spot_prices` dict:** When keys are `(symbol, exchange, time)`, whether to match by exact datetime or by date (e.g. any time on that date). Implementation uses exact key first, then date fallback.
- Whether `row_from_attribution_data` returns `pd.Series` or dict (implemented as `pd.Series`).
- **Backward compatibility:** API uses `spot_prices` and `leg_prices` (dict or callable); no legacy parameter names.

---

## 9. Performance and Caching (Proposed)

The core API `calculate_attribution_for_trade` can be slow when called repeatedly (e.g. over many trades) because it triggers multiple broker/ohlcutils lookups with no cross-call reuse. The following changes are proposed to improve latency and reduce redundant I/O.

### 9.1 Expose `historical_cache` in the public API

- **Current:** `mtm_entry_price` and `mtm_exit_price` accept an optional `historical_cache: Optional[Dict[Tuple[str, str], Optional[float]]]` (key: `(symbol, date_str)` → EOD close). `calculate_attribution_for_trade` always passes `None`, so every day-level call can trigger `get_historical_close_price` (and thus ohlcutils or broker historical).
- **Proposed:** Add optional parameter to the public API:
  ```text
  historical_cache: Optional[Dict[Tuple[str, str], Optional[float]]] = None
  ```
  When `day=True`, pass this cache into `mtm_entry_price` and `mtm_exit_price`. Callers that loop over many trades (e.g. scalping, batch PnL) should create one dict per batch and pass it in so each `(symbol, date_str)` is fetched once and reused.

### 9.2 Batch API (optional)

- **Proposed:** Add `calculate_attribution_for_trades(rows, broker, ..., historical_cache=None)` (or have callers pass a shared `historical_cache` into a loop of `calculate_attribution_for_trade`). A dedicated batch function could pre-populate `historical_cache` for all required (symbol, date) pairs (e.g. from a single broker/ohlcutils pass) then run single-trade logic per row. This reduces duplicate `get_historical_close_price` and optional spot/IV work when many trades share underlyings and dates.

### 9.3 Caching of “spot at time” / historical bars

- **Current:** Options path uses `get_option_underlying_price(..., as_of=time)`, which calls `utils._get_historical_close_at_time` — **no cache**; each call does `broker.get_historical(1m)` for that symbol/date. `get_spot_price_at_time` uses `_price_cache` with 60s TTL; historical (past) times are cached with the same short TTL.
- **Proposed:**
  - **In `utils`:** Add a cache for `_get_historical_close_at_time` keyed by e.g. `(symbol, exchange, date_str, time_key)` (e.g. `time_key = as_of.strftime("%Y%m%d_%H%M")`). For past dates, cache indefinitely or with long TTL.
  - **In attribution:** For `_price_cache`, when the requested time is in the past (`at_time < now`), do not expire after 60s (or use a long TTL / no TTL). Only “current” or near-current keys need short TTL.
  - **Unified “1m bars” cache (optional):** One fetch per `(symbol, exchange, date_str)` for 1m bars; both `get_spot_price_at_time` (past) and `get_option_underlying_price(as_of=...)` could consume from this cache to avoid duplicate `broker.get_historical(1m)` for the same day.

### 9.4 IV and price cache TTLs

- **Current:** `_iv_cache` uses `_IV_CACHE_TTL_SECONDS = 30`. IV for the same (symbol, price, spot, time) is deterministic.
- **Proposed:** For past times (`at_time < now - 1 minute`), cache IV indefinitely or with long TTL. Optionally increase `_IV_CACHE_TTL_SECONDS` for real-time keys (e.g. 300s) if acceptable for live dashboards.

### 9.5 Prefer passing `spot_prices` and `leg_prices` when available

- When the caller has already fetched spot and per-leg prices (e.g. from a batch or UI), passing `spot_prices=` and `leg_prices=` avoids all broker/Redis lookups for those. Document this as the fast path for loops (e.g. “for batch attribution, build once: `{(symbol, exchange, time): price}` and `{leg: (entry, exit)}`, then call `calculate_attribution_for_trade(row, ..., spot_prices=spot_dict, leg_prices=leg_dict)`”).

### 9.6 Other

- **`get_historical_close_price`:** Uses `refresh_mapping=True` on broker historical call; consider `refresh_mapping=False` for repeated (symbol, date) use or make it configurable to reduce overhead.
- **Redis leg prices:** `_get_leg_price_from_broker_row` does one or more Redis `hget` per key; batching (e.g. pipeline) could reduce round-trips when a row has many keys (lower priority than historical/spot caching).
