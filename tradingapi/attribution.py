"""
Attribution and MTM (Mark-to-Market) calculation for P&L.

This module provides:
- get_historical_close_price: EOD close for a symbol (ohlcutils first, then broker API; combo support; cache)
- MTM entry/exit prices for carried-over trades and current prices for today
- Attribution (spot, vol, timedecay, spread) per trade
- Real-time spot/IV fetching with caching; use calculate_attribution_for_trade(row, broker=...) for live use

Fast path for batch: pass underlying_prices= and leg_prices= (dicts) when already available to avoid
broker/Redis lookups.

Self-sufficient: defines get_historical_close_price here; uses tradingapi.utils (get_price_at_time,
parse_combo_symbol, parse_datetime) and broker.get_historical for intraday price;
chameli for IV and performance_attribution (options).
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pandas as pd

from . import trading_logger
from .broker_base import BrokerBase
from .utils import (
    get_price_at_time,
    get_future_underlying_price,
    get_option_underlying_price,
    get_historical_call_count,
    get_price_cache,
    parse_combo_symbol,
    set_price_cache,
)
from chameli.dateutils import (
    advance_by_biz_days,
    format_datetime,  # type: ignore[attr-defined]
    get_expiry,
    get_naive_dt,
    parse_datetime,  # type: ignore[attr-defined]
)
from chameli.europeanoptions import _parse_combo_symbol, calc_greeks, performance_attribution

# ---------------------------------------------------------------------------
# Historical close price: broker cache (Shoonya -> FivePaisa)
# ---------------------------------------------------------------------------
_historical_broker_cache: Dict[str, object] = {}
_HISTORICAL_BROKER_REDIS_DB = 0
# ---------------------------------------------------------------------------
# Unified IV cache: (symbol, time_key) -> iv. Price cache lives in utils.
# ---------------------------------------------------------------------------
_iv_cache: Dict[Tuple[str, str], float] = {}  # (symbol, time_key) -> iv


def _is_missing_number(value: Any) -> bool:
    return value is None or (isinstance(value, float) and value != value)


def _set_iv_cache(symbol: str, time_key: str, value: Optional[float]) -> None:
    if _is_missing_number(value):
        return
    _iv_cache[(symbol, time_key)] = float(cast(float, value))


def _get_historical_broker(broker: object) -> object:
    """
    Return the broker to use for historical data.
    If broker is Shoonya, use FivePaisa (Shoonya historical is unreliable); else return the passed broker.
    """
    try:
        from tradingapi.fivepaisa import FivePaisa
        from tradingapi.shoonya import Shoonya
    except ImportError:
        return broker
    if not isinstance(broker, Shoonya):  # type: ignore[name-defined]
        return broker
    app = _historical_broker_cache.get("FIVEPAISA")
    if app is not None and isinstance(app, FivePaisa):  # type: ignore[name-defined]
        return app
    try:
        app = FivePaisa()  # type: ignore[name-defined]
        app.connect(redis_db=_HISTORICAL_BROKER_REDIS_DB)
        if app.is_connected():
            _historical_broker_cache["FIVEPAISA"] = app
            return app
    except Exception as e:
        trading_logger.log_warning(f"Failed to create FivePaisa for historical data: {e}")
    trading_logger.log_warning("Could not get FivePaisa for historical data, using Shoonya (may be unreliable)")
    return broker


def get_historical_close_price(
    broker: object,
    symbol: str,
    date_str: Union[str, dt.datetime, dt.date, pd.Timestamp],
    exchange: str,
    *,
    refresh_mapping: bool = False,
) -> Optional[float]:
    """
    Get historical close price for a symbol on a specific date (EOD close, 15:29 bar).
    Delegates to utils.get_price_at_time(..., as_of=eod_datetime) for single symbols;
    combo symbols are decomposed and combined weighted by quantity. If broker is Shoonya,
    FivePaisa is used for the API fallback (db=0, cached).

    refresh_mapping: If True, broker loads symbol mapping from date's symbols CSV (default).
    Set False for batch/repeated (symbol, date) calls to avoid repeated mapping refresh.
    """
    if isinstance(date_str, pd.Timestamp):
        date_str = date_str.strftime("%Y%m%d")
    elif isinstance(date_str, dt.datetime):
        date_str = date_str.strftime("%Y%m%d")
    elif isinstance(date_str, dt.date) and not isinstance(date_str, dt.datetime):
        date_str = date_str.strftime("%Y%m%d")
    elif isinstance(date_str, str):
        try:
            date_str = format_datetime(date_str, "%Y%m%d")
        except (ValueError, TypeError):
            pass
    else:
        date_str = str(date_str)

    date_key: str = date_str if isinstance(date_str, str) else str(date_str)
    if ":" in symbol:
        try:
            legs = parse_combo_symbol(symbol)
            combo_price = 0.0
            for leg_symbol, quantity in legs.items():
                leg_price = get_historical_close_price(
                    broker, leg_symbol, date_key, exchange, refresh_mapping=refresh_mapping
                )
                if leg_price is None:
                    trading_logger.log_warning(f"Could not get price for combo leg {leg_symbol} on {date_key}")
                    return None
                combo_price += leg_price * quantity
            return combo_price
        except Exception as e:
            trading_logger.log_warning(f"Error parsing combo symbol {symbol} on {date_key}: {e}")
            return None

    hist_broker = _get_historical_broker(broker)
    eod_dt = dt.datetime.strptime(date_key + " 15:29:00", "%Y%m%d %H:%M:%S")
    result = get_price_at_time(
        cast(BrokerBase, hist_broker), symbol, exchange, as_of=eod_dt, mds="mds", refresh_mapping=False
    )
    if result is None:
        return None
    return float(result)


# ============================================================================
# Symbol parsing utilities
# ============================================================================


def get_underlying_symbol(symbol: str) -> str:
    """
    Get underlying symbol for a given symbol.

    Args:
        symbol: Symbol string (e.g., "NIFTY_OPT_20251230_CALL_3320")

    Returns:
        Underlying symbol (e.g., "NIFTY_IND___" or "TCS_STK___")
    """
    if "_OPT_" in symbol:
        base = symbol.split("_OPT_")[0]
    elif "_FUT_" in symbol:
        base = symbol.split("_FUT_")[0]
    else:
        return symbol

    if "NIFTY" in base or "SENSEX" in base:
        return base + "_IND___"
    return base + "_STK___"


def get_exchange(symbol: str) -> str:
    """Get exchange for a symbol (BSE if SENSEX, else NSE)."""
    return "BSE" if "SENSEX" in symbol else "NSE"


def is_futures_symbol(symbol: str) -> bool:
    """Check if symbol is a futures contract."""
    return "_FUT_" in symbol


def is_options_symbol(symbol: str) -> bool:
    """Check if symbol is an options contract."""
    return "_OPT_" in symbol


def is_stock_symbol(symbol: str) -> bool:
    """Check if symbol is a stock."""
    return "_STK_" in symbol


def _get_month_end_fut_expiry(opt_expiry_yyyymmdd: str, exchange: str) -> str:
    """
    Return month-end FUT expiry (yyyymmdd) for the option's expiry month.
    NSE month-end = last Tuesday (day_of_week=2); BSE = last Thursday (day_of_week=4).
    """
    try:
        d = dt.datetime.strptime(opt_expiry_yyyymmdd, "%Y%m%d").date()
        day_of_week = 2 if exchange == "NSE" else 4  # Tuesday / Thursday
        expiry_date = get_expiry(
            dt.datetime.combine(d, dt.time(0, 0)),
            weekly=0,
            day_of_week=day_of_week,
            exchange=exchange,
        )
        strftime_fn = getattr(expiry_date, "strftime", None)
        if strftime_fn is not None:
            return strftime_fn("%Y%m%d")
        return str(expiry_date)
    except Exception:
        return opt_expiry_yyyymmdd


def get_greeks_at_time(
    row: Union[pd.Series, Dict[str, Any]],
    current_time: dt.datetime,
    broker: Optional[object],
    greeks_list: Optional[List[str]] = None,
    leg_prices: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    For a trade row (combo or single leg), compute greeks at current_time for each option leg.
    Option price: if leg_prices is provided and has a valid price for the leg, use it (e.g. entry
    price or current mark); otherwise uses broker 1m OHLC close at current_time via
    get_price_at_time. Underlying is always from broker/API at current_time.
    Returns dict: leg_symbol -> {delta, gamma, theta, vega, vol} or {error: str} on failure.
    """
    if greeks_list is None:
        greeks_list = ["delta", "gamma", "theta", "vega"]
    combo = row.get("symbol", "")
    legs = _parse_combo_symbol(combo)
    out: Dict[str, Any] = {}
    for leg_symbol in legs:
        if not is_options_symbol(leg_symbol):
            continue
        exchange = get_exchange(leg_symbol)
        parts = leg_symbol.split("_")
        if len(parts) < 5:
            continue
        opt_expiry = parts[2]
        fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange)
        if broker is None:
            continue
        try:
            opt_price = None
            if leg_prices and leg_symbol in leg_prices:
                p = leg_prices[leg_symbol]
                if p is not None and (not isinstance(p, float) or not (p != p)):
                    opt_price = float(p)
            if opt_price is None:
                opt_price = get_price_at_time(
                    cast(BrokerBase, broker), leg_symbol, exchange, as_of=current_time, mds="mds"
                )
            if opt_price is None:
                continue
            underlying = get_option_underlying_price(
                [cast(BrokerBase, broker)],
                leg_symbol,
                opt_expiry,
                fut_expiry=fut_expiry,
                exchange=exchange,
                as_of=current_time,
            )
            if underlying is None:
                continue
            g = calc_greeks(
                long_symbol=leg_symbol,
                opt_price=float(opt_price),
                underlying=float(underlying),
                calc_time=current_time,
                greeks=greeks_list + ["vol"],
                risk_free_rate=0,
                exchange=exchange,
            )
            out[leg_symbol] = {
                k: (
                    getattr(g, k, None)
                    or (g.get(k) if isinstance(g, dict) else None)
                )
                for k in (greeks_list + ["vol"])
                if k in (greeks_list + ["vol"])
            }
        except Exception as e:
            out[leg_symbol] = {"error": str(e)}
    return out


# ============================================================================
# IV calculation
# ============================================================================


def get_iv_for_symbol(
    symbol: str,
    price: float,
    spot: float,
    time: dt.datetime,
    exchange: str,
) -> Optional[float]:
    """
    Get implied volatility for an options symbol.

    Args:
        symbol: Options symbol
        price: Option price
        spot: Underlying spot price
        time: Time to calculate IV at
        exchange: Exchange name

    Returns:
        IV as float, or None if not available
    """
    try:
        if "_OPT_" in symbol:
            expiry_str = symbol.split("_")[2]
            expiry_dt = dt.datetime.strptime(expiry_str + " 15:30:00", "%Y%m%d %H:%M:%S")
            if time >= expiry_dt:
                return 0.0
    except Exception:
        pass

    try:
        greeks = calc_greeks(
            long_symbol=symbol,
            opt_price=price,
            underlying=spot,
            calc_time=time,
            greeks=["vol"],
            risk_free_rate=0,
            exchange=exchange,
        )
        if isinstance(greeks, dict):
            return greeks.get("vol", None)
        return greeks
    except Exception:
        return None


# ============================================================================
# Underlying/leg price input normalization
# ============================================================================


_UnderlyingPricesArg = Optional[Dict[Tuple[str, Union[dt.datetime, dt.date]], float]]
_LegPricesArg = Optional[Dict[str, Dict[str, float]]]


def _lookup_underlying_price_from_map(
    underlying_prices: _UnderlyingPricesArg, symbol: str, time: dt.datetime
) -> Optional[float]:
    if underlying_prices is None:
        return None
    key_exact = (symbol, time)
    if key_exact in underlying_prices:
        return float(underlying_prices[key_exact])
    key_date = (symbol, time.date())
    if key_date in underlying_prices:
        return float(underlying_prices[key_date])
    return None


def _get_underlying_price(
    symbol: str,
    exchange: str,
    time: Optional[dt.datetime],
    *,
    underlying_prices: _UnderlyingPricesArg,
    broker: Optional[object],
) -> Optional[float]:
    if time is not None:
        from_map = _lookup_underlying_price_from_map(underlying_prices, symbol, time)
        if from_map is not None:
            return from_map
    if broker is None:
        return None

    broker_t = cast(BrokerBase, broker)

    if is_options_symbol(symbol):
        parts = symbol.split("_")
        if len(parts) < 3:
            trading_logger.log_error(f"Malformed option symbol for underlying lookup: {symbol}")
            return None

        try:
            opt_root = parts[0]
            opt_expiry = parts[2]
            fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange)
            underlying_fut_symbol = f"{opt_root}_FUT_{fut_expiry}___"
            time_key: Optional[str] = None
            if time is not None:
                time_key = time.replace(second=0, microsecond=0).strftime("%Y%m%d_%H%M")
                cached = get_price_cache(underlying_fut_symbol, time_key)
                if cached is not None:
                    return float(cached)

            underlying_price = get_option_underlying_price(
                [broker_t],
                symbol,
                opt_expiry,
                fut_expiry=fut_expiry,
                exchange=exchange,
                as_of=time,
            )
            if underlying_price is None:
                return None

            if time_key is not None:
                set_price_cache(underlying_fut_symbol, time_key, underlying_price)
            return float(underlying_price)
        except Exception:
            pass
        underlying_symbol = get_underlying_symbol(symbol)
        return get_price_at_time(broker_t, underlying_symbol, exchange, as_of=time, mds="mds")

    if is_futures_symbol(symbol):
        return get_future_underlying_price(broker_t, symbol, exchange, as_of=time)

    return get_price_at_time(broker_t, symbol, exchange, as_of=time, mds="mds")


def _lookup_leg_price_from_map(
    leg_prices: _LegPricesArg, leg_symbol: str, price_type: str
) -> Optional[float]:
    if leg_prices is None:
        return None
    row_prices = leg_prices.get(leg_symbol)
    if row_prices is None:
        return None
    value = row_prices.get(price_type)
    if value is None:
        return None
    return float(value)


def get_iv_for_symbol_cached(
    symbol: str, price: float, spot: float, at_time: dt.datetime, exchange: str
) -> Optional[float]:
    """
    Get implied volatility for an options symbol with caching.
    Keyed by (symbol, time_key) — same structure as utils price cache.
    No TTL, no eviction — cache is process-scoped and starts fresh on each
    tradingapi instance. IV is treated as immutable for a given symbol+minute
    since in practice each (symbol, time_key) maps to exactly one (price, spot)
    pair per attribution run.
    """
    time_key = at_time.replace(second=0, microsecond=0).strftime("%Y%m%d_%H%M")
    cache_key = (symbol, time_key)
    if cache_key in _iv_cache:
        return _iv_cache[cache_key]
    iv = get_iv_for_symbol(symbol, price, spot, at_time, exchange)
    if iv is not None:
        _set_iv_cache(symbol, time_key, iv)
    return iv


def row_from_attribution_data(
    symbol: str,
    entry_time: Union[dt.datetime, str, pd.Timestamp],
    exit_time: Union[dt.datetime, str, pd.Timestamp],
    entry_price: float,
    exit_price: float,
    entry_quantity: float,
    *,
    entry_keys: Optional[str] = None,
    exit_keys: Optional[str] = None,
    mtm: Optional[float] = None,
) -> pd.Series:
    """
    Build a row (pd.Series) suitable for calculate_attribution_for_trade.

    Single place to document and enforce the minimal row shape for attribution.
    """
    data: Dict[str, Any] = {
        "symbol": symbol,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "entry_quantity": entry_quantity,
    }
    if entry_keys is not None:
        data["entry_keys"] = entry_keys
    if exit_keys is not None:
        data["exit_keys"] = exit_keys
    if mtm is not None:
        data["mtm"] = mtm
    return pd.Series(data)


def _get_leg_price_from_broker_row(
    broker: object,
    row: Union[pd.Series, Dict[str, Any]],
    leg_symbol: str,
    price_type: str,
) -> Optional[float]:
    """
    Get per-leg price from Redis when broker has redis_o and row has entry_keys/exit_keys.
    For open positions (no exit_keys), fall back to current market price.
    """
    try:
        entry_keys_str = str(row.get("entry_keys", "") or "")
        exit_keys_str = str(row.get("exit_keys", "") or "")
        entry_keys = entry_keys_str.split() if entry_keys_str else []
        exit_keys = exit_keys_str.split() if exit_keys_str else []

        redis_o = getattr(broker, "redis_o", None)
        if redis_o is None:
            if price_type == "exit" and not exit_keys:
                exchange = get_exchange(leg_symbol)
                price = get_price_at_time(
                    cast(BrokerBase, broker), leg_symbol, exchange, as_of=None, mds="mds", last=True
                )
                if price is not None:
                    return float(price)
            return None

        def get_redis_conn_for_key(key: str) -> Optional[object]:
            try:
                broker_name = redis_o.hget(key, "broker")  # type: ignore[union-attr]
                if broker_name:
                    broker_name = broker_name.decode() if hasattr(broker_name, "decode") else str(broker_name)
                    broker_inst = getattr(broker, "broker", None)
                    if broker_inst and getattr(broker_inst, "name", "").upper() == broker_name:
                        return redis_o
            except Exception:
                pass
            return None

        if price_type == "entry":
            for key in entry_keys:
                conn = get_redis_conn_for_key(key)
                if conn is None:
                    continue
                try:
                    stored = conn.hget(key, "long_symbol")  # type: ignore[union-attr]
                    if stored:
                        stored = stored.decode() if hasattr(stored, "decode") else str(stored)
                        if stored == leg_symbol:
                            p = conn.hget(key, "price")  # type: ignore[union-attr]
                            if p is not None:
                                return float(p)
                except Exception:
                    continue
            return None

        if price_type == "exit":
            for key in exit_keys:
                conn = get_redis_conn_for_key(key)
                if conn is None:
                    continue
                try:
                    stored = conn.hget(key, "long_symbol")  # type: ignore[union-attr]
                    if stored:
                        stored = stored.decode() if hasattr(stored, "decode") else str(stored)
                        if stored == leg_symbol:
                            p = conn.hget(key, "price")  # type: ignore[union-attr]
                            if p is not None:
                                return float(p)
                except Exception:
                    continue
            # Open position: current market price
            try:
                exchange = get_exchange(leg_symbol)
                price = get_price_at_time(
                    cast(BrokerBase, broker), leg_symbol, exchange, as_of=None, mds="mds", last=True
                )
                if price is not None:
                    return float(price)
            except Exception:
                pass
    except Exception:
        pass
    return None


def get_per_leg_entry_prices(
    broker: object,
    row: Union[pd.Series, Dict[str, Any]],
) -> Dict[str, float]:
    """
    Get per-leg entry price for a combo/single-leg row from Redis (entry_keys) or row fallback.
    Returns dict leg_symbol -> price; only includes legs for which a price was found.
    Use with get_greeks_at_time(..., leg_prices=...) to compute greeks at entry using fill price.
    """
    out: Dict[str, float] = {}
    combo = row.get("symbol", "")
    legs = _parse_combo_symbol(combo)
    for leg_symbol in legs:
        p = _get_leg_price_from_broker_row(broker, row, leg_symbol, "entry")
        if p is not None:
            out[leg_symbol] = float(p)
    return out


# ============================================================================
# Attribution calculation (core logic)
# ============================================================================


def calculate_spot_attribution(
    symbol: str,
    entry_price: float,
    exit_price: float,
    entry_time: dt.datetime,
    exit_time: Optional[dt.datetime],
    quantity: float,
    underlying_prices: _UnderlyingPricesArg,
    broker: Optional[object],
) -> float:
    """
    Calculate spot attribution for stocks and futures.

    For stocks: spot_attrib = (exit_price - entry_price) * quantity
    For futures: spot_attrib = (underlying_t1 - underlying_t0) * quantity (underlying movement)
    """
    try:
        if is_stock_symbol(symbol):
            return (exit_price - entry_price) * quantity

        if is_futures_symbol(symbol):
            exchange = get_exchange(symbol)
            underlying_t0 = _get_underlying_price(
                symbol, exchange, entry_time, underlying_prices=underlying_prices, broker=broker
            )
            underlying_t1 = _get_underlying_price(
                symbol, exchange, exit_time, underlying_prices=underlying_prices, broker=broker
            )
            if underlying_t0 is None or underlying_t1 is None:
                return 0.0
            return (underlying_t1 - underlying_t0) * quantity

        return 0.0
    except Exception:
        return 0.0


def calculate_spread_attribution(
    symbol: str,
    entry_price: float,
    exit_price: float,
    entry_time: dt.datetime,
    exit_time: Optional[dt.datetime],
    quantity: float,
    underlying_prices: _UnderlyingPricesArg,
    broker: Optional[object],
) -> float:
    """
    Calculate spread attribution for futures trades.
    Spread attribution = (spread_t1 - spread_t0) * quantity
    where spread = futures_price - underlying_spot.
    """
    if not is_futures_symbol(symbol):
        return 0.0

    try:
        exchange = get_exchange(symbol)
        underlying_t0 = _get_underlying_price(
            symbol, exchange, entry_time, underlying_prices=underlying_prices, broker=broker
        )
        underlying_t1 = _get_underlying_price(
            symbol, exchange, exit_time, underlying_prices=underlying_prices, broker=broker
        )
        if underlying_t0 is None or underlying_t1 is None:
            return 0.0
        spread_t0 = entry_price - underlying_t0
        spread_t1 = exit_price - underlying_t1
        return (spread_t1 - spread_t0) * quantity
    except Exception:
        return 0.0


def calculate_attribution_for_trade(
    row: Union[pd.Series, Dict[str, Any]],
    broker: Optional[object] = None,
    *,
    underlying_prices: _UnderlyingPricesArg = None,
    leg_prices: _LegPricesArg = None,
    current_time: Optional[dt.datetime] = None,
    day: bool = True,
    refresh_mapping: bool = False,
) -> Dict[str, Any]:
    """
    Calculate attribution for a single trade.

    Row is the single input; underlying_prices and leg_prices can be dicts or None
    (derived from row and broker when None). current_time defaults to row["exit_time"] or now.

    When day=True (default): use day-level attribution with MTM for carried-forward trades
    (mtm_entry_price / mtm_exit_price for prior day close and current day close/mark).
    When day=False: use trade-level attribution from row entry/exit prices and times.

    refresh_mapping: when False (default), broker does not refresh symbol mapping on historical
    fetches (faster for batch). Set True to refresh mapping on each get_historical_close_price.

    Returns attribution by symbol type:
    - _STK_: only spot_attrib
    - _FUT_: spot_attrib, spread_attrib
    - _OPT_: spot_attrib, vol_attrib, timedecay_attrib

    Note:
        The broker passed in should be the same broker that holds the trade, as we need
        traded prices for combo legs (e.g. per-leg entry/exit from Redis or broker).
    """
    if isinstance(row, dict):
        row = pd.Series(row)
    result: dict[str, Any] = {
        "spot_attrib": 0.0,
        "vol_attrib": 0.0,
        "timedecay_attrib": 0.0,
        "spread_attrib": 0.0,
        "per_leg": {},
    }
    get_iv_fn = get_iv_for_symbol_cached

    try:
        combo_symbol = row["symbol"]
        entry_time = pd.to_datetime(row["entry_time"])
        exit_time_str = row.get("exit_time", "")

        if current_time is None:
            if exit_time_str == "" or exit_time_str == "0" or pd.isna(exit_time_str):
                current_time = dt.datetime.now()
            else:
                current_time = pd.to_datetime(exit_time_str)

        if exit_time_str == "" or exit_time_str == "0" or pd.isna(exit_time_str):
            exit_time = current_time
        else:
            exit_time = pd.to_datetime(exit_time_str)

        market_close_time = exit_time.replace(hour=15, minute=30, second=0, microsecond=0)
        if exit_time > market_close_time:
            exit_time = market_close_time

        # Naive datetimes for chameli
        entry_dt = get_naive_dt(cast(Union[dt.datetime, pd.Timestamp], entry_time))
        exit_dt = get_naive_dt(cast(Union[dt.datetime, pd.Timestamp], exit_time))
        if isinstance(entry_dt, pd.Timestamp):
            entry_dt = entry_dt.to_pydatetime()
        if isinstance(exit_dt, pd.Timestamp):
            exit_dt = exit_dt.to_pydatetime()
        entry_dt = cast(dt.datetime, entry_dt)
        exit_dt = cast(dt.datetime, exit_dt)
        original_entry_dt = entry_dt
        original_exit_dt = exit_dt
        entry_time_shifted = False
        exit_time_shifted = False
        no_exit_in_row = exit_time_str == "" or exit_time_str == "0" or pd.isna(exit_time_str)
        effective_open = no_exit_in_row

        entry_price = row.get("entry_price", 0.0)
        exit_price = row.get("exit_price", 0.0)
        if exit_price == 0 or exit_price is None:
            exit_price = row.get("mtm", entry_price)

        # Day-level attribution: compute effective attribution window and rely on as_of/live lookups downstream.
        if day and broker is not None and current_time is not None:
            attribution_date = current_time.date() if hasattr(current_time, "date") else get_naive_dt(current_time).date()  # type: ignore[union-attr]
            attribution_date_str = attribution_date.strftime("%Y%m%d")
            entry_price = mtm_entry_price(row, attribution_date, broker, refresh_mapping=refresh_mapping)
            exit_price = mtm_exit_price(row, attribution_date, broker, refresh_mapping=refresh_mapping)
            entry_date = entry_dt.date() if hasattr(entry_dt, "date") else entry_dt
            if attribution_date < entry_date:
                return result  # Trade did not exist on attribution day; zero attribution for this day
            if entry_date < attribution_date:
                prior = advance_by_biz_days(attribution_date_str, -1)
                prior_date_str = (
                    str(prior)
                    if isinstance(prior, str)
                    else (prior.strftime("%Y%m%d") if hasattr(prior, "strftime") else str(prior))
                )
                if len(prior_date_str) == 10 and prior_date_str[4] == "-":
                    prior_date_str = prior_date_str.replace("-", "")
                entry_dt = dt.datetime.strptime(prior_date_str + " 15:30:00", "%Y%m%d %H:%M:%S")
            market_close_attr = dt.datetime.strptime(attribution_date_str + " 15:30:00", "%Y%m%d %H:%M:%S")
            current_naive = get_naive_dt(current_time)
            current_dt = (
                current_naive.to_pydatetime()
                if isinstance(current_naive, pd.Timestamp)
                else cast(dt.datetime, current_naive)
            )
            if attribution_date == entry_date and current_dt < entry_dt:
                return result  # Current time is before trade entry; no attribution yet

            if not no_exit_in_row and hasattr(exit_dt, "date"):
                exit_date = exit_dt.date()
                if exit_date < attribution_date:
                    return result  # Trade already closed before attribution day; zero attribution for this day
                if exit_date == attribution_date:
                    if current_dt < exit_dt:
                        effective_open = True
                        exit_dt = current_dt
                    else:
                        effective_open = False
                else:
                    effective_open = True
                    exit_dt = (
                        current_dt
                        if (attribution_date == dt.date.today() and current_dt < market_close_attr)
                        else market_close_attr
                    )
            else:
                effective_open = True
                exit_dt = current_dt if current_dt < market_close_attr else market_close_attr

            entry_time_shifted = entry_dt != original_entry_dt
            exit_time_shifted = exit_dt != original_exit_dt

        # Minute-align entry/exit for all spot/IV lookups and performance_attribution (historical data is 1m only)
        entry_dt = entry_dt.replace(second=0, microsecond=0)
        exit_dt = exit_dt.replace(second=0, microsecond=0)
        # Effective-open positions use live exit-side lookup (as_of=None); closed positions use as_of=exit_dt.
        exit_lookup_time: Optional[dt.datetime] = None if effective_open else exit_dt

        legs = _parse_combo_symbol(combo_symbol)
        if len(legs) == 0:
            return result

        has_stocks = any(is_stock_symbol(leg) for leg in legs.keys())
        has_futures = any(is_futures_symbol(leg) for leg in legs.keys())
        has_options = any(is_options_symbol(leg) for leg in legs.keys())

        entry_qty = row["entry_quantity"]

        if has_stocks and not has_futures and not has_options:
            for leg_symbol, leg_qty in legs.items():
                if is_stock_symbol(leg_symbol):
                    signed_qty = leg_qty * float(entry_qty)
                    spot_attrib = calculate_spot_attribution(
                        leg_symbol,
                        entry_price,
                        exit_price,
                        entry_dt,
                        exit_lookup_time,
                        signed_qty,
                        underlying_prices,
                        broker,
                    )
                    result["spot_attrib"] += spot_attrib

        elif has_futures and not has_options:
            for leg_symbol, leg_qty in legs.items():
                if is_futures_symbol(leg_symbol):
                    signed_qty = leg_qty * float(entry_qty)
                    spot_attrib = calculate_spot_attribution(
                        leg_symbol,
                        entry_price,
                        exit_price,
                        entry_dt,
                        exit_lookup_time,
                        signed_qty,
                        underlying_prices,
                        broker,
                    )
                    result["spot_attrib"] += spot_attrib
                    spread_attrib = calculate_spread_attribution(
                        leg_symbol,
                        entry_price,
                        exit_price,
                        entry_dt,
                        exit_lookup_time,
                        signed_qty,
                        underlying_prices,
                        broker,
                    )
                    result["spread_attrib"] += spread_attrib

        elif has_options:
            adjusted_legs = {leg_symbol: int(leg_qty * float(entry_qty)) for leg_symbol, leg_qty in legs.items()}
            underlying_t0_dict = {}
            underlying_t1_dict = {}
            ivs_t0 = {}
            ivs_t1 = {}

            for leg_symbol in legs:
                exchange_leg = get_exchange(leg_symbol)

                underlying_t0 = _get_underlying_price(
                    leg_symbol, exchange_leg, entry_dt, underlying_prices=underlying_prices, broker=broker
                )
                underlying_t1 = _get_underlying_price(
                    leg_symbol, exchange_leg, exit_lookup_time, underlying_prices=underlying_prices, broker=broker
                )
                if underlying_t0 is None or underlying_t1 is None:
                    trading_logger.log_debug(
                        f"Could not get underlying prices for combo leg {leg_symbol}, skipping attribution"
                    )
                    return result
                underlying_t0_dict[leg_symbol] = underlying_t0
                underlying_t1_dict[leg_symbol] = underlying_t1

                leg_entry_price = None
                leg_exit_price = None
                if entry_time_shifted and broker is not None:
                    p = get_price_at_time(cast(BrokerBase, broker), leg_symbol, exchange_leg, as_of=entry_dt, mds="mds")
                    if p is not None:
                        leg_entry_price = p
                if (exit_time_shifted or effective_open) and broker is not None:
                    p = get_price_at_time(
                        cast(BrokerBase, broker),
                        leg_symbol,
                        exchange_leg,
                        as_of=None if effective_open else exit_dt,
                        mds="mds",
                    )
                    if p is not None:
                        leg_exit_price = p
                if leg_entry_price is None:
                    leg_entry_price = _lookup_leg_price_from_map(leg_prices, leg_symbol, "entry")
                if leg_exit_price is None:
                    leg_exit_price = _lookup_leg_price_from_map(leg_prices, leg_symbol, "exit")
                if leg_entry_price is None and len(legs) > 1 and broker is not None:
                    leg_entry_price = _get_leg_price_from_broker_row(broker, row, leg_symbol, "entry")
                if leg_exit_price is None and len(legs) > 1 and broker is not None:
                    leg_exit_price = _get_leg_price_from_broker_row(broker, row, leg_symbol, "exit")
                if leg_entry_price is None and len(legs) == 1:
                    leg_entry_price = entry_price
                if leg_exit_price is None and len(legs) == 1:
                    leg_exit_price = exit_price

                if leg_entry_price is None:
                    trading_logger.log_debug(
                        f"Could not get per-leg entry price for combo leg {leg_symbol}, skipping attribution"
                    )
                    return result
                if leg_exit_price is None:
                    trading_logger.log_debug(
                        f"Could not get per-leg exit price for combo leg {leg_symbol}, skipping attribution"
                    )
                    return result

                iv_0 = get_iv_fn(leg_symbol, leg_entry_price, underlying_t0, entry_dt, exchange_leg)
                iv_1 = get_iv_fn(leg_symbol, leg_exit_price, underlying_t1, exit_dt, exchange_leg)
                if iv_0 is None or iv_1 is None:
                    trading_logger.log_debug(f"Could not get IVs for combo leg {leg_symbol}, skipping attribution")
                    return result
                ivs_t0[leg_symbol] = iv_0
                ivs_t1[leg_symbol] = iv_1

            # Pass signed quantities so chameli computes attribution for actual position (short = negative qty)
            entry_qty_f = float(entry_qty)
            signed_legs = {leg: (qty if entry_qty_f >= 0 else -abs(qty)) for leg, qty in adjusted_legs.items()}
            combo_parts = [f"{leg}?{qty}" for leg, qty in signed_legs.items()]
            combo_symbol_str = ":".join(combo_parts)
            first_leg = next(iter(legs.keys()))
            exchange = get_exchange(first_leg)

            try:
                attrib = performance_attribution(
                    combo_symbol=combo_symbol_str,
                    spot_t0=underlying_t0_dict[first_leg],
                    spot_t1=underlying_t1_dict[first_leg],
                    ivs_t0=ivs_t0,
                    ivs_t1=ivs_t1,
                    t0=entry_dt,
                    t1=exit_dt,
                    r=0,
                    exchange=exchange,
                )
                underlying_attrib = attrib.get("underlying_attrib", 0.0) or 0.0
                vol_attrib = attrib.get("vol_attrib", 0.0) or 0.0
                timedecay_attrib = attrib.get("timedecay_attrib", 0.0) or 0.0
                result["spot_attrib"] = 0.0 if _is_missing_number(underlying_attrib) else underlying_attrib
                result["vol_attrib"] = 0.0 if _is_missing_number(vol_attrib) else vol_attrib
                result["timedecay_attrib"] = 0.0 if _is_missing_number(timedecay_attrib) else timedecay_attrib
                # Per-leg breakdown (sums to total); keys match chameli per_leg
                per_leg_raw = attrib.get("per_leg") or {}
                for sym, leg_data in per_leg_raw.items():
                    q = float(leg_data.get("qty", 0))
                    u0 = float(leg_data.get("underlying_scenario_p0", 0))
                    u1 = float(leg_data.get("underlying_scenario_p1", 0))
                    v0 = float(leg_data.get("vol_scenario_p0", 0))
                    v1 = float(leg_data.get("vol_scenario_p1", 0))
                    t0_ = float(leg_data.get("timedecay_scenario_p0", 0))
                    t1_ = float(leg_data.get("timedecay_scenario_p1", 0))
                    result["per_leg"][sym] = {
                        "spot_attrib": q * (u1 - u0),
                        "vol_attrib": q * (v1 - v0),
                        "timedecay_attrib": q * (t1_ - t0_),
                        "spread_attrib": 0.0,
                    }
            except Exception as e:
                trading_logger.log_debug(f"Error in performance_attribution for combo {combo_symbol}: {e}")

    except Exception:
        pass

    return result


def calculate_attribution_for_trades(
    rows: Union[pd.DataFrame, List[Union[pd.Series, Dict[str, Any]]]],
    broker: Optional[object] = None,
    *,
    underlying_prices: _UnderlyingPricesArg = None,
    leg_prices: _LegPricesArg = None,
    current_time: Optional[dt.datetime] = None,
    day: bool = True,
    refresh_mapping: bool = False,
) -> List[Dict[str, Any]]:
    """
    Calculate attribution for multiple trades.

    Args:
        rows: DataFrame (iterated by rows) or list of row-like dicts/Series.
        broker, underlying_prices, leg_prices, current_time, day, refresh_mapping: same as calculate_attribution_for_trade.

    Returns:
        List of attribution dicts (one per row), in the same order as rows.
    """
    if isinstance(rows, pd.DataFrame):
        row_list = [rows.loc[i] for i in range(len(rows))]
    else:
        row_list = list(rows)
    out: List[Dict[str, Any]] = []
    for row in row_list:
        row_arg = cast(Union[pd.Series, Dict[str, Any]], row)
        attrib = calculate_attribution_for_trade(
            row_arg,
            broker=broker,
            underlying_prices=underlying_prices,
            leg_prices=leg_prices,
            current_time=current_time,
            day=day,
            refresh_mapping=refresh_mapping,
        )
        out.append(attrib)
    return out


# ============================================================================
# MTM (Mark-to-Market) calculation utilities
# ============================================================================


def fork_partial_trades_in_dataframe(pnl: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Fork partially filled trades into closed and open rows.
    For each trade where entry_quantity + exit_quantity != 0:
    creates a closed row and an open row; removes the original partial row.
    """
    pnl = pnl.copy()
    pnl.reset_index(drop=True, inplace=True)

    partial_mask = (
        (pnl.entry_quantity != 0) & (pnl.exit_quantity != 0) & (abs(pnl.entry_quantity + pnl.exit_quantity) > 0.001)
    )
    partial_trades = pnl[partial_mask].copy()
    if partial_trades.empty:
        return pnl

    trading_logger.log_info(f"Forking {len(partial_trades)} partial trade(s) in dataframe for {date_str}")
    new_rows = []
    rows_to_drop = []

    for idx, row in partial_trades.iterrows():
        entry_quantity = float(row["entry_quantity"])
        exit_quantity = float(row["exit_quantity"])
        closed_quantity = -exit_quantity
        open_quantity = entry_quantity + exit_quantity

        entry_keys_str = str(row.get("entry_keys", ""))
        entry_keys = entry_keys_str.split() if entry_keys_str else []
        max_fork = 0
        for ek in entry_keys:
            for _, other_row in pnl.iterrows():
                other_entry_keys_str = str(other_row.get("entry_keys", ""))
                other_entry_keys = other_entry_keys_str.split() if other_entry_keys_str else []
                for other_ek in other_entry_keys:
                    if other_ek.startswith(ek) and len(other_ek) > len(ek):
                        suffix = other_ek[len(ek):]
                        if suffix.startswith("F") and suffix[1:].isdigit():
                            max_fork = max(max_fork, int(suffix[1:]))
        fork_num = max_fork + 1
        forked_entry_keys = [f"{ek}F{fork_num}" for ek in entry_keys] if entry_keys else []

        closed_row = row.copy()
        closed_row["exit_quantity"] = closed_quantity
        new_rows.append(closed_row)

        open_row = row.copy()
        open_row["entry_quantity"] = open_quantity
        open_row["exit_quantity"] = 0.0
        open_row["exit_time"] = ""
        open_row["exit_price"] = 0.0
        open_row["entry_keys"] = " ".join(forked_entry_keys) if forked_entry_keys else ""
        open_row["exit_keys"] = ""
        original_int_order_id = str(row.get("int_order_id", ""))
        open_row["int_order_id"] = (
            f"{original_int_order_id}_F{fork_num}" if original_int_order_id else f"FORKED_F{fork_num}"
        )
        new_rows.append(open_row)
        rows_to_drop.append(idx)
        trading_logger.log_debug(
            f"  Forked {row.get('int_order_id', 'unknown')}: closed_qty={closed_quantity}, open_qty={open_quantity}, fork_suffix=F{fork_num}"
        )

    pnl = pnl.drop(index=rows_to_drop)
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        pnl = pd.concat([pnl, new_df], ignore_index=True)
        pnl.reset_index(drop=True, inplace=True)
        trading_logger.log_info(f"  Added {len(new_rows)} forked trade row(s)")
    return pnl


def mtm_entry_price(
    row: pd.Series,
    mtm_date: Union[dt.date, str],
    broker: Optional[object] = None,
    *,
    refresh_mapping: bool = False,
) -> float:
    """
    Get MTM entry price for a trade row on a given date.

    For positions entered before mtm_date, returns the EOD close for the day before mtm_date.
    For positions entered on or after mtm_date, returns the entry_price.

    Args:
        row: DataFrame row with entry_price, entry_time, symbol
        mtm_date: Date for MTM (dt.date or YYYYMMDD str)
        broker: Broker instance for get_historical_close_price
        refresh_mapping: Passed to get_historical_close_price (default False).

    Returns:
        MTM entry price (float)
    """
    entry_price = float(row.get("entry_price", 0.0))
    if broker is None:
        return entry_price

    if isinstance(mtm_date, dt.date):
        mtm_date_str = mtm_date.strftime("%Y%m%d")
    else:
        mtm_date_str = str(mtm_date)

    entry_time_str = row.get("entry_time", "")
    if pd.isna(entry_time_str) or entry_time_str == "" or entry_time_str == "0":
        return entry_price

    try:
        entry_time = pd.to_datetime(entry_time_str)
        entry_date = entry_time.date() if hasattr(entry_time, "date") else entry_time
        if entry_date.strftime("%Y%m%d") >= mtm_date_str:
            return entry_price

        prior = advance_by_biz_days(mtm_date_str, -1)
        prior_date_str = (
            str(prior)
            if isinstance(prior, str)
            else (prior.strftime("%Y%m%d") if hasattr(prior, "strftime") else str(prior))
        )
        if len(prior_date_str) == 10 and prior_date_str[4] == "-":
            prior_date_str = prior_date_str.replace("-", "")
        symbol = str(row.get("symbol", ""))
        if not symbol:
            return entry_price

        try:
            exchange = "BSE" if "SENSEX" in symbol else "NSE"
            prior_eod_dt = dt.datetime.strptime(prior_date_str + " 15:29:00", "%Y%m%d %H:%M:%S")

            if ":" in symbol:
                legs = parse_combo_symbol(symbol)
                combo_price = 0.0
                for leg_symbol, quantity in legs.items():
                    leg_price = get_price_at_time(
                        cast(BrokerBase, broker),
                        leg_symbol,
                        exchange,
                        as_of=prior_eod_dt,
                        mds="mds",
                        refresh_mapping=refresh_mapping,
                    )
                    if leg_price is None:
                        return entry_price
                    combo_price += leg_price * quantity
                return float(combo_price)

            mtm_price = get_price_at_time(
                cast(BrokerBase, broker),
                symbol,
                exchange,
                as_of=prior_eod_dt,
                mds="mds",
                refresh_mapping=refresh_mapping,
            )
            if mtm_price is not None:
                return float(mtm_price)
        except Exception:
            pass
        return entry_price
    except Exception:
        return entry_price


def mtm_exit_price(
    row: pd.Series,
    mtm_date: Union[dt.date, str],
    broker: Optional[object] = None,
    *,
    refresh_mapping: bool = False,
) -> float:
    """
    Get MTM exit price for a trade row on a given date.

    For open positions on mtm_date: current MTM (market price if today before 15:30, else historical close).
    For closed positions: exit_price.

    refresh_mapping: Passed to get_historical_close_price (default False).
    """
    exit_price = float(row.get("exit_price", 0.0))
    entry_price = float(row.get("entry_price", 0.0))
    symbol = row.get("symbol", "")
    exchange = "BSE" if "SENSEX" in symbol else "NSE"
    exit_time_str = row.get("exit_time", "")

    if isinstance(mtm_date, dt.date):
        mtm_date_str = mtm_date.strftime("%Y%m%d")
        mtm_date_obj = mtm_date
    else:
        mtm_date_str = str(mtm_date)
        if len(mtm_date_str) == 10 and mtm_date_str[4] == "-":
            mtm_date_str = mtm_date_str.replace("-", "")
        try:
            mtm_date_obj = dt.datetime.strptime(mtm_date_str, "%Y%m%d").date()
        except ValueError:
            mtm_date_obj = None

    mtm_date_1530 = mtm_date_str + " 15:30:00"
    today = dt.date.today()

    if broker is None:
        trading_logger.log_error("mtm_exit_price: broker is None, falling back to entry_price")
        return entry_price

    use_mtm_exit = False
    if pd.isna(exit_time_str) or exit_time_str == "" or exit_time_str == "0":
        use_mtm_exit = True
    else:
        try:
            exit_time_dt = parse_datetime(exit_time_str)
            mtm_date_dt = parse_datetime(mtm_date_1530)
            use_mtm_exit = exit_time_dt > mtm_date_dt
        except (ValueError, TypeError):
            use_mtm_exit = False

    def _combo_or_single_price(as_of: Optional[dt.datetime], *, last: bool = False) -> Optional[float]:
        if ":" in symbol:
            legs = parse_combo_symbol(symbol)
            total = 0.0
            for leg_symbol, quantity in legs.items():
                leg_exch = "BSE" if "SENSEX" in leg_symbol else "NSE"
                p = get_price_at_time(
                    cast(BrokerBase, broker),
                    leg_symbol,
                    leg_exch,
                    as_of=as_of,
                    mds="mds",
                    last=last,
                    refresh_mapping=refresh_mapping,
                )
                if p is None:
                    return None
                total += p * quantity
            return total
        exch = "BSE" if "SENSEX" in symbol else "NSE"
        return get_price_at_time(
            cast(BrokerBase, broker), symbol, exch, as_of=as_of, mds="mds", last=last, refresh_mapping=refresh_mapping
        )

    if use_mtm_exit:
        if mtm_date_obj and mtm_date_obj == today:
            current_time = dt.datetime.now().time()
            market_close_time = dt.time(15, 30, 0)
            if current_time < market_close_time:
                try:
                    price = _combo_or_single_price(None, last=True)
                    if price is not None:
                        return float(price)
                except Exception as e:
                    trading_logger.log_warning(
                        f"Error getting current market price for {symbol}: {e}, falling back to historical"
                    )

            try:
                mtm_eod_dt = dt.datetime.strptime(mtm_date_str + " 15:29:00", "%Y%m%d %H:%M:%S")
                mtm_price = _combo_or_single_price(mtm_eod_dt, last=False)
                if mtm_price is not None:
                    return float(mtm_price)
                return entry_price
            except Exception as e:
                trading_logger.log_warning(f"Error fetching historical data for {symbol} on {mtm_date_str}: {e}")
                return entry_price
        else:
            try:
                mtm_eod_dt = dt.datetime.strptime(mtm_date_str + " 15:29:00", "%Y%m%d %H:%M:%S")
                mtm_price = _combo_or_single_price(mtm_eod_dt, last=False)
                if mtm_price is not None:
                    return float(mtm_price)
                return entry_price
            except Exception as e:
                trading_logger.log_warning(f"Error fetching historical data for {symbol} on {mtm_date_str}: {e}")
                return entry_price
    else:
        return exit_price


def mtm_df(
    pnl: pd.DataFrame,
    mtm_date: Union[dt.date, str],
    broker: Optional[object] = None,
    *,
    refresh_mapping: bool = False,
) -> pd.DataFrame:
    """
    Calculate MTM for each row on a given date.
    Forks partial trades first, then sets entry_price (MTM entry), mtm (MTM exit), exit_price = mtm per row.

    Args:
        pnl: P&L DataFrame with trades
        mtm_date: Date for MTM (dt.date or YYYYMMDD str)
        broker: Broker instance for historical/current prices
        refresh_mapping: Passed to mtm_entry_price/mtm_exit_price (default False).

    Returns:
        DataFrame with entry_price, mtm, exit_price populated
    """
    pnl = pnl.copy()
    pnl.reset_index(drop=True, inplace=True)

    if isinstance(mtm_date, dt.date):
        mtm_date_str = mtm_date.strftime("%Y%m%d")
    else:
        mtm_date_str = str(mtm_date)
    pnl = fork_partial_trades_in_dataframe(pnl, mtm_date_str)

    price_columns = ["entry_price", "mtm", "exit_price"]
    for col in price_columns:
        if col in pnl.columns:
            pnl[col] = pnl[col].astype(float)

    for index, row in pnl.iterrows():
        pnl.at[index, "entry_price"] = mtm_entry_price(row, mtm_date, broker, refresh_mapping=refresh_mapping)
        pnl.at[index, "mtm"] = mtm_exit_price(row, mtm_date, broker, refresh_mapping=refresh_mapping)
        pnl.at[index, "exit_price"] = pnl.loc[index, "mtm"]  # type: ignore
    return pnl
