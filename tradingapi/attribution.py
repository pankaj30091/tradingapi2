"""
Attribution and MTM (Mark-to-Market) calculation for P&L.

This module provides:
- get_historical_close_price: EOD close for a symbol (ohlcutils first, then broker API; combo support; cache)
- MTM entry/exit prices for carried-over trades and current prices for today
- Attribution (spot, vol, timedecay, spread) per trade
- Real-time spot/IV fetching with caching; use calculate_attribution_for_trade(row, broker=...) for live use

Self-sufficient: defines get_historical_close_price here; uses tradingapi.utils (get_price, get_mid_price,
parse_combo_symbol, historical_to_dataframes, parse_datetime) and broker.get_historical for intraday spot;
chameli for IV and performance_attribution (options).
"""

from __future__ import annotations

import datetime as dt
import time as _time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import pandas as pd

from tradingapi import trading_logger
from tradingapi.broker_base import BrokerBase
from tradingapi.utils import (
    get_mid_price,
    get_option_underlying_price,
    get_price,
    historical_to_dataframes,
    parse_combo_symbol,
    parse_datetime,
)

from chameli.dateutils import advance_by_biz_days, format_datetime, get_expiry, get_naive_dt
from chameli.europeanoptions import _parse_combo_symbol, calc_greeks, performance_attribution

# Optional ohlcutils for get_historical_close_price (local data first)
try:
    from ohlcutils.data import load_symbol
    from ohlcutils.enums import Periodicity

    _OHLCUTILS_AVAILABLE = True
except ImportError:
    _OHLCUTILS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Historical close price: broker cache (Shoonya -> FivePaisa) and price cache
# ---------------------------------------------------------------------------
_historical_broker_cache: Dict[str, object] = {}
_HISTORICAL_BROKER_REDIS_DB = 0
_historical_close_price_cache: Dict[Tuple[str, str], Optional[float]] = {}


def clear_historical_close_price_cache() -> None:
    """Clear the module-level historical close price cache (e.g. for tests or explicit teardown)."""
    _historical_close_price_cache.clear()


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
) -> Optional[float]:
    """
    Get historical close price for a symbol on a specific date.
    Uses an internal cache. First tries ohlcutils (local data) if available, then broker API.
    If broker is Shoonya, FivePaisa is used for the API fallback (db=0, cached).
    Supports combo symbols (SYMBOL1?QTY1:SYMBOL2?QTY2:...); decomposes and combines weighted by quantity.
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

    cache_key = (symbol, date_str)
    if cache_key in _historical_close_price_cache:
        trading_logger.log_debug(f"Using cached price for {symbol} on {date_str}")
        return _historical_close_price_cache[cache_key]

    if ":" in symbol:
        try:
            legs = parse_combo_symbol(symbol)
            combo_price = 0.0
            for leg_symbol, quantity in legs.items():
                leg_price = get_historical_close_price(broker, leg_symbol, date_str, exchange)
                if leg_price is None:
                    trading_logger.log_warning(
                        f"Could not get price for combo leg {leg_symbol} on {date_str}"
                    )
                    _historical_close_price_cache[cache_key] = None
                    return None
                combo_price += leg_price * quantity
            _historical_close_price_cache[cache_key] = combo_price
            return combo_price
        except Exception as e:
            trading_logger.log_warning(f"Error parsing combo symbol {symbol} on {date_str}: {e}")
            _historical_close_price_cache[cache_key] = None
            return None

    if _OHLCUTILS_AVAILABLE:
        try:
            date_obj = parse_datetime(date_str).date()
            date_str_formatted = date_obj.strftime("%Y-%m-%d")
            market_close_time = dt.datetime.combine(date_obj, dt.time(15, 30, 0))
            df = load_symbol(
                symbol,
                start_time=date_str_formatted,
                end_time=date_str_formatted + " 23:59:59",
                src=Periodicity.PERMIN,
                exchange=exchange,
                market_close_time="15:30:00",
            )
            if df is not None and not df.empty:
                import pytz

                tz = pytz.timezone("Asia/Kolkata")
                market_close_time_tz = tz.localize(market_close_time) if market_close_time.tzinfo is None else market_close_time
                if df.index.tz is None:  # type: ignore[union-attr]
                    df_index_tz = df.index.tz_localize(tz)  # type: ignore[union-attr]
                else:
                    df_index_tz = df.index
                filtered_df = df[df_index_tz <= market_close_time_tz]
                if not filtered_df.empty:
                    if "close" in filtered_df.columns:
                        close_price = float(filtered_df["close"].iloc[-1])
                        _historical_close_price_cache[cache_key] = close_price
                        return close_price
                    if "asettle" in filtered_df.columns:
                        close_price = float(filtered_df["asettle"].iloc[-1])
                        _historical_close_price_cache[cache_key] = close_price
                        return close_price
        except Exception as e:
            trading_logger.log_debug(f"ohlcutils load failed for {symbol} on {date_str}: {e}")

    hist_broker = _get_historical_broker(broker)
    try:
        date_obj = dt.datetime.strptime(date_str, "%Y%m%d").date()
        date_str_formatted = date_obj.strftime("%Y-%m-%d")
        hist_data = hist_broker.get_historical(  # type: ignore[union-attr]
            symbols=symbol,
            date_start=date_str_formatted,
            date_end=date_str_formatted,
            exchange=exchange,
            periodicity="1m",
            market_close_time="15:30:00",
            refresh_mapping=True,
        )
        dfs = historical_to_dataframes(hist_data)
        if dfs and not dfs[0].empty:
            df = dfs[0]
            if "close" in df.columns:
                close_price = float(df["close"].iloc[-1])
                _historical_close_price_cache[cache_key] = close_price
                return close_price
        _historical_close_price_cache[cache_key] = None
        return None
    except Exception as e:
        trading_logger.log_warning(f"Error getting historical price for {symbol} on {date_str}: {e}")
        _historical_close_price_cache[cache_key] = None
        return None


# ---------------------------------------------------------------------------
# Real-time caches (spot and IV) – used by get_current_spot_price,
# get_spot_price_at_time, get_iv_for_symbol_cached
# ---------------------------------------------------------------------------
_price_cache: Dict[Tuple[str, str, str], Tuple[float, float]] = {}  # (symbol, exchange, time_key) -> (price, ts)
_iv_cache: Dict[Tuple[str, float, float, str, str], Tuple[float, float]] = {}  # (symbol, price, spot, time_key, exch) -> (iv, ts)
_CACHE_TTL_SECONDS = 60
_IV_CACHE_TTL_SECONDS = 30


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
    NSE month-end = last Thursday (day_of_week=4); BSE = last Tuesday (day_of_week=2).
    """
    try:
        d = dt.datetime.strptime(opt_expiry_yyyymmdd, "%Y%m%d").date()
        day_of_week = 2 if exchange == "NSE" else 4  # Tuesday / Thursday
        expiry_date = get_expiry(d, weekly=0, day_of_week=day_of_week, exchange=exchange)
        return expiry_date.strftime("%Y%m%d")
    except Exception:
        return opt_expiry_yyyymmdd


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
# Real-time spot / IV fetching (with caching) – for live attribution
# ============================================================================


def get_current_spot_price(broker, underlying_symbol: str, exchange: str) -> Optional[float]:
    """Get current spot price for underlying (real-time) with caching."""
    now_ts = _time.time()
    time_key = "current"
    cache_key = (underlying_symbol, exchange, time_key)
    if cache_key in _price_cache:
        cached_price, cache_ts = _price_cache[cache_key]
        if now_ts - cache_ts < _CACHE_TTL_SECONDS:
            return cached_price
        del _price_cache[cache_key]
    try:
        quote = get_price([broker], underlying_symbol, exchange=exchange, mds=None, attempts=1)
        if quote and hasattr(quote, "last") and not pd.isna(quote.last):
            if hasattr(quote, "bid") and hasattr(quote, "ask"):
                if not pd.isna(quote.bid) and not pd.isna(quote.ask):
                    price = (quote.bid + quote.ask) / 2
                    _price_cache[cache_key] = (price, now_ts)
                    return price
            price = quote.last
            _price_cache[cache_key] = (price, now_ts)
            return price
    except Exception:
        pass
    return None


def get_spot_price_at_time(
    broker, underlying_symbol: str, exchange: str, at_time: dt.datetime
) -> Optional[float]:
    """
    Get spot price at a specific time (for real-time attribution) with caching.
    If at_time is within _CACHE_TTL_SECONDS of now, returns current spot. Otherwise
    tries broker.get_historical(1m) and closest bar close, then current spot as fallback.
    """
    now_ts = _time.time()
    today = dt.date.today()
    time_date = at_time.date() if hasattr(at_time, "date") else pd.to_datetime(at_time).date()
    now = dt.datetime.now()
    time_rounded = at_time.replace(second=0, microsecond=0)
    time_key = time_rounded.strftime("%Y%m%d_%H%M")
    cache_key = (underlying_symbol, exchange, time_key)
    if cache_key in _price_cache:
        cached_price, cache_ts = _price_cache[cache_key]
        return cached_price


    now_minus_ttl = now - dt.timedelta(seconds=_CACHE_TTL_SECONDS)
    within_ttl = now_minus_ttl <= at_time <= now
    if not within_ttl:
        try:
            date_str = at_time.strftime("%Y-%m-%d")
            hist = broker.get_historical(
                symbols=underlying_symbol,
                date_start=date_str,
                date_end=date_str,
                exchange=exchange,
                periodicity="1m",
                market_close_time="15:30:00",
                refresh_mapping=False,
            )
            if underlying_symbol.startswith("NIFTY_"):
                keys_to_replace = [k for k in hist if k.startswith("NSENIFTY_")]
                for old_key in keys_to_replace:
                    new_key = old_key.replace("NSENIFTY_", "NIFTY_", 1)
                    hist[new_key] = hist.pop(old_key)
            data = hist.get(underlying_symbol, [])
            if data:
                data = sorted(data, key=lambda x: x.date)
                target_naive = at_time.replace(tzinfo=None) if at_time.tzinfo else at_time
                candidates = []
                for x in data:
                    x_naive = x.date
                    if hasattr(x_naive, "tzinfo") and x_naive.tzinfo is not None:
                        x_naive = x_naive.replace(tzinfo=None)  # type: ignore[union-attr]
                    if x_naive <= target_naive:
                        candidates.append((x_naive, x))
                if candidates:
                    price = candidates[-1][1].close
                    _price_cache[cache_key] = (price, now_ts)
                    return price
                if data:
                    price = data[0].close
                    _price_cache[cache_key] = (price, now_ts)
                    return price
        except Exception:
            pass
    return get_current_spot_price(broker, underlying_symbol, exchange)


def get_iv_for_symbol_cached(
    symbol: str, price: float, spot: float, at_time: dt.datetime, exchange: str
) -> Optional[float]:
    """Get implied volatility for an options symbol with caching (real-time)."""
    now_ts = _time.time()
    time_rounded = at_time.replace(second=0, microsecond=0)
    time_key = time_rounded.strftime("%Y%m%d_%H%M")
    price_rounded = round(price, 2)
    spot_rounded = round(spot, 2)
    cache_key = (symbol, price_rounded, spot_rounded, time_key, exchange)
    if cache_key in _iv_cache:
        cached_iv, cache_ts = _iv_cache[cache_key]
        if now_ts - cache_ts < _IV_CACHE_TTL_SECONDS:
            return cached_iv
        del _iv_cache[cache_key]
    iv = get_iv_for_symbol(symbol, price, spot, at_time, exchange)
    if iv is not None:
        _iv_cache[cache_key] = (iv, now_ts)
    return iv


# Type aliases for spot_prices / leg_prices: callable or dict
_SpotLookup = Callable[[str, str, dt.datetime], Optional[float]]
_LegLookup = Callable[[str, str], Optional[float]]
_SpotPricesArg = Union[
    None,
    _SpotLookup,
    Dict[Tuple[str, str, Union[dt.datetime, dt.date]], float],
]
_LegPricesArg = Union[
    None,
    _LegLookup,
    Dict[str, Union[Dict[str, float], Tuple[float, float], List[float]]],
]


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


def _normalize_spot_prices(
    spot_prices: _SpotPricesArg,
    entry_time: dt.datetime,
    exit_time: dt.datetime,
) -> Optional[_SpotLookup]:
    """Return a callable (symbol, exchange, time) -> price, or None. Symbol is the trading symbol (option/fut/stock)."""
    if spot_prices is None:
        return None
    if callable(spot_prices):
        return spot_prices  # type: ignore[return-value]
    # Dict: keys (symbol, exchange, time) or (symbol, exchange, date)
    d = spot_prices

    def lookup(symbol: str, exchange: str, time: dt.datetime) -> Optional[float]:
        key_exact = (symbol, exchange, time)
        if key_exact in d:
            return d[key_exact]  # type: ignore[index]
        key_date = (symbol, exchange, time.date())
        if key_date in d:
            return d[key_date]  # type: ignore[index]
        return None

    return lookup


def _normalize_leg_prices(leg_prices: _LegPricesArg) -> Optional[_LegLookup]:
    """Return a callable (leg_symbol, 'entry'|'exit') -> price, or None."""
    if leg_prices is None:
        return None
    if callable(leg_prices):
        return leg_prices  # type: ignore[return-value]
    d = leg_prices

    def lookup(leg_symbol: str, price_type: str) -> Optional[float]:
        val = d.get(leg_symbol)
        if val is None:
            return None
        if isinstance(val, (tuple, list)) and len(val) >= 2:
            return float(val[0]) if price_type == "entry" else float(val[1])
        if isinstance(val, dict):
            return val.get(price_type)
        return None

    return lookup


def _build_spot_fn_from_row_broker(
    row: pd.Series,
    broker: Optional[object],
    entry_time: dt.datetime,
    exit_time: dt.datetime,
    entry_price: float,
    exit_price: float,
    legs: Dict[str, int],
) -> Optional[_SpotLookup]:
    has_futures = any(is_futures_symbol(leg) for leg in legs)
    has_options = any(is_options_symbol(leg) for leg in legs)
    if not has_futures and not has_options:
        return None
    if broker is None:
        return None
    brokers_list = [cast(BrokerBase, broker)]

    def fn(symbol: str, exchange: str, time: dt.datetime) -> Optional[float]:
        if is_options_symbol(symbol):
            try:
                parts = symbol.split("_")
                if len(parts) >= 3:
                    opt_root = parts[0]   # e.g. "NIFTY"
                    opt_expiry = parts[2] # e.g. "20251230"
                    fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange)
                    # *** Build the underlying futures symbol ***
                    underlying_fut_symbol = f"{opt_root}_FUT_{fut_expiry}___"

                    time_rounded = time.replace(second=0, microsecond=0)
                    time_key = time_rounded.strftime("%Y%m%d_%H%M")
                    cache_key = (underlying_fut_symbol, exchange, time_key)  # cache under FUT symbol
                    if cache_key in _price_cache:
                        cached_price, _ = _price_cache[cache_key]
                        return cached_price

                    p = get_option_underlying_price(
                        brokers_list,
                        symbol,
                        opt_expiry,
                        fut_expiry=fut_expiry,
                        exchange=exchange,
                        as_of=time,
                    )
                    if p is not None and not pd.isna(p):
                        _price_cache[cache_key] = (float(p), _time.time())  # stored under FUT symbol
                        return float(p)
                    return None
            except Exception:
                pass
            underlying_symbol = get_underlying_symbol(symbol)
            return get_spot_price_at_time(broker, underlying_symbol, exchange, time)

        underlying_symbol = get_underlying_symbol(symbol)
        return get_spot_price_at_time(broker, underlying_symbol, exchange, time)

    return fn

def _get_leg_price_from_broker_row(
    broker: object,
    row: pd.Series,
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
                import math
                exchange = get_exchange(leg_symbol)
                price = get_mid_price(cast(List[BrokerBase], [broker]), leg_symbol, exchange=exchange, mds=None, last=True)
                if price is not None and not math.isnan(price):
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
                import math
                exchange = get_exchange(leg_symbol)
                price = get_mid_price(cast(List[BrokerBase], [broker]), leg_symbol, exchange=exchange, mds=None, last=True)
                if price is not None and not math.isnan(price):
                    return float(price)
            except Exception:
                pass
    except Exception:
        pass
    return None


# ============================================================================
# Attribution calculation (core logic)
# ============================================================================


def calculate_spot_attribution(
    symbol: str,
    entry_price: float,
    exit_price: float,
    entry_time: dt.datetime,
    exit_time: dt.datetime,
    quantity: float,
    get_spot_price_fn: Callable[[str, str, dt.datetime], Optional[float]],
) -> float:
    """
    Calculate spot attribution for stocks and futures.

    For stocks: spot_attrib = (exit_price - entry_price) * quantity
    For futures: spot_attrib = (spot_t1 - spot_t0) * quantity (underlying movement)
    """
    try:
        if is_stock_symbol(symbol):
            return (exit_price - entry_price) * quantity

        if is_futures_symbol(symbol):
            exchange = get_exchange(symbol)
            spot_t0 = get_spot_price_fn(symbol, exchange, entry_time)
            spot_t1 = get_spot_price_fn(symbol, exchange, exit_time)
            if spot_t0 is None or spot_t1 is None:
                return 0.0
            return (spot_t1 - spot_t0) * quantity

        return 0.0
    except Exception:
        return 0.0


def calculate_spread_attribution(
    symbol: str,
    entry_price: float,
    exit_price: float,
    entry_time: dt.datetime,
    exit_time: dt.datetime,
    quantity: float,
    get_spot_price_fn: Callable[[str, str, dt.datetime], Optional[float]],
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
        spot_t0 = get_spot_price_fn(symbol, exchange, entry_time)
        spot_t1 = get_spot_price_fn(symbol, exchange, exit_time)
        if spot_t0 is None or spot_t1 is None:
            return 0.0
        spread_t0 = entry_price - spot_t0
        spread_t1 = exit_price - spot_t1
        return (spread_t1 - spread_t0) * quantity
    except Exception:
        return 0.0


def calculate_attribution_for_trade(
    row: Union[pd.Series, Dict[str, Any]],
    broker: Optional[object] = None,
    *,
    spot_prices: _SpotPricesArg = None,
    leg_prices: _LegPricesArg = None,
    current_time: Optional[dt.datetime] = None,
    day: bool = True,
) -> Dict[str, float]:
    """
    Calculate attribution for a single trade.

    Row is the single input; spot_prices and leg_prices can be dicts, callables, or None
    (derived from row and broker when None). current_time defaults to row["exit_time"] or now.

    When day=True (default): use day-level attribution with MTM for carried-forward trades
    (mtm_entry_price / mtm_exit_price for prior day close and current day close/mark).
    When day=False: use trade-level attribution from row entry/exit prices and times.

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
        no_exit_in_row = (
            exit_time_str == "" or exit_time_str == "0" or pd.isna(exit_time_str)
        )

        entry_price = row.get("entry_price", 0.0)
        exit_price = row.get("exit_price", 0.0)
        if exit_price == 0 or exit_price is None:
            exit_price = row.get("mtm", entry_price)

        # Day-level attribution: use MTM for carried-forward / open positions (existing helpers)
        if day and broker is not None and current_time is not None:
            attribution_date = current_time.date() if hasattr(current_time, "date") else get_naive_dt(current_time).date()  # type: ignore[union-attr]
            attribution_date_str = attribution_date.strftime("%Y%m%d")
            entry_price = mtm_entry_price(row, attribution_date, broker, None)
            exit_price = mtm_exit_price(row, attribution_date, broker, None)
            # MTM entry/exit times for spot/IV (keep entry_dt/exit_dt as original until we assign at end)
            mtm_entry_time = entry_dt
            entry_date = entry_dt.date() if hasattr(entry_dt, "date") else entry_dt
            if attribution_date < entry_date:
                return result  # Trade did not exist on attribution day; zero attribution for this day
            if entry_date < attribution_date:
                prior = advance_by_biz_days(attribution_date_str, -1)
                prior_date_str = str(prior) if isinstance(prior, str) else (prior.strftime("%Y%m%d") if hasattr(prior, "strftime") else str(prior))
                if len(prior_date_str) == 10 and prior_date_str[4] == "-":
                    prior_date_str = prior_date_str.replace("-", "")
                mtm_entry_time = dt.datetime.strptime(prior_date_str + " 15:30:00", "%Y%m%d %H:%M:%S")
            market_close_attr = dt.datetime.strptime(attribution_date_str + " 15:30:00", "%Y%m%d %H:%M:%S")
            current_naive = get_naive_dt(current_time)
            current_dt = current_naive.to_pydatetime() if isinstance(current_naive, pd.Timestamp) else cast(dt.datetime, current_naive)
            no_exit = exit_time_str == "" or exit_time_str == "0" or pd.isna(exit_time_str)
            if no_exit:
                mtm_exit_time = market_close_attr if current_dt >= market_close_attr else current_dt
            else:
                exit_date = exit_dt.date() if hasattr(exit_dt, "date") else exit_dt
                if exit_date < attribution_date:
                    return result  # Trade already closed before attribution day; zero attribution for this day
                if exit_date > attribution_date:
                    mtm_exit_time = current_dt if (attribution_date == dt.date.today() and current_dt < market_close_attr) else market_close_attr
                else:
                    mtm_exit_time = exit_dt  # exit on attribution day: original exit time
            entry_dt = mtm_entry_time
            exit_dt = mtm_exit_time

        legs = _parse_combo_symbol(combo_symbol)
        if len(legs) == 0:
            return result

        has_stocks = any(is_stock_symbol(leg) for leg in legs.keys())
        has_futures = any(is_futures_symbol(leg) for leg in legs.keys())
        has_options = any(is_options_symbol(leg) for leg in legs.keys())

        entry_qty = row["entry_quantity"]

        # Resolve get_spot_price_fn: normalized(spot_prices) or from broker/row, or no-op
        get_spot_price_fn = _normalize_spot_prices(spot_prices, entry_dt, exit_dt)
        if get_spot_price_fn is None:
            get_spot_price_fn = _build_spot_fn_from_row_broker(
                row, broker, entry_dt, exit_dt, entry_price, exit_price, legs
            )
        if get_spot_price_fn is None:
            get_spot_price_fn = lambda s, e, t: None  # noqa: E731

        # Resolve get_leg_price_fn: normalized(leg_prices) or from broker+row for multi-leg
        get_leg_price_fn = _normalize_leg_prices(leg_prices)
        if get_leg_price_fn is None and len(legs) > 1 and broker is not None:
            def _leg_fn(leg_symbol: str, price_type: str) -> Optional[float]:
                return _get_leg_price_from_broker_row(broker, row, leg_symbol, price_type)
            get_leg_price_fn = _leg_fn

        if has_stocks and not has_futures and not has_options:
            for leg_symbol, leg_qty in legs.items():
                if is_stock_symbol(leg_symbol):
                    signed_qty = leg_qty * float(entry_qty)
                    spot_attrib = calculate_spot_attribution(
                        leg_symbol, entry_price, exit_price,
                        entry_dt, exit_dt, signed_qty, get_spot_price_fn,
                    )
                    result["spot_attrib"] += spot_attrib

        elif has_futures and not has_options:
            for leg_symbol, leg_qty in legs.items():
                if is_futures_symbol(leg_symbol):
                    signed_qty = leg_qty * float(entry_qty)
                    spot_attrib = calculate_spot_attribution(
                        leg_symbol, entry_price, exit_price,
                        entry_dt, exit_dt, signed_qty, get_spot_price_fn,
                    )
                    result["spot_attrib"] += spot_attrib
                    spread_attrib = calculate_spread_attribution(
                        leg_symbol, entry_price, exit_price,
                        entry_dt, exit_dt, signed_qty, get_spot_price_fn,
                    )
                    result["spread_attrib"] += spread_attrib

        elif has_options:
            adjusted_legs = {
                leg_symbol: int(leg_qty * float(entry_qty))
                for leg_symbol, leg_qty in legs.items()
            }
            spot_t0_dict = {}
            spot_t1_dict = {}
            ivs_t0 = {}
            ivs_t1 = {}

            for leg_symbol in legs:
                exchange_leg = get_exchange(leg_symbol)
                # Derive the underlying futures symbol for this option leg
                if is_options_symbol(leg_symbol):
                    parts = leg_symbol.split("_")
                    opt_root = parts[0]
                    opt_expiry = parts[2] if len(parts) >= 3 else ""
                    fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange_leg)
                    underlying_fut_symbol = f"{opt_root}_FUT_{fut_expiry}___"
                else:
                    # For non-options, use leg_symbol directly
                    underlying_fut_symbol = leg_symbol

                spot_t0 = get_spot_price_fn(leg_symbol, exchange_leg, entry_dt)
                spot_t1 = get_spot_price_fn(leg_symbol, exchange_leg, exit_dt)
                if spot_t0 is None and broker is not None and is_options_symbol(leg_symbol):
                    try:
                        time_key_0 = entry_dt.replace(second=0, microsecond=0).strftime("%Y%m%d_%H%M")
                        cache_key_0 = (underlying_fut_symbol, exchange_leg, time_key_0)
                        if cache_key_0 in _price_cache:
                            spot_t0 = _price_cache[cache_key_0][0]
                        else:
                            parts = leg_symbol.split("_")
                            if len(parts) >= 3:
                                opt_expiry = parts[2]
                                fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange_leg)
                                p = get_option_underlying_price(
                                    [cast(BrokerBase, broker)],
                                    leg_symbol,
                                    opt_expiry,
                                    fut_expiry=fut_expiry,
                                    exchange=exchange_leg,
                                    as_of=entry_dt,
                                )
                                if p is not None and not pd.isna(p):
                                    _price_cache[cache_key_0] = (float(p), _time.time())
                                    spot_t0 = float(p)
                    except Exception:
                        pass
                if spot_t1 is None and broker is not None and is_options_symbol(leg_symbol):
                    try:
                        time_key_1 = exit_dt.replace(second=0, microsecond=0).strftime("%Y%m%d_%H%M")
                        cache_key_1 = (underlying_fut_symbol, exchange_leg, time_key_1)
                        if cache_key_1 in _price_cache:
                            spot_t1 = _price_cache[cache_key_1][0]
                        else:
                            parts = leg_symbol.split("_")
                            if len(parts) >= 3:
                                opt_expiry = parts[2]
                                fut_expiry = _get_month_end_fut_expiry(opt_expiry, exchange_leg)
                                p = get_option_underlying_price(
                                    [cast(BrokerBase, broker)],
                                    leg_symbol,
                                    opt_expiry,
                                    fut_expiry=fut_expiry,
                                    exchange=exchange_leg,
                                    as_of=exit_dt,
                                )
                                if p is not None and not pd.isna(p):
                                    _price_cache[cache_key_1] = (float(p), _time.time())
                                    spot_t1 = float(p)
                    except Exception:
                        pass
                if spot_t0 is None or spot_t1 is None:
                    trading_logger.log_debug(
                        f"Could not get spot prices for combo leg {leg_symbol}, skipping attribution"
                    )
                    return result
                spot_t0_dict[leg_symbol] = spot_t0
                spot_t1_dict[leg_symbol] = spot_t1

                leg_entry_price = None
                leg_exit_price = None
                if entry_dt != original_entry_dt and broker is not None:
                    p = get_spot_price_at_time(broker, leg_symbol, exchange_leg, entry_dt)
                    if p is not None and not pd.isna(p):
                        leg_entry_price = p
                if (exit_dt != original_exit_dt or no_exit_in_row) and broker is not None:
                    p = get_spot_price_at_time(broker, leg_symbol, exchange_leg, exit_dt)
                    if p is not None and not pd.isna(p):
                        leg_exit_price = p
                if leg_entry_price is None and get_leg_price_fn is not None:
                    try:
                        leg_entry_price = get_leg_price_fn(leg_symbol, "entry")
                    except Exception as e:
                        trading_logger.log_debug(
                            f"Error getting per-leg entry price for {leg_symbol}: {e}"
                        )
                if leg_exit_price is None and get_leg_price_fn is not None:
                    try:
                        leg_exit_price = get_leg_price_fn(leg_symbol, "exit")
                    except Exception as e:
                        trading_logger.log_debug(
                            f"Error getting per-leg exit price for {leg_symbol}: {e}"
                        )
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

                iv_0 = get_iv_fn(leg_symbol, leg_entry_price, spot_t0, entry_dt, exchange_leg)
                iv_1 = get_iv_fn(leg_symbol, leg_exit_price, spot_t1, exit_dt, exchange_leg)
                if iv_0 is None or iv_1 is None:
                    trading_logger.log_debug(
                        f"Could not get IVs for combo leg {leg_symbol}, skipping attribution"
                    )
                    return result
                ivs_t0[leg_symbol] = iv_0
                ivs_t1[leg_symbol] = iv_1

            # Pass signed quantities so chameli computes attribution for actual position (short = negative qty)
            entry_qty_f = float(entry_qty)
            signed_legs = {
                leg: (qty if entry_qty_f >= 0 else -abs(qty))
                for leg, qty in adjusted_legs.items()
            }
            combo_parts = [f"{leg}?{qty}" for leg, qty in signed_legs.items()]
            combo_symbol_str = ":".join(combo_parts)
            first_leg = next(iter(legs.keys()))
            exchange = get_exchange(first_leg)

            try:
                attrib = performance_attribution(
                    combo_symbol=combo_symbol_str,
                    spot_t0=spot_t0_dict[first_leg],
                    spot_t1=spot_t1_dict[first_leg],
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
                result["spot_attrib"] = 0.0 if pd.isna(underlying_attrib) else underlying_attrib
                result["vol_attrib"] = 0.0 if pd.isna(vol_attrib) else vol_attrib
                result["timedecay_attrib"] = 0.0 if pd.isna(timedecay_attrib) else timedecay_attrib
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
                trading_logger.log_debug(
                    f"Error in performance_attribution for combo {combo_symbol}: {e}"
                )

    except Exception:
        pass

    return result


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
        (pnl.entry_quantity != 0)
        & (pnl.exit_quantity != 0)
        & (abs(pnl.entry_quantity + pnl.exit_quantity) > 0.001)
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
        open_row["int_order_id"] = f"{original_int_order_id}_F{fork_num}" if original_int_order_id else f"FORKED_F{fork_num}"
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
    broker=None,
    historical_cache: Optional[Dict[Tuple[str, str], Optional[float]]] = None,
) -> float:
    """
    Get MTM entry price for a trade row on a given date.

    For positions entered before mtm_date, returns the EOD close for the day before mtm_date.
    For positions entered on or after mtm_date, returns the entry_price.

    Args:
        row: DataFrame row with entry_price, entry_time, symbol
        mtm_date: Date for MTM (dt.date or YYYYMMDD str)
        broker: Broker instance for get_historical_close_price
        historical_cache: Optional {(symbol, date_str): price} cache

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
        prior_date_str = str(prior) if isinstance(prior, str) else (prior.strftime("%Y%m%d") if hasattr(prior, "strftime") else str(prior))
        symbol = str(row.get("symbol", ""))
        if not symbol:
            return entry_price

        cache_key: Tuple[str, str] = (symbol, prior_date_str)
        if historical_cache is not None and cache_key in historical_cache:
            cached = historical_cache[cache_key]
            if cached is not None:
                return float(cached)
            historical_cache[cache_key] = entry_price
            return entry_price

        try:
            exchange = "BSE" if "SENSEX" in symbol else "NSE"
            mtm_price = get_historical_close_price(broker, symbol, prior_date_str, exchange)
            if mtm_price is not None:
                return float(mtm_price)
        except Exception:
            pass
        if historical_cache is not None:
            historical_cache[cache_key] = entry_price
        return entry_price
    except Exception:
        return entry_price


def mtm_exit_price(
    row: pd.Series,
    mtm_date: Union[dt.date, str],
    broker=None,
    historical_cache: Optional[Dict[Tuple[str, str], Optional[float]]] = None,
) -> float:
    """
    Get MTM exit price for a trade row on a given date.

    For open positions on mtm_date: current MTM (market price if today before 15:30, else historical close).
    For closed positions: exit_price.
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
        try:
            mtm_date_obj = dt.datetime.strptime(mtm_date_str, "%Y%m%d").date()
        except ValueError:
            mtm_date_obj = None

    mtm_date_1530 = mtm_date_str + " 15:30:00"
    today = dt.date.today()

    if broker is None:
        trading_logger.log_error("mtm_exit_price: broker is None, falling back to entry_price")
        return entry_price

    is_closed_check = False
    if pd.isna(exit_time_str) or exit_time_str == "" or exit_time_str == "0":
        is_closed_check = True
    else:
        try:
            exit_time_dt = parse_datetime(exit_time_str)
            mtm_date_dt = parse_datetime(mtm_date_1530)
            is_closed_check = exit_time_dt > mtm_date_dt
        except (ValueError, TypeError):
            is_closed_check = False

    if is_closed_check:
        if mtm_date_obj and mtm_date_obj == today:
            current_time = dt.datetime.now().time()
            market_close_time = dt.time(15, 30, 0)
            if current_time < market_close_time:
                try:
                    price = get_mid_price([broker], symbol, exchange=exchange, mds=None, last=True)
                    if price is not None and not pd.isna(price):
                        return float(price)
                except Exception as e:
                    trading_logger.log_warning(
                        f"Error getting current market price for {symbol}: {e}, falling back to historical"
                    )

            cache_key = (symbol, mtm_date_str)
            if historical_cache and cache_key in historical_cache:
                cached = historical_cache[cache_key]
                if cached is not None:
                    return float(cached)
            try:
                mtm_price = get_historical_close_price(broker, symbol, mtm_date_str, exchange)
                if mtm_price is not None:
                    return mtm_price
                return entry_price
            except Exception as e:
                trading_logger.log_warning(f"Error fetching historical data for {symbol} on {mtm_date_str}: {e}")
                return entry_price
        else:
            cache_key = (symbol, mtm_date_str)
            if historical_cache and cache_key in historical_cache:
                cached = historical_cache[cache_key]
                if cached is not None:
                    return float(cached)
            try:
                mtm_price = get_historical_close_price(broker, symbol, mtm_date_str, exchange)
                if mtm_price is not None:
                    return mtm_price
                return entry_price
            except Exception as e:
                trading_logger.log_warning(f"Error fetching historical data for {symbol} on {mtm_date_str}: {e}")
                return entry_price
    else:
        return exit_price


def mtm_df(
    pnl: pd.DataFrame,
    mtm_date: Union[dt.date, str],
    broker=None,
    historical_cache: Optional[Dict[Tuple[str, str], Optional[float]]] = None,
) -> pd.DataFrame:
    """
    Calculate MTM for each row on a given date.
    Forks partial trades first, then sets entry_price (MTM entry), mtm (MTM exit), exit_price = mtm per row.

    Args:
        pnl: P&L DataFrame with trades
        mtm_date: Date for MTM (dt.date or YYYYMMDD str)
        broker: Broker instance for historical/current prices
        historical_cache: Optional {(symbol, date_str): price} cache

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
        pnl.at[index, "entry_price"] = mtm_entry_price(row, mtm_date, broker, historical_cache)
        pnl.at[index, "mtm"] = mtm_exit_price(row, mtm_date, broker, historical_cache)
        pnl.at[index, "exit_price"] = pnl.loc[index, "mtm"]  # type: ignore
    return pnl
