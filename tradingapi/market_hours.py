import datetime as dt
from typing import Optional

from .config import get_market_close_time, get_market_open_time


def session_bounds(
    as_of: dt.date | dt.datetime,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    market: Optional[str] = None,
) -> tuple[dt.datetime, dt.datetime]:
    session_date = as_of.date() if isinstance(as_of, dt.datetime) else as_of
    open_time = dt.time.fromisoformat(get_market_open_time(exchange, market, symbol, session_date))
    close_time = dt.time.fromisoformat(get_market_close_time(exchange, market, symbol, session_date))
    return dt.datetime.combine(session_date, open_time), dt.datetime.combine(session_date, close_time)


def latest_close_datetime(as_of: dt.date | dt.datetime) -> dt.datetime:
    session_date = as_of.date() if isinstance(as_of, dt.datetime) else as_of
    return max(
        session_bounds(session_date, exchange=exchange, market=market)[1]
        for exchange in ("NSE", "BSE")
        for market in ("CASH", "FNO")
    )
