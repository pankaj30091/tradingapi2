import datetime as dt
import json
import os
import ssl
import threading
import time
import urllib3
from typing import Any, Callable, Dict, List, Optional, Union

import websocket as _ws_lib

import pandas as pd
import redis
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .broker_base import (
    BrokerBase,
    Brokers,
    HistoricalData,
    Order,
    OrderInfo,
    OrderStatus,
    Price,
)
from .config import get_config
from .exceptions import (
    AuthenticationError,
    BrokerConnectionError,
    MarketDataError,
    OrderError,
    ValidationError,
    create_error_context,
)
from . import trading_logger
from .globals import get_tradingapi_now
from .utils import set_starting_internal_ids_int, update_order_status

config = get_config()

_BASE_URL = config.get("IB.BASE_URL", "https://localhost:5000/v1/api")
_TICKLE_INTERVAL = int(config.get("IB.TICKLE_INTERVAL_SECONDS", 60))
# secdef batch limit per IBKR gateway spec
_SECDEF_BATCH = 50


class IB(BrokerBase):
    """Interactive Brokers Client Portal Web API via IBeam gateway."""

    def __init__(self, **kwargs):
        self._session = requests.Session()
        self._session.verify = False  # IBeam self-signed cert on localhost
        self._account_id: Optional[str] = None
        self._authenticated = False
        self._tickle_thread: Optional[threading.Thread] = None
        self._stop_tickle = threading.Event()
        self._base_url = _BASE_URL
        # WebSocket streaming state
        self._ws: Optional[_ws_lib.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_open_event = threading.Event()
        self._ws_lock = threading.Lock()
        self._stream_conid_map: Dict[int, tuple] = {}  # conid -> (long_symbol, exchange)
        self._stream_callback: Optional[Callable] = None
        self._stream_prices: Dict[str, Price] = {}
        super().__init__(**kwargs)

    def _initialize_broker(self, config_dict: dict):
        super()._initialize_broker(config_dict)
        self.broker = Brokers.INTERACTIVEBROKERS

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self._base_url}/{path.lstrip('/')}"

    def _init_brokerage_session(self) -> None:
        resp = self._session.post(
            self._url("/iserver/auth/ssodh/init"),
            json={"publish": True, "compete": True},
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            raise AuthenticationError(
                f"IBKR brokerage session init failed: HTTP {resp.status_code}",
                create_error_context(status=resp.status_code, body=resp.text[:500]),
            )
        trading_logger.log_info("IBKR brokerage session initialised", {})

    def _check_auth_status(self) -> bool:
        try:
            resp = self._session.get(self._url("/iserver/auth/status"), timeout=10)
            if resp.status_code == 200:
                return bool(resp.json().get("authenticated"))
        except Exception:
            pass
        return False

    def _start_tickle(self) -> None:
        self._stop_tickle.clear()

        def _tickle_loop():
            while not self._stop_tickle.wait(_TICKLE_INTERVAL):
                try:
                    self._session.post(self._url("/tickle"), timeout=10)
                except Exception as exc:
                    trading_logger.log_warning("Tickle failed", {"error": str(exc)})

        self._tickle_thread = threading.Thread(target=_tickle_loop, daemon=True, name="ib_tickle")
        self._tickle_thread.start()

    def _stop_tickle_thread(self) -> None:
        self._stop_tickle.set()
        if self._tickle_thread:
            self._tickle_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # BrokerBase interface
    # ------------------------------------------------------------------

    def connect(self, redis_db: int, as_of_date=None):
        for attempt in range(15):
            if self._check_auth_status():
                break
            trading_logger.log_info(f"Waiting for IBeam gateway... attempt {attempt + 1}/15", {})
            time.sleep(4)
        else:
            raise BrokerConnectionError("IBeam gateway not authenticated after 60s", {})

        self._init_brokerage_session()
        self._authenticated = True
        self._start_tickle()

        self._account_id = config.get("IB.ACCOUNT_ID") or self._fetch_primary_account()
        self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
        set_starting_internal_ids_int(self.redis_o)
        trading_logger.log_info("IBKR connected", {"account_id": self._account_id})

    def _fetch_primary_account(self) -> str:
        resp = self._session.get(self._url("/iserver/accounts"), timeout=10)
        resp.raise_for_status()
        accounts = resp.json().get("accounts", [])
        if not accounts:
            raise BrokerConnectionError("No IBKR accounts found", {})
        return accounts[0]

    def is_connected(self) -> bool:
        return self._authenticated and self._check_auth_status()

    def disconnect(self):
        self._stop_tickle_thread()
        self._stop_ws()
        try:
            self._session.post(self._url("/logout"), timeout=10)
        except Exception:
            pass
        self._authenticated = False
        trading_logger.log_info("IBKR disconnected", {})

    # ------------------------------------------------------------------
    # Symbology
    # ------------------------------------------------------------------

    def save_symbol_data(self, saveToFolder: bool = True) -> pd.DataFrame:
        """Fetch NSE equities + F&O futures from the gateway and return a symbols DataFrame."""
        resp = self._session.get(
            self._url("/trsrv/all-conids"),
            params={"exchange": "NSE", "type": "STOCK"},
            timeout=30,
        )
        resp.raise_for_status()
        entries = resp.json()

        ticker_by_conid = {e["conid"]: e["ticker"] for e in entries}
        all_conids = list(ticker_by_conid.keys())

        rows = []
        fo_stocks: List[tuple] = []  # (under_conid, ticker, tick_size) for hasOptions stocks
        for i in range(0, len(all_conids), _SECDEF_BATCH):
            batch = all_conids[i : i + _SECDEF_BATCH]
            r = self._session.get(
                self._url("/trsrv/secdef"),
                params={"conids": ",".join(str(c) for c in batch)},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            for defn in r.json().get("secdef", []):
                conid = defn["conid"]
                ticker = defn.get("ticker") or ticker_by_conid.get(conid, "")
                inc_rules = defn.get("incrementRules") or []
                tick_size = inc_rules[0]["increment"] if inc_rules else 0.05
                rows.append({
                    "long_symbol": f"{ticker}_STK___",
                    "LotSize": 1,
                    "Scripcode": conid,
                    "Exch": "NSE",
                    "ExchType": "CASH",
                    "TickSize": tick_size,
                    "trading_symbol": ticker,
                })
                if defn.get("hasOptions"):
                    fo_stocks.append((conid, ticker, tick_size))
            time.sleep(0.1)

        rows.extend(self._fetch_nse_futures_rows(fo_stocks))

        codes = pd.DataFrame(rows, columns=["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"])

        if saveToFolder:
            dt_today = get_tradingapi_now().strftime("%Y%m%d")
            dest = os.path.join(config.get("IB.SYMBOLCODES"), f"{dt_today}_symbols.csv")
            codes.to_csv(dest, index=False)
            trading_logger.log_info("IB symbol data saved", {"path": dest, "rows": len(codes)})

        return codes

    def _fetch_nse_futures_rows(self, fo_stocks: List[tuple]) -> List[dict]:
        """For each F&O-eligible underlying, fetch active NSE monthly futures contracts."""
        months = self._nse_active_months()
        rows = []
        for under_conid, ticker, tick_size in fo_stocks:
            for month in months:
                try:
                    r = self._session.get(
                        self._url("/iserver/secdef/info"),
                        params={
                            "conid": under_conid,
                            "secType": "FUT",
                            "exchange": "NSE",
                            "isBundle": "false",
                            "month": month,
                            "strike": "0",
                        },
                        timeout=10,
                    )
                    if r.status_code != 200:
                        continue
                    contracts = r.json()
                    if not isinstance(contracts, list) or not contracts:
                        continue
                    c = contracts[0]
                    maturity = c.get("maturityDate", "")
                    lot_size = int(float(c.get("multiplier") or 1)) or 1
                    rows.append({
                        "long_symbol": f"{ticker}_FUT_{maturity}__",
                        "LotSize": lot_size,
                        "Scripcode": c["conid"],
                        "Exch": "NSE",
                        "ExchType": "NFO",
                        "TickSize": tick_size,
                        "trading_symbol": f"{ticker}{maturity}F",
                    })
                except Exception:
                    pass
                time.sleep(0.05)
        return rows

    @staticmethod
    def _nse_active_months(n: int = 3) -> List[str]:
        """Return the next n NSE monthly expiry month codes (e.g. ['MAY26','JUN26','JUL26'])."""
        _MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                   "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        today = get_tradingapi_now().date()
        result = []
        year, month = today.year, today.month
        for _ in range(n + 1):
            # Last Thursday of this (year, month)
            last_day = (dt.date(year, month % 12 + 1, 1) if month < 12
                        else dt.date(year + 1, 1, 1)) - dt.timedelta(days=1)
            last_thu = last_day - dt.timedelta(days=(last_day.weekday() - 3) % 7)
            if last_thu >= today:
                result.append(f"{_MONTHS[month - 1]}{str(year)[2:]}")
                if len(result) == n:
                    break
            month += 1
            if month > 12:
                month = 1
                year += 1
        return result

    def update_symbology(self, **kwargs) -> pd.DataFrame:
        dt_today = get_tradingapi_now().strftime("%Y%m%d")
        symbols_path = os.path.join(config.get("IB.SYMBOLCODES"), f"{dt_today}_symbols.csv")
        try:
            codes = pd.read_csv(symbols_path)
        except FileNotFoundError:
            codes = self.save_symbol_data(saveToFolder=False)
            codes = codes.dropna(subset=["long_symbol"])

        self.exchange_mappings = {}
        for exchange, group in codes.groupby("Exch"):
            self.exchange_mappings[exchange] = {
                "symbol_map": dict(zip(group["long_symbol"], group["Scripcode"])),
                "contractsize_map": dict(zip(group["long_symbol"], group["LotSize"])),
                "exchange_map": dict(zip(group["long_symbol"], group["Exch"])),
                "exchangetype_map": dict(zip(group["long_symbol"], group["ExchType"])),
                "contracttick_map": dict(zip(group["long_symbol"], group["TickSize"])),
                "symbol_map_reversed": dict(zip(group["Scripcode"], group["long_symbol"])),
                "tradingsymbol_map": dict(zip(group["long_symbol"], group["trading_symbol"])),
            }

        trading_logger.log_info(
            "Symbology update completed",
            {"total_exchanges": len(self.exchange_mappings), "total_symbols": len(codes)},
        )
        return codes

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_quote(self, long_symbol: str, exchange: str = "NSE") -> Price:
        conid = self._resolve_conid(long_symbol, exchange)
        # Field codes: 31=last, 84=bid, 86=ask, 7295=open, 70=high, 71=low, 7762=volume
        resp = self._session.get(
            self._url("/iserver/marketdata/snapshot"),
            params={"conids": conid, "fields": "31,84,86,7295,70,71,7762"},
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            raise MarketDataError(f"No market data for {long_symbol}", {})
        d = rows[0]
        return Price(
            last=self._safe_float(d.get("31")),
            bid=self._safe_float(d.get("84")),
            ask=self._safe_float(d.get("86")),
            high=self._safe_float(d.get("70")),
            low=self._safe_float(d.get("71")),
            volume=int(self._safe_float(d.get("7762", 0))),
            symbol=long_symbol,
            exchange=exchange,
            src="IB",
            timestamp=get_tradingapi_now().isoformat(),
        )

    def get_historical(
        self,
        symbols,
        date_start,
        date_end=None,
        exchange: str = "NSE",
        periodicity: str = "1m",
        market_close_time: str = "15:30:00",
        refresh_mapping: bool = False,
    ) -> Dict[str, List[HistoricalData]]:
        if date_end is None:
            date_end = get_tradingapi_now().strftime("%Y-%m-%d")
        if isinstance(symbols, str):
            symbols = [symbols]
        elif isinstance(symbols, pd.DataFrame):
            symbols = symbols["long_symbol"].tolist()

        period_map = {"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h", "1d": "1d"}
        bar = period_map.get(periodicity, "1min")
        result: Dict[str, List[HistoricalData]] = {}

        for sym in symbols:
            conid = self._resolve_conid(sym, exchange)
            resp = self._session.get(
                self._url("/iserver/marketdata/history"),
                params={
                    "conid": conid,
                    "period": self._date_range_to_period(date_start, date_end),
                    "bar": bar,
                    "startTime": self._fmt_date(date_start),
                    "outsideRth": False,
                },
                timeout=30,
            )
            resp.raise_for_status()
            bars = []
            for b in resp.json().get("data", []):
                bars.append(HistoricalData(
                    date=dt.datetime.utcfromtimestamp(b["t"] / 1000),
                    open=float(b.get("o", float("nan"))),
                    high=float(b.get("h", float("nan"))),
                    low=float(b.get("l", float("nan"))),
                    close=float(b.get("c", float("nan"))),
                    volume=int(b.get("v", 0)),
                    intoi=0,
                    oi=0,
                ))
            result[sym] = bars
        return result

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def place_order(self, order: Order, **kwargs) -> Order:
        if not self._account_id:
            raise BrokerConnectionError("Not connected — account_id unknown", {})

        side = "BUY" if order.order_type in ("BUY", "COVER") else "SELL"
        order_type = "LMT" if order.price > 0 else "MKT"
        conid = self._resolve_conid(order.long_symbol, order.exchange or "NSE")

        payload = {
            "orders": [{
                "acctId": self._account_id,
                "conid": int(conid),
                "orderType": order_type,
                "side": side,
                "quantity": order.quantity,
                "tif": "IOC" if order.ioc_order else "DAY",
                **({"price": order.price} if order_type == "LMT" else {}),
            }]
        }

        resp = self._session.post(
            self._url(f"/iserver/account/{self._account_id}/orders"),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list) and data and data[0].get("messageIds"):
            order = self._confirm_order(order, data[0]["messageIds"])
        elif isinstance(data, list) and data:
            order.broker_order_id = str(data[0].get("orderId", ""))
            order.status = OrderStatus.PENDING

        order.broker = Brokers.INTERACTIVEBROKERS
        update_order_status(self, order.internal_order_id, order.broker_order_id)
        return order

    def _confirm_order(self, order: Order, message_ids: list) -> Order:
        resp = self._session.post(
            self._url(f"/iserver/reply/{message_ids[0]}"),
            json={"confirmed": True},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            order.broker_order_id = str(data[0].get("orderId", ""))
            order.status = OrderStatus.PENDING
        return order

    def modify_order(self, **kwargs) -> Order:
        broker_order_id = kwargs.get("broker_order_id")
        new_price = kwargs.get("new_price")
        new_quantity = kwargs.get("new_quantity")
        order: Order = kwargs.get("order", Order())

        payload = {}
        if new_price is not None:
            payload["price"] = new_price
        if new_quantity is not None:
            payload["quantity"] = new_quantity

        resp = self._session.post(
            self._url(f"/iserver/account/{self._account_id}/order/{broker_order_id}"),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        order.broker_order_id = str(broker_order_id)
        update_order_status(self, order.internal_order_id, order.broker_order_id)
        return order

    def cancel_order(self, **kwargs) -> Order:
        broker_order_id = kwargs.get("broker_order_id")
        order: Order = kwargs.get("order", Order())

        resp = self._session.delete(
            self._url(f"/iserver/account/{self._account_id}/order/{broker_order_id}"),
            timeout=30,
        )
        resp.raise_for_status()
        order.status = OrderStatus.CANCELLED
        order.broker_order_id = str(broker_order_id)
        update_order_status(self, order.internal_order_id, order.broker_order_id)
        return order

    def get_order_info(self, **kwargs) -> OrderInfo:
        broker_order_id = kwargs.get("broker_order_id")
        resp = self._session.get(self._url(f"/iserver/account/order/status/{broker_order_id}"), timeout=10)
        resp.raise_for_status()
        d = resp.json()
        status_map = {
            "Filled": OrderStatus.FILLED,
            "Submitted": OrderStatus.OPEN,
            "PreSubmitted": OrderStatus.PENDING,
            "Cancelled": OrderStatus.CANCELLED,
            "Inactive": OrderStatus.REJECTED,
        }
        status = status_map.get(d.get("order_status", ""), OrderStatus.UNDEFINED)
        return OrderInfo(
            order_size=int(d.get("size", 0)),
            order_price=float(d.get("price", float("nan"))),
            fill_size=int(d.get("filled_quantity", 0)),
            fill_price=float(d.get("avg_price", 0)),
            status=status,
            broker_order_id=str(broker_order_id),
            broker=Brokers.INTERACTIVEBROKERS,
        )

    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        resp = self._session.get(self._url("/iserver/account/orders"), timeout=10)
        resp.raise_for_status()
        orders = resp.json().get("orders", [])
        return pd.DataFrame(orders) if orders else pd.DataFrame()

    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        resp = self._session.get(self._url("/iserver/account/trades"), timeout=10)
        resp.raise_for_status()
        trades = resp.json()
        return pd.DataFrame(trades) if trades else pd.DataFrame()

    # ------------------------------------------------------------------
    # Positions / capital
    # ------------------------------------------------------------------

    def get_position(self, long_symbol: str) -> Union[pd.DataFrame, int]:
        resp = self._session.get(
            self._url(f"/iserver/account/{self._account_id}/positions/0"), timeout=10
        )
        resp.raise_for_status()
        positions = resp.json()
        if not positions:
            return 0
        conid = self._resolve_conid(long_symbol, "NSE")
        for pos in positions:
            if str(pos.get("conid")) == str(conid):
                return int(pos.get("position", 0))
        return 0

    def get_available_capital(self) -> Dict[str, float]:
        resp = self._session.get(
            self._url(f"/iserver/account/{self._account_id}/summary"),
            params={"fields": "TotalCashValue,NetLiquidation"},
            timeout=10,
        )
        resp.raise_for_status()
        summary = resp.json()
        cash = float(summary.get("TotalCashValue", {}).get("amount", 0))
        return {"cash": cash, "collateral": 0.0}

    # ------------------------------------------------------------------
    # Symbol utilities
    # ------------------------------------------------------------------

    def _resolve_conid(self, long_symbol: str, exchange: str = "NSE") -> str:
        """Resolve a long_symbol to an IBKR conid, populating exchange_mappings as a side-effect.

        Checks Redis cache first (24h TTL). On miss, calls the IBKR secdef API.
        Futures and options are lazy-loaded — they don't need to appear in the static
        symbol CSV to be tradeable.
        """
        cache_key = f"ib:conid:{long_symbol}:{exchange}"
        cached = self.redis_o.get(cache_key)
        if cached:
            return cached

        parts = long_symbol.split("_")
        ticker = parts[0]
        sec_type = parts[1] if len(parts) > 1 else "STK"
        contract_info: Optional[dict] = None

        if sec_type in ("STK", "IND"):
            resp = self._session.get(
                self._url("/iserver/secdef/search"),
                params={"symbol": ticker, "secType": sec_type},
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json()
            match = next(
                (r for r in results if exchange.upper() in r.get("description", "").upper()
                 or any(exchange.upper() in s.get("exchange", "") for s in r.get("sections", []))),
                results[0] if results else None,
            )
            if not match:
                raise ValidationError(f"No conid found for {long_symbol} on {exchange}", {})
            conid = str(match["conid"])

        elif sec_type == "FUT":
            expiry_str = parts[2] if len(parts) > 2 else ""
            month = self._expiry_to_ibkr_month(expiry_str)
            under_conid = self._resolve_conid(f"{ticker}_STK___", exchange)
            resp = self._session.get(
                self._url("/iserver/secdef/info"),
                params={"conid": under_conid, "secType": "FUT", "exchange": exchange,
                        "isBundle": "false", "month": month, "strike": "0"},
                timeout=10,
            )
            resp.raise_for_status()
            contracts = resp.json()
            if not contracts or isinstance(contracts, dict):
                raise ValidationError(f"No FUT contract for {long_symbol}", {})
            contract_info = contracts[0]
            conid = str(contract_info["conid"])

        elif sec_type == "OPT":
            expiry_str = parts[2] if len(parts) > 2 else ""
            right_str = parts[3] if len(parts) > 3 else "CALL"
            strike = parts[4] if len(parts) > 4 else "0"
            right = "C" if right_str.upper() == "CALL" else "P"
            month = self._expiry_to_ibkr_month(expiry_str)
            under_conid = self._resolve_conid(f"{ticker}_STK___", exchange)
            resp = self._session.get(
                self._url("/iserver/secdef/info"),
                params={"conid": under_conid, "secType": "OPT", "exchange": exchange,
                        "isBundle": "false", "month": month, "strike": strike, "right": right},
                timeout=10,
            )
            resp.raise_for_status()
            contracts = resp.json()
            if not contracts or isinstance(contracts, dict):
                raise ValidationError(f"No OPT contract for {long_symbol}", {})
            contract_info = contracts[0]
            conid = str(contract_info["conid"])

        else:
            raise ValidationError(f"Unsupported sec_type '{sec_type}' in {long_symbol}", {})

        self._populate_exchange_mapping(long_symbol, exchange, conid, contract_info)
        self.redis_o.setex(cache_key, 86400, conid)
        return conid

    def _populate_exchange_mapping(
        self, long_symbol: str, exchange: str, conid: str, contract_info: Optional[dict] = None
    ) -> None:
        """Upsert a single symbol into exchange_mappings (safe to call multiple times)."""
        parts = long_symbol.split("_")
        ticker = parts[0]
        sec_type = parts[1] if len(parts) > 1 else "STK"
        exch_type = "CASH" if sec_type in ("STK", "IND") else "NFO"

        lot_size = int(float(contract_info.get("multiplier") or 1)) or 1 if contract_info else 1

        # Prefer the underlying equity's tick size; fall back to IBKR incrementRules or 0.05
        stk_key = f"{ticker}_STK___"
        tick_size = (
            self.exchange_mappings.get(exchange, {}).get("contracttick_map", {}).get(stk_key)
            or 0.05
        )

        if sec_type == "FUT" and contract_info:
            maturity = contract_info.get("maturityDate", parts[2] if len(parts) > 2 else "")
            trading_symbol = f"{ticker}{maturity}F"
        elif sec_type == "OPT" and contract_info:
            maturity = contract_info.get("maturityDate", parts[2] if len(parts) > 2 else "")
            right_char = (parts[3][0] if len(parts) > 3 else "C").upper()
            strike = parts[4] if len(parts) > 4 else "0"
            trading_symbol = f"{ticker}{maturity}{right_char}{strike}"
        else:
            trading_symbol = ticker

        if exchange not in self.exchange_mappings:
            self.exchange_mappings[exchange] = {
                "symbol_map": {}, "contractsize_map": {}, "exchange_map": {},
                "exchangetype_map": {}, "contracttick_map": {},
                "symbol_map_reversed": {}, "tradingsymbol_map": {},
            }
        m = self.exchange_mappings[exchange]
        int_conid = int(conid)
        m["symbol_map"][long_symbol] = int_conid
        m["contractsize_map"][long_symbol] = lot_size
        m["exchange_map"][long_symbol] = exchange
        m["exchangetype_map"][long_symbol] = exch_type
        m["contracttick_map"][long_symbol] = tick_size
        m["symbol_map_reversed"][int_conid] = long_symbol
        m["tradingsymbol_map"][long_symbol] = trading_symbol

    @staticmethod
    def _expiry_to_ibkr_month(expiry: str) -> str:
        """Convert YYYYMMDD expiry to IBKR MMM YY month string (e.g. '20260529' → 'MAY26')."""
        _MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                   "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        try:
            d = dt.datetime.strptime(expiry[:8], "%Y%m%d")
            return f"{_MONTHS[d.month - 1]}{d.strftime('%y')}"
        except (ValueError, IndexError):
            return expiry

    def map_exchange_for_api(self, long_symbol: str, exchange: str) -> str:
        return exchange.upper()

    def map_exchange_for_db(self, long_symbol: str, exchange: str) -> str:
        return exchange.upper()

    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        raise NotImplementedError

    def get_min_lot_size(self, long_symbol: str, exchange: str) -> int:
        return 1

    # ------------------------------------------------------------------
    # Streaming market data (WebSocket)
    # ------------------------------------------------------------------

    _WS_FIELDS = ["31", "84", "86", "70", "71", "7762", "7295"]

    def _ws_url(self) -> str:
        return self._base_url.replace("https://", "wss://").replace("http://", "ws://") + "/ws"

    def _ws_on_open(self, ws):
        self._ws_open_event.set()
        trading_logger.log_info("IBKR WebSocket connected", {})

    def _ws_on_message(self, ws, raw: str):
        try:
            data = json.loads(raw)
        except Exception:
            return
        conid = data.get("conid")
        if not conid:
            return
        int_conid = int(conid)
        mapping = self._stream_conid_map.get(int_conid)
        if not mapping:
            return
        sym, exch = mapping
        existing = self._stream_prices.get(sym) or Price(symbol=sym, exchange=exch, src="IB")
        if "31" in data:
            existing.last = self._safe_float(data["31"])
        if "84" in data:
            existing.bid = self._safe_float(data["84"])
        if "86" in data:
            existing.ask = self._safe_float(data["86"])
        if "70" in data:
            existing.high = self._safe_float(data["70"])
        if "71" in data:
            existing.low = self._safe_float(data["71"])
        if "7762" in data:
            existing.volume = int(self._safe_float(data["7762"]) or 0)
        if "7295" in data:
            existing.prior_close = self._safe_float(data["7295"])
        existing.timestamp = get_tradingapi_now().isoformat()
        self._stream_prices[sym] = existing
        if self._stream_callback:
            try:
                self._stream_callback(existing)
            except Exception as exc:
                trading_logger.log_warning("IB stream callback error", {"error": str(exc)})

    def _ws_on_error(self, ws, error):
        trading_logger.log_warning("IBKR WebSocket error", {"error": str(error)})

    def _ws_on_close(self, ws, code, msg):
        self._ws_open_event.clear()
        trading_logger.log_info("IBKR WebSocket closed", {"code": code, "msg": msg})

    def _ensure_ws_connected(self) -> None:
        """Start the WebSocket thread if not already running."""
        with self._ws_lock:
            if self._ws_thread and self._ws_thread.is_alive() and self._ws_open_event.is_set():
                return
            self._ws_open_event.clear()
            ws_url = self._ws_url()
            self._ws = _ws_lib.WebSocketApp(
                ws_url,
                on_open=self._ws_on_open,
                on_message=self._ws_on_message,
                on_error=self._ws_on_error,
                on_close=self._ws_on_close,
            )
            self._ws_thread = threading.Thread(
                target=self._ws.run_forever,
                kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
                daemon=True,
                name="ib_ws",
            )
            self._ws_thread.start()
        if not self._ws_open_event.wait(timeout=15):
            raise BrokerConnectionError("IBKR WebSocket did not connect in 15s", {})

    def start_quotes_streaming(
        self,
        operation: str,
        symbols: List[str],
        ext_callback: Optional[Callable] = None,
        exchange: str = "NSE",
    ) -> None:
        """Subscribe ('s') or unsubscribe ('u') from IBKR real-time market data via WebSocket.

        Args:
            operation: 's' to subscribe, 'u' to unsubscribe.
            symbols: Long-symbol strings to subscribe/unsubscribe.
            ext_callback: Called with a Price object on each tick (subscribe only).
            exchange: Exchange name (default 'NSE').
        """
        if operation == "s":
            if ext_callback is not None:
                self._stream_callback = ext_callback
            self._ensure_ws_connected()
            for sym in symbols:
                try:
                    conid = self._resolve_conid(sym, exchange)
                    int_conid = int(conid)
                    self._stream_conid_map[int_conid] = (sym, exchange)
                    fields_json = json.dumps(self._WS_FIELDS)
                    self._ws.send(f"smd+{conid}+{{\"fields\":{fields_json}}}")  # type: ignore[union-attr]
                    trading_logger.log_info("IB subscribed", {"symbol": sym, "conid": conid})
                except Exception as exc:
                    trading_logger.log_warning("IB subscribe failed", {"symbol": sym, "error": str(exc)})
                    raise
        elif operation == "u":
            for sym in symbols:
                try:
                    conid = self._resolve_conid(sym, exchange)
                    int_conid = int(conid)
                    self._stream_conid_map.pop(int_conid, None)
                    self._stream_prices.pop(sym, None)
                    if self._ws and self._ws_open_event.is_set():
                        self._ws.send(f"umd+{conid}+{{}}")
                except Exception as exc:
                    trading_logger.log_warning("IB unsubscribe failed", {"symbol": sym, "error": str(exc)})

    def _stop_ws(self) -> None:
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._ws_thread:
            self._ws_thread.join(timeout=5)
        self._ws_open_event.clear()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    @staticmethod
    def _fmt_date(d) -> str:
        if isinstance(d, (dt.date, dt.datetime)):
            return d.strftime("%Y%m%d-%H:%M:%S")
        return str(d).replace("-", "") + "-00:00:00"

    @staticmethod
    def _date_range_to_period(start, end) -> str:
        try:
            if isinstance(start, str):
                start = dt.datetime.strptime(start[:10], "%Y-%m-%d")
            if isinstance(end, str):
                end = dt.datetime.strptime(end[:10], "%Y-%m-%d")
            days = (end - start).days
            if days <= 1:
                return "1d"
            if days <= 7:
                return "1w"
            if days <= 31:
                return "1m"
            if days <= 365:
                return "1y"
            return f"{days // 365 + 1}y"
        except Exception:
            return "1m"
