import atexit
import asyncio
import datetime as dt
import io
import json
import logging
import math
import os
import re
import secrets
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import pandas as pd
import requests
import redis
import pytz
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

from dhanhq import DhanFeed as MarketFeed
from dhanhq import dhanhq as dhanhq_sdk

from .broker_base import (
    BrokerBase,
    Brokers,
    HistoricalData,
    Order,
    OrderInfo,
    OrderStatus,
    Price,
    _normalize_as_of_date,
)
from .config import get_config
from chameli.dateutils import parse_datetime
from .utils import (
    delete_broker_order_id,
    json_serializer_default,
    set_starting_internal_ids_int,
    update_order_status,
)
from .exceptions import (
    ConfigurationError,
    DataError,
    RedisError,
    SymbolError,
    TradingAPIError,
    BrokerConnectionError,
    OrderError,
    MarketDataError,
    ValidationError,
    AuthenticationError,
    create_error_context,
)
from .error_handling import retry_on_error, log_execution_time, validate_inputs
from . import trading_logger
from . import globals as tradingapi_globals
from .globals import get_tradingapi_now

logger = logging.getLogger(__name__)
config = get_config()


def _is_nan(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _validate_datetime_input(date_input):
    """Helper function to validate datetime input for decorators."""
    try:
        parse_datetime(date_input)
        return True
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# Symbol file helpers
# ---------------------------------------------------------------------------

DHAN_SECURITY_LIST_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

_DHAN_EXCH_ID_MAP = {"NSE": "N", "BSE": "B", "MCX": "M"}
_DHAN_FEED_SEGMENT_MAP = {
    "IDX_I": ("IDX", 0),
    "NSE_EQ": ("NSE", 1),
    "NSE_FNO": ("NSE_FNO", 2),
    "NSE_CURRENCY": ("NSE_CURR", 3),
    "BSE_EQ": ("BSE", 4),
    "MCX_COMM": ("MCX", 5),
    "BSE_CURRENCY": ("BSE_CURR", 7),
    "BSE_FNO": ("BSE_FNO", 8),
}
_DHAN_UNSUPPORTED_SEGMENTS = {"NSE_CURRENCY", "BSE_CURRENCY"}
_DHAN_FEED_SEGMENT_NAME_MAP = {v[1]: k for k, v in _DHAN_FEED_SEGMENT_MAP.items()}


def _normalize_dhan_segment(segment: Any, exchange: Any, instrument_name: Any = None) -> str:
    segment_str = str(segment or "").strip().upper()
    exchange_str = str(exchange or "").strip().upper()
    instrument_str = str(instrument_name or "").strip().upper()

    if segment_str in _DHAN_FEED_SEGMENT_MAP:
        return segment_str
    if segment_str in ("I", "INDEX") or instrument_str == "INDEX":
        return dhanhq_sdk.INDEX
    if segment_str in ("M", "MCX", "MCX_COMM"):
        return dhanhq_sdk.MCX
    if segment_str in ("C", "CUR", "CURRENCY"):
        return "BSE_CURRENCY" if exchange_str == "BSE" else "NSE_CURRENCY"
    if segment_str in ("D", "DERIVATIVE", "DERIVATIVES", "FNO") or instrument_str.startswith(("FUT", "OPT")):
        return dhanhq_sdk.BSE_FNO if exchange_str == "BSE" else dhanhq_sdk.NSE_FNO
    if exchange_str == "BSE":
        return dhanhq_sdk.BSE
    if exchange_str == "MCX":
        return dhanhq_sdk.MCX
    return dhanhq_sdk.NSE


def _segment_to_exchange_char(segment: Any, exchange: Any = None) -> str:
    segment_str = str(segment or "").strip().upper()
    exchange_str = str(exchange or "").strip().upper()
    if segment_str in ("BSE_EQ", "BSE_FNO", "BSE_CURRENCY") or exchange_str in ("BSE", "B"):
        return "B"
    if segment_str == "MCX_COMM" or exchange_str in ("MCX", "M"):
        return "M"
    if exchange_str in ("NSE", "N"):
        return "N"
    return "N"


def _segment_to_feed_code(segment: Any) -> int:
    attr_name, default_value = _DHAN_FEED_SEGMENT_MAP.get(str(segment or "").strip().upper(), ("NSE", 1))
    return int(getattr(MarketFeed, attr_name, default_value))


def _clean_symbol(value: Any) -> str:
    if _is_nan(value):
        return ""
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum() or ch in ("&", "-"))


def _format_strike(value: Any) -> Optional[str]:
    if _is_nan(value):
        return None
    try:
        return ("%f" % float(value)).rstrip("0").rstrip(".")
    except Exception:
        return None


def _extract_dhan_error(response: Any) -> str:
    if not isinstance(response, dict):
        return str(response)
    remarks = response.get("remarks")
    if isinstance(remarks, dict):
        for key in ("error_message", "error_type", "error_code"):
            value = remarks.get(key)
            if value:
                return str(value)
    data = response.get("data")
    if isinstance(data, dict):
        nested = data.get("data")
        if isinstance(nested, dict) and nested:
            return "; ".join(f"{k}: {v}" for k, v in nested.items())
        status = data.get("status")
        if status and status != "success":
            return str(status)
    return str(remarks or response)


def _symbol_supports_oi(long_symbol: str) -> bool:
    return "_FUT_" in long_symbol or "_OPT_" in long_symbol


def _extract_dhan_derivative_underlying(row: pd.Series) -> str:
    trading_symbol = str(row.get("TradingSymbol", "") or "").strip().upper()
    match = re.match(r"^(.*?)-[A-Z]{3}\d{4}(?:-|$)", trading_symbol)
    if match:
        return _clean_symbol(match.group(1))
    return ""


def _build_dhan_long_symbol(row: pd.Series) -> Optional[str]:
    instrument = str(row.get("InstrumentName", "")).upper()
    trading_symbol = _clean_symbol(row.get("TradingSymbol", ""))
    segment = str(row.get("ExchSeg", "")).upper()

    if segment == "IDX_I" or instrument == "INDEX":
        base_symbol = trading_symbol
        return None if not base_symbol else f"{base_symbol}_IND___"
    if instrument == "EQUITY" or segment in ("NSE_EQ", "BSE_EQ"):
        return None if not trading_symbol else f"{trading_symbol}_STK___"
    if instrument.startswith("FUT"):
        base_symbol = _extract_dhan_derivative_underlying(row)
        expiry_raw = row.get("ExpiryDate")
        expiry = pd.to_datetime(str(expiry_raw), errors="coerce") if expiry_raw is not None else pd.NaT
        return None if pd.isna(expiry) or not base_symbol else f"{base_symbol}_FUT_{expiry.strftime('%Y%m%d')}__"
    if instrument.startswith("OPT"):
        base_symbol = _extract_dhan_derivative_underlying(row)
        expiry_raw = row.get("ExpiryDate")
        expiry = pd.to_datetime(str(expiry_raw), errors="coerce") if expiry_raw is not None else pd.NaT
        strike = _format_strike(row.get("StrikePrice"))
        if pd.isna(expiry) or not strike or not base_symbol:
            return None
        right = "CALL" if str(row.get("OptionType", "")).upper() in ("CE", "CALL") else "PUT"
        return f"{base_symbol}_OPT_{expiry.strftime('%Y%m%d')}_{right}_{strike}"
    return None

@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = False):
    """
    Download Dhan's security master CSV and convert to the internal long_symbol format.

    The CSV has columns including:
        SEM_TRADING_SYMBOL, SEM_INSTRUMENT_NAME, SEM_EXPIRY_DATE,
        SEM_STRIKE_PRICE, SEM_OPTION_TYPE, SEM_LOT_UNITS, SEM_TICK_SIZE,
        SEM_SMST_SECURITY_ID (=security_id / scrip code), SEM_EXM_EXCH_ID,
        SEM_SEGMENT (e.g. NSE_EQ, NSE_FNO, MCX_COMM …)

    Returns:
        pd.DataFrame with at minimum columns:
            long_symbol, LotSize, SecurityId, Exch, ExchSeg, TickSize
    """
    try:
        bhavcopyfolder = config.get("bhavcopy_folder")
        dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_dhan_codes.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        response = requests.get(DHAN_SECURITY_LIST_URL, headers=headers, timeout=(10, 300))
        if response.status_code != 200:
            raise Exception(f"Failed to fetch Dhan symbol data. Status: {response.status_code}")

        df = pd.read_csv(io.BytesIO(response.content), low_memory=False)
        df.columns = [col.strip() for col in df.columns]
        object_cols = df.select_dtypes(include=["object"]).columns
        for col in object_cols:
            df[col] = df[col].str.strip()

        # Normalise key columns
        df.rename(
            columns={
                "SEM_SMST_SECURITY_ID": "SecurityId",
                "SEM_LOT_UNITS":        "LotSize",
                "SEM_TICK_SIZE":        "TickSize",
                "SEM_EXM_EXCH_ID":     "Exch",        # NSE / BSE / MCX
                "SEM_SEGMENT":         "ExchSeg",      # E / D / C / M / I
                "SEM_TRADING_SYMBOL":  "TradingSymbol",
                "SEM_INSTRUMENT_NAME": "InstrumentName",
                "SEM_EXPIRY_DATE":     "ExpiryDate",
                "SEM_STRIKE_PRICE":    "StrikePrice",
                "SEM_OPTION_TYPE":     "OptionType",
                "SEM_CUSTOM_SYMBOL":   "CustomSymbol",
                "SM_SYMBOL_NAME":      "SymbolName",   # was missing
            },
            inplace=True,
        )
        
        for col in ["SecurityId", "LotSize", "TickSize", "StrikePrice"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["ExchSeg"] = df.apply(
            lambda row: _normalize_dhan_segment(row.get("ExchSeg"), row.get("Exch"), row.get("InstrumentName")),
            axis=1,
        )
        df["Exch"] = df.apply(lambda row: _segment_to_exchange_char(row.get("ExchSeg"), row.get("Exch")), axis=1)
        df = df[~df["ExchSeg"].isin(_DHAN_UNSUPPORTED_SEGMENTS)].copy()
        df["long_symbol"] = df.apply(_build_dhan_long_symbol, axis=1)
        df = df.dropna(subset=["long_symbol"])

        if saveToFolder:
            dest_symbol_file = (
                f"{config.get('DHAN.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
            )
            filtered = df[["long_symbol", "LotSize", "SecurityId", "Exch", "ExchSeg", "TickSize"]]
            filtered.to_csv(dest_symbol_file, index=False)

        return df

    except Exception as e:
        logger.error(f"Error in save_symbol_data (Dhan): {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Main Dhan broker class
# ---------------------------------------------------------------------------

class Dhan(BrokerBase):
    """
    Dhan broker implementation following the same interface as FivePaisa.

    Config keys expected under [DHAN] section:
        CLIENT_ID       – Dhan client id
        ACCESS_TOKEN    – Access token (set daily from web.dhan.co)
        USERTOKEN       – Path to file where access token is stored
        SYMBOLCODES     – Directory where date-stamped symbol CSV files are stored
    """

    @log_execution_time
    def __init__(self, **kwargs):
        try:
            super().__init__(**kwargs)
            self.codes = pd.DataFrame()
            self.broker = Brokers.DHAN
            self.api: Optional[Any] = None
            self._stream_credentials: Dict[str, str] = {}
            self.subscribe_thread = None
            self.subscribed_symbols: List[str] = []
            self._market_feed: Optional[Any] = None
            self._streaming = False
            self._stream_prices: Dict[str, Price] = {}
            self._stream_symbol_map: Dict[tuple[int, int], tuple[str, str]] = {}
            self._stream_loop: Optional[asyncio.AbstractEventLoop] = None
            self._stream_ws: Optional[Any] = None
            self._stream_connected_event = threading.Event()
            self._process_shutting_down = False
            self._quote_rate_limit_lock = threading.Lock()
            self._last_quote_api_call_ts = 0.0
            self._quote_rate_limit_interval_secs = 1.0  # default: 1 request/second
            self._quote_rate_limit_redis = None
            self._global_quote_rate_limit_key = "dhan:quote:last_call_ts"
            self._global_quote_rate_limit_lock_key = "dhan:quote:lock"
            self._historical_rate_limit_lock = threading.Lock()
            self._last_historical_api_call_ts = 0.0
            self._historical_rate_limit_interval_secs = 0.1  # Dhan historical limit: max 10 requests/second
            self._global_historical_rate_limit_key = "dhan:historical:last_call_ts"
            self._global_historical_rate_limit_lock_key = "dhan:historical:lock"
            self._stream_request_rate_limit_interval_secs = 0.1  # default: 10 streaming requests/second
            atexit.register(self._mark_process_shutting_down)

            trading_logger.log_info(
                "Dhan broker initialized", {"broker_type": "Dhan", "config_keys": list(kwargs.keys())}
            )
        except Exception as e:
            context = create_error_context(broker_type="Dhan", config_keys=list(kwargs.keys()), error=str(e))
            raise BrokerConnectionError(f"Failed to initialize Dhan broker: {str(e)}", context)

    def _mark_process_shutting_down(self):
        self._process_shutting_down = True
        self._streaming = False

    def _is_process_shutting_down(self) -> bool:
        return self._process_shutting_down or bool(getattr(sys, "is_finalizing", lambda: False)())

    # ------------------------------------------------------------------
    # Symbology
    # ------------------------------------------------------------------

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def update_symbology(self, **kwargs):
        """
        Load Dhan's security master and build internal exchange_mappings.

        Returns:
            pd.DataFrame: Symbol codes dataframe
        """
        try:
            trading_logger.log_info("Updating symbology", {"broker_name": self.broker.name})

            dt_today = get_tradingapi_now().strftime("%Y%m%d")
            symbol_codes_path = config.get(f"{self.broker.name}.SYMBOLCODES")
            if not symbol_codes_path:
                raise ConfigurationError(f"SYMBOLCODES path not found in config for {self.broker.name}")

            symbols_path = os.path.join(symbol_codes_path, f"{dt_today}_symbols.csv")

            if not os.path.exists(symbols_path):
                trading_logger.log_info("Symbols file not found, downloading", {"symbols_path": symbols_path})
                codes = save_symbol_data(saveToFolder=False)
            else:
                trading_logger.log_info("Loading existing symbols file", {"symbols_path": symbols_path})
                codes = pd.read_csv(symbols_path)

            codes["ExchSeg"] = codes.apply(
                lambda row: _normalize_dhan_segment(row.get("ExchSeg"), row.get("Exch"), row.get("InstrumentName")),
                axis=1,
            )
            codes["Exch"] = codes.apply(lambda row: _segment_to_exchange_char(row.get("ExchSeg"), row.get("Exch")), axis=1)
            codes = codes[~codes["ExchSeg"].isin(_DHAN_UNSUPPORTED_SEGMENTS)].copy()
            codes["SecurityId"] = pd.to_numeric(codes["SecurityId"], errors="coerce")
            codes = codes.dropna(subset=["long_symbol", "SecurityId"])

            # Build exchange_mappings keyed by single-char exchange (N / B / M)
            self.exchange_mappings = {}
            for exchange, group in codes.groupby("Exch"):
                self.exchange_mappings[exchange] = {
                    "symbol_map": dict(zip(group["long_symbol"], group["SecurityId"].astype(int))),
                    "contractsize_map": dict(zip(group["long_symbol"], group["LotSize"])),
                    "exchange_map": dict(zip(group["long_symbol"], group["Exch"])),
                    "exchangetype_map": dict(zip(group["long_symbol"], group["ExchSeg"])),
                    "contracttick_map": dict(zip(group["long_symbol"], group["TickSize"])),
                    "symbol_map_reversed": dict(zip(group["SecurityId"].astype(int), group["long_symbol"])),
                }

            trading_logger.log_info(
                "Symbology update completed",
                {"total_exchanges": len(self.exchange_mappings), "total_symbols": len(codes)},
            )
            return codes

        except (ConfigurationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(broker_name=self.broker.name, error=str(e))
            raise DataError(f"Unexpected error updating symbology: {str(e)}", context)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(redis_db=lambda x: isinstance(x, int) and x >= 0)
    def connect(self, redis_db: int, as_of_date=None):
        """
        Connect to Dhan trading platform.

        Dhan uses a static access token (generated from web.dhan.co or via DhanLogin).
        The token is read from the file at DHAN.USERTOKEN config path.

        Args:
            redis_db: Redis database number
            as_of_date: Optional back-test date
        """
        normalized = _normalize_as_of_date(as_of_date)
        if normalized is not None:
            tradingapi_globals.TRADINGAPI_NOW = normalized

        try:
            trading_logger.log_info("Connecting to Dhan", {"redis_db": redis_db})

            if config.get(self.broker.name) == {}:
                raise BrokerConnectionError("Dhan configuration not found or empty")

            # Load symbology first
            try:
                self.codes = self.update_symbology()
            except Exception as e:
                trading_logger.log_warning("Failed to update symbology", {"error": str(e)})

            # Read credentials
            client_id = config.get(f"{self.broker.name}.CLIENT_ID")
            if not client_id:
                raise AuthenticationError("DHAN.CLIENT_ID not configured")

            access_token = self._load_access_token()

            self._stream_credentials = {"client_id": str(client_id), "access_token": str(access_token)}
            self.api = dhanhq_sdk(client_id, access_token)

            # Verify connection
            if not self.is_connected():
                raise BrokerConnectionError("Connection verification failed after login")

            # Redis
            self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
            self.redis_o.ping()
            quote_rate_limit_redis_db = config.get(f"{self.broker.name}.QUOTE_RATE_LIMIT_REDIS_DB")
            if quote_rate_limit_redis_db is not None:
                self._quote_rate_limit_redis = redis.Redis(
                    db=int(quote_rate_limit_redis_db), encoding="utf-8", decode_responses=True
                )
                self._quote_rate_limit_redis.ping()
            else:
                self._quote_rate_limit_redis = self.redis_o

            # Optional rate-limit overrides from config (values are requests/second)
            quote_rate_limit_rps = config.get(f"{self.broker.name}.QUOTE_RATE_LIMIT_RPS")
            if quote_rate_limit_rps is not None:
                quote_rate_limit_rps = float(quote_rate_limit_rps)
                if quote_rate_limit_rps <= 0:
                    raise ConfigurationError("DHAN.QUOTE_RATE_LIMIT_RPS must be > 0")
                self._quote_rate_limit_interval_secs = 1.0 / quote_rate_limit_rps

            historical_rate_limit_rps = config.get(f"{self.broker.name}.HISTORICAL_RATE_LIMIT_RPS")
            if historical_rate_limit_rps is not None:
                historical_rate_limit_rps = float(historical_rate_limit_rps)
                if historical_rate_limit_rps <= 0:
                    raise ConfigurationError("DHAN.HISTORICAL_RATE_LIMIT_RPS must be > 0")
                self._historical_rate_limit_interval_secs = 1.0 / historical_rate_limit_rps

            stream_request_rate_limit_rps = config.get(f"{self.broker.name}.REQUEST_STREAMING_DATA_RATE_LIMIT_RPS")
            if stream_request_rate_limit_rps is not None:
                stream_request_rate_limit_rps = float(stream_request_rate_limit_rps)
                if stream_request_rate_limit_rps <= 0:
                    raise ConfigurationError("DHAN.REQUEST_STREAMING_DATA_RATE_LIMIT_RPS must be > 0")
                self._stream_request_rate_limit_interval_secs = 1.0 / stream_request_rate_limit_rps

            self.starting_order_ids_int = set_starting_internal_ids_int(redis_db=self.redis_o)

            trading_logger.log_info("Successfully connected to Dhan", {"redis_db": redis_db})
            return True

        except (ValidationError, BrokerConnectionError, AuthenticationError):
            raise
        except Exception as e:
            context = create_error_context(redis_db=redis_db, broker_name=self.broker.name, error=str(e))
            raise BrokerConnectionError(f"Unexpected error connecting to Dhan: {str(e)}", context)

    def _load_access_token(self) -> str:
        """
        Load access token from file, refreshing via TOTP if stale or missing.

        Tries in order:
        1. Token file exists and was written today → use it directly
        2. TOTP_TOKEN configured → do a fresh login and write to file
        3. ACCESS_TOKEN in config → use it as a last resort (no auto-refresh)
        """
        token_path = config.get(f"{self.broker.name}.USERTOKEN")

        # Check if today's token is already on disk
        if token_path and os.path.exists(token_path):
            mod_date = dt.datetime.fromtimestamp(os.path.getmtime(token_path)).date()
            if mod_date == dt.datetime.now().date():
                with open(token_path, "r") as f:
                    token = f.read().strip()
                if token:
                    trading_logger.log_info("Loaded today's token from file", {"broker": self.broker.name})
                    return token

        # Try TOTP-based fresh login
        totp_token = config.get(f"{self.broker.name}.TOTP_TOKEN")
        client_id = config.get(f"{self.broker.name}.CLIENT_ID")
        pin = config.get(f"{self.broker.name}.PIN")

        if totp_token and client_id and pin:
            token = self._fresh_login_totp(client_id, pin, totp_token, token_path)
            if token:
                return token

        # Last resort: static token in config
        token = config.get(f"{self.broker.name}.ACCESS_TOKEN")
        if token:
            trading_logger.log_warning(
                "Using static ACCESS_TOKEN from config — will not auto-refresh",
                {"broker": self.broker.name},
            )
            return token

        raise AuthenticationError(
            "Could not obtain Dhan access token. Configure TOTP_TOKEN+PIN or ACCESS_TOKEN."
        )
    
    
    def _fresh_login_totp(self, client_id: str, pin: str, totp_secret: str, token_path: Optional[str]) -> Optional[str]:
        """
        Obtain a fresh Dhan access token using TOTP, mirroring FivePaisa's _fresh_login logic.

        Uses Dhan's generateAccessToken endpoint:
            POST https://auth.dhan.co/app/generateAccessToken
            body: { dhanClientId, pin, totp }

        Retries up to 5 times with 40-second waits (same as FivePaisa) to handle
        TOTP window boundaries.
        """
        import pyotp

        url = "https://auth.dhan.co/app/generateAccessToken"
        max_attempts = 5

        for attempt in range(1, max_attempts + 1):
            try:
                trading_logger.log_info(
                    f"Dhan TOTP login attempt {attempt}/{max_attempts}",
                    {"broker": self.broker.name},
                )
                otp = pyotp.TOTP(totp_secret).now()
                resp = requests.post(
                    url,
                    params={"dhanClientId": client_id, "pin": pin, "totp": otp},
                    timeout=30,
                )
                data = resp.json() if resp.content else {}

                access_token = data.get("access_token") or data.get("accessToken")
                if access_token:
                    trading_logger.log_info("Dhan TOTP login successful", {"attempt": attempt})
                    if token_path:
                        try:
                            os.makedirs(os.path.dirname(token_path), exist_ok=True)
                            with open(token_path, "w") as f:
                                f.write(access_token)
                            trading_logger.log_info("Token saved", {"token_path": token_path})
                        except Exception as e:
                            trading_logger.log_warning("Could not save token to file", {"error": str(e)})
                    return access_token
                else:
                    trading_logger.log_warning(
                        "TOTP login returned no token",
                        {"attempt": attempt, "status_code": resp.status_code, "response": data},
                    )
                    if attempt < max_attempts:
                        time.sleep(40)

            except Exception as e:
                trading_logger.log_error(
                    f"TOTP login attempt {attempt} error", e, {"broker": self.broker.name}
                )
                if attempt < max_attempts:
                    time.sleep(40)

        trading_logger.log_warning("All TOTP login attempts failed", {"broker": self.broker.name})
        return None
    
    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def is_connected(self) -> bool:
        """Check connectivity by fetching fund limits."""
        try:
            if self.api is None:
                return False
            result = self.api.get_fund_limits()
            if not result or result.get("status") == "failure":
                trading_logger.log_warning("Dhan is_connected: fund limits check failed", {"result": result})
                return False
            trading_logger.log_debug("Dhan is_connected: OK", {"availabelBalance": result.get("data", {}).get("availabelBalance")})
            return True
        except Exception as e:
            trading_logger.log_error("Dhan is_connected error", e, {})
            return False

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def disconnect(self):
        """Disconnect from Dhan (stops streaming, clears references)."""
        try:
            try:
                self.stop_streaming(clear_state=False)
            except Exception:
                pass
            self.api = None
            self._stream_credentials = {}
            self._quote_rate_limit_redis = None
            trading_logger.log_info("Disconnected from Dhan")
            return True
        except Exception as e:
            raise BrokerConnectionError(f"Failed to disconnect from Dhan: {str(e)}")

    # ------------------------------------------------------------------
    # Capital
    # ------------------------------------------------------------------

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_available_capital(self) -> Dict[str, float]:
        """
            Returns dict with 'cash', 'collateral', and 'used' keys.

        Dhan's get_fund_limits() response data fields:
            availabelBalance, collateralAmount, utilisedAmount, …
        """
        try:
            if not self.is_connected():
                raise BrokerConnectionError("Dhan broker is not connected")

            result = self.api.get_fund_limits()
            if not result or result.get("status") == "failure":
                raise MarketDataError(f"get_fund_limits failed: {result}")

            data = result.get("data", {})
            cash = float(data.get("availabelBalance", 0) or 0)
            collateral = float(data.get("collateralAmount", 0) or 0)
            used = float(data.get("utilisedAmount", 0) or 0)

            trading_logger.log_debug("Available capital retrieved", {"cash": cash, "collateral": collateral, "used": used})
            return {"cash": cash, "collateral": collateral, "used": used}

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise MarketDataError(f"Failed to get available capital: {str(e)}", context)

    # ------------------------------------------------------------------
    # Exchange mapping helpers
    # ------------------------------------------------------------------

    def _get_quote_rate_limit_redis(self):
        return self._quote_rate_limit_redis or getattr(self, "redis_o", None)

    def _wait_for_quote_rate_limit(self):
        limiter_redis = self._get_quote_rate_limit_redis()
        if limiter_redis is not None:
            try:
                self._wait_for_quote_rate_limit_global(limiter_redis)
                return
            except Exception as e:
                trading_logger.log_warning("Global Dhan quote limiter unavailable, using local limiter", {"error": str(e)})
        with self._quote_rate_limit_lock:
            now = time.monotonic()
            elapsed = now - self._last_quote_api_call_ts
            if elapsed < self._quote_rate_limit_interval_secs:
                time.sleep(self._quote_rate_limit_interval_secs - elapsed)
            self._last_quote_api_call_ts = time.monotonic()

    def _wait_for_quote_rate_limit_global(self, limiter_redis):
        token = str(uuid4())
        lock_ttl_ms = 2000
        while True:
            acquired = limiter_redis.set(self._global_quote_rate_limit_lock_key, token, nx=True, px=lock_ttl_ms)
            if acquired:
                try:
                    last_call_ts = limiter_redis.get(self._global_quote_rate_limit_key)
                    now = time.time()
                    if last_call_ts is not None:
                        elapsed = now - float(last_call_ts)
                        if elapsed < self._quote_rate_limit_interval_secs:
                            time.sleep(self._quote_rate_limit_interval_secs - elapsed)
                    limiter_redis.set(self._global_quote_rate_limit_key, str(time.time()))
                    return
                finally:
                    if limiter_redis.get(self._global_quote_rate_limit_lock_key) == token:
                        limiter_redis.delete(self._global_quote_rate_limit_lock_key)
            time.sleep(0.05)

    def _wait_for_historical_rate_limit(self):
        limiter_redis = self._get_quote_rate_limit_redis()
        if limiter_redis is not None:
            try:
                self._wait_for_historical_rate_limit_global(limiter_redis)
                return
            except Exception as e:
                trading_logger.log_warning(
                    "Global Dhan historical limiter unavailable, using local limiter", {"error": str(e)}
                )
        with self._historical_rate_limit_lock:
            now = time.monotonic()
            elapsed = now - self._last_historical_api_call_ts
            if elapsed < self._historical_rate_limit_interval_secs:
                time.sleep(self._historical_rate_limit_interval_secs - elapsed)
            self._last_historical_api_call_ts = time.monotonic()

    def _wait_for_historical_rate_limit_global(self, limiter_redis):
        token = str(uuid4())
        lock_ttl_ms = 1000
        while True:
            acquired = limiter_redis.set(self._global_historical_rate_limit_lock_key, token, nx=True, px=lock_ttl_ms)
            if acquired:
                try:
                    last_call_ts = limiter_redis.get(self._global_historical_rate_limit_key)
                    now = time.time()
                    if last_call_ts is not None:
                        elapsed = now - float(last_call_ts)
                        if elapsed < self._historical_rate_limit_interval_secs:
                            time.sleep(self._historical_rate_limit_interval_secs - elapsed)
                    limiter_redis.set(self._global_historical_rate_limit_key, str(time.time()))
                    return
                finally:
                    if limiter_redis.get(self._global_historical_rate_limit_lock_key) == token:
                        limiter_redis.delete(self._global_historical_rate_limit_lock_key)
            time.sleep(0.05)

    def _fetch_dhan_historical(
        self,
        security_id: int,
        exchange_segment: str,
        instrument_type: str,
        from_date: str,
        to_date: str,
        periodicity: str,
        include_oi: bool,
    ) -> Dict[str, Any]:
        if self.api is None:
            raise BrokerConnectionError("Dhan API not initialized")

        payload: Dict[str, Any] = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument_type,
            "fromDate": from_date,
            "toDate": to_date,
        }
        if include_oi:
            payload["oi"] = True

        if periodicity in {"1d", "d"}:
            payload["expiryCode"] = 0
            url = self.api.base_url + "/charts/historical"
        else:
            interval = {"1m": 1, "5m": 5, "15m": 15, "25m": 25, "60m": 60}[periodicity]
            payload["interval"] = interval
            url = self.api.base_url + "/charts/intraday"

        self._wait_for_historical_rate_limit()
        response = self.api.session.post(url, headers=self.api.header, timeout=self.api.timeout, data=json.dumps(payload))
        return self.api._parse_response(response)

    def _persist_order_to_redis(self, order: Order, side_hint: Optional[str] = None) -> None:
        broker_order_id = str(getattr(order, "broker_order_id", "") or "").strip()
        if not broker_order_id or broker_order_id == "0":
            return
        self.redis_o.hmset(broker_order_id, {k: str(v) for k, v in order.to_dict().items()})
        side = str(side_hint or order.order_type or "").upper()
        key_name = "entry_keys" if side in ("BUY", "SHORT") else "exit_keys" if side in ("SELL", "COVER") else ""
        if key_name:
            existing = self.redis_o.hget(order.internal_order_id, key_name) or ""
            if broker_order_id not in existing.split():
                new_val = f"{existing} {broker_order_id}".strip()
                self.redis_o.hset(order.orderRef or order.internal_order_id, key_name, new_val)
            self.redis_o.hset(order.internal_order_id, "long_symbol", order.long_symbol)

    def _build_historical_placeholder(self) -> HistoricalData:
        return HistoricalData(
            date=dt.datetime(1970, 1, 1),
            open=float("nan"),
            high=float("nan"),
            low=float("nan"),
            close=float("nan"),
            volume=0,
            intoi=0,
            oi=0,
        )

    def _resolve_symbol_lookup(self, long_symbol: str, exchange: Optional[str] = None):
        preferred = exchange[0].upper() if exchange else None
        search_order = [preferred] if preferred else []
        search_order.extend([ex for ex in ("N", "B", "M") if ex not in search_order])

        for exch_char in search_order:
            exchange_data = self.exchange_mappings.get(exch_char, {})
            security_id = exchange_data.get("symbol_map", {}).get(long_symbol)
            if security_id is not None:
                return exch_char, int(security_id), self._exch_char_to_seg(long_symbol, exch_char)
        return None, None, None

    def _exch_char_to_seg(self, long_symbol: str, exchange: str) -> str:
        """Return Dhan SDK exchange segment string."""
        try:
            exch_char = exchange[0].upper()
            seg = (
                self.exchange_mappings.get(exch_char, {})
                .get("exchangetype_map", {})
                .get(long_symbol)
            )
            if seg:
                return seg
            if "_IND___" in long_symbol:
                return dhanhq_sdk.INDEX
            if "_FUT_" in long_symbol or "_OPT_" in long_symbol:
                return dhanhq_sdk.MCX if exch_char == "M" else (dhanhq_sdk.BSE_FNO if exch_char == "B" else dhanhq_sdk.NSE_FNO)
            if exch_char == "B":
                return dhanhq_sdk.BSE
            if exch_char == "M":
                return dhanhq_sdk.MCX
            return dhanhq_sdk.NSE
        except Exception:
            return dhanhq_sdk.NSE
        
    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def map_exchange_for_api(self, long_symbol, exchange) -> str:
        """Return single-char exchange code (N/B/M) for API calls."""
        return exchange[0].upper()

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def map_exchange_for_db(self, long_symbol, exchange) -> str:
        """Return full exchange name for DB (NSE/BSE/MCX)."""
        c = exchange[0].upper()
        return {"N": "NSE", "B": "BSE", "M": "MCX"}.get(c) or exchange

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(order=lambda x: isinstance(x, Order))
    def place_order(self, order: Order, **kwargs) -> Order:
        """
        Place an order via the Dhan API.

        Maps internal Order fields → Dhan SDK place_order() parameters:
            security_id     ← order.scrip_code (SecurityId)
            exchange_segment← derived from exchange + long_symbol
            transaction_type← BUY / SELL
            quantity        ← order.quantity
            order_type      ← LIMIT / MARKET (from price_type or price)
            product_type    ← INTRA (intraday) / CNC (delivery)
            price           ← order.price
            trigger_price   ← order.trigger_price (for SL orders)
            validity        ← DAY
            disclosed_quantity ← order.disqty
            after_market_order ← order.ahplaced == 'Y'
            correlation_id  ← order.internal_order_id  (idempotency)
        """
        try:
            mandatory_keys = ["long_symbol", "order_type", "quantity", "exchange", "internal_order_id", "paper"]
            order_dict = order.to_dict()
            missing_keys = [k for k in mandatory_keys if k not in order_dict or order_dict[k] is None]
            if missing_keys:
                raise ValidationError(f"Missing mandatory keys: {', '.join(missing_keys)}")

            order.broker = self.broker

            # Resolve security_id
            exch_char = self.map_exchange_for_api(order.long_symbol, order.exchange)
            order.scrip_code = (
                self.exchange_mappings.get(exch_char, {})
                .get("symbol_map", {})
                .get(order.long_symbol)
            )

            orig_order_type = order.order_type
            order.remote_order_id = get_tradingapi_now().strftime("%Y%m%d%H%M%S%f")[:-4]

            if order.scrip_code is None and not order.paper:
                trading_logger.log_info("No SecurityId found for symbol", {"long_symbol": order.long_symbol})
                self.log_and_return(order)
                return order

            # Map Dhan transaction type
            if order.order_type in ("BUY", "COVER"):
                dhan_transaction = dhanhq_sdk.BUY
            elif order.order_type in ("SELL", "SHORT"):
                dhan_transaction = dhanhq_sdk.SELL
            else:
                raise ValidationError(f"Invalid order_type: {order.order_type}")

            # Dhan uses MARGIN for carry-forward derivatives; CNC is only for delivery equities.
            exchange_segment = self._exch_char_to_seg(order.long_symbol, order.exchange)
            if order.is_intraday:
                dhan_product = dhanhq_sdk.INTRA
            elif exchange_segment in (dhanhq_sdk.NSE_FNO, dhanhq_sdk.BSE_FNO, dhanhq_sdk.MCX):
                dhan_product = dhanhq_sdk.MARGIN
            else:
                dhan_product = dhanhq_sdk.CNC

            # Map order type (limit vs market vs SL)
            has_trigger = not _is_nan(order.trigger_price)
            if has_trigger:
                dhan_order_type = dhanhq_sdk.SL
                order.is_stoploss_order = True
                order.stoploss_price = order.trigger_price
            elif _is_nan(order.price) or order.price == 0:
                dhan_order_type = dhanhq_sdk.MARKET
            else:
                dhan_order_type = dhanhq_sdk.LIMIT

            if not order.paper:
                try:
                    payload = dict(
                        security_id=str(order.scrip_code),
                        exchange_segment=exchange_segment,
                        transaction_type=dhan_transaction,
                        quantity=int(order.quantity),
                        order_type=dhan_order_type,
                        product_type=dhan_product,
                        price=float(order.price) if not _is_nan(order.price) else 0.0,
                        trigger_price=float(order.trigger_price) if has_trigger else 0.0,
                        disclosed_quantity=int(order.disqty) if order.disqty else 0,
                        after_market_order=order.ahplaced == "Y",
                        validity=dhanhq_sdk.DAY,
                        amo_time="OPEN" if order.ahplaced == "Y" else None,
                        bo_profit_value=None,
                        bo_stop_loss_Value=None,
                        tag=order.internal_order_id[:20] if order.internal_order_id else None,
                    )

                    if self.api is None:
                        raise BrokerConnectionError("Dhan API client not initialized")

                    out = self.api.place_order(**{k: v for k, v in payload.items() if v is not None})
                    trading_logger.log_info(
                        "Dhan place_order response",
                        {"response": json.dumps(out, default=str), "long_symbol": order.long_symbol},
                    )

                    if out and out.get("status") == "success":
                        resp_data = out.get("data", {})
                        order.broker_order_id = str(resp_data.get("orderId", ""))
                        order.exch_order_id = str(resp_data.get("exchangeOrderId", "0") or "0")
                        order.order_type = orig_order_type
                        order.orderRef = order.internal_order_id
                        order.message = out.get("remarks", "")

                        # Fetch fill info
                        try:
                            fills = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                            order.status = fills.status
                            order.exch_order_id = fills.exchange_order_id or order.exch_order_id
                            if fills.fill_price and fills.fill_price > 0:
                                order.price = fills.fill_price
                        except Exception as e:
                            trading_logger.log_warning("Could not fetch fill info after place", {"error": str(e)})

                        # Persist to Redis
                        try:
                            self._persist_order_to_redis(order, side_hint=orig_order_type)
                        except Exception as e:
                            trading_logger.log_error("Redis update failed after place_order", e, {})

                    else:
                        # API returned failure
                        err_msg = out.get("remarks", str(out)) if out else "None response"
                        context = create_error_context(order_data=order.to_dict(), api_response=out)
                        raise OrderError(f"Dhan place_order failed: {err_msg}", context)

                except (OrderError, BrokerConnectionError, ValidationError):
                    raise
                except Exception as e:
                    raise OrderError(f"Failed to place real order: {str(e)}")

            else:
                # Paper trade
                order.order_type = orig_order_type
                order.exch_order_id = str(secrets.randbelow(10 ** 15)) + "P"
                order.broker_order_id = str(secrets.randbelow(10 ** 8)) + "P"
                order.orderRef = order.internal_order_id
                order.message = "Paper Order"
                order.status = OrderStatus.FILLED
                order.scrip_code = 0 if order.scrip_code is None else order.scrip_code
                trading_logger.log_info("Placed Paper Order", {"order": str(order)})

            self.log_and_return(order)
            return order

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(order_data=order.to_dict() if order else None, error=str(e))
            raise OrderError(f"Unexpected error placing order: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        new_price=lambda x: isinstance(x, (int, float)) and x >= 0,
        new_quantity=lambda x: bool(
            isinstance(x, (int, float, str))
            and (isinstance(x, (int, float)) and x >= 0 or (isinstance(x, str) and x.strip() and float(x.strip()) >= 0))
        ),
    )
    def modify_order(self, **kwargs) -> Order:
        """
        Modify a live Dhan order (price + quantity).

        Dhan SDK: dhan.modify_order(order_id, order_type, leg_name, quantity,
                                     price, trigger_price, disclosed_quantity, validity)
        """
        try:
            for key in ("broker_order_id", "new_price", "new_quantity"):
                if key not in kwargs:
                    raise ValidationError(f"Missing mandatory key: {key}")

            broker_order_id = str(kwargs["broker_order_id"])
            new_price = float(kwargs["new_price"])
            new_quantity = int(float(str(kwargs["new_quantity"])))

            if not self.is_connected():
                raise BrokerConnectionError("Dhan broker is not connected")

            # Load order from Redis
            order_data = self.redis_o.hgetall(broker_order_id)
            if order_data:
                order = Order(**order_data)
            else:
                order = kwargs.get("order")
                if order is None:
                    raise OrderError(f"Order {broker_order_id} not found in Redis")
                order.broker_order_id = broker_order_id
                if not getattr(order, "orderRef", ""):
                    order.orderRef = order.internal_order_id
                try:
                    self._persist_order_to_redis(order)
                except Exception as e:
                    trading_logger.log_warning("Could not bootstrap Redis state before modify", {"error": str(e)})

            fills = None
            try:
                fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
            except Exception as e:
                trading_logger.log_warning("Could not fetch fills before modify", {"error": str(e)})

            order.status = fills.status if fills else OrderStatus.UNDEFINED

            if order.status not in (OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED):
                trading_logger.log_info(
                    "Order not modifiable, skipping modify",
                    {"broker_order_id": broker_order_id, "status": order.status.name},
                )
                return order

            has_trigger = not _is_nan(order.trigger_price) and order.trigger_price > 0
            dhan_order_type = dhanhq_sdk.SL if has_trigger else (dhanhq_sdk.MARKET if new_price == 0 else dhanhq_sdk.LIMIT)

            remaining_qty = new_quantity - (fills.fill_size if fills else 0)
            remaining_qty = max(remaining_qty, 0)

            try:
                out = self.api.modify_order(
                    order_id=broker_order_id,
                    order_type=dhan_order_type,
                    leg_name="ENTRY_LEG",
                    quantity=remaining_qty,
                    price=new_price,
                    trigger_price=float(order.trigger_price) if has_trigger else 0.0,
                    disclosed_quantity=0,
                    validity=dhanhq_sdk.DAY,
                )
            except Exception as e:
                raise OrderError(f"Dhan modify_order SDK call failed: {str(e)}")

            if out and out.get("status") == "success":
                order.price = new_price
                order.quantity = new_quantity
                # broker_order_id stays the same in Dhan (no new id on modify)
                try:
                    new_fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
                    order.status = new_fills.status
                    order.exch_order_id = new_fills.exchange_order_id or order.exch_order_id
                    if new_fills.order_price and new_fills.order_price > 0:
                        order.price = new_fills.order_price
                except Exception:
                    pass
                try:
                    self._persist_order_to_redis(order)
                    update_order_status(self, order.internal_order_id, broker_order_id)
                except Exception as e:
                    trading_logger.log_error("Redis update failed after modify", e, {})
                trading_logger.log_info("Order modified successfully", {"broker_order_id": broker_order_id})
            else:
                err_msg = out.get("remarks", str(out)) if out else "None response"
                raise OrderError(f"Dhan modify_order failed: {err_msg}")

            self.log_and_return(order)
            return order

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            raise OrderError(f"Unexpected error modifying order: {str(e)}")

    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def cancel_order(self, **kwargs) -> Order:
        """Cancel an open Dhan order."""
        try:
            broker_order_id = str(kwargs.get("broker_order_id", ""))
            if not broker_order_id:
                raise ValidationError("broker_order_id is required")

            order_data = self.redis_o.hgetall(broker_order_id)
            if order_data:
                order = Order(**order_data)
            else:
                order = kwargs.get("order")
                if order is None:
                    raise OrderError(f"Order {broker_order_id} not found in Redis")
                order.broker_order_id = broker_order_id
                if not getattr(order, "orderRef", ""):
                    order.orderRef = order.internal_order_id
                try:
                    self._persist_order_to_redis(order)
                except Exception as e:
                    trading_logger.log_warning("Could not bootstrap Redis state before cancel", {"error": str(e)})

            if order.status not in (OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED):
                trading_logger.log_info(
                    "Order not cancellable",
                    {"broker_order_id": broker_order_id, "status": order.status.name},
                )
                return order

            # Only cancel today's orders
            try:
                valid_date = parse_datetime(order.remote_order_id[:8])
                date_matches = valid_date.strftime("%Y-%m-%d") == dt.datetime.today().strftime("%Y-%m-%d")
            except Exception:
                date_matches = False

            if not date_matches:
                trading_logger.log_info("Order not from today, skipping cancel", {"broker_order_id": broker_order_id})
                return order

            fills = None
            try:
                fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
            except Exception:
                pass

            if fills and fills.fill_size >= round(float(order.quantity)):
                trading_logger.log_info("Order already fully filled", {"broker_order_id": broker_order_id})
                return order

            try:
                out = self.api.cancel_order(order_id=broker_order_id)
            except Exception as e:
                raise OrderError(f"Dhan cancel_order SDK call failed: {str(e)}")

            if out and out.get("status") == "success":
                try:
                    fills = update_order_status(self, order.internal_order_id, broker_order_id, eod=True)
                    if fills:
                        order.status = fills.status
                        order.quantity = fills.fill_size
                        order.price = fills.fill_price
                    else:
                        order.status = OrderStatus.CANCELLED
                except Exception as e:
                    trading_logger.log_error("Redis update failed after cancel", e, {})
                    order.status = OrderStatus.CANCELLED
                trading_logger.log_info("Order cancelled", {"broker_order_id": broker_order_id})
            else:
                err_msg = out.get("remarks", str(out)) if out else "None response"
                trading_logger.log_warning("Cancel order returned non-success", {"response": err_msg})

            self.log_and_return(order)
            return order

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            raise OrderError(f"Unexpected error cancelling order: {str(e)}")

    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def get_order_info(self, **kwargs) -> OrderInfo:
        """
        Fetch order status from Dhan.

        Uses dhan.get_order_by_id(order_id) which returns order details.
        """
        try:
            broker_order_id = str(kwargs.get("broker_order_id", ""))
            if not broker_order_id:
                raise ValidationError("broker_order_id required")

            order: Optional[Order] = kwargs.get("order")
            if order is None:
                order_data = self.redis_o.hgetall(broker_order_id)
                order = Order(**order_data) if order_data else Order(broker_order_id=broker_order_id, broker=self.broker.name)

            # Paper trade
            if str(broker_order_id).endswith("P"):
                return OrderInfo(
                    order_size=order.quantity,
                    order_price=order.price,
                    fill_size=order.quantity,
                    fill_price=order.price,
                    status=OrderStatus.FILLED,
                    broker_order_id=broker_order_id,
                    broker=self.broker,
                )

            # Historical order
            try:
                valid_date = parse_datetime(order.remote_order_id[:8])
                date_valid = True
            except Exception:
                date_valid = False

            if date_valid and valid_date.strftime("%Y-%m-%d") != dt.datetime.today().strftime("%Y-%m-%d"):
                oi = OrderInfo()
                oi.status = order.status
                oi.order_size = order.quantity
                oi.order_price = order.price
                oi.fill_size = order.quantity
                oi.fill_price = order.price
                oi.exchange_order_id = order.exch_order_id
                oi.broker = order.broker
                return oi

            if self.api is None:
                raise BrokerConnectionError("Dhan API not initialized")

            out = self.api.get_order_by_id(order_id=broker_order_id)
            if not out or out.get("status") == "failure":
                trading_logger.log_warning("get_order_by_id returned failure", {"order_id": broker_order_id})
                return OrderInfo(
                    order_size=order.quantity,
                    order_price=order.price,
                    fill_size=0,
                    fill_price=0,
                    status=OrderStatus.UNDEFINED,
                    broker_order_id=broker_order_id,
                    broker=self.broker,
                )

            data = out.get("data", {})
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                raise OrderError(f"Unexpected get_order_by_id payload type: {type(data).__name__}")

            # Dhan order status mapping
            dhan_status = str(data.get("orderStatus", "")).upper()
            status_map = {
                "TRADED": OrderStatus.FILLED,
                "PART_TRADED": OrderStatus.OPEN,
                "TRANSIT": OrderStatus.PENDING,
                "PENDING": OrderStatus.OPEN,
                "OPEN": OrderStatus.OPEN,
                "CANCELLED": OrderStatus.CANCELLED,
                "REJECTED": OrderStatus.REJECTED,
                "EXPIRED": OrderStatus.CANCELLED,
                "CONFIRM": OrderStatus.OPEN,
            }
            status = status_map.get(dhan_status, OrderStatus.UNDEFINED)

            fill_size = int(data.get("filledQty", 0) or 0)
            fill_price = float(data.get("averageTradedPrice", 0) or 0)
            exch_order_id = str(data.get("exchangeOrderId", order.exch_order_id) or order.exch_order_id)
            order_qty = int(data.get("quantity", order.quantity) or order.quantity)
            order_price = float(data.get("price", order.price) or order.price)
            error_message = str(
                data.get("omsErrorDescription")
                or data.get("omsErrorCode")
                or out.get("remarks", "")
                or ""
            ).strip()

            if order is not None and error_message:
                order.message = error_message

            if status == OrderStatus.REJECTED:
                try:
                    internal_order_id = self.redis_o.hget(broker_order_id, "orderRef")
                    if internal_order_id:
                        delete_broker_order_id(self, internal_order_id, broker_order_id)
                except Exception:
                    pass

            return OrderInfo(
                order_size=order_qty,
                order_price=order_price,
                fill_size=fill_size,
                fill_price=fill_price,
                status=status,
                broker_order_id=broker_order_id,
                exchange_order_id=exch_order_id,
                broker=self.broker,
            )

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            raise OrderError(f"Unexpected error in get_order_info: {str(e)}")

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(
        symbols=lambda x: x is not None,
        date_start=lambda x: _validate_datetime_input(x),
        date_end=lambda x: _validate_datetime_input(x),
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        periodicity=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: Union[str, dt.datetime, dt.date],
        date_end: Union[str, dt.datetime, dt.date] = get_tradingapi_now().strftime("%Y-%m-%d"),
        exchange: str = "N",
        periodicity: str = "1m",
        market_open_time: str = "09:15:00",
        market_close_time: str = "15:30:00",
        refresh_mapping: bool = False,
    ) -> Dict[str, List[HistoricalData]]:
        """
        Retrieve historical OHLCV data from Dhan.

        Dhan provides:
            intraday_minute_data(security_id, exchange_segment, instrument_type, from_date, to_date)
              – supported intervals: 1, 5, 15, 25, 60 min; last 5 trading days only
            historical_daily_data(security_id, exchange_segment, instrument_type, from_date, to_date)
              – full history available

        periodicity mapping (same style as FivePaisa):
            '1m','5m','15m','25m','60m'  → intraday_minute_data
            '1d','d'                     → historical_daily_data
        """
        try:
            # Normalise dates
            date_start_dt = parse_datetime(date_start)
            date_end_dt = parse_datetime(date_end)
            date_start_str = date_start_dt.strftime("%Y-%m-%d")
            date_end_str = date_end_dt.strftime("%Y-%m-%d")

            # Resolve symbols to a DataFrame
            if isinstance(symbols, str):
                exch_char, security_id, exchange_segment = self._resolve_symbol_lookup(symbols, exchange)
                if not security_id:
                    trading_logger.log_warning("SecurityId not found", {"symbol": symbols, "exchange": exchange})
                    return {}
                symbols_pd = pd.DataFrame(
                    [{"long_symbol": symbols, "SecurityId": security_id, "Exch": exch_char, "ExchSeg": exchange_segment}]
                )
            elif isinstance(symbols, dict):
                security_id = symbols.get("scrip_code") or symbols.get("security_id")
                if not security_id:
                    return {}
                long_symbol = symbols.get("long_symbol")
                if not isinstance(long_symbol, str) or not long_symbol.strip():
                    return {}
                exch_char, _, exchange_segment = self._resolve_symbol_lookup(long_symbol, exchange)
                symbols_pd = pd.DataFrame(
                    [{"long_symbol": long_symbol, "SecurityId": security_id, "Exch": exch_char, "ExchSeg": exchange_segment}]
                )
            else:
                symbols_pd = symbols.copy()
                if "SecurityId" not in symbols_pd.columns and "Scripcode" in symbols_pd.columns:
                    symbols_pd["SecurityId"] = symbols_pd["Scripcode"]

            # Determine Dhan periodicity (interval in minutes or "D")
            _INTRADAY_MAP = {"1m": 1, "5m": 5, "15m": 15, "25m": 25, "60m": 60}
            use_intraday = periodicity in _INTRADAY_MAP
            dhan_interval = _INTRADAY_MAP.get(periodicity)

            out: Dict[str, List[HistoricalData]] = {}

            for _, row in symbols_pd.iterrows():
                long_symbol = row["long_symbol"]
                exch_char = row.get("Exch")
                exchange_segment = row.get("ExchSeg")
                security_id = row.get("SecurityId")
                if _is_nan(security_id):
                    exch_char, security_id, exchange_segment = self._resolve_symbol_lookup(long_symbol, exchange)
                if security_id is None or exchange_segment is None:
                    out[long_symbol] = [self._build_historical_placeholder()]
                    continue
                security_id = int(security_id)

                # Determine instrument_type from exchange_segment
                _INDEX_UNDERLYINGS = ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX")
                symbol_upper = str(long_symbol).upper()
                if "_OPT_" in symbol_upper:
                    is_index_derivative = symbol_upper.startswith(_INDEX_UNDERLYINGS)
                    instrument_type = "OPTIDX" if is_index_derivative else "OPTSTK"
                elif "_FUT_" in symbol_upper:
                    is_index_derivative = symbol_upper.startswith(_INDEX_UNDERLYINGS)
                    instrument_type = "FUTIDX" if is_index_derivative else "FUTSTK"
                elif exchange_segment == "IDX_I" or "_IND___" in symbol_upper:
                    instrument_type = dhanhq_sdk.INDEX
                elif exchange_segment == "MCX_COMM":
                    instrument_type = "FUTCOM"
                elif exchange_segment in ("NSE_CURRENCY", "BSE_CURRENCY"):
                    instrument_type = "FUTCUR"
                else:
                    instrument_type = "EQUITY"

                data = None
                include_oi = _symbol_supports_oi(long_symbol)
                try:
                    data = self._fetch_dhan_historical(
                        security_id=security_id,
                        exchange_segment=exchange_segment,
                        instrument_type=instrument_type,
                        from_date=date_start_str,
                        to_date=date_end_str,
                        periodicity=periodicity,
                        include_oi=include_oi,
                    )
                except Exception as e:
                    trading_logger.log_error("Dhan historical API error", e, {"symbol": long_symbol})
                    out[long_symbol] = [self._build_historical_placeholder()]
                    continue

                historical_data_list = []

                if data and data.get("status") == "success":
                    raw = data.get("data", {})
                    # Dhan returns {"open":[], "high":[], "low":[], "close":[], "volume":[], "timestamp":[]}
                    opens = raw.get("open", [])
                    highs = raw.get("high", [])
                    lows = raw.get("low", [])
                    closes = raw.get("close", [])
                    volumes = raw.get("volume", [])
                    timestamps = raw.get("timestamp", [])  # EPOCH seconds
                    open_interest = raw.get("open_interest", [])

                    tz_ist = pytz.timezone("Asia/Kolkata")
                    market_open = pd.to_datetime(market_open_time).time()
                    market_close = pd.to_datetime(market_close_time).time()

                    for i in range(len(timestamps)):
                        try:
                            epoch = int(timestamps[i])
                            ts = pd.Timestamp(
                                dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).astimezone(tz_ist)
                            )

                            if use_intraday:
                                if not (market_open <= ts.time() < market_close):
                                    continue
                            else:
                                ts = ts.floor("D")

                            historical_data_list.append(HistoricalData(
                                date=ts,
                                open=float(opens[i]),
                                high=float(highs[i]),
                                low=float(lows[i]),
                                close=float(closes[i]),
                                volume=int(volumes[i]),
                                intoi=int(float(open_interest[i])) if i < len(open_interest) and not _is_nan(open_interest[i]) else 0,
                                oi=int(float(open_interest[i])) if i < len(open_interest) and not _is_nan(open_interest[i]) else 0,
                            ))
                        except Exception as e:
                            trading_logger.log_warning("Skipping malformed candle", {"index": i, "error": str(e)})
                            continue
                    historical_data_list.sort(key=lambda x: x.date)
                else:
                    trading_logger.log_info("No data from Dhan historical API", )

                if periodicity in {"1d", "d"} and date_end_dt.date() == get_tradingapi_now().date():
                    today_present = any(
                        isinstance(row.date, dt.datetime) and row.date.date() == date_end_dt.date() and not _is_nan(row.close)
                        for row in historical_data_list
                    )
                    if not today_present:
                        intraday_response = self._fetch_dhan_historical(
                            security_id=security_id,
                            exchange_segment=exchange_segment,
                            instrument_type=instrument_type,
                            from_date=date_end_dt.strftime("%Y-%m-%d"),
                            to_date=date_end_dt.strftime("%Y-%m-%d"),
                            periodicity="1m",
                            include_oi=include_oi,
                        )
                        if intraday_response and intraday_response.get("status") == "success":
                            raw_intraday = intraday_response.get("data", {})
                            timestamps = raw_intraday.get("timestamp", [])
                            opens = raw_intraday.get("open", [])
                            highs = raw_intraday.get("high", [])
                            lows = raw_intraday.get("low", [])
                            closes = raw_intraday.get("close", [])
                            volumes = raw_intraday.get("volume", [])
                            open_interest = raw_intraday.get("open_interest", [])
                            tz_ist = pytz.timezone("Asia/Kolkata")
                            cutoff_dt = date_end_dt if isinstance(date_end_dt, dt.datetime) else dt.datetime.combine(date_end_dt, dt.time.max)
                            if cutoff_dt.tzinfo is None:
                                cutoff_dt = tz_ist.localize(cutoff_dt)

                            day_rows = []
                            market_open = pd.to_datetime(market_open_time).time()
                            market_close = pd.to_datetime(market_close_time).time()
                            for i in range(len(timestamps)):
                                try:
                                    ts = dt.datetime.fromtimestamp(int(timestamps[i]), tz=dt.timezone.utc).astimezone(tz_ist)
                                    if ts.date() != date_end_dt.date():
                                        continue
                                    if not (market_open <= ts.time() < market_close):
                                        continue
                                    if ts > cutoff_dt:
                                        continue
                                    day_rows.append(
                                        {
                                            "date": ts,
                                            "open": float(opens[i]),
                                            "high": float(highs[i]),
                                            "low": float(lows[i]),
                                            "close": float(closes[i]),
                                            "volume": int(volumes[i]),
                                            "oi": int(float(open_interest[i])) if i < len(open_interest) and not _is_nan(open_interest[i]) else 0,
                                        }
                                    )
                                except Exception:
                                    continue

                            if day_rows:
                                today_bar = HistoricalData(
                                    date=pd.Timestamp(dt.datetime.combine(date_end_dt.date(), dt.time.min), tz=tz_ist),
                                    open=day_rows[0]["open"],
                                    high=max(row["high"] for row in day_rows),
                                    low=min(row["low"] for row in day_rows),
                                    close=day_rows[-1]["close"],
                                    volume=sum(row["volume"] for row in day_rows),
                                    intoi=day_rows[-1]["oi"],
                                    oi=day_rows[-1]["oi"],
                                )
                                historical_data_list = [
                                    row for row in historical_data_list
                                    if not (
                                        (isinstance(row.date, dt.datetime) and row.date.date() == date_end_dt.date())
                                        or (row.date == dt.datetime(1970, 1, 1))
                                    )
                                ]
                                historical_data_list.append(today_bar)

                out[long_symbol] = historical_data_list

            return out

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise MarketDataError(f"Unexpected error in get_historical: {str(e)}", context)

    # ------------------------------------------------------------------
    # Quote
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_quote(self, long_symbol: str, exchange: str = "NSE") -> Price:
        """
        Get live quote using Dhan's market quote REST API.

        Uses dhan.ohlc_data(securities={exchange_segment: [security_id]})
        for a quick OHLC snapshot, combined with dhan.market_quote() for
        bid/ask depth.
        """
        try:
            market_feed = Price()
            market_feed.src = "dhan"
            market_feed.symbol = long_symbol
            market_feed.exchange = self.map_exchange_for_db(long_symbol, exchange)
            market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            exch_char, security_id, exchange_segment = self._resolve_symbol_lookup(long_symbol, exchange)

            if security_id is None:
                trading_logger.log_warning("SecurityId not found for get_quote", {"long_symbol": long_symbol})
                return market_feed
            market_feed.exchange = self.map_exchange_for_db(long_symbol, exch_char)

            if self.api is None:
                raise BrokerConnectionError("Dhan API not initialized")

            self._wait_for_quote_rate_limit()
            out = self.api.quote_data(securities={exchange_segment: [int(security_id)]})
            if out and out.get("status") == "success":
                response_data = out.get("data", {}) or {}
                quote_root = response_data.get("data", response_data)
                data = quote_root.get(exchange_segment, {}).get(str(security_id), {})
                ohlc = data.get("ohlc", {})
                market_feed.last = float(data.get("last_price", data.get("lastTradedPrice", float("nan"))) or float("nan"))
                market_feed.high = float(ohlc.get("high", data.get("dayHigh", float("nan"))) or float("nan"))
                market_feed.low = float(ohlc.get("low", data.get("dayLow", float("nan"))) or float("nan"))
                market_feed.prior_close = float(ohlc.get("close", data.get("previousClosePrice", float("nan"))) or float("nan"))
                market_feed.volume = int(data.get("volume", data.get("totalTradedVolume", 0)) or 0)
                depth = data.get("depth", {})
                buy_qty = depth.get("buy", [{}])
                sell_qty = depth.get("sell", [{}])
                if buy_qty:
                    market_feed.bid = float(buy_qty[0].get("price", float("nan")) or float("nan"))
                    market_feed.bid_volume = int(buy_qty[0].get("quantity", 0) or 0)
                if sell_qty:
                    market_feed.ask = float(sell_qty[0].get("price", float("nan")) or float("nan"))
                    market_feed.ask_volume = int(sell_qty[0].get("quantity", 0) or 0)
                return market_feed

            trading_logger.log_warning(
                "Dhan quote_data unavailable",
                {"long_symbol": long_symbol, "error": _extract_dhan_error(out)},
            )

            return market_feed

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise MarketDataError(f"Unexpected error in get_quote: {str(e)}", context)

    # ------------------------------------------------------------------
    # Positions & holdings
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(long_symbol=lambda x: x is None or (isinstance(x, str) and len(x.strip()) >= 0))
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_position(self, long_symbol: str = "") -> Union[pd.DataFrame, int]:
        """
        Get net position from Dhan (positions + holdings merged).

        Returns signed int for a specific symbol, or full DataFrame if long_symbol is "".
        """
        try:
            if self.api is None:
                raise BrokerConnectionError("Dhan API not initialized")

            # --- Holdings (delivery positions) ---
            holding = pd.DataFrame(columns=["long_symbol", "quantity"])
            try:
                h_resp = self.api.get_holdings()
                if h_resp and h_resp.get("status") == "success":
                    h_data = h_resp.get("data", []) or []
                    if h_data:
                        h_df = pd.DataFrame(h_data)
                        # Dhan holdings: securityId, exchangeSegment, totalQty
                        h_df["SecurityId"] = pd.to_numeric(h_df.get("securityId", pd.Series()), errors="coerce")
                        h_df["Exch"] = h_df.get("exchangeSegment", pd.Series("NSE_EQ")).map(_segment_to_exchange_char)
                        h_df["long_symbol"] = self.get_long_name_from_broker_identifier(
                            Scripcode=h_df["SecurityId"], Exchange=h_df["Exch"]
                        )
                        h_df["quantity"] = pd.to_numeric(h_df.get("totalQty", pd.Series()), errors="coerce").fillna(0)
                        holding = h_df[["long_symbol", "quantity"]].dropna(subset=["long_symbol"])
            except Exception as e:
                trading_logger.log_error("Error fetching Dhan holdings", e, {})

            # --- Intraday positions ---
            position = pd.DataFrame(columns=["long_symbol", "quantity"])
            try:
                p_resp = self.api.get_positions()
                if p_resp and p_resp.get("status") == "success":
                    p_data = p_resp.get("data", []) or []
                    if p_data:
                        p_df = pd.DataFrame(p_data)
                        p_df["SecurityId"] = pd.to_numeric(p_df.get("securityId", pd.Series()), errors="coerce")
                        p_df["Exch"] = p_df.get("exchangeSegment", pd.Series("NSE_EQ")).map(_segment_to_exchange_char)
                        p_df["long_symbol"] = self.get_long_name_from_broker_identifier(
                            Scripcode=p_df["SecurityId"], Exchange=p_df["Exch"]
                        )
                        p_df["quantity"] = pd.to_numeric(p_df.get("netQty", pd.Series()), errors="coerce").fillna(0)
                        position = p_df[["long_symbol", "quantity"]].dropna(subset=["long_symbol"])
            except Exception as e:
                trading_logger.log_error("Error fetching Dhan positions", e, {})

            merged = pd.merge(position, holding, on="long_symbol", how="outer")
            result = merged.groupby("long_symbol", as_index=False).agg(
                {"quantity_x": "sum", "quantity_y": "sum"}
            )
            result["quantity"] = result["quantity_y"].fillna(0) + result["quantity_x"].fillna(0)
            result = result[["long_symbol", "quantity"]]

            if not long_symbol:
                return result

            pos = result.loc[result.long_symbol == long_symbol, "quantity"]
            if len(pos) == 0:
                return 0
            return int(pos.iloc[0])

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            raise MarketDataError(f"Unexpected error in get_position: {str(e)}")

    # ------------------------------------------------------------------
    # Orders today / trades today
    # ------------------------------------------------------------------

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        """
        Fetch today's order book from Dhan.

        Returns normalised DataFrame with columns:
            long_symbol, internal_order_id, order_time, side, broker_order_id,
            exchange_order_id, ordered, filled, price_type, order_price,
            fill_price, status
        """
        try:
            empty_columns = [
                "long_symbol", "internal_order_id", "order_time", "side",
                "broker_order_id", "exchange_order_id", "ordered", "filled",
                "price_type", "order_price", "fill_price", "status",
            ]
            all_columns = kwargs.get("all_columns", False)
            if self.api is None:
                raise BrokerConnectionError("Dhan API not initialized")

            resp = self.api.get_order_list()
            if not resp or resp.get("status") == "failure":
                return pd.DataFrame(columns=empty_columns if not all_columns else None)

            raw = resp.get("data", []) or []
            if not raw:
                return pd.DataFrame(columns=empty_columns if not all_columns else None)

            orders = pd.DataFrame(raw)

            # Resolve SecurityId → long_symbol
            orders["SecurityId"] = pd.to_numeric(orders.get("securityId", pd.Series()), errors="coerce")
            orders["ExchSeg"] = orders.get("exchangeSegment", "NSE_EQ")
            orders["Exch"] = orders["ExchSeg"].map(_segment_to_exchange_char)
            orders["long_symbol"] = self.get_long_name_from_broker_identifier(
                Scripcode=orders["SecurityId"], Exchange=orders["Exch"]
            )

            if all_columns:
                return orders

            # Normalise to standard schema
            side_map = {"BUY": "Buy", "SELL": "Sell"}
            orders["side"] = orders.get("transactionType", pd.Series()).map(side_map).fillna("")
            orders["price_type"] = orders.get("orderType", pd.Series()).fillna("")
            orders["order_time"] = orders.get("createTime", pd.Series()).fillna("")

            orders = orders.rename(columns={
                "correlationId": "internal_order_id",
                "orderId": "broker_order_id",
                "exchangeOrderId": "exchange_order_id",
                "quantity": "ordered",
                "filledQty": "filled",
                "price": "order_price",
                "averageTradedPrice": "fill_price",
                "orderStatus": "status",
            })

            keep = [
                "long_symbol", "internal_order_id", "order_time", "side",
                "broker_order_id", "exchange_order_id", "ordered", "filled",
                "price_type", "order_price", "fill_price", "status",
            ]
            orders = orders.reindex(columns=[c for c in keep if c in orders.columns], copy=False)
            return orders

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            raise MarketDataError(f"Unexpected error in get_orders_today: {str(e)}")

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        """Fetch today's trade book from Dhan."""
        try:
            if self.api is None:
                raise BrokerConnectionError("Dhan API not initialized")

            resp = self.api.get_trade_book()
            if not resp or resp.get("status") == "failure":
                return pd.DataFrame(columns=["long_symbol"])

            raw = resp.get("data", []) or []
            if not raw:
                return pd.DataFrame(columns=["long_symbol"])

            trades = pd.DataFrame(raw)
            trades["SecurityId"] = pd.to_numeric(trades.get("securityId", pd.Series()), errors="coerce")
            trades["ExchSeg"] = trades.get("exchangeSegment", "NSE_EQ")
            trades["Exch"] = trades["ExchSeg"].map(_segment_to_exchange_char)
            trades["long_symbol"] = self.get_long_name_from_broker_identifier(
                Scripcode=trades["SecurityId"], Exchange=trades["Exch"]
            )
            return trades

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            raise MarketDataError(f"Unexpected error in get_trades_today: {str(e)}")

    # ------------------------------------------------------------------
    # Symbol lookup
    # ------------------------------------------------------------------

    @log_execution_time
    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """
        Resolve Dhan SecurityId + single-char Exchange to long_symbol.

        Args:
            Scripcode (pd.Series): Dhan security IDs
            Exchange  (pd.Series): Single-char exchange codes (N/B/M)

        Returns:
            pd.Series of long_symbol values (None where not found)
        """
        try:
            scripcode = kwargs.get("Scripcode")
            exchange = kwargs.get("Exchange")

            if scripcode is None or exchange is None:
                raise ValidationError("Scripcode and Exchange are required")
            if not isinstance(scripcode, pd.Series) or len(scripcode) == 0:
                raise ValidationError("Scripcode must be a non-empty pd.Series")

            def lookup(sc, ex):
                try:
                    ex_char = str(ex)[0].upper() if ex else "N"
                    rev = self.exchange_mappings.get(ex_char, {}).get("symbol_map_reversed", {})
                    code = int(sc) if sc is not None and not _is_nan(sc) else None
                    if code is None:
                        return None
                    return rev.get(code)
                except Exception:
                    return None

            result = pd.Series(
                [lookup(scripcode.iloc[i], exchange.iloc[i]) for i in range(len(scripcode))],
                index=scripcode.index,
            )
            missing = result.isna().sum()
            if missing > 0:
                trading_logger.log_warning(
                    "Some rows had no mapping", {"missing_count": int(missing), "total": len(result)}
                )
            return result

        except (ValidationError, DataError):
            raise
        except Exception as e:
            raise DataError(f"Unexpected error in get_long_name_from_broker_identifier: {str(e)}")

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def get_min_lot_size(self, long_symbol: str, exchange: str = "N") -> int:
        """Return minimum lot size for a symbol."""
        try:
            exch_char, _, _ = self._resolve_symbol_lookup(long_symbol, exchange)
            if exch_char is None:
                return 0
            lot = (
                self.exchange_mappings.get(exch_char, {})
                .get("contractsize_map", {})
                .get(long_symbol, 0)
            )
            return int(lot) if lot else 0
        except Exception as e:
            raise SymbolError(f"Unexpected error in get_min_lot_size: {str(e)}")

    # ------------------------------------------------------------------
    # Streaming (live market feed)
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(
        operation=lambda x: isinstance(x, str) and x in ("s", "u"),
        symbols=lambda x: isinstance(x, list) and len(x) > 0,
    )
    def start_quotes_streaming(self, operation: str, symbols: List[str], ext_callback=None, exchange: str = "NSE"):
        """
        Subscribe / unsubscribe to live market feed via Dhan's MarketFeed websocket.

        operation: 's' = subscribe, 'u' = unsubscribe
        symbols:   list of long_symbol strings
        ext_callback: callable(Price) called on each tick
        """
        try:
            if self._is_process_shutting_down():
                trading_logger.log_info("Skipping Dhan streaming start during interpreter shutdown")
                return

            def _build_instruments(sym_list):
                instruments = []
                stream_symbol_map: Dict[tuple[int, int], tuple[str, str]] = {}
                for sym in sym_list:
                    exch_char, security_id, seg = self._resolve_symbol_lookup(sym, exchange)
                    if exch_char is not None and security_id is not None and seg is not None:
                        feed_segment = _segment_to_feed_code(seg)
                        security_id_int = int(security_id)
                        request_code = 17 if str(seg).upper() == "IDX_I" else getattr(MarketFeed, "Full", 21)
                        instruments.append((feed_segment, str(security_id_int), request_code))
                        stream_symbol_map[(feed_segment, security_id_int)] = (sym, self.map_exchange_for_db(sym, exch_char))
                return instruments, stream_symbol_map

            def _build_subscription_messages(instruments, stream_operation: str = "s"):
                grouped: Dict[int, List[tuple[int, str]]] = {}
                for feed_segment, security_id, request_code in instruments:
                    grouped.setdefault(int(request_code), []).append((int(feed_segment), str(security_id)))
                messages = []
                for request_code, instrument_list in grouped.items():
                    effective_request_code = int(request_code) if stream_operation == "s" else int(request_code) + 1
                    for i in range(0, len(instrument_list), 100):
                        batch = instrument_list[i : i + 100]
                        messages.append(
                            {
                                "RequestCode": effective_request_code,
                                "InstrumentCount": len(batch),
                                "InstrumentList": [
                                    {
                                        "ExchangeSegment": _DHAN_FEED_SEGMENT_NAME_MAP.get(feed_segment, "NSE_EQ"),
                                        "SecurityId": security_id,
                                    }
                                    for feed_segment, security_id in batch
                                ],
                            }
                        )
                return messages

            def _to_float(value: Any) -> float:
                if value in (None, "", "nan"):
                    return float("nan")
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return float("nan")

            def _to_int(value: Any) -> int:
                if value in (None, "", "nan"):
                    return 0
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return 0

            def _apply_if_valid(current: float, value: Any) -> float:
                parsed = _to_float(value)
                return current if math.isnan(parsed) else parsed

            def _update_depth_fields(price: Price, response: Dict[str, Any]) -> None:
                depth = response.get("depth") or []
                if not depth:
                    return
                best_level = depth[0] or {}
                price.bid = _apply_if_valid(price.bid, best_level.get("bid_price"))
                price.ask = _apply_if_valid(price.ask, best_level.get("ask_price"))
                bid_qty = _to_int(best_level.get("bid_quantity"))
                ask_qty = _to_int(best_level.get("ask_quantity"))
                if bid_qty:
                    price.bid_volume = bid_qty
                if ask_qty:
                    price.ask_volume = ask_qty

            def _on_tick(response):
                try:
                    if not ext_callback:
                        return
                    if not isinstance(response, dict):
                        return
                    security_id = response.get("security_id")
                    exch_seg = response.get("exchange_segment")
                    if security_id is None or exch_seg is None:
                        return
                    stream_key = (_to_int(exch_seg), _to_int(security_id))
                    symbol_meta = self._stream_symbol_map.get(stream_key)
                    if symbol_meta is None:
                        return
                    symbol, db_exchange = symbol_meta
                    price = self._stream_prices.get(symbol, Price())
                    price.src = "dhan"
                    price.symbol = symbol
                    price.exchange = db_exchange
                    price.last = _apply_if_valid(price.last, response.get("LTP"))
                    price.high = _apply_if_valid(price.high, response.get("high"))
                    price.low = _apply_if_valid(price.low, response.get("low"))
                    price.prior_close = _apply_if_valid(
                        price.prior_close,
                        response.get("prev_close", response.get("close")),
                    )
                    volume = _to_int(response.get("volume"))
                    if volume:
                        price.volume = volume
                    price.bid = _apply_if_valid(price.bid, response.get("bidPrice"))
                    price.ask = _apply_if_valid(price.ask, response.get("askPrice"))
                    bid_qty = _to_int(response.get("bidQty"))
                    ask_qty = _to_int(response.get("askQty"))
                    if bid_qty:
                        price.bid_volume = bid_qty
                    if ask_qty:
                        price.ask_volume = ask_qty
                    _update_depth_fields(price, response)
                    price.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._stream_prices[symbol] = price
                    ext_callback(price)
                except Exception as e:
                    trading_logger.log_error("Error in Dhan tick callback", e, {"response": response})

            if operation == "s":
                delta_symbols = [s for s in symbols if s not in self.subscribed_symbols]
                self.subscribed_symbols.extend(delta_symbols)
            elif operation == "u":
                delta_symbols = [s for s in symbols if s in self.subscribed_symbols]
                self.subscribed_symbols = [s for s in self.subscribed_symbols if s not in delta_symbols]
            else:
                delta_symbols = []

            all_instruments, stream_symbol_map = _build_instruments(self.subscribed_symbols)
            delta_instruments, _ = _build_instruments(delta_symbols)
            stream_prices = {symbol: self._stream_prices.get(symbol, Price()) for symbol, _ in stream_symbol_map.values()}
            self._stream_symbol_map = stream_symbol_map
            self._stream_prices = stream_prices
            if not all_instruments:
                if operation == "u":
                    return
                trading_logger.log_warning("No valid instruments for streaming")
                self._stream_symbol_map = {}
                self._stream_prices = {}
                return
            subscription_messages = _build_subscription_messages(all_instruments, "s")
            delta_messages = _build_subscription_messages(delta_instruments, operation)

            def _run_feed():
                loop = None
                try:
                    if self._is_process_shutting_down():
                        return
                    if not self._stream_credentials:
                        raise BrokerConnectionError("Dhan streaming credentials not initialized")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    self._stream_loop = loop
                    self._streaming = True
                    parser = MarketFeed(
                        self._stream_credentials["client_id"],
                        self._stream_credentials["access_token"],
                        [],
                        "v2",
                    )
                    self._market_feed = parser

                    async def _stream_main():
                        url = (
                            "wss://api-feed.dhan.co"
                            f"?version=2&token={self._stream_credentials['access_token']}"
                            f"&clientId={self._stream_credentials['client_id']}&authType=2"
                        )
                        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                            self._stream_ws = ws
                            self._stream_connected_event.set()
                            parser.ws = ws
                            for message in subscription_messages:
                                await ws.send(json.dumps(message))
                                await asyncio.sleep(self._stream_request_rate_limit_interval_secs)
                            while self._streaming:
                                raw = await ws.recv()
                                response = parser.process_data(raw)
                                if response:
                                    _on_tick(response)

                    loop.run_until_complete(_stream_main())
                except Exception as e:
                    if (
                        isinstance(e, RuntimeError)
                        and "cannot schedule new futures after interpreter shutdown" in str(e)
                    ) or self._is_process_shutting_down():
                        trading_logger.log_info("Dhan streaming thread stopped during interpreter shutdown")
                    elif isinstance(e, (ConnectionClosedOK, ConnectionClosed)) and not self._streaming:
                        trading_logger.log_info("Dhan streaming thread closed cleanly")
                    else:
                        trading_logger.log_error("Dhan streaming thread error", e, {})
                finally:
                    try:
                        if loop is not None and not loop.is_closed():
                            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
                            for task in pending:
                                task.cancel()
                            if pending:
                                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                            loop.run_until_complete(loop.shutdown_asyncgens())
                            loop.close()
                    except Exception:
                        pass
                    self._stream_ws = None
                    self._stream_connected_event.clear()
                    self._stream_loop = None
                    self._market_feed = None
                    asyncio.set_event_loop(None)

            if self.subscribe_thread is None or not self.subscribe_thread.is_alive():
                self._stream_connected_event.clear()
                self.subscribe_thread = threading.Thread(target=_run_feed, name="DhanMarketFeed", daemon=True)
                self.subscribe_thread.start()
                trading_logger.log_info("Dhan streaming thread started")
            else:
                if not delta_messages:
                    return
                if not self._stream_connected_event.wait(timeout=5.0):
                    raise BrokerConnectionError("Dhan streaming websocket is not active")
                ws = self._stream_ws
                loop = self._stream_loop
                if ws is None or loop is None or loop.is_closed():
                    raise BrokerConnectionError("Dhan streaming websocket is not active")

                async def _send_incremental_messages():
                    for message in delta_messages:
                        await ws.send(json.dumps(message))
                        await asyncio.sleep(self._stream_request_rate_limit_interval_secs)

                future = asyncio.run_coroutine_threadsafe(_send_incremental_messages(), loop)
                future.result(timeout=5.0)

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            raise MarketDataError(f"Unexpected error in start_quotes_streaming: {str(e)}")

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def stop_streaming(self, clear_state: bool = True):
        """Stop Dhan live market feed."""
        try:
            self._streaming = False
            if self._stream_ws is not None and self._stream_loop is not None:
                try:
                    ws = self._stream_ws
                    loop = self._stream_loop
                    if not loop.is_closed():
                        async def _close_ws():
                            await ws.close()
                            await ws.wait_closed()

                        future = asyncio.run_coroutine_threadsafe(_close_ws(), loop)
                        try:
                            future.result(timeout=2.0)
                        except Exception:
                            pass
                except Exception:
                    pass
            if clear_state:
                self._stream_symbol_map = {}
                self._stream_prices = {}
            if self.subscribe_thread is not None and self.subscribe_thread.is_alive():
                self.subscribe_thread.join(timeout=2.0)
            self.subscribe_thread = None
            trading_logger.log_info("Dhan streaming stopped")
        except Exception as e:
            raise BrokerConnectionError(f"Failed to stop Dhan streaming: {str(e)}")

    # ------------------------------------------------------------------
    # Utility: log_and_return (mirrors FivePaisa)
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(any_object=lambda x: x is not None)
    def log_and_return(self, any_object):
        """Log an object to Redis and return it."""
        try:
            if any_object is None:
                raise ValidationError("Object cannot be None")

            import inspect
            caller_function = inspect.stack()[1].function
            log_object = any_object.to_dict() if hasattr(any_object, "to_dict") else any_object
            log_entry = {
                "caller": caller_function,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "object": log_object,
            }
            try:
                self.redis_o.zadd(
                    "DHAN:LOG",
                    {json.dumps(log_entry, default=json_serializer_default): time.time()},
                )
            except Exception as e:
                raise RedisError(f"Failed to log object to Redis: {str(e)}")

            return any_object

        except (ValidationError, RedisError):
            raise
        except Exception as e:
            raise ValidationError(f"Unexpected error in log_and_return: {str(e)}")
