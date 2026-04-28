import calendar
import datetime as dt
import hashlib
import inspect
import io
import json
import logging
import math
import os
import re
import secrets
import signal
import sys
import threading
import time
import traceback
import zipfile
from urllib.parse import parse_qs, urlparse
from typing import Any, Dict, List, Optional, TypeVar, Union, cast
from uuid import uuid4

T = TypeVar("T")

import numpy as np
import pandas as pd
import pyotp
import pytz
import redis
import requests
from chameli.dateutils import format_datetime, get_expiry, parse_datetime
from selenium import webdriver
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def _validate_datetime_input(date_input):
    """Helper function to validate datetime input for decorators. Returns True if valid, False if invalid."""
    try:
        parse_datetime(date_input)
        return True
    except (ValueError, TypeError):
        return False
from NorenRestApiPy.NorenApi import NorenApi

from .broker_base import BrokerBase, Brokers, HistoricalData, Order, OrderInfo, OrderStatus, Price, _normalize_as_of_date
from .config import get_config
from .utils import json_serializer_default, parse_combo_symbol, set_starting_internal_ids_int, update_order_status
from .exceptions import (
    BrokerConnectionError,
    AuthenticationError,
    MarketDataError,
    DataError,
    SymbolError,
    RedisError,
    ValidationError,
    ConfigurationError,
    create_error_context,
)
from . import trading_logger
from .error_handling import validate_inputs, log_execution_time, retry_on_error
from . import globals as tradingapi_globals
from .globals import get_tradingapi_now

# Set up logging
logger = logging.getLogger(__name__)


# Exception handler
def my_handler(typ, value, trace) -> None:
    trading_logger.log_error(
        "Unhandled exception",
        context={
            "exception_type": typ.__name__,
            "exception_value": str(value),
            "traceback": "".join(traceback.format_tb(trace)),
        },
    )


sys.excepthook = my_handler
config = get_config()


class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(
            self, host="https://api.shoonya.com/NorenWClientTP/", websocket="wss://api.shoonya.com/NorenWSTP/"
        )
        global api
        api = self


class ShoonyaOAuthApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(
            self, host="https://trade.shoonya.com/NorenWClientAPI/", websocket="wss://api.shoonya.com/NorenWSTP/"
        )
        global api
        api = self


def _call_api_method(api_obj: object, method_name: str, **kwargs):
    """Call a dynamic Shoonya API method if present; raise clear error otherwise."""
    method = getattr(api_obj, method_name, None)
    if not callable(method):
        raise AttributeError(f"Shoonya API method '{method_name}' is unavailable")
    return cast(Any, method)(**kwargs)


@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = True) -> pd.DataFrame:
    def merge_without_last(lst) -> str:
        if len(lst) > 1:
            return "-".join(lst[:-1])
        else:
            return lst[0]

    bhavcopyfolder = config.get("bhavcopy_folder")
    _proxies = None
    try:
        from .proxy_utils import get_proxies_for_broker
        _proxies = get_proxies_for_broker("SHOONYA")
    except Exception:
        pass
    url = "https://api.shoonya.com/NSE_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_nse_cash.zip"
    headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    }
    response = requests.get(url, headers=headers, allow_redirects=True, proxies=_proxies or {}, timeout=(10,300))  # Add timeout to `requests.get` to fix Bandit issue
    if response.status_code == 200:
        with open(dest_file, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(dest_file, "r") as zip_ref:
            first_file = zip_ref.namelist()[0]  # get the first file
            with zip_ref.open(first_file) as file:
                codes = pd.read_csv(io.BytesIO(file.read()))
                codes["trading_symbol"] = np.where(
                    codes["Instrument"] == "INDEX", codes["Symbol"], codes["TradingSymbol"]
                )
                codes["Symbol"] = codes["TradingSymbol"].str.split("-").apply(lambda x: merge_without_last(x))
                codes["Symbol"] = codes["Symbol"].replace("NIFTY INDEX", "NIFTY")
                codes["Symbol"] = codes["Symbol"].replace("NIFTY BANK", "BANKNIFTY")
                codes["Symbol"] = codes["Symbol"].replace("INDIA VIX", "INDIAVIX")
                codes["StrikePrice"] = -1
                numeric_columns = [
                    "Token",
                    "StrikePrice",
                    "LotSize",
                    "TickSize",
                ]

                for col in numeric_columns:
                    codes[col] = pd.to_numeric(codes[col], errors="coerce")
                codes.columns = [col.strip() for col in codes.columns]
                codes = codes.map(lambda x: x.strip() if isinstance(x, str) else x)
                codes = codes[(codes.Instrument.isin(["EQ", "BE", "XX", "BZ", "RR", "IV", "INDEX"]))]
                codes["long_symbol"] = None

                def process_row(row) -> str:
                    symbol = row["Symbol"]
                    if row["Instrument"] == "INDEX":
                        return f"{symbol}_IND___".upper()
                    else:
                        return f"{symbol}_STK___".upper()

                codes["long_symbol"] = codes.apply(process_row, axis=1)
                codes["Exch"] = "NSE"
                codes["ExchType"] = "CASH"
                new_column_names = {
                    "LotSize": "LotSize",
                    "Token": "Scripcode",
                    "Exchange": "Exchange",
                    "ExchangeType": "ExchangeType",
                    "TickSize": "TickSize",
                }
                codes.rename(columns=new_column_names, inplace=True)
                codes_nse_cash = codes[
                    ["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]
                ]

    url = "https://api.shoonya.com/BSE_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_bse_cash.zip"
    response = requests.get(url, allow_redirects=True, timeout=10, proxies=_proxies or {})
    if response.status_code == 200:
        with open(dest_file, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(dest_file, "r") as zip_ref:
            first_file = zip_ref.namelist()[0]  # get the first file
            with zip_ref.open(first_file) as file:
                codes = pd.read_csv(io.BytesIO(file.read()))
                codes["Symbol"] = codes["TradingSymbol"]
                codes["StrikePrice"] = -1
                numeric_columns = [
                    "Token",
                    "StrikePrice",
                    "LotSize",
                    "TickSize",
                ]

                for col in numeric_columns:
                    codes[col] = pd.to_numeric(codes[col], errors="coerce")
                codes.columns = [col.strip() for col in codes.columns]
                codes = codes.map(lambda x: x.strip() if isinstance(x, str) else x)
                codes = codes[
                    (
                        codes.Instrument.isin(
                            ["A", "B", "IF", "T", "Z", "XT", "MT", "P", "SCOTT", "TS", "W", "X", "XT", "ZP"]
                        )
                    )
                ]
                codes["long_symbol"] = None

                def process_row(row) -> str:
                    symbol = row["Symbol"]
                    if row["Instrument"] == "INDEX":
                        return f"{symbol}_IND___".upper()
                    else:
                        return f"{symbol}_STK___".upper()

                codes["long_symbol"] = codes.apply(process_row, axis=1)
                codes["Exch"] = "BSE"
                codes["ExchType"] = "CASH"
                new_column_names = {
                    "LotSize": "LotSize",
                    "Token": "Scripcode",
                    "Exchange": "Exchange",
                    "ExchangeType": "ExchangeType",
                    "TickSize": "TickSize",
                    "TradingSymbol": "trading_symbol",
                }
                codes.rename(columns=new_column_names, inplace=True)
                codes_bse_cash = codes[
                    ["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]
                ]
                sensex_row = pd.DataFrame(
                    {
                        "long_symbol": ["SENSEX_IND___"],
                        "LotSize": [0],
                        "Scripcode": [1],
                        "Exch": ["BSE"],
                        "ExchType": ["CASH"],
                        "TickSize": [0],
                        "trading_symbol": ["SENSEX"],
                    }
                )
                codes_bse_cash = pd.concat([codes_bse_cash, sensex_row])
    url = "https://api.shoonya.com/NFO_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_fno.zip"
    response = requests.get(url, allow_redirects=True, timeout=10, proxies=_proxies or {})
    if response.status_code == 200:
        with open(dest_file, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(dest_file, "r") as zip_ref:
            first_file = zip_ref.namelist()[0]  # get the first file
            with zip_ref.open(first_file) as file:
                codes_fno = pd.read_csv(io.BytesIO(file.read()))
                numeric_columns = [
                    "Token",
                    "StrikePrice",
                    "LotSize",
                    "TickSize",
                ]

                for col in numeric_columns:
                    codes_fno[col] = pd.to_numeric(codes_fno[col], errors="coerce")
                codes_fno.columns = [col.strip() for col in codes_fno.columns]
                codes_fno = codes_fno.map(lambda x: x.strip() if isinstance(x, str) else x)
                codes_fno["long_symbol"] = None
                codes_fno["Expiry"] = pd.to_datetime(
                    codes_fno["Expiry"], format="%d-%b-%Y", errors="coerce"
                ).dt.strftime("%Y%m%d")
                # Split TradingSymbol into Symbol and the residual part.
                # - If it starts with SENSEX50, keep SENSEX50 in Symbol.
                # - Otherwise, capture leading token that ends with a letter (digits allowed inside, not trailing).
                codes_fno[["Symbol", "ts_rest"]] = codes_fno["TradingSymbol"].str.extract(
                    r"^(SENSEX50|[A-Z0-9&-]*?[A-Z&-])(?=(?:\d{2}|FUT))(.*)$",
                    expand=True,
                )
                # Fallback: if no match, keep entire TradingSymbol as Symbol
                codes_fno["Symbol"] = codes_fno["Symbol"].fillna(codes_fno["TradingSymbol"])

                def process_row(row) -> str:
                    symbol = row["Symbol"]
                    if row["Instrument"].startswith("OPT"):
                        return f"{symbol}_OPT_{row['Expiry']}_{'CALL' if row['OptionType']=='CE' else 'PUT'}_{row['StrikePrice']:g}".upper()
                    else:
                        return f"{symbol}_FUT_{row['Expiry']}__".upper()

                codes_fno["long_symbol"] = codes_fno.apply(process_row, axis=1)
                codes_fno["Exch"] = "NFO"
                codes_fno["ExchType"] = "NFO"
                new_column_names = {
                    "LotSize": "LotSize",
                    "Token": "Scripcode",
                    "Exchange": "Exchange",
                    "ExchangeType": "ExchangeType",
                    "TickSize": "TickSize",
                    "TradingSymbol": "trading_symbol",
                }
                codes_fno.rename(columns=new_column_names, inplace=True)
                codes_nse_fno = codes_fno[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize","trading_symbol"]]

    url = "https://api.shoonya.com/BFO_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_bse_fno.zip"
    response = requests.get(url, allow_redirects=True, timeout=10, proxies=_proxies or {})
    if response.status_code == 200:
        with open(dest_file, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(dest_file, "r") as zip_ref:
            first_file = zip_ref.namelist()[0]  # get the first file
            with zip_ref.open(first_file) as file:
                codes_fno = pd.read_csv(io.BytesIO(file.read()))
                numeric_columns = [
                    "Token",
                    "StrikePrice",
                    "LotSize",
                    "TickSize",
                ]

                for col in numeric_columns:
                    codes_fno[col] = pd.to_numeric(codes_fno[col], errors="coerce")
                codes_fno.columns = [col.strip() for col in codes_fno.columns]
                codes_fno = codes_fno.map(lambda x: x.strip() if isinstance(x, str) else x)
                codes_fno["long_symbol"] = None
                codes_fno["Expiry"] = pd.to_datetime(
                    codes_fno["Expiry"], format="%d-%b-%Y", errors="coerce"
                ).dt.strftime("%Y%m%d")
                codes_fno[["Symbol", "ts_rest"]] = codes_fno["TradingSymbol"].str.extract(
                    r"^(SENSEX50|[A-Z0-9&-]*?[A-Z&-])(?=(?:\d{2}|FUT))(.*)$",
                    expand=True,
                )

                def process_row(row) -> str:
                    symbol = row["Symbol"]
                    if row["Instrument"].startswith("OPT"):
                        return f"{symbol}_OPT_{row['Expiry']}_{'CALL' if row['OptionType']=='CE' else 'PUT'}_{row['StrikePrice']:g}".upper()
                    else:
                        return f"{symbol}_FUT_{row['Expiry']}__".upper()

                codes_fno["long_symbol"] = codes_fno.apply(process_row, axis=1)
                codes_fno["Exch"] = "BFO"
                codes_fno["ExchType"] = "BFO"
                new_column_names = {
                    "LotSize": "LotSize",
                    "Token": "Scripcode",
                    "Exchange": "Exchange",
                    "ExchangeType": "ExchangeType",
                    "TickSize": "TickSize",
                    "TradingSymbol": "trading_symbol",
                }
                codes_fno.rename(columns=new_column_names, inplace=True)
                codes_bse_fno = codes_fno[
                    ["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]
                ]

    codes = pd.concat([codes_nse_cash, codes_bse_cash, codes_nse_fno, codes_bse_fno])
    if saveToFolder:
        dest_symbol_file = f"{config.get('SHOONYA.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
        codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]].to_csv(
            dest_symbol_file, index=False
        )
    return codes


class Shoonya(BrokerBase):
    def __init__(self, account: Optional[str] = None, **kwargs):
        """
        mandatory_keys = None

        """
        super().__init__()
        self.broker = Brokers.SHOONYA
        self.account_key: str = account if account is not None else self.broker.name
        self.codes = pd.DataFrame()
        self.api = None
        self.subscribe_thread = None
        self.subscribed_symbols = []
        self.socket_opened = False
        self._is_connected_cache_ttl_secs = 3.0
        self._last_is_connected_check_ts = 0.0
        self._last_is_connected_result: Optional[bool] = None
        self._quote_rate_limit_lock = threading.Lock()
        self._last_quote_api_call_ts = 0.0
        self._quote_rate_limit_interval_secs = 1.0
        self._quote_rate_limit_redis = None
        self._global_quote_rate_limit_key = "shoonya:quote:last_call_ts"
        self._global_quote_rate_limit_lock_key = "shoonya:quote:lock"
        self._historical_rate_limit_lock = threading.Lock()
        self._last_historical_api_call_ts = 0.0
        self._historical_rate_limit_interval_secs = 0.5
        self._global_historical_rate_limit_key = "shoonya:historical:last_call_ts"
        self._global_historical_rate_limit_lock_key = "shoonya:historical:lock"
        self._stream_request_rate_limit_lock = threading.Lock()
        self._last_stream_request_api_call_ts = 0.0
        self._stream_request_rate_limit_interval_secs = 0.5
        self._global_stream_request_rate_limit_key = "shoonya:stream:last_call_ts"
        self._global_stream_request_rate_limit_lock_key = "shoonya:stream:lock"
        self._market_data_rate_limit_lock = threading.Lock()
        self._last_market_data_api_call_ts = 0.0
        self._global_market_data_rate_limit_key = "shoonya:market_data:last_call_ts"
        self._global_market_data_rate_limit_lock_key = "shoonya:market_data:lock"

    def _get_adjusted_expiry_date(self, year, month) -> dt.datetime:
        """
        Finds the last Friday or the nearest preceding business day, considering up to three consecutive weekday holidays.
        Assumes weekends (Saturday, Sunday) are non-business days.
        """
        # Start with the last day of the month
        last_day = dt.datetime(year, month, calendar.monthrange(year, month)[1])

        # Find the last Friday of the month
        while last_day.weekday() != 4:  # 4 = Friday
            last_day -= dt.timedelta(days=1)

        # Check if last Friday is a business day by assuming no more than three consecutive weekday holidays
        if last_day.weekday() == 4:
            # Last Friday is a candidate; check up to three days back
            for offset in range(4):  # Check last Friday and up to three preceding days
                potential_expiry = last_day - dt.timedelta(days=offset)
                if potential_expiry.weekday() < 5:  # Ensure it's a weekday
                    return potential_expiry

        raise ValueError("Could not determine a valid expiry day within expected range.")

    def _get_tradingsymbol_from_longname(self, long_name: str, exchange: str) -> str:
        def reverse_split_fno(long_name: str, exchange: str) -> str:
            if exchange in ["NSE", "NFO"]:
                parts = long_name.split("_")
                part1 = parts[0]
                part2 = dt.datetime.strptime(parts[2], "%Y%m%d").strftime("%d%b%y")
                part3 = parts[3][0] if parts[1].startswith("OPT") else "F"  # Check if it's an option or future
                part4 = parts[4]
                return f"{part1}{part2}{part3}{part4}"
            elif exchange in ["BSE", "BFO"]:
                trading_symbol = self.exchange_mappings[exchange]["tradingsymbol_map"].get(long_name)
                if trading_symbol is not None:
                    return trading_symbol
                else:
                    return ""  # Return empty string for missing symbols
            else:
                return ""  # Return empty string for unsupported exchanges

        def reverse_split_cash(long_name: str, exchange: str) -> str:
            if exchange in ["NSE", "NFO"]:
                parts = long_name.split("_")
                # part1 = '-'.join(parts[0].split('-')[:-1]) if '-' in parts[0] else parts[0]
                part1 = parts[0]
                return f"{part1}-EQ"
            elif exchange in ["BSE", "BFO"]:
                parts = long_name.split("_")
                part1 = parts[0]
                return f"{part1}"
            else:
                return ""  # Return empty string for unsupported exchanges

        if "FUT" in long_name or "OPT" in long_name:
            return reverse_split_fno(long_name, exchange).upper()
        else:
            return reverse_split_cash(long_name, exchange).upper()

    def _get_login_credentials(self) -> dict:
        credentials = {
            "user": config.get(f"{self.account_key}.USER"),
            "password": config.get(f"{self.account_key}.PWD"),
            "vendor_code": config.get(f"{self.account_key}.VC"),
            "api_secret": config.get(f"{self.account_key}.APPKEY"),
            "totp_token": config.get(f"{self.account_key}.TOKEN"),
            "imei": "abc12345",
        }
        missing = [name.upper() for name, value in credentials.items() if name != "imei" and not value]
        if missing:
            trading_logger.log_error(
                "Missing required configuration",
                context={"broker": self.broker.name, "missing_configs": missing},
            )
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return credentials

    def _get_oauth_credentials(self) -> Optional[dict]:
        client_id = config.get(f"{self.account_key}.CLIENT_ID")
        secret_code = config.get(f"{self.account_key}.SECRET_CODE")
        if not client_id or not secret_code:
            return None
        user = config.get(f"{self.account_key}.USER")
        return {
            "client_id": client_id,
            "secret_code": secret_code,
            "oauth_base_url": "https://trade.shoonya.com/NorenWClientAPI/",
            "login_url": (
                "https://trade.shoonya.com/OAuthlogin/investor-entry-level/login"
                f"?api_key={client_id}&route_to={user}+s+apikey"
            ),
            "geckodriver_path": os.path.expanduser("~/Downloads/geckodriver"),
        }

    def _extract_auth_code_from_url(self, value: Optional[str]) -> Optional[str]:
        if not value or "code=" not in value:
            return None
        code = parse_qs(urlparse(value).query).get("code", [None])[0]
        if code:
            return code
        match = re.search(r"[?&]code=([^&\"'\\s]+)", value)
        if match:
            return match.group(1)
        return None

    def _scan_browser_for_auth_code(self, driver) -> Optional[str]:
        for candidate in [driver.current_url]:
            code = self._extract_auth_code_from_url(candidate)
            if code:
                return code
        return None

    def _fill_browser_input(self, element, value: str) -> None:
        element.click()
        time.sleep(0.1)
        element.clear()
        element.send_keys(value)
        time.sleep(0.1)

    def _set_api_session_from_token(self, user: str, password: str, usertoken: str) -> None:
        self.api = ShoonyaOAuthApiPy()
        _call_api_method(
            self.api,
            "set_session",
            userid=user,
            password=password,
            usertoken=usertoken,
            accesstoken=usertoken,
        )
        if hasattr(self.api, "injectOAuthHeader"):
            cast(Any, getattr(self.api, "injectOAuthHeader"))(usertoken, user, user)

    def _login_with_web_oauth(self) -> dict:
        login_credentials = self._get_login_credentials()
        oauth_credentials = self._get_oauth_credentials()
        if oauth_credentials is None:
            raise ValueError("Missing required configuration: CLIENT_ID, SECRET_CODE")

        user = login_credentials["user"]
        password = login_credentials["password"]
        auth_code = None
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        driver = None
        try:
            driver = webdriver.Firefox(
                service=FirefoxService(oauth_credentials["geckodriver_path"]),
                options=options,
            )
            wait = WebDriverWait(driver, 30)
            driver.get(oauth_credentials["login_url"])

            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']")))
            time.sleep(1)
            all_inputs = driver.find_elements(
                By.CSS_SELECTOR,
                "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])",
            )
            visible_inputs = [inp for inp in all_inputs if inp.is_displayed()]
            if len(visible_inputs) < 3:
                raise AuthenticationError(
                    "Shoonya OAuth page did not expose expected login fields",
                    create_error_context(broker=self.broker.name, inputs_found=len(visible_inputs)),
                )

            self._fill_browser_input(visible_inputs[0], user)
            self._fill_browser_input(visible_inputs[1], password)
            otp_value = pyotp.TOTP(login_credentials["totp_token"]).now()
            self._fill_browser_input(visible_inputs[2], otp_value)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()

            start = time.time()
            while True:
                auth_code = self._scan_browser_for_auth_code(driver)
                if auth_code:
                    break
                if time.time() - start > 60:
                    new_otp = pyotp.TOTP(login_credentials["totp_token"]).now()
                    if new_otp != otp_value:
                        self._fill_browser_input(visible_inputs[2], new_otp)
                        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()
                        start = time.time()
                        otp_value = new_otp
                        continue
                    break
                time.sleep(0.5)
        except (InvalidSessionIdException, WebDriverException) as e:
            raise AuthenticationError(
                "Shoonya OAuth browser automation failed",
                create_error_context(broker=self.broker.name, error=str(e)),
            ) from e
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

        if not auth_code:
            raise AuthenticationError(
                "Shoonya OAuth browser flow did not return auth code",
                create_error_context(broker=self.broker.name),
            )

        checksum = hashlib.sha256(
            (oauth_credentials["client_id"] + oauth_credentials["secret_code"] + auth_code).encode()
        ).hexdigest()
        token_response = None
        last_http_error = None
        for attempt in range(1, 4):
            token_response = requests.post(
                oauth_credentials["oauth_base_url"] + "GenAcsTok",
                data=f'jData={json.dumps({"code": auth_code, "checksum": checksum})}',
                headers={"Authorization": f"Bearer {checksum}"},
                timeout=30,
            )
            try:
                token_response.raise_for_status()
                last_http_error = None
                break
            except requests.HTTPError as e:
                last_http_error = e
                if token_response.status_code not in {502, 503, 504} or attempt == 3:
                    raise
                time.sleep(2 * attempt)
        if last_http_error is not None:
            raise last_http_error
        token_data = token_response.json()
        usertoken = token_data.get("ActTok") or token_data.get("access_token") or token_data.get("token")
        if not usertoken:
            raise AuthenticationError(
                "GenAcsTok did not return access token",
                create_error_context(broker=self.broker.name, response=str(token_data)),
            )

        self._set_api_session_from_token(user, password, usertoken)
        return token_data

    def _login_with_fresh_totp(self) -> dict:
        credentials = self._get_login_credentials()
        api = ShoonyaApiPy()
        response = _call_api_method(
            api,
            "login",
            userid=credentials["user"],
            password=credentials["password"],
            twoFA=pyotp.TOTP(credentials["totp_token"]).now(),
            vendor_code=credentials["vendor_code"],
            api_secret=credentials["api_secret"],
            imei=credentials["imei"],
        )
        if not response or "susertoken" not in response:
            raise AuthenticationError(
                "Login failed - invalid response from broker",
                create_error_context(broker=self.broker.name, response=str(response)),
            )
        self.api = api
        return response

    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    def connect(self, redis_db: int, as_of_date=None, max_attempts: int = 5) -> bool:
        """
        Connect to Shoonya trading platform with enhanced session management.

        Args:
            redis_db: Redis database number
            as_of_date: Optional date (date, datetime, or str YYYYMMDD/YYYY-MM-DD).
                If provided, the broker loads the symbol file for that date.
            max_attempts: Number of login retry attempts (default: 5).

        Raises:
            ValueError: If configuration is missing or connection fails
        """
        normalized = _normalize_as_of_date(as_of_date)
        if normalized is not None:
            tradingapi_globals.TRADINGAPI_NOW = normalized

        previous_proxy_env = None
        try:
            from .proxy_utils import set_proxy_env_for_broker, restore_proxy_env
            previous_proxy_env = set_proxy_env_for_broker(self.account_key)
        except Exception:
            pass

        def _fresh_login(susertoken_path, max_attempts: int = max_attempts) -> None:
            """Perform fresh login with TOTP retry logic."""
            try:
                trading_logger.log_info("Performing fresh login", {"broker": self.broker.name})
                credentials = self._get_login_credentials()
                oauth_credentials = self._get_oauth_credentials()
                for attempt in range(1, max_attempts + 1):
                    try:
                        trading_logger.log_info(
                            f"Fresh login attempt {attempt}/{max_attempts}", {"broker": self.broker.name}
                        )
                        if oauth_credentials is not None:
                            out = self._login_with_web_oauth()
                            susertoken = (
                                out.get("ActTok") or out.get("access_token") or out.get("token") or out.get("susertoken")
                            )
                        else:
                            out = self._login_with_fresh_totp()
                            susertoken = out["susertoken"]

                        if not susertoken:
                            raise AuthenticationError(
                                "Login flow did not return a usable token",
                                create_error_context(broker=self.broker.name, response=str(out)),
                            )
                        os.makedirs(os.path.dirname(susertoken_path), exist_ok=True)
                        with open(susertoken_path, "w") as file:
                            file.write(susertoken)

                        trading_logger.log_info(
                            "Fresh login successful", {"broker": self.broker.name, "attempt": attempt}
                        )
                        return  # Success, exit retry loop

                    except Exception as e:
                        trading_logger.log_error(
                            f"Login attempt {attempt} failed",
                            e,
                            {
                                "broker": self.broker.name,
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "user": credentials["user"],
                            },
                        )
                        if attempt < max_attempts:
                            time.sleep(40)  # Wait for fresh TOTP
                        else:
                            trading_logger.log_error(
                                "Login failed after all attempts",
                                e,
                                {"broker": self.broker.name, "max_attempts": max_attempts},
                            )
                            raise
            except Exception as e:
                context = create_error_context(susertoken_path=susertoken_path, error=str(e))
                trading_logger.log_error("Error in _fresh_login", e, context)
                raise AuthenticationError(f"Error in _fresh_login: {str(e)}", context)

        def _verify_session(self) -> bool:
            """Verify if the current session is valid using existing is_connected method."""
            return self.is_connected()

        def _restore_session_from_token(susertoken_path) -> bool:
            """Attempt to restore session from existing token."""
            try:
                user = config.get(f"{self.account_key}.USER")
                pwd = config.get(f"{self.account_key}.PWD")

                if not user or not pwd:
                    trading_logger.log_warning("Missing credentials for session restore", {"broker": self.broker.name})
                    return False

                with open(susertoken_path, "r") as file:
                    susertoken = file.read().strip()

                if not susertoken:
                    trading_logger.log_warning("Empty token file", {"broker": self.broker.name})
                    return False

                self.api = ShoonyaOAuthApiPy()
                _call_api_method(
                    self.api,
                    "set_session",
                    userid=user,
                    password=pwd,
                    usertoken=susertoken,
                    accesstoken=susertoken,
                )
                if hasattr(self.api, "injectOAuthHeader"):
                    cast(Any, getattr(self.api, "injectOAuthHeader"))(susertoken, user, user)

                # Verify the session is actually working
                if _verify_session(self):
                    trading_logger.log_info("Session restored from token", {"broker": self.broker.name})
                    return True
                else:
                    trading_logger.log_warning(
                        "Session restoration failed - token may be invalid", {"broker": self.broker.name}
                    )
                    return False

            except Exception as e:
                trading_logger.log_error("Failed to restore session", e, {"broker": self.broker.name})
                return False

        def get_connected() -> bool:
            """Main connection logic with robust session management."""
            susertoken_path = config.get(f"{self.account_key}.USERTOKEN")

            if not susertoken_path:
                trading_logger.log_error("USERTOKEN path not configured", context={"broker": self.broker.name})
                raise ValueError("USERTOKEN path not configured")

            # Check if we can use existing token
            if os.path.exists(susertoken_path):
                try:
                    mod_time = os.path.getmtime(susertoken_path)
                    mod_datetime = dt.datetime.fromtimestamp(mod_time)
                    today = dt.datetime.now().date()

                    if mod_datetime.date() == today:
                        trading_logger.log_info(
                            "Attempting to use existing token",
                            {"broker": self.broker.name, "token_date": mod_datetime.date()},
                        )

                        # Try to restore session from token
                        if _restore_session_from_token(susertoken_path):
                            return True  # Successfully connected using existing token
                        else:
                            trading_logger.log_info(
                                "Existing token failed verification, performing fresh login",
                                {"broker": self.broker.name},
                            )
                            # Fall back to fresh login
                            _fresh_login(susertoken_path)
                            return True
                    else:
                        trading_logger.log_info(
                            "Token is from previous day, performing fresh login",
                            {"broker": self.broker.name, "token_date": mod_datetime.date(), "today": today},
                        )
                        _fresh_login(susertoken_path)
                        return True

                except Exception as e:
                    trading_logger.log_error(
                        "Error checking token file, performing fresh login",
                        e,
                        {"broker": self.broker.name, "susertoken_path": susertoken_path},
                    )
                    if isinstance(e, AuthenticationError):
                        trading_logger.log_error(
                            "Fresh login already failed; not retrying again",
                            e,
                            {"broker": self.broker.name, "susertoken_path": susertoken_path},
                        )
                        raise
                    _fresh_login(susertoken_path)
                    return True
            else:
                trading_logger.log_info("No existing token found, performing fresh login", {"broker": self.broker.name})
                _fresh_login(susertoken_path)
                return True

        try:
            trading_logger.log_info("Connecting to Shoonya", {"redis_db": redis_db, "broker_name": self.broker.name})

            if not config.get(self.account_key):
                context = create_error_context(broker_name=self.account_key, config_keys=list(config.keys()))
                trading_logger.log_error("Configuration file not found or empty", context=context)
                raise ValueError(f"Configuration section '{self.account_key}' not found or empty in config")

            try:
                self.codes = self.update_symbology()
                trading_logger.log_debug(
                    "Symbology updated successfully",
                    {"codes_shape": self.codes.shape if hasattr(self.codes, "shape") else None},
                )
            except Exception as e:
                trading_logger.log_warning("Failed to update symbology", {"error": str(e)})

            # Get connected using robust session management
            get_connected()

            # Initialize Redis connection with error handling
            try:
                self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
                # Test Redis connection
                self.redis_o.ping()
                trading_logger.log_info(
                    "Redis connection established", {"broker": self.broker.name, "redis_db": redis_db}
                )
            except Exception as e:
                trading_logger.log_error(
                    "Redis connection failed", e, {"broker": self.broker.name, "redis_db": redis_db}
                )
                raise

            try:
                self.starting_order_ids_int = set_starting_internal_ids_int(redis_db=self.redis_o)
                trading_logger.log_debug(
                    "Starting order IDs set", {"starting_order_ids_int": self.starting_order_ids_int}
                )
            except Exception as e:
                trading_logger.log_warning("Failed to set starting order IDs", {"error": str(e), "redis_db": redis_db})

            quote_rate_limit_redis_db = config.get(f"{self.account_key}.QUOTE_RATE_LIMIT_REDIS_DB")
            if quote_rate_limit_redis_db is not None:
                self._quote_rate_limit_redis = redis.Redis(
                    db=int(quote_rate_limit_redis_db), encoding="utf-8", decode_responses=True
                )
                self._quote_rate_limit_redis.ping()
            else:
                self._quote_rate_limit_redis = self.redis_o

            quote_rate_limit_rps = config.get(f"{self.account_key}.QUOTE_RATE_LIMIT_RPS")
            if quote_rate_limit_rps is not None:
                v = float(quote_rate_limit_rps)
                if v <= 0:
                    raise ConfigurationError(f"{self.account_key}.QUOTE_RATE_LIMIT_RPS must be > 0")
                self._quote_rate_limit_interval_secs = 1.0 / v

            historical_rate_limit_rps = config.get(f"{self.account_key}.HISTORICAL_RATE_LIMIT_RPS")
            if historical_rate_limit_rps is not None:
                v = float(historical_rate_limit_rps)
                if v <= 0:
                    raise ConfigurationError(f"{self.account_key}.HISTORICAL_RATE_LIMIT_RPS must be > 0")
                self._historical_rate_limit_interval_secs = 1.0 / v

            stream_request_rate_limit_rps = config.get(f"{self.account_key}.REQUEST_STREAMING_DATA_RATE_LIMIT_RPS")
            if stream_request_rate_limit_rps is not None:
                v = float(stream_request_rate_limit_rps)
                if v <= 0:
                    raise ConfigurationError(f"{self.account_key}.REQUEST_STREAMING_DATA_RATE_LIMIT_RPS must be > 0")
                self._stream_request_rate_limit_interval_secs = 1.0 / v

            trading_logger.log_info("Successfully connected to Shoonya", {"redis_db": redis_db})
            return True

        except (ValueError, BrokerConnectionError, AuthenticationError):
            raise
        except Exception as e:
            context = create_error_context(redis_db=redis_db, broker_name=self.broker.name, error=str(e))
            trading_logger.log_error("Unexpected error connecting to Shoonya", e, context)
            raise ValueError(f"Unexpected error connecting to Shoonya: {str(e)}")
        finally:
            try:
                from .proxy_utils import restore_proxy_env
                restore_proxy_env(previous_proxy_env)
            except Exception:
                pass

    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    def is_connected(self) -> bool:
        try:
            now = time.monotonic()
            if (
                self._last_is_connected_result is True
                and now - self._last_is_connected_check_ts < self._is_connected_cache_ttl_secs
            ):
                trading_logger.log_debug(
                    "Using cached connection check result",
                    {
                        "broker": self.broker.name,
                        "cache_age_secs": round(now - self._last_is_connected_check_ts, 3),
                    },
                )
                return True

            # Check if API object exists
            if not hasattr(self, "api") or self.api is None:
                trading_logger.log_warning("API object not initialized", {"broker": self.broker.name})
                self._last_is_connected_result = False
                return False

            # Check limits
            try:
                limits = self.api.get_limits()
                if not limits:
                    trading_logger.log_warning("Empty limits response", {"broker": self.broker.name})
                    self._last_is_connected_result = False
                    return False

                cash = limits.get("cash")
                if cash is None:
                    trading_logger.log_warning(
                        "No cash information in limits", {"broker": self.broker.name, "limits": str(limits)}
                    )
                    self._last_is_connected_result = False
                    return False

                cash_float = float(cash)
                if cash_float <= 0:
                    trading_logger.log_warning(
                        "Cash balance is zero or negative", {"broker": self.broker.name, "cash": cash_float}
                    )
                    self._last_is_connected_result = False
                    return False

            except Exception as e:
                trading_logger.log_error("Failed to get limits", e, {"broker": self.broker.name})
                self._last_is_connected_result = False
                return False

            # Test quote fetching with a known symbol
            try:
                quote = self.get_quote("NIFTY_IND___")
                if not quote or quote.last <= 0:
                    trading_logger.log_warning(
                        "Quote test failed", {"broker": self.broker.name, "quote_last": quote.last if quote else None}
                    )
                    self._last_is_connected_result = False
                    return False

            except Exception as e:
                trading_logger.log_error("Quote test failed", e, {"broker": self.broker.name})
                self._last_is_connected_result = False
                return False

            trading_logger.log_info(
                "Connection check successful",
                {"broker": self.broker.name, "cash": cash_float},
            )
            self._last_is_connected_result = True
            self._last_is_connected_check_ts = now
            return True

        except Exception as e:
            trading_logger.log_error("Connection check failed", e, {"broker": self.broker.name})
            self._last_is_connected_result = False
            return False

    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    def get_available_capital(self) -> Dict[str, float]:
        """
        Get available capital/balance for trading (cash + collateral).

        Returns:
            Dict[str, float]: Dictionary with 'cash' and 'collateral' keys containing float values

        Raises:
            BrokerConnectionError: If broker is not connected
            MarketDataError: If balance retrieval fails
        """
        try:
            if not self.is_connected():
                raise BrokerConnectionError("Shoonya broker is not connected")

            if self.api is None:
                raise BrokerConnectionError("Shoonya API is not initialized")

            limits = self.api.get_limits()
            if not limits:
                raise MarketDataError("No limits data available")

            cash = limits.get("cash")
            if cash is None:
                raise MarketDataError("No cash information in limits")

            cash_float = float(cash)

            # Try to get collateral value (field name may vary)
            collateral = 0.0
            collateral_fields = [
                "collateral",
                "Collateral",
                "collateralvalue",
                "CollateralValue",
                "holdingvalue",
                "HoldingValue",
                "securityvalue",
                "SecurityValue",
            ]
            for field in collateral_fields:
                if field in limits:
                    try:
                        collateral = float(limits[field])
                        break
                    except (ValueError, TypeError):
                        continue

            used = 0.0
            used_fields = [
                "usedmargin",
                "UsedMargin",
                "utilisedamount",
                "UtilisedAmount",
                "marginused",
                "MarginUsed",
                "spanused",
                "SpanUsed",
            ]
            for field in used_fields:
                if field in limits:
                    try:
                        used = float(limits[field])
                        break
                    except (ValueError, TypeError):
                        continue

            trading_logger.log_debug(
                "Available capital retrieved",
                {
                    "cash": cash_float,
                    "collateral": collateral,
                    "used": used,
                    "total_capital": cash_float + collateral,
                    "broker": self.broker.name,
                },
            )
            return {"cash": cash_float, "collateral": collateral, "used": used}

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e), broker=self.broker.name)
            trading_logger.log_error("Error getting available capital", e, context)
            raise MarketDataError(f"Failed to get available capital: {str(e)}", context)

    def disconnect(self) -> bool:
        """
        Disconnect from the Shoonya trading platform.

        Raises:
            BrokerConnectionError: If disconnection fails
        """
        try:
            trading_logger.log_info("Disconnecting from Shoonya", {"broker_type": self.broker.name})

            # Stop websocket if active
            try:
                if hasattr(self, "api") and self.api and hasattr(self, "socket_opened") and self.socket_opened:
                    self.api.close_websocket()
                    self.socket_opened = False
                    trading_logger.log_info("Closed WebSocket connection", {"broker_type": self.broker.name})
            except Exception as e:
                trading_logger.log_warning("Failed to close WebSocket during disconnect", {"error": str(e)})

            # Stop streaming thread if active
            try:
                if hasattr(self, "subscribe_thread") and self.subscribe_thread and self.subscribe_thread.is_alive():
                    trading_logger.log_info("Stopping streaming thread", {"broker_type": self.broker.name})
                    # Note: The actual streaming stop logic would be in the streaming method
            except Exception as e:
                trading_logger.log_warning("Failed to stop streaming thread during disconnect", {"error": str(e)})

            # Log out from broker session if supported by API client
            try:
                if self.api and hasattr(self.api, "logout"):
                    logout_response = self.api.logout()
                    trading_logger.log_info(
                        "Broker logout completed",
                        {"broker_type": self.broker.name, "logout_response": str(logout_response)},
                    )
            except Exception as e:
                trading_logger.log_warning("Failed broker logout during disconnect", {"error": str(e)})

            self._last_is_connected_result = False
            self._last_is_connected_check_ts = 0.0

            # Clear API reference
            if self.api:
                self.api = None
                trading_logger.log_info("API reference cleared", {"broker_type": self.broker.name})

            trading_logger.log_info("Successfully disconnected from Shoonya", {"broker_type": self.broker.name})
            return True

        except Exception as e:
            context = create_error_context(broker_type=self.broker.name, error=str(e))
            trading_logger.log_error("Failed to disconnect from Shoonya", e, context)
            raise BrokerConnectionError(f"Failed to disconnect from Shoonya: {str(e)}", context)

    def update_symbology(self, **kwargs) -> pd.DataFrame:
        dt_today = get_tradingapi_now().strftime("%Y%m%d")
        symbols_path = os.path.join(config.get(f"{self.account_key}.SYMBOLCODES"), f"{dt_today}_symbols.csv")
        if not os.path.exists(symbols_path):
            codes = save_symbol_data(saveToFolder=False)
            codes = codes.dropna(subset=["long_symbol"])
        else:
            codes = pd.read_csv(symbols_path)

        # Initialize dictionaries to hold mappings for each exchange
        self.exchange_mappings = {}

        # Iterate through the data frame and create mappings based on exchange

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
            {
                "total_exchanges": len(self.exchange_mappings),
                "total_symbols": len(codes),
            },
        )

        return codes

    def log_and_return(self, any_object: T) -> T:
        """
        Log and return an object with enhanced error handling.

        Args:
            any_object: Object to log and return

        Returns:
            The original object

        Raises:
            ValidationError: If any_object is None
            RedisError: If Redis logging fails
        """
        try:
            trading_logger.log_debug("Logging and returning object", {"object_type": type(any_object).__name__})

            if any_object is None:
                context = create_error_context(object_type=type(any_object))
                raise ValidationError("Object cannot be None", context)

            caller_function = inspect.stack()[1].function

            try:
                if hasattr(any_object, "to_dict"):
                    # Try to get the object's __dict__
                    try:
                        log_object = getattr(any_object, "to_dict")()  # Use the object's attributes as a dictionary
                    except Exception as e:
                        trading_logger.log_warning(
                            "Error converting object to dict",
                            {"error": str(e), "object_type": type(any_object).__name__},
                        )
                        log_object = {"error": f"Error accessing to_dict: {str(e)}"}
                else:
                    # If no __dict__, treat the object as a simple serializable object (e.g., a dict, list, etc.)
                    log_object = any_object

                # Add the calling function name to the log
                log_entry = {
                    "caller": caller_function,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "object": log_object,
                }

                # Log the entry to Redis
                try:
                    self.redis_o.zadd(
                        f"{self.broker.name.upper()}:LOG",
                        {json.dumps(log_entry, default=json_serializer_default): time.time()},
                    )
                    trading_logger.log_debug(
                        "Object logged to Redis successfully",
                        {"caller_function": caller_function, "object_type": type(any_object).__name__},
                    )
                except Exception as e:
                    context = create_error_context(
                        caller_function=caller_function, object_type=type(any_object).__name__, error=str(e)
                    )
                    raise RedisError(f"Failed to log object to Redis: {str(e)}", context)

                return any_object

            except Exception as e:
                context = create_error_context(object_type=type(any_object).__name__, error=str(e))
                trading_logger.log_warning(
                    "Error logging object to Redis", {"error": str(e), "object_type": type(any_object).__name__}
                )
                return any_object  # Return object even if logging fails

        except (ValidationError, RedisError):
            raise
        except Exception as e:
            context = create_error_context(object_type=type(any_object).__name__ if any_object else None, error=str(e))
            trading_logger.log_error("Unexpected error in log_and_return", e, context)
            return any_object  # Return object even if there's an error

    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(
        order=lambda x: x is not None and hasattr(x, "long_symbol"),
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        quantity=lambda x: isinstance(x, (int, float)) and x > 0,
        price=lambda x: isinstance(x, (int, float)) and x >= 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def place_order(self, order: Order, **kwargs) -> Order:
        try:
            if self.api is None:
                raise BrokerConnectionError("Shoonya API is not initialized")

            order.broker = self.broker

            # Validate exchange mapping exists
            if order.exchange not in self.exchange_mappings:
                trading_logger.log_error(
                    "Exchange not found in mappings",
                    context={"exchange": order.exchange, "available_exchanges": list(self.exchange_mappings.keys())},
                )
                return order

            order.scrip_code = self.exchange_mappings[order.exchange]["symbol_map"].get(order.long_symbol, None)
            orig_order_type = order.order_type

            if order.scrip_code is not None or order.paper:  # if paper, we dont check for valid scrip_code
                if order.order_type == "BUY" or order.order_type == "COVER":
                    order.order_type = "B"
                elif order.order_type == "SHORT" or order.order_type == "SELL":
                    order.order_type = "S"
                else:
                    trading_logger.log_error(
                        "Invalid order type", context={"order_type": order.order_type, "long_symbol": order.long_symbol}
                    )
                    return order
                order.remote_order_id = get_tradingapi_now().strftime("%Y%m%d%H%M%S%f")[:-4]

                if not order.paper:
                    try:
                        quantity = order.quantity
                        product_type = "C" if "_STK_" in order.long_symbol else "M"  # M is NRML , 'I' is MIS
                        has_trigger = not math.isnan(order.trigger_price)
                        price_type = "LMT" if order.price > 0 else "MKT"
                        if has_trigger:
                            price_type = "SL-LMT" if order.price > 0 else "SL-MKT"
                        trading_symbol = self._get_tradingsymbol_from_longname(order.long_symbol, order.exchange)

                        if not trading_symbol:
                            trading_logger.log_error(
                                "Failed to get trading symbol",
                                context={"long_symbol": order.long_symbol, "exchange": order.exchange},
                            )
                            return order

                        out = self.api.place_order(
                            buy_or_sell=order.order_type,
                            product_type=product_type,
                            exchange=order.exchange,
                            tradingsymbol=trading_symbol,
                            quantity=quantity,
                            discloseqty=0,
                            price_type=price_type,
                            price=order.price,
                            trigger_price=order.trigger_price if has_trigger else None,
                            retention="DAY",
                            remarks=order.internal_order_id,
                        )

                        trading_logger.log_info(
                            "Shoonya order info",
                            {
                                "order_info": json.dumps(out, indent=4, default=str),
                                "long_symbol": order.long_symbol,
                                "broker_order_id": order.broker_order_id if hasattr(order, "broker_order_id") else None,
                            },
                        )

                        if not out:
                            trading_logger.log_error(
                                "Empty response from broker",
                                context={
                                    "order": str(order),
                                    "long_symbol": order.long_symbol,
                                    "internal_order_id": order.internal_order_id,
                                },
                            )
                            order.status = OrderStatus.REJECTED
                            order.message = "Empty response from broker"
                            return order

                        if out.get("stat") is None:
                            trading_logger.log_error(
                                "Error placing order",
                                context={
                                    "order": str(order),
                                    "response": str(out),
                                    "long_symbol": order.long_symbol,
                                    "internal_order_id": order.internal_order_id,
                                },
                            )
                            order.status = OrderStatus.REJECTED
                            order.message = "Invalid response from broker"
                            return order

                        if out["stat"].upper() == "OK":
                            order.broker_order_id = out.get("norenordno")
                            order.exch_order_id = out.get("norenordno")
                            order.order_type = orig_order_type
                            order.orderRef = order.internal_order_id

                            if not order.broker_order_id:
                                trading_logger.log_error(
                                    "No broker order ID in response",
                                    context={
                                        "order": str(order),
                                        "response": str(out),
                                        "long_symbol": order.long_symbol,
                                        "internal_order_id": order.internal_order_id,
                                    },
                                )
                                order.status = OrderStatus.REJECTED
                                order.message = "No broker order ID in response"
                                return order

                            try:
                                fills = self.get_order_info(broker_order_id=order.broker_order_id)
                                order.exch_order_id = fills.exchange_order_id
                                order.status = fills.status
                            except Exception as e:
                                trading_logger.log_error(
                                    "Failed to get order info", e, {"broker_order_id": order.broker_order_id}
                                )
                                # Continue with default status
                                order.status = OrderStatus.PENDING

                            try:
                                order.message = self.api.single_order_history(order.broker_order_id)[0].get("rejreason")
                            except Exception as e:
                                trading_logger.log_error(
                                    "Error getting order history", e, {"broker_order_id": order.broker_order_id}
                                )

                            if order.price == 0:
                                if fills.fill_price > 0 and order.price == 0:
                                    order.price = fills.fill_price

                            trading_logger.log_info("Placed Order", {"order": str(order)})
                        else:
                            trading_logger.log_error(
                                "Order placement failed",
                                context={
                                    "order": str(order),
                                    "response": str(out),
                                    "long_symbol": order.long_symbol,
                                    "internal_order_id": order.internal_order_id,
                                },
                            )
                            order.status = OrderStatus.REJECTED
                            order.message = out.get("emsg", "Order placement failed")
                            return order

                    except Exception as e:
                        trading_logger.log_error(
                            "Exception during order placement",
                            e,
                            {
                                "order": str(order),
                                "long_symbol": order.long_symbol,
                                "internal_order_id": order.internal_order_id,
                            },
                        )
                        order.status = OrderStatus.REJECTED
                        order.message = f"Exception during order placement: {str(e)}"
                        return order
                else:
                    order.order_type = orig_order_type
                    order.exch_order_id = str(secrets.randbelow(10**15)) + "P"
                    order.broker_order_id = str(secrets.randbelow(10**8)) + "P"
                    order.orderRef = order.internal_order_id
                    order.message = "Paper Order"
                    order.status = OrderStatus.FILLED
                    order.scrip_code = 0 if order.scrip_code is None else order.scrip_code
                    trading_logger.log_info("Placed Paper Order", {"order": str(order)})

                self.log_and_return(order)
                return order

            if order.scrip_code is None:
                trading_logger.log_info("No broker identifier found for symbol", {"long_symbol": order.long_symbol})

            self.log_and_return(order)
            return order

        except Exception as e:
            trading_logger.log_error("Unexpected error in place_order", e, {"order": str(order) if order else "None"})
            return order

    @log_execution_time
    @validate_inputs(
        broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        new_price=lambda x: isinstance(x, (int, float)) and x >= 0,
        new_quantity=lambda x: isinstance(x, (int, float)) and x > 0,
    )
    def modify_order(self, **kwargs) -> Order:
        """
        Args:
            **kwargs:
                broker_order_id (str): Broker order ID to modify.
                new_price (float): New limit price (0 for market).
                new_quantity (int): New total quantity.
                order (Order, optional): Order object to bootstrap Redis state if not cached.
        """
        mandatory_keys = ["broker_order_id", "new_price", "new_quantity"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id")
        if broker_order_id is None:
            raise ValueError("broker_order_id is required")
        new_price = float(kwargs.get("new_price", 0.0))
        new_quantity = int(kwargs.get("new_quantity", 0))

        # Check connection before proceeding
        if not self.is_connected():
            raise BrokerConnectionError("Shoonya broker is not connected")

        assert self.api is not None  # Type checker assurance after connection check
        order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.broker_order_id != "0":
            fills = self.get_order_info(broker_order_id=broker_order_id)
            if order.status in [OrderStatus.OPEN]:
                # Determine if this is an entry or exit order based on order_type
                # BUY/SHORT are entry orders, SELL/COVER are exit orders
                order_side = "entry" if order.order_type in ["BUY", "SHORT"] else "exit"
                trading_logger.log_info(
                    f"Modifying {order_side} order",
                    {
                        "broker_order_id": broker_order_id,
                        "old_price": order.price,
                        "new_price": new_price,
                        "old_quantity": order.quantity,
                        "new_quantity": new_quantity,
                        "current_fills": str(fills.fill_size),
                    },
                )
                long_symbol = order.long_symbol
                exchange = order.exchange
                trading_symbol = self._get_tradingsymbol_from_longname(long_symbol, exchange)
                newprice_type = "LMT" if new_price > 0 else "MKT"
                try:
                    out = self.api.modify_order(
                        exchange=exchange,
                        tradingsymbol=trading_symbol,
                        orderno=broker_order_id,
                        newquantity=new_quantity,
                        newprice_type=newprice_type,
                        newprice=new_price,
                    )
                except Exception as e:
                    trading_logger.log_error(
                        "Exception during SDK modify_order call",
                        e,
                        {
                            "exchange": exchange,
                            "tradingsymbol": trading_symbol,
                            "orderno": broker_order_id,
                            "newquantity": new_quantity,
                            "newprice_type": newprice_type,
                            "newprice": new_price,
                            "broker_order_id": broker_order_id,
                            "long_symbol": order.long_symbol,
                        },
                    )
                    out = None
                if out is None:
                    trading_logger.log_error(
                        "Error modifying order - API returned None",
                        None,
                        {
                            "broker_order_id": broker_order_id,
                            "old_price": order.price,
                            "new_price": new_price,
                            "old_quantity": order.quantity,
                            "new_quantity": new_quantity,
                            "long_symbol": order.long_symbol,
                        },
                    )
                elif out["stat"].upper() == "OK":
                    self.log_and_return(out)
                    order.quantity = new_quantity
                    order.price = new_price
                    order.price_type = new_price
                    order_info = self.get_order_info(broker_order_id=broker_order_id)
                    order.status = order_info.status
                    order.exch_order_id = order_info.exchange_order_id
                    self.redis_o.hmset(broker_order_id, {key: str(val) for key, val in order.to_dict().items()})
                    trading_logger.log_info(
                        "Order modified successfully",
                        {
                            "broker_order_id": broker_order_id,
                            "new_price": new_price,
                            "new_quantity": new_quantity,
                        },
                    )
                else:
                    # Log the failure with full details
                    trading_logger.log_error(
                        "Order modification failed - broker returned non-OK status",
                        None,
                        {
                            "broker_order_id": broker_order_id,
                            "old_price": order.price,
                            "new_price": new_price,
                            "old_quantity": order.quantity,
                            "new_quantity": new_quantity,
                            "long_symbol": order.long_symbol,
                            "api_response": out,
                            "api_status": out.get("stat") if out else None,
                        },
                    )
                    self.log_and_return(out)
                self.log_and_return(order)
                return order
            else:
                trading_logger.log_info(
                    "Order status does not allow modification",
                    {"broker_order_id": order.broker_order_id, "status": str(order.status)},
                )
                self.log_and_return(order)
                return order
        return Order()

    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def cancel_order(self, **kwargs) -> Order:
        """
        mandatory_keys = ['broker_order_id']

        """
        if self.api is None:
            raise BrokerConnectionError("Shoonya API is not initialized")

        mandatory_keys = ["broker_order_id"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id")
        if broker_order_id is None:
            raise ValueError("broker_order_id is required")

        order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.status in [OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED]:
            try:
                valid_date = parse_datetime(order.remote_order_id[:8])
                date_matches = valid_date.strftime("%Y-%m-%d") == dt.datetime.today().strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_matches = False
            if date_matches:
                fills = self.get_order_info(broker_order_id=broker_order_id)
                if fills.fill_size < round(float(order.quantity)):
                    trading_logger.log_info(
                        "Cancelling broker order",
                        {
                            "broker_order_id": broker_order_id,
                            "long_symbol": order.long_symbol,
                            "filled": str(fills.fill_size),
                            "ordered": order.quantity,
                        },
                    )
                    out = self.api.cancel_order(orderno=broker_order_id)
                    self.log_and_return(out)
                    fills = update_order_status(self, order.internal_order_id, broker_order_id, eod=True)
                    self.log_and_return(fills)
                    if fills is not None:
                        order.status = fills.status
                        order.quantity = fills.fill_size
                        order.price = fills.fill_price
                    self.log_and_return(order)
                    return order
        self.log_and_return(order)
        return order

    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def get_order_info(self, **kwargs) -> OrderInfo:
        """
        mandatory_keys = ['broker_order_id']

        """
        if self.api is None:
            raise BrokerConnectionError("Shoonya API is not initialized")

        trading_logger.log_debug("Getting order info", {"broker_order_id": kwargs.get("broker_order_id")})

        def return_db_as_fills(order: Order) -> OrderInfo:
            order_info = OrderInfo()
            try:
                valid_date = parse_datetime(order.remote_order_id[:8])
                date_differs = valid_date.strftime("%Y-%m-%d") != dt.datetime.today().strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_differs = False
            if date_differs:
                order_info.status = order.status
            else:
                order_info.status = OrderStatus.HISTORICAL
            order_info.order_size = int(float(order.quantity))
            order_info.order_price = float(order.price)
            order_info.fill_size = int(float(order.quantity))
            order_info.fill_price = float(order.price)
            order_info.exchange_order_id = order.exch_order_id
            order_info.broker = order.broker
            return order_info

        mandatory_keys = ["broker_order_id"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id", "0")
        order_info = OrderInfo()
        status_mapping = {
            "PENDING": OrderStatus.PENDING,
            "CANCELED": OrderStatus.CANCELLED,
            "OPEN": OrderStatus.OPEN,
            "REJECTED": OrderStatus.REJECTED,
            "COMPLETE": OrderStatus.FILLED,
            "TRIGGER_PENDING": OrderStatus.PENDING,
            "INVALID_STATUS_TYPE": OrderStatus.UNDEFINED,
        }
        order = Order(**self.redis_o.hgetall(broker_order_id))
        if str(broker_order_id).endswith("P"):
            trading_logger.log_debug("Paper Trade being skipped", {"broker_order_id": broker_order_id})
            return OrderInfo(
                order_size=order.quantity,
                order_price=order.price,
                fill_size=order.quantity,
                fill_price=order.price,
                status=OrderStatus.FILLED,
                broker_order_id=order.broker_order_id,
                broker=self.broker,
            )

        try:
            valid_date = parse_datetime(order.remote_order_id[:8])
            date_valid = True
        except (ValueError, TypeError):
            date_valid = False
        # Compare as string: valid_date is datetime from parse_datetime; only use return_db_as_fills for past days
        if date_valid and (
            valid_date.strftime("%Y-%m-%d") != dt.datetime.today().strftime("%Y-%m-%d")
            or (order.remote_order_id != "" and order.broker != self.broker)
        ):
            return return_db_as_fills(order)

        out = self.api.single_order_history(broker_order_id)
        if out is None:
            order_info.order_size = int(float(order.quantity))
            order_info.order_price = float(order.price)
            order_info.fill_size = int(float(order.quantity))
            order_info.fill_price = float(order.price)
            order_info.exchange_order_id = order.exch_order_id
            order_info.broker = self.broker
            order_info.status = OrderStatus.UNDEFINED
            return order_info

        trading_logger.log_debug("Order Status", {"order_status": json.dumps(out, indent=4, default=str)})
        latest_status = out[0]
        order_info.order_size = int(latest_status.get("qty"))
        order_info.order_price = float(latest_status.get("prc"))
        order_info.fill_size = int(latest_status.get("fillshares", 0))
        order_info.fill_price = float(latest_status.get("avgprc", 0))
        order_info.exchange_order_id = latest_status.get("exchordid")
        order_info.broker_order_id = broker_order_id
        order_info.broker = self.broker
        if latest_status.get("status") in status_mapping:
            order_info.status = status_mapping[latest_status.get("status")]
        else:
            order_info.status = OrderStatus.UNDEFINED
        return order_info

    # ------------------------------------------------------------------
    # Rate limiting helpers
    # ------------------------------------------------------------------

    def _get_rate_limit_redis(self):
        return self._quote_rate_limit_redis or getattr(self, "redis_o", None)

    def _set_local_market_data_last_call_ts(self, ts: float) -> None:
        self._last_market_data_api_call_ts = ts
        self._last_quote_api_call_ts = ts
        self._last_historical_api_call_ts = ts
        self._last_stream_request_api_call_ts = ts

    def _wait_for_market_data_rate_limit_local(self, interval_secs: float) -> None:
        with self._market_data_rate_limit_lock:
            now = time.monotonic()
            elapsed = now - self._last_market_data_api_call_ts
            if elapsed < interval_secs:
                time.sleep(interval_secs - elapsed)
                now = time.monotonic()
            self._set_local_market_data_last_call_ts(now)

    def _wait_for_market_data_rate_limit_global(self, limiter_redis, interval_secs: float) -> None:
        token = str(uuid4())
        lock_ttl_ms = 2000
        while True:
            acquired = limiter_redis.set(self._global_market_data_rate_limit_lock_key, token, nx=True, px=lock_ttl_ms)
            if acquired:
                try:
                    last_call_ts = limiter_redis.get(self._global_market_data_rate_limit_key)
                    if last_call_ts is None:
                        legacy_ts = []
                        for key in (
                            self._global_historical_rate_limit_key,
                            self._global_quote_rate_limit_key,
                            self._global_stream_request_rate_limit_key,
                        ):
                            value = limiter_redis.get(key)
                            try:
                                if value is not None:
                                    legacy_ts.append(float(value))
                            except (TypeError, ValueError):
                                continue
                        if legacy_ts:
                            last_call_ts = str(max(legacy_ts))
                    now = time.time()
                    if last_call_ts is not None:
                        elapsed = now - float(last_call_ts)
                        if elapsed < interval_secs:
                            time.sleep(interval_secs - elapsed)
                            now = time.time()
                    ts = str(now)
                    limiter_redis.set(self._global_market_data_rate_limit_key, ts)
                    limiter_redis.set(self._global_historical_rate_limit_key, ts)
                    limiter_redis.set(self._global_quote_rate_limit_key, ts)
                    limiter_redis.set(self._global_stream_request_rate_limit_key, ts)
                    return
                finally:
                    if limiter_redis.get(self._global_market_data_rate_limit_lock_key) == token:
                        limiter_redis.delete(self._global_market_data_rate_limit_lock_key)
            time.sleep(0.05)

    def _wait_for_quote_rate_limit(self):
        limiter_redis = self._get_rate_limit_redis()
        if limiter_redis is not None:
            try:
                self._wait_for_market_data_rate_limit_global(limiter_redis, self._quote_rate_limit_interval_secs)
                return
            except Exception as e:
                trading_logger.log_warning("Global Shoonya quote limiter unavailable, using local", {"error": str(e)})
        self._wait_for_market_data_rate_limit_local(self._quote_rate_limit_interval_secs)

    def _wait_for_historical_rate_limit(self):
        limiter_redis = self._get_rate_limit_redis()
        if limiter_redis is not None:
            try:
                self._wait_for_market_data_rate_limit_global(limiter_redis, self._historical_rate_limit_interval_secs)
                return
            except Exception as e:
                trading_logger.log_warning(
                    "Global Shoonya historical limiter unavailable, using local", {"error": str(e)}
                )
        self._wait_for_market_data_rate_limit_local(self._historical_rate_limit_interval_secs)

    def _wait_for_stream_request_rate_limit(self):
        limiter_redis = self._get_rate_limit_redis()
        if limiter_redis is not None:
            try:
                self._wait_for_market_data_rate_limit_global(
                    limiter_redis, self._stream_request_rate_limit_interval_secs
                )
                return
            except Exception as e:
                trading_logger.log_warning(
                    "Global Shoonya stream request limiter unavailable, using local", {"error": str(e)}
                )
        self._wait_for_market_data_rate_limit_local(self._stream_request_rate_limit_interval_secs)

    @retry_on_error(max_retries=3, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(
        symbols=lambda x: x is not None and (isinstance(x, str) or isinstance(x, dict) or isinstance(x, pd.DataFrame)),
        date_start=lambda x: _validate_datetime_input(x),
        date_end=lambda x: _validate_datetime_input(x),
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        periodicity=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: Union[str, dt.datetime, dt.date],
        date_end: Union[str, dt.datetime, dt.date] = get_tradingapi_now().strftime("%Y-%m-%d"),
        exchange="NSE",
        periodicity="1m",
        market_open_time="09:15:00",
        market_close_time="15:30:00",
        refresh_mapping: bool = False,
    ) -> Dict[str, List[HistoricalData]]:
        """
        Retrieves historical bars from 5paisa.

        Args:
            symbols (Union[str,dict,pd.DataFrame]): If dataframe is provided, it needs to contain columns [long_symbol, Scripcode].
                If dict is provided, it needs to contain (long_symbol, scrip_code, exch, exch_type). Else symbol long_name.
            date_start (str): Date formatted as YYYY-MM-DD.
            date_end (str): Date formatted as YYYY-MM-DD.
            periodicity (str): Defaults to '1m'.
            market_close_time (str): Defaults to '15:30:00'. Only historical data with timestamp less than market_close_time is returned.
            refresh_mapping: If True, load symbol mapping from date_end's symbols CSV file instead of using cached mapping.
                Defaults to False.

        Returns:
            Dict[str, List[HistoricalData]]: Dictionary with historical data for each symbol.
        """
        try:
            if self.api is None:
                raise BrokerConnectionError("Shoonya API is not initialized")

            trading_logger.log_debug(
                "Getting historical data",
                {
                    "symbols": str(symbols) if isinstance(symbols, (str, dict)) else f"DataFrame({len(symbols)} rows)",
                    "date_start": date_start,
                    "date_end": date_end,
                    "exchange": exchange,
                    "periodicity": periodicity,
                    "refresh_mapping": refresh_mapping,
                },
            )
            timezone = pytz.timezone("Asia/Kolkata")

            def extract_number(s: str) -> int:
                # Search for digits in the string
                match = re.search(r"\d+", s)
                # Convert to integer if match is found, else return None
                return int(match.group()) if match else 1  # default return 1 if no number found

            # Load symbol mapping from file if refresh_mapping is True
            refresh_symbol_map = None
            refresh_exchangetype_map = None
            if refresh_mapping:
                try:
                    # Parse date_end to get YYYYMMDD format
                    try:
                        date_end_dt = parse_datetime(date_end)
                        date_end_str = date_end_dt.strftime("%Y-%m-%d")
                        date_end_valid = True
                    except (ValueError, TypeError):
                        date_end_valid = False
                    if not date_end_valid:
                        raise ValueError(f"Invalid date_end format: {date_end}")
                    date_end_obj = dt.datetime.strptime(str(date_end_str), "%Y-%m-%d")
                    date_end_yyyymmdd = date_end_obj.strftime("%Y%m%d")

                    # Get symbol codes path from config
                    symbol_codes_path = config.get(f"{self.account_key}.SYMBOLCODES")
                    if not symbol_codes_path:
                        context = create_error_context(broker_name=self.broker.name)
                        raise ConfigurationError(
                            f"SYMBOLCODES path not found in config for {self.broker.name}", context
                        )

                    # Construct path to symbols file
                    symbols_file_path = os.path.join(symbol_codes_path, f"{date_end_yyyymmdd}_symbols.csv")

                    trading_logger.log_debug(
                        "Loading symbol mapping from file",
                        {"symbols_file_path": symbols_file_path, "date": date_end_yyyymmdd},
                    )

                    if not os.path.exists(symbols_file_path):
                        trading_logger.log_warning(
                            "Symbols file not found for refresh_mapping", {"symbols_file_path": symbols_file_path}
                        )
                        # Fall back to default mapping
                        refresh_symbol_map = None
                    else:
                        # Load the CSV file
                        codes = pd.read_csv(symbols_file_path)

                        # Create symbol_map and exchangetype_map for each exchange
                        refresh_symbol_map = {}
                        refresh_exchangetype_map = {}
                        for exch, group in codes.groupby("Exch"):
                            refresh_symbol_map[exch] = dict(zip(group["long_symbol"], group["Scripcode"]))
                            refresh_exchangetype_map[exch] = dict(zip(group["long_symbol"], group["ExchType"]))

                        trading_logger.log_debug(
                            "Symbol mapping loaded from file",
                            {"exchanges": list(refresh_symbol_map.keys()), "total_symbols": len(codes)},
                        )
                except Exception as e:
                    trading_logger.log_warning(
                        "Error loading symbol mapping from file, falling back to default",
                        {"error": str(e)},
                    )
                    refresh_symbol_map = None
                    refresh_exchangetype_map = None

            scripCode = None
            # Determine the format of symbols and create a DataFrame
            if isinstance(symbols, str):
                exchange = self.map_exchange_for_api(symbols, exchange)
                # Use refresh_symbol_map if available, otherwise use default mapping
                if refresh_symbol_map and exchange in refresh_symbol_map:
                    scripCode = refresh_symbol_map[exchange].get(symbols)
                else:
                    scripCode = self.exchange_mappings[exchange]["symbol_map"].get(symbols)
                if scripCode:
                    symbols_pd = pd.DataFrame([{"long_symbol": symbols, "Scripcode": scripCode}])
                else:
                    trading_logger.log_error(
                        "Did not get ScripCode for symbols", context={"symbols": symbols, "symbol_type": "string"}
                    )
                    return {}
            elif isinstance(symbols, dict):
                scripCode = symbols.get("scrip_code")
                if scripCode:
                    symbols_pd = pd.DataFrame([{"long_symbol": symbols.get("long_symbol"), "Scripcode": scripCode}])
                else:
                    trading_logger.log_error(
                        "Did not get ScripCode for symbols", context={"symbols": symbols, "symbol_type": "dict"}
                    )
                    return {}
            else:
                symbols_pd = symbols

            out = {}  # Initialize the output dictionary

            def _get_time_price_series_with_retry(
                exch: str,
                token: str,
                start_time: float,
                end_time: float,
                interval: int,
                symbol: str,
            ) -> Optional[List]:
                max_attempts = 3
                for attempt in range(1, max_attempts + 1):
                    if self.api is None:
                        raise BrokerConnectionError("API client not initialized")
                    try:
                        self._wait_for_historical_rate_limit()
                        return self.api.get_time_price_series(
                            exchange=exch,
                            token=token,
                            starttime=start_time,
                            endtime=end_time,
                            interval=interval,
                        )
                    except json.JSONDecodeError as e:
                        trading_logger.log_warning(
                            "JSON decode error in get_time_price_series",
                            {
                                "long_symbol": symbol,
                                "exchange": exch,
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "error": str(e),
                            },
                        )
                        if attempt == max_attempts:
                            raise
                        try:
                            self.connect(redis_db=0, as_of_date=get_tradingapi_now())
                            trading_logger.log_info(
                                "Reconnected after JSON decode error",
                                {"long_symbol": symbol, "attempt": attempt},
                            )
                        except Exception as reconnect_error:
                            trading_logger.log_error(
                                "Reconnect failed after JSON decode error",
                                reconnect_error,
                                {"long_symbol": symbol, "attempt": attempt},
                            )
                        time.sleep(0.4 * attempt)
                return None

            for index, row_outer in symbols_pd.iterrows():
                long_symbol = row_outer["long_symbol"]
                trading_logger.log_debug(
                    "Processing historical data",
                    {"index": str(index), "total_symbols": str(len(symbols)), "long_symbol": long_symbol},
                )
                exchange = self.map_exchange_for_api(long_symbol, exchange)
                historical_data_list = []
                exch = exchange

                # Parse date_start (accepts datetime, date, or string) -> datetime for API
                try:
                    date_start_parsed = parse_datetime(date_start)
                    date_start_valid = True
                except (ValueError, TypeError):
                    date_start_valid = False
                if (
                    not date_start_valid
                    or date_start_parsed is None
                    or not isinstance(date_start_parsed, (dt.datetime, dt.date))
                ):
                    raise ValueError(f"Invalid date_start format: {date_start}")
                date_start_dt = dt.datetime.strptime(
                    date_start_parsed.strftime("%Y-%m-%d") + " " + market_open_time,
                    "%Y-%m-%d %H:%M:%S",
                )

                # Parse date_end (accepts datetime, date, or string) -> datetime for API
                try:
                    date_end_parsed = parse_datetime(date_end)
                    date_end_valid = True
                except (ValueError, TypeError):
                    date_end_valid = False
                if (
                    not date_end_valid
                    or date_end_parsed is None
                    or not isinstance(date_end_parsed, (dt.datetime, dt.date))
                ):
                    raise ValueError(f"Invalid date_end format: {date_end}")
                date_end_dt = dt.datetime.strptime(
                    date_end_parsed.strftime("%Y-%m-%d") + " " + market_close_time,
                    "%Y-%m-%d %H:%M:%S",
                )
                data: Optional[List] = None
                try:
                    if self.api is None:
                        raise BrokerConnectionError("API client not initialized")
                    if periodicity.endswith("m"):
                        data = _get_time_price_series_with_retry(
                            exch=exch,
                            token=str(row_outer["Scripcode"]),
                            start_time=date_start_dt.timestamp(),
                            end_time=date_end_dt.timestamp(),
                            interval=extract_number(periodicity),
                            symbol=long_symbol,
                        )
                    elif periodicity == "1d":
                        if row_outer["long_symbol"] == "NSENIFTY_IND___":
                            row_outer["long_symbol"] = "NIFTY_IND___"
                        trading_symbol = self.exchange_mappings[exchange]["tradingsymbol_map"].get(
                            row_outer["long_symbol"]
                        )

                        def _timeout_handler(signum, frame) -> None:
                            raise TimeoutError("daily_price_series call timed out")

                        signal.signal(signal.SIGALRM, _timeout_handler)  # Install the handler

                        attempts = 3
                        wait_seconds = 2

                        for attempt in range(attempts):
                            start_time = time.time()
                            signal.alarm(2)  # Trigger a timeout in 3 seconds
                            try:
                                self._wait_for_historical_rate_limit()
                                data = self.api.get_daily_price_series(
                                    exchange=exch,
                                    tradingsymbol=trading_symbol,
                                    startdate=date_start_dt.timestamp(),
                                    enddate=date_end_dt.timestamp(),
                                )
                                # If call succeeds, break out of loop
                                break
                            except Exception as e:
                                trading_logger.log_error(
                                    "Error in get_daily_price_series",
                                    e,
                                    {"long_symbol": row_outer["long_symbol"], "attempt": attempt + 1},
                                )
                                data = None

                            finally:
                                signal.alarm(0)  # Cancel the alarm

                            elapsed = time.time() - start_time
                            if elapsed < wait_seconds:
                                trading_logger.log_info(
                                    "Reattempting to get daily data", {"long_symbol": row_outer["long_symbol"]}
                                )
                                time.sleep(wait_seconds - elapsed)
                except Exception as e:
                    trading_logger.log_error(
                        "Error in get_time_price_series or get_daily_price_series",
                        e,
                        {"long_symbol": long_symbol, "periodicity": periodicity},
                    )
                    data = None

                # Process data if available
                if data is not None and isinstance(data, list):  # type: ignore[reportUnreachable]
                    if len(data) > 0:
                        market_open = pd.to_datetime(market_open_time).time()
                        market_close = pd.to_datetime(market_close_time).time()
                        for d in data:
                            if isinstance(d, str):
                                d = json.loads(d)
                            if periodicity.endswith("m"):
                                date = pd.Timestamp(
                                    timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%m-%Y %H:%M:%S"))
                                )
                                # Filter by market open/close time for intraday
                                if not (market_open <= date.time() < market_close):
                                    continue
                            elif periodicity == "1d":
                                date = pd.Timestamp(timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%b-%Y")))
                            historical_data = HistoricalData(
                                date=date,
                                open=float(d.get("into", "nan")),
                                high=float(d.get("inth", "nan")),
                                low=float(d.get("intl", "nan")),
                                close=float(d.get("intc", "nan")),
                                volume=int(float((d.get("intv", 0)))),
                                intoi=int(float(d.get("intoi", 0))),
                                oi=int(float(d.get("oi", 0))),
                            )
                            historical_data_list.append(historical_data)
                else:
                    # data is None or empty list
                    trading_logger.log_debug("No data found for symbol", {"long_symbol": long_symbol})
                    historical_data_list.append(
                        HistoricalData(
                            date=dt.datetime(1970, 1, 1),
                            open=float("nan"),
                            high=float("nan"),
                            low=float("nan"),
                            close=float("nan"),
                            volume=0,
                            intoi=0,
                            oi=0,
                        )
                    )
                if periodicity == "1d" and date_end_dt.date() == get_tradingapi_now().date():
                    # make a call to permin data for start date and end date of today
                    if historical_data_list:
                        last_date = historical_data_list[0].date
                        if last_date is not None:
                            today_start = last_date + dt.timedelta(days=1)
                        else:
                            today_start = get_tradingapi_now().date()
                        today_start = dt.datetime.combine(today_start, dt.datetime.min.time())
                    else:
                        today_start = dt.datetime.combine(dt.datetime.today(), dt.datetime.min.time())
                    try:
                        self._wait_for_historical_rate_limit()
                        intraday_data = self.api.get_time_price_series(
                            exchange=exch,
                            token=str(row_outer["Scripcode"]),
                            starttime=today_start.timestamp(),
                            interval=1,  # Request 1-minute data
                        )
                    except Exception as e:
                        trading_logger.log_error(
                            "Error in get_time_price_series for intraday data",
                            e,
                            {"long_symbol": long_symbol},
                        )
                        intraday_data = None

                    if intraday_data:
                        df_intraday = pd.DataFrame(intraday_data)
                        df_intraday["time"] = pd.to_datetime(df_intraday["time"], format="%d-%m-%Y %H:%M:%S")
                        df_intraday.set_index("time", inplace=True)
                        df_intraday[["into", "inth", "intl", "intc", "intv", "intoi", "oi"]] = df_intraday[
                            ["into", "inth", "intl", "intc", "intv", "intoi", "oi"]
                        ].apply(pd.to_numeric, errors="coerce")
                        df_intraday = (
                            df_intraday.resample("D")
                            .agg(
                                {
                                    "into": "first",
                                    "inth": "max",
                                    "intl": "min",
                                    "intc": "last",
                                    "intv": "sum",
                                    "intoi": "sum",
                                    "oi": "sum",
                                }
                            )
                            .dropna()
                        )
                        date_start = timezone.localize(date_start_dt)
                        date_end = timezone.localize(date_end_dt)
                        for _, row in df_intraday.iterrows():
                            date = pd.Timestamp(row.name).tz_localize(timezone)  # type: ignore
                            if date_start <= date <= date_end:
                                historical_data = HistoricalData(
                                    date=date,
                                    open=row["into"],
                                    high=row["inth"],
                                    low=row["intl"],
                                    close=row["intc"],
                                    volume=row["intv"],
                                    intoi=row["intoi"],
                                    oi=row["oi"],
                                )
                                historical_data_list.append(historical_data)
                # Remap NIFTY_ to NSENIFTY_ for output key only (legacy: permin data to database)
                s = long_symbol.replace("/", "-")
                output_key = "NSENIFTY" + s[s.find("_") :] if s.startswith("NIFTY_") else long_symbol
                out[output_key] = historical_data_list

            return out
        except Exception as e:
            trading_logger.log_error(
                "Unexpected error in get_historical",
                e,
                {
                    "symbols": str(symbols) if isinstance(symbols, (str, dict)) else f"DataFrame({len(symbols)} rows)",
                    "date_start": date_start,
                    "date_end": date_end,
                    "exchange": exchange,
                },
            )
            return {}

    def map_exchange_for_api(self, long_symbol, exchange) -> str:
        """
        Map the exchange for API based on the long symbol and exchange.

        Args:
            long_symbol (str): The symbol string containing details like "_OPT_", "_FUT_".
            exchange (str): The original exchange identifier ("N", "B", or others).

        Returns:
            str: Mapped exchange for API.
        """
        try:
            trading_logger.log_debug("Mapping exchange for API", {"long_symbol": long_symbol, "exchange": exchange})

            if not exchange or len(exchange) == 0:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange)
                raise ValidationError("Exchange cannot be empty", context)

            exchange_map = {
                "N": "NFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "NSE",
                "B": "BFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "BSE",
                "NSE": "NFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "NSE",
                "BSE": "BFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "BSE",
            }

            # Return mapped exchange if "N" or "B", otherwise default to the given exchange
            result = cast(str, exchange_map.get(exchange, exchange))

            trading_logger.log_debug(
                "Exchange mapped for API",
                {"long_symbol": long_symbol, "original_exchange": exchange, "mapped_exchange": result},
            )

            return result

        except (ValidationError, IndexError) as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise ValidationError(f"Error mapping exchange for API: {str(e)}", context)
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise ValidationError(f"Unexpected error mapping exchange for API: {str(e)}", context)

    def map_exchange_for_db(self, long_symbol, exchange) -> str:
        """
        Map the exchange for the database based on the exchange's starting letter.

        Args:
            long_symbol (str): The symbol string (not used in this mapping but kept for consistency).
            exchange (str): The original exchange identifier.

        Returns:
            str: Mapped exchange ("NSE", "BSE", or the original exchange).
        """
        try:
            trading_logger.log_debug("Mapping exchange for DB", {"long_symbol": long_symbol, "exchange": exchange})

            if not exchange or len(exchange) == 0:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange)
                raise ValidationError("Exchange cannot be empty", context)

            if exchange.startswith("N"):
                result = "NSE"
            elif exchange.startswith("B"):
                result = "BSE"
            else:
                result = exchange

            trading_logger.log_debug(
                "Exchange mapped for DB",
                {"long_symbol": long_symbol, "original_exchange": exchange, "mapped_exchange": result},
            )

            return result

        except (ValidationError, IndexError) as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise ValidationError(f"Error mapping exchange for DB: {str(e)}", context)
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise ValidationError(f"Unexpected error mapping exchange for DB: {str(e)}", context)

    def convert_ft_to_ist(self, ft: int) -> str:
        """
        Convert a timestamp to IST date and time.

        Args:
            ft: Timestamp in seconds since epoch

        Returns:
            str: The corresponding date and time in IST (yyyy-mm-dd hh:mm:ss).
        """
        try:
            trading_logger.log_debug("Converting timestamp to IST", {"timestamp": ft})

            if ft == 0:
                result = get_tradingapi_now().strftime("%Y-%m-%d %H:%M:%S")
                trading_logger.log_debug("Using current time for zero timestamp")
                return result

            utc_time = dt.datetime.fromtimestamp(ft, tz=dt.timezone.utc)
            # Add 5 hours and 30 minutes to get IST
            ist_time = utc_time + dt.timedelta(hours=5, minutes=30)

            # Format the datetime
            formatted_time = ist_time.strftime("%Y-%m-%d %H:%M:%S")
            return formatted_time

        except Exception as e:
            trading_logger.log_warning("Error converting timestamp to IST", {"timestamp": ft, "error": str(e)})
            return get_tradingapi_now().strftime("%Y-%m-%d %H:%M:%S")

    @retry_on_error(max_retries=3, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """Get Quote details of a symbol.

        Args:
            long_symbol (str): Long symbol.
            exchange (str): Exchange name. Defaults to "NSE".

        Returns:
            Price: Quote details.
        """
        try:
            if self.api is None:
                raise BrokerConnectionError("Shoonya API is not initialized")
            trading_logger.log_debug("Fetching quote", {"long_symbol": long_symbol, "exchange": exchange})
            mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
            market_feed = Price()  # Initialize with default values
            market_feed.src = "sh"
            market_feed.symbol = long_symbol

            # Validate exchange mapping exists
            if mapped_exchange not in self.exchange_mappings:
                trading_logger.log_error(
                    "Exchange mapping not found",
                    context={
                        "mapped_exchange": mapped_exchange,
                        "available_exchanges": list(self.exchange_mappings.keys()),
                    },
                )
                return market_feed

            token = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
            if token is None:
                trading_logger.log_error(
                    "No token found for symbol",
                    context={"long_symbol": long_symbol, "mapped_exchange": mapped_exchange},
                )
                return market_feed  # Return default Price object if no token is found

            try:
                self._wait_for_quote_rate_limit()
                tick_data = self.api.get_quotes(exchange=mapped_exchange, token=str(token))

                if not tick_data:
                    trading_logger.log_warning(
                        "Empty tick data received",
                        {"long_symbol": long_symbol, "exchange": mapped_exchange, "token": token},
                    )
                    return market_feed

                # Safely extract and convert values with validation
                def safe_float(value, default=float("nan")) -> float:
                    """Safely convert value to float with validation."""
                    if value in [None, 0, "0", "0.00", float("nan"), ""]:
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default

                def safe_int(value, default=0) -> int:
                    """Safely convert value to int with validation."""
                    if value in [None, 0, "0", float("nan"), ""]:
                        return default
                    try:
                        return int(float(value))
                    except (ValueError, TypeError):
                        return default

                market_feed.bid = safe_float(tick_data.get("bp1"))
                market_feed.ask = safe_float(tick_data.get("sp1"))
                market_feed.bid_volume = safe_int(tick_data.get("bq1"))
                market_feed.ask_volume = safe_int(tick_data.get("sq1"))
                market_feed.prior_close = safe_float(tick_data.get("c"))
                market_feed.last = safe_float(tick_data.get("lp"))
                market_feed.high = safe_float(tick_data.get("h"))
                market_feed.low = safe_float(tick_data.get("l"))
                market_feed.volume = safe_int(tick_data.get("v"))

                # Handle exchange mapping
                try:
                    market_feed.exchange = self.map_exchange_for_db(long_symbol, tick_data.get("exch"))
                except Exception as e:
                    trading_logger.log_error(
                        "Failed to map exchange for DB", e, {"long_symbol": long_symbol, "exch": tick_data.get("exch")}
                    )
                    market_feed.exchange = mapped_exchange

                # Handle timestamp conversion
                try:
                    lut_value = tick_data.get("lut", 0)
                    if lut_value:
                        market_feed.timestamp = self.convert_ft_to_ist(int(lut_value))
                    else:
                        market_feed.timestamp = dt.datetime.now()
                except Exception as e:
                    trading_logger.log_error(
                        "Failed to convert timestamp", e, {"long_symbol": long_symbol, "lut": tick_data.get("lut")}
                    )
                    market_feed.timestamp = dt.datetime.now()

                trading_logger.log_debug(
                    "Quote fetched successfully",
                    {
                        "long_symbol": long_symbol,
                        "exchange": mapped_exchange,
                        "bid": market_feed.bid,
                        "ask": market_feed.ask,
                        "last": market_feed.last,
                        "volume": market_feed.volume,
                    },
                )

            except Exception as e:
                trading_logger.log_error(
                    "Error fetching quote for symbol",
                    e,
                    {"long_symbol": long_symbol, "exchange": mapped_exchange, "token": token},
                )

        except Exception as e:
            trading_logger.log_error(
                "Unexpected error in get_quote", e, {"long_symbol": long_symbol, "exchange": exchange}
            )

        return market_feed

    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(
        operation=lambda x: isinstance(x, str) and x in ["s", "u"],
        symbols=lambda x: isinstance(x, list) and len(x) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def start_quotes_streaming(self, operation: str, symbols: List[str], ext_callback=None, exchange="NSE") -> None:
        """
        Start streaming quotes for the given symbols.

        Args:
            operation (str): 's' for subscribe, 'u' for unsubscribe.
            symbols (List[str]): List of symbols to subscribe/unsubscribe.
            ext_callback (function): External callback function for processing price updates.
            exchange (str): Exchange name (default: 'NSE').
        """
        if self.api is None:
            raise BrokerConnectionError("Shoonya API is not initialized")

        try:
            trading_logger.log_info(
                "Starting quotes streaming",
                {"operation": operation, "symbols_count": len(symbols), "exchange": exchange},
            )

            if not symbols or len(symbols) == 0:
                trading_logger.log_error(
                    "Symbols list cannot be empty", context={"operation": operation, "exchange": exchange}
                )
                return

            prices: Dict[str, Price] = {}
            mapped_exchange = self.map_exchange_for_api(symbols[0], exchange)

            # Function to map JSON data to a Price object
            def map_to_price(json_data) -> Price:
                price = Price()
                price.src = "sh"
                price.bid = (
                    float("nan")
                    if json_data.get("bp1") in [None, 0, "0", "0.00", float("nan")]
                    else float(json_data.get("bp1"))
                )
                price.ask = (
                    float("nan")
                    if json_data.get("sp1") in [None, 0, "0", "0.00", float("nan")]
                    else float(json_data.get("sp1"))
                )
                price.bid_volume = (
                    float("nan")
                    if json_data.get("bq1") in [None, 0, "0", float("nan")]
                    else float(json_data.get("bq1"))
                )
                price.ask_volume = (
                    float("nan")
                    if json_data.get("sq1") in [None, 0, "0", float("nan")]
                    else float(json_data.get("sq1"))
                )
                price.prior_close = (
                    float("nan")
                    if json_data.get("c") in [None, 0, "0", "0,00", float("nan")]
                    else float(json_data.get("c"))
                )
                price.last = (
                    float("nan")
                    if json_data.get("lp") in [None, 0, "0", "0.00", float("nan")]
                    else float(json_data.get("lp"))
                )
                price.high = (
                    float("nan")
                    if json_data.get("h") in [None, 0, "0", "0.00", float("nan")]
                    else float(json_data.get("h"))
                )
                price.low = (
                    float("nan")
                    if json_data.get("l") in [None, 0, "0", "0.00", float("nan")]
                    else float(json_data.get("l"))
                )
                price.volume = float("nan") if json_data.get("v") in [None, float("nan")] else float(json_data.get("v"))
                symbol = self.exchange_mappings[json_data.get("e")]["symbol_map_reversed"].get(int(json_data.get("tk")))
                price.exchange = self.map_exchange_for_db(symbol, json_data.get("e"))
                price.timestamp = self.convert_ft_to_ist(int(json_data.get("ft", 0)))
                price.symbol = symbol
                return price

            # Function to handle incoming WebSocket messages
            def on_message(message) -> None:
                if message.get("t") == "tk":
                    price = map_to_price(message)
                    prices[message.get("tk")] = price
                    if ext_callback is not None:
                        try:
                            ext_callback(price)
                        except Exception as e:
                            trading_logger.log_error(
                                "Error in external price callback",
                                e,
                                {"symbol": price.symbol, "src": price.src},
                            )
                elif message.get("t") == "tf":
                    required_keys = {"bp1", "sp1", "c", "lp", "bq1", "sq1", "h", "l"}
                    if required_keys & message.keys():
                        price = prices.get(message.get("tk"))
                        if price is not None:
                            if message.get("bp1"):
                                price.bid = float(message.get("bp1"))
                            if message.get("sp1"):
                                price.ask = float(message.get("sp1"))
                            if message.get("bq1"):
                                price.bid_volume = float(message.get("bq1"))
                            if message.get("sq1"):
                                price.ask_volume = float(message.get("sq1"))
                            if message.get("c"):
                                price.prior_close = float(message.get("c"))
                            if message.get("lp"):
                                price.last = float(message.get("lp"))
                            if message.get("h"):
                                price.high = float(message.get("h"))
                            if message.get("l"):
                                price.low = float(message.get("l"))
                            if message.get("v"):
                                price.volume = float(message.get("v"))
                            price.timestamp = self.convert_ft_to_ist(int(message.get("ft", 0)))
                            prices[message.get("tk")] = price
                            if ext_callback is not None:
                                try:
                                    ext_callback(price)
                                except Exception as e:
                                    trading_logger.log_error(
                                        "Error in external price callback",
                                        e,
                                        {"symbol": price.symbol, "src": price.src},
                                    )

            # Function to handle WebSocket errors
            def handle_socket_error(error=None) -> None:
                if error:
                    trading_logger.log_error("WebSocket error", context={"error": str(error)})
                else:
                    trading_logger.log_error("WebSocket error. Connection to remote host was lost.")

            def handle_socket_close(close_code=None, close_msg=None) -> None:
                if close_msg:
                    trading_logger.log_error("WebSocket closed", context={"close_msg": str(close_msg)})
                    initiate_reconnect()

            def initiate_reconnect(max_retries=5, retry_delay=5) -> None:
                """
                Attempt to reconnect the WebSocket connection.
                """
                for attempt in range(max_retries):
                    try:
                        trading_logger.log_info(
                            "Reconnect attempt", {"attempt": attempt + 1, "max_retries": max_retries}
                        )

                        # Close the existing WebSocket connection if open
                        if hasattr(self, "api") and self.api and hasattr(self, "socket_opened") and self.socket_opened:
                            try:
                                self.api.close_websocket()
                                self.socket_opened = False
                                trading_logger.log_info("Closed existing WebSocket connection")
                            except Exception as e:
                                trading_logger.log_warning(
                                    "Failed to close existing WebSocket", context={"error": str(e)}
                                )

                        # Ensure API is initialized before attempting reconnection
                        if self.api is None:
                            trading_logger.log_error("Cannot reconnect: Shoonya API is not initialized")
                            continue

                        # Reinitialize the WebSocket connection
                        connect_and_subscribe()

                        # Wait for the WebSocket to open with timeout
                        timeout = 10  # seconds
                        start_time = time.time()
                        while not self.socket_opened and (time.time() - start_time) < timeout:
                            time.sleep(0.5)  # Check more frequently

                        if self.socket_opened:
                            trading_logger.log_info("WebSocket reconnected successfully.")
                            try:
                                # Rebuild subscription list from all currently subscribed symbols
                                active_symbols = (
                                    list(self.subscribed_symbols) if hasattr(self, "subscribed_symbols") else []
                                )
                                if active_symbols:
                                    reconnect_req_list = expand_symbols_to_request(active_symbols)
                                    trading_logger.log_info(
                                        "Resubscribing to symbols after reconnection",
                                        {
                                            "symbols_count": len(active_symbols),
                                            "req_list": reconnect_req_list,
                                        },
                                    )
                                    self._wait_for_stream_request_rate_limit()
                                    self.api.subscribe(reconnect_req_list)
                                else:
                                    trading_logger.log_warning(
                                        "No active symbols found in subscribed_symbols during reconnection; "
                                        "skipping resubscribe."
                                    )
                                return
                            except Exception as e:
                                trading_logger.log_error("Failed to resubscribe after reconnection", e)
                        else:
                            trading_logger.log_warning("WebSocket did not open within the expected time.")

                    except Exception as e:
                        trading_logger.log_error("Reconnect attempt failed", e, {"attempt": attempt + 1})

                        # Wait before the next retry with exponential backoff
                        wait_time = retry_delay * (2**attempt)
                        trading_logger.log_info(
                            "Waiting before next retry", {"wait_time": wait_time, "attempt": attempt + 1}
                        )
                        time.sleep(wait_time)

                trading_logger.log_error("Max reconnect attempts reached. Unable to reconnect the WebSocket.")
                # Set a flag to indicate connection failure
                if hasattr(self, "socket_opened"):
                    self.socket_opened = False

            # Function to handle WebSocket connection opening
            def on_socket_open() -> None:
                trading_logger.log_info("WebSocket connection opened")
                self.socket_opened = True

            # Function to establish WebSocket connection and subscribe
            def connect_and_subscribe() -> None:
                if self.api is None:
                    trading_logger.log_error("Cannot start websocket: Shoonya API is not initialized")
                    return

                self.api.start_websocket(
                    subscribe_callback=on_message,
                    socket_close_callback=handle_socket_close,
                    socket_error_callback=handle_socket_error,
                    socket_open_callback=on_socket_open,
                )
                while not self.socket_opened:
                    time.sleep(1)

            def resolve_exchange_from_symbology(long_symbol: str):
                """Resolve API exchange for a symbol from symbology (which exchange's symbol_map contains it)."""
                for exch in self.exchange_mappings:
                    if long_symbol in self.exchange_mappings[exch]["symbol_map"]:
                        return exch
                return None

            # Function to expand symbols into request format
            def expand_symbols_to_request(symbol_list) -> List[str]:
                """Uses symbology to resolve exchange per symbol so reconnect works for mixed NSE/BSE symbols.
                Combo symbols (format SYM1?qty1:SYM2?qty2) are expanded into their legs; each leg is looked up."""
                req_list = []
                added = set()  # "exch|scrip_code" already added, to avoid duplicate subscriptions

                def add_symbol(long_symbol: str) -> None:
                    exch_for_symbol = resolve_exchange_from_symbology(long_symbol)
                    if exch_for_symbol is None:
                        exch_for_symbol = mapped_exchange
                    scrip_code = self.exchange_mappings[exch_for_symbol]["symbol_map"].get(long_symbol)
                    if scrip_code:
                        token = f"{exch_for_symbol}|{scrip_code}"
                        if token not in added:
                            added.add(token)
                            req_list.append(token)
                    else:
                        trading_logger.log_error(
                            "Did not find scrip_code for symbol", context={"symbol": long_symbol}
                        )

                for symbol in symbol_list:
                    if "?" in symbol:
                        try:
                            legs = parse_combo_symbol(symbol)
                            for leg_symbol in legs:
                                add_symbol(leg_symbol)
                        except SymbolError:
                            trading_logger.log_error(
                                "Did not find scrip_code for symbol", context={"symbol": symbol}
                            )
                    else:
                        # Single symbol; strip trailing ?qty if present (e.g. NIFTY_OPT_..._PUT_25700?65)
                        long_symbol = symbol.split("?", 1)[0].strip() if "?" in symbol else symbol
                        add_symbol(long_symbol)
                return req_list

            # Function to update the subscription list
            def update_subscription_list(operation, symbols) -> None:
                if operation == "s":
                    self.subscribed_symbols = list(set(self.subscribed_symbols + symbols))
                elif operation == "u":
                    self.subscribed_symbols = list(set(self.subscribed_symbols) - set(symbols))

            # Update subscriptions and request list
            update_subscription_list(operation, symbols)
            req_list = expand_symbols_to_request(symbols)

            # Start the WebSocket connection if not already started
            if self.subscribe_thread is None:
                self.subscribe_thread = threading.Thread(target=connect_and_subscribe, name="MarketDataStreamer")
                self.subscribe_thread.start()

            # Wait until the socket is opened before subscribing/unsubscribing
            while not self.socket_opened:
                time.sleep(1)

                # Manage subscription based on operation
                if req_list:
                    if operation == "s":
                        trading_logger.log_info("Requesting streaming", {"req_list": req_list})
                        self._wait_for_stream_request_rate_limit()
                        self.api.subscribe(req_list)
                    elif operation == "u":
                        trading_logger.log_info("Unsubscribing streaming", {"req_list": req_list})
                        self.api.unsubscribe(req_list)
        except Exception as e:
            trading_logger.log_error(
                "Unexpected error in start_quotes_streaming",
                e,
                {"operation": operation, "symbols_count": len(symbols) if symbols else 0, "exchange": exchange},
            )

    @log_execution_time
    @validate_inputs(long_symbol=lambda x: x is None or (isinstance(x, str) and len(x.strip()) >= 0))
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_position(self, long_symbol: str = "") -> Union[pd.DataFrame, int]:
        if self.api is None:
            raise BrokerConnectionError("Shoonya API is not initialized")

        try:
            trading_logger.log_debug("Getting position", {"long_symbol": long_symbol if long_symbol else "all"})

            # Get holdings (delivery positions) if API supports it
            holding = pd.DataFrame(columns=["long_symbol", "quantity"]).astype({"quantity": float})
            get_holdings_fn = getattr(self.api, "get_holdings", None)
            if callable(get_holdings_fn):
                try:
                    raw_holding = get_holdings_fn()
                    if isinstance(raw_holding, list):
                        holding = pd.DataFrame(raw_holding)
                    elif isinstance(raw_holding, dict):
                        holding = pd.DataFrame([raw_holding])
                    else:
                        holding = pd.DataFrame(columns=["long_symbol", "quantity"])
                    if len(holding) > 0:
                        try:
                            # Resolve long_symbol from mappings using Scripcode + Exchange
                            if "exch" not in holding.columns and "exch_tsym" not in holding.columns:
                                raise ValidationError("Holdings require 'exch' or 'exch_tsym' for mapping")

                            def _from_exch_tsym(x, key):
                                if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict):
                                    return x[0].get(key)
                                return None

                            exch_tsym_series = (
                                holding["exch_tsym"] if "exch_tsym" in holding.columns else pd.Series([None] * len(holding))
                            )
                            token_col = (
                                holding["token"]
                                if "token" in holding.columns
                                else exch_tsym_series.apply(lambda x: _from_exch_tsym(x, "token"))
                            )
                            exchange_col = (
                                holding["exch"]
                                if "exch" in holding.columns
                                else exch_tsym_series.apply(lambda x: _from_exch_tsym(x, "exch"))
                            )

                            if token_col.isnull().all():
                                raise ValidationError("Holdings require 'token' or 'exch_tsym[].token' for mapping")

                            holding["long_symbol"] = self.get_long_name_from_broker_identifier(
                                Scripcode=token_col,
                                Exchange=exchange_col,
                            )
                            holding["quantity"] = (
                                pd.to_numeric(holding["holdqty"], errors="coerce").fillna(0.0)
                                if "holdqty" in holding.columns
                                else 0.0
                            )
                            holding["quantity"] = holding["quantity"] + (
                                pd.to_numeric(holding["btstqty"], errors="coerce").fillna(0.0)
                                if "btstqty" in holding.columns
                                else 0.0
                            )
                            holding["quantity"] = holding["quantity"] + (
                                pd.to_numeric(holding["brkcolqty"], errors="coerce").fillna(0.0)
                                if "brkcolqty" in holding.columns
                                else 0.0
                            )
                            holding["quantity"] = holding["quantity"] + (
                                pd.to_numeric(holding["npoadqty"], errors="coerce").fillna(0.0)
                                if "npoadqty" in holding.columns
                                else 0.0
                            )
                            holding["quantity"] = pd.to_numeric(holding["quantity"], errors="coerce").fillna(0.0).astype(float)
                            holding = holding.loc[:, ["long_symbol", "quantity"]]
                            holding = holding[holding["long_symbol"].notna()]
                        except Exception as e:
                            trading_logger.log_error(
                                "Error processing holdings data", e, {"holdings_count": len(holding)}
                            )
                            holding = (
                                pd.DataFrame(columns=["long_symbol", "quantity"])
                                .astype({"quantity": float})
                                .astype({"quantity": float})
                            )
                except Exception as e:
                    trading_logger.log_debug("Holdings not available, using positions only", {"error": str(e)})
                    holding = pd.DataFrame(columns=["long_symbol", "quantity"]).astype({"quantity": float})

            # Get positions (intraday/F&O)
            position = pd.DataFrame(self.api.get_positions())
            if len(position) == 0:
                position = pd.DataFrame(columns=["long_symbol", "quantity"]).astype({"quantity": float})
            if len(position) > 0:
                try:
                    # Resolve long_symbol from mappings using Scripcode + Exchange (columns: token, exch)
                    position["long_symbol"] = self.get_long_name_from_broker_identifier(
                        Scripcode=position["token"],
                        Exchange=position["exch"],
                    )
                    position = position.loc[:, ["long_symbol", "netqty"]]
                    position.columns = ["long_symbol", "quantity"]
                    position["quantity"] = pd.to_numeric(position["quantity"], errors="coerce").fillna(0)
                    position = position[position["long_symbol"].notna()]
                except Exception as e:
                    trading_logger.log_error("Error processing positions data", e, {"positions_count": len(position)})
                    position = pd.DataFrame(columns=["long_symbol", "quantity"]).astype({"quantity": float})

            # Merge holdings and positions (outer), sum quantities
            merged = pd.merge(position, holding, on="long_symbol", how="outer")
            if len(merged) == 0:
                result = pd.DataFrame(columns=["long_symbol", "quantity"]).astype({"quantity": float})
                if long_symbol is None or long_symbol == "":
                    trading_logger.log_debug("Returning all positions", {"position_count": len(result)})
                    return result
                return 0
            # Ensure numeric before groupby (API may return quantity as str; sum on str causes concatenation error)
            for col in ["quantity_x", "quantity_y"]:
                if col in merged.columns:
                    merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).astype(float)
            result = merged.groupby("long_symbol", as_index=False).agg({"quantity_x": "sum", "quantity_y": "sum"})
            # Force numeric again after groupby (defensive for any dtype issues)
            qty_x = pd.to_numeric(result["quantity_x"], errors="coerce").fillna(0).astype(float)
            qty_y = pd.to_numeric(result["quantity_y"], errors="coerce").fillna(0).astype(float)
            result["quantity"] = qty_x + qty_y
            result = result.loc[:, ["long_symbol", "quantity"]]

            trading_logger.log_debug(
                "Position data processed",
                {"holdings_count": len(holding), "positions_count": len(position), "merged_count": len(result)},
            )

            if long_symbol is None or long_symbol == "":
                trading_logger.log_debug("Returning all positions", {"position_count": len(result)})
                return result
            else:
                pos_filtered = result.loc[result["long_symbol"] == long_symbol, "quantity"]
                if len(pos_filtered) == 0:
                    trading_logger.log_debug("No position found for symbol", {"long_symbol": long_symbol})
                    return 0
                elif len(pos_filtered) == 1:
                    position_value = pos_filtered.item()
                    trading_logger.log_debug(
                        "Position retrieved for symbol", {"long_symbol": long_symbol, "quantity": position_value}
                    )
                    return position_value
                else:
                    trading_logger.log_error(
                        "Multiple positions found for symbol",
                        context={"long_symbol": long_symbol, "position_count": len(pos_filtered)},
                    )
                    raise MarketDataError(f"Multiple positions found for symbol: {long_symbol}")

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, error=str(e))
            trading_logger.log_error("Unexpected error getting position", e, context)
            raise MarketDataError(f"Unexpected error getting position: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        """Get today's orders from Shoonya order book (token, exch).

        Args:
            all_columns (bool): If False (default), return a subset of columns with renamed
                and formatted fields (long_symbol, internal_order_id, order_time, side, broker_order_id, order_price, fill_price, etc.).
                If True, return as-is dump: all Noren columns plus long_symbol, no renaming
                or reordering.
        """
        try:
            all_columns = kwargs.get("all_columns", False)
            trading_logger.log_debug("Getting orders for today")
            if self.api is None:
                raise BrokerConnectionError("API client not initialized")
            raw = self.api.get_order_book()
            # Noren returns a list of dicts (each dict is an order)
            orders = pd.DataFrame(raw if isinstance(raw, list) else [])
            if len(orders) == 0:
                trading_logger.log_debug("No orders found for today")
                return orders
            try:
                # Resolve long_symbol from mappings (columns: token, exch)
                orders["long_symbol"] = self.get_long_name_from_broker_identifier(
                    Scripcode=orders["token"],
                    Exchange=orders["exch"],
                )
                if not all_columns:
                    # ordenttm: epoch (seconds or ms) -> human readable yyyy-mm-dd HH:MM:SS.ss (via chameli format_datetime)
                    def _format_ordenttm(ts):
                        try:
                            t = float(ts) if ts not in (None, "") else None
                            if t is None:
                                return ""
                            # Epoch: treat as seconds; if > 1e12 assume milliseconds
                            sec = t / 1000.0 if t > 1e12 else t
                            d = dt.datetime.fromtimestamp(sec)
                            return format_datetime(d, "%Y-%m-%d %H:%M:%S") + f".{d.microsecond // 1000:03d}"
                        except (TypeError, ValueError, OSError):
                            return "" if pd.isna(ts) else str(ts)

                    # Column may be ordenttm or different casing from Noren
                    ts_col = next((c for c in orders.columns if str(c).lower() == "ordenttm"), "ordenttm")
                    if ts_col in orders.columns:
                        orders["ordenttm"] = orders[ts_col].apply(_format_ordenttm)
                    else:
                        orders["ordenttm"] = ""
                    # trantype: B -> Buy, S -> Sell
                    trantype_map = {"B": "Buy", "S": "Sell"}
                    orders["trantype"] = orders["trantype"].map(trantype_map).fillna(orders["trantype"])
                    # order_price = prc (limit price; 0 for MKT). avgprc = fill_price (average fill price).
                    # Column order: ... filled, price_type, order_price, fill_price, status
                    keep = [
                        "long_symbol", "remarks", "ordenttm", "trantype", "norenordno", "exchordid", "qty", "fillshares",
                        "prctyp", "prc", "avgprc", "status",
                    ]
                    orders = orders.reindex(columns=[c for c in keep if c in orders.columns], copy=False)
                    orders = orders.rename(columns={
                        "norenordno": "broker_order_id",
                        "exchordid": "exchange_order_id",
                        "qty": "ordered",
                        "ordenttm": "order_time",
                        "trantype": "side",
                        "prc": "order_price",
                        "prctyp": "price_type",
                        "fillshares": "filled",
                        "avgprc": "fill_price",
                        "remarks": "internal_order_id",
                    })
                trading_logger.log_debug("Orders retrieved successfully", {"order_count": len(orders)})
                return orders
            except Exception as e:
                trading_logger.log_error("Error processing orders data", e, {"orders_count": len(orders)})
                raise MarketDataError(f"Error processing orders data: {str(e)}", create_error_context(error=str(e)))
        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            trading_logger.log_error("Unexpected error getting orders", e, context)
            raise MarketDataError(f"Unexpected error getting orders: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        try:
            trading_logger.log_debug("Getting trades for today")
            result = super().get_trades_today(**kwargs)
            if result is not None and len(result) > 0:
                trading_logger.log_debug("Trades retrieved successfully", {"trade_count": len(result)})
            else:
                trading_logger.log_debug("No trades found for today")
            return result
        except Exception as e:
            context = create_error_context(error=str(e))
            trading_logger.log_error("Unexpected error getting trades", e, context)
            raise

    @log_execution_time
    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """Resolve broker Scripcode + Exchange to long_symbol via mappings.

        Args:
            Scripcode (pd.Series): Broker scrip codes (e.g. position["token"]).
            Exchange (pd.Series): Exchange per row (e.g. position["exch"]).

        Returns:
            pd.Series: long_symbol per row. None where lookup fails.
        """
        try:
            scripcode = kwargs.get("Scripcode")
            exchange = kwargs.get("Exchange")

            if scripcode is None or exchange is None:
                context = create_error_context(
                    available_keys=list(kwargs.keys()),
                    message="Scripcode and Exchange are required",
                )
                raise ValidationError("Missing required arguments: Scripcode and Exchange", context)
            if not isinstance(scripcode, pd.Series) or not isinstance(exchange, pd.Series) or len(scripcode) == 0:
                context = create_error_context(
                    scripcode_type=type(scripcode),
                    exchange_type=type(exchange),
                    len_scripcode=len(scripcode) if hasattr(scripcode, "__len__") else None,
                )
                raise ValidationError("Scripcode and Exchange must be non-empty pandas Series", context)

            trading_logger.log_debug(
                "Resolving long name from mappings (Scripcode + Exchange)",
                {"row_count": len(scripcode)},
            )

            def lookup(scripcode_val, exchange_val):
                try:
                    exch_map = self.exchange_mappings.get(exchange_val, {})
                    rev = exch_map.get("symbol_map_reversed", {})
                    code = int(scripcode_val) if scripcode_val is not None else None
                    if code is None:
                        return None
                    return rev.get(code) or rev.get(scripcode_val)
                except (TypeError, ValueError, KeyError):
                    return None

            result = pd.Series(
                [lookup(scripcode.iloc[i], exchange.iloc[i]) for i in range(len(scripcode))],
                index=scripcode.index,
            )
            missing = result.isna().sum()
            if missing > 0:
                trading_logger.log_warning(
                    "Some rows had no mapping for Scripcode+Exchange",
                    {"missing_count": int(missing), "total": len(result)},
                )
            return result

        except (ValidationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(kwargs=kwargs, error=str(e))
            trading_logger.log_error("Unexpected error generating long name", e, context)
            raise DataError(f"Unexpected error generating long name: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def get_min_lot_size(self, long_symbol, exchange="NSE") -> Union[int, float]:
        try:
            trading_logger.log_debug("Getting minimum lot size", {"long_symbol": long_symbol, "exchange": exchange})

            try:
                exchange = self.map_exchange_for_api(long_symbol, exchange)
            except Exception as e:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
                raise SymbolError(f"Failed to map exchange: {str(e)}", context)

            try:
                code = self.exchange_mappings[exchange]["symbol_map"].get(long_symbol)
            except KeyError as e:
                context = create_error_context(
                    exchange=exchange, long_symbol=long_symbol, available_exchanges=list(self.exchange_mappings.keys())
                )
                raise SymbolError(f"Exchange mapping not found: {str(e)}", context)

            if code is not None:
                try:
                    lot_size_result = self.codes.loc[self.codes.Scripcode == code, "LotSize"]
                    # Handle both Series (multiple matches) and scalar (single match) cases
                    if isinstance(lot_size_result, pd.Series):
                        lot_size_raw = lot_size_result.iloc[0] if len(lot_size_result) > 0 else 1
                    else:
                        # Single match returns scalar
                        lot_size_raw = lot_size_result
                    # Handle potential complex numbers by taking real part
                    if isinstance(lot_size_raw, complex):
                        lot_size = float(lot_size_raw.real)
                    else:
                        lot_size = float(lot_size_raw)  # type: ignore[arg-type]
                    trading_logger.log_debug(
                        "Lot size retrieved successfully",
                        {"long_symbol": long_symbol, "exchange": exchange, "code": code, "lot_size": lot_size},
                    )
                    return lot_size
                except Exception as e:
                    trading_logger.log_warning(
                        "Error retrieving lot size from codes",
                        {"long_symbol": long_symbol, "exchange": exchange, "code": code, "error": str(e)},
                    )
                    return 0
            else:
                trading_logger.log_warning("Symbol code not found", {"long_symbol": long_symbol, "exchange": exchange})
                return 0

        except (ValidationError, SymbolError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            trading_logger.log_error("Unexpected error getting minimum lot size", e, context)
            raise SymbolError(f"Unexpected error getting minimum lot size: {str(e)}", context)
