import calendar
import datetime as dt
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
from typing import Dict, List, Union
from urllib.parse import parse_qs, urlparse
import hashlib
import httpx
import asyncio
import numpy as np
import pandas as pd
import pyotp
import pytz
import redis
import requests
from chameli.dateutils import valid_datetime, get_expiry
from NorenRestApiPy.NorenApi import NorenApi

from .broker_base import BrokerBase, Brokers, HistoricalData, Order, OrderInfo, OrderStatus, Price
from .config import get_config
from .utils import set_starting_internal_ids_int, update_order_status
from .globals import get_tradingapi_now
from . import trading_logger
from .error_handling import validate_inputs, log_execution_time, retry_on_error
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
    NetworkError,
    AuthenticationError,
    create_error_context,
)

# Set up logging
logger = logging.getLogger(__name__)


# Exception handler
def my_handler(typ, value, trace):
    trading_logger.log_error(
        "Unhandled exception",
        {
            "exception_type": typ.__name__,
            "exception_value": str(value),
            "traceback": "".join(traceback.format_tb(trace)),
        },
    )


sys.excepthook = my_handler
config = get_config()


class FlatTradeApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(
            self,
            host="https://piconnect.flattrade.in/PiConnectTP/",
            websocket="wss://piconnect.flattrade.in/PiConnectWSTp/",
        )


@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = True):
    def merge_without_last(lst):
        if len(lst) > 1:
            return "-".join(lst[:-1])
        else:
            return lst[0]

    bhavcopyfolder = config.get("bhavcopy_folder")
    url = "https://api.shoonya.com/NSE_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_flattradecodes_nse_cash.zip"
    response = requests.get(url, allow_redirects=True, timeout=10)
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

                def process_row(row):
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
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_flattradecodes_bse_cash.zip"
    response = requests.get(url, allow_redirects=True, timeout=10)
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

                def process_row(row):
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
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_flattradecodes_fno.zip"
    response = requests.get(url, allow_redirects=True, timeout=10)
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

                def process_row(row):
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
                codes_nse_fno = codes_fno[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize"]]

    url = "https://api.shoonya.com/BFO_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_flattradecodes_bse_fno.zip"
    response = requests.get(url, allow_redirects=True, timeout=10)
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
                codes_fno["Symbol"] = codes_fno["TradingSymbol"].str.extract(
                    r"^([A-Z]+(?:50)?)(?=\d{2}(?:[A-Z]\d+|\d+)[A-Z]{2})"
                )

                def process_row(row):
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
        dest_symbol_file = f"{config.get('FLATTRADE.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
        # Create the folder if it does not exist
        dest_folder = os.path.dirname(dest_symbol_file)
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder, exist_ok=True)
        codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]].to_csv(
            dest_symbol_file, index=False
        )
    return codes


class FlatTrade(BrokerBase):
    def __init__(self, **kwargs):
        """
        mandatory_keys = None

        """
        super().__init__()
        self.broker = Brokers.FLATTRADE
        self.codes = pd.DataFrame()
        self.api = None
        self.subscribe_thread = None
        self.subscribed_symbols = []
        self.socket_opened = False

    def _get_adjusted_expiry_date(self, year, month):
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
        def reverse_split_fno(long_name, exchange):
            if exchange in ["NSE", "NFO"]:
                parts = long_name.split("_")
                part1 = parts[0]
                part2 = dt.datetime.strptime(parts[2], "%Y%m%d").strftime("%d%b%y")
                part3 = parts[3][0] if parts[1].startswith("OPT") else "FUT"  # Check if it's an option or future
                part4 = parts[4]
                return f"{part1}{part2}{part3}{part4}"
            elif exchange in ["BSE", "BFO"]:
                trading_symbol = self.exchange_mappings[exchange]["tradingsymbol_map"].get(long_name)
                if trading_symbol is not None:
                    return trading_symbol
                else:
                    return pd.NA

        def reverse_split_cash(long_name, exchange):
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
                return pd.NA

        if "FUT" in long_name or "OPT" in long_name:
            return reverse_split_fno(long_name, exchange).upper()
        else:
            return reverse_split_cash(long_name, exchange).upper()

    @log_execution_time
    @validate_inputs(redis_db=lambda x: isinstance(x, int) and x >= 0)
    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    def connect(self, redis_db: int):
        """
        Connect to FlatTrade trading platform with enhanced session management.

        Args:
            redis_db: Redis database number

        Raises:
            ValidationError: If redis_db is invalid
            BrokerConnectionError: If connection fails
            AuthenticationError: If authentication fails
        """

        def extract_credentials():
            """Extract credentials from config with validation."""
            try:
                credentials = {
                    "user": config.get(f"{self.broker.name}.USER"),
                    "pwd": config.get(f"{self.broker.name}.PWD"),
                    "api_key": config.get(f"{self.broker.name}.APIKEY"),
                    "api_secret": config.get(f"{self.broker.name}.SECRETKEY"),
                    "token": config.get(f"{self.broker.name}.TOKEN"),
                }

                missing_keys = [key for key, value in credentials.items() if not value]
                if missing_keys:
                    context = create_error_context(missing_keys=missing_keys, available_keys=list(config.keys()))
                    raise AuthenticationError(f"Missing required FlatTrade credentials: {missing_keys}", context)

                return credentials
            except Exception as e:
                context = create_error_context(error=str(e), config_keys=list(config.keys()))
                raise AuthenticationError(f"Error extracting credentials: {str(e)}", context)

        def _verify_session(self):
            """Verify if the current session is valid using existing is_connected method."""
            return self.is_connected()

        def _restore_session_from_token(susertoken_path):
            """Attempt to restore session from existing token."""
            try:
                credentials = extract_credentials()
                user = credentials["user"]
                pwd = credentials["pwd"]

                with open(susertoken_path, "r") as file:
                    susertoken = file.read().strip()

                if not susertoken:
                    trading_logger.log_warning("Empty token file", {"broker": self.broker.name})
                    return False

                self.api = FlatTradeApiPy()
                self.api.set_session(userid=user, password=pwd, usertoken=susertoken)

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
                trading_logger.log_warning("Failed to restore session", e, {"broker": self.broker.name})
                return False

        def _fresh_login(susertoken_path):
            """Perform fresh login with enhanced error handling."""
            try:
                trading_logger.log_info("Performing fresh login", {"broker": self.broker.name})

                credentials = extract_credentials()
                user = credentials["user"]
                pwd = credentials["pwd"]
                api_key = credentials["api_key"]
                api_secret = credentials["api_secret"]
                token = credentials["token"]

                HOST = "https://auth.flattrade.in"
                API_HOST = "https://authapi.flattrade.in"

                routes = {
                    "session": f"{API_HOST}/auth/session",
                    "ftauth": f"{API_HOST}/ftauth",
                    "apitoken": f"{API_HOST}/trade/apitoken",
                }

                headers = {
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Host": "authapi.flattrade.in",
                    "Origin": f"{HOST}",
                    "Referer": f"{HOST}/",
                }

                def encode_item(item):
                    encoded_item = hashlib.sha256(item.encode()).hexdigest()
                    return encoded_item

                def get_authcode():
                    try:
                        with requests.Session() as session:
                            session.headers.update(headers)
                            response = session.post(routes["session"])
                            if response.status_code == 200:
                                sid = response.text
                                trading_logger.log_debug(
                                    "Session created successfully", {"sid": sid[:10] + "..." if len(sid) > 10 else sid}
                                )

                                response = session.post(
                                    routes["ftauth"],
                                    json={
                                        "UserName": user,
                                        "Password": encode_item(pwd),
                                        "App": "",
                                        "ClientID": "",
                                        "Key": "",
                                        "APIKey": api_key,
                                        "PAN_DOB": pyotp.TOTP(token).now(),
                                        "Sid": sid,
                                        "Override": "",
                                    },
                                )

                                if response.status_code == 200:
                                    response_data = response.json()
                                    if response_data.get("emsg") == "DUPLICATE":
                                        trading_logger.log_info(
                                            "Duplicate session detected, retrying with override",
                                            {"emsg": response_data.get("emsg")},
                                        )
                                        response = session.post(
                                            routes["ftauth"],
                                            json={
                                                "UserName": user,
                                                "Password": encode_item(pwd),
                                                "App": "",
                                                "ClientID": "",
                                                "Key": "",
                                                "APIKey": api_key,
                                                "PAN_DOB": pyotp.TOTP(token).now(),
                                                "Sid": sid,
                                                "Override": "Y",
                                            },
                                        )
                                        if response.status_code == 200:
                                            response_data = response.json()
                                        else:
                                            context = create_error_context(
                                                status_code=response.status_code, response_text=response.text
                                            )
                                            raise AuthenticationError(
                                                f"Authentication failed with override: {response.status_code}", context
                                            )
                                    else:
                                        trading_logger.log_debug(
                                            "Authentication successful", {"emsg": response_data.get("emsg")}
                                        )

                                    redirect_url = response_data.get("RedirectURL", "")
                                    if not redirect_url:
                                        context = create_error_context(response_data=response_data)
                                        raise AuthenticationError("No redirect URL in response", context)

                                    query_params = parse_qs(urlparse(redirect_url).query)
                                    if "code" in query_params:
                                        code = query_params["code"][0]
                                        trading_logger.log_debug(
                                            "Auth code obtained",
                                            {"code": code[:10] + "..." if len(code) > 10 else code},
                                        )
                                        return code
                                    else:
                                        context = create_error_context(
                                            redirect_url=redirect_url, query_params=query_params
                                        )
                                        raise AuthenticationError("No auth code in redirect URL", context)
                                else:
                                    context = create_error_context(
                                        status_code=response.status_code, response_text=response.text
                                    )
                                    raise AuthenticationError(f"Authentication failed: {response.status_code}", context)
                            else:
                                context = create_error_context(
                                    status_code=response.status_code, response_text=response.text
                                )
                                raise AuthenticationError(f"Session creation failed: {response.status_code}", context)
                    except Exception as e:
                        context = create_error_context(error=str(e))
                        raise AuthenticationError(f"Error in get_authcode: {str(e)}", context)

                def get_apitoken(code):
                    try:
                        with requests.Session() as session:
                            response = session.post(
                                routes["apitoken"],
                                json={
                                    "api_key": api_key,
                                    "request_code": code,
                                    "api_secret": encode_item(f"{api_key}{code}{api_secret}"),
                                },
                            )

                            if response.status_code == 200:
                                token_data = response.json()
                                token = token_data.get("token", "")
                                if not token:
                                    context = create_error_context(token_data=token_data)
                                    raise AuthenticationError("No token in response", context)

                                trading_logger.log_debug("API token obtained successfully")
                                return token
                            else:
                                context = create_error_context(
                                    status_code=response.status_code, response_text=response.text
                                )
                                raise AuthenticationError(f"Token request failed: {response.status_code}", context)
                    except Exception as e:
                        context = create_error_context(error=str(e))
                        raise AuthenticationError(f"Error in get_apitoken: {str(e)}", context)

                # TOTP retry logic to handle stale tokens
                max_attempts = 5
                for attempt in range(1, max_attempts + 1):
                    try:
                        trading_logger.log_info(
                            f"Fresh login attempt {attempt}/{max_attempts}",
                            {"broker_name": self.broker.name, "user": user},
                        )

                        request_token = get_authcode()
                        susertoken = get_apitoken(request_token)

                        trading_logger.log_info(
                            "Fresh login completed successfully",
                            {"user": user, "token_length": len(susertoken), "attempt": attempt},
                        )

                        # Save token to file
                        try:
                            with open(susertoken_path, "w") as file:
                                file.write(susertoken)
                            trading_logger.log_debug("Token saved to file", {"susertoken_path": susertoken_path})
                        except Exception as e:
                            context = create_error_context(susertoken_path=susertoken_path, error=str(e))
                            raise AuthenticationError(f"Failed to save token: {str(e)}", context)

                        # Initialize API
                        try:
                            self.api = FlatTradeApiPy()
                            self.api.set_session(userid=user, password=pwd, usertoken=susertoken)
                            trading_logger.log_info("API session established", {"user": user})
                        except Exception as e:
                            context = create_error_context(user=user, error=str(e))
                            raise AuthenticationError(f"Failed to set API session: {str(e)}", context)

                        return  # Success, exit retry loop

                    except Exception as e:
                        trading_logger.log_error(
                            f"Login attempt {attempt} failed",
                            e,
                            {
                                "broker_name": self.broker.name,
                                "user": user,
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                            },
                        )
                        if attempt < max_attempts:
                            time.sleep(40)  # Wait for fresh TOTP
                        else:
                            context = create_error_context(user=user, max_attempts=max_attempts, error=str(e))
                            raise AuthenticationError(
                                f"Fresh login failed after {max_attempts} attempts: {str(e)}", context
                            )

            except Exception as e:
                context = create_error_context(susertoken_path=susertoken_path, error=str(e))
                raise AuthenticationError(f"Error in _fresh_login: {str(e)}", context)

        def get_connected():
            """Main connection logic with robust session management."""
            susertoken_path = config.get(f"{self.broker.name}.USERTOKEN")

            if not susertoken_path:
                context = create_error_context(broker_name=self.broker.name, config_keys=list(config.keys()))
                raise BrokerConnectionError("USERTOKEN path not configured", context)

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
                    trading_logger.log_warning(
                        "Error checking token file, performing fresh login",
                        e,
                        {"broker": self.broker.name, "susertoken_path": susertoken_path},
                    )
                    _fresh_login(susertoken_path)
                    return True
            else:
                trading_logger.log_info("No existing token found, performing fresh login", {"broker": self.broker.name})
                _fresh_login(susertoken_path)
                return True

        try:
            trading_logger.log_info("Connecting to FlatTrade", {"redis_db": redis_db, "broker_name": self.broker.name})

            # Validate configuration
            if config.get(f"{self.broker.name}") == {}:
                context = create_error_context(broker_name=self.broker.name, config_keys=list(config.keys()))
                raise BrokerConnectionError("Configuration file not found or empty", context)

            # Update symbology
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

            # Set up Redis connection
            try:
                self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
                # Test Redis connection
                self.redis_o.ping()
                trading_logger.log_debug("Redis connection established", {"redis_db": redis_db})
            except Exception as e:
                context = create_error_context(redis_db=redis_db, error=str(e))
                raise BrokerConnectionError(f"Failed to connect to Redis: {str(e)}", context)

            # Set starting order IDs
            try:
                self.starting_order_ids_int = set_starting_internal_ids_int(redis_db=self.redis_o)
                trading_logger.log_debug(
                    "Starting order IDs set", {"starting_order_ids_int": self.starting_order_ids_int}
                )
            except Exception as e:
                trading_logger.log_warning("Failed to set starting order IDs", {"error": str(e), "redis_db": redis_db})

            trading_logger.log_info("Successfully connected to FlatTrade", {"redis_db": redis_db})
            return True

        except (ValidationError, BrokerConnectionError, AuthenticationError):
            raise
        except Exception as e:
            context = create_error_context(redis_db=redis_db, broker_name=self.broker.name, error=str(e))
            raise BrokerConnectionError(f"Unexpected error connecting to FlatTrade: {str(e)}", context)

    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    def is_connected(self):
        """
        Check if the FlatTrade broker is connected with enhanced error handling.

        Returns:
            bool: True if connected, False otherwise

        Raises:
            BrokerConnectionError: If connection check fails
        """
        trading_logger.log_debug("Checking FlatTrade connection", {"broker_type": self.broker.name})

        if not self.api:
            trading_logger.log_warning("API not initialized", {"broker_type": self.broker.name})
            return False

        # Check margin balance
        limits_data = self.api.get_limits()
        if not limits_data:
            trading_logger.log_warning("No limits data available", {"broker_type": self.broker.name})
            return False

        cash_balance = float(limits_data.get("cash", 0))

        trading_logger.log_debug("Margin check completed", {"cash_balance": cash_balance})

        if cash_balance <= 0:
            trading_logger.log_warning("Insufficient cash balance", {"cash_balance": cash_balance})
            return False

        # Check quote availability
        quote = self.get_quote("NIFTY_IND___")
        if not quote or quote.last <= 0:
            trading_logger.log_warning(
                "Quote check failed", {"broker_type": self.broker.name, "quote_last": quote.last if quote else None}
            )
            return False

        trading_logger.log_debug("Quote check completed", {"quote_last": quote.last, "symbol": "NIFTY_IND___"})

        trading_logger.log_info(
            "Connection check successful",
            {"broker_type": self.broker.name, "cash_balance": cash_balance, "quote_last": quote.last if quote else None},
        )
        return True

    @retry_on_error(max_retries=2, delay=0.5, backoff_factor=2.0)
    @log_execution_time
    def get_available_capital(self) -> float:
        """
        Get available capital/balance for trading (cash + collateral).

        Returns:
            float: Available capital amount (cash + collateral from limits)

        Raises:
            BrokerConnectionError: If broker is not connected
            MarketDataError: If balance retrieval fails
        """
        try:
            if not self.is_connected():
                raise BrokerConnectionError("FlatTrade broker is not connected")

            limits_data = self.api.get_limits()
            if not limits_data:
                raise MarketDataError("No limits data available")

            cash = limits_data.get("cash")
            if cash is None:
                raise MarketDataError("No cash information in limits")

            cash_float = float(cash)
            
            # Try to get collateral value (field name may vary)
            collateral = 0.0
            collateral_fields = ["collateral", "Collateral", "collateralvalue", "CollateralValue",
                               "holdingvalue", "HoldingValue", "securityvalue", "SecurityValue"]
            for field in collateral_fields:
                if field in limits_data:
                    try:
                        collateral = float(limits_data[field])
                        break
                    except (ValueError, TypeError):
                        continue
            
            total_capital = cash_float + collateral

            trading_logger.log_debug(
                "Available capital retrieved",
                {
                    "cash": cash_float,
                    "collateral": collateral,
                    "total_capital": total_capital,
                    "broker_type": self.broker.name,
                },
            )
            return total_capital

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e), broker_type=self.broker.name)
            trading_logger.log_error("Error getting available capital", e, context)
            raise MarketDataError(f"Failed to get available capital: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def disconnect(self):
        """
        Disconnect from the FlatTrade trading platform with enhanced error handling.

        Returns:
            bool: True if disconnection successful

        Raises:
            BrokerConnectionError: If disconnection fails
        """
        try:
            trading_logger.log_info("Disconnecting from FlatTrade", {"broker_type": self.broker.name})

            # Stop streaming if active
            try:
                if hasattr(self, "subscribe_thread") and self.subscribe_thread and self.subscribe_thread.is_alive():
                    trading_logger.log_info("Stopping streaming thread", {"broker_type": self.broker.name})
                    # Note: The actual streaming stop logic would be in the streaming method
            except Exception as e:
                trading_logger.log_warning("Failed to stop streaming during disconnect", {"error": str(e)})

            # Clear API reference
            if self.api:
                self.api = None
                trading_logger.log_info("API reference cleared", {"broker_type": self.broker.name})

            # Clear other references
            self.subscribed_symbols = []
            self.socket_opened = False

            trading_logger.log_info("Successfully disconnected from FlatTrade", {"broker_type": self.broker.name})
            return True

        except Exception as e:
            context = create_error_context(broker_type=self.broker.name, error=str(e))
            raise BrokerConnectionError(f"Failed to disconnect from FlatTrade: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def update_symbology(self, **kwargs):
        dt_today = get_tradingapi_now().date()
        symbols_path = os.path.join(config.get(f"{self.broker.name}.SYMBOLCODES"), f"{dt_today}_symbols.csv")
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
        return codes

    def log_and_return(self, any_object):
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
                        log_object = any_object.to_dict()  # Use the object's attributes as a dictionary
                    except Exception as e:
                        trading_logger.log_warning(
                            "Error converting object to dict", {"error": str(e), "object_type": type(any_object).__name__}
                        )
                        log_object = {"error": f"Error accessing to_dict: {str(e)}"}
                else:
                    # If no __dict__, treat the object as a simple serializable object (e.g., a dict, list, etc.)
                    log_object = any_object

                # Add the calling function name to the log
                log_entry = {"caller": caller_function, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), "object": log_object}

                # Log the entry to Redis
                try:
                    self.redis_o.zadd(f"{self.broker.name.upper()}:LOG", {json.dumps(log_entry): time.time()})
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
                trading_logger.log_warning("Error logging object to Redis", {"error": str(e), "object_type": type(any_object).__name__})
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
            order.broker = self.broker

            # Validate exchange mapping exists
            if order.exchange not in self.exchange_mappings:
                trading_logger.log_error(
                    "Exchange not found in mappings",
                    {"exchange": order.exchange, "available_exchanges": list(self.exchange_mappings.keys())},
                )
                return order

            order.scrip_code = self.exchange_mappings[order.exchange]["symbol_map"].get(order.long_symbol, None)
            orig_order_type = order.order_type

            if order.scrip_code is not None or order.paper:  # if paper, we dont check for valid scrip_code
                has_trigger = not math.isnan(order.trigger_price)
                if has_trigger:
                    order.is_stoploss_order = True
                    order.stoploss_price = order.trigger_price
                if order.order_type == "BUY" or order.order_type == "COVER":
                    order.order_type = "B"
                elif order.order_type == "SHORT" or order.order_type == "SELL":
                    order.order_type = "S"
                else:
                    trading_logger.log_error(
                        "Invalid order type", {"order_type": order.order_type, "long_symbol": order.long_symbol}
                    )
                    return order
                order.remote_order_id = get_tradingapi_now().strftime("%Y%m%d%H%M%S%f")[:-4]

                if not order.paper:
                    try:
                        quantity = order.quantity
                        product_type = "C" if "_STK_" in order.long_symbol else "M"  # M is NRML , 'I' is MIS
                        price_type = "LMT" if order.price > 0 else "MKT"
                        if has_trigger:
                            price_type = "SL-LMT" if order.price > 0 else "SL-MKT"
                        trading_symbol = self._get_tradingsymbol_from_longname(order.long_symbol, order.exchange)

                        if not trading_symbol:
                            trading_logger.log_error(
                                "Failed to get trading symbol",
                                {"long_symbol": order.long_symbol, "exchange": order.exchange},
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
                            "Flattrade order info", {"order_info": json.dumps(out, indent=4, default=str), "long_symbol": order.long_symbol, "broker_order_id": order.broker_order_id if hasattr(order, 'broker_order_id') else None}
                        )

                        if not out:
                            trading_logger.log_error(
                                "Empty response from broker",
                                {"order": str(order), "long_symbol": order.long_symbol, "internal_order_id": order.internal_order_id}
                            )
                            return order

                        if out.get("stat") is None:
                            trading_logger.log_error(
                                "Error placing order",
                                {"order": str(order), "response": str(out), "long_symbol": order.long_symbol, "internal_order_id": order.internal_order_id}
                            )
                            return order

                        if out["stat"].upper() == "OK":
                            order.broker_order_id = out.get("norenordno")
                            order.exch_order_id = out.get("norenordno")
                            order.order_type = orig_order_type
                            order.orderRef = order.internal_order_id

                            if not order.broker_order_id:
                                trading_logger.log_error(
                                    "No broker order ID in response",
                                    {"order": str(order), "response": str(out), "long_symbol": order.long_symbol, "internal_order_id": order.internal_order_id}
                                )
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
                                {"order": str(order), "response": str(out), "long_symbol": order.long_symbol, "internal_order_id": order.internal_order_id}
                            )
                            return order

                    except Exception as e:
                        trading_logger.log_error(
                            "Exception during order placement",
                            e,
                            {"order": str(order), "long_symbol": order.long_symbol, "internal_order_id": order.internal_order_id}
                        )
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
        mandatory_keys = ['broker_order_id', 'new_price', 'new_quantity']

        """
        mandatory_keys = ["broker_order_id", "new_price", "new_quantity"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id")
        new_price = float(kwargs.get("new_price", 0.0))
        new_quantity = int(kwargs.get("new_quantity", 0))
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
    def cancel_order(self, **kwargs):
        """
        mandatory_keys = ['broker_order_id']

        """
        mandatory_keys = ["broker_order_id"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id")

        order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.status in [OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED]:
            valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
            if valid_date and valid_date == dt.datetime.today().strftime("%Y-%m-%d"):
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

        def return_db_as_fills(order: Order):
            order_info = OrderInfo()
            valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
            if valid_date and valid_date != dt.datetime.today().strftime("%Y-%m-%d"):
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

        valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
        if (
            valid_date
            and valid_date != dt.datetime.today().strftime("%Y-%m-%d")
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

    @retry_on_error(max_retries=3, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    @validate_inputs(
        symbols=lambda x: x is not None and (isinstance(x, str) or isinstance(x, dict) or isinstance(x, pd.DataFrame)),
        date_start=lambda x: valid_datetime(x)[0] is not False,
        date_end=lambda x: valid_datetime(x)[0] is not False,
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
                    date_end_str, _ = valid_datetime(date_end, "%Y-%m-%d")
                    date_end_obj = dt.datetime.strptime(date_end_str, "%Y-%m-%d")
                    date_end_yyyymmdd = date_end_obj.strftime("%Y%m%d")
                    
                    # Get symbol codes path from config
                    symbol_codes_path = config.get(f"{self.broker.name}.SYMBOLCODES")
                    if not symbol_codes_path:
                        context = create_error_context(broker_name=self.broker.name)
                        raise ConfigurationError(f"SYMBOLCODES path not found in config for {self.broker.name}", context)
                    
                    # Construct path to symbols file
                    symbols_file_path = os.path.join(symbol_codes_path, f"{date_end_yyyymmdd}_symbols.csv")
                    
                    trading_logger.log_debug(
                        "Loading symbol mapping from file",
                        {"symbols_file_path": symbols_file_path, "date": date_end_yyyymmdd}
                    )
                    
                    if not os.path.exists(symbols_file_path):
                        trading_logger.log_warning(
                            "Symbols file not found for refresh_mapping",
                            {"symbols_file_path": symbols_file_path}
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
                            {"exchanges": list(refresh_symbol_map.keys()), "total_symbols": len(codes)}
                        )
                except Exception as e:
                    trading_logger.log_warning(
                        "Error loading symbol mapping from file, falling back to default",
                        {"error": str(e)},
                        exc_info=True
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
                        "Did not get ScripCode for symbols", {"symbols": symbols, "symbol_type": "string"}
                    )
                    return {}
            elif isinstance(symbols, dict):
                scripCode = symbols.get("scrip_code")
                if scripCode:
                    symbols_pd = pd.DataFrame([{"long_symbol": symbols.get("long_symbol"), "Scripcode": scripCode}])
                else:
                    trading_logger.log_error(
                        "Did not get ScripCode for symbols", {"symbols": symbols, "symbol_type": "dict"}
                    )
                    return {}
            else:
                symbols_pd = symbols

            out = {}  # Initialize the output dictionary

            for index, row_outer in symbols_pd.iterrows():
                trading_logger.log_debug(
                    "Processing historical data",
                    {"index": str(index), "total_symbols": str(len(symbols)), "long_symbol": row_outer["long_symbol"]},
                )
                exchange = self.map_exchange_for_api(row_outer["long_symbol"], exchange)
                historical_data_list = []
                exch = exchange
                s = row_outer["long_symbol"].replace("/", "-")
                row_outer["long_symbol"] = "NSENIFTY" + s[s.find("_") :] if s.startswith("NIFTY_") else s
                # we do the above remapping for downloading permin data to database for legacy reasons.
                # once NSENIFTY is amended to NIFTY in databae, we can remove this line.

                date_start_dt, _ = valid_datetime(date_start, None)
                date_end, _ = valid_datetime(date_end, "%Y%m%d")
                date_end_dt, _ = valid_datetime(date_end + " " + market_close_time, None)
                if isinstance(date_start_dt, dt.date) and not isinstance(date_start_dt, dt.datetime):
                    date_start_dt = dt.datetime.combine(date_start_dt, dt.datetime.min.time())
                if isinstance(date_end_dt, dt.date) and not isinstance(date_end_dt, dt.datetime):
                    date_end_dt = dt.datetime.combine(date_end_dt, dt.datetime.min.time())
                try:
                    if periodicity.endswith("m"):
                        data = self.api.get_time_price_series(
                            exchange=exch,
                            token=str(row_outer["Scripcode"]),
                            starttime=date_start_dt.timestamp(),
                            endtime=date_end_dt.timestamp(),
                            interval=extract_number(periodicity),
                        )
                    elif periodicity == "1d":
                        if row_outer["long_symbol"] == "NSENIFTY_IND___":
                            row_outer["long_symbol"] = "NIFTY_IND___"
                        trading_symbol = self.exchange_mappings[exchange]["tradingsymbol_map"].get(row_outer["long_symbol"])

                        def _timeout_handler(signum, frame):
                            raise TimeoutError("daily_price_series call timed out")

                        signal.signal(signal.SIGALRM, _timeout_handler)  # Install the handler

                        attempts = 3
                        wait_seconds = 2
                        data = None

                        for attempt in range(attempts):
                            start_time = time.time()
                            signal.alarm(2)  # Trigger a timeout in 3 seconds
                            try:
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
                        {"long_symbol": row_outer["long_symbol"], "periodicity": periodicity},
                    )
                    data = None

                if not (data is None or len(data) == 0):
                    market_open = pd.to_datetime(market_open_time).time()
                    market_close = pd.to_datetime(market_close_time).time()
                    for d in data:
                        if isinstance(d, str):
                            d = json.loads(d)
                        if periodicity.endswith("m"):
                            date = pd.Timestamp(timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%m-%Y %H:%M:%S")))
                            # Filter by market open/close time for intraday
                            if not (market_open <= date.time() < market_close):
                                continue
                            date = pd.Timestamp(timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%m-%Y %H:%M:%S")))
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
                    trading_logger.log_debug("No data found for symbol", {"long_symbol": row_outer["long_symbol"]})
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
                        today_start = dt.datetime.combine(get_tradingapi_now().date(), dt.datetime.min.time())
                    try:
                        intraday_data = self.api.get_time_price_series(
                            exchange=exch,
                            token=str(row_outer["Scripcode"]),
                            starttime=today_start.timestamp(),
                            interval=1,  # Request 1-minute data
                        )
                    except Exception as e:
                        trading_logger.log_error(
                            "Error in get_time_price_series for intraday data", e, {"long_symbol": row_outer["long_symbol"]}
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
                            date = pd.Timestamp(row.name).tz_localize(timezone)
                            if date_start <= date <= date_end:
                                historical_data = HistoricalData(
                                    date=pd.Timestamp(row.name).tz_localize(timezone),
                                    open=row["into"],
                                    high=row["inth"],
                                    low=row["intl"],
                                    close=row["intc"],
                                    volume=row["intv"],
                                    intoi=row["intoi"],
                                    oi=row["oi"],
                                )
                                historical_data_list.append(historical_data)
                out[row_outer["long_symbol"]] = historical_data_list

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

    def map_exchange_for_api(self, long_symbol, exchange):
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
            result = exchange_map.get(exchange, exchange)

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

    def map_exchange_for_db(self, long_symbol, exchange):
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

    def convert_ft_to_ist(self, ft: int):
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
            trading_logger.log_debug("Fetching quote", {"long_symbol": long_symbol, "exchange": exchange})
            mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
            market_feed = Price()  # Initialize with default values
            market_feed.src = "sh"
            market_feed.symbol = long_symbol

            # Validate exchange mapping exists
            if mapped_exchange not in self.exchange_mappings:
                trading_logger.log_error(
                    "Exchange mapping not found",
                    {"mapped_exchange": mapped_exchange, "available_exchanges": list(self.exchange_mappings.keys())},
                )
                return market_feed

            token = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
            if token is None:
                trading_logger.log_error(
                    "No token found for symbol", {"long_symbol": long_symbol, "mapped_exchange": mapped_exchange}
                )
                return market_feed  # Return default Price object if no token is found

            try:
                tick_data = self.api.get_quotes(exchange=mapped_exchange, token=str(token))

                if not tick_data:
                    trading_logger.log_warning(
                        "Empty tick data received",
                        {"long_symbol": long_symbol, "exchange": mapped_exchange, "token": token},
                    )
                    return market_feed

                # Safely extract and convert values with validation
                def safe_float(value, default=float("nan")):
                    """Safely convert value to float with validation."""
                    if value in [None, 0, "0", "0.00", float("nan"), ""]:
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default

                def safe_int(value, default=0):
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
                    trading_logger.log_warning(
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
                    trading_logger.log_warning(
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
    def start_quotes_streaming(self, operation: str, symbols: List[str], ext_callback=None, exchange="NSE"):
        """
        Start streaming quotes for the given symbols.

        Args:
            operation (str): 's' for subscribe, 'u' for unsubscribe.
            symbols (List[str]): List of symbols to subscribe/unsubscribe.
            ext_callback (function): External callback function for processing price updates.
            exchange (str): Exchange name (default: 'NSE').
        """
        try:
            trading_logger.log_info(
                "Starting quotes streaming",
                {"operation": operation, "symbols_count": len(symbols), "exchange": exchange},
            )

            if not symbols or len(symbols) == 0:
                trading_logger.log_error("Symbols list cannot be empty", {"operation": operation, "exchange": exchange})
                return

            prices = {}
            mapped_exchange = self.map_exchange_for_api(symbols[0], exchange)

            # Function to map JSON data to a Price object
            def map_to_price(json_data):
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
                    float("nan") if json_data.get("bq1") in [None, 0, "0", float("nan")] else float(json_data.get("bq1"))
                )
                price.ask_volume = (
                    float("nan") if json_data.get("sq1") in [None, 0, "0", float("nan")] else float(json_data.get("sq1"))
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
            def on_message(message):
                if message.get("t") == "tk":
                    price = map_to_price(message)
                    prices[message.get("tk")] = price
                    ext_callback(price)
                elif message.get("t") == "tf":
                    required_keys = {"bp1", "sp1", "c", "lp", "bq1", "sq1", "h", "l"}
                    if required_keys & message.keys():
                        price = prices.get(message.get("tk"))
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
                        ext_callback(price)

            # Function to handle WebSocket errors
            def handle_socket_error(error=None):
                if error:
                    trading_logger.log_error("WebSocket error", {"error": str(error)})
                else:
                    trading_logger.log_error("WebSocket error. Connection to remote host was lost.")

            def handle_socket_close(close_code=None, close_msg=None):
                if close_msg:
                    trading_logger.log_error("WebSocket closed", {"close_msg": str(close_msg)})
                    initiate_reconnect()

            def initiate_reconnect(max_retries=5, retry_delay=5):
                """
                Attempt to reconnect the WebSocket connection.
                """
                for attempt in range(max_retries):
                    try:
                        trading_logger.log_info("Reconnect attempt", {"attempt": attempt + 1, "max_retries": max_retries})

                        # Close the existing WebSocket connection if open
                        if hasattr(self, "api") and self.api and hasattr(self, "socket_opened") and self.socket_opened:
                            try:
                                self.api.close_websocket()
                                self.socket_opened = False
                                trading_logger.log_info("Closed existing WebSocket connection")
                            except Exception as e:
                                trading_logger.log_warning("Failed to close existing WebSocket", e)

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
                                self.api.subscribe(req_list)
                                trading_logger.log_info("Resubscribed to symbols after reconnection")
                                return
                            except Exception as e:
                                trading_logger.log_error("Failed to resubscribe after reconnection", e)
                        else:
                            trading_logger.log_warning("WebSocket did not open within the expected time.")

                    except Exception as e:
                        trading_logger.log_error("Reconnect attempt failed", e, {"attempt": attempt + 1})

                        # Wait before the next retry with exponential backoff
                        wait_time = retry_delay * (2**attempt)
                        trading_logger.log_info("Waiting before next retry", {"wait_time": wait_time, "attempt": attempt + 1})
                        time.sleep(wait_time)

                trading_logger.log_error("Max reconnect attempts reached. Unable to reconnect the WebSocket.")
                # Set a flag to indicate connection failure
                if hasattr(self, "socket_opened"):
                    self.socket_opened = False

            # Function to handle WebSocket connection opening
            def on_socket_open():
                trading_logger.log_info("WebSocket connection opened")
                self.socket_opened = True

            # Function to establish WebSocket connection and subscribe
            def connect_and_subscribe():
                self.api.start_websocket(
                    subscribe_callback=on_message,
                    socket_close_callback=handle_socket_close,
                    socket_error_callback=handle_socket_error,
                    socket_open_callback=on_socket_open,
                )
                while not self.socket_opened:
                    time.sleep(1)

            # Function to expand symbols into request format
            def expand_symbols_to_request(symbol_list):
                req_list = []
                for symbol in symbol_list:
                    scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(symbol)
                    if scrip_code:
                        req_list.append(f"{mapped_exchange}|{scrip_code}")
                    else:
                        trading_logger.log_error("Did not find scrip_code for symbol", {"symbol": symbol})
                return req_list

            # Function to update the subscription list
            def update_subscription_list(operation, symbols):
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
    @validate_inputs(long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_position(self, long_symbol: str):
        try:
            trading_logger.log_debug("Getting position", {"long_symbol": long_symbol if long_symbol else "all"})

            pos = pd.DataFrame(self.api.get_positions())
            if len(pos) > 0:
                try:
                    pos["long_symbol"] = self.get_long_name_from_broker_identifier(ScripName=pos.tsym)
                except Exception as e:
                    trading_logger.log_error("Error processing positions data", e, {"positions_count": len(pos)})
                    return pd.DataFrame(columns=["long_symbol", "quantity"])

                if long_symbol is None or long_symbol == "":
                    trading_logger.log_debug("Returning all positions", {"position_count": len(pos)})
                    return pos
                else:
                    pos_filtered = pos.loc[pos.long_symbol == long_symbol, "netqty"]
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
                            {"long_symbol": long_symbol, "position_count": len(pos_filtered)},
                        )
                        raise MarketDataError(f"Multiple positions found for symbol: {long_symbol}")
            else:
                trading_logger.log_debug("No positions found")
                return pos

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, error=str(e))
            trading_logger.log_error("Unexpected error getting position", e, context)
            raise MarketDataError(f"Unexpected error getting position: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_orders_today(self, **kwargs):
        try:
            trading_logger.log_debug("Getting orders for today")
            result = super().get_orders_today(**kwargs)
            if result is not None and len(result) > 0:
                trading_logger.log_debug("Orders retrieved successfully", {"order_count": len(result)})
            else:
                trading_logger.log_debug("No orders found for today")
            return result
        except Exception as e:
            context = create_error_context(error=str(e))
            trading_logger.log_error("Unexpected error getting orders", e, context)
            raise

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_trades_today(self, **kwargs):
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
    @validate_inputs(ScripName=lambda x: isinstance(x, pd.Series) and len(x) > 0)
    def get_long_name_from_broker_identifier(self, **kwargs):
        """Generates Long Name

        Args:
            ScripName (pd.Series): position.ScripName from 5paisa position

        Returns:
            pd.series: long name
        """
        try:
            trading_logger.log_debug(
                "Generating long name from broker identifier",
                {"scrip_name_count": len(kwargs.get("ScripName", pd.Series()))},
            )

            ScripName = kwargs.get("ScripName")
            if ScripName is None:
                context = create_error_context(available_keys=list(kwargs.keys()))
                raise ValidationError("Missing required argument: 'ScripName'", context)

            if not isinstance(ScripName, pd.Series) or len(ScripName) == 0:
                context = create_error_context(
                    scrip_name_type=type(ScripName),
                    scrip_name_length=len(ScripName) if hasattr(ScripName, "__len__") else None,
                )
                raise ValidationError("ScripName must be a non-empty pandas Series", context)

            def split_fno(fno_symbol):
                # Check if it's SENSEX format (ends with CE/PE)
                if fno_symbol.startswith("SENSEX") and (fno_symbol.endswith("CE") or fno_symbol.endswith("PE")):
                    # Extract symbol
                    symbol = "SENSEX"

                    # Extract option type (last 2 characters)
                    option_type = fno_symbol[-2:]  # CE or PE
                    part3 = "C" if option_type == "CE" else "P"

                    # Extract the part after SENSEX and before CE/PE
                    middle_part = fno_symbol[6:-2]  # e.g., "25SEP91600", "25DEC89000", "2580591600"

                    # Try to parse different SENSEX formats
                    try:
                        # Extract year (first 2 digits)
                        year = "20" + middle_part[:2]

                        # Extract the remaining part after year
                        remaining = middle_part[2:]

                        # Check if next 3 characters are letters (month abbreviation) - check this FIRST
                        if len(remaining) >= 3 and remaining[:3].isalpha():
                            # Month abbreviation format (3 letters)
                            month_abbr = remaining[:3]

                            # Convert month abbreviation to number
                            month_map = {
                                "JAN": "01",
                                "FEB": "02",
                                "MAR": "03",
                                "APR": "04",
                                "MAY": "05",
                                "JUN": "06",
                                "JUL": "07",
                                "AUG": "08",
                                "SEP": "09",
                                "OCT": "10",
                                "NOV": "11",
                                "DEC": "12",
                            }

                            if month_abbr in month_map:
                                month = month_map[month_abbr]

                                # For 3-letter month format, no day is given
                                # We need to find the last working Tuesday of the month
                                # For now, we'll use a placeholder day (25th)
                                day = "25"  # Placeholder - should be calculated as last working Tuesday
                                first_dom = f"01-{month}-{year}"
                                expiry = get_expiry(first_dom, weekly=0, day_of_week=2, exchange="BSE")
                                day = expiry[:2]
                                # Extract strike (remaining digits after month abbreviation)
                                strike = remaining[3:]

                                # Construct date string
                                part2 = f"{year}{month}{day}"
                                part4 = strike

                                return f"{symbol}_OPT_{part2}_{'CALL' if part3=='C' else 'PUT'}_{part4}"

                        # Check if next character is a digit (single digit month)
                        elif remaining[0].isdigit():
                            # Single digit month format
                            month_digit = remaining[0]
                            month = month_digit.zfill(2)  # 8 -> 08 (August)

                            # Extract day (next 2 digits)
                            day = remaining[1:3]

                            # Extract strike (remaining digits before CE/PE)
                            strike = remaining[3:]

                            # Construct date string
                            part2 = f"{year}{month}{day}"
                            part4 = strike

                            return f"{symbol}_OPT_{part2}_{'CALL' if part3=='C' else 'PUT'}_{part4}"

                        # Check if next character is O, N, or D (single letter month)
                        elif remaining[0] in ["O", "N", "D"]:
                            # Single letter month format
                            month_letter = remaining[0]

                            # Map single letters to months: O=October, N=November, D=December
                            month_map = {"O": "10", "N": "11", "D": "12"}  # October  # November  # December

                            month = month_map[month_letter]

                            # Extract day (next 2 digits)
                            day = remaining[1:3]

                            # Extract strike (remaining digits before CE/PE)
                            strike = remaining[3:]

                            # Construct date string
                            part2 = f"{year}{month}{day}"
                            part4 = strike

                            return f"{symbol}_OPT_{part2}_{'CALL' if part3=='C' else 'PUT'}_{part4}"

                        # Fallback for unexpected formats
                        else:
                            trading_logger.log_warning(
                                "Unexpected SENSEX format",
                                {"fno_symbol": fno_symbol, "middle_part": middle_part, "remaining": remaining},
                            )
                            part2 = "20250101"
                            part4 = middle_part[2:]  # Use remaining part as strike
                            return f"{symbol}_OPT_{part2}_{'CALL' if part3=='C' else 'PUT'}_{part4}"

                    except Exception as e:
                        trading_logger.log_error(
                            "Error parsing SENSEX symbol", e, {"fno_symbol": fno_symbol, "middle_part": middle_part}
                        )
                        part2 = "20250101"
                        part4 = middle_part
                        return f"{symbol}_OPT_{part2}_{'CALL' if part3=='C' else 'PUT'}_{part4}"

                else:
                    # Original NIFTY format: NIFTY07AUG25C25050
                    part1 = re.search(r"^.*?(?=\d{2}[A-Z]{3}\d{2})", fno_symbol).group()
                    date_match = re.search(r"\d{2}[A-Z]{3}\d{2}", fno_symbol)
                    part2 = dt.datetime.strptime(date_match.group(), "%d%b%y").date().strftime("%Y%m%d")
                    part3 = re.search(r"(?<=\d{2}[A-Z]{3}\d{2}).*?([A-Z])", fno_symbol).group(1)
                    part4 = re.search(r"\d{2}[A-Z]{3}\d{2}\D(.*)", fno_symbol).group(1)
                    return f"{part1}_{'FUT' if part3 == 'F' else 'OPT'}_{part2}_{'CALL' if part3=='C' else 'PUT' if part3 =='P' else ''}_{part4}"

            def split_cash(cash_symbol):
                lst = cash_symbol.split("_")
                if len(lst) > 1:
                    return "-".join(lst[:-1]) + "_STK___"
                else:
                    return lst[0] + "_STK___"

            result = ScripName.apply(lambda x: split_cash(x) if x[-3] == "-" else split_fno(x))

            trading_logger.log_debug(
                "Long name generated successfully", {"input_count": len(ScripName), "output_count": len(result)}
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
    def get_min_lot_size(self, long_symbol, exchange="NSE"):
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
                    lot_size = self.codes.loc[self.codes.Scripcode == code, "LotSize"].item()
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
