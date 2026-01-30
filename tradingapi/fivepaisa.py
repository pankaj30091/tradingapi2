import datetime as dt
import inspect
import io
import json
import logging
import math
import os
import re
import secrets  # Replace `random` with `secrets` for cryptographic randomness
import sys
import threading
import time
import traceback
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyotp
import redis
import requests
from chameli.dateutils import valid_datetime
from py5paisa import FivePaisaClient

from .broker_base import BrokerBase, Brokers, HistoricalData, Order, OrderInfo, OrderStatus, Price
from .config import get_config
from .utils import delete_broker_order_id, set_starting_internal_ids_int, update_order_status
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
from .error_handling import retry_on_error, safe_execute, log_execution_time, handle_broker_errors, validate_inputs
from . import trading_logger
from .globals import get_tradingapi_now

logger = logging.getLogger(__name__)
config = get_config()


# Enhanced exception handler with structured logging
def my_handler(typ, value, trace):
    context = create_error_context(
        exception_type=typ.__name__, exception_value=str(value), traceback="".join(traceback.format_tb(trace))
    )
    trading_logger.log_error(f"Uncaught exception: {typ.__name__}", value, context)


sys.excepthook = my_handler


@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = False):
    try:
        bhavcopyfolder = config.get("bhavcopy_folder")
        url = "https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/All"
        dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_codes.csv"
        response = requests.get(
            url, allow_redirects=True, timeout=100
        )  # Add timeout to `requests.get` to fix Bandit issue

        if response.status_code != 200:
            raise Exception(f"Failed to fetch symbol data. Status code: {response.status_code}")

        df = pd.read_csv(io.BytesIO(response.content))
        # Rename the column
        df.rename(columns={"ScripCode": "Scripcode"}, inplace=True)
        # Save the DataFrame back to CSV
        df.to_csv(dest_file, index=False)
        codes = pd.read_csv(dest_file, dtype="str")
        numeric_columns = [
            "Scripcode",
            "StrikeRate",
            "LotSize",
            "QtyLimit",
            "Multiplier",
            "TickSize",
        ]
        for col in numeric_columns:
            codes[col] = pd.to_numeric(codes[col], errors="coerce")
        codes.columns = [col.strip() for col in codes.columns]
        codes = codes.map(lambda x: x.strip() if isinstance(x, str) else x)
        codes = codes[
            (codes.Exch.isin(["N", "M", "B"]))
            & (codes.ExchType.isin(["C", "D"]))
            & (codes.Series.isin(["EQ", "BE", "XX", "BZ", "RR", "IV", ""]))
        ]
        pattern = r"\d+GS\d+"
        codes = codes[~codes["Name"].str.contains(pattern, regex=True, na=True)]
        codes["long_symbol"] = None
        # Converting specific columns to numeric
        numeric_columns = ["LotSize", "TickSize", "Scripcode"]

        for col in numeric_columns:
            codes[col] = pd.to_numeric(codes[col], errors="coerce")

        # Vectorized string splitting
        codes["symbol_vec"] = codes["Name"].str.split(" ")

        # Function to process each row
        def process_row(row):
            symbol_vec = row["symbol_vec"]
            ticksize = row["TickSize"]

            if row["QtyLimit"] == 0 and row["LotSize"] == 2000 and row["TickSize"] == 0 and row["Exch"] in ["N"]:
                return f"{''.join(symbol_vec).replace('/', '')}_IND___".upper()
            elif (
                row["QtyLimit"] == 0
                and row["Exch"] in ["B"]
                and row["Scripcode"] >= 999900
                and row["Scripcode"] <= 999999
            ):
                return f"{''.join(symbol_vec)}_IND___".upper()
            elif len(symbol_vec) == 1 and ticksize > 0:
                return f"{symbol_vec[0]}_STK___".upper()
            elif len(symbol_vec) == 4:
                expiry_str = f"{symbol_vec[3]}{symbol_vec[2]}{symbol_vec[1]}"
                try:
                    expiry = dt.datetime.strptime(expiry_str, "%Y%b%d").strftime("%Y%m%d")
                    return f"{symbol_vec[0]}_FUT_{expiry}__".upper()
                except Exception as e:
                    trading_logger.log_error(
                        "Error processing symbol row", e, {"symbol_vec": symbol_vec, "row": str(row)}
                    )
                    # Also log to the standard logger as backup
                    logger.error(f"Error processing symbol row: {e}", exc_info=True)
                    return None
            elif len(symbol_vec) == 6:
                expiry_str = f"{symbol_vec[3]}{symbol_vec[2]}{symbol_vec[1]}"
                try:
                    expiry = dt.datetime.strptime(expiry_str, "%Y%b%d").strftime("%Y%m%d")
                    right = "CALL" if symbol_vec[4] == "CE" else "PUT"
                    strike = ("%f" % float(symbol_vec[5])).rstrip("0").rstrip(".")
                    return f"{symbol_vec[0]}_OPT_{expiry}_{right}_{strike}".upper()
                except Exception as e:
                    trading_logger.log_error(
                        "Error processing symbol row", e, {"symbol_vec": symbol_vec, "row": str(row)}
                    )
                    # Also log to the standard logger as backup
                    logger.error(f"Error processing symbol row: {e}", exc_info=True)
                    return None
            else:
                return None

        codes["long_symbol"] = codes.apply(process_row, axis=1)
        codes = codes.dropna(subset=["long_symbol"])
        codes = codes.drop(columns=["symbol_vec"])
        if saveToFolder:
            dest_symbol_file = (
                f"{config.get('FIVEPAISA.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
            )
            filtered_codes = codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize"]]

            filtered_codes.to_csv(dest_symbol_file, index=False)
        return codes
    except Exception as e:
        # Log error to both trading_logger and standard logger
        try:
            trading_logger.log_error(
                "Error in save_symbol_data",
                e,
                {"saveToFolder": saveToFolder, "dest_file": dest_file if "dest_file" in locals() else None},
            )
        except Exception:
            pass  # If trading_logger fails, continue with standard logging

        # Also log to standard logger as backup
        logger.error(f"Error in save_symbol_data: {e}", exc_info=True)
        raise


class FivePaisa(BrokerBase):
    """
    FivePaisa broker implementation with enhanced error handling and logging.
    """

    @log_execution_time
    def __init__(self, **kwargs):
        """
        Initialize FivePaisa broker with enhanced error handling.

        Args:
            **kwargs: Configuration parameters for FivePaisa broker

        Raises:
            ValidationError: If configuration parameters are invalid
            BrokerConnectionError: If broker initialization fails
        """
        try:
            super().__init__(**kwargs)
            self.codes = pd.DataFrame()
            self.broker = Brokers.FIVEPAISA
            self.api = None
            self.subscribe_thread = None
            self.subscribed_symbols = []

            trading_logger.log_info(
                "FivePaisa broker initialized", {"broker_type": "FivePaisa", "config_keys": list(kwargs.keys())}
            )
        except Exception as e:
            context = create_error_context(broker_type="FivePaisa", config_keys=list(kwargs.keys()), error=str(e))
            raise BrokerConnectionError(f"Failed to initialize FivePaisa broker: {str(e)}", context)

    @log_execution_time
    @validate_inputs(any_object=lambda x: x is not None)
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
                            "Error accessing object to_dict method",
                            {"object_type": type(any_object).__name__, "error": str(e)},
                        )
                        log_object = {"error": f"Error accessing __dict__: {str(e)}"}
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
                    self.redis_o.zadd("FIVEPAISA:LOG", {json.dumps(log_entry): time.time()})
                    trading_logger.log_debug(
                        "Object logged to Redis successfully",
                        {"caller_function": caller_function, "object_type": type(any_object).__name__},
                    )
                except Exception as e:
                    context = create_error_context(
                        caller_function=caller_function, object_type=type(any_object).__name__, error=str(e)
                    )
                    raise RedisError(f"Failed to log object to Redis: {str(e)}", context)

            except Exception as e:
                trading_logger.log_error(
                    "Error processing object for logging",
                    e,
                    {"caller_function": caller_function, "object_type": type(any_object).__name__},
                )
                # Continue execution even if logging fails

            return any_object

        except (ValidationError, RedisError):
            raise
        except Exception as e:
            context = create_error_context(object_type=type(any_object).__name__, error=str(e))
            raise ValidationError(f"Unexpected error in log_and_return: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def _validate_quantity(self, quantity):
        """
        Validate quantity parameter that can be int, float, or string representation.

        Args:
            quantity: Quantity value to validate

        Returns:
            bool: True if valid quantity, False otherwise
        """
        try:
            if isinstance(quantity, (int, float)):
                return quantity >= 0
            elif isinstance(quantity, str) and quantity.strip():
                # Try to convert string to float
                float_val = float(quantity.strip())
                return float_val >= 0
            else:
                return False
        except (ValueError, TypeError):
            return False

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def update_symbology(self, **kwargs):
        """
        Update symbology mappings with enhanced error handling.

        Args:
            **kwargs: Additional keyword arguments

        Returns:
            pd.DataFrame: Updated symbology codes

        Raises:
            ConfigurationError: If configuration is invalid
            DataError: If symbology data processing fails
            FileNotFoundError: If symbol file is not found
        """
        try:
            trading_logger.log_info("Updating symbology", {"broker_name": self.broker.name})

            try:
                dt_today = get_tradingapi_now().strftime("%Y%m%d")
                symbol_codes_path = config.get(f"{self.broker.name}.SYMBOLCODES")

                if not symbol_codes_path:
                    context = create_error_context(broker_name=self.broker.name, config_keys=list(config.keys()))
                    raise ConfigurationError(f"SYMBOLCODES path not found in config for {self.broker.name}", context)

                symbols_path = os.path.join(symbol_codes_path, f"{dt_today}_symbols.csv")

                trading_logger.log_debug("Symbols file path", {"symbols_path": symbols_path, "date": dt_today})

            except Exception as e:
                context = create_error_context(broker_name=self.broker.name, error=str(e))
                raise ConfigurationError(f"Error getting symbols path: {str(e)}", context)

            # Load symbols data
            try:
                if not os.path.exists(symbols_path):
                    trading_logger.log_info(
                        "Symbols file not found, generating new data", {"symbols_path": symbols_path}
                    )
                    codes = save_symbol_data(saveToFolder=False)
                    codes = codes.dropna(subset=["long_symbol"])
                else:
                    trading_logger.log_info("Loading existing symbols file", {"symbols_path": symbols_path})
                    codes = pd.read_csv(symbols_path)

                trading_logger.log_debug(
                    "Symbols data loaded", {"data_shape": codes.shape, "columns": list(codes.columns)}
                )

            except FileNotFoundError as e:
                context = create_error_context(symbols_path=symbols_path, error=str(e))
                raise FileNotFoundError(f"Symbols file not found: {str(e)}", context)
            except Exception as e:
                context = create_error_context(symbols_path=symbols_path, error=str(e))
                raise DataError(f"Error loading symbols data: {str(e)}", context)

            # Initialize dictionaries to hold mappings for each exchange
            try:
                self.exchange_mappings = {}

                for exchange, group in codes.groupby("Exch"):
                    try:
                        self.exchange_mappings[exchange] = {
                            "symbol_map": dict(zip(group["long_symbol"], group["Scripcode"])),
                            "contractsize_map": dict(zip(group["long_symbol"], group["LotSize"])),
                            "exchange_map": dict(zip(group["long_symbol"], group["Exch"])),
                            "exchangetype_map": dict(zip(group["long_symbol"], group["ExchType"])),
                            "contracttick_map": dict(zip(group["long_symbol"], group["TickSize"])),
                            "symbol_map_reversed": dict(zip(group["Scripcode"], group["long_symbol"])),
                        }

                        trading_logger.log_debug(
                            "Exchange mappings created", {"exchange": exchange, "symbol_count": len(group)}
                        )

                    except Exception as e:
                        trading_logger.log_error(
                            "Error creating mappings for exchange",
                            e,
                            {"exchange": exchange, "group_shape": group.shape},
                        )
                        continue

                trading_logger.log_info(
                    "Symbology update completed",
                    {"total_exchanges": len(self.exchange_mappings), "total_symbols": len(codes)},
                )

            except Exception as e:
                context = create_error_context(codes_shape=codes.shape if codes is not None else None, error=str(e))
                raise DataError(f"Error creating exchange mappings: {str(e)}", context)

            return codes

        except (ConfigurationError, DataError, FileNotFoundError):
            raise
        except Exception as e:
            context = create_error_context(broker_name=self.broker.name, error=str(e))
            raise DataError(f"Unexpected error updating symbology: {str(e)}", context)

    @log_execution_time
    @validate_inputs(redis_db=lambda x: isinstance(x, int) and x >= 0)
    def connect(self, redis_db: int):
        """
        Connect to FivePaisa trading platform with enhanced session management.

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
                    "APP_SOURCE": config.get(f"{self.broker.name}.APP_SOURCE"),
                    "APP_NAME": config.get(f"{self.broker.name}.APP_NAME"),
                    "USER_ID": config.get(f"{self.broker.name}.USER_ID"),
                    "PASSWORD": config.get(f"{self.broker.name}.PASSWORD"),
                    "USER_KEY": config.get(f"{self.broker.name}.USER_KEY"),
                    "ENCRYPTION_KEY": config.get(f"{self.broker.name}.ENCRYPTION_KEY"),
                }

                missing_keys = [key for key, value in credentials.items() if not value]
                if missing_keys:
                    context = create_error_context(missing_keys=missing_keys, available_keys=list(config.keys()))
                    raise AuthenticationError(f"Missing required FivePaisa credentials: {missing_keys}", context)

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
                client_id = config.get(f"{self.broker.name}.CLIENT_ID")
                if not client_id:
                    trading_logger.log_warning(
                        "CLIENT_ID not configured for session restore", {"broker": self.broker.name}
                    )
                    return False

                with open(susertoken_path, "r") as file:
                    susertoken = file.read().strip()

                if not susertoken:
                    trading_logger.log_warning("Empty token file", {"broker": self.broker.name})
                    return False

                self.api = FivePaisaClient(cred=extract_credentials())
                self.api.set_access_token(susertoken, client_id)

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
            """Perform fresh login with TOTP retry logic."""
            try:
                trading_logger.log_info("Performing fresh login", {"broker": self.broker.name})

                self.api = FivePaisaClient(cred=extract_credentials())
                max_attempts = 5

                for attempt in range(1, max_attempts + 1):
                    try:
                        trading_logger.log_info(
                            f"Fresh login attempt {attempt}/{max_attempts}", {"broker": self.broker.name}
                        )

                        totp_token = config.get(f"{self.broker.name}.TOTP_TOKEN")
                        if not totp_token:
                            raise AuthenticationError("TOTP_TOKEN not configured")

                        otp = pyotp.TOTP(totp_token).now()
                        client_id = config.get(f"{self.broker.name}.CLIENT_ID")
                        pin = config.get(f"{self.broker.name}.PIN")

                        if not client_id or not pin:
                            raise AuthenticationError("CLIENT_ID or PIN not configured")

                        self.api.get_totp_session(client_id, otp, pin)

                        if self.api.access_token:
                            trading_logger.log_info(
                                "Fresh login successful", {"broker": self.broker.name, "attempt": attempt}
                            )

                            susertoken = self.api.access_token

                            # Save token
                            try:
                                with open(susertoken_path, "w") as file:
                                    file.write(susertoken)
                                trading_logger.log_info("Token saved successfully", {"broker": self.broker.name})
                                return  # Success, exit retry loop
                            except Exception as e:
                                trading_logger.log_warning(
                                    "Failed to save token",
                                    e,
                                    {"broker": self.broker.name, "susertoken_path": susertoken_path},
                                )
                                # Continue even if save fails, as we have a valid session
                                return
                        else:
                            trading_logger.log_warning(
                                "Login attempt failed - no access token",
                                {"broker": self.broker.name, "attempt": attempt, "max_attempts": max_attempts},
                            )
                            if attempt < max_attempts:
                                time.sleep(40)  # Wait for fresh TOTP
                            else:
                                raise AuthenticationError("Login failed - no access token received")

                    except Exception as e:
                        trading_logger.log_error(
                            f"Login attempt {attempt} failed",
                            e,
                            {"broker": self.broker.name, "attempt": attempt, "max_attempts": max_attempts},
                        )
                        if attempt < max_attempts:
                            time.sleep(40)  # Wait for fresh TOTP
                        else:
                            raise AuthenticationError(f"Fresh login failed after {max_attempts} attempts: {str(e)}")

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
            trading_logger.log_info("Connecting to FivePaisa", {"redis_db": redis_db, "broker_name": self.broker.name})

            if config.get(self.broker.name) == {}:
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

            trading_logger.log_info("Successfully connected to FivePaisa", {"redis_db": redis_db})
            return True

        except (ValidationError, BrokerConnectionError, AuthenticationError):
            raise
        except Exception as e:
            context = create_error_context(redis_db=redis_db, broker_name=self.broker.name, error=str(e))
            raise BrokerConnectionError(f"Unexpected error connecting to FivePaisa: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def is_connected(self):
        """
        Check if the FivePaisa broker is connected.

        Returns:
            bool: True if connected, False otherwise

        Raises:
            BrokerConnectionError: If connection check fails
        """
        try:
            if not self.api:
                trading_logger.log_warning("API not initialized", {"broker_type": "FivePaisa"})
                return False

            # Check margin balance
            try:
                margin_data = self.api.margin()
                if not margin_data or len(margin_data) == 0:
                    trading_logger.log_warning("No margin data available", {"broker_type": "FivePaisa"})
                    return False

                ledger_balance = float(margin_data[0]["Ledgerbalance"])
                funds_payln = float(margin_data[0]["FundsPayln"])
                total_balance = ledger_balance + funds_payln

                trading_logger.log_debug(
                    "Margin check completed",
                    {"ledger_balance": ledger_balance, "funds_payln": funds_payln, "total_balance": total_balance},
                )

            except Exception as e:
                trading_logger.log_error("Error checking margin", e, {"broker_type": "FivePaisa"})
                return False

            # Check quote availability
            try:
                quote = self.get_quote("NIFTY_IND___")
                if not quote or quote.last <= 0:
                    trading_logger.log_warning(
                        "Quote check failed", {"broker_type": "FivePaisa", "quote_last": quote.last if quote else None}
                    )
                    return False

                trading_logger.log_debug("Quote check completed", {"quote_last": quote.last, "symbol": "NIFTY_IND___"})

            except Exception as e:
                trading_logger.log_error(
                    "Error checking quote", e, {"broker_type": "FivePaisa", "symbol": "NIFTY_IND___"}
                )
                return False

            trading_logger.log_info(
                "Connection check successful",
                {
                    "broker_type": self.broker.name,
                    "total_balance": total_balance,
                    "quote_last": quote.last if quote else None,
                },
            )
            return True

        except Exception as e:
            context = create_error_context(broker_type="FivePaisa", error=str(e))
            trading_logger.log_error("Connection check failed", e, context)
            return False

    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    @log_execution_time
    def get_available_capital(self) -> float:
        """
        Get available capital/balance for trading (cash + collateral).

        Returns:
            float: Available capital amount (Ledgerbalance + FundsPayln + Collateral)

        Raises:
            BrokerConnectionError: If broker is not connected
            MarketDataError: If balance retrieval fails
        """
        try:
            if not self.is_connected():
                raise BrokerConnectionError("FivePaisa broker is not connected")

            margin_data = self.api.margin()
            if not margin_data or len(margin_data) == 0:
                raise MarketDataError("No margin data available")

            ledger_balance = float(margin_data[0]["Ledgerbalance"])
            funds_payln = float(margin_data[0]["FundsPayln"])
            cash = ledger_balance + funds_payln
            
            # Get collateral value (FivePaisa has both MF and regular collateral)
            collateral = 0.0
            try:
                mf_collateral = float(margin_data[0].get("MFCollateralValueAfterHaircut", 0))
                regular_collateral = float(margin_data[0].get("CollateralValueAfterHairCut", 0))
                collateral = mf_collateral + regular_collateral
            except (ValueError, TypeError, KeyError):
                # If specific fields not found, try alternative field names
                collateral_fields = ["Collateral", "collateral", "CollateralValue", "collateral_value"]
                for field in collateral_fields:
                    if field in margin_data[0]:
                        try:
                            collateral = float(margin_data[0][field])
                            break
                        except (ValueError, TypeError):
                            continue
            
            total_capital = cash + collateral

            trading_logger.log_debug(
                "Available capital retrieved",
                {
                    "ledger_balance": ledger_balance,
                    "funds_payln": funds_payln,
                    "cash": cash,
                    "collateral": collateral,
                    "total_capital": total_capital,
                },
            )
            return total_capital

        except (BrokerConnectionError, MarketDataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            trading_logger.log_error("Error getting available capital", e, context)
            raise MarketDataError(f"Failed to get available capital: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def disconnect(self):
        """
        Disconnect from the FivePaisa trading platform.

        Raises:
            BrokerConnectionError: If disconnection fails
        """
        try:
            trading_logger.log_info("Disconnecting from FivePaisa", {"broker_type": self.broker.name})

            # Stop streaming if active
            try:
                self.stop_streaming()
            except Exception as e:
                trading_logger.log_warning("Failed to stop streaming during disconnect", {"error": str(e)})

            # Clear API reference
            if self.api:
                self.api = None
                trading_logger.log_info("API reference cleared", {"broker_type": self.broker.name})

            trading_logger.log_info("Successfully disconnected from FivePaisa", {"broker_type": self.broker.name})
            return True

        except Exception as e:
            context = create_error_context(broker_type=self.broker.name, error=str(e))
            raise BrokerConnectionError(f"Failed to disconnect from FivePaisa: {str(e)}", context)

    @log_execution_time
    @validate_inputs(order=lambda x: isinstance(x, Order))
    def place_order(self, order: Order, **kwargs) -> Order:
        """
        Place an order with the FivePaisa broker with enhanced error handling.

        Args:
            order: Order object to place
            **kwargs: Additional order parameters

        Returns:
            Order: Updated order object

        Raises:
            ValidationError: If order parameters are invalid
            OrderError: If order placement fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            # Validate mandatory keys
            mandatory_keys = [
                "long_symbol",
                "order_type",
                "quantity",
                "price",
                "exchange",
                "exchange_segment",
                "internal_order_id",
                "paper",
            ]
            order_dict = order.to_dict()
            missing_keys = [key for key in mandatory_keys if key not in order_dict or order_dict[key] is None]

            if missing_keys:
                context = create_error_context(
                    missing_keys=missing_keys, available_keys=list(order_dict.keys()), order_data=order_dict
                )
                raise ValidationError(f"Missing mandatory keys: {', '.join(missing_keys)}", context)

            # Set broker and get scrip code
            order.broker = self.broker
            try:
                order.scrip_code = self.exchange_mappings[order.exchange]["symbol_map"].get(order.long_symbol, None)
            except KeyError as e:
                context = create_error_context(
                    exchange=order.exchange,
                    long_symbol=order.long_symbol,
                    available_exchanges=list(self.exchange_mappings.keys()),
                )
                raise OrderError(f"Exchange mapping not found: {str(e)}", context)

            orig_order_type = order.order_type

            # Check if we can place the order
            if order.scrip_code is not None or order.paper:
                has_trigger = not math.isnan(order.trigger_price)
                if has_trigger:
                    order.is_stoploss_order = True
                    order.stoploss_price = order.trigger_price
                # Map order type
                if order.order_type in ["BUY", "COVER"]:
                    order.order_type = "B"
                elif order.order_type in ["SHORT", "SELL"]:
                    order.order_type = "S"
                else:
                    context = create_error_context(
                        order_type=order.order_type, valid_types=["BUY", "COVER", "SHORT", "SELL"]
                    )
                    raise ValidationError(f"Invalid order type: {order.order_type}", context)

                order.remote_order_id = get_tradingapi_now().strftime("%Y%m%d%H%M%S%f")[:-4]

                if not order.paper:
                    # Real order placement
                    try:
                        # Calculate quantity for MCX
                        if order.exchange == "M":
                            min_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(order.long_symbol)
                            quantity: Optional[int] = None
                            if min_size is not None and order.quantity is not None:
                                quantity = int(round(order.quantity / min_size, 0))
                            else:
                                trading_logger.log_warning(
                                    "Could not calculate quantity for MCX",
                                    {
                                        "min_size": min_size,
                                        "order_quantity": order.quantity,
                                        "long_symbol": order.long_symbol,
                                    },
                                )
                                quantity = order.quantity
                        else:
                            quantity = order.quantity

                        # Place order with API
                        payload = {
                            "OrderType": order.order_type,
                            "Exchange": order.exchange,
                            "ExchangeType": order.exchange_segment,
                            "ScripCode": order.scrip_code,
                            "Qty": quantity,
                            "Price": order.price,
                            "RemoteOrderID": order.remote_order_id,
                            "IsStopLossOrder": bool(order.is_stoploss_order),
                            "StopLossPrice": order.stoploss_price if has_trigger else order.stoploss_price,
                            "DisQty": getattr(order, "disqty", quantity),
                            "IsIntraday": bool(order.is_intraday),
                            "AHPlaced": order.ahplaced,
                            "IOCOrder": bool(order.ioc_order),
                        }
                        if has_trigger:
                            payload["TriggerPrice"] = order.trigger_price

                        # Verify API is connected before making call
                        if not self.api:
                            context = create_error_context(
                                order_data=order.to_dict(),
                                payload=payload,
                            )
                            raise BrokerConnectionError(
                                "FivePaisa API client is not initialized. Please call connect() first.", context
                            )

                        # Make API call with error handling
                        try:
                            out = self.api.place_order(**payload)
                        except Exception as api_error:
                            context = create_error_context(
                                order_data=order.to_dict(),
                                payload=payload,
                                api_error=str(api_error),
                                api_error_type=type(api_error).__name__,
                            )
                            trading_logger.log_error(
                                "Exception during FivePaisa API place_order call",
                                api_error,
                                context,
                            )
                            raise OrderError(f"FivePaisa API call failed: {str(api_error)}", context)

                        trading_logger.log_info(
                            "FivePaisa order info", {"order_info": json.dumps(out, indent=4, default=str), "long_symbol": order.long_symbol, "broker_order_id": order.broker_order_id if hasattr(order, 'broker_order_id') else None}
                        )

                        if out is not None:
                            # Update order with response
                            order.exch_order_id = out.get("ExchOrderID")
                            # Convert broker_order_id to string as API returns integer but validation expects string
                            broker_order_id_raw = out.get("BrokerOrderID")
                            order.broker_order_id = str(broker_order_id_raw) if broker_order_id_raw is not None else ""
                            order.local_order_id = out.get("LocalOrderID")
                            order.order_type = orig_order_type
                            order.orderRef = order.internal_order_id
                            order.message = out.get("Message")
                            order.status = out.get("Status")

                            # Get exchange order ID
                            try:
                                order.exch_order_id = self._get_exchange_order_id(
                                    order.broker_order_id, order, delete=False
                                )
                            except Exception as e:
                                trading_logger.log_warning(
                                    "Failed to get exchange order ID",
                                    {"broker_order_id": order.broker_order_id, "error": str(e)},
                                )

                            # Get order info
                            try:
                                fills = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                                order.status = fills.status
                                order.exch_order_id = fills.exchange_order_id

                                if fills.status == OrderStatus.REJECTED:
                                    trading_logger.log_warning(
                                        "Order rejected by broker",
                                        {"order": str(order), "broker_order_id": order.broker_order_id},
                                    )
                                    return order

                                if fills.fill_price > 0:
                                    order.price = fills.fill_price

                            except Exception as e:
                                trading_logger.log_error(
                                    "Failed to get order info",
                                    e,
                                    {"broker_order_id": order.broker_order_id, "order": str(order)},
                                )

                            trading_logger.log_info("Placed Order", {"order": str(order)})
                            self.log_and_return(order)
                            return order
                        else:
                            # API returned None - check if order was actually placed (idempotency check)
                            trading_logger.log_warning(
                                "FivePaisa API returned None, checking if order already exists by remote_order_id",
                                {
                                    "remote_order_id": order.remote_order_id,
                                    "internal_order_id": order.internal_order_id,
                                    "long_symbol": order.long_symbol,
                                },
                            )

                            # Check if order with same remote_order_id already exists
                            existing_order = self._check_order_exists_by_remote_id(
                                order.remote_order_id, order.exchange, order.scrip_code
                            )

                            if existing_order:
                                # Order already exists! Skip placing new order and update Redis with existing details
                                trading_logger.log_info(
                                    "Order already exists with same remote_order_id, retrieving details to avoid duplicate",
                                    {
                                        "remote_order_id": order.remote_order_id,
                                        "existing_broker_order_id": existing_order.get("BrokerOrderID"),
                                    },
                                )

                                # Update order with existing order details
                                broker_order_id_raw = existing_order.get("BrokerOrderID")
                                order.broker_order_id = str(broker_order_id_raw) if broker_order_id_raw is not None else ""
                                order.exch_order_id = existing_order.get("ExchOrderID") or "0"
                                order.local_order_id = existing_order.get("LocalOrderID") or 0
                                order.order_type = orig_order_type
                                order.orderRef = order.internal_order_id
                                order.message = existing_order.get("Message") or ""

                                # Get full order info to get accurate status and fill price
                                if order.broker_order_id and order.broker_order_id != "0":
                                    try:
                                        fills = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                                        order.status = fills.status
                                        order.exch_order_id = fills.exchange_order_id
                                        if fills.fill_price > 0:
                                            order.price = fills.fill_price
                                    except Exception as e:
                                        trading_logger.log_warning(
                                            "Failed to get full order info for existing order, using basic status",
                                            {"broker_order_id": order.broker_order_id, "error": str(e)},
                                        )
                                        # Fallback to basic status from API response - convert string to enum if needed
                                        api_status = existing_order.get("Status")
                                        if api_status:
                                            try:
                                                # Try to convert string status to enum
                                                if isinstance(api_status, str):
                                                    order.status = OrderStatus[api_status.upper()]
                                                elif isinstance(api_status, OrderStatus):
                                                    order.status = api_status
                                                else:
                                                    order.status = OrderStatus.UNDEFINED
                                            except (KeyError, AttributeError):
                                                order.status = OrderStatus.UNDEFINED
                                        else:
                                            order.status = OrderStatus.UNDEFINED
                                else:
                                    # No broker_order_id available, set status to UNDEFINED
                                    trading_logger.log_warning(
                                        "No broker_order_id available for existing order",
                                        {"remote_order_id": order.remote_order_id},
                                    )
                                    order.status = OrderStatus.UNDEFINED

                                # Update Redis with the existing order details (same as successful order placement)
                                # Only update Redis if we have a valid broker_order_id
                                if order.broker_order_id and order.broker_order_id != "0":
                                    try:
                                        # Store full order dict in Redis keyed by broker_order_id
                                        self.redis_o.hmset(
                                            str(order.broker_order_id),
                                            {key: str(val) for key, val in order.to_dict().items()},
                                        )

                                        # Update entry_keys or exit_keys based on order type
                                        if order.order_type in ["BUY", "SHORT"]:
                                            current_entry_keys = self.redis_o.hget(order.internal_order_id, "entry_keys")
                                            if current_entry_keys is None or str(order.broker_order_id) not in current_entry_keys:
                                                new_entry_keys = (
                                                    str(order.broker_order_id)
                                                    if current_entry_keys is None
                                                    else current_entry_keys + " " + str(order.broker_order_id)
                                                )
                                                self.redis_o.hset(order.orderRef, "entry_keys", new_entry_keys)
                                                self.redis_o.hset(order.internal_order_id, "long_symbol", order.long_symbol)

                                        elif order.order_type in ["SELL", "COVER"]:
                                            current_exit_keys = self.redis_o.hget(order.internal_order_id, "exit_keys")
                                            if current_exit_keys is None or str(order.broker_order_id) not in current_exit_keys:
                                                new_exit_keys = (
                                                    str(order.broker_order_id)
                                                    if current_exit_keys is None
                                                    else current_exit_keys + " " + str(order.broker_order_id)
                                                )
                                                self.redis_o.hset(order.orderRef, "exit_keys", new_exit_keys)

                                        trading_logger.log_info(
                                            "Updated Redis with existing order details",
                                            {
                                                "broker_order_id": order.broker_order_id,
                                                "internal_order_id": order.internal_order_id,
                                                "order_type": order.order_type,
                                            },
                                        )
                                    except Exception as e:
                                        trading_logger.log_error(
                                            "Failed to update Redis with existing order details",
                                            e,
                                            {
                                                "broker_order_id": order.broker_order_id,
                                                "internal_order_id": order.internal_order_id,
                                            },
                                        )
                                else:
                                    trading_logger.log_warning(
                                        "Skipping Redis update - broker_order_id is empty or '0'",
                                        {
                                            "broker_order_id": order.broker_order_id,
                                            "internal_order_id": order.internal_order_id,
                                        },
                                    )

                                trading_logger.log_info(
                                    "Retrieved and synced existing order to prevent duplicate placement",
                                    {"order": str(order), "broker_order_id": order.broker_order_id},
                                )
                                self.log_and_return(order)
                                return order

                            else:
                                # Order doesn't exist - this is a real failure
                                context = create_error_context(
                                    order_data=order.to_dict(),
                                    api_response=out,
                                    payload_sent=payload,
                                    api_connected=bool(self.api),
                                    remote_order_id=order.remote_order_id,
                                )
                                trading_logger.log_error(
                                    "FivePaisa API returned None and order does not exist on broker",
                                    None,
                                    context,
                                )
                                raise OrderError(
                                    "FivePaisa API returned None for order placement and order not found on broker. "
                                    "This may indicate: 1) API connection issue, 2) Invalid order parameters, "
                                    "3) Broker rejection, 4) Network timeout. Check logs for details.",
                                    context,
                                )

                    except Exception as e:
                        context = create_error_context(order_data=order.to_dict(), error=str(e))
                        raise OrderError(f"Failed to place real order: {str(e)}", context)
                else:
                    # Paper order
                    try:
                        order.order_type = orig_order_type
                        order.exch_order_id = str(secrets.randbelow(10**15)) + "P"
                        order.broker_order_id = str(secrets.randbelow(10**8)) + "P"
                        order.orderRef = order.internal_order_id
                        order.message = "Paper Order"
                        order.status = OrderStatus.FILLED
                        order.scrip_code = 0 if order.scrip_code is None else order.scrip_code

                        trading_logger.log_info("Placed Paper Order", {"order": str(order)})

                    except Exception as e:
                        context = create_error_context(order_data=order.to_dict(), error=str(e))
                        raise OrderError(f"Failed to place paper order: {str(e)}", context)

                self.log_and_return(order)
                return order
            else:
                # No scrip code found
                context = create_error_context(
                    long_symbol=order.long_symbol,
                    exchange=order.exchange,
                    scrip_code=order.scrip_code,
                    paper=order.paper,
                )
                trading_logger.log_info("No broker identifier found for symbol", {"long_symbol": order.long_symbol})
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
        new_quantity=lambda x: isinstance(x, (int, float, str))
        and (isinstance(x, (int, float)) and x >= 0 or (isinstance(x, str) and x.strip() and float(x.strip()) >= 0)),
    )
    def modify_order(self, **kwargs) -> Order:
        """
        Modify an existing order with the FivePaisa broker with enhanced error handling.

        Args:
            **kwargs: Order modification parameters including:
                - broker_order_id: Broker order ID to modify
                - new_price: New price for the order
                - new_quantity: New quantity for the order

        Returns:
            Order: Updated order object

        Raises:
            ValidationError: If modification parameters are invalid
            OrderError: If order modification fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            # Validate mandatory keys
            mandatory_keys = ["broker_order_id", "new_price", "new_quantity"]
            missing_keys = [key for key in mandatory_keys if key not in kwargs]
            if missing_keys:
                context = create_error_context(missing_keys=missing_keys, available_keys=list(kwargs.keys()))
                raise ValidationError(f"Missing mandatory keys: {', '.join(missing_keys)}", context)

            broker_order_id = str(kwargs.get("broker_order_id", "0"))
            new_price = float(kwargs.get("new_price", 0))
            new_quantity = kwargs.get("new_quantity", 0)

            # Validate parameters
            if not broker_order_id or broker_order_id == "0":
                context = create_error_context(broker_order_id=broker_order_id)
                raise ValidationError("Invalid broker_order_id", context)

            if new_price < 0:
                context = create_error_context(new_price=new_price)
                raise ValidationError("New price cannot be negative", context)

            if new_quantity < 0:
                context = create_error_context(new_quantity=new_quantity)
                raise ValidationError("New quantity cannot be negative", context)

            # Get order from Redis
            try:
                order_data = self.redis_o.hgetall(broker_order_id)
                if not order_data:
                    context = create_error_context(broker_order_id=broker_order_id)
                    raise OrderError("Order not found in Redis", context)
                order = Order(**order_data)
            except Exception as e:
                context = create_error_context(broker_order_id=broker_order_id, error=str(e))
                raise OrderError(f"Failed to retrieve order from Redis: {str(e)}", context)

            # Get current order info
            try:
                fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
            except Exception as e:
                trading_logger.log_error(
                    "Failed to get order info", e, {"broker_order_id": broker_order_id, "order": str(order)}
                )
                fills = None

            if fills and fills.status in [OrderStatus.UNDEFINED, OrderStatus.PENDING]:
                trading_logger.log_warning(
                    "Order not active with exchange",
                    {"broker_order_id": broker_order_id, "status": fills.status.name if fills.status else None},
                )

            order.status = fills.status if fills else OrderStatus.UNDEFINED

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
                        "current_fills": str(fills.fill_size) if fills else "0",
                        "long_symbol": order.long_symbol,
                    },
                )

                try:
                    exch_order_id = order.exch_order_id

                    # Calculate order quantity
                    if order.exchange == "M":
                        long_symbol = order.long_symbol
                        contract_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(long_symbol)
                        if contract_size:
                            order_quantity = int(round(new_quantity / contract_size, 0))
                        else:
                            trading_logger.log_warning(
                                "Contract size not found for MCX",
                                {"long_symbol": long_symbol, "exchange": order.exchange},
                            )
                            order_quantity = new_quantity
                    else:
                        order_quantity = new_quantity - (fills.fill_size if fills else 0)
                        order_quantity = order_quantity if order_quantity > 0 else 0

                    # Modify order with API
                    # Convert numpy/pandas types to native Python types before passing to SDK
                    # The SDK's set_payload doesn't convert types, and JSON serialization fails with int64/float64
                    try:
                        # Convert to native types for JSON serialization
                        native_exch_order_id = int(exch_order_id) if isinstance(exch_order_id, (np.integer, np.int64, np.int32)) else str(exch_order_id)
                        native_price = float(new_price) if isinstance(new_price, (np.floating, np.float64, np.float32)) else float(new_price)
                        native_qty = int(order_quantity) if isinstance(order_quantity, (np.integer, np.int64, np.int32)) else int(order_quantity)
                        out = self.api.modify_order(ExchOrderID=native_exch_order_id, Price=native_price, Qty=native_qty)
                    except Exception as e:
                        trading_logger.log_error(
                            "Exception during SDK modify_order call",
                            e,
                            {
                                "exch_order_id": exch_order_id,
                                "price": new_price,
                                "qty": order_quantity,
                                "broker_order_id": broker_order_id,
                                "long_symbol": order.long_symbol,
                            },
                        )
                        out = None

                    if out is None:
                        context = create_error_context(
                            broker_order_id=broker_order_id,
                            exch_order_id=exch_order_id,
                            new_price=new_price,
                            order_quantity=order_quantity,
                            long_symbol=order.long_symbol,
                        )
                        trading_logger.log_error(
                            "Error modifying order - API returned None",
                            None,
                            context,
                        )
                        raise OrderError("API returned None for order modification", context)

                    self.log_and_return(out)

                    if out.get("Status") == 0:
                        # Update order with new values
                        order.price_type = new_price
                        order.quantity = new_quantity
                        order.price = new_price
                        order.broker_order_id = str(out.get("BrokerOrderID", ""))

                        # Get updated order info
                        try:
                            order_info = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                            order.status = order_info.status
                            order.exch_order_id = order_info.exchange_order_id
                        except Exception as e:
                            trading_logger.log_error(
                                "Failed to get updated order info", e, {"broker_order_id": order.broker_order_id}
                            )

                        # Update broker order ID
                        try:
                            self._update_broker_order_id(order.internal_order_id, broker_order_id, out["BrokerOrderID"])
                        except Exception as e:
                            trading_logger.log_warning(
                                "Failed to update broker order ID",
                                {
                                    "internal_order_id": order.internal_order_id,
                                    "old_broker_order_id": broker_order_id,
                                    "new_broker_order_id": out["BrokerOrderID"],
                                    "error": str(e),
                                },
                            )

                        # Update Redis
                        try:
                            self.redis_o.hmset(
                                order.broker_order_id, {key: str(val) for key, val in order.to_dict().items()}
                            )
                        except Exception as e:
                            trading_logger.log_error(
                                "Failed to update Redis",
                                e,
                                {"broker_order_id": order.broker_order_id, "order_data": order.to_dict()},
                            )

                        # Update order status
                        try:
                            fills = update_order_status(self, order.internal_order_id, order.broker_order_id)
                            order.status = fills.status
                        except Exception as e:
                            trading_logger.log_error(
                                "Failed to update order status",
                                e,
                                {
                                    "internal_order_id": order.internal_order_id,
                                    "broker_order_id": order.broker_order_id,
                                },
                            )

                        trading_logger.log_info(
                            "Order modified successfully",
                            {
                                "broker_order_id": order.broker_order_id,
                                "new_price": new_price,
                                "new_quantity": new_quantity,
                            },
                        )
                    else:
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
                                "api_status": out.get("Status"),
                                "api_response": out,
                            },
                        )

                except Exception as e:
                    context = create_error_context(
                        broker_order_id=broker_order_id, new_price=new_price, new_quantity=new_quantity, error=str(e)
                    )
                    raise OrderError(f"Failed to modify order: {str(e)}", context)
            else:
                trading_logger.log_info(
                    "Order status does not allow modification",
                    {"broker_order_id": broker_order_id, "status": order.status.name if order.status else None},
                )

            self.log_and_return(order)
            return order

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(kwargs=kwargs, error=str(e))
            raise OrderError(f"Unexpected error modifying order: {str(e)}", context)

    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def cancel_order(self, **kwargs) -> Order:
        """
        Cancel an existing order with the FivePaisa broker with enhanced error handling.

        Args:
            **kwargs: Order cancellation parameters including:
                - broker_order_id: Broker order ID to cancel

        Returns:
            Order: Updated order object

        Raises:
            ValidationError: If cancellation parameters are invalid
            OrderError: If order cancellation fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            # Validate mandatory keys
            mandatory_keys = ["broker_order_id"]
            missing_keys = [key for key in mandatory_keys if key not in kwargs]
            if missing_keys:
                context = create_error_context(missing_keys=missing_keys, available_keys=list(kwargs.keys()))
                raise ValidationError(f"Missing mandatory keys: {', '.join(missing_keys)}", context)

            broker_order_id = str(kwargs.get("broker_order_id", "0"))

            # Validate broker_order_id
            if not broker_order_id or broker_order_id == "0":
                context = create_error_context(broker_order_id=broker_order_id)
                raise ValidationError("Invalid broker_order_id", context)

            # Get order from Redis
            try:
                order_data = self.redis_o.hgetall(broker_order_id)
                if not order_data:
                    context = create_error_context(broker_order_id=broker_order_id)
                    raise OrderError("Order not found in Redis", context)
                order = Order(**order_data)
            except Exception as e:
                context = create_error_context(broker_order_id=broker_order_id, error=str(e))
                raise OrderError(f"Failed to retrieve order from Redis: {str(e)}", context)

            if order.status in [OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED]:
                try:
                    valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
                    if valid_date and valid_date == dt.datetime.today().strftime("%Y-%m-%d"):
                        # Get current order info
                        try:
                            fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
                        except Exception as e:
                            trading_logger.log_error(
                                "Failed to get order info for cancellation",
                                e,
                                {"broker_order_id": broker_order_id, "order": str(order)},
                            )
                            fills = None

                        if fills and fills.fill_size < round(float(order.quantity)):
                            trading_logger.log_info(
                                "Cancelling broker order",
                                {
                                    "broker_order_id": broker_order_id,
                                    "long_symbol": order.long_symbol,
                                    "filled": str(fills.fill_size),
                                    "ordered": order.quantity,
                                },
                            )

                            # Cancel order with API
                            try:
                                out = self.api.cancel_order(exch_order_id=order.exch_order_id)
                                self.log_and_return(out)

                                if out and "BrokerOrderID" in out:
                                    # Convert broker_order_id to string for consistency
                                    order.broker_order_id = str(out["BrokerOrderID"])

                                    # Update broker order ID
                                    try:
                                        self._update_broker_order_id(
                                            order.internal_order_id, broker_order_id, out["BrokerOrderID"]
                                        )
                                    except Exception as e:
                                        trading_logger.log_warning(
                                            "Failed to update broker order ID",
                                            {
                                                "internal_order_id": order.internal_order_id,
                                                "old_broker_order_id": broker_order_id,
                                                "new_broker_order_id": out["BrokerOrderID"],
                                                "error": str(e),
                                            },
                                        )

                                    # Update Redis
                                    try:
                                        self.redis_o.hmset(
                                            order.broker_order_id,
                                            {key: str(val) for key, val in order.to_dict().items()},
                                        )
                                    except Exception as e:
                                        trading_logger.log_error(
                                            "Failed to update Redis",
                                            e,
                                            {"broker_order_id": order.broker_order_id, "order_data": order.to_dict()},
                                        )

                                    # Update order status
                                    try:
                                        fills = update_order_status(
                                            self, order.internal_order_id, order.broker_order_id, eod=True
                                        )
                                        self.log_and_return(fills)
                                        order.status = fills.status
                                        order.quantity = fills.fill_size
                                        order.price = fills.fill_price
                                    except Exception as e:
                                        trading_logger.log_error(
                                            "Failed to update order status",
                                            e,
                                            {
                                                "internal_order_id": order.internal_order_id,
                                                "broker_order_id": order.broker_order_id,
                                            },
                                        )

                                    trading_logger.log_info(
                                        "Order cancelled successfully",
                                        {"broker_order_id": order.broker_order_id, "symbol": order.long_symbol},
                                    )
                                else:
                                    trading_logger.log_warning(
                                        "Order cancellation failed",
                                        {"broker_order_id": broker_order_id, "api_response": out},
                                    )

                            except Exception as e:
                                context = create_error_context(
                                    broker_order_id=broker_order_id, exch_order_id=order.exch_order_id, error=str(e)
                                )
                                raise OrderError(f"Failed to cancel order: {str(e)}", context)
                        else:
                            trading_logger.log_info(
                                "Order already filled or cannot be cancelled",
                                {
                                    "broker_order_id": broker_order_id,
                                    "fill_size": fills.fill_size if fills else None,
                                    "order_quantity": order.quantity,
                                },
                            )
                    else:
                        trading_logger.log_info(
                            "Order is not from today",
                            {
                                "broker_order_id": broker_order_id,
                                "valid_date": valid_date,
                                "today": dt.datetime.today().strftime("%Y-%m-%d"),
                            },
                        )
                except Exception as e:
                    context = create_error_context(
                        broker_order_id=broker_order_id,
                        order_status=order.status.name if order.status else None,
                        error=str(e),
                    )
                    raise OrderError(f"Error processing order cancellation: {str(e)}", context)
            else:
                trading_logger.log_info(
                    "Order status does not allow cancellation",
                    {"broker_order_id": broker_order_id, "status": order.status.name if order.status else None},
                )

            self.log_and_return(order)
            return order

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(kwargs=kwargs, error=str(e))
            raise OrderError(f"Unexpected error cancelling order: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        internal_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        old_broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        new_broker_order_id=lambda x: ((isinstance(x, str) and len(x.strip()) > 0) or isinstance(x, int)),
    )
    def _update_broker_order_id(self, internal_order_id: str, old_broker_order_id: str, new_broker_order_id: str):
        """
        Update broker order ID in Redis with enhanced error handling.

        Args:
            internal_order_id: Internal order ID
            old_broker_order_id: Old broker order ID
            new_broker_order_id: New broker order ID

        Raises:
            ValidationError: If parameters are invalid
            RedisError: If Redis operations fail
        """
        try:
            trading_logger.log_debug(
                "Updating broker order ID",
                {
                    "internal_order_id": internal_order_id,
                    "old_broker_order_id": old_broker_order_id,
                    "new_broker_order_id": new_broker_order_id,
                },
            )

            # Retrieve the entry keys
            try:
                entry_keys = self.redis_o.hget(internal_order_id, "entry_keys")
            except Exception as e:
                context = create_error_context(internal_order_id=internal_order_id, error=str(e))
                raise RedisError(f"Failed to retrieve entry keys: {str(e)}", context)

            # Initialize new_entry_keys as None
            new_entry_keys = None
            new_exit_keys = None

            # Check if broker_order_id is in entry_keys
            if entry_keys and str(old_broker_order_id) in entry_keys:
                new_entry_keys = entry_keys.replace(str(old_broker_order_id), str(new_broker_order_id))
                trading_logger.log_debug(
                    "Found broker order ID in entry keys",
                    {
                        "internal_order_id": internal_order_id,
                        "old_broker_order_id": old_broker_order_id,
                        "new_broker_order_id": new_broker_order_id,
                    },
                )
            else:
                # Retrieve the exit keys if broker_order_id is not in entry_keys
                try:
                    exit_keys = self.redis_o.hget(internal_order_id, "exit_keys")
                except Exception as e:
                    context = create_error_context(internal_order_id=internal_order_id, error=str(e))
                    raise RedisError(f"Failed to retrieve exit keys: {str(e)}", context)

                if exit_keys and str(old_broker_order_id) in exit_keys:
                    new_exit_keys = exit_keys.replace(str(old_broker_order_id), str(new_broker_order_id))
                    trading_logger.log_debug(
                        "Found broker order ID in exit keys",
                        {
                            "internal_order_id": internal_order_id,
                            "old_broker_order_id": old_broker_order_id,
                            "new_broker_order_id": new_broker_order_id,
                        },
                    )

            # Perform Redis operations only if broker_order_id is found in either entry_keys or exit_keys
            if new_entry_keys is not None or new_exit_keys is not None:
                try:
                    pipe = self.redis_o.pipeline()
                    if new_entry_keys is not None:
                        pipe.hset(internal_order_id, "entry_keys", new_entry_keys)
                    if new_exit_keys is not None:
                        pipe.hset(internal_order_id, "exit_keys", new_exit_keys)
                    pipe.rename(str(old_broker_order_id), str(new_broker_order_id))
                    pipe.execute()
                    pipe.reset()

                    trading_logger.log_info(
                        "Broker order ID updated successfully",
                        {
                            "internal_order_id": internal_order_id,
                            "old_broker_order_id": old_broker_order_id,
                            "new_broker_order_id": new_broker_order_id,
                        },
                    )

                except Exception as e:
                    context = create_error_context(
                        internal_order_id=internal_order_id,
                        old_broker_order_id=old_broker_order_id,
                        new_broker_order_id=new_broker_order_id,
                        error=str(e),
                    )
                    raise RedisError(f"Failed to update broker order ID in Redis: {str(e)}", context)
            else:
                trading_logger.log_warning(
                    "Broker order ID not found in entry or exit keys",
                    {
                        "internal_order_id": internal_order_id,
                        "old_broker_order_id": old_broker_order_id,
                        "entry_keys": entry_keys,
                        "exit_keys": exit_keys if "exit_keys" in locals() else None,
                    },
                )

        except (ValidationError, RedisError):
            raise
        except Exception as e:
            context = create_error_context(
                internal_order_id=internal_order_id,
                old_broker_order_id=old_broker_order_id,
                new_broker_order_id=new_broker_order_id,
                error=str(e),
            )
            raise RedisError(f"Unexpected error updating broker order ID: {str(e)}", context)

    @log_execution_time
    @validate_inputs(broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def get_order_info(self, **kwargs) -> OrderInfo:
        """
        Get order information with enhanced error handling.

        Args:
            **kwargs: Order info parameters including:
                - broker_order_id: Broker order ID to get info for
                - order: Optional Order object

        Returns:
            OrderInfo: Order information object

        Raises:
            ValidationError: If parameters are invalid
            OrderError: If order info retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_debug("Getting order info", {"broker_order_id": kwargs.get("broker_order_id")})

            def return_db_as_fills(order: Order):
                """Return order info from database for historical orders."""
                try:
                    order_info = OrderInfo()
                    valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
                    if valid_date and valid_date != dt.datetime.today().strftime("%Y-%m-%d"):
                        order_info.status = order.status
                    else:
                        order_info.status = OrderStatus.HISTORICAL
                    order_info.order_size = order.quantity
                    order_info.order_price = order.price
                    order_info.fill_size = order.quantity
                    order_info.fill_price = order.price
                    order_info.exchange_order_id = order.exch_order_id
                    order_info.broker = order.broker
                    return order_info
                except Exception as e:
                    trading_logger.log_error("Error in return_db_as_fills", e, {"order": str(order)})
                    raise

            def get_orderinfo_from_orders(exch_order_id: str, order: Order, broker_order_id: str) -> OrderInfo:
                """Get order info from order book."""
                try:
                    orders = pd.DataFrame(self.api.order_book())
                    if len(orders) > 0:
                        fivepaisa_order = orders[orders.BrokerOrderId.astype(str) == str(broker_order_id)]
                        if len(fivepaisa_order) == 1:
                            if fivepaisa_order.OrderStatus.str.lower().str.contains("rejected").item() is True:
                                # order cancelled by broker before reaching exchange
                                trading_logger.log_info(
                                    "Order rejected by broker",
                                    {"broker_order_id": broker_order_id, "reason": str(fivepaisa_order.Reason.item())},
                                )
                                broker_order_id = str(fivepaisa_order.BrokerOrderId.item())
                                try:
                                    internal_order_id = self.redis_o.hget(broker_order_id, "orderRef")
                                    if internal_order_id is not None:
                                        delete_broker_order_id(self, internal_order_id, broker_order_id)
                                except Exception as e:
                                    trading_logger.log_warning(
                                        "Failed to delete broker order ID",
                                        {"broker_order_id": broker_order_id, "error": str(e)},
                                    )
                                return OrderInfo(
                                    order_size=order.quantity,
                                    order_price=order.price,
                                    fill_size=0,
                                    fill_price=0,
                                    status=OrderStatus.REJECTED,
                                    broker_order_id=order.broker_order_id,
                                    exchange_order_id=fivepaisa_order.ExchOrderID.item(),
                                    broker=self.broker,
                                )
                            else:
                                fill_size = fivepaisa_order.TradedQty.item()
                                fill_price = fivepaisa_order.AveragePrice.item()
                                status = OrderStatus.UNDEFINED
                                if "cancel" in fivepaisa_order.OrderStatus.str.lower().item():
                                    status = OrderStatus.CANCELLED
                                elif fill_size == round(float(order.quantity)):
                                    status = OrderStatus.FILLED
                                elif fivepaisa_order.ExchOrderID.item() not in [None, "None", 0, "0"]:
                                    status = OrderStatus.OPEN
                                else:
                                    status = OrderStatus.PENDING
                                if fivepaisa_order.Exch.item() == "M":
                                    try:
                                        long_symbol = (
                                            self.get_long_name_from_broker_identifier(
                                                ScripName=pd.Series[order.long_symbol]
                                            ).item()
                                            if order
                                            else self.get_long_name_from_broker_identifier(
                                                ScripName=fivepaisa_order.ScripName
                                            ).item()
                                        )
                                        contract_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(
                                            long_symbol
                                        )
                                        return OrderInfo(
                                            order_size=order.quantity,
                                            order_price=order.price,
                                            fill_size=fill_size * contract_size,
                                            fill_price=fill_price,
                                            status=status,
                                            broker_order_id=order.broker_order_id,
                                            exchange_order_id=fivepaisa_order.ExchOrderID.item(),
                                            broker=self.broker,
                                        )
                                    except Exception as e:
                                        trading_logger.log_error(
                                            "Error processing MCX order",
                                            e,
                                            {"broker_order_id": broker_order_id, "order": str(order)},
                                        )
                                        raise
                                else:
                                    return OrderInfo(
                                        order_size=order.quantity,
                                        order_price=order.price,
                                        fill_size=fill_size,
                                        fill_price=fill_price,
                                        status=status,
                                        broker_order_id=order.broker_order_id,
                                        exchange_order_id=fivepaisa_order.ExchOrderID.item(),
                                        broker=self.broker,
                                    )
                        else:
                            trading_logger.log_debug(
                                "Found duplicate orders in order book",
                                {"exch_order_id": exch_order_id, "broker_order_id": broker_order_id},
                            )
                            # iterate over order_info
                            status = OrderStatus.UNDEFINED
                            for index, row in fivepaisa_order.iterrows():
                                if row["BrokerOrderId"] == int(broker_order_id):
                                    order_size = row["Qty"]
                                    order_price = row["Rate"]
                                    fill_size = order_size - row["PendingQty"]
                                    fill_price = row["AveragePrice"]
                                    if "cancel" in row["OrderStatus"].lower():
                                        status = OrderStatus.CANCELLED
                                    elif "reject" in row["OrderStatus"].lower():
                                        trading_logger.log_info(
                                            "Order rejected",
                                            {"broker_order_id": broker_order_id, "reason": row["Reason"]},
                                        )
                                        status = OrderStatus.REJECTED
                                    elif fill_size == order_size:
                                        status = OrderStatus.FILLED
                                    elif row["ExchOrderID"] not in [0, "0", None, "None"]:
                                        status = OrderStatus.OPEN
                                    else:
                                        status = OrderStatus.PENDING
                                    if order.exchange == "M":
                                        try:
                                            long_symbol = (
                                                self.get_long_name_from_broker_identifier(
                                                    ScripName=pd.Series[order.long_symbol]
                                                ).item()
                                                if order.long_symbol is not None
                                                else self.get_long_name_from_broker_identifier(
                                                    ScripName=row["ScripName"]
                                                ).item()
                                            )
                                            contract_size = self.exchange_mappings[order.exchange][
                                                "contractsize_map"
                                            ].get(long_symbol)
                                            return OrderInfo(
                                                order_size=order_size,
                                                order_price=order_price,
                                                fill_size=fill_size * contract_size,
                                                fill_price=fill_price,
                                                status=status,
                                                broker_order_id=broker_order_id,
                                                exchange_order_id=row["ExchOrderID"],
                                                broker=self.broker,
                                            )
                                        except Exception as e:
                                            trading_logger.log_error(
                                                "Error processing MCX order from duplicates",
                                                e,
                                                {"broker_order_id": broker_order_id, "order": str(order)},
                                            )
                                            raise
                                    else:
                                        return OrderInfo(
                                            order_size=order_size,
                                            order_price=order_price,
                                            fill_size=fill_size,
                                            fill_price=fill_price,
                                            status=status,
                                            broker_order_id=broker_order_id,
                                            exchange_order_id=row["ExchOrderID"],
                                            broker=self.broker,
                                        )
                    return OrderInfo(
                        order_size=order.quantity,
                        order_price=order.price,
                        fill_size=0,
                        fill_price=0,
                        status=OrderStatus.UNDEFINED,
                        broker_order_id=order.broker_order_id,
                        exchange_order_id=order.exch_order_id,
                        broker=self.broker,
                    )
                except Exception as e:
                    context = create_error_context(
                        exch_order_id=exch_order_id, broker_order_id=broker_order_id, order=str(order), error=str(e)
                    )
                    raise OrderError(f"Error getting order info from orders: {str(e)}", context)

            # Validate mandatory keys
            mandatory_keys = ["broker_order_id"]
            missing_keys = [key for key in mandatory_keys if key not in kwargs]
            if missing_keys:
                context = create_error_context(missing_keys=missing_keys, available_keys=list(kwargs.keys()))
                raise ValidationError(f"Missing mandatory keys: {', '.join(missing_keys)}", context)

            broker_order_id = str(kwargs.get("broker_order_id", "0"))

            # Validate broker_order_id
            if not broker_order_id or broker_order_id == "0":
                context = create_error_context(broker_order_id=broker_order_id)
                raise ValidationError("Invalid broker_order_id", context)

            # Get order from kwargs or Redis
            order = kwargs.get("order", None)
            if order is None:
                try:
                    order_data = self.redis_o.hgetall(broker_order_id)
                    if not order_data:
                        context = create_error_context(broker_order_id=broker_order_id)
                        raise OrderError("Order not found in Redis", context)
                    order = Order(**order_data)
                except Exception as e:
                    context = create_error_context(broker_order_id=broker_order_id, error=str(e))
                    raise OrderError(f"Failed to retrieve order from Redis: {str(e)}", context)

            # Check for historical orders
            try:
                valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
                if (
                    valid_date
                    and valid_date != dt.datetime.today().strftime("%Y-%m-%d")
                    or (order.remote_order_id == "" and order.broker != self.broker)
                ):
                    return return_db_as_fills(order)
            except Exception as e:
                trading_logger.log_warning(
                    "Error checking order date",
                    {"broker_order_id": broker_order_id, "remote_order_id": order.remote_order_id, "error": str(e)},
                )

            # Handle orders with no data in Redis
            if str(order.broker_order_id) == "0":
                return get_orderinfo_from_orders("0", order=order, broker_order_id=broker_order_id)

            # Handle paper trades
            if str(broker_order_id).endswith("P"):
                trading_logger.log_debug("Paper trade being skipped", {"broker_order_id": broker_order_id})
                return OrderInfo(
                    order_size=order.quantity,
                    order_price=order.price,
                    fill_size=order.quantity,
                    fill_price=order.price,
                    status=OrderStatus.FILLED,
                    broker_order_id=order.broker_order_id,
                    broker=self.broker,
                )

            # Check order date
            remote_order_id = order.remote_order_id
            try:
                valid_date, _ = valid_datetime(remote_order_id[:8], "%Y-%m-%d")
                if not (valid_date and valid_date == dt.datetime.today().strftime("%Y-%m-%d")):
                    # we cannot update orders that were placed before today
                    return OrderInfo(
                        order_size=order.quantity,
                        order_price=order.price,
                        fill_size=order.quantity,
                        fill_price=order.price,
                        status=OrderStatus.HISTORICAL,
                        broker_order_id=order.broker_order_id,
                        exchange_order_id=order.exch_order_id,
                        broker=self.broker,
                    )
            except Exception as e:
                trading_logger.log_warning(
                    "Error checking remote order date",
                    {"broker_order_id": broker_order_id, "remote_order_id": remote_order_id, "error": str(e)},
                )

            # Handle orders with no exchange order ID
            if order.exch_order_id in [0, "0", None, "None"]:
                if "reject" in order.message.lower() or "cancel" in order.message.lower():
                    fills = OrderInfo()
                    if "reject" in order.message.lower():
                        fills.status = OrderStatus.REJECTED
                    if "cancel" in order.message.lower():
                        fills.status = OrderStatus.CANCELLED
                    fills.fill_price = 0
                    fills.fill_size = 0
                    fills.broker = self.broker.name
                    fills.broker_order_id = order.broker_order_id
                    fills.order_price = order.price
                    fills.order_size = order.quantity
                    return fills
                return get_orderinfo_from_orders("0", order=order, broker_order_id=broker_order_id)
            else:
                # Handle orders with exchange order ID
                exchange = order.exchange
                if exchange == "M":
                    if order.exch_order_id not in ["0", "None", 0, None]:
                        return get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                else:
                    try:
                        long_symbol = order.long_symbol
                        exch = self.exchange_mappings[order.exchange]["exchange_map"].get(long_symbol)
                        req_list_ = [
                            {
                                "Exch": exch,
                                "ExchOrderID": order.exch_order_id,
                                "ExchType": order.exchange_segment,
                                "ScripCode": order.scrip_code,
                            }
                        ]
                        trade_info = self.api.fetch_trade_info(req_list_)
                        if trade_info is None:
                            return get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                        trade_details = trade_info["TradeDetail"]
                        if len(trade_details) > 0:
                            price = [trade["Qty"] * trade["Rate"] for trade in trade_details]
                            filled = [trade["Qty"] for trade in trade_details]
                            price = np.sum(price)
                            fill_size = np.sum(filled)
                            fill_price = price / fill_size
                            status = OrderStatus.UNDEFINED
                            if str(trade_info["Status"]) == "Cancelled":
                                status = OrderStatus.CANCELLED
                            elif fill_size == round(float(order.quantity)):
                                status = OrderStatus.FILLED
                            elif trade_details[0].get("ExchOrderID") not in [None, "None", 0, "0"]:
                                status = OrderStatus.OPEN
                            else:
                                status = OrderStatus.PENDING
                            trading_logger.log_debug(
                                "Trade details processed",
                                {
                                    "broker_order_id": broker_order_id,
                                    "fill_size": fill_size,
                                    "fill_price": fill_price,
                                    "status": status.name if status else None,
                                },
                            )
                            return OrderInfo(
                                order_size=order.quantity,
                                order_price=order.price,
                                fill_size=fill_size,
                                fill_price=fill_price,
                                status=status,
                                broker_order_id=order.broker_order_id,
                                exchange_order_id=trade_details[0].get("ExchOrderID"),
                                broker=self.broker,
                            )
                        else:
                            return get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                    except Exception as e:
                        context = create_error_context(
                            broker_order_id=broker_order_id,
                            exchange=exchange,
                            long_symbol=order.long_symbol,
                            error=str(e),
                        )
                        raise OrderError(f"Error processing trade info: {str(e)}", context)

            trading_logger.log_warning(
                "No valid return path found for order info", {"broker_order_id": broker_order_id}
            )
            return OrderInfo(
                order_size=0,
                order_price=0,
                fill_size=0,
                fill_price=0,
                status=OrderStatus.UNDEFINED,
                broker_order_id="",
                exchange_order_id="",
                broker=self.broker,
            )

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(kwargs=kwargs, error=str(e))
            raise OrderError(f"Unexpected error getting order info: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        symbols=lambda x: x is not None,
        date_start=lambda x: valid_datetime(x)[0] is not False,
        date_end=lambda x: valid_datetime(x)[0] is not False,
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
        Retrieves historical bars from FivePaisa with enhanced error handling.

        Args:
            symbols: If dataframe is provided, it needs to contain columns [long_symbol, Scripcode].
                If dict is provided, it needs to contain (long_symbol, scrip_code, exch, exch_type). Else symbol long_name.
            date_start: Start date (can be string, datetime, or date object).
            date_end: End date (can be string, datetime, or date object).
            exchange: Exchange name. Defaults to "N".
            periodicity: Defaults to '1m'.
            market_close_time: Defaults to '15:30:00'. Only historical data with timestamp less than market_close_time is returned.
            refresh_mapping: If True, load symbol mapping from date_end's symbols CSV file instead of using cached mapping.
                Defaults to False.

        Returns:
            Dict[str, List[HistoricalData]]: Dictionary with historical data for each symbol.

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If historical data retrieval fails
            BrokerConnectionError: If broker connection issues
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
                    symbol_codes_path = config.get("FIVEPAISA.SYMBOLCODES")
                    if not symbol_codes_path:
                        context = create_error_context(broker_name=self.broker.name)
                        raise ConfigurationError("FIVEPAISA.SYMBOLCODES path not found in config", context)
                    
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
            try:
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
                        trading_logger.log_warning(
                            "ScripCode not found for symbol", {"symbol": symbols, "exchange": exchange}
                        )
                        return {}
                elif isinstance(symbols, dict):
                    scripCode = symbols.get("scrip_code")
                    if scripCode:
                        symbols_pd = pd.DataFrame([{"long_symbol": symbols.get("long_symbol"), "Scripcode": scripCode}])
                    else:
                        trading_logger.log_warning("ScripCode not found in dict", {"symbols": symbols})
                        return {}
                else:
                    symbols_pd = symbols
            except Exception as e:
                context = create_error_context(symbols_type=type(symbols).__name__, symbols=str(symbols), error=str(e))
                raise MarketDataError(f"Error processing symbols: {str(e)}", context)

            out = {}  # Initialize the output dictionary

            for index, row_outer in symbols_pd.iterrows():
                try:
                    trading_logger.log_debug(
                        "Processing symbol for historical data",
                        {"index": index, "symbol_count": len(symbols_pd), "long_symbol": row_outer["long_symbol"]},
                    )

                    exchange = self.map_exchange_for_api(row_outer["long_symbol"], exchange)
                    historical_data_list = []
                    exch = exchange
                    exch_type = (
                        symbols.get("exch_type")
                        if isinstance(symbols, dict)
                        else (
                            refresh_exchangetype_map[exchange].get(row_outer["long_symbol"])
                            if refresh_exchangetype_map and exchange in refresh_exchangetype_map
                            else self.exchange_mappings[exchange]["exchangetype_map"].get(row_outer["long_symbol"])
                        )
                    )

                    s = row_outer["long_symbol"].replace("/", "-")
                    row_outer["long_symbol"] = "NSENIFTY" + s[s.find("_") :] if s.startswith("NIFTY_") else s

                    try:
                        date_start_str, _ = valid_datetime(date_start, "%Y-%m-%d")
                        date_end_str, _ = valid_datetime(date_end, "%Y-%m-%d")
                    except Exception as e:
                        trading_logger.log_error(
                            "Error validating dates", e, {"date_start": date_start, "date_end": date_end}
                        )
                        continue

                    try:
                        data = self.api.historical_data(
                            exch, exch_type, row_outer["Scripcode"], periodicity, date_start_str, date_end_str
                        )
                    except Exception as e:
                        trading_logger.log_error(
                            "Error fetching historical data from API",
                            e,
                            {
                                "exch": exch,
                                "exch_type": exch_type,
                                "scripcode": row_outer["Scripcode"],
                                "periodicity": periodicity,
                                "date_start": date_start_str,
                                "date_end": date_end_str,
                            },
                        )
                        continue

                    if not (data is None or len(data) == 0):
                        try:
                            data.columns = ["date", "open", "high", "low", "close", "volume"]
                            data["date"] = pd.to_datetime(data["date"])
                            data["date"] = data["date"].dt.tz_localize("Asia/Kolkata")
                            if "m" in periodicity:
                                market_open = pd.to_datetime(market_open_time).time()
                                market_close = pd.to_datetime(market_close_time).time()
                                data = data[
                                    (data["date"].dt.time >= market_open) & (data["date"].dt.time < market_close)
                                ]
                            # Ensure date has time set to 00:00:00 for 'd', 'w', or 'm' periodicity
                            if any(period in periodicity for period in ["d"]):
                                data["date"] = data["date"].dt.floor("D")

                            for _, row in data.iterrows():
                                historical_data = HistoricalData(
                                    date=row.get("date"),
                                    open=row.get("open"),
                                    high=row.get("high"),
                                    low=row.get("low"),
                                    close=row.get("close"),
                                    volume=row.get("volume"),
                                    intoi=row.get("intoi"),
                                    oi=row.get("oi"),
                                )
                                historical_data_list.append(historical_data)

                            trading_logger.log_debug(
                                "Historical data processed successfully",
                                {"symbol": row_outer["long_symbol"], "data_points": len(historical_data_list)},
                            )

                        except Exception as e:
                            trading_logger.log_error(
                                "Error processing historical data",
                                e,
                                {
                                    "symbol": row_outer["long_symbol"],
                                    "data_shape": data.shape if data is not None else None,
                                },
                            )
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
                    else:
                        trading_logger.log_debug("No data found for symbol", {"symbol": row_outer["long_symbol"]})
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

                    out[row_outer["long_symbol"]] = historical_data_list

                except Exception as e:
                    trading_logger.log_error(
                        "Error processing symbol",
                        e,
                        {
                            "index": index,
                            "long_symbol": row_outer.get("long_symbol", "unknown"),
                            "symbols_count": len(symbols_pd),
                        },
                    )
                    continue

            trading_logger.log_debug(
                "Historical data retrieval completed", {"symbols_processed": len(out), "total_symbols": len(symbols_pd)}
            )

            return out

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(
                symbols_type=type(symbols).__name__,
                date_start=date_start,
                date_end=date_end,
                exchange=exchange,
                error=str(e),
            )
            raise MarketDataError(f"Unexpected error getting historical data: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        bar_duration=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        adj=lambda x: isinstance(x, bool),
    )
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_close(
        self,
        long_symbol: str,
        exchange="N",
        bar_duration="1m",
        timestamp: dt.datetime = dt.datetime(1970, 1, 1),
        adj=False,
    ) -> float:
        """
        Get last traded price from FivePaisa with enhanced error handling.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name. Defaults to "N".
            bar_duration: Bar duration. Defaults to "1m".
            timestamp: Formatted as %Y-%m-%d%T%H:%M:%S. Defaults to None and then provides the last traded price.
            adj: If True, provide hlc3 for the shortlisted bar. Defaults to False.

        Returns:
            float: Last traded price on or before timestamp. NaN if no price found.

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If price retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_info(
                "Getting close price",
                {
                    "long_symbol": long_symbol,
                    "exchange": exchange,
                    "bar_duration": bar_duration,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "adj": adj,
                },
            )

            try:
                exchange = self.map_exchange_for_api(long_symbol, exchange)
            except Exception as e:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
                raise MarketDataError(f"Failed to map exchange: {str(e)}", context)

            # Process timestamp
            try:
                if timestamp is None:
                    timestamp = dt.datetime.now()
                elif isinstance(timestamp, str):
                    timestamp, _ = valid_datetime(timestamp)
                cutoff_time = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                context = create_error_context(timestamp=timestamp, error=str(e))
                raise MarketDataError(f"Error processing timestamp: {str(e)}", context)

            # Get exchange mappings
            try:
                exch_type = self.exchange_mappings[exchange]["exchangetype_map"].get(long_symbol)
                scrip_code = self.exchange_mappings["N"]["symbol_map"].get(long_symbol)
            except KeyError as e:
                context = create_error_context(
                    exchange=exchange, long_symbol=long_symbol, available_exchanges=list(self.exchange_mappings.keys())
                )
                raise MarketDataError(f"Exchange mapping not found: {str(e)}", context)

            if not scrip_code:
                trading_logger.log_warning(
                    "Scrip code not found for symbol", {"long_symbol": long_symbol, "exchange": exchange}
                )
                return float("nan")

            # Fetch historical data
            try:
                md = self.api.historical_data(
                    exchange,
                    exch_type,
                    scrip_code,
                    bar_duration,
                    (timestamp - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                    cutoff_time,
                )
            except Exception as e:
                context = create_error_context(
                    exchange=exchange,
                    exch_type=exch_type,
                    scrip_code=scrip_code,
                    bar_duration=bar_duration,
                    error=str(e),
                )
                raise MarketDataError(f"Failed to fetch historical data: {str(e)}", context)

            if md is not None and len(md) > 0:
                try:
                    md = md.loc[md.Datetime <= cutoff_time,].tail(1).squeeze()

                    if adj:
                        result = (md.High + md.Low + md.Close) / 3
                        trading_logger.log_debug(
                            "Adjusted close price calculated",
                            {
                                "long_symbol": long_symbol,
                                "high": md.High,
                                "low": md.Low,
                                "close": md.Close,
                                "result": result,
                            },
                        )
                    else:
                        result = md.Close
                        trading_logger.log_debug("Close price retrieved", {"long_symbol": long_symbol, "close": result})

                    return result

                except Exception as e:
                    trading_logger.log_error(
                        "Error processing market data",
                        e,
                        {"long_symbol": long_symbol, "data_shape": md.shape if md is not None else None},
                    )
                    return float("nan")
            else:
                trading_logger.log_warning(
                    "No market data found", {"long_symbol": long_symbol, "exchange": exchange, "timestamp": cutoff_time}
                )
                return float("nan")

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(
                long_symbol=long_symbol,
                exchange=exchange,
                bar_duration=bar_duration,
                timestamp=timestamp.isoformat() if timestamp else None,
                error=str(e),
            )
            raise MarketDataError(f"Unexpected error getting close price: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def map_exchange_for_api(self, long_symbol, exchange):
        """
        Map exchange for API calls with enhanced error handling.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            str: Mapped exchange for API calls

        Raises:
            ValidationError: If parameters are invalid
        """
        try:
            trading_logger.log_debug("Mapping exchange for API", {"long_symbol": long_symbol, "exchange": exchange})

            if not exchange or len(exchange) == 0:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange)
                raise ValidationError("Exchange cannot be empty", context)

            result = exchange[0]

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

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def map_exchange_for_db(self, long_symbol, exchange):
        """
        Map exchange for database operations with enhanced error handling.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            str: Mapped exchange for database operations

        Raises:
            ValidationError: If parameters are invalid
        """
        try:
            trading_logger.log_debug("Mapping exchange for DB", {"long_symbol": long_symbol, "exchange": exchange})

            if not exchange or len(exchange) == 0:
                context = create_error_context(long_symbol=long_symbol, exchange=exchange)
                raise ValidationError("Exchange cannot be empty", context)

            if exchange[0] == "N":
                result = "NSE"
            elif exchange[0] == "B":
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

    @log_execution_time
    @validate_inputs(date_string=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def convert_to_ist(self, date_string):
        """
        Convert a string in the format '/Date(1732010010000)/' to IST date and time with enhanced error handling.

        Args:
            date_string: The string containing the date in /Date(milliseconds)/ format.

        Returns:
            str: The corresponding date and time in IST (yyyy-mm-dd hh:mm:ss).

        Raises:
            ValidationError: If parameters are invalid
            DataError: If date conversion fails
        """
        try:
            trading_logger.log_debug("Converting to IST", {"date_string": date_string})

            if not date_string or not isinstance(date_string, str):
                context = create_error_context(date_string=date_string, date_string_type=type(date_string))
                raise ValidationError("Invalid date string", context)

            # Extract the timestamp using regex
            match = re.search(r"/Date\((\d+)\)/", date_string)
            if not match:
                trading_logger.log_warning(
                    "Invalid date format", {"date_string": date_string, "expected_format": "/Date(milliseconds)/"}
                )
                return get_tradingapi_now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                # Convert the timestamp from milliseconds to seconds
                timestamp_ms = int(match.group(1))
                timestamp_s = timestamp_ms / 1000

                # Convert to UTC datetime
                utc_time = dt.datetime.fromtimestamp(timestamp_s, tz=dt.timezone.utc)

                # Convert to IST by adding 5 hours and 30 minutes
                ist_time = utc_time + dt.timedelta(hours=5, minutes=30)

                # Format the IST datetime
                result = ist_time.strftime("%Y-%m-%d %H:%M:%S")

                trading_logger.log_debug(
                    "Date converted to IST successfully",
                    {"date_string": date_string, "timestamp_ms": timestamp_ms, "ist_time": result},
                )

                return result

            except (ValueError, OverflowError) as e:
                context = create_error_context(
                    date_string=date_string,
                    timestamp_ms=timestamp_ms if "timestamp_ms" in locals() else None,
                    error=str(e),
                )
                raise DataError(f"Error converting timestamp: {str(e)}", context)

        except (ValidationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(date_string=date_string, error=str(e))
            raise DataError(f"Unexpected error converting to IST: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """
        Get quote details of a symbol with enhanced error handling.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name. Defaults to "NSE".

        Returns:
            Price: Quote details.

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If quote retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_debug("Fetching quote", {"long_symbol": long_symbol, "exchange": exchange})

            mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
            market_feed = Price()  # Initialize with default values
            market_feed.src = "fp"
            market_feed.symbol = long_symbol

            try:
                exch_type = self.exchange_mappings[mapped_exchange]["exchangetype_map"].get(long_symbol)
                scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
            except KeyError as e:
                context = create_error_context(
                    mapped_exchange=mapped_exchange,
                    long_symbol=long_symbol,
                    available_exchanges=list(self.exchange_mappings.keys()),
                )
                raise MarketDataError(f"Exchange mapping not found: {str(e)}", context)

            if scrip_code is None:
                context = create_error_context(
                    long_symbol=long_symbol,
                    mapped_exchange=mapped_exchange,
                    available_symbols=list(self.exchange_mappings[mapped_exchange]["symbol_map"].keys()),
                )
                trading_logger.log_warning("No scrip code found for symbol", context)
                return market_feed  # Return default Price object if no scrip code is found

            req_list = [
                {"Exch": mapped_exchange, "ExchType": exch_type, "ScripCode": scrip_code},
            ]

            try:
                # Fetch market feed
                out = self.api.fetch_market_feed_scrip(req_list)
                if not out or "Data" not in out or len(out["Data"]) == 0:
                    context = create_error_context(
                        long_symbol=long_symbol,
                        mapped_exchange=mapped_exchange,
                        scrip_code=scrip_code,
                        api_response=out,
                    )
                    raise MarketDataError("No market feed data received", context)

                snapshot = out["Data"][0]

                # Fetch market depth
                out = self.api.fetch_market_depth_by_scrip(
                    Exchange=mapped_exchange, ExchangeType=exch_type, ScripCode=scrip_code
                )

                if not out or "MarketDepthData" not in out:
                    context = create_error_context(
                        long_symbol=long_symbol,
                        mapped_exchange=mapped_exchange,
                        scrip_code=scrip_code,
                        api_response=out,
                    )
                    raise MarketDataError("No market depth data received", context)

                market_depth_data = out["MarketDepthData"]
                bids = [entry for entry in market_depth_data if entry["BbBuySellFlag"] == 66]
                asks = [entry for entry in market_depth_data if entry["BbBuySellFlag"] == 83]

                # Get the best bid and best ask
                best_bid = max(bids, key=lambda x: x["Price"]) if bids else None
                best_ask = min(asks, key=lambda x: x["Price"]) if asks else None

                # Extract prices and quantities
                best_bid_price = best_bid["Price"] if best_bid else None
                best_bid_quantity = best_bid["Quantity"] if best_bid else None

                best_ask_price = best_ask["Price"] if best_ask else None
                best_ask_quantity = best_ask["Quantity"] if best_ask else None

                # Update market_feed with fetched data
                market_feed.ask = (
                    best_ask_price if best_ask_price is not None and best_ask_price != 0 else market_feed.ask
                )
                market_feed.bid = (
                    best_bid_price if best_bid_price is not None and best_bid_price != 0 else market_feed.bid
                )
                market_feed.bid_volume = (
                    best_bid_quantity
                    if best_bid_quantity is not None and best_bid_quantity > 0
                    else market_feed.bid_volume
                )
                market_feed.ask_volume = (
                    best_ask_quantity
                    if best_ask_quantity is not None and best_ask_quantity > 0
                    else market_feed.ask_volume
                )
                market_feed.exchange = snapshot["Exch"]
                market_feed.high = snapshot["High"] if snapshot["High"] > 0 else market_feed.high
                market_feed.low = snapshot["Low"] if snapshot["Low"] > 0 else market_feed.low
                market_feed.last = snapshot["LastRate"] if snapshot["LastRate"] > 0 else market_feed.last
                market_feed.prior_close = snapshot["PClose"] if snapshot["PClose"] > 0 else market_feed.prior_close
                market_feed.volume = snapshot["TotalQty"] if snapshot["TotalQty"] > 0 else market_feed.volume
                market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                trading_logger.log_debug(
                    "Quote fetched successfully",
                    {
                        "long_symbol": long_symbol,
                        "bid": market_feed.bid,
                        "ask": market_feed.ask,
                        "last": market_feed.last,
                        "volume": market_feed.volume,
                    },
                )

            except Exception as e:
                context = create_error_context(
                    long_symbol=long_symbol, mapped_exchange=mapped_exchange, scrip_code=scrip_code, error=str(e)
                )
                trading_logger.log_error("Error fetching quote", e, context)
                raise MarketDataError(f"Error fetching quote for symbol {long_symbol}: {str(e)}", context)

            return market_feed

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e))
            raise MarketDataError(f"Unexpected error fetching quote: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        operation=lambda x: isinstance(x, str) and x in ["s", "u"],
        symbols=lambda x: isinstance(x, list) and len(x) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def start_quotes_streaming(self, operation: str, symbols=List[str], ext_callback=None, exchange="NSE"):
        """
        Start quotes streaming with enhanced error handling.

        Args:
            operation: Operation type ("s" for subscribe, "u" for unsubscribe)
            symbols: List of symbols to stream
            ext_callback: External callback function
            exchange: Exchange name. Defaults to "NSE".

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If streaming fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_info(
                "Starting quotes streaming",
                {"operation": operation, "symbols_count": len(symbols), "exchange": exchange},
            )

            if not symbols or len(symbols) == 0:
                context = create_error_context(symbols=symbols)
                raise ValidationError("Symbols list cannot be empty", context)

            try:
                mapped_exchange = self.map_exchange_for_api(symbols[0], exchange)
            except Exception as e:
                context = create_error_context(symbols=symbols, exchange=exchange, error=str(e))
                raise MarketDataError(f"Failed to map exchange: {str(e)}", context)

            def map_to_price(json_data):
                """Map JSON data to Price object."""
                try:
                    price = Price()
                    price.src = "fp"
                    price.bid = json_data.get("BidRate", float("nan"))
                    price.ask = json_data.get("OffRate", float("nan"))
                    price.bid_volume = json_data.get("BidQty", float("nan"))
                    price.ask_volume = json_data.get("OffQty", float("nan"))
                    price.last = json_data.get("LastRate", float("nan"))
                    price.prior_close = json_data.get("PClose", float("nan"))
                    price.high = json_data.get("High", float("nan"))
                    price.low = json_data.get("Low", float("nan"))
                    price.volume = json_data.get("TotalQty", float("nan"))
                    price.symbol = self.exchange_mappings[json_data["Exch"]]["symbol_map_reversed"].get(
                        json_data.get("Token")
                    )
                    price.exchange = self.map_exchange_for_db(price.symbol, json_data["Exch"])
                    price.timestamp = self.convert_to_ist(json_data["TickDt"])
                    return price
                except Exception as e:
                    trading_logger.log_error("Error mapping price data", e, {"json_data": json_data})
                    raise

            def on_message(ws, message):
                """Handle incoming WebSocket messages."""
                try:
                    data_str = message.replace("\/", "/")
                    json_data = json.loads(data_str)
                    if len(json_data) == 1:
                        price = map_to_price(json_data[0])
                        if ext_callback:
                            ext_callback(price)
                except Exception as e:
                    trading_logger.log_error("Error processing WebSocket message", e, {"message": message})

            def error_data(ws, err):
                """Handle WebSocket errors."""
                try:
                    trading_logger.log_error("WebSocket error", {"error": str(err)})
                    reconnect()
                except Exception as e:
                    trading_logger.log_error("Error handling WebSocket error", e, {"original_error": str(err)})

            def reconnect():
                """Reconnect to WebSocket."""
                trading_logger.log_info("Attempting to reconnect...")
                time.sleep(5)  # Wait for a few seconds before reconnecting
                try:
                    req_data = expand_symbols_to_request(
                        self.subscribed_symbols
                    )  # Store req_data for reconnection purposes
                    connect_and_receive(req_data)
                except Exception as e:
                    trading_logger.log_error("Reconnection failed", e, {"subscribed_symbols": self.subscribed_symbols})
                    reconnect()

            def connect_and_receive(req_data):
                """Connect and receive data."""
                try:
                    self.api.connect(req_data)
                    self.api.receive_data(on_message)
                    self.api.error_data(error_data)
                except Exception as e:
                    trading_logger.log_error("Error in connect_and_receive", e, {"req_data": req_data})
                    raise

            def expand_symbols_to_request(symbols: list):
                """Expand symbols to request format."""
                try:
                    req_list = []
                    for long_symbol in symbols:
                        market_feed = Price()
                        market_feed.src = "fp"
                        market_feed.symbol = long_symbol
                        exch_type = self.exchange_mappings[mapped_exchange]["exchangetype_map"].get(long_symbol)
                        scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
                        if scrip_code is None:
                            trading_logger.log_warning(
                                "Scrip code not found for symbol",
                                {"long_symbol": long_symbol, "mapped_exchange": mapped_exchange},
                            )
                            continue
                        req_list.append({"Exch": mapped_exchange, "ExchType": exch_type, "ScripCode": scrip_code})
                    return req_list
                except Exception as e:
                    trading_logger.log_error(
                        "Error expanding symbols to request",
                        e,
                        {"symbols": symbols, "mapped_exchange": mapped_exchange},
                    )
                    raise

            def update_current_subscriptions(operation, symbols):
                """Update current subscriptions."""
                try:
                    if operation == "s":
                        self.subscribed_symbols.extend(symbols)
                        trading_logger.log_info(
                            "Symbols subscribed", {"symbols": symbols, "total_subscribed": len(self.subscribed_symbols)}
                        )
                    elif operation == "u":
                        self.subscribed_symbols = list(set(self.subscribed_symbols) - set(symbols))
                        trading_logger.log_info(
                            "Symbols unsubscribed",
                            {"symbols": symbols, "total_subscribed": len(self.subscribed_symbols)},
                        )
                except Exception as e:
                    trading_logger.log_error(
                        "Error updating subscriptions", e, {"operation": operation, "symbols": symbols}
                    )
                    raise

            try:
                update_current_subscriptions(operation, symbols)
                req_list = expand_symbols_to_request(symbols)

                if req_list is not None and len(req_list) > 0:
                    req_data = self.api.Request_Feed("mf", operation, req_list)

                    # Start the connection and receiving data in a separate thread
                    if self.subscribe_thread is None:
                        self.subscribe_thread = threading.Thread(
                            target=connect_and_receive, args=(req_data,), name="MarketDataStreamer"
                        )
                        self.subscribe_thread.start()
                        time.sleep(2)
                        trading_logger.log_info("Streaming thread started", {"thread_name": "MarketDataStreamer"})
                    else:
                        trading_logger.log_info(
                            "Requesting streaming for existing connection", {"req_data": json.dumps(req_data)}
                        )
                        self.api.ws.send(json.dumps(req_data))
                else:
                    trading_logger.log_warning(
                        "No valid request list generated", {"symbols": symbols, "req_list": req_list}
                    )

            except Exception as e:
                context = create_error_context(operation=operation, symbols=symbols, exchange=exchange, error=str(e))
                raise MarketDataError(f"Error starting quotes streaming: {str(e)}", context)

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(operation=operation, symbols=symbols, exchange=exchange, error=str(e))
            raise MarketDataError(f"Unexpected error starting quotes streaming: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def stop_streaming(self):
        """
        Stop quotes streaming with enhanced error handling.

        Raises:
            BrokerConnectionError: If stopping streaming fails
        """
        try:
            trading_logger.log_info("Stopping quotes streaming")

            try:
                self.api.close_data()
                trading_logger.log_info("Streaming stopped successfully")
            except Exception as e:
                context = create_error_context(error=str(e))
                raise BrokerConnectionError(f"Failed to stop streaming: {str(e)}", context)

        except BrokerConnectionError:
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise BrokerConnectionError(f"Unexpected error stopping streaming: {str(e)}", context)

    @log_execution_time
    @validate_inputs(long_symbol=lambda x: x is None or (isinstance(x, str) and len(x.strip()) >= 0))
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_position(self, long_symbol: str = "") -> Union[pd.DataFrame, int]:
        """
        Retrieves position from FivePaisa with enhanced error handling.

        Args:
            long_symbol: Symbol name. Defaults to "" and returns all positions

        Returns:
            Union[pd.DataFrame, int]: Signed position if long_symbol is not None.
            Else dataframe containing all positions

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If position retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_debug("Getting position", {"long_symbol": long_symbol if long_symbol else "all"})

            # Get holdings data
            try:
                holding = pd.DataFrame(self.api.holdings())
                holding = pd.DataFrame(columns=["long_symbol", "quantity"]) if len(holding) == 0 else holding
                if len(holding) > 0:
                    try:
                        holding["long_symbol"] = self.get_long_name_from_broker_identifier(ScripName=holding.Symbol)
                        holding = holding.loc[:, ["long_symbol", "Quantity"]]
                        holding.columns = ["long_symbol", "quantity"]
                    except Exception as e:
                        trading_logger.log_error("Error processing holdings data", e, {"holdings_count": len(holding)})
                        holding = pd.DataFrame(columns=["long_symbol", "quantity"])
            except Exception as e:
                context = create_error_context(error=str(e))
                raise MarketDataError(f"Failed to retrieve holdings: {str(e)}", context)

            # Get positions data
            try:
                position = pd.DataFrame(self.api.positions())
                position = pd.DataFrame(columns=["long_symbol", "quantity"]) if len(position) == 0 else position
                if len(position) > 0:
                    try:
                        position["long_symbol"] = self.get_long_name_from_broker_identifier(
                            ScripName=position.ScripName
                        )
                        position = position.loc[:, ["long_symbol", "NetQty"]]
                        position.columns = ["long_symbol", "quantity"]
                    except Exception as e:
                        trading_logger.log_error(
                            "Error processing positions data", e, {"positions_count": len(position)}
                        )
                        position = pd.DataFrame(columns=["long_symbol", "quantity"])
            except Exception as e:
                context = create_error_context(error=str(e))
                raise MarketDataError(f"Failed to retrieve positions: {str(e)}", context)

            # Merge holdings and positions
            try:
                merged_df = pd.merge(position, holding, on="long_symbol", how="outer")
                result = merged_df.groupby("long_symbol", as_index=False).agg(
                    {"quantity_x": "sum", "quantity_y": "sum"}
                )
                result["quantity"] = result.quantity_y + result.quantity_x
                result = result.loc[:, ["long_symbol", "quantity"]]

                trading_logger.log_debug(
                    "Position data processed",
                    {"holdings_count": len(holding), "positions_count": len(position), "merged_count": len(result)},
                )

            except Exception as e:
                context = create_error_context(holdings_count=len(holding), positions_count=len(position), error=str(e))
                raise MarketDataError(f"Failed to merge position data: {str(e)}", context)

            # Return appropriate result
            if long_symbol is None or long_symbol == "":
                trading_logger.log_debug("Returning all positions", {"position_count": len(result)})
                return result
            else:
                try:
                    pos = result.loc[result.long_symbol == long_symbol, "quantity"]
                    if len(pos) == 0:
                        trading_logger.log_debug("No position found for symbol", {"long_symbol": long_symbol})
                        return 0
                    elif len(pos) == 1:
                        position_value = pos.item()
                        trading_logger.log_debug(
                            "Position retrieved for symbol", {"long_symbol": long_symbol, "quantity": position_value}
                        )
                        return position_value
                    else:
                        context = create_error_context(long_symbol=long_symbol, position_count=len(pos))
                        raise MarketDataError(f"Multiple positions found for symbol: {long_symbol}", context)
                except Exception as e:
                    if isinstance(e, MarketDataError):
                        raise
                    context = create_error_context(long_symbol=long_symbol, result_data=str(result), error=str(e))
                    raise MarketDataError(f"Error retrieving position for symbol: {str(e)}", context)

        except (ValidationError, MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(long_symbol=long_symbol, error=str(e))
            raise MarketDataError(f"Unexpected error getting position: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        """
        Get today's orders with enhanced error handling.

        Returns:
            pd.DataFrame: DataFrame containing today's orders or None if no orders

        Raises:
            MarketDataError: If order retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_debug("Getting orders for today")

            try:
                orders = self.api.order_book()
                orders = pd.DataFrame.from_dict(orders)

                if len(orders.index) > 0:
                    try:
                        orders = orders.assign(
                            long_symbol=self.get_long_name_from_broker_identifier(ScripName=orders.ScripName)
                        )
                        trading_logger.log_debug("Orders retrieved successfully", {"order_count": len(orders)})
                        return orders
                    except Exception as e:
                        trading_logger.log_error("Error processing orders data", e, {"order_count": len(orders)})
                        return None
                else:
                    trading_logger.log_debug("No orders found for today")
                    return None

            except Exception as e:
                context = create_error_context(error=str(e))
                raise MarketDataError(f"Failed to retrieve orders: {str(e)}", context)

        except (MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise MarketDataError(f"Unexpected error getting orders: {str(e)}", context)

    @log_execution_time
    @retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        """
        Get today's trades with enhanced error handling.

        Returns:
            pd.DataFrame: DataFrame containing today's trades or None if no trades

        Raises:
            MarketDataError: If trade retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_debug("Getting trades for today")

            try:
                trades = self.api.get_tradebook()
                trades = pd.DataFrame.from_dict(trades)

                if len(trades) > 0:
                    try:
                        trades = trades.loc[trades.Status == 0, "TradeBookDetail"]
                        trades = pd.DataFrame([trade for trade in trades])
                        trades.ExchangeTradeTime = trades.ExchangeTradeTime.apply(self._convert_date_string)
                        trades = trades.assign(
                            long_symbol=self.get_long_name_from_broker_identifier(ScripName=trades.ScripName)
                        )

                        trading_logger.log_debug("Trades retrieved successfully", {"trade_count": len(trades)})
                        return trades
                    except Exception as e:
                        trading_logger.log_error("Error processing trades data", e, {"trade_count": len(trades)})
                        return None
                else:
                    trading_logger.log_debug("No trades found for today")
                    return None

            except Exception as e:
                context = create_error_context(error=str(e))
                raise MarketDataError(f"Failed to retrieve trades: {str(e)}", context)

        except (MarketDataError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise MarketDataError(f"Unexpected error getting trades: {str(e)}", context)

    @log_execution_time
    @validate_inputs(ScripName=lambda x: isinstance(x, pd.Series) and len(x) > 0)
    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """
        Generates Long Name with enhanced error handling.

        Args:
            kwargs: Arbitrary keyword arguments. Expected key:
                - ScripName (pd.Series): position.ScripName from 5paisa position.

        Returns:
            pd.Series: Long name.

        Raises:
            ValidationError: If parameters are invalid
            DataError: If data processing fails
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

            try:
                ScripName = ScripName.reset_index(drop=True)
                symbol = ScripName.str.split().str[0]
                sec_type = pd.Series("", index=np.arange(len(ScripName)))
                expiry = pd.Series("", index=np.arange(len(ScripName)))
                right = pd.Series("", index=np.arange(len(ScripName)))
                strike = pd.Series("", index=np.arange(len(ScripName)))

                type_map = {
                    6: "OPT",
                    4: "FUT",
                    1: "STK",
                }
                sec_types = ScripName.str.split().str.len().map(type_map)

                for idx, sec_type in enumerate(sec_types):
                    try:
                        if sec_type == "OPT":
                            expiry.iloc[idx] = pd.to_datetime(("-").join(ScripName[idx].split()[1:4])).strftime(
                                "%Y%m%d"
                            )
                            right_map = {
                                "PE": "PUT",
                                "CE": "CALL",
                            }
                            right.iloc[idx] = right_map.get(ScripName[idx].split()[4])
                            strike.iloc[idx] = ScripName[idx].split()[5].strip("0").strip(".")
                        elif sec_type == "FUT":
                            expiry.iloc[idx] = pd.to_datetime(("-").join(ScripName[idx].split()[1:4])).strftime(
                                "%Y%m%d"
                            )
                    except Exception as e:
                        trading_logger.log_warning(
                            "Error processing scrip name",
                            {
                                "index": idx,
                                "scrip_name": ScripName[idx] if idx < len(ScripName) else None,
                                "error": str(e),
                            },
                        )

                result = symbol + "_" + sec_types + "_" + expiry + "_" + right + "_" + strike

                trading_logger.log_debug(
                    "Long name generated successfully", {"input_count": len(ScripName), "output_count": len(result)}
                )

                return result

            except Exception as e:
                context = create_error_context(
                    scrip_name_sample=ScripName.head(3).tolist() if len(ScripName) > 0 else None, error=str(e)
                )
                raise DataError(f"Error processing scrip names: {str(e)}", context)

        except (ValidationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(kwargs=kwargs, error=str(e))
            raise DataError(f"Unexpected error generating long name: {str(e)}", context)

    @log_execution_time
    def _check_order_exists_by_remote_id(self, remote_order_id: str, exchange: str, scrip_code: int) -> Optional[Dict]:
        """
        Check if an order with the given remote_order_id already exists on the broker.
        This is used for idempotency checks to prevent duplicate orders.

        Args:
            remote_order_id: The remote order ID to check
            exchange: Exchange code
            scrip_code: Scrip code for the order

        Returns:
            Dict with order details if found, None otherwise
        """
        try:
            if exchange == "M":
                # For MCX, remote_order_id check may not work reliably, skip
                return None

            req_list = [{"Exch": exchange, "RemoteOrderID": remote_order_id}]
            status = self.api.fetch_order_status(req_list)

            if status is not None and len(status.get("OrdStatusResLst", [])) > 0:
                for sub_status in status["OrdStatusResLst"]:
                    if str(sub_status.get("ScripCode")) == str(scrip_code):
                        trading_logger.log_info(
                            "Found existing order with same remote_order_id",
                            {
                                "remote_order_id": remote_order_id,
                                "exchange": exchange,
                                "scrip_code": scrip_code,
                                "broker_order_id": sub_status.get("BrokerOrderID"),
                            },
                        )
                        return sub_status
        except Exception as e:
            trading_logger.log_warning(
                "Error checking for existing order by remote_order_id",
                {"remote_order_id": remote_order_id, "exchange": exchange, "error": str(e)},
            )
        return None

    @log_execution_time
    @validate_inputs(
        broker_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0, delete=lambda x: isinstance(x, bool)
    )
    def _get_exchange_order_id(self, broker_order_id: str, order: Order = Order(), delete: bool = True) -> str:
        """
        Retrieves exchange order id for trades executed today with enhanced error handling.

        Args:
            broker_order_id: Broker order ID
            order: Order object. Defaults to empty Order.
            delete: If True, broker order id is deleted from redis if no exchange order id is found. Defaults to True.

        Returns:
            str: Exchange order ID if available, else "0"

        Raises:
            ValidationError: If parameters are invalid
            OrderError: If order retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        try:
            trading_logger.log_info("Getting exchange order ID", {"broker_order_id": broker_order_id, "delete": delete})

            def get_exchange_order_id_from_orders(broker_order_id: str) -> str:
                """Get exchange order ID from order book."""
                try:
                    orders = pd.DataFrame(self.api.order_book())
                    fivepaisa_order = orders[orders.BrokerOrderId == int(broker_order_id)]
                    if len(fivepaisa_order) == 1:
                        exch_order_id = str(fivepaisa_order.ExchOrderID.item())
                        trading_logger.log_debug(
                            "Exchange order ID found in order book",
                            {"broker_order_id": broker_order_id, "exch_order_id": exch_order_id},
                        )
                        return exch_order_id
                    else:
                        trading_logger.log_warning(
                            "Trade not found in order book",
                            {"broker_order_id": broker_order_id, "order_count": len(fivepaisa_order)},
                        )
                        return "0"
                except Exception as e:
                    trading_logger.log_error(
                        "Error getting exchange order ID from orders", e, {"broker_order_id": broker_order_id}
                    )
                    return "0"

            exch_order_id = "0"

            # Get order from Redis if not provided
            if order is None or not order.internal_order_id:
                try:
                    order_data = self.redis_o.hgetall(broker_order_id)
                    if not order_data:
                        context = create_error_context(broker_order_id=broker_order_id)
                        raise OrderError("Order not found in Redis", context)
                    order = Order(**order_data)
                except Exception as e:
                    context = create_error_context(broker_order_id=broker_order_id, error=str(e))
                    raise OrderError(f"Failed to retrieve order from Redis: {str(e)}", context)

            # Check if order already has exchange order ID
            if order.exch_order_id not in ["0", "None", 0, None]:
                trading_logger.log_debug(
                    "Order already has exchange order ID",
                    {"broker_order_id": broker_order_id, "exch_order_id": order.exch_order_id},
                )
                return order.exch_order_id
            else:
                # Try to get exchange order ID from API
                try:
                    exch = order.exchange
                    if exch != "M":
                        remote_order_id = order.remote_order_id
                    else:
                        remote_order_id = broker_order_id

                    req_list_ = [{"Exch": exch, "RemoteOrderID": remote_order_id}]
                    status = self.api.fetch_order_status(req_list_)

                    if status is not None and len(status.get("OrdStatusResLst", [])) > 0:
                        for sub_status in status["OrdStatusResLst"]:
                            if str(sub_status.get("ScripCode")) == str(order.scrip_code):
                                exch_order_id = str(sub_status.get("ExchOrderID") or "")
                                trading_logger.log_info(
                                    "Retrieved exchange order ID from API",
                                    {"broker_order_id": broker_order_id, "exch_order_id": exch_order_id},
                                )
                                return exch_order_id
                    else:
                        trading_logger.log_warning(
                            "No order status found from API",
                            {"broker_order_id": broker_order_id, "exchange": exch, "remote_order_id": remote_order_id},
                        )

                except Exception as e:
                    trading_logger.log_error(
                        "Error fetching order status from API",
                        e,
                        {"broker_order_id": broker_order_id, "exchange": order.exchange},
                    )

                # Try to get from order book
                exch_order_id = get_exchange_order_id_from_orders(broker_order_id)
                if exch_order_id not in ["0", "None", 0, ""]:
                    trading_logger.log_info(
                        "Exchange order ID found in order book",
                        {"broker_order_id": broker_order_id, "exch_order_id": exch_order_id},
                    )
                    return exch_order_id
                else:
                    trading_logger.log_warning("Order not found in order book", {"broker_order_id": broker_order_id})

                    if delete:
                        try:
                            internal_order_id = self.redis_o.hget(broker_order_id, "orderRef")
                            trading_logger.log_info(
                                "Deleting broker order ID",
                                {"broker_order_id": broker_order_id, "internal_order_id": internal_order_id},
                            )
                            delete_broker_order_id(self, internal_order_id, broker_order_id)
                        except Exception as e:
                            trading_logger.log_error(
                                "Error deleting broker order ID", e, {"broker_order_id": broker_order_id}
                            )

                    return "0"

        except (ValidationError, OrderError, BrokerConnectionError):
            raise
        except Exception as e:
            context = create_error_context(broker_order_id=broker_order_id, delete=delete, error=str(e))
            raise OrderError(f"Unexpected error getting exchange order ID: {str(e)}", context)

    @log_execution_time
    @validate_inputs(date_string=lambda x: isinstance(x, str) and len(x.strip()) > 0)
    def _convert_date_string(self, date_string: str) -> dt.datetime:
        """
        Convert 5paisa datestring in positions/orders to datetime with enhanced error handling.

        Args:
            date_string: 5paisa datetime string

        Returns:
            dt.datetime: datetime object

        Raises:
            ValidationError: If parameters are invalid
            DataError: If date conversion fails
        """
        try:
            trading_logger.log_debug("Converting date string", {"date_string": date_string})

            if not date_string or not isinstance(date_string, str):
                context = create_error_context(date_string=date_string, date_string_type=type(date_string))
                raise ValidationError("Invalid date string", context)

            # Extract timestamp and timezone offset from the string
            pattern = r"/Date\((\d+)([+-]\d{4})\)/"
            match = re.match(pattern, date_string)

            if match:
                try:
                    timestamp = int(match.group(1))
                    # Create a datetime object using the timestamp
                    result = dt.datetime.fromtimestamp(timestamp / 1000)

                    trading_logger.log_debug(
                        "Date string converted successfully",
                        {"date_string": date_string, "timestamp": timestamp, "result": result.isoformat()},
                    )

                    return result
                except (ValueError, OverflowError) as e:
                    context = create_error_context(
                        date_string=date_string, timestamp=timestamp if "timestamp" in locals() else None, error=str(e)
                    )
                    raise DataError(f"Error converting timestamp: {str(e)}", context)
            else:
                trading_logger.log_warning(
                    "Incorrect date string format", {"date_string": date_string, "expected_pattern": pattern}
                )
                return dt.datetime(1970, 1, 1)

        except (ValidationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(date_string=date_string, error=str(e))
            raise DataError(f"Unexpected error converting date string: {str(e)}", context)

    @log_execution_time
    @validate_inputs(
        long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def get_min_lot_size(self, long_symbol, exchange="N") -> int:
        """
        Get minimum lot size for a symbol with enhanced error handling.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name. Defaults to "N".

        Returns:
            int: Minimum lot size for the symbol

        Raises:
            ValidationError: If parameters are invalid
            SymbolError: If symbol lookup fails
        """
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
            raise SymbolError(f"Unexpected error getting minimum lot size: {str(e)}", context)
