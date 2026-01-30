import datetime as dt
import glob
import json
import logging
import math
import os
import re
import secrets
import sys
import time
import traceback
from collections import OrderedDict
from copy import deepcopy
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import redis
from chameli.dateutils import calc_fractional_business_days, valid_datetime
from chameli.europeanoptions import BlackScholesDelta, BlackScholesIV
from requests import Session
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .broker_base import BrokerBase, HistoricalData, Order, OrderInfo, OrderStatus, Position, Price
from .config import get_config
from .exceptions import (
    SymbolError,
    TradingAPIError,
    ValidationError,
    DataError,
    RedisError,
    MarketDataError,
    OrderError,
    PnLError,
    CommissionError,
    create_error_context,
)
from .error_handling import retry_on_error, safe_execute, log_execution_time, handle_broker_errors, validate_inputs
from . import trading_logger
from .globals import get_tradingapi_now

logger = logging.getLogger(__name__)
r = redis.Redis(db=1, encoding="utf-8", decode_responses=True)


# Enhanced exception handler with structured logging
def my_handler(typ, value, trace):
    context = create_error_context(
        exception_type=typ.__name__, exception_value=str(value), traceback="".join(traceback.format_tb(trace))
    )
    trading_logger.log_error(f"Uncaught exception: {typ.__name__}", value, context)


sys.excepthook = my_handler
config = get_config()
mds_history = redis.Redis(db=1, encoding="utf-8", decode_responses=True)
pubsub = mds_history.pubsub()

empty_trades = {
    "symbol": pd.Series(dtype="object"),
    "side": pd.Series(dtype="object"),
    "entry_time": pd.Series(dtype="string"),
    "entry_quantity": pd.Series(dtype="float64"),
    "entry_price": pd.Series(dtype="float64"),
    "exit_time": pd.Series(dtype="string"),
    "exit_quantity": pd.Series(dtype="float64"),
    "exit_price": pd.Series(dtype="float64"),
    "commission": pd.Series(dtype="int64"),
    "int_order_id": pd.Series(dtype="object"),
    "entry_keys": pd.Series(dtype="object"),
    "exit_keys": pd.Series(dtype="object"),
    "additional_info": pd.Series(dtype="object"),
    "realized_pnl": pd.Series(dtype="float64"),
    "gross_pnl": pd.Series(dtype="float64"),
    "mtm": pd.Series(dtype="float64"),
    "pnl": pd.Series(dtype="float64"),
}

empty_trades = pd.DataFrame(empty_trades)

empty_md = {
    "date": pd.Series(dtype="datetime64[ns]"),
    "open": pd.Series(dtype="float64"),
    "high": pd.Series(dtype="float64"),
    "low": pd.Series(dtype="float64"),
    "close": pd.Series(dtype="float64"),
    "volume": pd.Series(dtype="int64"),
}
empty_md = pd.DataFrame(empty_md)


def hget_with_default(brok: BrokerBase, hash_name: str, key: str, default_value):
    value = brok.redis_o.hget(hash_name, key)
    if value is None:
        return default_value
    else:
        return value


def get_all_strategy_names(redis_db) -> set:
    """
    Returns a set of strategies used within the database of the trading system.
    :return:
    """
    out = set()
    if redis_db is not None:
        # Use scan() with cursor 0 instead of scan_iter() to avoid cursor state issues
        cursor = 0
        while True:
            cursor, keys = redis_db.scan(cursor, match="[A-Z]*_[0-9]*", count=1000)
            for key in keys:
                out.add(key.split("_")[0])
            if cursor == 0:
                break
    return out


def set_starting_internal_ids_int(redis_db) -> dict:
    """Sets the internal_ids for each strategy. New orders start from the values defined by this function"""
    out: dict[str, int] = {}
    strategies = get_all_strategy_names(redis_db)
    for s in strategies:
        # Use scan() with cursor 0 instead of scan_iter() to avoid cursor state issues
        pattern = s + "_" + "*"
        cursor = 0
        while True:
            cursor, keys = redis_db.scan(cursor, match=pattern, count=1000)
            for key in keys:
                if out.get(s, 1) <= int(key.split("_")[1]):
                    out[s] = int(key.split("_")[1]) + 1
            if cursor == 0:
                break
    logger.debug(f"Starting internal ids : {out} ")
    return out


def _safe_parse_json_or_dict(json_str):
    """
    Safely parse a string that could be either JSON or Python dict string.

    Args:
        json_str (str): String that could be JSON or Python dict

    Returns:
        dict: Parsed dictionary

    Raises:
        DataError: If the string cannot be parsed as either JSON or dict
    """
    if not json_str or not json_str.strip():
        return {}

    try:
        # First try to parse as JSON
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            # If JSON fails, try to parse as Python dict string
            import ast

            # Handle common problematic values before parsing
            # Replace bare 'nan' with 'float("nan")' for ast.literal_eval
            processed_str = json_str.replace("'nan'", "float('nan')")
            processed_str = processed_str.replace("nan", "float('nan')")

            # Also handle other common problematic values
            processed_str = processed_str.replace("'inf'", "float('inf')")
            processed_str = processed_str.replace("'-inf'", "float('-inf')")
            processed_str = processed_str.replace("inf", "float('inf')")
            processed_str = processed_str.replace("-inf", "float('-inf')")

            # Handle bare identifiers that should be floats
            import re

            processed_str = re.sub(r"\bnan\b", "float('nan')", processed_str)
            processed_str = re.sub(r"\binf\b", "float('inf')", processed_str)
            processed_str = re.sub(r"\b-inf\b", "float('-inf')", processed_str)

            return ast.literal_eval(processed_str)
        except (ValueError, SyntaxError) as e:
            # If ast.literal_eval still fails, try a more robust approach
            try:
                # Use eval with restricted globals for safety
                safe_globals = {
                    "True": True,
                    "False": False,
                    "None": None,
                    "float": float,
                    "int": int,
                    "str": str,
                    "nan": float("nan"),
                    "inf": float("inf"),
                }
                return eval(json_str, safe_globals, {})
            except Exception as eval_error:
                raise DataError(
                    f"Cannot parse as JSON or Python dict: {str(e)}",
                    {
                        "input_preview": str(json_str)[:200],
                        "input_length": len(str(json_str)),
                        "ast_error": str(e),
                        "eval_error": str(eval_error),
                    },
                )


@log_execution_time
def _merge_additional_info(old_info, new_info):
    """
    Merge two JSON strings. Duplicate keys in new_info will be renamed
    with a suffix '_1', '_2', etc., to avoid conflicts.

    Args:
        old_info (str): JSON string representing the original information.
        new_info (str): JSON string representing the new information.

    Returns:
        str: Merged JSON string.

    Raises:
        DataError: If either input is not a valid JSON string.
    """
    try:
        # Parse the JSON strings into dictionaries using the safe parser
        old_dict = _safe_parse_json_or_dict(old_info)
        new_dict = _safe_parse_json_or_dict(new_info)

        if not isinstance(old_dict, dict) or not isinstance(new_dict, dict):
            raise DataError(
                "Both inputs must be JSON objects (dictionaries).",
                {
                    "old_info_type": type(old_info).__name__,
                    "new_info_type": type(new_info).__name__,
                    "old_info": str(old_info)[:100],
                    "new_info": str(new_info)[:100],
                },
            )

        # Create a merged dictionary
        merged_dict = old_dict.copy()

        for key, value in new_dict.items():
            if key not in merged_dict:
                # If the key is unique, add it
                merged_dict[key] = value
            else:
                # If key exists, generate a new key with a numeric suffix
                suffix = 1
                new_key = f"{key}_{suffix}"
                while new_key in merged_dict:
                    suffix += 1
                    new_key = f"{key}_{suffix}"
                merged_dict[new_key] = value

        # Convert the merged dictionary back to JSON string
        return json.dumps(merged_dict)

    except DataError as e:
        # Re-raise DataError from the helper function with additional context
        context = create_error_context(
            old_info_preview=str(old_info)[:200] if old_info else None,
            new_info_preview=str(new_info)[:200] if new_info else None,
            old_info_length=len(str(old_info)) if old_info else 0,
            new_info_length=len(str(new_info)) if new_info else 0,
            original_error=str(e),
        )
        raise DataError(f"Error parsing additional info: {str(e)}", context)
    except Exception as e:
        context = create_error_context(
            old_info_type=type(old_info).__name__, new_info_type=type(new_info).__name__, error_type=type(e).__name__
        )
        raise DataError(f"Error merging additional info: {str(e)}", context)


def is_not_int_order_id(s):
    return not bool(re.match(r"^.*_\d+$", s))


def needs_refresh_status_from_redis(broker: BrokerBase, pnl_df: pd.DataFrame) -> bool:
    """
    Returns True if any trade's entry/exit order price in Redis is 0.
    """
    for _, row in pnl_df.iterrows():
        for order_id_col in ["entry_internal_orderid", "exit_internal_orderid"]:
            order_id = row.get(order_id_col)
            if order_id:
                redis_key = f"order:{order_id}"
                price = broker.redis_o.hget(redis_key, "price")
                if price is not None and float(price) == 0.0:
                    return True
    return False


@log_execution_time
@validate_inputs(
    strategy=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    start_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    end_time=lambda x: x is None or (isinstance(x, str) and len(x.strip()) > 0),
    market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_pnl_table(
    broker: BrokerBase,
    strategy: str,
    start_time: str = "1970-01-01 00:00:00",
    end_time: str = None,
    refresh_status=False,
    market_close_time="15:30:00",
    eod=False,
) -> pd.DataFrame:
    """Get P&L table for a specified strategy with enhanced error handling.

    Args:
        broker (BrokerBase): broker instance
        strategy (str): strategy name
        start_time (str, optional): Start Date for strategy pnl. Defaults to "1970-01-01 00:00:00".
        end_time (str, optional): End Date for strategy pnl. Defaults to current time when function is called.
        refresh_status (bool, optional): Whether to refresh order status. Defaults to False.
        market_close_time (str, optional): closing time for option expiry, defaults to "15:30:00"
        eod (bool, optional): Whether this is end-of-day processing. Defaults to False.

    Returns:
        pd.DataFrame: strategy pnl

    Raises:
        ValidationError: If input parameters are invalid
        RedisError: If there are issues with Redis operations
        PnLError: If there are issues with P&L calculations
    """
    # Set default end_time inside the function body (evaluated at call time, not import time)
    if end_time is None:
        end_time = get_tradingapi_now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        int_order_ids = []
        trades = []

        # Get all internal order IDs for the strategy
        # Use scan_iter with match pattern to find all order IDs
        # Note: scan_iter should return fresh results, but if there are issues with stale cursors,
        # we can force a fresh scan by using scan() with cursor 0
        pattern = strategy + "_" + "*"
        
        # Force a fresh scan by using scan() with cursor 0 to avoid any potential cursor state issues
        # This ensures we always get the latest keys, even if the connection was created earlier
        cursor = 0
        scan_count = 0
        while True:
            cursor, keys = broker.redis_o.scan(cursor, match=pattern, count=1000)
            int_order_ids.extend(keys)
            scan_count += len(keys)
            if cursor == 0:
                break
        
        # Log what we found for debugging
        if len(int_order_ids) > 0:
            # Sort numerically by extracting the number after the underscore
            def extract_order_number(order_id):
                try:
                    # Extract number after last underscore (e.g., "SCALPING01_1949" -> 1949)
                    parts = order_id.rsplit("_", 1)
                    if len(parts) == 2:
                        return int(parts[1])
                    return 0
                except (ValueError, IndexError):
                    return 0
            
            int_order_ids.sort(key=extract_order_number)
            last_5 = int_order_ids[-5:] if len(int_order_ids) >= 5 else int_order_ids
            trading_logger.log_debug(
                f"Found {len(int_order_ids)} order IDs for strategy {strategy}",
                {
                    "strategy": strategy,
                    "count": len(int_order_ids),
                    "first": int_order_ids[0] if int_order_ids else None,
                    "last": int_order_ids[-1] if int_order_ids else None,
                    "last_5": last_5,
                }
            )

        if not int_order_ids:
            trading_logger.log_info(
                f"No internal order IDs found for strategy: {strategy}",
                {"strategy": strategy, "start_time": start_time, "end_time": end_time},
            )
            return empty_trades

        # Sort numerically by extracting the number after the underscore
        def extract_order_number(order_id):
            try:
                # Extract number after last underscore (e.g., "SCALPING01_1949" -> 1949)
                parts = order_id.rsplit("_", 1)
                if len(parts) == 2:
                    return int(parts[1])
                return 0
            except (ValueError, IndexError):
                return 0
        
        int_order_ids.sort(key=extract_order_number)

        for int_order_id in int_order_ids:
            try:
                if is_not_int_order_id(int_order_id):
                    continue

                symbol = broker.redis_o.hget(int_order_id, "long_symbol")
                if not symbol:
                    trading_logger.log_warning(
                        f"No symbol found for internal order ID: {int_order_id}",
                        {"int_order_id": int_order_id, "strategy": strategy},
                    )
                    continue

                entry_keys = hget_with_default(broker, int_order_id, "entry_keys", "").split()
                exit_keys = hget_with_default(broker, int_order_id, "exit_keys", "").split()
                additional_info_entry = ""
                additional_info_exit = ""
                
                # Debug: Log entry_keys and exit_keys for trades with exit_keys (exits placed)
                if len(exit_keys) > 0:
                    trading_logger.log_debug("Trade with exit_keys processing in get_pnl_table", {
                        "int_order_id": int_order_id,
                        "symbol": symbol,
                        "entry_keys": entry_keys,
                        "exit_keys": exit_keys,
                        "entry_keys_count": len(entry_keys),
                        "exit_keys_count": len(exit_keys),
                    })

                if len(entry_keys) == 0:
                    trading_logger.log_error(
                        f"No entry key found for int_order_id: {int_order_id}",
                        None,
                        {"int_order_id": int_order_id, "strategy": strategy, "symbol": symbol},
                    )
                    continue

                # Process entry keys
                if len(entry_keys) > 0:
                    try:
                        order_1 = Order(**broker.redis_o.hgetall(entry_keys[0]))
                        side = order_1.order_type if "?" not in symbol else "BUY"
                        entry_time, _ = valid_datetime(order_1.remote_order_id[:-2], "%Y-%m-%d %H:%M:%S")
                        # Handle case where valid_datetime returns False (parsing failed)
                        if entry_time is False or entry_time is None:
                            entry_time = ""
                        for ek in entry_keys:
                            additional_info_entry = _merge_additional_info(
                                additional_info_entry, hget_with_default(broker, ek, "additional_info", "")
                            )
                    except Exception as e:
                        trading_logger.log_error(
                            f"Error processing entry keys for {int_order_id}",
                            e,
                            {"int_order_id": int_order_id, "symbol": symbol, "entry_keys": entry_keys},
                        )
                        continue

                # Process exit keys
                if len(exit_keys) > 0:
                    try:
                        order_1 = Order(**broker.redis_o.hgetall(exit_keys[-1]))
                        exit_time, _ = valid_datetime(order_1.remote_order_id[:-2], "%Y-%m-%d %H:%M:%S")
                        # Handle case where valid_datetime returns False (parsing failed)
                        # This can happen when exit order status is UNDEFINED and remote_order_id is malformed
                        if exit_time is False or exit_time is None:
                            exit_time = ""
                        for ek in exit_keys:
                            additional_info_exit = _merge_additional_info(
                                additional_info_exit, hget_with_default(broker, ek, "additional_info", "")
                            )
                    except Exception as e:
                        trading_logger.log_error(
                            f"Error processing exit keys for {int_order_id}",
                            e,
                            {"int_order_id": int_order_id, "symbol": symbol, "exit_keys": exit_keys},
                        )
                        exit_time = ""
                else:
                    exit_time = ""

                additional_info = _merge_additional_info(additional_info_entry, additional_info_exit)
                entry_quantity = 0
                entry_price: float = 0.0
                exit_quantity = 0
                exit_price: float = 0.0
                commission = 0

                # Refresh status if requested
                if refresh_status:
                    # Debug: Log refresh for trades with exit_keys
                    if len(exit_keys) > 0:
                        trading_logger.log_debug("Refreshing entry status for trade with exit_keys", {
                            "int_order_id": int_order_id,
                            "symbol": symbol,
                            "entry_keys": entry_keys,
                            "exit_keys": exit_keys,
                        })
                    try:
                        for entry_key in entry_keys:
                            order = Order(**broker.redis_o.hgetall(entry_key))
                            broker_order_id = order.broker_order_id
                            if len(exit_keys) > 0:
                                trading_logger.log_debug("Calling update_order_status for entry", {
                                    "int_order_id": int_order_id,
                                    "entry_key": entry_key,
                                    "broker_order_id": broker_order_id,
                                })
                            update_order_status(broker, int_order_id, broker_order_id, eod=eod)
                    except Exception as e:
                        trading_logger.log_error(
                            f"Error refreshing entry status for {int_order_id}",
                            e,
                            {"int_order_id": int_order_id, "entry_keys": entry_keys, "exit_keys": exit_keys},
                        )

                # Calculate entry position
                try:
                    entry_position = get_open_position_by_order(broker, int_order_id, exclude_zero=True, side=["entry"])
                    base_position = parse_combo_symbol(symbol)
                    position_combo_info = calculate_extra_combo_positions(entry_position, base_position)
                    entry_quantity = position_combo_info.get("total_in_progress", 0)
                    entry_price = (
                        sum(position.value for position in entry_position.values()) / entry_quantity
                        if entry_quantity != 0
                        else 0.0
                    )
                except Exception as e:
                    trading_logger.log_error(
                        f"Error calculating entry position for {int_order_id}",
                        e,
                        {"int_order_id": int_order_id, "symbol": symbol},
                    )
                    continue

                # Refresh exit status if requested
                if refresh_status:
                    # Debug: Log refresh for trades with exit_keys
                    if len(exit_keys) > 0:
                        trading_logger.log_debug("Refreshing exit status for trade with exit_keys", {
                            "int_order_id": int_order_id,
                            "symbol": symbol,
                            "entry_keys": entry_keys,
                            "exit_keys": exit_keys,
                        })
                    try:
                        for exit_key in exit_keys:
                            # Get order data before refresh to see what we're working with
                            exit_order_data_before = broker.redis_o.hgetall(exit_key)
                            if len(exit_keys) > 0:
                                trading_logger.log_debug("Exit order data before refresh", {
                                    "int_order_id": int_order_id,
                                    "exit_key": exit_key,
                                    "exit_order_data": dict(exit_order_data_before) if exit_order_data_before else {},
                                })
                            
                            order = Order(**exit_order_data_before) if exit_order_data_before else None
                            if order is None:
                                trading_logger.log_warning("Exit order not found in Redis before refresh", {
                                    "int_order_id": int_order_id,
                                    "exit_key": exit_key,
                                })
                                continue
                            
                            broker_order_id = order.broker_order_id
                            if len(exit_keys) > 0:
                                trading_logger.log_debug("Calling update_order_status for exit", {
                                    "int_order_id": int_order_id,
                                    "exit_key": exit_key,
                                    "broker_order_id": broker_order_id,
                                })
                            
                            update_order_status(broker, int_order_id, broker_order_id, eod=eod)
                            
                            # Get order data after refresh to see if anything changed
                            exit_order_data_after = broker.redis_o.hgetall(exit_key)
                            if len(exit_keys) > 0:
                                trading_logger.log_debug("Exit order data after refresh", {
                                    "int_order_id": int_order_id,
                                    "exit_key": exit_key,
                                    "exit_order_data": dict(exit_order_data_after) if exit_order_data_after else {},
                                })
                    except Exception as e:
                        trading_logger.log_error(
                            f"Error refreshing exit status for {int_order_id}",
                            e,
                            {"int_order_id": int_order_id, "exit_keys": exit_keys},
                        )

                # Calculate exit position
                # Only calculate if exit_keys exist - if empty, set defaults and continue (don't skip trade)
                if len(exit_keys) > 0:
                    try:
                        exit_position = get_open_position_by_order(broker, int_order_id, exclude_zero=True, side=["exit"])
                        base_position = parse_combo_symbol(symbol)
                        position_combo_info = calculate_extra_combo_positions(exit_position, base_position)
                        exit_quantity = position_combo_info.get("total_in_progress", 0)
                        exit_price = (
                            sum(position.value for position in exit_position.values()) / exit_quantity
                            if exit_quantity != 0
                            else 0
                        )
                    except Exception as e:
                        trading_logger.log_error(
                            f"Error calculating exit position for {int_order_id}",
                            e,
                            {"int_order_id": int_order_id, "symbol": symbol, "exit_keys": exit_keys},
                        )
                        # Don't skip trade - set defaults instead so trade is still included in results
                        # This is important when exit_keys are being written but order data isn't ready yet
                        exit_quantity = 0
                        exit_price = 0.0
                else:
                    # exit_keys empty - set defaults, don't skip trade
                    exit_quantity = 0
                    exit_price = 0.0

                # Validate quantity consistency
                if abs(exit_quantity) > abs(entry_quantity):
                    trading_logger.log_error(
                        f"Exit Quantity > Entry Quantity!! for internal order id: {int_order_id}",
                        None,
                        {
                            "int_order_id": int_order_id,
                            "entry_quantity": entry_quantity,
                            "exit_quantity": exit_quantity,
                            "symbol": symbol,
                        },
                    )
                    # raise ValueError("exit quantity greater than entry quantity")

                if eod:
                    # Handle option expiration
                    if exit_quantity + entry_quantity != 0 and contains_earlier_date(
                        symbol, get_tradingapi_now().strftime("%Y%m%d"), market_close_time
                    ):
                        try:
                            # are options expiration needed?
                            get_open_position_by_order(
                                broker=broker, int_order_id=int_order_id, market_close_time=market_close_time
                            )
                            exit_keys = hget_with_default(broker, int_order_id, "exit_keys", "").split()
                            exit_position = get_open_position_by_order(
                                broker, int_order_id, exclude_zero=True, side=["exit"]
                            )
                            base_position = parse_combo_symbol(symbol)
                            position_combo_info = calculate_extra_combo_positions(exit_position, base_position)
                            exit_quantity = position_combo_info.get("total_in_progress", 0)
                            exit_price = (
                                sum(position.value for position in exit_position.values()) / exit_quantity
                                if exit_quantity != 0
                                else 0
                            )
                            exit_time = get_tradingapi_now().strftime("%Y-%m-%d %H:%M:%S")
                        except Exception as e:
                            trading_logger.log_error(
                                f"Error handling option expiration for {int_order_id}",
                                e,
                                {
                                    "int_order_id": int_order_id,
                                    "symbol": symbol,
                                    "market_close_time": market_close_time,
                                },
                            )

                # create trade row for internal order id
                row = {
                    "symbol": symbol,
                    "side": side,
                    "entry_time": entry_time,
                    "entry_quantity": entry_quantity,
                    "entry_price": float(entry_price),
                    "exit_time": exit_time,
                    "exit_quantity": exit_quantity,
                    "exit_price": float(exit_price),
                    "commission": commission,
                    "int_order_id": int_order_id,
                    "entry_keys": " ".join(entry_keys),
                    "exit_keys": " ".join(exit_keys),
                    "additional_info": additional_info,
                }
                trades.append(row)
                
                # Debug: Log when trades with exit_keys are added to trades list
                if len(exit_keys) > 0:
                    trading_logger.log_debug("Trade with exit_keys added to trades list", {
                        "int_order_id": int_order_id,
                        "symbol": symbol,
                        "entry_time": str(entry_time),
                        "exit_time": str(exit_time),
                        "entry_time_type": type(entry_time).__name__,
                        "exit_time_type": type(exit_time).__name__,
                        "entry_quantity": entry_quantity,
                        "exit_quantity": exit_quantity,
                    })

            except Exception as e:
                trading_logger.log_error(
                    f"Error processing internal order ID: {int_order_id}",
                    e,
                    {"int_order_id": int_order_id, "strategy": strategy},
                )
                continue

        # Process results
        try:
            out = pd.DataFrame(trades)
            if len(out) > 0:
                # Debug: Log trades with exit_keys before filtering
                if "int_order_id" in out.columns and "exit_keys" in out.columns:
                    trades_with_exits = out[out["exit_keys"].astype(str).str.strip() != ""]
                    if len(trades_with_exits) > 0:
                        for idx, row in trades_with_exits.iterrows():
                            trading_logger.log_debug("Trade with exit_keys in DataFrame before filter", {
                                "int_order_id": row.get("int_order_id", ""),
                                "symbol": row.get("symbol", ""),
                                "entry_time": str(row.get("entry_time", "")),
                                "exit_time": str(row.get("exit_time", "")),
                                "entry_time_type": str(type(row.get("entry_time", ""))),
                                "exit_time_type": str(type(row.get("exit_time", ""))),
                                "start_time": start_time,
                                "end_time": end_time,
                            })
                
                out = out.loc[
                    ((out.entry_time >= start_time) | (out.entry_time == ""))
                    & ((out.exit_time <= end_time) | (out.exit_time == 0)|(out.entry_time == "")),
                ]
                
                # Debug: Log trades with exit_keys that were filtered out
                if "int_order_id" in out.columns and "exit_keys" in out.columns:
                    trades_before = pd.DataFrame(trades)
                    if "int_order_id" in trades_before.columns and "exit_keys" in trades_before.columns:
                        trades_with_exits_before = trades_before[trades_before["exit_keys"].astype(str).str.strip() != ""]
                        trades_with_exits_after = out[out["exit_keys"].astype(str).str.strip() != ""]
                        
                        # Find trades that were filtered out
                        if len(trades_with_exits_before) > 0:
                            before_ids = set(trades_with_exits_before["int_order_id"].values)
                            after_ids = set(trades_with_exits_after["int_order_id"].values) if len(trades_with_exits_after) > 0 else set()
                            filtered_out_ids = before_ids - after_ids
                            
                            for filtered_id in filtered_out_ids:
                                row_filtered = trades_with_exits_before[trades_with_exits_before["int_order_id"] == filtered_id].iloc[0]
                                entry_time_val = row_filtered.get("entry_time", "")
                                exit_time_val = row_filtered.get("exit_time", "")
                                
                                # Check each condition
                                entry_time_ok = (str(entry_time_val) >= start_time) or (str(entry_time_val) == "")
                                exit_time_ok = (str(exit_time_val) <= end_time) or (str(exit_time_val) == "0") or (str(entry_time_val) == "")
                                
                                trading_logger.log_warning("Trade with exit_keys was filtered out by DataFrame filter", {
                                    "int_order_id": filtered_id,
                                    "symbol": row_filtered.get("symbol", ""),
                                    "start_time": start_time,
                                    "end_time": end_time,
                                    "entry_time": str(entry_time_val),
                                    "exit_time": str(exit_time_val),
                                    "entry_time_type": str(type(entry_time_val)),
                                    "exit_time_type": str(type(exit_time_val)),
                                    "entry_time_ok": entry_time_ok,
                                    "exit_time_ok": exit_time_ok,
                                    "trades_count_before": len(trades),
                                    "trades_count_after": len(out),
                                })
                # pnl = (out.exit_price + out.entry_price) * out.exit_quantity
                # pnl = np.where(out["side"] == "BUY", pnl, -pnl)
                out["mtm"] = np.where(out["exit_quantity"] != 0, out["exit_price"], out["entry_price"])
                out["gross_pnl"] = -1 * (
                    out["exit_price"] * out["exit_quantity"]
                    + out["mtm"] * -1 * (out["entry_quantity"] + out["exit_quantity"])
                    + out["entry_quantity"] * out["entry_price"]
                )
                out["pnl"] = out["gross_pnl"]
                out.sort_values(["entry_time", "symbol"], ascending=[1, 1], inplace=True)
                out.reset_index(drop=True, inplace=True)
            else:
                out = empty_trades
        except Exception as e:
            trading_logger.log_error(
                "Error processing P&L table results",
                e,
                {"strategy": strategy, "trades_count": len(trades), "start_time": start_time, "end_time": end_time},
            )
            out = empty_trades

        return out
    except Exception as e:
        trading_logger.log_error(
            "Error in get_pnl_table",
            e,
            {
                "strategy": strategy,
                "start_time": start_time,
                "end_time": end_time,
                "refresh_status": refresh_status,
                "market_close_time": market_close_time,
                "eod": eod,
            },
        )
        raise PnLError(
            f"Failed to get P&L table for strategy {strategy}: {str(e)}", {"strategy": strategy, "error": str(e)}
        )


@log_execution_time
@validate_inputs(
    input_string=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    comparison_date=lambda x: isinstance(x, str) and len(x.strip()) == 8,
    market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def contains_earlier_date(input_string: str, comparison_date: str, market_close_time: str) -> bool:
    """Check if input string contains dates earlier than comparison date.

    Args:
        input_string: String to search for dates
        comparison_date: Date to compare against (YYYYMMDD format)
        market_close_time: Market close time (HH:MM:SS format)

    Returns:
        bool: True if earlier date found, False otherwise

    Raises:
        ValidationError: If input parameters are invalid
        DataError: If date parsing fails
    """
    try:
        # Regular expressions to match dates in various formats
        date_patterns = [
            re.compile(r"\d{8}"),  # YYYYMMDD
            re.compile(r"\d{4}-\d{2}-\d{2}"),  # YYYY-MM-DD
            re.compile(r"\d{2}/\d{2}/\d{4}"),  # MM/DD/YYYY
        ]

        # Convert the comparison date to a datetime object
        comparison_date_obj = dt.datetime.strptime(comparison_date, "%Y%m%d")

        # Get the current datetime and convert market close time to datetime object
        current_datetime = get_tradingapi_now()
        market_close_time_obj = dt.datetime.strptime(market_close_time, "%H:%M:%S").time()

        # Define the comparison operator function based on market close time
        def comparison_operator(date_obj):
            if current_datetime.time() < market_close_time_obj:
                return date_obj < comparison_date_obj
            else:
                return date_obj <= comparison_date_obj

        for pattern in date_patterns:
            # Find all substrings that match the pattern
            dates = pattern.findall(input_string)

            for date_str in dates:
                try:
                    if pattern == date_patterns[0]:
                        date_obj = dt.datetime.strptime(date_str, "%Y%m%d")
                    elif pattern == date_patterns[1]:
                        date_obj = dt.datetime.strptime(date_str, "%Y-%m-%d")
                    elif pattern == date_patterns[2]:
                        date_obj = dt.datetime.strptime(date_str, "%m/%d/%Y")

                    if comparison_operator(date_obj):
                        trading_logger.log_debug(
                            "Earlier date found",
                            {
                                "input_string": input_string[:100],
                                "comparison_date": comparison_date,
                                "found_date": date_str,
                                "market_close_time": market_close_time,
                            },
                        )
                        return True
                except ValueError as e:
                    trading_logger.log_debug(
                        "Invalid date format skipped",
                        {"date_string": date_str, "pattern": str(pattern), "error": str(e)},
                    )
                    continue

        return False

    except ValueError as e:
        context = create_error_context(
            input_string=input_string[:100],
            comparison_date=comparison_date,
            market_close_time=market_close_time,
            error=str(e),
        )
        raise DataError(f"Date parsing error: {str(e)}", context)
    except Exception as e:
        context = create_error_context(
            input_string=input_string[:100],
            comparison_date=comparison_date,
            market_close_time=market_close_time,
            error_type=type(e).__name__,
        )
        raise DataError(f"Error in date comparison: {str(e)}", context)


@log_execution_time
@validate_inputs(
    strategy=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    broker_entry_side=lambda x: x is None or (isinstance(x, str) and x in ["BUY", "SHORT"]),
)
def get_orders_by_symbol(broker, strategy: str, long_symbol: str, broker_entry_side: str) -> list[str]:
    """Get a list of internal order IDs for a specified symbol, strategy and entry side combination.

    Args:
        broker: Broker instance
        strategy (str): strategy name
        long_symbol (str): long symbol name
        broker_entry_side (str): side of the entry trade. 'B' or 'S'.
                                So if entry was "SHORT", broker_entry_side will be 'S'

    Returns:
        list[str]: list of internal order ids that meet entry combination

    Raises:
        ValidationError: If input parameters are invalid
        RedisError: If Redis operations fail
    """
    try:
        int_order_ids = []

        # Scan for internal order IDs
        try:
            # Use scan() with cursor 0 instead of scan_iter() to avoid cursor state issues
            pattern = strategy + "_" + "*"
            cursor = 0
            while True:
                cursor, keys = broker.redis_o.scan(cursor, match=pattern, count=1000)
                int_order_ids.extend(keys)
                if cursor == 0:
                    break
        except Exception as e:
            context = create_error_context(strategy=strategy, scan_pattern=f"{strategy}_*", error=str(e))
            raise RedisError(f"Failed to scan Redis for strategy {strategy}: {str(e)}", context)

        if not int_order_ids:
            trading_logger.log_info(
                "No internal order IDs found for strategy", {"strategy": strategy, "long_symbol": long_symbol}
            )
            return []

        # Filter by long_symbol
        try:
            int_order_ids = [
                int_order_id
                for int_order_id in int_order_ids
                if broker.redis_o.type(int_order_id) == "hash"
                and broker.redis_o.hexists(int_order_id, "long_symbol")
                and long_symbol == broker.redis_o.hget(int_order_id, "long_symbol")
            ]
        except Exception as e:
            context = create_error_context(
                strategy=strategy, long_symbol=long_symbol, int_order_ids_count=len(int_order_ids), error=str(e)
            )
            raise RedisError(f"Failed to filter orders by symbol {long_symbol}: {str(e)}", context)

        # Filter by broker entry side if specified
        combo = False
        if broker_entry_side is not None:
            try:
                entry_keys = []
                for int_order_id in int_order_ids:
                    symbol = broker.redis_o.hget(int_order_id, "long_symbol")
                    combo = True if ":" in symbol else False
                    entry_keys_str = broker.redis_o.hget(int_order_id, "entry_keys")
                    if entry_keys_str:
                        entry_keys.append(entry_keys_str.split()[0])
                    else:
                        trading_logger.log_warning(
                            "No entry keys found for internal order ID",
                            {"int_order_id": int_order_id, "strategy": strategy},
                        )

                jsons = []
                for entry_key in entry_keys:
                    if entry_key:
                        jsons.append(broker.redis_o.hgetall(entry_key))
                    else:
                        jsons.append({})
                if combo:
                    indices = [i for i in range(len(jsons)) if "BUY" == broker_entry_side]
                else:
                    indices = [i for i in range(len(jsons)) if jsons[i].get("order_type") == broker_entry_side]
                int_order_ids = [int_order_ids[index] for index in indices]

            except Exception as e:
                context = create_error_context(
                    strategy=strategy,
                    long_symbol=long_symbol,
                    broker_entry_side=broker_entry_side,
                    int_order_ids_count=len(int_order_ids),
                    error=str(e),
                )
                raise RedisError(f"Failed to filter orders by entry side {broker_entry_side}: {str(e)}", context)

        trading_logger.log_debug(
            "Orders by symbol retrieved",
            {
                "strategy": strategy,
                "long_symbol": long_symbol,
                "broker_entry_side": broker_entry_side,
                "order_count": len(int_order_ids),
            },
        )

        return int_order_ids

    except (ValidationError, RedisError):
        raise
    except Exception as e:
        context = create_error_context(
            strategy=strategy, long_symbol=long_symbol, broker_entry_side=broker_entry_side, error_type=type(e).__name__
        )
        raise RedisError(f"Unexpected error in get_orders_by_symbol: {str(e)}", context)


@log_execution_time
@validate_inputs(
    int_order_id=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_open_position_by_order(
    broker: BrokerBase,
    int_order_id: str,
    exclude_zero: bool = True,
    side: List = ["entry", "exit"],
    market_close_time: str = "15:30:00",
) -> Dict[str, Position]:
    """Get open positions for a specific internal order ID.

    Args:
        broker: Broker instance
        int_order_id: Internal order ID
        exclude_zero: Whether to exclude zero positions
        side: List of sides to include ("entry", "exit")
        market_close_time: Market close time for expiry calculations

    Returns:
        Dict[str, Position]: Dictionary of symbol to position mapping

    Raises:
        ValidationError: If input parameters are invalid
        RedisError: If Redis operations fail
        OrderError: If order processing fails
    """
    trading_logger.log_debug(
        f"Getting open position for {int_order_id}",
        {
            "int_order_id": int_order_id,
            "exclude_zero": exclude_zero,
            "side": side,
            "market_close_time": market_close_time,
        },
    )

    def _get_position_by_broker_order_ids(keys, positions: Dict[str, Position]) -> Dict[str, Position]:
        """Helper function to get positions from broker order IDs."""
        try:
            for key in keys:
                if not key:
                    trading_logger.log_warning(
                        "Empty key found in broker order IDs", {"int_order_id": int_order_id, "keys": keys}
                    )
                    continue

                try:
                    order_data = broker.redis_o.hgetall(key)
                    if not order_data:
                        trading_logger.log_warning(
                            "No order data found for key", {"key": key, "int_order_id": int_order_id}
                        )
                        continue

                    order = Order(**order_data)
                    symbol = order.long_symbol
                    position = positions.get(symbol, Position())
                    position.symbol = symbol
                    quantity = order.quantity if order.order_type in ["BUY", "COVER"] else -1 * order.quantity
                    position.value = order.price * quantity + position.price * position.size
                    position.size = position.size + quantity
                    if position.size != 0:
                        position.price = position.value / position.size
                    else:
                        position.price = 0
                    positions[symbol] = position

                except Exception as e:
                    trading_logger.log_error(
                        f"Error processing order key {key}",
                        e,
                        {"key": key, "int_order_id": int_order_id, "error": str(e)},
                    )
                    continue

            return positions

        except Exception as e:
            context = create_error_context(int_order_id=int_order_id, keys_count=len(keys), error=str(e))
            raise OrderError(f"Failed to get positions from broker order IDs: {str(e)}", context)

    def _expire_derivative(
        broker: BrokerBase, internal_order_id: str, symbol: str, size: int, market_close_time: str
    ) -> bool:
        expire = False
        expiry = symbol.split("_")[2]
        if (
            expiry is not None
            and len(expiry) > 0
            and (
                expiry < get_tradingapi_now().strftime("%Y%m%d")
                or (
                    expiry == get_tradingapi_now().strftime("%Y%m%d")
                    and get_tradingapi_now().strftime("%H:%M:%S") > market_close_time
                )
            )
        ):
            expire = True
            entry_keys = hget_with_default(broker, int_order_id, "entry_keys", "").split()
            entry_key = entry_keys[0]
            exchange = hget_with_default(broker, entry_key, "exchange", "NSE")
            exit_price = broker.get_quote(symbol, exchange).last
            exit_quantity = abs(size)
            sq_off_order = Order(
                order_type="SELL" if size > 0 else "COVER",
                quantity=exit_quantity,
                exchange="SQUAREOFF",
                exchange_segment="D",
                is_intraday=False,
                price=exit_price,
                ahplaced="N",
            )
            sq_off_order.exch_order_id = (
                str(secrets.randbelow(9000000000000000 - 1000000000000000) + 1000000000000000) + "P"
            )
            sq_off_order.remote_order_id = get_tradingapi_now().strftime("%Y%m%d%H%M%S%f")[:-4]
            sq_off_order.broker_order_id = str(secrets.randbelow(90000000) + 10000000) + "P"
            sq_off_order.orderRef = internal_order_id
            sq_off_order.internal_order_id = internal_order_id
            sq_off_order.message = "Expiration Paper Order"
            sq_off_order.status = OrderStatus.FILLED
            sq_off_order.long_symbol = symbol
            sq_off_order.scrip_code = hget_with_default(broker, entry_key, "scrip_code", "0")
            sq_off_order.price_type = "LMT*1"
            _process_broker_order_update(broker, sq_off_order, symbol)
        return expire

    positions: Dict[str, Position] = {}
    entry = True if "entry" in side else False
    exit = True if "exit" in side else False
    if entry:
        entry_keys = hget_with_default(broker, int_order_id, "entry_keys", "").split()
        positions = _get_position_by_broker_order_ids(entry_keys, positions)
    if exit:
        exit_keys = hget_with_default(broker, int_order_id, "exit_keys", "").split()
        positions = _get_position_by_broker_order_ids(exit_keys, positions)
    for position in positions.values():
        position.price = position.value / position.size if position.size != 0 else 0
    if exclude_zero:
        positions = {key: value for key, value in positions.items() if value.size != 0}
    if entry and exit:
        expired_contracts = []
        # check for expired contracts
        for symbol, position in positions.items():
            if position.size != 0:
                expired = _expire_derivative(broker, int_order_id, symbol, position.size, market_close_time)
                if expired:
                    expired_contracts.append(symbol)
        positions = {key: value for key, value in positions.items() if value.symbol not in expired_contracts}
    return positions


@log_execution_time
@validate_inputs(combo_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0)
def parse_combo_symbol(combo_symbol):
    """Parse a combo symbol string into individual symbols and quantities.

    Args:
        combo_symbol: Symbol string that may contain combo notation

    Returns:
        OrderedDict: Dictionary mapping symbol names to quantities

    Raises:
        ValidationError: If input parameter is invalid
        SymbolError: If combo symbol parsing fails
    """
    try:
        result = OrderedDict()

        if "?" not in combo_symbol:
            result[combo_symbol] = 1
            trading_logger.log_debug("Parsed simple symbol", {"combo_symbol": combo_symbol, "result": dict(result)})
            return result

        symbols = combo_symbol.split(":")

        if len(symbols) == 0:
            context = create_error_context(combo_symbol=combo_symbol, split_result=symbols)
            raise SymbolError("Empty combo symbol after splitting", context)

        for i, symbol in enumerate(symbols):
            try:
                if "?" not in symbol:
                    context = create_error_context(combo_symbol=combo_symbol, symbol=symbol, index=i)
                    raise SymbolError(f"Symbol {symbol} does not contain quantity separator '?'", context)

                name, quantity_str = symbol.split("?", 1)

                if not name.strip():
                    context = create_error_context(combo_symbol=combo_symbol, symbol=symbol, index=i)
                    raise SymbolError(f"Empty symbol name in combo {combo_symbol}", context)

                try:
                    quantity = int(quantity_str)
                except ValueError as e:
                    context = create_error_context(
                        combo_symbol=combo_symbol, symbol=symbol, quantity_str=quantity_str, index=i
                    )
                    raise SymbolError(f"Invalid quantity '{quantity_str}' in combo symbol", context)

                result[name.strip()] = quantity

            except SymbolError:
                raise
            except Exception as e:
                context = create_error_context(combo_symbol=combo_symbol, symbol=symbol, index=i, error=str(e))
                raise SymbolError(f"Error parsing symbol '{symbol}' in combo: {str(e)}", context)

        trading_logger.log_debug(
            "Parsed combo symbol", {"combo_symbol": combo_symbol, "symbols_count": len(result), "result": dict(result)}
        )

        return result

    except (ValidationError, SymbolError):
        raise
    except Exception as e:
        context = create_error_context(combo_symbol=combo_symbol, error_type=type(e).__name__)
        raise SymbolError(f"Unexpected error parsing combo symbol: {str(e)}", context)


def get_exit_candidates(broker: BrokerBase, strategy: str, long_symbol: str, side: str) -> list:
    """Retreive exit candidates for a specified strategy, side and entry order type

    Args:
        strategy (str): strategy name
        long_symbol (str): long symbol
        side (str): 'SELL' if original position is long. 'COVER' if original position is short. side should be the order type that will result in position closure.

    Returns:
        list: list of internal order ids that have matching positions for exit
    """
    if side == "SELL":
        broker_entry_side = "BUY"
    elif side == "COVER":
        broker_entry_side = "SHORT"
    else:
        broker_entry_side = "UNDEFINED"
    int_order_ids = get_orders_by_symbol(broker, strategy, long_symbol, broker_entry_side)
    # get keys that have an open position. This is equal to keys that have an exit order that have a status of
    # filled
    if int_order_ids:
        int_order_ids = [
            int_order_id for int_order_id in int_order_ids if len(get_open_position_by_order(broker, int_order_id)) != 0
        ]
    else:
        # Handle the case where int_order_ids is empty, if needed
        pass
    # order keys to support fifo, key with lower integer component should be first
    int_order_ids.sort(key=lambda pair: pair.split("_")[1])
    logger.info("Exit Candidates -->" + ",".join(int_order_ids))
    return int_order_ids


@log_execution_time
@validate_inputs(
    symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_limit_price(
    broker: BrokerBase,
    price_type=None,
    order_type=None,
    symbol=None,
    price_broker: Optional[List[BrokerBase]] = None,
    exchange="NSE",
    mds: Optional[str] = None,
):
    """Get limit price based on price type and market data.

    Args:
        broker: Broker instance
        price_type: Type of price calculation (LMT, BID, ASK, MKT, etc.)
        order_type: Type of order (BUY, SELL, SHORT, COVER)
        symbol: Trading symbol
        price_broker: Optional list of brokers for price data
        exchange: Exchange name
        mds: Market data service channel name (str). If None or empty, uses broker quotes. 
             If provided (e.g., "mds"), uses market data service with that channel.

    Returns:
        float: Calculated limit price

    Raises:
        ValidationError: If input parameters are invalid
        MarketDataError: If price calculation fails
    """
    exchange = broker.map_exchange_for_api(symbol, exchange)
    if price_broker is None:
        price_broker = [broker]
    # quantity = order.quantity if order.order_type=='BUY' or order.order_type=='COVER' else order.quantity*-1
    ref_price = float("nan")
    pattern_plus = r"(LMT|BID|ASK)\s*\+\s*([+-]?\d+(\.\d+)?)"
    pattern_minus = r"(LMT|BID|ASK)\s*\-\s*([+-]?\d+(\.\d+)?)"
    pattern_mult = r"(LMT|BID|ASK)\s*\*\s*([+-]?\d+(\.\d+)?)"
    match_plus = re.match(pattern_plus, str(price_type))
    match_minus = re.match(pattern_minus, str(price_type))
    match_mult = re.match(pattern_mult, str(price_type))
    if price_type in ["LMT", "BID", "ASK"] or match_plus or match_minus or match_mult:
        ticker = get_price(price_broker, symbol, checks=["bid", "ask"], exchange=exchange, mds=mds)
        if "BID" in str(price_type):
            ref_price = ticker.bid
        elif "ASK" in str(price_type):
            ref_price = ticker.ask
        else:
            ref_price = (ticker.bid + ticker.ask) / 2
        if match_plus:
            ref_price = ref_price + float(match_plus.group(2))
        elif match_minus:
            ref_price = ref_price - float(match_minus.group(2))
        elif match_mult:
            ref_price = ref_price * float(match_mult.group(2))
        else:
            pass
        if not math.isnan(ref_price):
            ref_price = int(
                ref_price / broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05)
            ) * broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05)
            if ref_price <= 0:
                ref_price = (
                    max(
                        broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05),
                        ticker.bid + broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05),
                    )
                    if order_type in ["BUY", "COVER"]
                    else ticker.ask - broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05)
                )
    elif price_type == "MKT":
        ticker = get_price(price_broker, symbol, checks=["bid", "ask"], exchange=exchange, mds=mds)
        if order_type in ["BUY", "COVER"]:
            ref_price = ticker.ask
            # lmt_price = int(ticker.ask / 2) - 1
        else:
            ref_price = ticker.bid
            # lmt_price = int(ticker.bid * 2) + 1
        if ref_price <= 0:
            ref_price = (
                max(
                    broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05),
                    ticker.bid + broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05),
                )
                if order_type in ["BUY", "COVER"]
                else ticker.ask - broker.exchange_mappings[exchange]["contracttick_map"].get(symbol, 0.05)
            )
    elif isinstance(price_type, float) or isinstance(price_type, int):
        ref_price = price_type
    elif isinstance(price_type, list):
        # Handle list inputs (e.g., from scalping.py which returns [mid_price])
        # Extract first element if it's a single-element list, or first element of first sublist
        if price_type and isinstance(price_type[0], list):
            # Nested list: [[price1], [price2]] -> extract first price from first sublist
            ref_price = price_type[0][0] if price_type[0] else 0
        elif price_type:
            # Simple list: [price] -> extract first price
            ref_price = price_type[0]
        else:
            ref_price = 0
    else:
        ref_price = 0
    return ref_price


def get_combo_sub_order_type(order: Order, sub_order_qty: int) -> str:
    """Get the order type  for combo suborder

    Args:
        order (Order): combo order
        sub_order_qty (int): suborder quantity as per combo definition when buying 1 unit of combo

    Returns:
        str: order type of ['BUY','SELL','SHORT','COVER']
    """
    if "?" not in order.long_symbol:
        return order.order_type

    if order.order_type in ["BUY", "SHORT"]:
        if sub_order_qty > 0:
            return "BUY"
        else:
            return "SHORT"

    if order.order_type in ["SELL", "COVER"]:
        if sub_order_qty > 0:
            return "COVER"
        else:
            return "SELL"
    return "UNDEFINED"


@log_execution_time
@validate_inputs(strategy=lambda x: isinstance(x, str) and len(x.strip()) > 0, paper=lambda x: isinstance(x, bool))
def transmit_entry_order(
    broker: BrokerBase, strategy: str, order: Order, paper: bool = True, price_broker: Optional[List[BrokerBase]] = None
) -> str:
    """Transmit entry order to broker with enhanced error handling.

    Args:
        broker: Broker instance
        strategy: Strategy name
        order: Order object to transmit
        paper: Whether this is a paper trade
        price_broker: Optional list of brokers for price data

    Returns:
        str: Internal order ID

    Raises:
        ValidationError: If input parameters are invalid
        OrderError: If order transmission fails
    """
    """Entry order sent to broker

    Args:
        strategy (str): strategy
        long_symbol (str): long symbol
        order (Order): order object
        paper (bool): If true (default), places simulated order

    Returns:
        str: internal order id
    """
    # Enforce: combo orders (with ":") must have order_type "BUY"
    if ":" in order.long_symbol and order.order_type != "BUY":
        error_msg = f"Combo order {order.long_symbol} must have order_type 'BUY'. Got '{order.order_type}' instead."
        logger.error(error_msg)
        raise OrderError(error_msg, {"symbol": order.long_symbol, "order_type": order.order_type})

    if order.price is None:
        error_msg = f"Order not placed. Price was set as None for symbol: {order.long_symbol}"
        logger.error(error_msg)
        raise OrderError(error_msg, {"symbol": order.long_symbol})
    if price_broker is None:
        price_broker = [broker]
    if not order.internal_order_id:
        next_order_id = broker.starting_order_ids_int.get(strategy, 1)
        broker.starting_order_ids_int[strategy] = next_order_id + 1
        internal_order_id = strategy + "_" + str(next_order_id)
        order.internal_order_id = internal_order_id
        order.paper = paper
    order.paper = paper

    if "?" in order.long_symbol:
        combo_symbols = parse_combo_symbol(order.long_symbol)
        symbols = list(combo_symbols.keys())
        quantities = [
            x * order.quantity if order.order_type == "BUY" else -x * order.quantity for x in combo_symbols.values()
        ]
        if isinstance(order.price_type, list):
            price_types = order.price_type
        else:
            price_types = [order.price_type] * len(symbols)
        # price_types = [0] * len(symbols) if paper is False else ["MKT"] * len(symbols)
        additional_infos = [""] * len(combo_symbols)
        trigger_prices = [order.trigger_price] * len(symbols)
    else:
        symbols = [order.long_symbol]
        quantities = [order.quantity if order.order_type == "BUY" else -1 * order.quantity]
        price_types = [order.price_type]
        additional_infos = [order.additional_info]
        trigger_prices = [order.trigger_price]
    symbols, quantities, price_types, additional_infos, trigger_prices = _sort_list(
        symbols, quantities, price_types, additional_infos, trigger_prices=trigger_prices
    )
    int_order_id = ""
    for symbol, quantity, price_type, additional_info, trigger_price in zip(
        symbols, quantities, price_types, additional_infos, trigger_prices
    ):
        temp_order = deepcopy(order)
        temp_order.order_type = get_combo_sub_order_type(order, quantity)
        temp_order.quantity = abs(quantity)
        temp_order.long_symbol = symbol
        temp_order.trigger_price = temp_order._convert_to_float(trigger_price, "trigger_price")
        temp_order.is_stoploss_order = temp_order.is_stoploss_order or not math.isnan(temp_order.trigger_price)
        lmt_price = get_limit_price(
            broker,
            price_type=price_type,
            order_type=temp_order.order_type,
            symbol=symbol,
            price_broker=price_broker,
            exchange=temp_order.exchange,
        )
        temp_order.price = lmt_price
        out = broker.place_order(temp_order)
        int_order_id = _process_broker_order_update(broker, out, order.long_symbol)
    return int_order_id


def transmit_exit_order(
    broker: BrokerBase,
    strategy: str,
    order: Order,
    validate_db_position=True,
    paper=True,
    price_broker: Optional[List[BrokerBase]] = None,
    int_order_id: str = "",
) -> None:
    """Exit order sent to broker

    Args:
        strategy (str): name of strategy
        long_symbol (str): symbol to exit
        order (Order): order object
        paper (bool): If true (default), places simulated order
        int_order_id (str) : if specified, exits are effected from the specified order id
    """

    def cancel_internal_order_id(
        broker: BrokerBase,
        internal_order_id: str,
    ):
        order_mapping = broker.redis_o.hgetall(internal_order_id)
        entry_keys = order_mapping.get("entry_keys")
        if entry_keys is not None:
            entry_keys_list = entry_keys.split(" ")
            for ek in entry_keys_list:
                broker.cancel_order(broker_order_id=ek)
        exit_keys = order_mapping.get("exit_keys")
        if exit_keys is not None:
            exit_keys_list = exit_keys.split(" ")
            for ek in exit_keys_list:
                broker.cancel_order(broker_order_id=ek)

    logger.info(
        f"Exit order. Symbol: {order.long_symbol}{chr(10)} Side: {order.order_type}. Quantity: {order.quantity}"
    )
    exit_candidates = (
        [int_order_id] if int_order_id else get_exit_candidates(broker, strategy, order.long_symbol, order.order_type)
    )
    order.paper = paper
    if price_broker is None:
        price_broker = [broker]

    if "?" in order.long_symbol:
        combo_symbols = parse_combo_symbol(order.long_symbol)
        exit_symbols = list(combo_symbols.keys())
        exit_quantities_required = [
            x * order.quantity if order.order_type == "COVER" else -x * order.quantity for x in combo_symbols.values()
        ]

        if isinstance(order.price_type, list):
            price_types = order.price_type
        else:
            price_types = [order.price_type] * len(exit_symbols)
        trigger_prices = [order.trigger_price] * len(exit_symbols)
        # price_types = [0] * len(exit_symbols) if paper is False else ["MKT"] * len(exit_symbols)

    else:
        exit_symbols = [order.long_symbol]
        exit_quantities_required = [order.quantity if order.order_type == "COVER" else -1 * order.quantity]
        price_types = order.price_type if isinstance(order.price_type, list) else [order.price_type]
        trigger_prices = [order.trigger_price] if not isinstance(order.trigger_price, list) else order.trigger_price
    quantities_remaining = exit_quantities_required
    additional_infos = order.additional_info if isinstance(order.additional_info, list) else [order.additional_info]

    (
        exit_symbols,
        quantities_remaining,
        price_types,
        additional_infos,
        trigger_prices,
    ) = _sort_list(exit_symbols, quantities_remaining, price_types, additional_infos, trigger_prices=trigger_prices)

    if len(exit_candidates) > 0 and validate_db_position:
        for internal_order_id in exit_candidates:
            if any(quantities_remaining):
                cancel_internal_order_id(broker, internal_order_id)
                actual_positions = get_open_position_by_order(broker, internal_order_id)
                loop_quantities_remaining = deepcopy(quantities_remaining)
                for exit_symbol, quantity, price_type, additional_info, trigger_price in zip(
                    exit_symbols, loop_quantities_remaining, price_types, additional_infos, trigger_prices
                ):
                    actual_position = actual_positions.get(exit_symbol, Position())
                    if quantity != 0 and quantity * actual_position.size < 0:
                        order_new = deepcopy(order)
                        order_new.additional_info = _merge_additional_info(order_new.additional_info, additional_info)
                        order_new.long_symbol = exit_symbol
                        order_new.quantity = min(abs(quantity), abs(actual_position.size))
                        order_new.order_type = get_combo_sub_order_type(order, quantity)
                        order_new.internal_order_id = internal_order_id
                        order_new.orderRef = internal_order_id
                        order_new.trigger_price = order_new._convert_to_float(trigger_price, "trigger_price")
                        if not math.isnan(order_new.trigger_price):
                            order_new.is_stoploss_order = True
                        lmt_price = get_limit_price(
                            broker,
                            price_type=price_type,
                            order_type=order_new.order_type,
                            symbol=exit_symbol,
                            price_broker=price_broker,
                            exchange=order_new.exchange,
                        )
                        order_new.price = lmt_price
                        out = broker.place_order(order=order_new)
                        _process_broker_order_update(broker, out, order.long_symbol)
                        calc_index = exit_symbols.index(exit_symbol)
                        if order_new.order_type in ["COVER"]:
                            quantities_remaining[calc_index] -= order_new.quantity
                        else:
                            quantities_remaining[calc_index] += order_new.quantity
    else:
        logger.info(
            f"No exit candidates found for symbol: {order.long_symbol} and order: {order.order_type} and strategy: "
            f"{strategy}"
        )


def calculate_extra_combo_positions(order_position: Dict[str, Position], base_position: Dict[str, int]) -> dict:
    """Calculates extra positions outside completed combos.

    Args:
        order_position (dict): Positions of symbols within a combo.
        base_position (dict): Baseline combo position making 1 combo unit.

    Returns:
        dict: Dictionary with incomplete details, total in-progress combos, and completed combos.
    """
    # Calculate complete multiples and determine first sign
    complete_multiples = []
    for symbol, base_pos in base_position.items():
        actual_position = order_position.get(symbol, Position())
        complete_multiples.append(actual_position.size / base_pos)

    # Determine the sign of the first non-zero multiple
    first_sign = 1  # Default to positive if all are zeros
    for x in complete_multiples:
        if x != 0:
            first_sign = int(math.copysign(1, x))
            break

    # Check if all multiples are on the same side (considering 0 as positive or negative)
    all_same_side = all(int(math.copysign(1, x)) == first_sign or x == 0 for x in complete_multiples)

    if not all_same_side:
        raise ValueError("Combo Order does not have consistent fill sides")

    # Calculate completed and in-progress combos
    abs_multiples = [abs(m) for m in complete_multiples]
    completed = math.floor(min(abs_multiples))
    in_progress = math.ceil(max(abs_multiples))

    # Adjust completed and in-progress based on sign of first non-zero multiple
    completed *= first_sign
    in_progress *= first_sign

    # Calculate extra positions
    out = {}
    for symbol, base_pos in base_position.items():
        actual_position = order_position.get(symbol, Position())
        extra_position = actual_position.size - base_pos * completed
        if extra_position != 0:
            out[symbol] = extra_position

    return {"incomplete_details": out, "total_in_progress": in_progress, "total_completed": completed}


def _process_broker_order_update(broker: BrokerBase, order: Order, long_symbol: str) -> str:
    """Processes broker  response to a fresh order (entry/exit)

    Args:
        broker (BrokerBase): broker instance
        order (Order): Order
        long_symbol (str): combo symbol if combo order, else actual symbol

    Returns:
        str: internal order id
    """
    if order is None:
        logger.error(f"Order Not placed as None was received. Mother symbol: {long_symbol}. Are you logged in??")
        return ""
    if order.status in [OrderStatus.REJECTED, OrderStatus.UNDEFINED]:
        logger.error(
            f"Order not placed for {order.long_symbol}. Status was {order.status}. Message was {order.message}. Mother symbol was: {long_symbol}"
        )
        return ""

    if order.order_type in ["BUY", "SHORT"]:
        current_orders = broker.redis_o.hget(order.internal_order_id, "entry_keys")
        new_orders = (
            str(order.broker_order_id) if current_orders is None else current_orders + " " + str(order.broker_order_id)
        )
        broker.redis_o.hset(order.orderRef, "entry_keys", new_orders)
        broker.redis_o.hset(order.internal_order_id, "long_symbol", long_symbol)

    if order.order_type in ["SELL", "COVER"]:
        current_orders = broker.redis_o.hget(order.internal_order_id, "exit_keys")
        new_orders = (
            str(order.broker_order_id) if current_orders is None else current_orders + " " + str(order.broker_order_id)
        )
        broker.redis_o.hset(order.orderRef, "exit_keys", new_orders)
        broker.redis_o.hset(order.internal_order_id, "long_symbol", long_symbol)

    broker.redis_o.hmset(str(order.broker_order_id), {key: str(val) for key, val in order.to_dict().items()})
    update_order_status(broker, order.internal_order_id, str(order.broker_order_id), eod=False)
    return order.internal_order_id


def exit_is_expiration(broker: BrokerBase, internal_order_id: str) -> bool:
    """Are exit key(s) all expired

    Args:
        broker (BrokerBase): broker
        internal_order_id (str): internal order id

    Returns:
        bool: True if exit key(s) exits and are expiration(s). Else False
    """
    exit_keys = hget_with_default(broker, internal_order_id, "exit_keys", "").split()
    if len(exit_keys) == 0:
        return False
    out = True
    for ek in exit_keys:
        message = broker.redis_o.hget(ek, "message")
        if message and "expir" not in message.lower():
            out = out and False
    return out


def delete_broker_order_id(
    broker,
    internal_order_id: str,
    broker_order_id: str = "0",
):
    """Safely removes broker_order_id from redis and updates internal_order_id with keys

    Args:
        internal_order_id (str): internal  order id
        broker_order_id (str): broker order id
    """
    if broker_order_id == "0":
        logger.info(f"Deleting internal order id: {internal_order_id} and linked broker order ids")
        order_mapping = broker.redis_o.hgetall(internal_order_id)
        entry_keys = order_mapping.get("entry_keys")
        if entry_keys is not None:
            entry_keys_list = entry_keys.split(" ")
            for ek in entry_keys_list:
                logger.info(f"Deleting order. Order {ek}")
                broker.redis_o.delete(ek)
        exit_keys = order_mapping.get("exit_keys")
        if exit_keys is not None:
            exit_keys_list = exit_keys.split(" ")
            for ek in exit_keys_list:
                logger.info(f"Deleting order. Order {ek}")
                broker.redis_o.delete(ek)
        logger.info(f"Deleting order. Order {internal_order_id}")
        broker.redis_o.delete(internal_order_id)
    else:
        order_mapping = broker.redis_o.hgetall(internal_order_id)
        if len(order_mapping) == 0:
            logger.info(f"Internal Order ID: {internal_order_id} not found")
            return
        # check if removal is from entry
        if str(broker_order_id) in order_mapping.get("entry_keys"):
            entry_keys = order_mapping.get("entry_keys").split()
            entry_keys.remove(str(broker_order_id))
            logger.info(f"Deleting broker order id: {broker_order_id}")
            exit_keys = hget_with_default(broker, internal_order_id, "exit_keys", "").split()
            if len(entry_keys) == 0 and len(exit_keys) > 0:
                if exit_is_expiration(broker, internal_order_id):
                    pipe = broker.redis_o.pipeline()
                    pipe.delete(internal_order_id)
                    pipe.delete(broker_order_id)
                    for ek in exit_keys:
                        pipe.delete(ek)
                    pipe.execute()
                    pipe.reset()
                    logger.info(
                        f"Deleting order and internal_order_id. Order {broker_order_id}. Internal Order ID: {internal_order_id}"
                    )
                else:
                    logger.error(f"Unexpected exit key found for order id {internal_order_id}")
            else:
                order_mapping["entry_keys"] = " ".join(entry_keys)
                pipe = broker.redis_o.pipeline()
                pipe.delete(internal_order_id)
                if not order_mapping["entry_keys"].strip() == "":
                    pipe.hmset(internal_order_id, order_mapping)
                pipe.delete(broker_order_id)
                pipe.execute()
                pipe.reset()
                logger.info(
                    f"Deleting unfilled order and updating internal_order_id. Order {broker_order_id}. Internal Order ID: {internal_order_id}"
                )
        elif order_mapping.get("exit_keys") is not None and str(broker_order_id) in order_mapping.get("exit_keys"):
            # check if removal is from exit
            exit_keys = order_mapping.get("exit_keys").split()
            exit_keys.remove(str(broker_order_id))
            if len(exit_keys) == 0:
                order_mapping.pop("exit_keys")
            else:
                order_mapping["exit_keys"] = " ".join(exit_keys)
            pipe = broker.redis_o.pipeline()
            pipe.delete(internal_order_id)
            pipe.hmset(internal_order_id, order_mapping)
            pipe.delete(broker_order_id)
            pipe.execute()
            pipe.reset()
            logger.info(
                f"Deleting  order and updating internal_order_id. Order {broker_order_id}. Internal Order ID: {internal_order_id}"
            )


def update_order_status(
    broker: BrokerBase, internal_order_id: str, broker_order_id: str, eod: bool = False
) -> OrderInfo:
    """
    if eod is false, [price,quantity,status] is updated to [fill_price/order_price,order_size,orderstatus]
    if eod is true, [price,quantity,status] is updated to [fill_price,fill_size,orderstatus]. in addition to eod functionality, order is deleted if fill_size is zero
    """
    # Debug: Log before calling get_order_info, especially for exit orders
    try:
        # Check if this is an exit order by checking if broker_order_id is in exit_keys
        exit_keys = hget_with_default(broker, internal_order_id, "exit_keys", "").split()
        is_exit_order = broker_order_id in exit_keys
        if is_exit_order:
            trading_logger.log_debug("Calling get_order_info for exit order in update_order_status", {
                "internal_order_id": internal_order_id,
                "broker_order_id": broker_order_id,
                "eod": eod,
                "exit_keys": exit_keys,
            })
    except Exception:
        pass  # Don't fail if we can't check exit_keys
    
    try:
        fills = broker.get_order_info(broker_order_id=broker_order_id)
        
        # Debug: Log result of get_order_info for exit orders
        try:
            exit_keys = hget_with_default(broker, internal_order_id, "exit_keys", "").split()
            is_exit_order = broker_order_id in exit_keys
            if is_exit_order:
                trading_logger.log_debug("get_order_info result for exit order", {
                    "internal_order_id": internal_order_id,
                    "broker_order_id": broker_order_id,
                    "status": fills.status.name if hasattr(fills.status, 'name') else str(fills.status),
                    "fill_size": fills.fill_size if hasattr(fills, 'fill_size') else None,
                    "fill_price": fills.fill_price if hasattr(fills, 'fill_price') else None,
                    "order_size": fills.order_size if hasattr(fills, 'order_size') else None,
                    "order_price": fills.order_price if hasattr(fills, 'order_price') else None,
                })
        except Exception:
            pass  # Don't fail if we can't log
    except Exception as e:
        trading_logger.log_error(
            f"Error calling get_order_info in update_order_status",
            e,
            {"internal_order_id": internal_order_id, "broker_order_id": broker_order_id},
        )
        raise
    required_attributes = [
        "broker",
        "status",
        "fill_size",
        "fill_price",
        "order_price",
        "order_size",
        "exchange_order_id",
    ]
    for attr in required_attributes:
        if not hasattr(fills, attr) or getattr(fills, attr) in [None, "0"]:
            logger.error(
                f"Missing or invalid attribute {attr} in order information for broker_order_id: {broker_order_id}"
            )
            return fills

    if broker.broker != fills.broker:
        return fills
    logger.debug(f"Order information for {internal_order_id}, broker_order_id: {broker_order_id} {fills}")
    if fills.status == OrderStatus.CANCELLED and fills.fill_size > 0:
        broker.redis_o.hset(broker_order_id, "price", str(fills.fill_price))
        broker.redis_o.hset(broker_order_id, "quantity", str(fills.fill_size))
        broker.redis_o.hset(broker_order_id, "status", fills.status.name)
        broker.redis_o.hset(broker_order_id, "exch_order_id", fills.exchange_order_id)
    elif (fills.status == OrderStatus.CANCELLED and fills.fill_size == 0) or (fills.status == OrderStatus.REJECTED):
        delete_broker_order_id(broker, internal_order_id, broker_order_id)
    elif eod:
        if fills.fill_size > 0:
            broker.redis_o.hset(broker_order_id, "price", str(fills.fill_price))
            broker.redis_o.hset(broker_order_id, "quantity", str(fills.fill_size))
            broker.redis_o.hset(broker_order_id, "status", fills.status.name)
            broker.redis_o.hset(broker_order_id, "exch_order_id", fills.exchange_order_id)
        else:
            delete_broker_order_id(broker, internal_order_id, broker_order_id)
    else:
        if fills.status == OrderStatus.HISTORICAL:
            return fills
        # Check if fill_price is 0 or non-numeric
        try:
            fill_price_numeric = float(fills.fill_price)
            is_invalid_price = fill_price_numeric == 0
        except (ValueError, TypeError):
            is_invalid_price = True
        if is_invalid_price:
            broker.redis_o.hset(broker_order_id, "price", str(fills.order_price))
            broker.redis_o.hset(broker_order_id, "exch_order_id", fills.exchange_order_id)
        else:
            broker.redis_o.hset(broker_order_id, "price", str(fills.fill_price))
            broker.redis_o.hset(broker_order_id, "exch_order_id", fills.exchange_order_id)
        broker.redis_o.hset(broker_order_id, "quantity", str(fills.order_size))
        broker.redis_o.hset(broker_order_id, "status", fills.status.name)
    return fills


@log_execution_time
@validate_inputs(
    symbol_name=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_linked_options(broker: BrokerBase, symbol_name: str, expiry: str = "", exchange: str = "NSE") -> list[str]:
    """Get option chain with enhanced error handling.

    Args:
        broker: Broker instance
        symbol_name: Symbol name either long_name or short_name
        expiry: Optional expiry formatted as yyyymmdd
        exchange: Exchange name

    Returns:
        list[str]: List containing longnames of available option symbols

    Raises:
        ValidationError: If input parameters are invalid
        SymbolError: If symbol mapping fails
    """
    symbol_name_opt = symbol_name.replace("_STK_", "_OPT_").replace("_IND_", "_OPT_")
    exchange = broker.map_exchange_for_api(symbol_name_opt, exchange)
    symbols = list(broker.exchange_mappings[exchange]["symbol_map"].keys())
    short_symbol = symbol_name.split("_")[0]
    linked_options = []
    if expiry:
        expiry, _ = valid_datetime(expiry, "%Y%m%d")
        symbols = [s for s in symbols if s.startswith(f"{short_symbol}_OPT_{expiry}")]
    else:
        symbols = [s for s in symbols if s.startswith(f"{short_symbol}_OPT_")]
    linked_options = list(symbols)
    return linked_options


def get_linked_futures(symbol_name: str, expiry: str = "", file_path: str = "") -> list[str]:
    """Get futures chain

    Args:
        symbol_name (str): symbol name either long_name or short_name
        expiry (str, optional): Expiry formatted as yyyymmdd. Defaults to None.
        file_path (str, optional): Full path to symbols file. Defaults to None.

    Returns:
        list[str]: List containing longnames of available option symbols
    """
    if file_path:
        symbols = pd.read_csv(file_path)
    else:
        return []
    short_symbol = symbol_name.split("_")[0]
    linked_futures = []
    if expiry:
        symbols = symbols.loc[symbols["long_symbol"].str.contains(f"\\b{short_symbol}_FUT_{expiry}"), "long_symbol"]
    else:
        symbols = symbols.loc[symbols["long_symbol"].str.contains(f"\\b{short_symbol}_FUT_"), "long_symbol"]
    linked_futures = list(symbols)
    return linked_futures


def get_latest_symbol_file(directory_path="/home/psharma/onedrive/data/static/symbols"):
    pattern = "*_symbols.csv"
    files = glob.glob(os.path.join(directory_path, pattern))
    dates = [int(os.path.basename(file).split("_")[0]) for file in files]
    latest_index = dates.index(max(dates))
    latest_file = files[latest_index] if files else None
    return latest_file


def get_universe(file_path: str = "", product_types=["OPT", "FUT"], exchanges=["N", "B"], expiry=None):
    """Get list of all symbols for a given product type and exchange

    Args:
        file_path (str, optional): _description_. Defaults to empty string.
        product_types (list, optional): _description_. Defaults to ["OPT", "FUT"].
        exchanges (list, optional): _description_. Defaults to ["N", "B"].
        expiry (str, optional): expiry formatted as yyyymmdd

    Returns:
        list: list of long names
    """
    if not file_path:
        file_path = get_latest_symbol_file()
    symbols = pd.read_csv(file_path)
    out: List[str] = []

    for exchange in exchanges:
        exchange = exchange[0]
        for prod_type in product_types:
            # Filter condition for prod_type and exchange
            condition = (symbols["long_symbol"].str.contains(f"_{prod_type}_")) & (symbols["Exch"] == exchange)

            # Add condition for expiry if it's not None
            if expiry is not None:
                condition &= symbols["long_symbol"].str.contains(expiry)

            temp = list(symbols.loc[condition, "long_symbol"])
            out = out + temp
    return out


def get_unique_short_symbol_names(exchange: str, sec_type: str, file_path: str = "") -> list[str]:
    """Get a list of all short symbol names for a specified exchange and security type.

    Args:
        exchange (str): Exchange name. Either 'N' for NSE or 'M' for MCX.
        sec_type (str): Security type like 'IND', 'STK', 'FUT', 'OPT'.
        file_path (str, optional): Path to symbols file. Defaults to an empty string.

    Returns:
        list[str]: List of short symbol names.
    """
    if not file_path:
        logger.error("File path is empty. Please provide a valid file path.")
        return []

    if os.path.isdir(file_path):
        # Get list of files in directory sorted by modification time in descending order
        files = sorted(os.listdir(file_path), reverse=True)
        if files:
            # Read CSV from the first file in the sorted list
            first_file = files[0]
            symbols = pd.read_csv(os.path.join(file_path, first_file))
        else:
            logger.error("No files found in the specified directory.")
            return []
    elif os.path.isfile(file_path):
        # Read CSV from the given file
        symbols = pd.read_csv(file_path)
    else:
        logger.error("Invalid file path provided.")
        return []

    # Ensure required columns exist
    if "Exch" not in symbols.columns or "long_symbol" not in symbols.columns:
        logger.error("Missing required columns 'Exch' or 'long_symbol' in the symbols file.")
        return []

    # Filter and extract unique short names
    filtered_symbols = symbols[(symbols.Exch == exchange) & (symbols.long_symbol.str.contains(f"_{sec_type}"))]
    short_names = filtered_symbols["long_symbol"].str.split("_").str[0].unique()
    return list(short_names)


@log_execution_time
@retry_on_error(max_retries=2, delay=1.0, backoff_factor=2.0)
@validate_inputs(
    long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_margin_zerodha(broker: BrokerBase, long_symbol: str, proxy: str = "", exchange="NSE") -> int:
    """Get margin from Zerodha for option contracts with enhanced error handling.

    Args:
        broker: Broker instance
        long_symbol: Long symbol name
        proxy: Optional proxy specified as ipaddress:port
        exchange: Exchange name

    Returns:
        int: Margin for contract

    Raises:
        ValidationError: If input parameters are invalid
        MarginError: If margin calculation fails
        NetworkError: If network request fails
    """
    # https://stackoverflow.com/questions/56591881/web-scrape-with-multiple-inputs-and-collect-total-margin-required
    symbol = long_symbol.split("_")[0]
    strike_price = float(long_symbol.split("_")[4])
    option_type = long_symbol.split("_")[3]
    expiry = long_symbol.split("_")[2]

    BASE_URL = "https://zerodha.com/margin-calculator/SPAN"
    scrip = symbol + dt.datetime.strptime(expiry, "%Y%m%d").strftime("%y%b").upper()
    z_option_type = "CE" if option_type == "CALL" else "PE"
    strike_price_str = ("%f" % strike_price).rstrip("0").rstrip(".")
    #     long_symbol = f'{symbol}_OPT_{expiry}_{option_type}_{strike_price}'
    quantity = broker.get_min_lot_size(long_symbol, exchange)
    payload = {
        "action": "calculate",
        "exchange[]": "NFO",
        "product[]": "OPT",
        "scrip[]": scrip,
        "option_type[]": z_option_type,
        "strike_price[]": strike_price_str,
        "qty[]": quantity,
        "trade[]": "sell",
    }
    try:
        session = Session()
        if proxy:
            proxies = {"http": "https://" + proxy}
            res = session.post(BASE_URL, data=payload, proxies=proxies)
        else:
            res = session.post(BASE_URL, data=payload)
        data = res.json()
        try:
            output = data["total"]["total"]
            output = int(output)
        except Exception:
            output = 1000000
        logger.info(f"margin for {long_symbol}:{output}")
        session.close()
        return output
    except Exception:
        logger.exception("Error")
        return 1000000


def get_margin_samco(long_symbol: str, driver_path: str = "", proxy: str = "") -> int:
    """Get margin from SAMCO for option contracts

    Args:
        long_symbol (str): long symbol
        driver_path (str, optional): path to firefox driver. Defaults to None.
        proxy (str, optional): proxy address, specified as ipaddress:port . Defaults to None.

    Returns:
        int: margin
    """
    output = 1000000
    strike = long_symbol.split("_")[4]
    type = long_symbol.split("_")[3].lower()
    expiry = long_symbol.split("_")[2]
    symbol = long_symbol.split("_")[0]

    driver = None
    try:
        margin_url = "https://www.samco.in/span"
        op = webdriver.FirefoxOptions()
        if not driver_path:
            driver_path = "/home/psharma/Downloads/geckodriver"
        if proxy:
            op.add_argument("--proxy-server=%s" % proxy)
        op.add_argument("--headless")
        driver = webdriver.Firefox(executable_path=driver_path, options=op)
        driver.get(margin_url)
        driver.implicitly_wait(10)
        reset = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//a[@class='close-icon sprite-icon'])[1]"))
        )
        driver.execute_script("arguments[0].click();", reset)
        # reset.click()
        exchange = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='exchange'])[1]"))
        )
        exchange.send_keys("NFO")
        product = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='product'])[1]"))
        )
        product.send_keys("Options")
        underlying = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='underlying'])[1]"))
        )
        underlying.send_keys(symbol)
        expiry_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='expiry'])[1]"))
        )
        expiry_formatted = dt.datetime.strptime(expiry, "%Y%m%d").strftime("%d%b%y").upper()
        expiry_element.send_keys(expiry_formatted)

        strike_1 = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='strike_price'])[1]"))
        )
        time.sleep(2)
        strike_1.send_keys(strike)

        sell = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//label[normalize-space()='Sell'])[1]"))
        )
        driver.execute_script("arguments[0].click();", sell)
        # sell.click()

        if type == "put":
            opt_type = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "(//select[@name='option'])[1]"))
            )
            opt_type.send_keys("PUTS")
        add = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//button[normalize-space()='Add'])[1]"))
        )
        driver.execute_script("arguments[0].click();", add)
        wait = 0
        while (
            int(
                "".join(
                    list(filter(str.isdigit, (driver.find_element("xpath", "(//span[@id='total_margin'])[1]").text)))
                )
            )
            == 0
            and wait < 20
        ):
            wait = wait + 1
            time.sleep(1)
        try:
            output = int(
                "".join(
                    list(filter(str.isdigit, (driver.find_element("xpath", "(//span[@id='total_margin'])[1]").text)))
                )
            )
        except Exception:
            output = 100000000
        logger.info(f"margin for {long_symbol}:{output}")
        if driver:
            driver.close()
    except Exception:
        logger.exception("Error")
        if driver:
            driver.close()
        if proxy is not None:
            proxy = get_free_proxy(driver_path)
    return output


def get_margin_5p(long_symbol, strike_price, type, expiry, driver_path: str = "", proxy: str = "") -> int:
    output = 1000000
    strike = long_symbol.split("_")[4]
    type = long_symbol.split("_")[3].lower()
    expiry = long_symbol.split("_")[2]
    symbol = long_symbol.split("_")[0]

    driver = None
    try:
        margin_url = "https://zerodha.com/margin-calculator/SPAN"
        op = webdriver.FirefoxOptions()
        if not driver_path:
            driver_path = "/home/psharma/Downloads/geckodriver"
        if proxy:
            op.add_argument("--proxy-server=%s" % proxy)
        op.add_argument("--headless")
        driver = webdriver.Firefox(executable_path=driver_path, options=op)
        driver.get(margin_url)
        driver.implicitly_wait(10)
        # driver.find_element_by_xpath("(//a[@id='btnReset'])[1]").click()
        reset = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "(//input[@id='reset'])[1]")))
        reset.click()
        segment = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='exchange'])[1]"))
        )
        segment.send_keys("NFO")
        product = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//select[@id='product'])[1]"))
        )
        product.send_keys("Options")
        symbol_box = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//span[@id='select2-scrip-container'])[1]"))
        )
        expiry_formatted = dt.datetime.strptime(expiry, "%Y%m%d").strftime("%d-%b-%y").upper()
        symbol_formatted = f"{symbol} {expiry_formatted}"
        symbol_box.click()
        symbol_txtbox = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//input[@role='textbox'])[1]"))
        )
        symbol_txtbox.send_keys(symbol_formatted)
        symbol_txtbox.send_keys(Keys.DOWN)
        symbol_txtbox.send_keys(Keys.ENTER)
        if type == "put":
            opt_type = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "(//select[@id='option_type'])[1]"))
            )
            opt_type.send_keys("Puts")
            # opt_type.send_keys(Keys.ENTER)
        else:
            pass
        strike = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "(//input[@id='strike_price'])[1]"))
        )
        strike_price = ("%f" % strike_price).rstrip("0").rstrip(".")
        strike.send_keys(strike_price)
        side = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "(//input[@value='sell'])[1]")))
        side.click()
        add = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "(//input[@value='Add'])[1]")))
        add.click()
        wait = 0
        while (
            int(
                "".join(
                    list(filter(str.isdigit, (driver.find_element("xpath", "(//span[@class='val total'])[1]").text)))
                )
            )
            == 0
            and wait < 20
        ):
            wait = wait + 1
            time.sleep(1)
        try:
            output = int(
                "".join(
                    list(filter(str.isdigit, (driver.find_element("xpath", "(//span[@class='val total'])[1]").text)))
                )
            )
        except Exception:
            output = 100000000
        logger.info(f"margin for {long_symbol}:{output}")
        if driver:
            driver.close()
    except Exception:
        if driver:
            driver.close()
        if proxy is not None:
            proxy = get_free_proxy(driver_path)
    return output


def get_free_proxy(driver_path: str = "") -> str:
    """Get working proxy. This requires firefox driver (geckodriver) to be available in specified driver path

    Args:
        driver_path (str, optional): _description_. Defaults to None.

    Returns:
        str: proxy as ipaddress:port
    """
    op = webdriver.FirefoxOptions()
    op.add_argument("--headless")
    if not driver_path:
        driver_path = "/home/psharma/Downloads/geckodriver"
    driver = webdriver.Firefox(executable_path=driver_path, options=op)
    driver.get("https://sslproxies.org")
    table = driver.find_element(By.TAG_NAME, "table")
    thead = table.find_element(By.TAG_NAME, "thead").find_elements(By.TAG_NAME, "th")
    tbody = table.find_element(By.TAG_NAME, "tbody").find_elements(By.TAG_NAME, "tr")

    headers = []
    for th in thead:
        headers.append(th.text.strip())

    proxies = []
    for tr in tbody:
        proxy_data = {}
        tds = tr.find_elements(By.TAG_NAME, "td")
        for i in range(len(headers)):
            proxy_data[headers[i]] = tds[i].text.strip()
        proxies.append(proxy_data)
    driver.close()

    selected_proxy_index = None
    for i in range(0, len(proxies)):
        try:
            if proxies[i]["Country"] in ["India", "Singapore"]:
                login_url = "https://login.5paisa.com/lwp"
                op = webdriver.FirefoxOptions()
                PROXY = proxies[i]["IP Address"] + ":" + proxies[i]["Port"]
                op.add_argument("--proxy-server=%s" % PROXY)
                op.add_argument("--headless")
                dr = webdriver.Firefox(executable_path=driver_path, options=op)
                dr.get(login_url)
                if WebDriverWait(dr, 5).until(EC.visibility_of_element_located((By.ID, "loginUser"))):
                    dr.close()
                    selected_proxy_index = i
                    break
        except Exception:
            logger.exception("Error")
            continue
    
    if selected_proxy_index is None:
        logger.warning("No working proxy found, returning default")
        return None
    
    logger.info(f"Proxy selected: {proxies[selected_proxy_index]}")
    return proxies[selected_proxy_index].get("IP Address", "127.0.0.1") + ":" + proxies[selected_proxy_index].get("Port", "0")


@log_execution_time
@validate_inputs(
    long_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_yield(broker: BrokerBase, long_symbol: str, proxy=None, exchange="NSE") -> list[float]:
    """Get yield to expiry of an option contract with enhanced error handling.

    Args:
        broker: Broker instance
        long_symbol: Long symbol name
        proxy: Optional proxy of form ipaddress:port
        exchange: Exchange name

    Returns:
        list[float]: [midprice, bid, ask, yield, margin]

    Raises:
        ValidationError: If input parameters are invalid
        MarketDataError: If price retrieval fails
        MarginError: If margin calculation fails
    """
    mid = 0.0
    margin = get_margin_zerodha(broker, long_symbol, proxy)
    size = broker.get_min_lot_size(long_symbol, exchange)
    quote = broker.get_quote(long_symbol, exchange)
    if quote.bid > 0 and quote.ask > 0:
        mid = (quote.bid + quote.ask) / 2
    mid = mid if mid > 0 else -1
    if size is not None and quote.bid > 0 and quote.ask > 0:
        max_return = (mid * size) / margin
        return [mid, quote.bid, quote.ask, max_return, margin]
    else:
        return [mid, quote.bid, quote.ask, 100, margin]


def review_price_history(symbol: str, exchange: str = "NSE", count: int = 1):
    """Retrieve the last `count` prices for a symbol from Redis."""
    global mds_history
    price_history_key = f"price_history:{symbol}~{exchange}"
    price_history = mds_history.zrevrange(price_history_key, 0, 0)
    return [json.loads(price) for price in price_history]


def listen_with_timeout(pubsub, symbol, timeout=60):
    start_time = time.time()
    while time.time() - start_time <= timeout:
        message = pubsub.get_message()  # Non-blocking
        if message:
            if message["type"] == "message" and json.loads(message["data"])["symbol"] == symbol:
                return message
        time.sleep(0.1)  # Prevent busy-waiting
    return None  # Return None if no valid message is received


def is_within_60_seconds(json_price):
    # Parse the timestamp string into a datetime object
    price = Price.from_dict(json_price)
    try:
        timestamp, _ = valid_datetime(price.timestamp)
    except ValueError:
        return False  # Return False if the timestamp format is invalid
    now = dt.datetime.now()
    return now <= timestamp + dt.timedelta(seconds=60)


def _get_price_mds(brok: BrokerBase, symbol: str, exchange: str = "NSE", channel: str = "mds"):
    mapped_exchange = brok.map_exchange_for_db(symbol, exchange)
    history = review_price_history(symbol, mapped_exchange)
    if len(history) == 1:
        if is_within_60_seconds(history[0]):
            return Price.from_dict(history[0])
    logger.info(
        f"symbol:{symbol} subscription received by {brok.broker.name}. exchange received in request:{exchange}, exchange sent in broker request:{mapped_exchange}"
    )
    # 'subscribe' or 'unsubscribe'
    out = get_price(brok, symbol, exchange=exchange)
    request = {
        "action": "subscribe",
        "symbol": symbol,
        "exchange": mapped_exchange,
        "channel": channel,
    }
    mds_history.publish("subscription_requests", json.dumps(request))
    return out


@log_execution_time
@retry_on_error(max_retries=3, delay=0.5, backoff_factor=2.0)
def get_price(
    brokers: Union[List[BrokerBase], BrokerBase],
    long_symbol: str,
    checks: List[str] = ["bid", "ask", "last", "prior_close"],
    attempts: int = 3,
    exchange="NSE",
    mds: Optional[str] = None,
) -> Price:
    def sum_prices(prices: List[Price]) -> Price:
        if not prices:
            return Price()  # Return an empty Price if the list is empty

        return Price(
            bid=sum(price.bid for price in prices),
            ask=sum(price.ask for price in prices),
            bid_volume=sum(price.bid_volume for price in prices),
            ask_volume=sum(price.ask_volume for price in prices),
            prior_close=sum(price.prior_close for price in prices),
            last=sum(price.last for price in prices),
            symbol=prices[0].symbol,  # Assuming symbol remains the same for all prices
        )

    if isinstance(brokers, BrokerBase):
        brokers = [brokers]

    if "?" not in long_symbol:
        out = Price()
        out.symbol = long_symbol
        for attempt in range(attempts):
            for broker in brokers:
                try:
                    mapped_exchange = broker.map_exchange_for_db(long_symbol, exchange)
                    if mds:
                        price = _get_price_mds(broker, long_symbol, mapped_exchange, channel=mds)
                    else:
                        price = broker.get_quote(long_symbol, mapped_exchange)
                    out.update(price)
                    if all(not math.isnan(getattr(out, check)) for check in checks):
                        trading_logger.log_debug(
                            f"Successfully retrieved price for {long_symbol}",
                            {
                                "symbol": long_symbol,
                                "exchange": exchange,
                                "attempt": attempt + 1,
                                "broker": broker.broker.name if hasattr(broker, "broker") else "unknown",
                            },
                        )
                        return out
                except Exception as e:
                    trading_logger.log_warning(
                        f"Failed to get price for {long_symbol} from {broker.broker.name if hasattr(broker, 'broker') else 'unknown'}",
                        {
                            "symbol": long_symbol,
                            "exchange": exchange,
                            "attempt": attempt + 1,
                            "broker": broker.broker.name if hasattr(broker, "broker") else "unknown",
                            "error": str(e),
                        },
                    )
                    continue
    else:
        out_list = []
        symbols = parse_combo_symbol(long_symbol)
        for symbol, size in symbols.items():
            out = Price()
            valid_price_found = False
            for attempt in range(attempts):
                for broker in brokers:
                    try:
                        mapped_exchange = broker.map_exchange_for_db(symbol, exchange)
                        if mds:
                            price = _get_price_mds(broker, symbol, mapped_exchange, channel=mds)
                        else:
                            price = broker.get_quote(symbol, mapped_exchange)
                        out.update(price, size)
                        if all(not math.isnan(getattr(out, check)) for check in checks):
                            valid_price_found = True
                            break  # Stop attempts for this symbol if a valid price is found
                    except Exception as e:
                        trading_logger.log_warning(
                            f"Failed to get price for {symbol} from {broker.broker.name if hasattr(broker, 'broker') else 'unknown'}",
                            {
                                "symbol": symbol,
                                "exchange": exchange,
                                "attempt": attempt + 1,
                                "broker": broker.broker.name if hasattr(broker, "broker") else "unknown",
                                "error": str(e),
                            },
                        )
                        continue
                if valid_price_found:
                    break  # Stop attempts for other brokers for this symbol
            out_list.append(out)
        out = sum_prices(out_list)
        out.symbol = long_symbol

    trading_logger.log_debug(
        f"Retrieved price for {long_symbol}", {"symbol": long_symbol, "exchange": exchange, "final_price": str(out)}
    )
    return out


def get_mid_price(brokers: list[BrokerBase], long_symbol: str, exchange="NSE", mds: Optional[str] = None, last=False):
    if isinstance(brokers, BrokerBase):
        brokers = [brokers]
    try:
        quote = get_price(brokers, long_symbol, exchange=exchange, mds=mds)
    except Exception as e:
        logger.error(f"Error getting price for {long_symbol} on {exchange}: {e}")
        return float("nan")
    mid = float("nan")
    if quote.bid > 0 and quote.ask > 0:
        mid = (quote.bid + quote.ask) / 2
        mapped_exchange = brokers[0].map_exchange_for_api(long_symbol, exchange)
        ticksize = brokers[0].exchange_mappings[mapped_exchange]["contracttick_map"].get(long_symbol, 0.05)
        mid = round(mid / ticksize) * ticksize
        mid = round(mid, 2)  # <-- Add this line to ensure two decimal places
    mid = mid if mid > 0 else float("nan")
    if last and math.isnan(mid):
        return quote.last
    return mid


def get_option_underlying_price(
    brokers: list[BrokerBase], symbol: str, opt_expiry: str, fut_expiry: str = "", exchange="NSE", mds: Optional[str] = None, last=False
) -> float:
    """Retrieve underlying price for a symbol and interpolates, if if needed

    Args:
        symbol (str): sybol name. Should be atleast of form symbol_type___
        opt_expiry (str): expiry date of option. The expiry is NOT picked from symbol
        fut_expiry (str, optional): expiry date of underlying future. Defaults to None.

    Returns:
        float: price of underlying
    """
    if not fut_expiry:
        underlying = (
            symbol.split("_")[0] + "_IND___"
            if "NIFTY" in symbol or "SENSEX" in symbol
            else symbol.split("_")[0] + "_STK___"
        )
        price_f = get_mid_price(brokers, underlying, exchange=exchange, mds=mds, last=last)
    else:
        underlying = symbol.split("_")[0] + "_FUT" + "_" + fut_expiry + "__"
        price_f = get_mid_price(brokers, underlying, exchange=exchange, mds=mds, last=last)
        if math.isnan(price_f):
            return float("nan")

    # Interpolate for index options if needed
    if ("NIFTY" in symbol or "SENSEX" in symbol) and fut_expiry:
        t_o = calc_fractional_business_days(
            dt.datetime.now(), dt.datetime.strptime(opt_expiry + " 15:30:00", "%Y%m%d %H:%M:%S")
        )
        t_f = calc_fractional_business_days(
            dt.datetime.now(), dt.datetime.strptime(fut_expiry + " 15:30:00", "%Y%m%d %H:%M:%S")
        )
        underlying_ind = f'{symbol.split("_")[0]}_IND___'
        last_price = get_mid_price(brokers, underlying_ind, exchange=exchange, mds=mds, last=True)
        if not math.isnan(last_price):
            price_u = last_price
        else:
            price_u = get_price(brokers, underlying_ind, checks=["prior_close"], exchange=exchange, mds=mds).prior_close
        price_f = price_u + (price_f - price_u) * t_o / t_f
    return price_f


def calculate_delta(
    brokers: list[BrokerBase], long_symbol, price_f, market_close_time="15:30:00", exchange="NSE", mds: Optional[str] = None
):
    delta = float("nan")
    ticker = get_price(brokers, long_symbol, checks=["bid", "ask", "prior_close"], exchange=exchange, mds=mds)
    price = (ticker.bid + ticker.ask) / 2 if ticker.bid > 0 and ticker.ask > 0 else ticker.prior_close
    t = (
        calc_fractional_business_days(
            get_tradingapi_now(),
            dt.datetime.strptime(long_symbol.split("_")[2] + " " + market_close_time, "%Y%m%d %H:%M:%S"),
        )
        / 252
    )  # convert days number of years
    vol = BlackScholesIV(
        S=price_f,
        X=float(long_symbol.split("_")[4]),
        r=0,
        T=t,
        OptionType=long_symbol.split("_")[3],
        OptionPrice=price,
    )
    delta = BlackScholesDelta(
        S=price_f,
        X=float(long_symbol.split("_")[4]),
        r=0,
        sigma=vol,
        T=t,
        OptionType=long_symbol.split("_")[3],
    )
    return delta


def find_option_with_delta(
    brokers: list[BrokerBase],
    price_f,
    option_chain,
    target_delta,
    return_lower_delta,
    market_close_time="15:30:00",
    exchange="NSE",
    mds: Optional[str] = "mds",
):
    # Determine the correct option exchange
    opt_exchange = "NFO" if exchange == "NSE" else "BFO" if exchange == "BSE" else exchange

    left, right = 0, len(option_chain) - 1
    best_index = -1  # Default to -1 if no valid option is found
    best_delta = float("-inf") if return_lower_delta else float("inf")  # Best delta found so far

    # Determine if delta is increasing or decreasing
    mid = (left + right) // 2
    delta_1, delta_2 = float("nan"), float("nan")

    # Find the first valid left-side delta
    i = mid
    while i >= left and math.isnan(delta_1):
        delta_1 = calculate_delta(brokers, option_chain[i], price_f, market_close_time, exchange=opt_exchange, mds=mds)
        i -= 1

    # Find the first valid right-side delta
    i = mid + 1
    while i <= right and math.isnan(delta_2):
        delta_2 = calculate_delta(brokers, option_chain[i], price_f, market_close_time, exchange=opt_exchange, mds=mds)
        i += 1

    # If we cannot determine a valid direction, return -1
    if math.isnan(delta_1) or math.isnan(delta_2):
        return best_index

    increasing = abs(delta_2) > abs(delta_1)  # True if deltas increase with strike price
    # if delta is nan, we need to decide if the search range is to the left or right of the present strike
    # and use delta_2's position vis_a-vis target_delta to determine the direction of move to identity next best delta.
    move_right_on_nan_delta = True
    if abs(delta_2) > target_delta and increasing:
        move_right_on_nan_delta = False
    elif abs(delta_2) > target_delta and not increasing:
        move_right_on_nan_delta = True
    elif abs(delta_2) < target_delta and increasing:
        move_right_on_nan_delta = True
    elif abs(delta_2) < target_delta and not increasing:
        move_right_on_nan_delta = False

    while left <= right:
        mid = (left + right) // 2
        delta = calculate_delta(brokers, option_chain[mid], price_f, market_close_time, exchange=opt_exchange, mds=mds)
        delta = abs(delta)  # always select an option using the absolute value of delta.

        if math.isnan(delta):
            # Skip NaN values by moving in the correct direction
            if move_right_on_nan_delta:
                left = mid + 1  # Move right
            else:
                right = mid - 1  # Move right
            continue

        # Update best index if this delta is a better fit
        if return_lower_delta:
            if delta <= target_delta and delta > best_delta:
                best_delta = delta
                best_index = mid
        else:
            if delta >= target_delta and delta < best_delta:
                best_delta = delta
                best_index = mid

        # Adjust binary search range based on delta trend
        if increasing:
            if delta > target_delta:
                right = mid - 1  # Search lower strikes for smaller deltas
            else:
                left = mid + 1  # Search higher strikes for bigger deltas
        else:
            if delta > target_delta:
                left = mid + 1  # Search higher strikes for smaller deltas
            else:
                right = mid - 1  # Search lower strikes for bigger deltas

    return best_index


@log_execution_time
@validate_inputs(
    underlying_symbol=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    delta=lambda x: isinstance(x, (int, float)) and 0 < x < 1,
    opt_expiry=lambda x: isinstance(x, str) and len(x.strip()) == 8,
    option_type=lambda x: isinstance(x, str) and x in ["CALL", "PUT"],
    exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    market_close_time=lambda x: isinstance(x, str) and len(x.strip()) > 0,
)
def get_delta_strike(
    brokers: list[BrokerBase],
    underlying_symbol: str,
    delta: float,
    opt_expiry: str,
    option_type: str,
    fut_expiry=None,
    rounding=None,
    return_lower_delta=True,
    use_future=True,
    search_range=[0.8, 1.2],
    market_close_time="15:30:00",
    exchange="NSE",
    mds: Optional[str] = None,
) -> Union[str, None]:
    """Get option strike price for a given delta with enhanced error handling.

    Args:
        brokers: List of broker instances
        underlying_symbol: Underlying symbol name
        delta: Target delta value (0-1)
        opt_expiry: Option expiry date (YYYYMMDD)
        option_type: Option type (CALL/PUT)
        fut_expiry: Optional future expiry date
        rounding: Optional rounding value for strikes
        return_lower_delta: Whether to return lower delta if exact not found
        use_future: Whether to use future prices
        search_range: Range around underlying price to search
        market_close_time: Market close time
        exchange: Exchange name
        mds: Market data service channel name (str). If None or empty, uses broker quotes. 
             If provided (e.g., "mds"), uses market data service with that channel.

    Returns:
        Union[str, None]: Option symbol or None if not found

    Raises:
        ValidationError: If input parameters are invalid
        MarketDataError: If price retrieval fails
        SymbolError: If symbol mapping fails
    """
    """Returns long symbol for specified value  of absolute delta


    Args:
        underlying_symbol (str): underlying symbol as _STK___ or _IND___
        delta (float): absolute value of delta
        expiry (str): formatted as YYYYMMDD
        option_type (str): CALL or PUT
        rounding (float, optional): Rounding of strike. Defaults to None.
        return_lower_strike (bool, optional): should lower strike value be returend. Defaults to True.
        use_future(bool, optional): Should future prices be used as underlying
        search_range (list, optional): the range of strikes to search with reference to underlying price. Defaults to [0.8,1.2].

    Returns:
        Union[str,None]: long_symbol or None
    """
    if isinstance(brokers, BrokerBase):
        brokers = [brokers]
    exchange = brokers[0].map_exchange_for_api(underlying_symbol, exchange)
    if use_future:
        price_f = get_option_underlying_price(
            brokers, underlying_symbol, opt_expiry, fut_expiry, exchange=exchange, mds=mds
        )
    else:
        price_f = get_price(brokers, underlying_symbol, checks=["last"], exchange=exchange, mds=mds).last
    option_chain = get_linked_options(brokers[0], underlying_symbol, opt_expiry, exchange)
    # option_chain = get_opt_chain(broker, underlying_symbol.split("_")[0], opt_expiry_yyyy_mm_dd)
    option_chain = [opt for opt in option_chain if f"_{option_type}_" in opt]
    option_chain = [
        opt for opt in option_chain if search_range[0] * price_f < float(opt.split("_")[4]) < search_range[1] * price_f
    ]
    option_chain = sorted(option_chain, key=lambda x: float(x.split("_")[-1]))  # sort ascending
    if rounding is not None and rounding > 0:
        option_chain = [t for t in option_chain if abs(float(t.split("_")[4]) % rounding) < 1e-6]
    if len(option_chain) == 0:
        logger.info(f"Option Chain not found for symbol: {underlying_symbol}")
        return None
    index = find_option_with_delta(
        brokers, price_f, option_chain, delta, return_lower_delta, market_close_time, exchange=exchange, mds=mds
    )
    if index >= 0:
        return option_chain[index]
    else:
        return None


def get_impact_cost(brokers: list[BrokerBase], symbol: str, exchange="NSE", mds: Optional[str] = None) -> dict:
    ticker = get_price(brokers, symbol, checks=["bid", "ask"], exchange=exchange, mds=mds)
    mid_price = (ticker.ask + ticker.bid) / 2
    if mid_price == 0 or math.isnan(mid_price):
        impact_cost = float("nan")
    else:
        impact_cost = (ticker.ask - ticker.bid) / mid_price
    return {"ticker": ticker, "impact_cost": impact_cost}


def _sort_list(symbols, quantities, price_types, additional_info, exchanges=None, trigger_prices=None):
    # Handle cases where price_types is None or partially filled
    if not price_types or price_types == [None]:
        price_types = ["LMT"] * len(symbols)
    elif len(price_types) < len(symbols):
        # Propagate the first price type to all symbols
        price_types = [price_types[0]] * len(symbols)

    # Ensure all lists are the same length
    n = len(symbols)
    if not price_types or len(price_types) < n:
        price_types = [price_types[0]] * n if price_types else ["LMT"] * n
    if not quantities or len(quantities) < n:
        quantities = [quantities[0]] * n if quantities else [1] * n
    if not additional_info or len(additional_info) < n:
        additional_info = (additional_info + [""] * n)[:n]
    include_triggers = trigger_prices is not None
    if include_triggers:
        if not isinstance(trigger_prices, list):
            trigger_prices = [trigger_prices]
        if len(trigger_prices) < n:
            trigger_prices = (trigger_prices + [trigger_prices[0] if trigger_prices else float("nan")] * n)[:n]
        trigger_prices = trigger_prices[:n]
    if exchanges is not None and (not exchanges or len(exchanges) < n):
        exchanges = (exchanges + [exchanges[0]] * n)[:n]

    # Zip the relevant pairs based on whether exchanges are provided
    if exchanges is not None:
        if include_triggers:
            zipped_pairs = list(zip(quantities, symbols, exchanges, price_types, additional_info, trigger_prices))
        else:
            zipped_pairs = list(zip(quantities, symbols, exchanges, price_types, additional_info))
    else:
        if include_triggers:
            zipped_pairs = list(zip(quantities, symbols, price_types, additional_info, trigger_prices))
        else:
            zipped_pairs = list(zip(quantities, symbols, price_types, additional_info))

    # Sort zipped pairs by quantities in descending order
    sorted_pairs = sorted(zipped_pairs, reverse=True)

    # Unpack the sorted pairs into lists
    if exchanges is not None:
        if not sorted_pairs:
            sorted_quantities = []
            sorted_symbols = []
            sorted_exchanges = []
            sorted_order_types = []
            sorted_additional_info = []
            sorted_triggers = [] if include_triggers else None
        else:
            if include_triggers:
                (
                    sorted_quantities,
                    sorted_symbols,
                    sorted_exchanges,
                    sorted_order_types,
                    sorted_additional_info,
                    sorted_triggers,
                ) = zip(*sorted_pairs)
            else:
                (
                    sorted_quantities,
                    sorted_symbols,
                    sorted_exchanges,
                    sorted_order_types,
                    sorted_additional_info,
                ) = zip(*sorted_pairs)
        return (
            list(sorted_symbols),
            list(sorted_quantities),
            list(sorted_order_types),
            list(sorted_exchanges),
            list(sorted_additional_info),
            list(sorted_triggers) if include_triggers else None,
        )
    else:
        if not sorted_pairs:
            if include_triggers:
                return [], [], [], [], []
            return [], [], [], []
        if include_triggers:
            (
                sorted_quantities,
                sorted_symbols,
                sorted_order_types,
                sorted_additional_info,
                sorted_triggers,
            ) = zip(*sorted_pairs)
            return (
                list(sorted_symbols),
                list(sorted_quantities),
                list(sorted_order_types),
                list(sorted_additional_info),
                list(sorted_triggers),
            )
        else:
            sorted_quantities, sorted_symbols, sorted_order_types, sorted_additional_info = zip(*sorted_pairs)
            return list(sorted_symbols), list(sorted_quantities), list(sorted_order_types), list(sorted_additional_info)


@log_execution_time
@validate_inputs(
    strategy=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    entry=lambda x: isinstance(x, bool),
    validate_db_position=lambda x: isinstance(x, bool),
    paper=lambda x: isinstance(x, bool),
)
def place_combo_order(
    execution_broker: BrokerBase,
    strategy: str,
    symbols: list[str],
    quantities: list[int],
    entry: bool,
    additional_infos: list[str] = [""],
    exchanges: list[str] = ["NSE"],
    price_broker: Optional[List[BrokerBase]] = None,
    price_types: list = [],
    trigger_prices: Union[List[float], float, None] = None,
    validate_db_position: bool = True,
    paper: bool = True,
) -> dict:
    """Place a combo order with broker with enhanced error handling.

    Args:
        execution_broker: Broker instance for order execution
        strategy: Strategy name
        symbols: List of symbols to trade
        quantities: List of quantities for each symbol
        entry: True if entry order, False if exit order
        additional_infos: List of additional info strings
        exchanges: List of exchanges for each symbol
        price_broker: Optional list of brokers for price data
        price_types: List of price types for each symbol
        validate_db_position: Whether to validate database position
        paper: Whether this is a paper trade

    Returns:
        dict: Dictionary with symbol to internal order ID mapping

    Raises:
        ValidationError: If input parameters are invalid
        OrderError: If order placement fails
    """
    """Place a combo order with broker

    Args:
        broker: broker instance
        strategy (str): strategy name
        symbols (list[str]): list of symbols
        quantities (list[int]): list of quantities
        entry (bool): True if entry else False
        additional_infos (list): list of json formatted string,
        price_types (list, optional): list of order types. Should be same length as symbols. Defaults to None which reflects in limit orders at midprice
        trigger_prices (list | float | None): trigger price(s) for conditional orders. Scalar applied to all legs.
        validate_db_position(bool): if set to false, exit orders are generated without updating redis
        paper: if set to True (default), simulated orders are generated

    Returns:
        dict: for entry orders a dict containing symbol:int order id is returned. dict is empty for exit orders
    """

    if price_broker is None:
        price_broker = [execution_broker]
    if not isinstance(symbols, list):
        symbols = [symbols]
    if not isinstance(price_types, list):
        price_types = [price_types]
    if not isinstance(quantities, list):
        quantities = [quantities]
    if not isinstance(exchanges, list):
        exchanges = [exchanges]
    if len(exchanges) < len(symbols):
        exchanges = [exchanges[0]] * len(symbols)
    if not isinstance(additional_infos, list):
        additional_infos = [additional_infos]
    if len(additional_infos) < len(symbols):
        additional_infos = additional_infos + [""] * (len(symbols) - len(additional_infos))
    if trigger_prices is None:
        trigger_prices = [float("nan")] * len(symbols)
    elif isinstance(trigger_prices, list):
        trigger_prices = (trigger_prices + [trigger_prices[-1]] * len(symbols))[: len(symbols)] if trigger_prices else [
            float("nan")
        ] * len(symbols)
    else:
        trigger_prices = [trigger_prices] * len(symbols)
    exchanges = [
        execution_broker.map_exchange_for_api(symbol, exchange) for symbol, exchange in zip(symbols, exchanges)
    ]
    (
        symbols,
        quantities,
        price_types,
        exchanges,
        additional_infos,
        trigger_prices,
    ) = _sort_list(symbols, quantities, price_types, additional_infos, exchanges, trigger_prices)
    out = {}
    for symbol, exchange, quantity, price_type, additional_info, trigger_price in zip(
        symbols, exchanges, quantities, price_types, additional_infos, trigger_prices
    ):
        size = quantity
        if entry:
            side = "BUY" if size > 0 else "SHORT"
        else:
            side = "COVER" if size > 0 else "SELL"
        size = abs(size)
        exch = exchange
        exch_type = execution_broker.exchange_mappings[exch]["exchangetype_map"].get(symbol.split("?")[0])
        temp_order = Order(
            order_type=side,
            quantity=size,
            exchange=exch,
            exchange_segment=exch_type,
            is_intraday=False,
            price=float("nan"),
            ahplaced="N",
            long_symbol=symbol,
            price_type=price_type,
            additional_info=additional_info,
            trigger_price=trigger_price,
        )
        if not math.isnan(temp_order.trigger_price):
            temp_order.is_stoploss_order = True
        logger.info(f"{symbol} {exch} {size} {side}")
        if entry:
            temp = transmit_entry_order(execution_broker, strategy, temp_order, paper=paper)
            out[symbol] = temp
        else:
            transmit_exit_order(execution_broker, strategy, temp_order, validate_db_position, paper=paper)
    return out


@log_execution_time
@validate_inputs(pnl=lambda x: isinstance(x, pd.DataFrame) and len(x) >= 0)
def calculate_mtm(brokers: list[BrokerBase], pnl: pd.DataFrame, mds: Optional[str] = None) -> pd.DataFrame:
    """Calculates MTM for a profit DataFrame which has open trades.

    Args:
        brokers: List of broker instances
        pnl: Profit DataFrame with open trades
        mds: Market data service channel name (str). If None or empty, uses broker quotes. 
             If provided (e.g., "mds"), uses market data service with that channel.

    Returns:
        pd.DataFrame: Profit DataFrame with mark-to-market prices

    Raises:
        ValidationError: If input parameters are invalid
        MarketDataError: If price retrieval fails
        PnLError: If MTM calculation fails
    """
    pnl = pnl.copy()
    pnl.reset_index(drop=True, inplace=True)
    for index, row in pnl.iterrows():
        if row["exit_quantity"] + row["entry_quantity"] != 0 and row["entry_price"] != 0:
            logger.debug(f'Getting mtm for {row["symbol"]}')
            exchange = "BSE" if "SENSEX" in row["symbol"] else "NSE"
            quote = get_price(
                brokers,
                row["symbol"],
                checks=["bid", "ask", "last", "prior_close"],
                attempts=1,
                exchange=exchange,
                mds=mds,
            )
            if all(not math.isnan(attr) for attr in [quote.bid, quote.ask]):
                mtm_price = (quote.bid + quote.ask) / 2
            else:
                mtm_price = quote.last if not math.isnan(quote.last) else quote.prior_close
            pnl.loc[index, "mtm"] = mtm_price
    pnl["gross_pnl"] = -1 * (
        pnl["exit_price"] * pnl["exit_quantity"]
        + pnl["mtm"] * -1 * (pnl["entry_quantity"] + pnl["exit_quantity"])
        + pnl["entry_quantity"] * pnl["entry_price"]
    )
    return pnl


def get_expiry_from_FormattedExpiryTime(timestamp_str) -> str:
    """Convert str formatted as "/Date(1703754000000+0530)/" to yyyy-mm-dd

    Args:
        timestamp_str (_type_): str formatted as "/Date(1703754000000+0530)/"

    Returns:
        str: date formatted as yyyy-mm-dd
    """
    timestamp_str = timestamp_str.replace("/Date(", "").replace(")/", "")
    # Extract milliseconds and timezone offset
    milliseconds = int(timestamp_str[:-5])
    timezone_offset = int(timestamp_str[-5:])

    # Create a timedelta object for the timezone offset
    offset_timedelta = dt.timedelta(minutes=timezone_offset // 100 * 60 + timezone_offset % 100)

    # Convert milliseconds to seconds and add the offset
    timestamp_seconds = milliseconds / 1000
    timestamp_with_offset = timestamp_seconds + offset_timedelta.total_seconds()

    # Convert the timestamp to a datetime object
    date_object = dt.datetime.fromtimestamp(timestamp_with_offset, tz=dt.timezone.utc)

    # Format the datetime object as a string
    formatted_date = date_object.strftime("%Y-%m-%d")
    return formatted_date


def historical_to_dataframes(historical_data: Dict[str, List[HistoricalData]]) -> List[pd.DataFrame]:
    """
    Converts a dictionary of lists of HistoricalData to a list of pandas DataFrames.

    Args:
        historical_data (Dict[str, List[HistoricalData]]): Dictionary with symbol as keys and lists of HistoricalData as values.

    Returns:
        List[pd.DataFrame]: List of DataFrames, each containing historical data for a symbol.
    """
    dataframes = []
    for symbol, data_list in historical_data.items():
        try:
            # Convert list of HistoricalData to a DataFrame
            df = pd.DataFrame(
                [{**data.to_dict(), "symbol": symbol} for data in data_list if data.date != dt.datetime(1970, 1, 1)]
            )  # Filter out entries with missing dates

            if not df.empty:
                df = df.sort_values(by="date")
                df.reset_index(inplace=True, drop=True)
                dataframes.append(df)
        except Exception as e:
            print(f"Error processing data for symbol {symbol}: {e}")

    return dataframes


def _get_active_commission_config(entry_date: str, config) -> str:
    """
    Get the active commission configuration effective date based on the entry date.

    :param entry_date: A string in the form "yyyy-mm-dd".
    :param config: A dictionary containing the YAML data loaded using yaml.safe_load.
    :return: The most recent effective_date that is less than the entry_date.
    """

    # Extract and sort the effective dates as strings
    effective_dates = sorted(list(config.get("commissions").keys()))

    # Find the most recent effective date less than entry_date
    for effective_date in effective_dates:
        if effective_date < entry_date:
            return effective_date

    # Return 1970 if no effective date is found
    return "1970-01-01"


def _update_commissions(dataframe: pd.DataFrame, brok: Optional[BrokerBase] = None):
    """get commission values in trades dataframe

    Args:
        dataframe (pd.DataFrame): trades dataframe
        brok (BrokerBase, optional): BrokerBase containing database with trades. Defaults to None.
    """

    def get_redis_price(broker_order_id: str, long_symbol: str, brok: Optional[BrokerBase] = None):
        """Fetch the price from Redis using the broker_order_id and ensure it matches the long_symbol."""
        if brok is None:
            return 0
        order_data = brok.redis_o.hgetall(broker_order_id)
        if order_data.get("long_symbol") == long_symbol:
            return float(order_data.get("price", 0))
        return 0

    broker_name = brok.broker.name if brok is not None else "UNDEFINED"

    if "commission" not in dataframe.columns:
        dataframe["commission"] = 0.0

    for index, row in dataframe.iterrows():
        symbol_parts = row["symbol"].split("_", 2)
        if len(symbol_parts) < 2:
            continue  # Skip if the symbol is not in the expected format
        entry_date = row["entry_time"][0:10]
        exit_date = row["exit_time"][0:10]
        entry_gst = config.get_commission_by_date(entry_date, f"{broker_name}.GST", 0)
        exit_gst = config.get_commission_by_date(exit_date, f"{broker_name}.GST", 0)

        # Check if the symbol is a combo symbol and split it into legs
        legs = row["symbol"].split(":") if ":" in row["symbol"] else [row["symbol"]]
        entry_keys = row["entry_keys"].split(" ") if "entry_keys" in row and row["entry_keys"] else []
        exit_keys = row["exit_keys"].split(" ") if "exit_keys" in row and row["exit_keys"] else []
        total_commission = 0
        if len(legs) > 1 and brok is None:
            raise ValueError("brok needs to be specfied for combo trades")
        for leg in legs:
            leg_parts = leg.split("?")
            leg_symbol = leg_parts[0]
            leg_size = float(leg_parts[1]) if len(leg_parts) > 1 else 1

            leg_symbol_parts = leg_symbol.split("_")
            symbol_type = leg_symbol_parts[1]

            # Determine the trade_side for each leg based on leg_size
            if leg_size > 0:
                trade_side = row["side"]
            else:
                trade_side = "SHORT" if row["side"] == "BUY" else "BUY"

            exit_side = "SHORT" if trade_side == "BUY" else "BUY"  # Determine exit side

            # Find the correct entry and exit keys for this leg
            entry_price = None
            exit_price = None

            for entry_key in entry_keys:
                price = get_redis_price(entry_key, leg_symbol, brok)
                if price > 0:
                    entry_price = price
                    break

            for exit_key in exit_keys:
                price = get_redis_price(exit_key, leg_symbol, brok)
                if price > 0:
                    exit_price = price
                    break

            # Fallback to row's entry_price and exitPrice if not found in Redis
            entry_price = entry_price if entry_price is not None else row["entry_price"]
            # exit_price is set to 0 for combo orders irrespective of mtm.
            exit_price = exit_price if exit_price is not None else row["exit_price"] if len(legs) == 1 else 0

            # Fetch commission rates for the entry trade
            flat_rate = config.get_commission_by_date(entry_date, f"{broker_name}.{symbol_type}.{trade_side}.flat", 0)
            per_commission = config.get_commission_by_date(
                entry_date, f"{broker_name}.{symbol_type}.{trade_side}.percentage.commission", 0
            )
            per_stt = config.get_commission_by_date(
                entry_date, f"{broker_name}.{symbol_type}.{trade_side}.percentage.stt", 0
            )
            per_exchange = config.get_commission_by_date(
                entry_date, f"{broker_name}.{symbol_type}.{trade_side}.percentage.exchange", 0
            )
            per_sebi = config.get_commission_by_date(
                entry_date, f"{broker_name}.{symbol_type}.{trade_side}.percentage.sebi", 0
            )
            per_stampduty = config.get_commission_by_date(
                entry_date, f"{broker_name}.{symbol_type}.{trade_side}.percentage.stampduty", 0
            )

            # Calculate the total commission for the entry trade
            entry_trade_value = abs(row["entry_quantity"] * abs(leg_size) * entry_price)
            entry_commission = (
                flat_rate * (100 + entry_gst) / 100
                + (per_commission / 100 * entry_trade_value) * (100 + entry_gst) / 100
                + (per_stt / 100 * entry_trade_value)
                + (per_exchange / 100 * entry_trade_value) * (100 + entry_gst) / 100
                + (per_sebi / 100 * entry_trade_value) * (100 + entry_gst) / 100
                + (per_stampduty / 100 * entry_trade_value)
            )

            # Fetch commission rates for the exit trade
            flat_rate = config.get_commission_by_date(exit_date, f"{broker_name}.{symbol_type}.{exit_side}.flat", 0)
            per_commission = config.get_commission_by_date(
                exit_date, f"{broker_name}.{symbol_type}.{exit_side}.percentage.commission", 0
            )
            per_stt = config.get_commission_by_date(
                exit_date, f"{broker_name}.{symbol_type}.{exit_side}.percentage.stt", 0
            )
            per_exchange = config.get_commission_by_date(
                exit_date, f"{broker_name}.{symbol_type}.{exit_side}.percentage.exchange", 0
            )
            per_sebi = config.get_commission_by_date(
                exit_date, f"{broker_name}.{symbol_type}.{exit_side}.percentage.sebi", 0
            )
            per_stampduty = config.get_commission_by_date(
                exit_date, f"{broker_name}.{symbol_type}.{exit_side}.percentage.stampduty", 0
            )

            # Calculate the total commission for the exit trade
            exit_trade_value = abs(row["exit_quantity"] * abs(leg_size) * exit_price)
            exit_commission = (
                flat_rate * (100 + exit_gst) / 100
                + (per_commission / 100 * exit_trade_value) * (100 + exit_gst) / 100
                + (per_stt / 100 * exit_trade_value)
                + (per_exchange / 100 * exit_trade_value) * (100 + exit_gst) / 100
                + (per_sebi / 100 * exit_trade_value) * (100 + exit_gst) / 100
                + (per_stampduty / 100 * exit_trade_value)
            )

            # Sum up the commission for this leg
            total_commission += entry_commission + exit_commission

        # Update the dataframe with the total commission for the combo symbol
        dataframe.at[index, "commission"] = round(total_commission, 0)

    return dataframe


@log_execution_time
@validate_inputs(trades=lambda x: isinstance(x, pd.DataFrame) and len(x) >= 0)
def calc_pnl(trades: pd.DataFrame, brok: Optional[BrokerBase] = None):
    """Calculates absolute profit/loss arising from a trade object.
    This function adds/amends pnl column to input dataframe.

    Args:
        trades: Dataframe of trades
        brok: Optional broker instance for commission calculations

    Returns:
        pd.DataFrame: Trades dataframe with calculated P&L

    Raises:
        ValidationError: If input parameters are invalid
        PnLError: If P&L calculation fails
        CommissionError: If commission calculation fails
    """
    broker_name = brok.broker.name if brok is not None else "UNDEFINED"
    if len(trades) == 0:
        logger.info("No trades")
        return trades
    trades = trades.copy()
    if broker_name is not None:
        trades = _update_commissions(trades, brok=brok)
    trades["gross_pnl"] = -1 * (
        trades["exit_price"] * trades["exit_quantity"]
        + trades["mtm"] * -1 * (trades["entry_quantity"] + trades["exit_quantity"])
        + trades["entry_quantity"] * trades["entry_price"]
    )
    trades["pnl"] = trades["gross_pnl"] - trades["commission"]
    return trades


@log_execution_time
@validate_inputs(
    strategy_name=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    capital_allocated=lambda x: isinstance(x, (int, float)) and x >= 0,
)
def register_strategy_capital(
    strategy_name: str,
    broker: BrokerBase,
    capital_allocated: float,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    date: Optional[str] = None,
) -> bool:
    """
    Register strategy capital allocation and broker capital availability in Redis DB 0.
    
    Updates the consolidated JSON key: capital:registration:{YYYYMMDD}
    
    Args:
        strategy_name: Strategy/algorithm name
        broker: BrokerBase instance (used to get broker name and available capital)
        capital_allocated: Capital allocated to this strategy
        redis_host: Redis host (default: localhost)
        redis_port: Redis port (default: 6379)
        date: Date in YYYYMMDD format (defaults to today)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Determine date
        if date is None:
            date = dt.datetime.now().strftime("%Y%m%d")
        
        # Get broker name
        if hasattr(broker, 'broker') and broker.broker:
            try:
                broker_name = broker.broker.name
            except AttributeError:
                broker_name = "UNKNOWN"
        else:
            broker_name = "UNKNOWN"
        
        # Get broker available capital
        try:
            capital_available = broker.get_available_capital()
        except Exception as e:
            logger.warning(f"Failed to get available capital from broker {broker_name}: {e}")
            capital_available = 0.0
        
        # Connect to Redis DB 0
        redis_conn = redis.Redis(host=redis_host, db=0, port=redis_port, decode_responses=True)
        
        # Get or create registration data
        registration_key = f"capital:registration:{date}"
        existing_data_str = redis_conn.get(registration_key)
        
        if existing_data_str:
            try:
                registration_data = json.loads(existing_data_str)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse existing registration data for {date}, creating new")
                registration_data = {
                    "date": date,
                    "brokers": {},
                    "strategies": {},
                    "last_updated": dt.datetime.now().isoformat(),
                }
        else:
            registration_data = {
                "date": date,
                "brokers": {},
                "strategies": {},
                "last_updated": dt.datetime.now().isoformat(),
            }
        
        # Update broker capital
        current_time = dt.datetime.now().isoformat()
        registration_data["brokers"][broker_name] = {
            "capital_available": capital_available,
            "timestamp": current_time,
        }
        
        # Update strategy registration
        registration_data["strategies"][strategy_name] = {
            "broker": broker_name,
            "capital_allocated": capital_allocated,
            "timestamp": current_time,
        }
        
        # Update last_updated timestamp
        registration_data["last_updated"] = current_time
        
        # Save back to Redis
        redis_conn.set(registration_key, json.dumps(registration_data))
        
        # Also register broker in strategy:broker_universe set
        try:
            # Get Redis DB number from broker's redis_o connection
            redis_db = 0  # Default
            if hasattr(broker, 'redis_o') and broker.redis_o:
                # Try to get DB number from connection pool
                if hasattr(broker.redis_o, 'connection_pool'):
                    pool = broker.redis_o.connection_pool
                    if hasattr(pool, 'connection_kwargs') and 'db' in pool.connection_kwargs:
                        redis_db = pool.connection_kwargs['db']
            
            # Add broker:DB entry to strategy:broker_universe set
            broker_universe_key = "strategy:broker_universe"
            broker_entry = f"{broker_name}:{redis_db}"
            redis_conn.sadd(broker_universe_key, broker_entry)
            logger.debug(f"Added broker to strategy:broker_universe: {broker_entry}")
        except Exception as e:
            # Non-critical error, log but don't fail the registration
            logger.warning(f"Failed to register broker in strategy:broker_universe: {e}")
        
        logger.info(
            f"Registered strategy capital: {strategy_name} with broker {broker_name}, "
            f"allocated={capital_allocated}, broker_capital={capital_available}"
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to register strategy capital: {e}", exc_info=True)
        return False
