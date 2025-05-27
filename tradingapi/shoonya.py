import calendar
import datetime as dt
import inspect
import io
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
import traceback
import zipfile
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import pyotp
import pytz
import redis
import requests
from chameli.dateutils import valid_datetime
from NorenRestApiPy.NorenApi import NorenApi

from .broker_base import (BrokerBase, Brokers, HistoricalData, Order,
                          OrderInfo, OrderStatus, Price)
from .config import get_config
from .utils import set_starting_internal_ids_int, update_order_status

# Set up logging
logger = logging.getLogger(__name__)


# Exception handler
def my_handler(typ, value, trace):
    logger.error("%s %s %s", typ.__name__, value, "".join(traceback.format_tb(trace)))


sys.excepthook = my_handler
config = get_config()


class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(
            self, host="https://api.shoonya.com/NorenWClientTP/", websocket="wss://api.shoonya.com/NorenWSTP/"
        )
        global api
        api = self


def save_symbol_data(saveToFolder: bool = True):
    def merge_without_last(lst):
        if len(lst) > 1:
            return "-".join(lst[:-1])
        else:
            return lst[0]

    bhavcopyfolder = config.get("bhavcopy_folder")
    url = "https://api.shoonya.com/NSE_symbols.txt.zip"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_nse_cash.zip"
    response = requests.get(url, allow_redirects=True)
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
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_bse_cash.zip"
    response = requests.get(url, allow_redirects=True)
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
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_fno.zip"
    response = requests.get(url, allow_redirects=True)
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
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_shoonyacodes_bse_fno.zip"
    response = requests.get(url, allow_redirects=True)
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
                    r"^([A-Z]+(?:50)?)(?=\d{2}[A-Z]{3}|\d{2}\d{6})"
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
        dest_symbol_file = f"{config.get('SHOONYA.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
        codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "trading_symbol"]].to_csv(
            dest_symbol_file, index=False
        )
    return codes


class Shoonya(BrokerBase):
    def __init__(self, **kwargs):
        """
        mandatory_keys = None

        """
        super().__init__()
        self.broker = Brokers.SHOONYA
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

    def connect(self, redis_db: int):
        def _fresh_login(susertoken_path):
            user = config.get(f"{self.broker.name}.USER")
            pwd = config.get(f"{self.broker.name}.PWD")
            vc = config.get(f"{self.broker.name}.VC")
            app_key = config.get(f"{self.broker.name}.APPKEY")
            token = config.get(f"{self.broker.name}.TOKEN")
            factor2 = pyotp.TOTP(token).now()
            self.api = ShoonyaApiPy()
            out = self.api.login(
                userid=user,
                password=pwd,
                twoFA=pyotp.TOTP(token).now(),
                vendor_code=vc,
                api_secret=app_key,
                imei="abc12345",
            )
            susertoken = out["susertoken"]
            with open(susertoken_path, "w") as file:
                file.write(susertoken)

        if config.get(f"{self.broker.name}") is not {}:
            self.codes = self.update_symbology()
            susertoken_path = config.get(f"{self.broker.name}.USERTOKEN")
            fresh_login_needed = True
            if os.path.exists(susertoken_path):
                mod_time = os.path.getmtime(susertoken_path)
                mod_datetime = dt.datetime.fromtimestamp(mod_time)
                today = dt.datetime.now().date()
                if mod_datetime.date() == today:
                    fresh_login_needed = False
                    user = config.get(f"{self.broker.name}.USER")
                    pwd = config.get(f"{self.broker.name}.PWD")
                    with open(susertoken_path, "r") as file:
                        susertoken = file.read().strip()
                    self.api = ShoonyaApiPy()
                    self.api.set_session(userid=user, password=pwd, usertoken=susertoken)

            if fresh_login_needed:
                self.fp = _fresh_login(susertoken_path)

            self.redis_o = redis.Redis(db=redis_db, charset="utf-8", decode_responses=True)
            self.starting_order_ids_int = set_starting_internal_ids_int(redis_db=self.redis_o)
        else:
            logger.error("Configuration file not found.")
            sys.exit(1)

    def is_connected(self):
        try:
            if float(self.api.get_limits().get("cash")) > 0 and self.get_quote("NIFTY_IND___").last > 0:
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            return False

    def disconnect(self):
        return super().disconnect()

    def update_symbology(self, **kwargs):
        dt_today = dt.datetime.today().strftime("%Y%m%d")
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
        caller_function = inspect.stack()[1].function

        if hasattr(any_object, "to_dict"):
            # Try to get the object's __dict__
            try:
                log_object = any_object.to_dict()  # Use the object's attributes as a dictionary
            except Exception as e:
                log_object = {"error": f"Error accessing __dict__: {str(e)}"}
        else:
            # If no __dict__, treat the object as a simple serializable object (e.g., a dict, list, etc.)
            log_object = any_object

        # Add the calling function name to the log
        log_entry = {"caller": caller_function, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), "object": log_object}

        # Log the entry to Redis
        self.redis_o.zadd("SHOONYA:LOG", {json.dumps(log_entry): time.time()})

    def place_order(self, order: Order, **kwargs) -> Order:
        order.broker = self.broker
        order.scrip_code = self.exchange_mappings[order.exchange]["symbol_map"].get(order.long_symbol, None)
        orig_order_type = order.order_type
        if order.scrip_code is not None or order.paper:  # if paper, we dont check for valid scrip_code
            if order.order_type == "BUY" or order.order_type == "COVER":
                order.order_type = "B"
            elif order.order_type == "SHORT" or order.order_type == "SELL":
                order.order_type = "S"
            order.remote_order_id = dt.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-4]
            if not order.paper:
                quantity = order.quantity
                product_type = "C" if "_STK_" in order.long_symbol else "M"  # M is NRML , 'I' is MIS
                price_type = "LMT" if order.price > 0 else "MKT"
                trading_symbol = self._get_tradingsymbol_from_longname(order.long_symbol, order.exchange)
                out = self.api.place_order(
                    buy_or_sell=order.order_type,
                    product_type=product_type,
                    exchange=order.exchange,
                    tradingsymbol=trading_symbol,
                    quantity=quantity,
                    discloseqty=0,
                    price_type=price_type,
                    price=order.price,
                    trigger_price=None,
                    retention="DAY",
                    remarks=order.internal_order_id,
                )
                logger.info(f"Shoonya order info: {json.dumps(out, indent=4,default=str)}")
                if out["stat"] is None:
                    logger.error(f"Error placing order: {order}")
                    return order
                if out["stat"].upper() == "OK":
                    order.broker_order_id = out.get("norenordno")
                    order.exch_order_id = out.get("norenordno")
                    order.order_type = orig_order_type
                    order.orderRef = order.internal_order_id
                    fills = self.get_order_info(broker_order_id=order.broker_order_id)
                    order.exch_order_id = fills.exchange_order_id
                    order.status = fills.status
                    try:
                        order.message = self.api.single_order_history(order.broker_order_id)[0].get("rejreason")
                    except Exception as e:
                        logger.error(f"Error getting order history: {str(e)}")
                    if order.price == 0:
                        if fills.fill_price > 0 and order.price == 0:
                            order.price = fills.fill_price
                            logger.info(f"Placed Order: {order}")
            else:
                order.order_type = orig_order_type
                order.exch_order_id = str(random.randint(10000000, 99999999)) + "P"
                order.broker_order_id = str(random.randint(10000000, 99999999)) + "P"
                order.orderRef = order.internal_order_id
                order.message = "Paper Order"
                order.status = OrderStatus.FILLED
                order.scrip_code = 0 if order.scrip_code is None else order.scrip_code
                logger.info(f"Placed Paper Order: {order}")
            self.log_and_return(order)
            return order
        if order.scrip_code is None:
            logger.info(f"No broker identifier found for symbol: {order.long_symbol}")
        self.log_and_return(order)
        return order

    def modify_order(self, **kwargs) -> Order:
        """
        mandatory_keys = ['broker_order_id', 'new_price', 'new_quantity']

        """
        mandatory_keys = ["broker_order_id", "new_price", "new_quantity"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = kwargs.get("broker_order_id")
        new_price = kwargs.get("new_price")
        new_quantity = kwargs.get("new_quantity")
        order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.broker_order_id != "0":
            fills = self.get_order_info(broker_order_id=broker_order_id)
            if order.status in [OrderStatus.OPEN]:
                logger.info(
                    f"Modifying entry order for {broker_order_id} as not filled. Old Price: {order.price}, New Price: {new_price}."
                    f"Old Quantity: {order.quantity}, New Quantity: {new_quantity}, Current Fills: {str(fills.fill_size)}"
                )
                long_symbol = order.long_symbol
                exchange = order.exchange
                trading_symbol = self._get_tradingsymbol_from_longname(long_symbol, exchange)
                newprice_type = "LMT" if new_price > 0 else "MKT"
                out = self.api.modify_order(
                    exchange=exchange,
                    tradingsymbol=trading_symbol,
                    orderno=broker_order_id,
                    newquantity=new_quantity,
                    newprice_type=newprice_type,
                    newprice=new_price,
                )
                if out is None:
                    logger.error(f"Error modifying order {broker_order_id}")
                elif out["stat"].upper() == "OK":
                    self.log_and_return(out)
                    order.quantity = new_quantity
                    order.price = new_price
                    order.price_type = new_price
                    order_info = self.get_order_info(broker_order_id=broker_order_id)
                    order.status = order_info.status
                    order.exch_order_id = order_info.exchange_order_id
                    self.redis_o.hmset(broker_order_id, {key: str(val) for key, val in order.to_dict().items()})
                else:
                    self.log_and_return(out)
                self.log_and_return(order)
                return order
            else:
                logger.info(
                    f"Order status does not allow modification for {order.broker_order_id}. Status was {order.status}"
                )
                self.log_and_return(order)
                return order

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
                    logger.info(
                        f"Cancelling broker_order_id {broker_order_id} for symbol {order.long_symbol}. "
                        f"Filled: {str(fills.fill_size)}. Ordered: {order.quantity}"
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
        broker_order_id = kwargs.get("broker_order_id")
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
            logger.debug(f"Paper Trade: {broker_order_id} being skipped")
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

        logger.debug(f"Order Status: {json.dumps(out,indent=4,default=str)}")
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

    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: str,
        date_end: str = dt.datetime.today().strftime("%Y-%m-%d"),
        exchange="NSE",
        periodicity="1m",
        market_close_time="15:30:00",
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

        Returns:
            Dict[str, List[HistoricalData]]: Dictionary with historical data for each symbol.
        """
        timezone = pytz.timezone("Asia/Kolkata")

        def extract_number(s: str) -> int:
            # Search for digits in the string
            match = re.search(r"\d+", s)
            # Convert to integer if match is found, else return None
            return int(match.group()) if match else None

        scripCode = None
        # Determine the format of symbols and create a DataFrame
        if isinstance(symbols, str):
            exchange = self.map_exchange_for_api(symbols, exchange)
            scripCode = self.exchange_mappings[exchange]["symbol_map"].get(symbols)
            if scripCode:
                symbols_pd = pd.DataFrame([{"long_symbol": symbols, "Scripcode": scripCode}])
            else:
                logger.error(f"Did not get ScripCode for {symbols}")
                return {}
        elif isinstance(symbols, dict):
            scripCode = symbols.get("scrip_code")
            if scripCode:
                symbols_pd = pd.DataFrame([{"long_symbol": symbols.get("long_symbol"), "Scripcode": scripCode}])
            else:
                logger.error(f"Did not get ScripCode for {symbols}")
                return {}
        else:
            symbols_pd = symbols

        out = {}  # Initialize the output dictionary

        for index, row_outer in symbols_pd.iterrows():
            logger.debug(f"{str(index)}:{str(len(symbols))}:{row_outer['long_symbol']}")
            exchange = self.map_exchange_for_api(row_outer["long_symbol"], exchange)
            historical_data_list = []
            exch = exchange
            s = row_outer["long_symbol"].replace("/", "-")
            row_outer["long_symbol"] = "NSENIFTY" + s[s.find("_") :] if s.startswith("NIFTY_") else s
            # we do the above remapping for downloading permin data to database for legacy reasons.
            # once NSENIFTY is amended to NIFTY in databae, we can remove this line.
            date_start, _ = valid_datetime(date_start, None)
            date_end, _ = valid_datetime(date_end, "%Y%m%d")
            date_end, _ = valid_datetime(date_end + " " + market_close_time, None)
            if isinstance(date_start, dt.date) and not isinstance(date_start, dt.datetime):
                date_start = dt.datetime.combine(date_start, dt.datetime.min.time())
            if isinstance(date_end, dt.date) and not isinstance(date_end, dt.datetime):
                date_end = dt.datetime.combine(date_end, dt.datetime.min.time())
            try:
                if periodicity.endswith("m"):
                    data = self.api.get_time_price_series(
                        exchange=exch,
                        token=str(row_outer["Scripcode"]),
                        starttime=date_start.timestamp(),
                        endtime=date_end.timestamp(),
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
                                startdate=date_start.timestamp(),
                                enddate=date_end.timestamp(),
                            )
                            # If call succeeds, break out of loop
                            break
                        except Exception as e:
                            logger.error(f"Error in get_daily_price_series: {e}")
                            data = None

                        finally:
                            signal.alarm(0)  # Cancel the alarm

                        elapsed = time.time() - start_time
                        if elapsed < wait_seconds:
                            logger.info(f"Reattempting to get daily data for {row_outer['long_symbol']}")
                            time.sleep(wait_seconds - elapsed)
            except Exception as e:
                logger.error(f"Error in get_time_price_series or get_daily_price_series: {e}")
                data = None

            if not (data is None or len(data) == 0):
                for d in data:
                    if isinstance(d, str):
                        d = json.loads(d)
                    if periodicity.endswith("m"):
                        date = pd.Timestamp(timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%m-%Y %H:%M:%S")))
                    elif periodicity == "1d":
                        date = pd.Timestamp(timezone.localize(dt.datetime.strptime(d.get("time"), "%d-%b-%Y")))
                    historical_data = HistoricalData(
                        date=date,
                        open=float(d.get("into", "nan")),
                        high=float(d.get("inth", "nan")),
                        low=float(d.get("intl", "nan")),
                        close=float(d.get("intc", "nan")),
                        volume=float(d.get("intv", "nan")),
                        intoi=float(d.get("intoi", "nan")),
                        oi=float(d.get("oi", "nan")),
                    )
                    historical_data_list.append(historical_data)
            else:
                logger.debug(f"No data found for {row_outer['long_symbol']}")
                historical_data_list.append(
                    HistoricalData(
                        date=None,
                        open=float("nan"),
                        high=float("nan"),
                        low=float("nan"),
                        close=float("nan"),
                        volume=float("nan"),
                        intoi=float("nan"),
                        oi=float("nan"),
                    )
                )
            if periodicity == "1d" and date_end.date() == dt.datetime.today().date():
                # make a call to permin data for start date and end date of today
                if historical_data_list:
                    last_date = historical_data_list[0].date
                    if last_date is not None:
                        today_start = last_date + dt.timedelta(days=1)
                    else:
                        today_start = dt.datetime.today()
                    today_start = dt.datetime.combine(today_start, dt.datetime.min.time())
                else:
                    today_start = dt.datetime.combine(dt.datetime.today(), dt.datetime.min.time())
                try:
                    intraday_data = self.api.get_time_price_series(
                        exchange=exch,
                        token=str(row_outer["Scripcode"]),
                        starttime=today_start.timestamp(),
                        interval=1,  # Request 1-minute data
                    )
                except Exception as e:
                    logger.error(f"Error in get_time_price_series for intraday data: {e}")
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
                    date_start = timezone.localize(date_start)
                    date_end = timezone.localize(date_end)
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

    def map_exchange_for_api(self, long_symbol, exchange):
        """
        Map the exchange for API based on the long symbol and exchange.

        Args:
            long_symbol (str): The symbol string containing details like "_OPT_", "_FUT_".
            exchange (str): The original exchange identifier ("N", "B", or others).

        Returns:
            str: Mapped exchange for API.
        """
        exchange_map = {
            "N": "NFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "NSE",
            "B": "BFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "BSE",
            "NSE": "NFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "NSE",
            "BSE": "BFO" if any(sub in long_symbol for sub in ["_OPT_", "_FUT_"]) else "BSE",
        }

        # Return mapped exchange if "N" or "B", otherwise default to the given exchange
        return exchange_map.get(exchange, exchange)

    def map_exchange_for_db(self, long_symbol, exchange):
        """
        Map the exchange for the database based on the exchange's starting letter.

        Args:
            long_symbol (str): The symbol string (not used in this mapping but kept for consistency).
            exchange (str): The original exchange identifier.

        Returns:
            str: Mapped exchange ("NSE", "BSE", or the original exchange).
        """
        if exchange.startswith("N"):
            return "NSE"
        elif exchange.startswith("B"):
            return "BSE"
        else:
            return exchange

    def convert_ft_to_ist(self, ft: int):
        if ft == 0:
            return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        utc_time = dt.datetime.utcfromtimestamp(ft)
        # Add 5 hours and 30 minutes to get IST
        ist_time = utc_time + dt.timedelta(hours=5, minutes=30)

        # Format the datetime
        formatted_time = ist_time.strftime("%Y-%m-%d %H:%M:%S")
        return formatted_time

    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
        market_feed = Price()
        market_feed.src = "sh"
        market_feed.symbol = long_symbol
        token = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
        if token is not None:
            tick_data = self.api.get_quotes(exchange=mapped_exchange, token=str(token))
            market_feed.bid = (
                float("nan")
                if tick_data.get("bp1") in [None, 0, "0", "0.00", float("nan")]
                else float(tick_data.get("bp1"))
            )
            market_feed.ask = (
                float("nan")
                if tick_data.get("sp1") in [None, 0, "0", "0.00", float("nan")]
                else float(tick_data.get("sp1"))
            )
            market_feed.bid_volume = (
                float("nan") if tick_data.get("bq1") in [None, 0, "0", float("nan")] else float(tick_data.get("bq1"))
            )
            market_feed.ask_volume = (
                float("nan") if tick_data.get("sq1") in [None, 0, "0", float("nan")] else float(tick_data.get("sq1"))
            )
            market_feed.prior_close = (
                float("nan")
                if tick_data.get("c") in [None, 0, "0", "0,00", float("nan")]
                else float(tick_data.get("c"))
            )
            market_feed.last = (
                float("nan")
                if tick_data.get("lp") in [None, 0, "0", "0.00", float("nan")]
                else float(tick_data.get("lp"))
            )
            market_feed.high = (
                float("nan")
                if tick_data.get("h") in [None, 0, "0", "0.00", float("nan")]
                else float(tick_data.get("h"))
            )
            market_feed.low = (
                float("nan")
                if tick_data.get("l") in [None, 0, "0", "0.00", float("nan")]
                else float(tick_data.get("l"))
            )
            market_feed.volume = (
                float("nan") if tick_data.get("v") in [None, float("nan")] else float(tick_data.get("v"))
            )
            market_feed.exchange = self.map_exchange_for_db(long_symbol, tick_data.get("exch"))
            # market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
            market_feed.timestamp = self.convert_ft_to_ist(int(tick_data.get("lut", 0)))
            return market_feed
        else:
            return market_feed

    def start_quotes_streaming(self, operation: str, symbols: List[str], ext_callback=None, exchange="NSE"):
        prices = {}
        mapped_exchange = self.map_exchange_for_api(symbols[0], exchange)

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
            # price.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
            price.timestamp = self.convert_ft_to_ist(int(json_data.get("ft", 0)))
            price.symbol = symbol
            return price

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
                        price.last = float(message.get("h"))
                    if message.get("l"):
                        price.last = float(message.get("l"))
                    if message.get("v"):
                        price.volume = float(message.get("v"))
                    # price.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
                    price.timestamp = self.convert_ft_to_ist(int(message.get("ft", 0)))
                    prices[message.get("tk")] = price
                    ext_callback(price)
            else:
                pass

        def handle_socket_error(error=None):
            if error:
                logger.error(f"WebSocket error: {str(error)}")
            else:
                logger.error("WebSocket error. Connection to remote host was lost.")

            initiate_reconnect()

        def initiate_reconnect():
            logger.info("Attempting to reconnect...")
            time.sleep(5)
            try:
                self.socket_opened = False
                connect_and_subscribe()
                while not self.socket_opened:
                    time.sleep(1)
                self.api.subscribe(req_list)
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                self.socket_opened = False
                initiate_reconnect()

        def on_socket_open():
            logger.info("WebSocket connection opened")
            self.socket_opened = True

        def connect_and_subscribe():
            self.api.start_websocket(
                subscribe_callback=on_message,
                socket_close_callback=handle_socket_error,
                socket_error_callback=handle_socket_error,
                socket_open_callback=on_socket_open,
            )
            while not self.socket_opened:
                time.sleep(1)

        def expand_symbols_to_request(symbol_list):
            req_list = []
            for symbol in symbol_list:
                scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(symbol)
                if scrip_code:
                    req_list.append(f"{mapped_exchange}|{scrip_code}")
                else:
                    logger.error(f"Did not find scrip_code for {symbol}")
            return req_list

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
                logger.info(f"Requesting streaming for {req_list}")
                self.api.subscribe(req_list)
            elif operation == "u":
                logger.info(f"Unsubscribing streaming for {req_list}")
                self.api.unsubscribe(req_list)

    def get_position(self, long_symbol: str):
        pos = pd.DataFrame(self.api.get_positions())
        if len(pos) > 0:
            pos["long_symbol"] = self.get_long_name_from_broker_identifier(pos.tsym)
            if long_symbol is None:
                return pos
            else:
                pos = pos.loc[pos.long_symbol == long_symbol, "netqty"]
                if len(pos) == 0:
                    return 0
                elif len(pos) == 1:
                    return pos.item()
                else:
                    return Exception
        return pos

    def get_orders_today(self, **kwargs):
        return super().get_orders_today(**kwargs)

    def get_trades_today(self, **kwargs):
        return super().get_trades_today(**kwargs)

    def get_long_name_from_broker_identifier(self, **kwargs):
        #    def get_long_name_from_shoonya(ScripName: pd.Series) -> pd.Series:
        """Generates Long Name

        Args:
            ScripName (pd.Series): position.ScripName from 5paisa position

        Returns:
            pd.series: long name
        """

        def split_fno(fno_symbol):
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

        ScripName = kwargs.get("ScripName")
        return ScripName.apply(lambda x: split_cash(x) if x[-3] == "-" else split_fno(x))

    def get_min_lot_size(self, long_symbol, exchange="NSE"):
        exchange = self.map_exchange_for_api(long_symbol, exchange)
        code = self.exchange_mappings[exchange]["symbol_map"].get(long_symbol)
        if code is not None:
            return self.codes.loc[self.codes.Scripcode == code, "LotSize"].item()
        else:
            return 0
