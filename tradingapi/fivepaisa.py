import datetime as dt
import inspect
import io
import json
import logging
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

from .broker_base import (BrokerBase, Brokers, HistoricalData, Order,
                          OrderInfo, OrderStatus, Price)
from .config import get_config
from .utils import (delete_broker_order_id, set_starting_internal_ids_int,
                    update_order_status)

logger = logging.getLogger(__name__)
config = get_config()


# Exception handler
def my_handler(typ, value, trace):
    logger.error("%s %s %s", typ.__name__, value, "".join(traceback.format_tb(trace)))


sys.excepthook = my_handler


def save_symbol_data(saveToFolder: bool = False):
    bhavcopyfolder = config.get("bhavcopy_folder")
    url = "https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/All"
    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_codes.csv"
    response = requests.get(url, allow_redirects=True, timeout=10)  # Add timeout to `requests.get` to fix Bandit issue
    if response.status_code == 200:
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
                except ValueError:
                    return pd.NA
            elif len(symbol_vec) == 6:
                expiry_str = f"{symbol_vec[3]}{symbol_vec[2]}{symbol_vec[1]}"
                try:
                    expiry = dt.datetime.strptime(expiry_str, "%Y%b%d").strftime("%Y%m%d")
                    right = "CALL" if symbol_vec[4] == "CE" else "PUT"
                    strike = ("%f" % float(symbol_vec[5])).rstrip("0").rstrip(".")
                    return f"{symbol_vec[0]}_OPT_{expiry}_{right}_{strike}".upper()
                except ValueError:
                    return pd.NA
            else:
                return pd.NA

        # Apply the function to each row
        codes["long_symbol"] = codes.apply(process_row, axis=1)

        # Save to CSV
        filtered_codes = codes.dropna(subset=["long_symbol"])
        if saveToFolder:
            dest_symbol_file = (
                f"{config.get('FIVEPAISA.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
            )
            filtered_codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize"]].to_csv(
                dest_symbol_file, index=False
            )
        return filtered_codes


class FivePaisa(BrokerBase):
    def __init__(self, **kwargs):
        """
        mandatory_keys = None

        Raises:
            ValueError: _description_
        """
        super().__init__()
        self.codes = pd.DataFrame()
        self.broker = Brokers.FIVEPAISA
        self.api = None
        self.subscribe_thread = None
        self.subscribed_symbols = []

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
        self.redis_o.zadd("FIVEPAISA:LOG", {json.dumps(log_entry): time.time()})

    def update_symbology(self, **kwargs):
        dt_today = dt.datetime.today().strftime("%Y%m%d")
        symbols_path = os.path.join(config.get(f"{self.broker.name}.SYMBOLCODES"), f"{dt_today}_symbols.csv")

        # Load symbols data
        if not os.path.exists(symbols_path):
            codes = save_symbol_data(saveToFolder=False)
            codes = codes.dropna(subset=["long_symbol"])
        else:
            codes = pd.read_csv(symbols_path)

        # Initialize dictionaries to hold mappings for each exchange
        self.exchange_mappings = {}

        for exchange, group in codes.groupby("Exch"):
            self.exchange_mappings[exchange] = {
                "symbol_map": dict(zip(group["long_symbol"], group["Scripcode"])),
                "contractsize_map": dict(zip(group["long_symbol"], group["LotSize"])),
                "exchange_map": dict(zip(group["long_symbol"], group["Exch"])),
                "exchangetype_map": dict(zip(group["long_symbol"], group["ExchType"])),
                "contracttick_map": dict(zip(group["long_symbol"], group["TickSize"])),
                "symbol_map_reversed": dict(zip(group["Scripcode"], group["long_symbol"])),
            }
        return codes

    def connect(self, redis_db: int):
        def extract_credentials():
            return {
                "APP_SOURCE": config.get(f"{self.broker.name}.APP_SOURCE"),
                "APP_NAME": config.get(f"{self.broker.name}.APP_NAME"),
                "USER_ID": config.get(f"{self.broker.name}.USER_ID"),
                "PASSWORD": config.get(f"{self.broker.name}.PASSWORD"),
                "USER_KEY": config.get(f"{self.broker.name}.USER_KEY"),
                "ENCRYPTION_KEY": config.get(f"{self.broker.name}.ENCRYPTION_KEY"),
            }

        susertoken_path = config.get(f"{self.broker.name}.USERTOKEN")
        logged_in_user = False
        if config.get(self.broker.name) != {}:
            self.codes = self.update_symbology()
            if os.path.exists(susertoken_path):
                mod_time = os.path.getmtime(susertoken_path)
                mod_datetime = dt.datetime.fromtimestamp(mod_time)
                today = dt.datetime.now().date()
                if mod_datetime.date() == today:
                    with open(susertoken_path, "r") as file:
                        susertoken = file.read().strip()
                    client_id = config.get(f"{self.broker.name}.CLIENT_ID")
                    self.api = FivePaisaClient(cred=extract_credentials())
                    self.api.set_access_token(susertoken, client_id)
                    if self.api.Login_check() == ".ASPXAUTH=None":
                        logged_in_user = False
                    else:
                        logged_in_user = True
            if not logged_in_user:
                self.api = FivePaisaClient(cred=extract_credentials())
                max_attempts = 5
                for attempt in range(1, max_attempts + 1):
                    otp = pyotp.TOTP(config.get(f"{self.broker.name}.TOTP_TOKEN")).now()
                    self.api.get_totp_session(
                        config.get(f"{self.broker.name}.CLIENT_ID"),
                        otp,
                        config.get(f"{self.broker.name}.PIN"),
                    )
                    if self.api.access_token:
                        logger.info(f"Connected successfully on attempt {attempt}.")
                        susertoken = self.api.access_token
                        with open(susertoken_path, "w") as file:
                            file.write(susertoken)
                            logged_in_user = True
                            break
                    else:
                        logger.warning(f"Attempt {attempt} failed. Retrying in 40 seconds...")
                        time.sleep(40)
                # attributes_string = ""
                # for key, value in vars(self.api).items():
                #     attributes_string += "{}: {}\n".format(key, value)
                # logger.debug(f"{attributes_string}")
            self.redis_o = redis.Redis(db=redis_db, charset="utf-8", decode_responses=True)
            self.starting_order_ids_int = set_starting_internal_ids_int(redis_db=self.redis_o)
            if not logged_in_user:
                logger.error("Error: Not connected. Access token is missing after 5 attempts.")
                raise ValueError("Error: Not connected. Access token is missing.")
        else:
            logger.error("Configuration file not found.")
            sys.exit(1)

    def is_connected(self):
        try:
            if (
                float(self.api.margin()[0]["Ledgerbalance"]) + float(self.api.margin()[0]["FundsPayln"]) > 0
                and self.get_quote("NIFTY_IND___").last > 0
            ):
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            return False

    def disconnect(self):
        return super().disconnect()

    def place_order(self, order: Order, **kwargs) -> Order:
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
        # Check for missing keys
        missing_keys = [key for key in mandatory_keys if key not in order.to_dict() or order.to_dict()[key] is None]
        # If there are missing keys, raise an exception or print an error
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
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
                if order.exchange == "M":
                    min_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(order.long_symbol)
                    quantity: Optional[int] = None  # Initialize as None
                    if min_size is not None and order.quantity is not None:
                        quantity = int(round(order.quantity / min_size, 0))
                else:
                    quantity = order.quantity
                out = self.api.place_order(
                    OrderType=order.order_type,
                    Exchange=order.exchange,
                    ExchangeType=order.exchange_segment,
                    ScripCode=order.scrip_code,
                    Qty=quantity,
                    Price=order.price,
                    RemoteOrderID=order.remote_order_id,
                )
                if out is not None:
                    order.exch_order_id = out.get("ExchOrderID")
                    order.broker_order_id = out.get("BrokerOrderID")
                    order.local_order_id = out.get("LocalOrderID")
                    order.order_type = orig_order_type
                    order.orderRef = order.internal_order_id
                    order.message = out.get("Message")
                    order.status = out.get("Status")
                    order.exch_order_id = self._get_exchange_order_id(order.broker_order_id, order, delete=False)
                    fills = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                    order.status = fills.status
                    order.exch_order_id = fills.exchange_order_id
                    if fills.status == OrderStatus.REJECTED:
                        logger.info(f"Order Rejected by broker. Order: {order}")
                        return order
                    if fills.fill_price > 0:
                        order.price = fills.fill_price
                    logger.info(f"Placed Order: {order}")
                    self.log_and_return(order)
                    return order
            else:
                order.order_type = orig_order_type
                order.exch_order_id = str(secrets.randbelow(10**15)) + "P"  # Replace `random` with `secrets`
                order.broker_order_id = str(secrets.randbelow(10**8)) + "P"  # Replace `random` with `secrets`
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
        broker_order_id = str(kwargs.get("broker_order_id", "0"))
        new_price = float(kwargs.get("new_price", 0))
        new_quantity = kwargs.get("new_quantity", 0)
        order = Order(**self.redis_o.hgetall(broker_order_id))
        fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
        if fills.status in [OrderStatus.UNDEFINED, OrderStatus.PENDING]:
            logger.info("Order not active with exchange and cannot be cancelled. f{fills}")
        order.status = fills.status
        if order.status in [OrderStatus.OPEN]:
            logger.info(
                f"Modifying order for {broker_order_id} as not filled. Old Price: {order.price}, New Price: {new_price}."
                f"Old Quantity: {order.quantity}, New Quantity: {new_quantity}, Fill info: {fills}"
            )
            exch_order_id = order.exch_order_id
            if order.exchange == "M":
                long_symbol = order.long_symbol
                order_quantity = int(
                    round(new_quantity / self.exchange_mappings[order.exchange]["contractsize_map"].get(long_symbol), 0)
                )
            else:
                order_quantity = new_quantity - fills.fill_size
                order_quantity = order_quantity if order_quantity > 0 else 0
            out = self.api.modify_order(ExchOrderID=exch_order_id, Price=new_price, Qty=order_quantity)
            if out is None:
                logger.error(f"Error modifying order {broker_order_id}")
            else:
                try:
                    self.log_and_return(out)
                    if out["Status"] == 0:
                        order.price_type = new_price
                        order.quantity = new_quantity
                        order.price = new_price
                        order.broker_order_id = str(out.get("BrokerOrderID", ""))
                        order_info = self.get_order_info(broker_order_id=order.broker_order_id, order=order)
                        order.status = order_info.status
                        order.exch_order_id = order_info.exchange_order_id
                        self._update_broker_order_id(order.internal_order_id, broker_order_id, out["BrokerOrderID"])
                        self.redis_o.hmset(
                            order.broker_order_id, {key: str(val) for key, val in order.to_dict().items()}
                        )
                        fills = update_order_status(self, order.internal_order_id, order.broker_order_id)
                        order.status = fills.status
                except Exception as e:
                    logger.error(f"Error: {e}", exc_info=True)
            self.log_and_return(order)
            return order
        else:
            logger.info(
                f"Order status does not allow modification for {order.broker_order_id}. Status was {order.status}"
            )
            self.log_and_return(order)
            return order

    def cancel_order(self, **kwargs) -> Order:
        """
        mandatory_keys = ['broker_order_id']

        """
        mandatory_keys = ["broker_order_id"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = str(kwargs.get("broker_order_id", "0"))

        order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.status in [OrderStatus.OPEN, OrderStatus.PENDING, OrderStatus.UNDEFINED]:
            valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
            if valid_date and valid_date == dt.datetime.today().strftime("%Y-%m-%d"):
                fills = self.get_order_info(broker_order_id=broker_order_id, order=order)
                if fills.fill_size < round(float(order.quantity)):
                    logger.info(
                        f"Cancelling broker_order_id {broker_order_id} for symbol {order.long_symbol}. "
                        f"Filled: {str(fills.fill_size)}. Ordered: {order.quantity}"
                    )
                    out = self.api.cancel_order(exch_order_id=order.exch_order_id)
                    self.log_and_return(out)
                    order.broker_order_id = out["BrokerOrderID"]
                    self._update_broker_order_id(order.internal_order_id, broker_order_id, out["BrokerOrderID"])
                    self.redis_o.hmset(order.broker_order_id, {key: str(val) for key, val in order.to_dict().items()})
                    fills = update_order_status(self, order.internal_order_id, order.broker_order_id, eod=True)
                    self.log_and_return(fills)
                    order.status = fills.status
                    order.quantity = fills.fill_size
                    order.price = fills.fill_price
                    self.log_and_return(order)
                    return order
        self.log_and_return(order)
        return order

    def _update_broker_order_id(self, internal_order_id: str, old_broker_order_id: str, new_broker_order_id: str):
        # Retrieve the entry keys
        entry_keys = self.redis_o.hget(internal_order_id, "entry_keys")
        # Initialize new_entry_keys as None
        new_entry_keys = None
        new_exit_keys = None

        # Check if broker_order_id is in entry_keys
        if entry_keys and str(old_broker_order_id) in entry_keys:
            new_entry_keys = entry_keys.replace(str(old_broker_order_id), str(new_broker_order_id))
        else:
            # Retrieve the exit keys if broker_order_id is not in entry_keys
            exit_keys = self.redis_o.hget(internal_order_id, "exit_keys")
            if exit_keys and str(old_broker_order_id) in exit_keys:
                new_exit_keys = exit_keys.replace(str(old_broker_order_id), str(new_broker_order_id))

        # Perform Redis operations only if broker_order_id is found in either entry_keys or exit_keys
        if new_entry_keys is not None or new_exit_keys is not None:
            pipe = self.redis_o.pipeline()
            if new_entry_keys is not None:
                pipe.hset(internal_order_id, "entry_keys", new_entry_keys)
            if new_exit_keys is not None:
                pipe.hset(internal_order_id, "exit_keys", new_exit_keys)
            pipe.rename(str(old_broker_order_id), str(new_broker_order_id))
            pipe.execute()
            pipe.reset()

    def get_order_info(self, **kwargs) -> OrderInfo:
        def return_db_as_fills(order: Order):
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

        def get_orderinfo_from_orders(exch_order_id: str, order: Order, broker_order_id: str) -> OrderInfo:
            orders = pd.DataFrame(self.api.order_book())
            if len(orders) > 0:
                fivepaisa_order = orders[orders.BrokerOrderId.astype(str) == str(broker_order_id)]
                if len(fivepaisa_order) == 1:
                    if fivepaisa_order.OrderStatus.str.lower().str.contains("rejected").item() is True:
                        # order cancelled by broker before reaching exchange
                        logger.info(f"Order Rejected Reason: {str(fivepaisa_order.Reason.item())}")
                        broker_order_id = str(fivepaisa_order.BrokerOrderId.item())
                        internal_order_id = self.redis_o.hget(broker_order_id, "orderRef")
                        if internal_order_id is not None:
                            delete_broker_order_id(self, internal_order_id, broker_order_id)
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
                            long_symbol = (
                                self.get_long_name_from_broker_identifier(ScripName=pd.Series[order.long_symbol]).item()
                                if order
                                else self.get_long_name_from_broker_identifier(
                                    ScripName=fivepaisa_order.ScripName
                                ).item()
                            )
                            contract_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(long_symbol)
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
                    logger.debug(
                        f"Found duplicate orders with same exchange order id in order book. exchange order id:{exch_order_id}"
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
                                logger.info(f"Order Rejected Reason: {row['Reason']}")
                                status = OrderStatus.REJECTED
                            elif fill_size == order_size:
                                status = OrderStatus.FILLED
                            elif row["ExchOrderID"] not in [0, "0", None, "None"]:
                                status = OrderStatus.OPEN
                            else:
                                status = OrderStatus.PENDING
                            if order.exchange == "M":
                                long_symbol = (
                                    self.get_long_name_from_broker_identifier(
                                        ScripName=pd.Series[order.long_symbol]
                                    ).item()
                                    if order.long_symbol is not None
                                    else self.get_long_name_from_broker_identifier(ScripName=row["ScripName"]).item()
                                )
                                contract_size = self.exchange_mappings[order.exchange]["contractsize_map"].get(
                                    long_symbol
                                )
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

        mandatory_keys = ["broker_order_id"]
        missing_keys = [key for key in mandatory_keys if key not in kwargs]
        if missing_keys:
            raise ValueError(f"Missing mandatory keys: {', '.join(missing_keys)}")
        broker_order_id = str(kwargs.get("broker_order_id", "0"))
        order = kwargs.get("order", None)
        if order is None:
            order = Order(**self.redis_o.hgetall(broker_order_id))
        valid_date, _ = valid_datetime(order.remote_order_id[:8], "%Y-%m-%d")
        if (
            valid_date
            and valid_date != dt.datetime.today().strftime("%Y-%m-%d")
            or (order.remote_order_id == "" and order.broker != self.broker)
        ):
            return return_db_as_fills(order)

        if str(order.broker_order_id) == "0":  # no data in redis
            out = get_orderinfo_from_orders("0", order=order, broker_order_id=broker_order_id)
            return out

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
        remote_order_id = order.remote_order_id
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
            out = get_orderinfo_from_orders("0", order=order, broker_order_id=broker_order_id)
            return out
        else:
            exchange = order.exchange
            if exchange == "M":
                if order.exch_order_id not in ["0", "None", 0, None]:
                    out = get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                    return out
            else:
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
                    out = get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                    return out
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
                    logger.debug(
                        f"Fill prices: {' '.join(str(trade['Rate']) for trade in trade_details)}, Fill Sizes: {' '.join(str(trade['Qty']) for trade in trade_details)}"
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
                    out = get_orderinfo_from_orders(order.exch_order_id, order, broker_order_id)
                    return out
        logger.error("get_order_info: No valid return path found.")
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

    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: str,
        date_end: str = dt.datetime.today().strftime("%Y-%m-%d"),
        exchange: str = "N",  # Fix type for `exchange`
        periodicity: str = "1m",  # Fix type for `periodicity`
        market_close_time: str = "15:30:00",
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
            dest_folder (str, optional): Path to data folder. Defaults to None. If provided, historical data is saved to dest_folder as rds file.

        Returns:
            Dict[str, List[HistoricalData]]: Dictionary with historical data for each symbol.
        """
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
            exch_type = (
                symbols.get("exch_type")
                if isinstance(symbols, dict)
                else self.exchange_mappings[exchange]["exchangetype_map"].get(row_outer["long_symbol"])
            )
            s = row_outer["long_symbol"].replace("/", "-")
            row_outer["long_symbol"] = "NSENIFTY" + s[s.find("_") :] if s.startswith("NIFTY_") else s
            date_start_str, _ = valid_datetime(date_start, "%Y-%m-%d")
            date_end_str, _ = valid_datetime(date_end, "%Y-%m-%d")
            data = self.api.historical_data(
                exch, exch_type, row_outer["Scripcode"], periodicity, date_start_str, date_end_str
            )
            if not (data is None or len(data) == 0):
                data.columns = ["date", "open", "high", "low", "close", "volume"]
                data["date"] = pd.to_datetime(data["date"])
                data["date"] = data["date"].dt.tz_localize("Asia/Kolkata")
                data = data[data["date"].dt.time < pd.to_datetime(market_close_time).time()]
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
            else:
                logger.debug(f"No data found for {row_outer['long_symbol']}")
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

        return out

    def get_close(
        self,
        long_symbol: str,
        exchange="N",
        bar_duration="1m",
        timestamp: dt.datetime = dt.datetime(1970, 1, 1),
        adj=False,
    ) -> float:
        """Get last traded price from five paisa

        Args:
            long_symbol (str): long symbol
            bar_duration (str, optional): Defaults to '1m'.
            timestamp (dt.datetime, optional): Formatted as %Y-%m-%d%T%H:%M:%S. Defaults to None and then provides the last traded price
            adj (bool, optional): If True, provide hlc3 for the shortlisted bar. Defaults to False.

        Returns:
            float: last traded price on or before timestamp. None if no price found
        """
        exchange = self.map_exchange_for_api(long_symbol, exchange)
        if timestamp is None:
            timestamp = dt.datetime.now()
        elif isinstance(timestamp, str):
            timestamp, _ = valid_datetime(timestamp)
        cutoff_time = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

        md = self.api.historical_data(
            exchange,
            self.exchange_mappings[exchange]["exchangetype_map"].get(long_symbol),
            self.exchange_mappings["N"]["symbol_map"].get(long_symbol),
            bar_duration,
            (timestamp - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            cutoff_time,
        )
        if md is not None:
            md = md.loc[md.Datetime <= cutoff_time,].tail(1).squeeze()
            if adj:
                return (md.High + md.Low + md.Close) / 3
            else:
                return md.Close
        else:
            return float("nan")

    def map_exchange_for_api(self, long_symbol, exchange):
        return exchange[0]

    def map_exchange_for_db(self, long_symbol, exchange):
        if exchange[0] == "N":
            return "NSE"
        elif exchange[0] == "B":
            return "BSE"
        else:
            return exchange

    def convert_to_ist(self, date_string):
        """
        Convert a string in the format '/Date(1732010010000)/' to IST date and time.

        Args:
            date_string (str): The string containing the date in /Date(milliseconds)/ format.

        Returns:
            str: The corresponding date and time in IST (yyyy-mm-dd hh:mm:ss).
        """
        # Extract the timestamp using regex
        match = re.search(r"/Date\((\d+)\)/", date_string)
        if not match:
            logger.error(f"Invalid date format. Expected '/Date(milliseconds)/'.Received {date_string}")
            return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Convert the timestamp from milliseconds to seconds
        timestamp_ms = int(match.group(1))
        timestamp_s = timestamp_ms / 1000

        # Convert to UTC datetime
        utc_time = dt.datetime.utcfromtimestamp(timestamp_s)

        # Convert to IST by adding 5 hours and 30 minutes
        ist_time = utc_time + dt.timedelta(hours=5, minutes=30)

        # Format the IST datetime
        return ist_time.strftime("%Y-%m-%d %H:%M:%S")

    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """Get Quote details of a symbol

        Args:
            long_symbol (str): long symbol

        Returns:
            json: Quote
        """
        mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
        market_feed = Price()
        market_feed.src = "fp"
        market_feed.symbol = long_symbol
        exch_type = self.exchange_mappings[mapped_exchange]["exchangetype_map"].get(long_symbol)
        scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
        if scrip_code is None:
            return market_feed
        req_list = [
            {"Exch": mapped_exchange, "ExchType": exch_type, "ScripCode": scrip_code},
        ]
        out = self.api.fetch_market_feed_scrip(req_list)
        snapshot = out["Data"][0]
        out = self.api.fetch_market_depth_by_scrip(
            Exchange=mapped_exchange, ExchangeType=exch_type, ScripCode=scrip_code
        )
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

        market_feed.ask = best_ask_price if best_ask_price is not None and best_ask_price != 0 else market_feed.ask
        market_feed.bid = best_bid_price if best_bid_price is not None and best_bid_price != 0 else market_feed.bid
        market_feed.bid_volume = (
            best_bid_quantity if best_bid_quantity is not None and best_bid_quantity > 0 else market_feed.bid_volume
        )
        market_feed.ask_volume = (
            best_ask_quantity if best_ask_quantity is not None and best_ask_quantity > 0 else market_feed.ask_volume
        )
        market_feed.exchange = snapshot["Exch"]
        market_feed.high = snapshot["High"] if snapshot["High"] > 0 else market_feed.high
        market_feed.low = snapshot["Low"] if snapshot["Low"] > 0 else market_feed.low
        market_feed.last = snapshot["LastRate"] if snapshot["LastRate"] > 0 else market_feed.last
        market_feed.prior_close = snapshot["PClose"] if snapshot["PClose"] > 0 else market_feed.prior_close
        market_feed.src = "fp"
        market_feed.symbol = long_symbol
        market_feed.volume = snapshot["TotalQty"] if snapshot["TotalQty"] > 0 else market_feed.volume
        market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return market_feed

    def start_quotes_streaming(self, operation: str, symbols=List[str], ext_callback=None, exchange="NSE"):
        logger.info(f"Operation: {operation}, symbols: {symbols},exchange:{exchange}")
        mapped_exchange = self.map_exchange_for_api(symbols[0], exchange)

        def map_to_price(json_data):
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
            price.symbol = self.exchange_mappings[json_data["Exch"]]["symbol_map_reversed"].get(json_data.get("Token"))
            price.exchange = self.map_exchange_for_db(price.symbol, json_data["Exch"])
            price.timestamp = self.convert_to_ist(json_data["TickDt"])
            return price

        def on_message(ws, message):
            try:
                data_str = message.replace("\/", "/")
                json_data = json.loads(data_str)
                if len(json_data) == 1:
                    price = map_to_price(json_data[0])
                    ext_callback(price)
            except Exception:
                logger.error(f"{traceback.format_exc()}")

        def error_data(ws, err):
            try:
                logger.error(f"WebSocket error: {err}")
                reconnect()
            except Exception as e:
                logger.error(f"Error handling WebSocket error: {e}")

        def reconnect():
            logger.info("Attempting to reconnect...")
            time.sleep(5)  # Wait for a few seconds before reconnecting
            try:
                req_data = expand_symbols_to_request(
                    self.subscribed_symbols
                )  # Store req_data for reconnection purposes
                connect_and_receive(req_data)
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                reconnect()

        # Function to connect and receive data
        def connect_and_receive(req_data):
            self.api.connect(req_data)  # Implement your connection logic here
            self.api.receive_data(on_message)
            self.api.error_data(error_data)

        def expand_symbols_to_request(symbols: list):
            req_list = []
            for long_symbol in symbols:
                market_feed = Price()
                market_feed.src = "fp"
                market_feed.symbol = long_symbol
                exch_type = self.exchange_mappings[mapped_exchange]["exchangetype_map"].get(long_symbol)
                scrip_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
                if scrip_code is None:
                    logger.error(f"Did not find scrip_code for {long_symbol}")
                    continue
                req_list.append({"Exch": mapped_exchange, "ExchType": exch_type, "ScripCode": scrip_code})
                return req_list

        def update_current_subscriptions(operation, symbols):
            if operation == "s":
                self.subscribed_symbols.extend(symbols)
            elif operation == "u":
                self.subscribed_symbols = list(set(self.subscribed_symbols) - set(symbols))

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
            else:
                logger.info(f"Requesting streaming for {json.dumps(req_data)}")
                self.api.ws.send(json.dumps(req_data))

    def stop_streaming(self):
        self.api.close_data()

    def get_position(self, long_symbol: str = "") -> Union[pd.DataFrame, int]:
        """Retrieves position from 5paisa

        Args:
            long_symbol (str, optional): symbol name. Defaults to None and returns all positions

        Returns:
            Union[pd.DataFrame, int]: siged position if long_symbol is not None. Else dataframe containing all positions
        """
        holding = pd.DataFrame(self.api.holdings())
        holding = pd.DataFrame(columns=["long_symbol", "quantity"]) if len(holding) == 0 else holding
        if len(holding) > 0:
            holding["long_symbol"] = self.get_long_name_from_broker_identifier(ScripName=holding.Symbol)
            holding = holding.loc[:, ["long_symbol", "Quantity"]]
            holding.columns = ["long_symbol", "quantity"]
        position = pd.DataFrame(self.api.positions())
        position = pd.DataFrame(columns=["long_symbol", "quantity"]) if len(position) == 0 else position
        if len(position) > 0:
            position["long_symbol"] = self.get_long_name_from_broker_identifier(ScripName=position.ScripName)
            position = position.loc[:, ["long_symbol", "NetQty"]]
            position.columns = ["long_symbol", "quantity"]
        merged_df = pd.merge(position, holding, on="long_symbol", how="outer")
        result = merged_df.groupby("long_symbol", as_index=False).agg({"quantity_x": "sum", "quantity_y": "sum"})
        result["quantity"] = result.quantity_y + result.quantity_x
        result = result.loc[:, ["long_symbol", "quantity"]]
        if long_symbol is None:
            return result
        else:
            pos = result.loc[result.long_symbol == long_symbol, "quantity"]
            if len(pos) == 0:
                return 0
            elif len(pos) == 1:
                return pos.item()
            else:
                return Exception

    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        orders = self.api.order_book()
        orders = pd.DataFrame.from_dict(orders)
        if len(orders.index) > 0:
            orders = orders.assign(long_symbol=self.get_long_name_from_broker_identifier(ScripName=orders.ScripName))
            return orders
        else:
            return None

    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        trades = self.api.get_tradebook()
        trades = pd.DataFrame.from_dict(trades)
        if len(trades) > 0:
            trades = trades.loc[trades.Status == 0, "TradeBookDetail"]
            trades = pd.DataFrame([trade for trade in trades])
            trades.ExchangeTradeTime = trades.ExchangeTradeTime.apply(self._convert_date_string)
            trades = trades.assign(long_symbol=self.get_long_name_from_broker_identifier(ScripName=trades.ScripName))
            return trades
        else:
            return None

    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """
        Generates Long Name.

        Args:
            kwargs: Arbitrary keyword arguments. Expected key:
                - ScripName (pd.Series): position.ScripName from 5paisa position.

        Returns:
            pd.Series: Long name.
        """
        ScripName = kwargs.get("ScripName")
        if ScripName is None:
            raise ValueError("Missing required argument: 'ScripName'")
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
            if sec_type == "OPT":
                expiry.iloc[idx] = pd.to_datetime(("-").join(ScripName[idx].split()[1:4])).strftime("%Y%m%d")
                right_map = {
                    "PE": "PUT",
                    "CE": "CALL",
                }
                right.iloc[idx] = right_map.get(ScripName[idx].split()[4])
                strike.iloc[idx] = ScripName[idx].split()[5].strip("0").strip(".")
            elif sec_type == "FUT":
                expiry.iloc[idx] = pd.to_datetime(("-").join(ScripName[idx].split()[1:4])).strftime("%Y%m%d")

        return symbol + "_" + sec_types + "_" + expiry + "_" + right + "_" + strike

    def _get_exchange_order_id(self, broker_order_id: str, order: Order = Order(), delete: bool = True) -> str:
        """Retrieves exchange order id for trades executed today.

        Args:
            broker_order_id (str): broker order id
            order (Order, optional): Order object. Defaults to None.
            delete (bool, optional): if True, broker order id is deleted from redis, if no exchange order id is found
            Defaults to True.

        Returns:
            str: exchange order if available, else empty string
        """

        def get_exchange_order_id_from_orders(broker_order_id: str) -> str:
            orders = pd.DataFrame(self.api.order_book())
            fivepaisa_order = orders[orders.BrokerOrderId == int(broker_order_id)]
            if len(fivepaisa_order) == 1:
                return str(fivepaisa_order.ExchOrderID.item())
            else:
                logger.error(f"Trade {broker_order_id} did not exist in 5paisa.")
                return "0"

        exch_order_id = "0"
        if order is None:
            order = Order(**self.redis_o.hgetall(broker_order_id))
        if order.exch_order_id not in ["0", "None", 0, None]:
            return order.exch_order_id
        else:
            exch = order.exchange
            if exch != "M":
                remote_order_id = order.remote_order_id
            else:
                remote_order_id = broker_order_id
            req_list_ = [{"Exch": exch, "RemoteOrderID": remote_order_id}]
            status = self.api.fetch_order_status(req_list_)
            if status is not None and len(status["OrdStatusResLst"]) > 0:
                for sub_status in status["OrdStatusResLst"]:
                    if str(sub_status.get("ScripCode")) == str(order.scrip_code):
                        exch_order_id = str(sub_status.get("ExchOrderID") or "")
                        logger.info(f"Retrieving Exchange ID {exch_order_id}")
                        return exch_order_id

            exch_order_id = get_exchange_order_id_from_orders(broker_order_id)
            if exch_order_id not in ["0", "None", 0, ""]:
                return exch_order_id
            else:
                logger.error(f"Order for {broker_order_id} did not exist in 5paisa orderbook.")
                if delete:
                    logger.error(
                        f"Deleting {broker_order_id} "
                        f"for intenal order {self.redis_o.hget(broker_order_id, 'orderRef')}"
                    )
                    delete_broker_order_id(self, self.redis_o.hget(broker_order_id, "orderRef"), broker_order_id)
                return "0"

    def _convert_date_string(self, date_string: str) -> dt.datetime:
        """convert 5paisa datestring in positions/orders to datetime

        Args:
            date_string (str): 5paisa datetime string

        Raises:
            ValueError: _description_

        Returns:
            dt.datetime: datetime object
        """
        # Extract timestamp and timezone offset from the string
        # match = re.match(r"/Date\((\d+)([-+]\d{2})(\d{2})\)/", date_string)
        pattern = r"/Date\((\d+)([+-]\d{4})\)/"
        match = re.match(pattern, date_string)
        if match:
            timestamp = int(match.group(1))
            # timezone_hours = int(match.group(2))
            # timezone_minutes = int(match.group(3))
            # Create a datetime object using the timestamp and timezone offset
            temp = dt.datetime.fromtimestamp(timestamp / 1000)
            return temp
        else:
            logger.error("incorrect datestring format: {date_string}")
            time.time()
            return dt.datetime(1970, 1, 1)

    def get_min_lot_size(self, long_symbol, exchange="N") -> int:
        exchange = self.map_exchange_for_api(long_symbol, exchange)
        code = self.exchange_mappings[exchange]["symbol_map"].get(long_symbol)
        if code is not None:
            return self.codes.loc[self.codes.Scripcode == code, "LotSize"].item()
        else:
            return 0
