import datetime as dt
import io
import inspect
import json
import logging
import math
import sys
import time
import traceback
import zipfile
from typing import Dict, List, Union

import pandas as pd
import redis
import requests
from breeze_connect import BreezeConnect

from .broker_base import (
    BrokerBase,
    Brokers,
    HistoricalData,
    Order,
    OrderInfo,
    OrderStatus,
    Position,
    Price,
)
from .config import get_config
from .utils import set_starting_internal_ids_int, update_order_status
from .exceptions import (
    AuthenticationError,
    BrokerConnectionError,
    ConfigurationError,
    DataError,
    MarketDataError,
    OrderError,
    SymbolError,
    TradingAPIError,
    ValidationError,
    create_error_context,
)
from .error_handling import retry_on_error, safe_execute, log_execution_time, validate_inputs
from . import trading_logger
from .globals import get_tradingapi_now

logger = logging.getLogger(__name__)
config = get_config()


@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = True) -> pd.DataFrame:
    """
    Download and process ICICIDirect/Breeze symbol master into a standard
    long_symbol-based CSV layout similar to other brokers.

    This implementation is intentionally conservative and focuses on
    cash/index instruments. Derivatives can be added later once the
    exact file structure is finalized.
    """
    bhavcopyfolder = config.get("bhavcopy_folder")
    url = config.get("ICICIDIRECT.SYMBOL_MASTER_URL")

    if not bhavcopyfolder:
        raise ConfigurationError(
            "Missing 'bhavcopy_folder' in config for IciciDirect symbol data",
            create_error_context(),
        )

    if not url:
        raise ConfigurationError(
            "Missing 'ICICIDIRECT.SYMBOL_MASTER_URL' in config for IciciDirect symbol data",
            create_error_context(),
        )

    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_icicidirect_instruments.csv"

    try:
        response = requests.get(url, allow_redirects=True, timeout=60)
        if response.status_code != 200:
            raise DataError(
                f"Failed to fetch IciciDirect symbol data. Status code: {response.status_code}",
                create_error_context(status_code=response.status_code, url=url),
            )

        # The SecurityMaster is a ZIP; read specific inner files for NSE/BSE cash and F&O.
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            all_names = set(zf.namelist())
            wanted_files = [
                "NSEScripMaster.txt",   # NSE cash
                "BSEScripMaster.txt",   # BSE cash
                "FONSEScripMaster.txt", # NSE F&O
                "FOBSEScripMaster.txt", # BSE F&O
            ]

            parts: list[pd.DataFrame] = []

            for fname in wanted_files:
                if fname not in all_names:
                    continue

                with zf.open(fname) as f:
                    df = pd.read_csv(f)
                # Clean up column names: trim whitespace and surrounding quotes
                df.columns = [c.strip().strip('"') for c in df.columns]

                trading_logger.log_debug(
                    "IciciDirect raw symbol master slice loaded",
                    {"file": fname, "shape": df.shape, "columns": list(df.columns)},
                )

                lower_cols = {c.lower(): c for c in df.columns}

                def pick_column(candidates):
                    for key in candidates:
                        if key in lower_cols:
                            return lower_cols[key]
                    return None

                symbol_col = pick_column(["stock_code", "stockcode", "exchangecode", "securitysymbol", "short_name"])
                lot_col = pick_column(["lot_size", "lotsize", "lot"])
                tick_col = pick_column(["tick_size", "ticksize"])
                scrip_code = pick_column(["token", "scripcode", "shortname"])
                instrument_col = pick_column(["instrument", "instrumentname"])

                expiry_col = pick_column(["expiry_date", "expirydate", "expiry"])
                strike_col = pick_column(["strike_price", "strikeprice", "strike"])
                option_col = pick_column(["option_type", "optiontype", "option"])

                if not symbol_col:
                    trading_logger.log_warning(
                        "Unable to infer symbol column from IciciDirect slice",
                        {"file": fname, "columns": list(df.columns)},
                    )
                    continue

                part = pd.DataFrame()
                part["stock_code"] = df[symbol_col].astype(str).str.strip()

                # Assign exchange and segment based on file name
                if fname == "NSEScripMaster.txt":
                    part["Exch"] = "NSE"
                    part["ExchType"] = "CASH"
                elif fname == "BSEScripMaster.txt":
                    part["Exch"] = "BSE"
                    part["ExchType"] = "CASH"
                elif fname == "FONSEScripMaster.txt":
                    part["Exch"] = "NFO"
                    part["ExchType"] = "NFO"
                elif fname == "FOBSEScripMaster.txt":
                    part["Exch"] = "BFO"
                    part["ExchType"] = "BFO"
                else:
                    part["Exch"] = "UNKNOWN"
                    part["ExchType"] = "UNKNOWN"

                if lot_col and lot_col in df.columns:
                    part["LotSize"] = pd.to_numeric(df[lot_col], errors="coerce").fillna(1).astype(int)
                else:
                    part["LotSize"] = 1
                
                if fname in ["FONSEScripMaster.txt", "FOBSEScripMaster.txt"]:
                    part["ExchType"] = df[instrument_col].astype(str).str[:3]
                    
                    if expiry_col and expiry_col in df.columns:
                        part["ExpiryDate"] = df[expiry_col]
                    if strike_col and strike_col in df.columns:
                        part["StrikePrice"] = df[strike_col]
                    if option_col and option_col in df.columns:
                        part["OptionType"] = df[option_col]

                if tick_col and tick_col in df.columns:
                    part["TickSize"] = pd.to_numeric(df[tick_col], errors="coerce").fillna(0.05)
                    if fname in ["FONSEScripMaster.txt", "FOBSEScripMaster.txt"]:
                         part["TickSize"] = part["TickSize"] / 100.0
                else:
                    part["TickSize"] = 0.05

                def make_long_symbol(row):
                    sym = row["stock_code"]
                    sym_u = str(sym).upper().replace(" ", "")

                    # Cash: follow STK/IND pattern similar to other brokers
                    if row["ExchType"] == "CASH":
                        if any(idx in sym_u for idx in ["NIFTY", "BANKNIFTY", "SENSEX", "INDIA VIX", "INDIAVIX", "INDEX"]):
                            return f"{sym_u}_IND___"
                        else:
                            return f"{sym_u}_STK___"
                    # F&O: Construct SYMBOL_EXPIRY_STRIKE_TYPE
                    else:
                        try:
                            # Parse Expiry: 24-Dec-2025 -> 20251224
                            expiry_raw = str(row.get("ExpiryDate", ""))
                            if expiry_raw and expiry_raw != "nan":
                                try:
                                    dt_obj = dt.datetime.strptime(expiry_raw, "%d-%b-%Y")
                                    expiry_str = dt_obj.strftime("%Y%m%d")
                                except ValueError:
                                    expiry_str = expiry_raw
                            else:
                                expiry_str = ""

                            # Parse Strike: 31000.0 -> 31000
                            strike_raw = row.get("StrikePrice", 0)
                            try:
                                strike_val = float(strike_raw)
                                if strike_val.is_integer():
                                    strike_str = str(int(strike_val))
                                else:
                                    strike_str = str(strike_val)
                            except (ValueError, TypeError):
                                strike_str = str(strike_raw)

                            # Parse Option Type: PA->PUT, CA->CALL, XX->FUT
                            opt_raw = str(row.get("OptionType", "")).upper()
                            if opt_raw in ["PA", "PE"]:
                                opt_type = "PUT"
                            elif opt_raw in ["CA", "CE"]:
                                opt_type = "CALL"
                            elif opt_raw == "XX":
                                opt_type = "FUT"
                            else:
                                opt_type = opt_raw

                            if opt_type == "FUT":
                                return f"{sym_u}_FUT_{expiry_str}__"
                            else:
                                return f"{sym_u}_OPT_{expiry_str}_{opt_type}_{strike_str}"

                        except Exception:
                             return f"{sym_u}_DERIV___"
                
                part["long_symbol"] = part.apply(make_long_symbol, axis=1)
                part["Scripcode"] = df[scrip_code].astype(str).str.strip()

                parts.append(part)

        if not parts:
            raise DataError(
                "No recognizable IciciDirect symbol master slices found in ZIP",
                create_error_context(files=list(all_names)),
            )

        codes = pd.concat(parts, ignore_index=True)

        # Reorder columns to the common schema
        codes = codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "stock_code"]]

        if saveToFolder:
            dest_symbol_file = (
                f"{config.get('ICICIDIRECT.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
            )
            try:
                codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize"]].to_csv(
                    dest_symbol_file, index=False
                )
                trading_logger.log_info(
                    "IciciDirect symbols CSV written",
                    {"path": dest_symbol_file, "rows": len(codes)},
                )
            except Exception as e:
                trading_logger.log_error(
                    "Error writing IciciDirect symbols CSV", e, {"path": dest_symbol_file}
                )

        return codes

    except (ConfigurationError, DataError):
        raise
    except Exception as e:
        context = create_error_context(url=url, error=str(e))
        trading_logger.log_error("Error in IciciDirect save_symbol_data", e, {"url": url})
        raise DataError(f"Error fetching/parsing IciciDirect symbol data: {str(e)}", context)


def my_handler(typ, value, trace):
    """
    Unhandled exception hook for this module, consistent with other brokers.
    """
    context = create_error_context(
        exception_type=typ.__name__,
        exception_value=str(value),
        traceback="".join(traceback.format_tb(trace)),
    )
    trading_logger.log_error(f"Uncaught exception: {typ.__name__}", value, context)


sys.excepthook = my_handler


class IciciDirect(BrokerBase):
    """
    ICICIDirect broker implementation using BreezeConnect SDK.

    This is a first-cut implementation that wires IciciDirect into the common
    BrokerBase interface. Several advanced methods are left as TODOs so they
    can be implemented incrementally.
    """

    @log_execution_time
    def __init__(self, **kwargs):
        """
        Initialize IciciDirect broker.

        Expected configuration keys (in config.py):
            - ICICIDIRECT.API_KEY
            - ICICIDIRECT.API_SECRET
            - ICICIDIRECT.API_SESSION_TOKEN  (or equivalent, depending on your auth flow)
        """
        try:
            super().__init__(**kwargs)
            self.broker = Brokers.ICICIDIRECT if hasattr(Brokers, "ICICIDIRECT") else Brokers.UNDEFINED
            self.api: BreezeConnect | None = None
            self.codes = pd.DataFrame()
            self.starting_order_ids_int: Dict[str, int] = {}
            self.redis_o = redis.Redis(db=0, encoding="utf-8", decode_responses=True)

            trading_logger.log_info(
                "IciciDirect broker initialized",
                {"broker_type": "IciciDirect", "config_keys": list(kwargs.keys())},
            )
        except Exception as e:
            context = create_error_context(
                broker_type="IciciDirect",
                config_keys=list(kwargs.keys()),
                error=str(e),
            )
            raise BrokerConnectionError(f"Failed to initialize IciciDirect broker: {str(e)}", context)

    # ------------------------------------------------------------------
    # Connection / session management
    # ------------------------------------------------------------------

    @log_execution_time
    @retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
    def connect(self, redis_db: int):
        """
        Initialize BreezeConnect session and internal Redis state.
        """
        try:
            api_key = config.get("ICICIDIRECT.API_KEY")
            api_secret = config.get("ICICIDIRECT.API_SECRET")
            session_token = config.get("ICICIDIRECT.API_SESSION_TOKEN")

            if not api_key or not api_secret or not session_token:
                raise ConfigurationError(
                    "Missing ICICIDIRECT credentials in config",
                    create_error_context(
                        api_key_present=bool(api_key),
                        api_secret_present=bool(api_secret),
                        session_token_present=bool(session_token),
                    ),
                )

            self.api = BreezeConnect(api_key=api_key)
            self.api.generate_session(api_secret=api_secret, session_token=session_token)

            self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
            self.starting_order_ids_int = set_starting_internal_ids_int(self.redis_o)

            trading_logger.log_info(
                "IciciDirect connected",
                {"redis_db": redis_db},
            )
        except (AuthenticationError, ConfigurationError):
            raise
        except Exception as e:
            context = create_error_context(
                broker="IciciDirect",
                error=str(e),
            )
            raise BrokerConnectionError(f"Error connecting to IciciDirect: {str(e)}", context)

    def is_connected(self):
        """
        Lightweight connectivity check.
        """
        return self.api is not None

    def disconnect(self):
        """
        BreezeConnect is HTTP-based and generally stateless; we just drop the client.
        """
        try:
            self.api = None
            trading_logger.log_info("IciciDirect disconnected", {})
        except Exception as e:
            context = create_error_context(error=str(e))
            raise BrokerConnectionError(f"Failed to disconnect IciciDirect: {str(e)}", context)

    # ------------------------------------------------------------------
    # Symbology / exchange mapping
    # ------------------------------------------------------------------

    def update_symbology(self, **kwargs):
        """
        Download IciciDirect symbol master and build exchange_mappings.
        """
        try:
            save_to_folder = kwargs.get("saveToFolder", False)
            codes = save_symbol_data(saveToFolder=save_to_folder)
            self.codes = codes

            # Build exchange_mappings in the same structure used by other brokers
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
                        "IciciDirect exchange mappings created",
                        {"exchange": exchange, "symbol_count": len(group)},
                    )
                except Exception as e:
                    trading_logger.log_error(
                        "Error creating IciciDirect mappings for exchange",
                        e,
                        {"exchange": exchange, "group_shape": group.shape},
                    )
                    continue

            trading_logger.log_info(
                "IciciDirect symbology update completed",
                {"total_exchanges": len(self.exchange_mappings), "total_symbols": len(codes)},
            )

            return codes
        except (ConfigurationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise DataError(f"Unexpected error updating IciciDirect symbology: {str(e)}", context)

    def map_exchange_for_api(self, long_symbol, exchange) -> str:
        """
        Map internal exchange representation to Breeze exchange_code.

        For now this is a simple passthrough; extend as needed once symbology is in place.
        """
        return exchange

    def map_exchange_for_db(self, long_symbol, exchange) -> str:
        """
        Map exchange for database usage. Currently same as API mapping.
        """
        return self.map_exchange_for_api(long_symbol, exchange)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(long_symbol=lambda s: isinstance(s, str) and len(s.strip()) > 0)
    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """
        Get a quote from BreezeConnect.

        NOTE: This is a minimal mapping that assumes long_symbol can be used as stock_code.
        You should refine this once you have a proper symbol master for IciciDirect.
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
            if mapped_exchange not in self.exchange_mappings:
                raise SymbolError(
                    f"Exchange {mapped_exchange} not available for IciciDirect",
                    create_error_context(
                        mapped_exchange=mapped_exchange,
                        available_exchanges=list(self.exchange_mappings.keys()),
                    ),
                )

            stock_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(long_symbol)
            if not stock_code:
                raise SymbolError(
                    f"Symbol {long_symbol} not found for exchange {mapped_exchange} in IciciDirect mappings",
                    create_error_context(
                        long_symbol=long_symbol,
                        mapped_exchange=mapped_exchange,
                    ),
                )

            exchange_code = mapped_exchange

            resp = self.api.get_quotes(
                stock_code=stock_code,
                exchange_code=exchange_code,
                product_type="cash",
            )

            md = resp.get("Success", [])[0] if isinstance(resp.get("Success"), list) else resp.get("Success", {})

            bid = float(md.get("best_bid_price", "nan"))
            ask = float(md.get("best_ask_price", "nan"))
            last = float(md.get("ltp", "nan"))
            high = float(md.get("high", "nan"))
            low = float(md.get("low", "nan"))
            volume = int(md.get("volume", 0))
            timestamp = md.get("exchange_time", "")

            return Price(
                bid=bid,
                ask=ask,
                bid_volume=0,
                ask_volume=0,
                prior_close=float("nan"),
                last=last,
                high=high,
                low=low,
                volume=volume,
                symbol=long_symbol,
                exchange=exchange,
                src="ICICIDIRECT",
                timestamp=timestamp,
            )
        except Exception as e:
            context = create_error_context(
                long_symbol=long_symbol,
                exchange=exchange,
                error=str(e),
            )
            raise MarketDataError(f"Error getting quote from IciciDirect: {str(e)}", context)

    @log_execution_time
    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: Union[str, dt.datetime, dt.date],
        date_end: Union[str, dt.datetime, dt.date] = get_tradingapi_now().strftime("%Y-%m-%d"),
        exchange: str = "N",
        periodicity: str = "1m",
        market_close_time: str = "15:30:00",
    ) -> Dict[str, List[HistoricalData]]:
        """
        TODO: Implement using Breeze historical APIs.
        """
        raise NotImplementedError("IciciDirect.get_historical is not implemented yet")

    # ------------------------------------------------------------------
    # Orders / Positions
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(order=lambda o: isinstance(o, Order))
    def place_order(self, order: Order, **kwargs) -> Order:
        """
        Place an order via BreezeConnect.

        This is a basic mapping; you will likely need to refine product/action/order_type
        mapping once you integrate it with real IciciDirect usage.
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            mapped_exchange = self.map_exchange_for_api(order.long_symbol, order.exchange or "NSE")
            if mapped_exchange not in self.exchange_mappings:
                raise SymbolError(
                    f"Exchange {mapped_exchange} not available for IciciDirect",
                    create_error_context(
                        mapped_exchange=mapped_exchange,
                        available_exchanges=list(self.exchange_mappings.keys()),
                    ),
                )

            stock_code = self.exchange_mappings[mapped_exchange]["symbol_map"].get(order.long_symbol)
            if not stock_code:
                raise SymbolError(
                    f"Symbol {order.long_symbol} not found for exchange {mapped_exchange} in IciciDirect mappings",
                    create_error_context(
                        long_symbol=order.long_symbol,
                        mapped_exchange=mapped_exchange,
                    ),
                )

            exchange_code = mapped_exchange

            action = order.order_type.capitalize() if order.order_type else "Buy"
            order_type = "limit" if not math.isnan(order.price) else "market"
            quantity = str(order.quantity)

            resp = self.api.place_order(
                stock_code=stock_code,
                exchange_code=exchange_code,
                product="cash",
                action=action,
                order_type=order_type,
                quantity=quantity,
                price=str(order.price) if not math.isnan(order.price) else "0",
                validity="day",
                **kwargs,
            )

            success = resp.get("Success", {})
            broker_order_id = success.get("order_id") or success.get("order_no")

            order.broker_order_id = str(broker_order_id or "")
            order.status = OrderStatus.PENDING

            update_order_status(
                broker=self,
                order=order,
                broker_order_id=order.broker_order_id,
                additional_info=json.dumps({"raw_response": resp}),
            )

            return order
        except Exception as e:
            context = create_error_context(
                long_symbol=order.long_symbol,
                order_type=order.order_type,
                quantity=order.quantity,
                error=str(e),
            )
            raise OrderError(f"Error placing order via IciciDirect: {str(e)}", context)

    def modify_order(self, **kwargs) -> Order:
        """
        TODO: Implement mapping to Breeze modify order API.
        """
        raise NotImplementedError("IciciDirect.modify_order is not implemented yet")

    def cancel_order(self, **kwargs) -> Order:
        """
        TODO: Implement mapping to Breeze cancel order API.
        """
        raise NotImplementedError("IciciDirect.cancel_order is not implemented yet")

    def get_order_info(self, **kwargs) -> OrderInfo:
        """
        TODO: Implement using Breeze order status / order book.
        """
        raise NotImplementedError("IciciDirect.get_order_info is not implemented yet")

    def get_position(self, long_symbol: str) -> Union[pd.DataFrame, int]:
        """
        TODO: Implement using Breeze portfolio/positions API.
        """
        raise NotImplementedError("IciciDirect.get_position is not implemented yet")

    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        """
        TODO: Implement using Breeze order book API.
        """
        raise NotImplementedError("IciciDirect.get_orders_today is not implemented yet")

    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        """
        TODO: Implement using Breeze trade book API.
        """
        raise NotImplementedError("IciciDirect.get_trades_today is not implemented yet")

    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """
        TODO: Implement inverse mapping: broker identifier -> long_symbol.
        """
        raise NotImplementedError("IciciDirect.get_long_name_from_broker_identifier is not implemented yet")

    def get_min_lot_size(self, long_symbol: str, exchange: str) -> int:
        """
        TODO: Implement using IciciDirect symbol master (self.codes).
        """
        raise NotImplementedError("IciciDirect.get_min_lot_size is not implemented yet")

    def get_available_capital(self) -> float:
        """
        TODO: Implement using IciciDirect balance/margin API.
        """
        raise NotImplementedError("IciciDirect.get_available_capital is not implemented yet")


