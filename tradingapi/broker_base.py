import datetime as dt
import json
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Union
from .globals import get_tradingapi_now
import pandas as pd
import redis

logger = logging.getLogger(__name__)

from .exceptions import (
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

# Removed trading_logger import to avoid circular import issues

NEXT_DAY_TIMESTAMP = int((get_tradingapi_now() + dt.timedelta(days=1)).timestamp())


class Brokers(Enum):
    UNDEFINED = 1
    FIVEPAISA = 2
    SHOONYA = 3
    INTERACTIVEBROKERS = 4
    DHAN = 5
    ICICIDIRECT = 6
    FLATTRADE = 7


@dataclass
class HistoricalData:
    date: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    intoi: int
    oi: int

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None and not (isinstance(v, float) and math.isnan(v))}
        # return asdict(self)

    def __repr__(self):
        return f"HistoricalData({self.__dict__})"


class OrderStatus(Enum):
    UNDEFINED = 1  # No information on status
    HISTORICAL = 2  # Order is from earlier days, no broker status
    PENDING = 3  # Pending with  Broker
    REJECTED = 4  # Rejected by Broker
    OPEN = 5  # active with exchange
    FILLED = 6  # Filled with exchange
    CANCELLED = 7  # Cancelled by Exchange


def _validate_quantity(quantity):
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


def _validate_price(price):
    """
    Validate price parameter that can be int, float, string representation, or NaN.

    Args:
        price: Price value to validate

    Returns:
        bool: True if valid price, False otherwise
    """
    try:
        if isinstance(price, (int, float)):
            return True  # Allow any numeric value including NaN
        elif isinstance(price, str) and price.strip():
            # Try to convert string to float
            float(price.strip())
            return True
        else:
            return False
    except (ValueError, TypeError):
        return False


class Order:
    def __init__(
        self,
        long_symbol: str = "",
        order_type: str = "",
        price_type: float = 0.0,
        quantity: int = 0,
        exchange: str = "",
        exchange_segment: str = "",
        price: float = float("nan"),
        is_intraday: bool = True,
        internal_order_id: str = "",
        remote_order_id: str = "",
        scrip_code: int = 0,
        exch_order_id: str = "0",
        broker_order_id: str = "0",
        stoploss_price: float = 0.0,
        trigger_price: float = float("nan"),
        is_stoploss_order: bool = False,
        ioc_order: bool = False,
        scripdata: str = "",
        orderRef: str = "",
        order_id: int = 0,
        local_order_id: int = 0,
        disqty: int = 0,
        message: str = "",
        status: str = "UNDEFINED",
        vtd: str = f"/Date({NEXT_DAY_TIMESTAMP})/",
        ahplaced: str = "N",
        IsGTCOrder: bool = False,
        IsEOSOrder: bool = False,
        paper: bool = True,
        broker: str = "UNDEFINED",
        additional_info: str = "",
        **kwargs: Any,
    ):
        """
        Initialize an Order object with various attributes related to trading orders.

        Args:
            long_symbol: Trading symbol
            order_type: Type of order (BUY, SELL, SHORT, COVER)
            price_type: Price type for the order
            quantity: Order quantity
            exchange: Exchange name
            exchange_segment: Exchange segment
            price: Order price
            is_intraday: Whether order is intraday
            internal_order_id: Internal order ID
            remote_order_id: Remote order ID
            scrip_code: Scrip code
            exch_order_id: Exchange order ID
            broker_order_id: Broker order ID
            stoploss_price: Stop loss price
            trigger_price: Trigger price for stop/conditional orders
            is_stoploss_order: Whether it's a stop loss order
            ioc_order: Whether it's an IOC order
            scripdata: Scrip data
            orderRef: Order reference
            order_id: Order ID
            local_order_id: Local order ID
            disqty: Disclosed quantity
            message: Order message
            status: Order status
            vtd: VTD string
            ahplaced: After hours placement
            IsGTCOrder: Whether it's a GTC order
            IsEOSOrder: Whether it's an EOS order
            paper: Whether it's a paper trade
            broker: Broker name
            additional_info: Additional information

        Raises:
            ValidationError: If input parameters are invalid
        """
        self.long_symbol = long_symbol
        self.price_type = price_type
        self.order_type = order_type
        self.quantity = self._convert_to_int(quantity, "quantity")
        self.price = self._convert_to_float(price, "price")
        self.exchange = exchange
        self.exchange_segment = exchange_segment
        self.internal_order_id = internal_order_id
        self.remote_order_id = remote_order_id
        self.scrip_code = self._convert_to_int(scrip_code, "scrip_code")
        self.exch_order_id = exch_order_id
        self.broker_order_id = broker_order_id
        self.stoploss_price = self._convert_to_float(stoploss_price, "stoploss_price")
        self.is_stoploss_order = self._convert_to_bool(is_stoploss_order, "is_stoploss_order")
        self.trigger_price = self._convert_to_float(trigger_price, "trigger_price")
        self.ioc_order = self._convert_to_bool(ioc_order, "ioc_order")
        self.scripdata = scripdata
        self.orderRef = orderRef
        self.order_id = self._convert_to_int(order_id, "order_id")
        self.local_order_id = self._convert_to_int(local_order_id, "local_order_id")
        self.disqty = 0 if disqty == "None" else self._convert_to_int(disqty, "disqty")
        self.message = message
        self.vtd = vtd
        self.ahplaced = ahplaced
        self.IsGTCOrder = self._convert_to_bool(IsGTCOrder, "IsGTCOrder")
        self.IsEOSOrder = self._convert_to_bool(IsEOSOrder, "IsEOSOrder")
        self.paper = self._convert_to_bool(paper, "paper")
        self.is_intraday = self._convert_to_bool(is_intraday, "is_intraday")
        self.additional_info = additional_info

        # Setting status using enum
        self.status = self._set_status(status)

        # Setting broker using enum
        self.broker = self._set_broker(broker)

        # Handling any additional keyword arguments
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _set_status(self, status: str) -> OrderStatus:
        """
        Convert a string status to an OrderStatus enum. Default to UNDEFINED if invalid.
        """
        try:
            return OrderStatus[status.upper()]
        except KeyError:
            return OrderStatus.UNDEFINED

    def _set_broker(self, broker: str) -> Brokers:
        """
        Convert a string broker to a Brokers enum. Default to UNDEFINED if invalid.
        """
        try:
            return Brokers[broker.upper()]
        except KeyError:
            return Brokers.UNDEFINED

    def _convert_to_int(self, value: Any, argument_name: str = "unknown") -> int:
        """
        Convert a value to an integer if possible, otherwise return 0.
        Handles various types including strings, floats, and objects that can be converted to numbers.
        """
        try:
            # Handle None values
            if value is None:
                logger.warning(
                    "Received None value, returning 0",
                    extra={"value": value, "default": 0, "method": "_convert_to_int", "argument_name": argument_name},
                )
                return 0

            # Handle integers directly
            if isinstance(value, int):
                return value

            # Handle floats
            elif isinstance(value, float):
                return int(value)

            # Handle strings (including object representations like '260.0')
            elif isinstance(value, str):
                # Strip whitespace and handle empty strings
                value = value.strip()
                if not value:
                    logger.warning(
                        "Empty string value, returning 0",
                        extra={
                            "value": repr(value),
                            "default": 0,
                            "method": "_convert_to_int",
                            "argument_name": argument_name,
                        },
                    )
                    return 0

                try:
                    # Try to convert string to float first, then to int
                    return int(float(value))
                except ValueError:
                    logger.warning(
                        "Failed to convert string to int",
                        extra={
                            "value": repr(value),
                            "default": 0,
                            "method": "_convert_to_int",
                            "argument_name": argument_name,
                        },
                    )
                    return 0

            # Handle other objects by converting to string first
            else:
                try:
                    # Convert object to string and then to number
                    str_value = str(value).strip()
                    if not str_value:
                        logger.warning(
                            "Object converted to empty string, returning 0",
                            extra={
                                "value": repr(value),
                                "value_type": type(value).__name__,
                                "default": 0,
                                "method": "_convert_to_int",
                                "argument_name": argument_name,
                            },
                        )
                        return 0

                    return int(float(str_value))
                except (ValueError, TypeError):
                    logger.warning(
                        "Failed to convert object to int",
                        extra={
                            "value": repr(value),
                            "value_type": type(value).__name__,
                            "default": 0,
                            "method": "_convert_to_int",
                            "argument_name": argument_name,
                        },
                    )
                    return 0

        except Exception as e:
            logger.error(
                "Error converting value to int",
                exc_info=True,
                extra={
                    "value": repr(value),
                    "value_type": type(value).__name__,
                    "method": "_convert_to_int",
                    "argument_name": argument_name,
                },
            )
            return 0

    def _convert_to_float(self, value: Any, argument_name: str = "unknown") -> float:
        """
        Convert a value to a float if possible, otherwise return NaN.
        Handles various types including strings, integers, and objects that can be converted to numbers.
        """
        try:
            # Handle None values
            if value is None:
                logger.warning(
                    "Received None value, returning NaN",
                    extra={
                        "value": value,
                        "default": float("nan"),
                        "method": "_convert_to_float",
                        "argument_name": argument_name,
                    },
                )
                return float("nan")

            # Handle numbers directly
            if isinstance(value, (int, float)):
                return float(value)

            # Handle strings
            elif isinstance(value, str):
                # Strip whitespace and handle empty strings
                value = value.strip()
                if not value:
                    logger.warning(
                        "Empty string value, returning NaN",
                        extra={
                            "value": repr(value),
                            "default": float("nan"),
                            "method": "_convert_to_float",
                            "argument_name": argument_name,
                        },
                    )
                    return float("nan")

                try:
                    return float(value)
                except ValueError:
                    logger.warning(
                        "Failed to convert string to float",
                        extra={
                            "value": repr(value),
                            "default": float("nan"),
                            "method": "_convert_to_float",
                            "argument_name": argument_name,
                        },
                    )
                    return float("nan")

            # Handle other objects by converting to string first
            else:
                try:
                    # Convert object to string and then to number
                    str_value = str(value).strip()
                    if not str_value:
                        logger.warning(
                            "Object converted to empty string, returning NaN",
                            extra={
                                "value": repr(value),
                                "value_type": type(value).__name__,
                                "default": float("nan"),
                                "method": "_convert_to_float",
                                "argument_name": argument_name,
                            },
                        )
                        return float("nan")

                    return float(str_value)
                except (ValueError, TypeError):
                    logger.warning(
                        "Failed to convert object to float",
                        extra={
                            "value": repr(value),
                            "value_type": type(value).__name__,
                            "default": float("nan"),
                            "method": "_convert_to_float",
                            "argument_name": argument_name,
                        },
                    )
                    return float("nan")

        except Exception as e:
            logger.error(
                "Error converting value to float",
                exc_info=True,
                extra={
                    "value": repr(value),
                    "value_type": type(value).__name__,
                    "method": "_convert_to_float",
                    "argument_name": argument_name,
                },
            )
            return float("nan")

    def _convert_to_bool(self, value: Any, argument_name: str = "unknown") -> bool:
        """
        Convert a value to a boolean if possible.
        """
        try:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "y")
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return False
        except Exception as e:
            logger.error(
                "Error converting value to bool",
                exc_info=True,
                extra={
                    "value": value,
                    "value_type": type(value).__name__,
                    "method": "_convert_to_bool",
                    "argument_name": argument_name,
                },
            )
            return False

    def to_dict(self):
        result = self.__dict__.copy()
        # Convert enum to its name (string)
        result["status"] = self.status.name
        result["broker"] = self.broker.name
        return result

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4, default=str)


class Price:
    def __init__(
        self,
        bid: float = float("nan"),
        ask: float = float("nan"),
        bid_volume: int = 0,
        ask_volume: int = 0,
        prior_close: float = float("nan"),
        last: float = float("nan"),
        high: float = float("nan"),
        low: float = float("nan"),
        volume: int = 0,
        symbol: str = "",
        exchange: str = "",
        src: str = "",
        timestamp: str = "",
    ):
        """
        Initialize a Price object with market data.

        Args:
            bid: Bid price
            ask: Ask price
            bid_volume: Bid volume
            ask_volume: Ask volume
            prior_close: Prior close price
            last: Last traded price
            high: High price
            low: Low price
            volume: Total volume
            symbol: Trading symbol
            exchange: Exchange name
            src: Source of the price data
            timestamp: Timestamp of the price data

        Raises:
            ValidationError: If input parameters are invalid
        """
        self.bid = bid
        self.ask = ask
        self.bid_volume = bid_volume
        self.ask_volume = ask_volume
        self.prior_close = prior_close
        self.last = last
        self.high = high
        self.low = low
        self.volume = volume
        self.symbol = symbol
        self.exchange = exchange
        self.src = src
        self.timestamp = timestamp

    def __add__(self, other):
        def safe_add(a, b):
            if math.isnan(a) or math.isnan(b):
                return float("nan")
            return a + b

        return Price(
            bid=safe_add(self.bid, other.bid),
            ask=safe_add(self.ask, other.ask),
            bid_volume=safe_add(self.bid_volume, other.bid_volume),
            ask_volume=safe_add(self.ask_volume, other.ask_volume),
            prior_close=safe_add(self.prior_close, other.prior_close),
            last=safe_add(self.last, other.last),
            high=safe_add(self.high, other.high),
            low=safe_add(self.low, other.low),
            volume=safe_add(self.volume, other.volume),
        )
        # dont change symbol

    def update(self, other, size=1):
        self.bid = other.bid * size if other.bid * size is not float("nan") else self.bid
        self.ask = other.ask * size if other.ask * size is not float("nan") else self.ask
        self.bid_volume = other.bid_volume if other.bid_volume is not float("nan") else self.bid_volume
        self.ask_volume = other.ask_volume if other.ask_volume is not float("nan") else self.ask_volume
        self.prior_close = other.prior_close * size if other.prior_close is not float("nan") else self.prior_close
        self.last = other.last * size if other.last * size is not float("nan") else self.last
        self.high = other.high * size if other.high * size is not float("nan") else self.high
        self.low = other.low * size if other.low * size is not float("nan") else self.low
        self.volume = other.volume if other.volume is not float("nan") else self.volume
        self.symbol = other.symbol
        self.exchange = other.exchange
        self.src = other.src
        self.timestamp = other.timestamp

    def to_dict(self):
        return {
            "bid": self.bid,
            "ask": self.ask,
            "bid_volume": self.bid_volume,
            "ask_volume": self.ask_volume,
            "prior_close": self.prior_close,
            "last": self.last,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "src": self.src,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            bid=data.get("bid", float("nan")),
            ask=data.get("ask", float("nan")),
            bid_volume=data.get("bid_volume", 0),
            ask_volume=data.get("ask_volume", 0),
            prior_close=data.get("prior_close", float("nan")),
            last=data.get("last", float("nan")),
            high=data.get("high", float("nan")),
            low=data.get("low", float("nan")),
            volume=data.get("volume", 0),
            symbol=data.get("symbol", ""),
            exchange=data.get("exchange", ""),
            src=data.get("src", ""),
            timestamp=data.get("timestamp", ""),
        )

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4, default=str)


@dataclass
class Position:
    symbol: str = ""
    size: int = 0
    price: float = 0
    value: float = 0

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None and not (isinstance(v, float) and math.isnan(v))}
        # return asdict(self)

    def __repr__(self):
        return f"Position({self.__dict__})"


class OrderInfo:
    def __init__(
        self,
        order_size: int = 0,
        order_price: float = float("nan"),
        fill_size: int = 0,
        fill_price: float = 0,
        status: OrderStatus = OrderStatus.UNDEFINED,
        broker_order_id: str = "",
        exchange_order_id: str = "",
        broker=Brokers.UNDEFINED,
    ):
        self.order_size = order_size
        self.order_price = order_price
        self.fill_size = fill_size
        self.fill_price = fill_price
        self.status = status
        self.broker_order_id = broker_order_id
        self.exchange_order_id = exchange_order_id
        self.broker = broker

    def to_dict(self):
        return {
            "order_size": str(self.order_size),
            "order_price": str(self.order_price),
            "fill_size": str(self.fill_size),
            "fill_price": str(self.fill_price),
            "status": self.status.name if isinstance(self.status, Enum) else self.status,
            "broker_order_id": self.broker_order_id,
            "exchange_order_id": self.exchange_order_id,
            "broker": self.broker.name if isinstance(self.broker, Enum) else self.broker,
        }

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4, default=str)


class BrokerBase(ABC):
    """
    Abstract base class for broker implementations with enhanced error handling.

    This class defines the interface that all broker implementations must follow.
    It includes common error handling patterns and logging capabilities.
    """

    def __init__(self, **kwargs):
        """
        Initialize the broker with configuration parameters.

        Args:
            **kwargs: Configuration parameters for the broker

        Raises:
            ValidationError: If configuration parameters are invalid
            BrokerConnectionError: If broker initialization fails
        """
        try:
            self._validate_config(kwargs)
            self._initialize_broker(kwargs)
            logger.info(
                "Broker initialized successfully",
                extra={"broker_type": self.__class__.__name__, "config_keys": list(kwargs.keys())},
            )
        except Exception as e:
            context = create_error_context(
                broker_type=self.__class__.__name__, config_keys=list(kwargs.keys()), error=str(e)
            )
            raise BrokerConnectionError(f"Failed to initialize broker: {str(e)}", context)

    def _validate_config(self, config: dict):
        """Validate broker configuration parameters."""
        if not isinstance(config, dict):
            raise ValidationError("Configuration must be a dictionary")

        # Add specific validation logic for each broker type
        logger.debug(
            "Validating broker configuration",
            extra={"broker_type": self.__class__.__name__, "config_keys": list(config.keys())},
        )

    def _initialize_broker(self, config: dict):
        """Initialize broker-specific components."""
        self.broker = Brokers.UNDEFINED
        self.starting_order_ids_int = {}
        self.redis_o = redis.Redis(db=0, encoding="utf-8", decode_responses=True)
        self.exchange_mappings = {
            "symbol_map": {},
            "contractsize_map": {},
            "exchange_map": {},
            "exchangetype_map": {},
            "contracttick_map": {},
            "symbol_map_reversed": {},
        }

    @abstractmethod
    def update_symbology(self, **kwargs):
        """
        Update the symbology mapping for the broker.

        Raises:
            ValidationError: If symbology parameters are invalid
            BrokerConnectionError: If symbology update fails
        """
        pass

    @abstractmethod
    def connect(self, redis_db: int):
        """
        Connect to the broker's trading platform.

        Args:
            redis_db: Redis database number

        Raises:
            ValidationError: If redis_db is invalid
            BrokerConnectionError: If connection fails
            AuthenticationError: If authentication fails
        """
        pass

    @abstractmethod
    def is_connected(self):
        """
        Check if the broker is connected.

        Returns:
            bool: True if connected, False otherwise

        Raises:
            BrokerConnectionError: If connection check fails
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Disconnect from the broker's trading platform.

        Raises:
            BrokerConnectionError: If disconnection fails
        """
        pass

    @abstractmethod
    def place_order(self, order: Order, **kwargs) -> Order:
        """
        Place an order with the broker.

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
        pass

    @abstractmethod
    def modify_order(self, **kwargs) -> Order:
        """
        Modify an existing order.

        Args:
            **kwargs: Order modification parameters

        Returns:
            Order: Updated order object

        Raises:
            ValidationError: If modification parameters are invalid
            OrderError: If order modification fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def cancel_order(self, **kwargs) -> Order:
        """
        Cancel an existing order.

        Args:
            **kwargs: Order cancellation parameters

        Returns:
            Order: Updated order object

        Raises:
            ValidationError: If cancellation parameters are invalid
            OrderError: If order cancellation fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def get_order_info(self, **kwargs) -> OrderInfo:
        """
        Get information about an order.

        Args:
            **kwargs: Order information parameters

        Returns:
            OrderInfo: Order information object

        Raises:
            ValidationError: If parameters are invalid
            OrderError: If order information retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
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
        Get historical data for symbols.

        Args:
            symbols: Symbol(s) to get historical data for
            date_start: Start date for historical data (can be string, datetime, or date object)
            date_end: End date for historical data (can be string, datetime, or date object)
            exchange: Exchange name
            periodicity: Data periodicity
            market_close_time: Market close time

        Returns:
            Dict[str, List[HistoricalData]]: Historical data for each symbol

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If historical data retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def map_exchange_for_api(self, long_symbol, exchange) -> str:
        """
        Map exchange for API calls.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            str: Mapped exchange name

        Raises:
            ValidationError: If parameters are invalid
            SymbolError: If symbol mapping fails
        """
        pass

    @abstractmethod
    def map_exchange_for_db(self, long_symbol, exchange) -> str:
        """
        Map exchange for database operations.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            str: Mapped exchange name

        Raises:
            ValidationError: If parameters are invalid
            SymbolError: If symbol mapping fails
        """
        pass

    @abstractmethod
    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """
        Get quote for a symbol.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Price: Price object with market data

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If quote retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def get_position(self, long_symbol: str) -> Union[pd.DataFrame, int]:
        """
        Get position for a symbol.

        Args:
            long_symbol: Trading symbol

        Returns:
            Union[pd.DataFrame, int]: Position data

        Raises:
            ValidationError: If parameters are invalid
            MarketDataError: If position retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        """
        Get orders for today.

        Args:
            **kwargs: Additional parameters

        Returns:
            pd.DataFrame: Orders data

        Raises:
            OrderError: If order retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        """
        Get trades for today.

        Args:
            **kwargs: Additional parameters

        Returns:
            pd.DataFrame: Trades data

        Raises:
            MarketDataError: If trade retrieval fails
            BrokerConnectionError: If broker connection issues
        """
        pass

    @abstractmethod
    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        """
        Get long name from broker identifier.

        Args:
            **kwargs: Additional parameters

        Returns:
            pd.Series: Long names

        Raises:
            ValidationError: If parameters are invalid
            SymbolError: If symbol mapping fails
        """
        pass

    @abstractmethod
    def get_min_lot_size(self, long_symbol: str, exchange: str) -> int:
        """
        Get minimum lot size for a symbol.

        Args:
            long_symbol: Trading symbol
            exchange: Exchange name

        Returns:
            int: Minimum lot size

        Raises:
            ValidationError: If parameters are invalid
            SymbolError: If symbol lookup fails
        """
        pass

    @abstractmethod
    def get_available_capital(self) -> float:
        """
        Get available capital/balance for trading.

        Returns:
            float: Available capital amount

        Raises:
            BrokerConnectionError: If broker is not connected
            MarketDataError: If balance retrieval fails
        """
        pass
