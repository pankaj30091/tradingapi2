# tradingapi

Python package for broker integration with unified interfaces for multiple Indian brokers. Provides comprehensive trading functionality including order placement, historical data access, symbology handling, margin calculations, and commission management.

## Features

- **Unified Broker Interfaces**: Consistent API across multiple brokers (FivePaisa, Shoonya, FlatTrade, ICICIDirect)
- **Order Management**: Place, modify, cancel orders with support for combo orders, trigger prices, and stop-loss
- **Historical Data**: Fetch historical market data with flexible date ranges and periodicity
- **Symbology Management**: Automatic symbol code mapping and parsing for futures/options
- **Commission Calculation**: Date-based commission structure with automatic selection
- **Structured Logging**: Comprehensive logging with context and runtime log level toggling
- **Error Handling**: Robust error handling with retries and validation
- **Redis Integration**: Order tracking and logging via Redis

## API Reference

### Data Structures (`broker_base.py`)

#### Enums
- **`Brokers`**: Enum for broker types
  - `FIVEPAISA`, `SHOONYA`, `FLATTRADE`, `ICICIDIRECT`, `DHAN`, `INTERACTIVEBROKERS`, `UNDEFINED`
- **`OrderStatus`**: Enum for order status
  - `UNDEFINED`, `HISTORICAL`, `PENDING`, `REJECTED`, `OPEN`, `FILLED`, `CANCELLED`

#### Classes
- **`Order`**: Order object with attributes:
  - `long_symbol`, `order_type`, `quantity`, `price`, `exchange`, `is_intraday`
  - `internal_order_id`, `remote_order_id`, `broker_order_id`
  - `trigger_price`, `is_stoploss_order`, `paper`, `status`, `additional_info`
- **`HistoricalData`**: Historical market data dataclass
  - `date`, `open`, `high`, `low`, `close`, `volume`, `intoi`, `oi`
- **`Price`**: Current price information
  - `bid`, `ask`, `last`, `volume`, `timestamp`
- **`Position`**: Position information dataclass
  - `symbol`, `quantity`, `avg_price`, `pnl`
- **`OrderInfo`**: Order information dataclass
  - `order_id`, `status`, `filled_quantity`, `pending_quantity`, `price`, `message`

### Broker Classes

#### Abstract Base Class
- **`BrokerBase`**: Abstract base class for all broker implementations

**Abstract Methods (must be implemented by all brokers):**
- `connect(redis_db: int)`: Connect to broker platform
- `disconnect()`: Disconnect from broker platform
- `is_connected()`: Check connection status
- `update_symbology(**kwargs)`: Update symbol mappings
- `place_order(order: Order, **kwargs) -> Order`: Place an order
- `modify_order(**kwargs) -> Order`: Modify existing order
- `cancel_order(**kwargs) -> Order`: Cancel existing order
- `get_order_info(**kwargs) -> OrderInfo`: Get order information
- `get_historical(...) -> Dict[str, List[HistoricalData]]`: Fetch historical data

#### Broker Implementations
- **`FivePaisa`**: FivePaisa broker implementation
- **`Shoonya`**: Shoonya broker implementation
- **`FlatTrade`**: FlatTrade broker implementation
- **`IciciDirect`**: ICICIDirect broker implementation

### Utility Functions (`utils.py`)

#### Order Management
- `place_combo_order(execution_broker, strategy, symbols, quantities, entry, ...)`: Place multi-leg combo orders
- `transmit_entry_order(broker, strategy, order, paper, price_broker)`: Transmit entry order
- `transmit_exit_order(broker, strategy, order, validate_db_position, paper, price_broker, int_order_id)`: Transmit exit order
- `update_order_status(broker, order)`: Update order status from broker
- `delete_broker_order_id(broker, internal_order_id)`: Delete broker order ID mapping

#### Price & Market Data
- `get_price(broker, symbol, exchange, mds)`: Get current price for symbol
- `get_mid_price(brokers, long_symbol, exchange, mds, last)`: Get mid price from multiple brokers
- `get_limit_price(broker, price_type, order_type, symbol, price_broker, exchange, mds)`: Calculate limit price
- `get_option_underlying_price(broker, option_symbol, exchange)`: Get underlying price for option
- `get_impact_cost(brokers, symbol, exchange, mds)`: Calculate impact cost

#### Options & Greeks
- `calculate_delta(broker, option_symbol, exchange)`: Calculate option delta
- `find_option_with_delta(broker, underlying_symbol, expiry, target_delta, option_type, exchange)`: Find option by delta
- `get_delta_strike(broker, underlying_symbol, expiry, target_delta, option_type, exchange)`: Get strike price for target delta
- `get_linked_options(broker, symbol_name, expiry, exchange)`: Get linked options for symbol
- `get_linked_futures(symbol_name, expiry, file_path)`: Get linked futures for symbol

#### Position & P&L
- `get_pnl_table(broker, strategy, long_symbol, broker_entry_side, date_start, date_end)`: Get P&L table
- `get_open_position_by_order(broker, strategy, long_symbol, broker_entry_side)`: Get open position by order
- `get_exit_candidates(broker, strategy, long_symbol, side)`: Get exit candidates
- `calculate_mtm(brokers, pnl, mds)`: Calculate mark-to-market P&L
- `calc_pnl(trades, broker)`: Calculate P&L from trades DataFrame

#### Symbol & Universe Management
- `get_universe(file_path, product_types, exchanges, expiry)`: Get trading universe
- `get_unique_short_symbol_names(exchange, sec_type, file_path)`: Get unique short symbol names
- `get_latest_symbol_file(directory_path)`: Get latest symbol file
- `parse_combo_symbol(combo_symbol)`: Parse combo symbol string

#### Margin & Risk
- `get_margin_zerodha(broker, long_symbol, proxy, exchange)`: Get margin requirement (Zerodha)
- `get_margin_samco(long_symbol, driver_path, proxy)`: Get margin requirement (Samco)
- `get_margin_5p(long_symbol, strike_price, type, expiry, driver_path, proxy)`: Get margin requirement (5Paisa)
- `get_yield(broker, long_symbol, proxy, exchange)`: Get yield information

#### Data Conversion & Utilities
- `historical_to_dataframes(historical_data)`: Convert historical data to DataFrames
- `get_orders_by_symbol(broker, strategy, long_symbol, broker_entry_side)`: Get orders by symbol
- `review_price_history(symbol, exchange, count)`: Review price history
- `get_combo_sub_order_type(order, sub_order_qty)`: Get combo sub-order type

#### Configuration & Setup
- `get_all_strategy_names(redis_db)`: Get all strategy names from Redis
- `set_starting_internal_ids_int(redis_db)`: Set starting internal order IDs

### Configuration Functions (`config.py`)

- `get_config() -> Config`: Get the loaded configuration instance
- `load_config(default_config_path)`: Load configuration from file
- `is_config_loaded() -> bool`: Check if configuration is loaded

### Logging Functions (`__init__.py`)

- `configure_logging(module_names, level, log_file, clear_existing_handlers, enable_console, backup_count, format_string, enable_structured_logging, configure_root_logger)`: Configure logging
- `initialize_config(config_file_path, force_reload)`: Initialize configuration
- `get_default_config_path()`: Get default config file path
- `enable_runtime_log_level_toggle(enable)`: Enable/disable runtime log level toggle
- `trading_logger`: Global logger instance with methods:
  - `log_info(message, context)`
  - `log_error(message, error, context, exc_info)`
  - `log_warning(message, context)`
  - `log_debug(message, context)`

## Installation

### From Source

```bash
git clone https://bitbucket.org/incurrency/tradingapi2.git
cd tradingapi2
pip install -e .
```

### From pypi
```bash
pip install tradingapi
```

### Dependencies

The package requires:
- Python 3.7+
- Redis (for order tracking and logging)
- Broker-specific SDKs (automatically installed via dependencies)

## Configuration

### Configuration File Structure

The package uses YAML configuration files. A sample configuration file is provided at `tradingapi/config/config_sample.yaml`.

#### Default Configuration Path

By default, the package loads configuration from:
```
tradingapi/config/config.yaml
```

#### Custom Configuration Path

You can override the default configuration path using the `TRADINGAPI_CONFIG_PATH` environment variable:

```bash
export TRADINGAPI_CONFIG_PATH=/path/to/your/config.yaml
```

### Configuration File Format

Create a `config.yaml` file with the following structure:

```yaml
# Bhavcopy folder - path where symbol files from brokers are saved
bhavcopy_folder: /path/to/bhavcopy/folder

# Commission files configuration
commissions:
  - effective_date: "2020-01-01"
    file: "commissions_20200101.yaml"
  - effective_date: "2024-10-01"
    file: "commissions_20241001.yaml"
  - effective_date: "2024-12-02"
    file: "commissions_20241216.yaml"

# Required general settings
tz: "Asia/Kolkata"
datapath: "/path/to/data"
market_open_time: "09:15:00"

# FivePaisa Broker Configuration
FIVEPAISA:
  APP_NAME: "your_app_name"
  APP_SOURCE: "your_app_source"
  USER_ID: "your_user_id"
  PASSWORD: "your_password"
  USER_KEY: "your_user_key"
  ENCRYPTION_KEY: "your_encryption_key"
  TOTP_TOKEN: "your_totp_token"
  PIN: "your_pin"
  CLIENT_ID: "your_client_id"
  SYMBOLCODES: "/path/to/fivepaisa/symbols"
  USERTOKEN: ""

# Shoonya Broker Configuration
SHOONYA:
  USER: "your_username"
  PWD: "your_password"
  VC: "your_vc"
  APPKEY: "your_app_key"
  TOKEN: "your_token"
  SYMBOLCODES: "/path/to/shoonya/symbols"
  USERTOKEN: ""

# FlatTrade Broker Configuration
FLATTRADE:
  USER: "your_username"
  PWD: "your_password"
  VC: "your_vc"
  APPKEY: "your_app_key"
  TOKEN: "your_token"
  SYMBOLCODES: "/path/to/flattrade/symbols"
  USERTOKEN: ""

# ICICIDirect Broker Configuration
ICICIDIRECT:
  API_KEY: "your_api_key"
  API_SECRET: "your_api_secret"
  API_SESSION_TOKEN: "your_session_token"
  SYMBOLCODES: "/path/to/icicidirect/symbols"
```

### Commission Files

Commission files define broker-specific commission structures. They are YAML files located in the same directory as your main config file. Example structure:

```yaml
SHOONYA:
  GST: 18
  STK:
    BUY:
      flat: 0
      percentage:
        commission: 0
        stt: 0.10
        exchange: 0.00345
        sebi: 0.0001
        stampduty: 0.015
    SHORT:
      flat: 0
      percentage:
        commission: 0
        stt: 0.10
        exchange: 0.00345
        sebi: 0.0001
        stampduty: 0
  FUT:
    BUY:
      flat: 5
      percentage:
        commission: 0
        stt: 0
        exchange: 0.002
        sebi: 0.0001
        stampduty: 0.002
```

The package automatically selects the appropriate commission file based on the effective date.

## File Paths

### Important Paths

- **Configuration Directory**: `tradingapi/config/`
  - Main config: `config.yaml` (or custom path via `TRADINGAPI_CONFIG_PATH`)
  - Commission files: `commissions_*.yaml`
  - Sample config: `config_sample.yaml`

- **Symbol Files**: Paths specified in broker configuration (`SYMBOLCODES`)
  - Format: `{YYYYMMDD}_symbols.csv`
  - Example: `20241216_symbols.csv`

- **Bhavcopy Folder**: Path specified in config (`bhavcopy_folder`)
  - Used for storing symbol files from brokers

### Configuration Access

Access configuration values programmatically:

```python
from tradingapi.config import get_config

config = get_config()

# Get a configuration value using dot notation
app_name = config.get("FIVEPAISA.APP_NAME")
symbol_path = config.get("FIVEPAISA.SYMBOLCODES")

# Get commission data for a specific date
commission_data = config.get_commission_for_date("2024-12-16")
```

## Usage

### Basic Setup

```python
from tradingapi.fivepaisa import FivePaisa

# Configuration is automatically loaded on import
# Credentials are read from config during connect()

# Initialize broker (no parameters needed)
broker = FivePaisa()

# Connect to broker (requires Redis)
# Credentials are automatically read from config.yaml
broker.connect(redis_db=0)
```

### Broker Initialization

#### FivePaisa

```python
from tradingapi.fivepaisa import FivePaisa

# Credentials are automatically read from config during connect()
broker = FivePaisa()
broker.connect(redis_db=0)
```

#### Shoonya

```python
from tradingapi.shoonya import Shoonya

# Credentials are automatically read from config during connect()
broker = Shoonya()
broker.connect(redis_db=0)
```

#### FlatTrade

```python
from tradingapi.flattrade import FlatTrade

# Credentials are automatically read from config during connect()
broker = FlatTrade()
broker.connect(redis_db=0)
```

#### ICICIDirect

```python
from tradingapi.icicidirect import IciciDirect

# Credentials are automatically read from config during connect()
broker = IciciDirect()
broker.connect(redis_db=0)
```

### Placing Orders

```python
from tradingapi.broker_base import Order, OrderStatus

# Create an order
order = Order(
    long_symbol="RELIANCE",
    exchange="NSE",
    quantity=10,
    price=2500.0,
    order_type="BUY",  # Options: "BUY", "SELL", "SHORT", "COVER"
    is_intraday=True,  # True for intraday, False for delivery
)

# Place the order
result_order = broker.place_order(order)

# Check order status
if result_order.status == OrderStatus.FILLED:
    print(f"Order placed successfully: {result_order.remote_order_id}")
else:
    print(f"Order status: {result_order.status}, Message: {result_order.message}")

# Order status values:
# OrderStatus.PENDING - Order pending with broker
# OrderStatus.OPEN - Order active with exchange
# OrderStatus.FILLED - Order filled/completed
# OrderStatus.REJECTED - Order rejected by broker
# OrderStatus.CANCELLED - Order cancelled
# OrderStatus.HISTORICAL - Order from earlier days (no broker status)
# OrderStatus.UNDEFINED - No status information
```

### Placing Combo Orders

Combo orders allow you to place multiple orders simultaneously (e.g., spreads, straddles, or multi-leg strategies):

```python
from tradingapi.utils import place_combo_order

# Example: Place a spread order (buy one symbol, sell another)
order_ids = place_combo_order(
    execution_broker=broker,
    strategy="spread_strategy",
    symbols=["RELIANCE", "TCS"],  # List of symbols
    quantities=[10, -10],  # Positive = BUY/SHORT, Negative = SELL/COVER
    entry=True,  # True for entry, False for exit
    exchanges=["NSE", "NSE"],  # Exchange for each symbol
    price_types=[],  # Empty list = limit orders at mid-price
    trigger_prices=None,  # Optional: trigger prices for conditional orders
    paper=False,  # Set to True for paper trading
    validate_db_position=True,  # Validate position in DB for exit orders
)

# Returns a dict mapping symbol to internal order ID (for entry orders)
# Example: {"RELIANCE": "12345", "TCS": "12346"}
```

**Key Parameters:**
- `symbols`: List of symbols to trade (e.g., `["RELIANCE", "TCS"]`)
- `quantities`: List of quantities for each symbol. Positive values mean BUY (entry) or COVER (exit). Negative values mean SHORT (entry) or SELL (exit)
- `entry`: `True` for entry orders, `False` for exit orders
- `exchanges`: List of exchanges (defaults to `["NSE"]` if not provided)
- `price_types`: List of price types. Empty list defaults to limit orders at mid-price. Can specify price types per symbol
- `trigger_prices`: Optional trigger price(s). Can be a single float (applied to all legs) or a list of floats (one per leg)
- `paper`: `True` for simulated orders, `False` for real orders
- `validate_db_position`: For exit orders, whether to validate position exists in database

**Example: Options Spread**
```python
# Buy call option, sell put option (straddle)
order_ids = place_combo_order(
    execution_broker=broker,
    strategy="straddle_strategy",
    symbols=["NIFTY_OPT_20241226_19000_CE", "NIFTY_OPT_20241226_19000_PE"],
    quantities=[50, -50],  # Buy call, sell put
    entry=True,
    exchanges=["NFO", "NFO"],
    price_types=[],  # Limit orders at mid-price
    paper=False
)
```

**Example: Exit Combo Order**
```python
# Exit a multi-leg position
place_combo_order(
    execution_broker=broker,
    strategy="spread_strategy",
    symbols=["RELIANCE", "TCS"],
    quantities=[-10, 10],  # Reverse the entry positions
    entry=False,  # Exit order
    exchanges=["NSE", "NSE"],
    validate_db_position=True,  # Check position exists before exiting
    paper=False
)
# Returns empty dict for exit orders
```

### Using Utility Functions

```python
from tradingapi.utils import transmit_entry_order, get_limit_price

# Place entry order with utility function
order_id = transmit_entry_order(
    broker=broker,
    strategy="my_strategy",
    order=order,
    paper=False  # Set to True for paper trading
)

# Get limit price
limit_price = get_limit_price(
    broker=broker,
    price_type=[2500.0],  # Can be numeric, list, or broker price type
    symbol="RELIANCE",
    exchange="NSE"
)
```

### Fetching Historical Data

```python
from datetime import datetime, timedelta

# Fetch historical data
historical_data = broker.get_historical(
    symbols="RELIANCE",
    date_start="2024-01-01",
    date_end="2024-12-16",
    exchange="N",
    periodicity="1m",  # 1m, 5m, 1d, etc.
    market_close_time="15:30:00"
)

# Access data
for symbol, data_list in historical_data.items():
    for data_point in data_list:
        print(f"{data_point.date}: O={data_point.open}, H={data_point.high}, "
              f"L={data_point.low}, C={data_point.close}, V={data_point.volume}")
```

### Logging Configuration

```python
from tradingapi import configure_logging
import logging

# Configure logging
configure_logging(
    level=logging.INFO,
    log_file="/path/to/logs/tradingapi.log",
    enable_console=True,
    backup_count=7,  # Keep 7 days of logs
    enable_structured_logging=True
)

# Use the logger
from tradingapi import trading_logger

trading_logger.log_info("Application started", {"user": "john_doe"})
trading_logger.log_error("Error occurred", error=exception, context={"order_id": "12345"})
```

### Runtime Log Level Toggle

The package supports runtime log level toggling via SIGUSR1 signal:

```bash
# Toggle between DEBUG and INFO logging levels
kill -SIGUSR1 <process_id>
```

This is automatically enabled when the package is imported.

## Supported Brokers

- **FivePaisa**: Full support for order placement, historical data, market data streaming
- **Shoonya**: Full support for order placement, historical data, market data streaming
- **FlatTrade**: Full support for order placement, historical data, market data streaming
- **ICICIDirect**: Basic support (some features may be in development)

## Error Handling

The package includes comprehensive error handling with custom exceptions:

- `ConfigurationError`: Configuration-related errors
- `ValidationError`: Input validation errors
- `BrokerConnectionError`: Broker connection issues
- `OrderError`: Order placement/modification errors
- `MarketDataError`: Market data retrieval errors
- `SymbolError`: Symbol-related errors
- `NetworkError`: Network-related errors
- `AuthenticationError`: Authentication failures

All errors include structured context for debugging.

## Contributing

Pull requests and issue reports are welcome. Please include:
- Clear reproduction steps
- Relevant logs
- Configuration details (with sensitive data redacted)
- Expected vs actual behavior

## License

MIT License

## Repository

- **Homepage**: https://bitbucket.org/incurrency/tradingpapi2
- **Repository**: https://bitbucket.org/incurrency/tradingapi2

## Support

For issues and questions, please open an issue in the repository or contact the maintainers.
