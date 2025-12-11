# tradingapi

Python utilities for broker integration (Shoonya, FivePaisa, Flattrade) with helpers for order placement, symbology handling, margin fetch, and rate-limited market data access.

## Features
- Unified broker interfaces and helpers (`BrokerBase`, Shoonya, FivePaisa, Flattrade).
- Order utilities: combo handling, trigger/stop-loss support, idempotency checks, and Redis-backed tracking.
- Symbology parsing for weekly/monthly expiries and futures/options normalization.
- Margin helpers and misc utilities for rounding, limit-price selection, and retries.

## Installation
```bash
pip install tradingapi
```

## Quick start
```python
from tradingapi.fivepaisa import FivePaisa
from tradingapi.utils import transmit_entry_order

broker = FivePaisa()
broker.connect()  # configure credentials in your env/config

# Place an order (example)
# order = ...
# transmit_entry_order(broker, order)
```

## Configuration
- Sample configs live under `tradingapi/config/`.
- Ensure broker credentials and any Redis settings are provided via your environment/config files.

## Contributing
Pull requests and issue reports are welcome. Please include clear repro steps and logs when reporting broker/API issues.


