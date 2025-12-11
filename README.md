# tradingapi

Python utilities for broker integration (Shoonya, FivePaisa, Flattrade) with helpers for order placement, symbology handling, margin fetch, and rate-limited market data access.

## Features
- Unified broker interfaces and helpers (`BrokerBase`, Shoonya, FivePaisa, Flattrade).
- Order utilities: combo handling, trigger/stop-loss support, idempotency checks, and Redis-backed tracking.
- Symbology parsing for weekly/monthly expiries and futures/options normalization.
- Margin helpers and misc utilities for rounding, limit-price selection, and retries.
- Supports trigger price propagation, stop-loss flagging, and idempotent retries for FivePaisa.
- Shoonya/FivePaisa symbol parsing for weekly expiries (incl. October regex fixes).
- Redis-backed order tracking for entry/exit keys.

## Installation
```bash
pip install tradingapi
```

## Brokers & config
- Supported: Shoonya, FivePaisa, Flattrade.
- Sample configs live under `tradingapi/config/` (e.g., `config_sample.yaml`, commissions).
- Provide broker credentials and Redis settings via your environment/config files.
- If you use Redis-backed order tracking, ensure Redis is reachable and configured.

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

### Limit price helper
```python
from tradingapi.utils import get_limit_price

# price_type can be numeric, list (e.g., [mid_price]), or a broker price type
limit_px = get_limit_price(broker, price_type=[123.45], symbol="RELIANCE", exchange="NSE")
```

### Idempotent FivePaisa order placement
- Orders are checked by `remote_order_id` to avoid duplicates.
- Trigger/stop-loss flags are propagated; payloads are logged for debugging.

## Configuration
- Sample configs live under `tradingapi/config/`.
- Ensure broker credentials and any Redis settings are provided via your environment/config files.

## Contributing
Pull requests and issue reports are welcome. Please include clear repro steps and logs when reporting broker/API issues.


