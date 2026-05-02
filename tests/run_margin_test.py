import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tradingapi.dhan import Dhan  # noqa: E402
from tradingapi.fivepaisa import FivePaisa  # noqa: E402
from tradingapi.flattrade import FlatTrade  # noqa: E402
from tradingapi.icicidirect import IciciDirect  # noqa: E402
from tradingapi.shoonya import Shoonya  # noqa: E402


BROKER_MAP = {
    "dhan": Dhan,
    "fivepaisa": FivePaisa,
    "shoonya": Shoonya,
    "flattrade": FlatTrade,
    "icicidirect": IciciDirect,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone tradingapi margin test runner.")
    parser.add_argument("--broker", required=True, choices=sorted(BROKER_MAP.keys()))
    parser.add_argument("--combo-symbol", required=True, help="Single symbol or combo symbol.")
    parser.add_argument("--order-size", required=True, type=int, help="Order size in lots/combo units.")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--account", default=None, help="Optional tradingapi account key override.")
    parser.add_argument("--redis-db", default=6, type=int)
    parser.add_argument("--mds", default=None, help="Optional market data source.")
    return parser.parse_args()


def instantiate_broker(name: str, account: str | None) -> Any:
    broker_cls = BROKER_MAP[name]
    return broker_cls(account=account) if account else broker_cls()


def main() -> int:
    args = parse_args()
    broker = instantiate_broker(args.broker, args.account)

    print(f"Connecting broker={args.broker} account={args.account or 'default'} redis_db={args.redis_db}")
    connected = broker.connect(redis_db=args.redis_db)
    if not connected:
        print("Connection failed")
        return 1

    result: dict[str, Any] = {
        "broker": args.broker,
        "account": args.account,
        "combo_symbol": args.combo_symbol,
        "order_size": args.order_size,
        "exchange": args.exchange,
    }

    print("Calling broker.get_margin_requirement(...)")
    broker_margin = broker.get_margin_requirement(
        args.combo_symbol,
        args.order_size,
        exchange=args.exchange,
        mds=args.mds,
    )
    result["broker_margin"] = broker_margin

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
