import os
import sys
import types
import tempfile
import unittest

import pandas as pd

PROJECT_ROOT = "/home/psharma/onedrive/code/tradingapi2"
CONFIG_SAMPLE = "/home/psharma/onedrive/code/tradingapi2/tradingapi/config/config_sample.yaml"

sys.path.insert(0, PROJECT_ROOT)
os.environ.setdefault("TRADINGAPI_CONFIG_PATH", CONFIG_SAMPLE)


class FakeBreezeConnect:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = {}

    def generate_session(self, api_secret: str, session_token: str):
        self.session = {"api_secret": api_secret, "session_token": session_token}

    def get_funds(self):
        return {"Success": {"available_cash": "1000", "collateral": "250"}}

    def place_order(self, **kwargs):
        return {"Success": {"order_id": "OID123"}}

    def modify_order(self, **kwargs):
        return {"Success": {"order_id": kwargs.get("order_id")}}

    def cancel_order(self, **kwargs):
        return {"Success": {"order_id": kwargs.get("order_id")}}

    def get_order_detail(self, **kwargs):
        return {"Success": {"order_id": kwargs.get("order_id"), "status": "complete", "quantity": "10"}}

    def get_order_list(self, **kwargs):
        return {"Success": []}

    def get_trade_list(self, **kwargs):
        return {"Success": []}

    def get_portfolio_positions(self):
        return {"Success": [{"stock_code": "123", "qty": "10"}]}

    def get_historical_data(self, **kwargs):
        return {"Success": []}


fake_module = types.ModuleType("breeze_connect")
fake_module.BreezeConnect = FakeBreezeConnect
sys.modules["breeze_connect"] = fake_module

import tradingapi.icicidirect as icicidirect  # noqa: E402
from tradingapi.config import get_config  # noqa: E402
from tradingapi.broker_base import Order, OrderStatus  # noqa: E402


class FakeRedis:
    def __init__(self, *args, **kwargs):
        pass

    def scan(self, cursor, match=None, count=None):
        return 0, []


class IciciDirectTests(unittest.TestCase):
    def setUp(self):
        self.config = get_config()
        self.original_configs = dict(self.config.configs)

        self.tmp_dir = tempfile.TemporaryDirectory()
        self.usertoken_path = os.path.join(self.tmp_dir.name, "icici_usertoken.txt")

        self.config.configs["ICICIDIRECT"] = {
            "API_KEY": "test_key",
            "API_SECRET": "test_secret",
            "USERTOKEN": self.usertoken_path,
            "REDIRECT_URL": "http://127.0.0.1:5000/callback",
        }

        self.config.configs.setdefault("bhavcopy_folder", self.tmp_dir.name)

        icicidirect.redis.Redis = FakeRedis

    def tearDown(self):
        self.config.configs = self.original_configs
        self.tmp_dir.cleanup()

    def _make_broker(self):
        broker = icicidirect.IciciDirect()
        broker.codes = pd.DataFrame([{"dummy": 1}])
        broker.exchange_mappings = {
            "NSE": {
                "symbol_map": {"RELIANCE_STK___": "123"},
                "contractsize_map": {"RELIANCE_STK___": 1},
                "exchange_map": {"RELIANCE_STK___": "NSE"},
                "exchangetype_map": {"RELIANCE_STK___": "CASH"},
                "contracttick_map": {"RELIANCE_STK___": 0.05},
                "symbol_map_reversed": {"123": "RELIANCE_STK___"},
            }
        }
        return broker

    def test_connect_uses_cached_usertoken(self):
        with open(self.usertoken_path, "w") as file:
            file.write("cached_token")

        broker = self._make_broker()
        broker.connect(redis_db=0)

        self.assertIsNotNone(broker.api)
        self.assertEqual(broker.api.session.get("session_token"), "cached_token")

    def test_place_order_sets_broker_order_id(self):
        broker = self._make_broker()
        broker.api = FakeBreezeConnect("test_key")

        order = Order(
            long_symbol="RELIANCE_STK___",
            exchange="NSE",
            quantity=10,
            price=2500.0,
            order_type="BUY",
        )

        result = broker.place_order(order)
        self.assertEqual(result.broker_order_id, "OID123")
        self.assertEqual(result.status, OrderStatus.PENDING)

    def test_get_available_capital_returns_cash_and_collateral(self):
        broker = self._make_broker()
        broker.api = FakeBreezeConnect("test_key")

        funds = broker.get_available_capital()
        self.assertEqual(funds["cash"], 1000.0)
        self.assertEqual(funds["collateral"], 250.0)

    def test_get_order_info_maps_status(self):
        broker = self._make_broker()
        broker.api = FakeBreezeConnect("test_key")

        info = broker.get_order_info(broker_order_id="OID123")
        self.assertEqual(info.status, OrderStatus.FILLED)
        self.assertEqual(info.broker_order_id, "OID123")


if __name__ == "__main__":
    unittest.main()
