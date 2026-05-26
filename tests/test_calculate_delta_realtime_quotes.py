import math
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi import utils


class CalculateDeltaRealtimeQuotesTest(unittest.TestCase):
    def test_calculate_delta_returns_nan_without_live_bid_ask(self):
        ticker = SimpleNamespace(bid=0.0, ask=0.0, prior_close=123.45)

        with patch.object(utils, "get_price", return_value=ticker):
            delta = utils.calculate_delta(
                brokers=[],
                long_symbol="TCS_OPT_20260630_PUT_2160",
                price_f=2325.1,
                exchange="NFO",
                mds="mds",
            )

        self.assertTrue(math.isnan(delta))


if __name__ == "__main__":
    unittest.main()
