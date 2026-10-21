import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi import flattrade


class FlatTradeSymbologyTest(unittest.TestCase):
    def test_time_price_series_retries_json_decode_errors(self):
        broker = object.__new__(flattrade.FlatTrade)
        broker.api = Mock()
        broker.api.get_time_price_series.side_effect = [
            json.JSONDecodeError("empty", "", 0),
            json.JSONDecodeError("empty", "", 0),
            [{"time": "21-10-2026 09:15:00"}],
        ]
        broker._wait_for_historical_rate_limit = Mock()

        with patch.object(flattrade.time, "sleep"):
            result = broker._get_time_price_series_with_retry(
                exchange="NSE",
                token="26000",
                starttime=1.0,
                endtime=2.0,
                interval=1,
                symbol="NIFTY_IND___",
            )

        self.assertEqual(result, [{"time": "21-10-2026 09:15:00"}])
        self.assertEqual(broker.api.get_time_price_series.call_count, 3)

    def test_update_uses_bod_filename(self):
        with tempfile.TemporaryDirectory() as folder:
            symbols_path = Path(folder) / "20260902_symbols.csv"
            symbols_path.write_text(
                "long_symbol,LotSize,Scripcode,Exch,ExchType,TickSize,trading_symbol\n"
                "NIFTY_IND___,1,26000,NSE,NSE,0.05,Nifty 50\n",
                encoding="utf-8",
            )
            broker = object.__new__(flattrade.FlatTrade)
            broker.account_key = "FLATTRADE"
            with (
                patch.object(
                    flattrade.config,
                    "get",
                    return_value=folder,
                ),
                patch.object(
                    flattrade,
                    "get_tradingapi_now",
                    return_value=dt.datetime(2026, 9, 2, 9, 57),
                ),
                patch.object(
                    flattrade,
                    "save_symbol_data",
                    side_effect=AssertionError("BOD file should be reused"),
                ),
            ):
                broker.update_symbology()

            self.assertEqual(
                broker.exchange_mappings["NSE"]["symbol_map"]["NIFTY_IND___"],
                26000,
            )


if __name__ == "__main__":
    unittest.main()
