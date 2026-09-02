import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi import flattrade


class FlatTradeSymbologyTest(unittest.TestCase):
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
