import datetime as dt
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi import config
from tradingapi import market_hours
from tradingapi import utils


MARKET_CONFIG = {
    "market_open_time": "09:15:00",
    "market_close_time": "15:30:00",
    "market_hours": [
        {
            "effective_date": "2026-08-03",
            "exchanges": {
                "NSE": {
                    "CASH": {"open_time": "09:15:00", "close_time": "15:35:00"},
                    "FNO": {"open_time": "09:15:00", "close_time": "15:40:00"},
                },
                "BSE": {
                    "CASH": {"open_time": "09:15:00", "close_time": "15:35:00"},
                    "FNO": {"open_time": "09:15:00", "close_time": "15:40:00"},
                },
            },
        }
    ],
}


class MarketHoursConfigTest(unittest.TestCase):
    def setUp(self):
        self.config = config.get_config()
        self.config_patch = patch.object(self.config, "configs", MARKET_CONFIG)
        self.config_patch.start()

    def tearDown(self):
        self.config_patch.stop()

    def test_pre_effective_date_uses_legacy_fallback(self):
        self.assertEqual(
            config.get_market_close_time(
                "NFO",
                symbol="NIFTY_OPT_20260730_CALL_25000",
                as_of="2026-07-30",
            ),
            "15:30:00",
        )

    def test_nse_cash_and_fno_are_resolved_separately(self):
        self.assertEqual(
            config.get_market_close_time("NSE", symbol="RELIANCE_STK___", as_of="2026-08-03"),
            "15:35:00",
        )
        self.assertEqual(
            config.get_market_close_time("NSE", symbol="NIFTY_FUT_20260827__", as_of="2026-08-03"),
            "15:40:00",
        )

    def test_exchange_alias_infers_fno(self):
        self.assertEqual(
            config.get_market_close_time("BFO", as_of="2026-08-03"),
            "15:40:00",
        )
        self.assertEqual(
            config.get_market_close_time(
                symbol="SENSEX_OPT_20260827_CALL_80000", as_of="2026-08-03"
            ),
            "15:40:00",
        )

    def test_session_bounds_uses_symbol_segment(self):
        session_date = dt.date(2026, 8, 3)
        cash = market_hours.session_bounds(session_date, exchange="NSE", symbol="NIFTY_IND___")
        fno = market_hours.session_bounds(
            session_date, exchange="NSE", symbol="NIFTY_OPT_20260827_CALL_25000"
        )
        self.assertEqual(cash[1].time(), dt.time(15, 35))
        self.assertEqual(fno[1].time(), dt.time(15, 40))

    def test_latest_close_covers_mixed_market_services(self):
        self.assertEqual(
            market_hours.latest_close_datetime(dt.date(2026, 8, 3)).time(),
            dt.time(15, 40),
        )

    def test_latest_effective_schedule_wins(self):
        updated = {
            **MARKET_CONFIG,
            "market_hours": MARKET_CONFIG["market_hours"]
            + [
                {
                    "effective_date": "2027-01-01",
                    "exchanges": {
                        "NSE": {
                            "FNO": {
                                "open_time": "09:20:00",
                                "close_time": "15:45:00",
                            }
                        }
                    },
                }
            ],
        }
        with patch.object(self.config, "configs", updated):
            self.assertEqual(
                config.get_market_close_time("NFO", as_of="2027-01-02"),
                "15:45:00",
            )
            self.assertEqual(
                config.get_market_open_time("NFO", as_of="2027-01-02"),
                "09:20:00",
            )
            self.assertFalse(
                config.is_within_market_hours(
                    dt.datetime(2027, 1, 2, 9, 17), exchange="NFO"
                )
            )
            self.assertTrue(
                config.is_within_market_hours(
                    dt.datetime(2027, 1, 2, 9, 21), exchange="NFO"
                )
            )

    def test_derivative_expiry_uses_schedule_for_expiry_date(self):
        before = utils._derivative_expiry_datetime(
            "20260730", "NIFTY_OPT_20260730_CALL_25000", "NFO"
        )
        after = utils._derivative_expiry_datetime(
            "20260827", "NIFTY_OPT_20260827_CALL_25000", "NFO"
        )
        self.assertEqual(before.strftime("%H:%M:%S"), "15:30:00")
        self.assertEqual(after.strftime("%H:%M:%S"), "15:40:00")


if __name__ == "__main__":
    unittest.main()
