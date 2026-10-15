import pandas as pd
import pytest

from tradingapi.broker_base import Price
from tradingapi import utils


def test_calculate_mtm_uses_last_when_bid_and_ask_are_zero(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_price",
        lambda *args, **kwargs: Price(bid=0, ask=0, last=-1188, prior_close=-1692),
    )
    trades = pd.DataFrame([{
        "symbol": "PUT?-20:CALL?-20",
        "entry_quantity": 18,
        "exit_quantity": 0,
        "entry_price": -1957.83332,
        "exit_price": 0,
        "mtm": -1957.83332,
    }])

    marked = utils.calculate_mtm([], trades, mds="mds")

    assert marked.loc[0, "mtm"] == -1188
    assert marked.loc[0, "gross_pnl"] == pytest.approx(13856.99976)
