from types import SimpleNamespace
from unittest.mock import Mock

from tradingapi import utils


def _execution_broker():
    broker = Mock()
    broker.broker = SimpleNamespace(name="DHAN")
    broker.map_exchange_for_api.return_value = "NSE"
    broker.exchange_mappings = {
        "NSE": {"exchangetype_map": {"TCS_STK___": "C"}},
    }
    broker.starting_order_ids_int = {}
    return broker


def test_place_combo_order_forwards_price_broker_for_entry(monkeypatch):
    execution_broker = _execution_broker()
    data_broker = Mock()
    transmit = Mock(return_value="TEST_1")
    monkeypatch.setattr(utils, "transmit_entry_order", transmit)

    utils.place_combo_order(
        execution_broker,
        "TEST",
        ["TCS_STK___"],
        [1],
        entry=True,
        exchanges=["NSE"],
        price_broker=[data_broker],
        price_types=["LMT"],
    )

    assert transmit.call_args.kwargs["price_broker"] == [data_broker]


def test_place_combo_order_forwards_price_broker_for_exit(monkeypatch):
    execution_broker = _execution_broker()
    data_broker = Mock()
    transmit = Mock()
    monkeypatch.setattr(utils, "transmit_exit_order", transmit)

    utils.place_combo_order(
        execution_broker,
        "TEST",
        ["TCS_STK___"],
        [-1],
        entry=False,
        exchanges=["NSE"],
        price_broker=[data_broker],
        price_types=["LMT"],
    )

    assert transmit.call_args.kwargs["price_broker"] == [data_broker]
