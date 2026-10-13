from types import SimpleNamespace
from unittest.mock import Mock

import pytest

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


@pytest.mark.parametrize("target,expected", [("TEST_2", ["TEST_2"]), ("", ["TEST_1"])])
def test_exit_fill_attribution_and_retry(monkeypatch, target, expected):
    broker = _execution_broker()
    broker.redis_o.hgetall.return_value = {}
    symbol = "TCS_STK___"
    positions = {"TEST_1": -520, "TEST_2": -130}
    filled = []
    monkeypatch.setattr(utils, "get_exit_candidates", lambda *args: ["TEST_1", "TEST_2"])
    monkeypatch.setattr(utils, "get_open_position_by_order", lambda _, key: {
        symbol: SimpleNamespace(size=positions[key]),
    })
    monkeypatch.setattr(utils, "get_limit_price", lambda *args, **kwargs: 114.3)
    monkeypatch.setattr(utils, "_process_broker_order_update", lambda *args: None)

    def fill(order):
        positions[order.internal_order_id] += order.quantity
        filled.append(order.internal_order_id)
        return order

    broker.place_order.side_effect = fill
    utils.place_combo_order(broker, "TEST", [symbol], [130], entry=False,
                           price_types=["MKT"], exit_int_order_id=target)
    assert filled == expected
    if target:
        assert positions == {"TEST_1": -520, "TEST_2": 0}
        utils.place_combo_order(broker, "TEST", [symbol], [130], entry=False,
                               price_types=["MKT"], exit_int_order_id=target)
        assert filled == ["TEST_2"]


def test_target_survives_freeze_slices(monkeypatch):
    broker = _execution_broker()
    transmit = Mock()
    monkeypatch.setattr(utils, "transmit_exit_order", transmit)
    monkeypatch.setattr(utils.time, "sleep", lambda _: None)
    utils.place_combo_order(broker, "TEST", ["TCS_STK___"], [260], entry=False,
                           price_types=["MKT"], freeze_limit_lots=130,
                           exit_int_order_id="TEST_2")
    assert transmit.call_count == 2
    assert all(call.kwargs["int_order_id"] == "TEST_2" for call in transmit.call_args_list)


@pytest.mark.parametrize("entry,validate,target", [
    (True, True, "TEST_2"), (False, False, "TEST_2"), (False, True, "OTHER_2"),
])
def test_invalid_exit_target_rejected(entry, validate, target):
    with pytest.raises(ValueError, match="exit_int_order_id"):
        utils.place_combo_order(_execution_broker(), "TEST", ["TCS_STK___"], [130],
                               entry=entry, validate_db_position=validate,
                               price_types=["MKT"], exit_int_order_id=target)


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
