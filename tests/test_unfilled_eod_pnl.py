from unittest.mock import MagicMock

import pytest

from tradingapi.broker_base import Brokers, OrderInfo, OrderStatus
from tradingapi.utils import (
    _get_confirmed_open_position_by_order,
    should_prune_terminal_zero_fill_leg,
    update_order_status,
    _process_broker_order_update,
    get_open_position_by_order,
)
from tradingapi.broker_base import Order
from tradingapi.fivepaisa import FivePaisa
from tradingapi.exceptions import ValidationError


def test_invalid_broker_id_cannot_overwrite_shared_key():
    broker = MagicMock()
    order = Order(broker_order_id="0", internal_order_id="NEW_1", status="OPEN")
    assert _process_broker_order_update(broker, order, "NIFTY_OPT_20261123_PUT_24900") == ""
    broker.redis_o.hmset.assert_not_called()
    broker.redis_o.hset.assert_not_called()


def test_broker_remap_rejects_zero_before_redis_mutation():
    broker = MagicMock()
    with pytest.raises(ValidationError):
        FivePaisa._update_broker_order_id(broker, "NEW_1", "12345", "0")
    broker.redis_o.pipeline.assert_not_called()


def test_positions_ignore_zero_without_expiring():
    broker = MagicMock()
    broker.redis_o.hget.return_value = "0"
    broker.redis_o.hgetall.return_value = {
        "internal_order_id": "OTHER_1", "long_symbol": "NIFTY_OPT_20261123_PUT_24900",
        "quantity": "1170", "order_type": "SHORT", "price": "17.1",
    }
    assert get_open_position_by_order(broker, "OLD_1") == {}
    assert all(call.args[0] != "0" for call in broker.redis_o.hgetall.call_args_list)
    broker.get_quote.assert_not_called()
    broker.redis_o.hmset.assert_not_called()


def test_cancelled_zero_fill_should_prune():
    fills = OrderInfo(
        order_size=65,
        order_price=25.0,
        fill_size=0,
        fill_price=0,
        status=OrderStatus.CANCELLED,
        broker_order_id="449150478",
        exchange_order_id="1400000189211783",
        broker=Brokers.FIVEPAISA,
    )
    assert should_prune_terminal_zero_fill_leg(fills) is True


def test_eod_undefined_zero_fill_zeros_redis_quantity():
    redis = MagicMock()
    redis.hgetall.return_value = {
        "long_symbol": "NIFTY_OPT_20260915_CALL_23300",
        "order_type": "SELL",
        "quantity": "65",
        "price": "25",
        "exchange": "NSE",
        "broker_order_id": "449150478",
        "internal_order_id": "CASOTM01_2",
        "status": "OPEN",
        "broker": "FIVEPAISA",
        "remote_order_id": "2026091515150000",
        "exch_order_id": "1400000189211783",
    }
    broker = MagicMock()
    broker.redis_o = redis
    broker.broker = Brokers.FIVEPAISA
    broker.get_order_info.return_value = OrderInfo(
        order_size=65,
        order_price=25.0,
        fill_size=0,
        fill_price=0,
        status=OrderStatus.UNDEFINED,
        broker_order_id="449150478",
        exchange_order_id="1400000189211783",
        broker=Brokers.FIVEPAISA,
    )

    update_order_status(broker, "CASOTM01_2", "449150478", eod=True)

    redis.hset.assert_any_call("449150478", "quantity", "0")
    redis.delete.assert_not_called()


def test_confirmed_positions_exclude_rejected_combo_leg():
    redis = MagicMock()
    redis.hgetall.side_effect = lambda key: {
        "STRANGLENIFTY01_29": {
            "entry_keys": "put-order call-order",
            "exit_keys": "",
        },
        "put-order": {
            "long_symbol": "NIFTY_OPT_20261123_PUT_24900",
            "order_type": "SHORT",
            "quantity": "1170",
            "broker_order_id": "put-order",
        },
        "call-order": {
            "long_symbol": "NIFTY_OPT_20261123_CALL_25900",
            "order_type": "SHORT",
            "quantity": "1170",
            "broker_order_id": "call-order",
        },
    }.get(key, {})
    broker = MagicMock()
    broker.redis_o = redis
    broker.get_order_info.side_effect = [
        OrderInfo(fill_size=0, status=OrderStatus.REJECTED, broker_order_id="put-order"),
        OrderInfo(fill_size=1170, fill_price=14.1, status=OrderStatus.FILLED, broker_order_id="call-order"),
    ]

    positions = _get_confirmed_open_position_by_order(broker, "STRANGLENIFTY01_29")

    assert list(positions) == ["NIFTY_OPT_20261123_CALL_25900"]
    assert positions["NIFTY_OPT_20261123_CALL_25900"].size == -1170
