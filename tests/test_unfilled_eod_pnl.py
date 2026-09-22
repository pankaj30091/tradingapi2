from unittest.mock import MagicMock

from tradingapi.broker_base import Brokers, OrderInfo, OrderStatus
from tradingapi.utils import should_prune_terminal_zero_fill_leg, update_order_status


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
