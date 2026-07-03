from tradingapi.broker_base import (
    is_broker_side_terminal_message,
    is_broker_side_terminal_order,
    is_missing_exchange_order_id,
)


def test_is_missing_exchange_order_id():
    assert is_missing_exchange_order_id("0")
    assert is_missing_exchange_order_id(0)
    assert is_missing_exchange_order_id(None)
    assert is_missing_exchange_order_id("")
    assert not is_missing_exchange_order_id("2700000201461104")


def test_is_broker_side_terminal_message():
    assert is_broker_side_terminal_message("Trading not allowed in illiquid  contract")
    assert is_broker_side_terminal_message("Order rejected by RMS")
    assert is_broker_side_terminal_message(
        "RMS:23226063020407:You have insufficient funds. Please add Rs.71301.37 to trade."
    )
    assert is_broker_side_terminal_message("Cancelled by user")
    assert not is_broker_side_terminal_message("")
    assert not is_broker_side_terminal_message("Success")


def test_is_broker_side_terminal_order_requires_missing_exchange_id():
    msg = "Trading not allowed in illiquid contract"
    assert is_broker_side_terminal_order("0", msg)
    assert not is_broker_side_terminal_order("12345", msg)
