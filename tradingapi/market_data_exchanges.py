"""Which exchanges each broker supports for streaming market data (YAML + defaults)."""

from __future__ import annotations

from typing import Union

from .broker_base import Brokers
from tradingapi import trading_logger

_BUILTIN_MARKET_DATA_EXCHANGES_BY_BROKER: dict[str, frozenset[str]] = {
    "DHAN": frozenset({"NSE", "BSE"}),
    "FIVEPAISA": frozenset({"NSE", "BSE", "MCX"}),
    "SHOONYA": frozenset({"NSE", "BSE", "MCX"}),
}

_DEFAULT_UNKNOWN_BROKER_EXCHANGES: frozenset[str] = frozenset({"NSE", "BSE", "MCX"})


def normalize_market_data_exchange(exchange: str) -> str:
    return str(exchange).strip().upper()


def _broker_section_key(broker: Union[Brokers, str]) -> str:
    if isinstance(broker, Brokers):
        return broker.name
    return str(broker).strip().upper()


def get_market_data_exchanges_for_broker(broker: Union[Brokers, str]) -> frozenset[str]:
    """
    Normalized exchange codes the broker supports for quote streaming.

    If ``{BROKER}.EXCHANGES`` is set in YAML to a list, that list is used.
    Otherwise built-in defaults apply (Dhan: NSE/BSE; FivePaisa/Shoonya: NSE/BSE/MCX;
    others: NSE/BSE/MCX).
    """
    from .config import get_config

    key = _broker_section_key(broker)
    raw = get_config().get(f"{key}.EXCHANGES")
    if raw is None:
        raw = get_config().get(f"{key}.exchanges")  # legacy lowercase key
    if isinstance(raw, list):
        return frozenset(
            normalize_market_data_exchange(x) for x in raw if x is not None and str(x).strip()
        )
    if raw is not None:
        trading_logger.log_warning(
            f"Invalid exchanges config (expected list) for broker {key}; using built-in defaults",
            context={"broker": key, "exchanges_raw_type": type(raw).__name__},
        )
    return _BUILTIN_MARKET_DATA_EXCHANGES_BY_BROKER.get(key, _DEFAULT_UNKNOWN_BROKER_EXCHANGES)


def broker_supports_market_data_exchange(broker: Union[Brokers, str], exchange: str) -> bool:
    ex = normalize_market_data_exchange(exchange)
    return ex in get_market_data_exchanges_for_broker(broker)
