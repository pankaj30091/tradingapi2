"""
Allocation helpers for budget-based position sizing.

Strategies read their daily pct from ~/onedrive/.tradingapi/allocations/master_allocation_today.yaml,
then call these helpers at each entry decision to derive a live per-entry target margin.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    pass

_ALLOCATIONS_DIR = Path(os.path.expanduser("~/onedrive/.tradingapi/allocations"))
_TODAY_SYMLINK = _ALLOCATIONS_DIR / "master_allocation_today.yaml"


def load_today_allocation(broker_name: str, strategy_name: str) -> float:
    """Return the pct (0.0–1.0) allocated to strategy_name on broker_name today.

    Reads master_allocation_today.yaml (a symlink to today's dated file).
    Raises FileNotFoundError if the symlink/file is missing.
    Raises ValueError if the date in the file does not match today.
    Returns 0.0 if the strategy is listed with pct == 0 (inactive today).
    Raises KeyError if the broker/strategy is not found in the file at all.
    """
    if not _TODAY_SYMLINK.exists():
        raise FileNotFoundError(
            f"Master allocation file not found: {_TODAY_SYMLINK}. "
            "Run generate_master_allocation.py before starting strategies."
        )

    with open(_TODAY_SYMLINK) as f:
        data = yaml.safe_load(f)

    file_date = str(data.get("date", "")).strip()
    today_str = dt.date.today().isoformat()
    if file_date != today_str:
        raise ValueError(
            f"Master allocation file is stale: file date={file_date!r}, today={today_str!r}. "
            "Regenerate with generate_master_allocation.py."
        )

    broker_key = broker_name.strip().upper()
    brokers = data.get("brokers", {})
    if broker_key not in brokers:
        raise KeyError(
            f"Broker {broker_key!r} not found in master allocation. "
            f"Available brokers: {list(brokers.keys())}"
        )

    strategies = brokers[broker_key].get("strategies", {})
    strategy_key = strategy_name.strip().upper()
    if strategy_key not in strategies:
        raise KeyError(
            f"Strategy {strategy_key!r} not found under broker {broker_key!r} in master allocation. "
            f"Available strategies: {list(strategies.keys())}"
        )

    entry = strategies[strategy_key]
    return float(entry.get("pct", 0.0))


def broker_capital(broker) -> float:
    """Fetch live total capital from broker: cash + collateral."""
    caps = broker.get_available_capital()
    return float(caps.get("cash", 0.0)) + float(caps.get("collateral", 0.0))


def current_utilisation(broker, strategy_name: str) -> float:
    """Sum of margin currently consumed by open positions of this strategy.

    Calls broker.get_margin_requirement() for each open position (net=False).
    Exchange is read from Redis per position; defaults to NSE if missing.
    """
    from tradingapi.utils import get_pnl_table, hget_with_default

    try:
        pnl = get_pnl_table(broker, strategy_name, refresh_status=True)
    except Exception:
        return 0.0

    if pnl.empty:
        return 0.0

    open_mask = (pnl["entry_quantity"].fillna(0) + pnl["exit_quantity"].fillna(0)) != 0
    exit_empty = pnl["exit_time"].fillna("").astype(str).str.strip().isin(["", "0", "NaT"])
    open_pnl = pnl.loc[open_mask | exit_empty]

    total = 0.0
    for _, row in open_pnl.iterrows():
        combo_symbol = str(row.get("symbol", "")).strip()
        if not combo_symbol:
            continue
        net_qty = float(row.get("entry_quantity", 0) or 0) + float(row.get("exit_quantity", 0) or 0)
        if net_qty == 0:
            continue
        # Signed: positive = long, negative = short.
        # parse_combo_symbol handles both simple and combo symbols uniformly.
        order_size = int(net_qty)

        entry_keys_str = str(row.get("entry_keys", "")).strip()
        first_key = entry_keys_str.split()[0] if entry_keys_str else ""
        exchange = hget_with_default(broker, first_key, "exchange", "NSE") if first_key else "NSE"

        try:
            margin = broker.get_margin_requirement(combo_symbol, order_size, exchange)
            if margin is not None:
                total += float(margin)
        except Exception:
            pass
    return total


def remaining_budget(budget: float, utilised: float) -> float:
    return max(0.0, budget - utilised)


def _remaining_factor_sum(open_positions: int, max_positions: int, decay: float, min_frac: float) -> float:
    """Sum of decay fractions for future legs [open_positions, max_positions)."""
    return float(sum(
        max(min_frac, decay ** k)
        for k in range(open_positions, max_positions)
    ))


def scalping_entry_target(
    remaining: float,
    open_positions: int,
    max_positions: int,
    decay: float,
    min_frac: float,
) -> float:
    """Target margin for the next scalping entry given remaining budget.

    Entry 1 (0 open): remaining / sum([1.0, 0.75, 0.5625]) ≈ remaining / 2.3125
    Entry 2 (1 open): remaining / sum([0.75, 0.5625]) ≈ remaining / 1.3125
    Entry 3 (2 open): remaining / 0.5625 → clamped to remaining (last leg gets all that's left)
    """
    if open_positions >= max_positions:
        return 0.0
    factor_sum = _remaining_factor_sum(open_positions, max_positions, decay, min_frac)
    if factor_sum <= 0:
        return remaining
    raw = remaining / factor_sum
    return min(raw, remaining)


def strangle_entry_target(budget_now: float, remaining: float, scheduled_entries: int) -> float:
    """Target margin for the next strangle entry.

    Equal weighting across scheduled entries, clamped to what's actually left.
    """
    if scheduled_entries <= 0:
        return remaining
    equal_share = budget_now / scheduled_entries
    return min(equal_share, remaining)
