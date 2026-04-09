import datetime as dt
import argparse
import json
import math
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tradingapi.broker_base import HistoricalData  # noqa: E402
from tradingapi.dhan import Dhan  # noqa: E402
from tradingapi.fivepaisa import FivePaisa  # noqa: E402
from tradingapi.flattrade import FlatTrade  # noqa: E402
from tradingapi.icicidirect import IciciDirect  # noqa: E402
from tradingapi.shoonya import Shoonya  # noqa: E402
from tradingapi.utils import place_combo_order  # noqa: E402


# ---------------------------------------------------------------------------
# User config
# ---------------------------------------------------------------------------

BROKER_NAME = "dhan"
REDIS_DB = 4
CONFIG_PATH = os.environ.get("TRADINGAPI_CONFIG_PATH", "/home/psharma/onedrive/.tradingapi/tradingapi.yaml")
ORDER_STRATEGY_PREFIX = "brokertest"
LOCAL_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
QUOTE_SLEEP_SEC = 1.4
STREAM_WAIT_SEC = 8.0
STREAM_SETTLE_SEC = 1.0
POST_ORDER_SLEEP_SEC = 2.0
POST_MODIFY_SLEEP_SEC = 1.5
POST_CANCEL_SLEEP_SEC = 1.5

CORE_SYMBOLS = {
    "equity_nse": ("INFY_STK___", "NSE"),
    "index_nse": ("NIFTY_IND___", "NSE"),
    "index_bse": ("SENSEX_IND___", "BSE"),
}

INDEX_OPTION_RULES = [
    ("NIFTY", "NSE"),
    ("SENSEX", "BSE"),
]
ORDER_TEST_EQUITY = ("INFY_STK___", "NSE", 1)
# Default option-order leg uses SENSEX so live orders cover BSE while INFY covers NSE.
ORDER_TEST_OPTION_UNDERLYING = "SENSEX"


BROKER_MAP = {
    "dhan": Dhan,
    "fivepaisa": FivePaisa,
    "shoonya": Shoonya,
    "flattrade": FlatTrade,
    "icicidirect": IciciDirect,
}

GROUP_QUOTES = "quotes"
GROUP_HISTORICAL = "historical"
GROUP_ORDERS = "orders"
ALL_GROUPS = [GROUP_QUOTES, GROUP_HISTORICAL, GROUP_ORDERS]


def now_local() -> str:
    return dt.datetime.now().strftime(LOCAL_TIMESTAMP_FORMAT)


def sleep_quote_gap() -> None:
    time.sleep(QUOTE_SLEEP_SEC)


def round_tick(price: float, tick_size: float) -> float:
    return round(round(price / tick_size) * tick_size, 8)


def to_serializable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value
    if isinstance(value, HistoricalData):
        return {k: to_serializable(v) for k, v in value.to_dict().items()}
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, pd.Series):
        return value.tolist()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return {k: to_serializable(v) for k, v in value.__dict__.items()}
    if isinstance(value, dict):
        return {k: to_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_serializable(v) for v in value]
    return str(value)


class StepLogger:
    def __init__(self) -> None:
        self.steps: List[Dict[str, Any]] = []

    def call(self, function_name: str, fn, **context):
        request_ts = now_local()
        try:
            returned = fn()
            return_ts = now_local()
            self.steps.append(
                {
                    "function": function_name,
                    "request_ts": request_ts,
                    "return_ts": return_ts,
                    **context,
                    "returned": to_serializable(returned),
                }
            )
            return returned
        except Exception as exc:
            return_ts = now_local()
            self.steps.append(
                {
                    "function": function_name,
                    "request_ts": request_ts,
                    "return_ts": return_ts,
                    **context,
                    "returned": {
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    },
                }
            )
            raise


def instantiate_broker(broker_name: str) -> Any:
    broker_cls = BROKER_MAP.get(str(broker_name).lower())
    if broker_cls is None:
        supported = ", ".join(sorted(BROKER_MAP.keys()))
        raise ValueError(f"Unsupported broker: {broker_name}. Supported brokers: {supported}")
    return broker_cls()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run broker live tests by group.")
    parser.add_argument(
        "--broker",
        default=BROKER_NAME,
        choices=sorted(BROKER_MAP.keys()),
        help="Broker name to test.",
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        default=["all"],
        choices=ALL_GROUPS + ["all"],
        help="Test groups to run (default: all).",
    )
    return parser.parse_args()


def get_tick_size(broker: Any, symbol: str, exchange: str, default_tick: float = 0.05) -> float:
    exch_char = broker.map_exchange_for_api(symbol, exchange)
    return float(
        broker.exchange_mappings.get(exch_char, {}).get("contracttick_map", {}).get(symbol, default_tick) or default_tick
    )


def extract_positive_spot(quote: Dict[str, Any]) -> float:
    for key in ("last", "bid", "ask", "prior_close", "high", "low"):
        try:
            value = float(quote.get(key, float("nan")))
        except (TypeError, ValueError):
            continue
        if not math.isnan(value) and value > 0:
            return value
    return float("nan")


def select_nearest_index_call(broker: Any, underlying: str, exchange: str, spot: float) -> Optional[Dict[str, Any]]:
    pattern = re.compile(rf"^{re.escape(underlying)}_OPT_(\d{{8}})_CALL_([0-9]+(?:\.[0-9]+)?)$")
    rows: List[Tuple[str, dt.date, float]] = []
    today = dt.datetime.now().date()
    for symbol in broker.codes["long_symbol"].tolist():
        match = pattern.match(str(symbol))
        if not match:
            continue
        expiry_s, strike_s = match.groups()
        expiry = dt.datetime.strptime(expiry_s, "%Y%m%d").date()
        if expiry < today:
            continue
        rows.append((symbol, expiry, float(strike_s)))
    if not rows:
        return None
    nearest_expiry = min(x[1] for x in rows)
    nearest_rows = [x for x in rows if x[1] == nearest_expiry]
    best = min(nearest_rows, key=lambda row: (abs(row[2] - spot), row[2]))
    return {
        "symbol": best[0],
        "exchange": exchange,
        "expiry": best[1].strftime("%Y-%m-%d"),
        "strike": best[2],
    }


def find_reconciled_order_row(
    orders_today: pd.DataFrame,
    internal_id: str,
    symbol: str,
    request_ts: str,
) -> Optional[Dict[str, Any]]:
    rows = orders_today.copy()
    if internal_id:
        rows = rows[rows["internal_order_id"] == internal_id]
    else:
        rows = rows[rows["long_symbol"] == symbol]
    if len(rows) == 0:
        return None
    rows["order_time"] = pd.to_datetime(rows["order_time"], errors="coerce")
    candidate_rows = rows
    try:
        request_dt = pd.to_datetime(request_ts)
        timed_rows = rows[rows["order_time"] >= request_dt - pd.Timedelta(minutes=1)]
        if len(timed_rows) > 0:
            rows = timed_rows
    except Exception:
        pass
    if len(rows) == 0:
        rows = candidate_rows
    if len(rows) == 0:
        return None
    rows = rows.sort_values("order_time")
    row = rows.iloc[-1].to_dict()
    return {str(k): to_serializable(v) for k, v in row.items()}


def markdown_block(data: Any) -> str:
    return "```python\n" + json.dumps(to_serializable(data), indent=2, default=str) + "\n```"


def summarize_symbology(codes: pd.DataFrame) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "rows": int(len(codes)),
        "segments": {},
    }
    if "ExchSeg" not in codes.columns or "long_symbol" not in codes.columns:
        return summary
    for segment, group in codes.groupby("ExchSeg"):
        summary["segments"][str(segment)] = group["long_symbol"].astype(str).head(5).tolist()
    return summary


def summarize_historical(result: Dict[str, List[HistoricalData]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    for symbol, rows in result.items():
        head_rows = [to_serializable(x) for x in rows[:5]]
        tail_rows = [to_serializable(x) for x in rows[-5:]] if len(rows) > 5 else head_rows
        summary[symbol] = {
            "count": len(rows),
            "head": head_rows,
            "tail": tail_rows,
        }
    return summary


def write_report(report: Dict[str, Any]) -> Path:
    timestamp_slug = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    broker_slug = str(report.get("meta", {}).get("broker", BROKER_NAME)).upper()
    dest = REPO_ROOT / "docs" / f"{broker_slug}_LIVE_TEST_REPORT_{timestamp_slug}.md"

    lines: List[str] = []
    lines.append(f"# {broker_slug.title()} Live Test Report")
    lines.append("")
    lines.append(f"Start time: `{report['meta']['started_at']}`")
    lines.append(f"End time: `{report['meta']['finished_at']}`")
    lines.append(f"Broker: `{report['meta']['broker']}`")
    lines.append(f"Config: `{report['meta']['config_path']}`")
    lines.append(f"Redis DB for orders: `{report['meta']['redis_db_orders']}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("Verified:")
    for item in report["summary"]["verified"]:
        lines.append(f"- {item}")
    if report["summary"]["findings"]:
        lines.append("")
        lines.append("Findings:")
        for item in report["summary"]["findings"]:
            lines.append(f"- {item}")
    if report["summary"]["residual_risks"]:
        lines.append("")
        lines.append("Residual risks:")
        for item in report["summary"]["residual_risks"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## Step Log")
    lines.append("")
    for step in report["steps"]:
        title_parts = [step["function"]]
        if step.get("symbol"):
            title_parts.append(step["symbol"])
        lines.append(f"### {' | '.join(title_parts)}")
        lines.append("")
        lines.append(f"Request sent: `{step['request_ts']}`")
        lines.append(f"Function returned: `{step['return_ts']}`")
        for key in ("exchange", "strategy", "broker_order_id", "internal_order_id"):
            if step.get(key):
                lines.append(f"{key.replace('_', ' ').title()}: `{step[key]}`")
        lines.append("")
        lines.append("Return value:")
        lines.append(markdown_block(step["returned"]))
        lines.append("")
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def main() -> None:
    args = parse_args()
    selected_groups = ALL_GROUPS if "all" in args.groups else [g for g in ALL_GROUPS if g in args.groups]
    broker_name = str(args.broker).lower()

    if CONFIG_PATH:
        os.environ["TRADINGAPI_CONFIG_PATH"] = CONFIG_PATH

    broker = instantiate_broker(broker_name)
    steps = StepLogger()
    summary_verified: List[str] = []
    findings: List[str] = []
    residual_risks: List[str] = [
        "Unexpected fills remain theoretically possible even with deep-away limit prices."
    ]

    report: Dict[str, Any] = {
        "meta": {
            "started_at": now_local(),
            "finished_at": "",
            "broker": broker_name.upper(),
            "redis_db_orders": REDIS_DB,
            "config_path": CONFIG_PATH,
            "groups_requested": list(args.groups),
            "groups_executed": selected_groups,
        },
        "steps": steps.steps,
        "summary": {
            "verified": summary_verified,
            "findings": findings,
            "residual_risks": residual_risks,
        },
    }

    steps.call("connect", lambda: broker.connect(redis_db=REDIS_DB))
    connected = steps.call("is_connected", broker.is_connected)
    if connected:
        summary_verified.append("connect / is_connected")
    try:
        codes = steps.call("update_symbology", broker.update_symbology)
        if steps.steps:
            steps.steps[-1]["returned"] = summarize_symbology(codes)
        summary_verified.append("symbology")
    except Exception as exc:
        findings.append(f"symbology failed: {type(exc).__name__}: {exc}")
        report["meta"]["finished_at"] = now_local()
        report_path = write_report(report)
        print(json.dumps({"report_path": str(report_path), "error": str(exc)}, indent=2))
        return

    selected_options: Dict[str, Dict[str, Any]] = {}
    if GROUP_QUOTES in selected_groups:
        core_quotes: Dict[str, Dict[str, Any]] = {}
        for key, (symbol, exchange) in CORE_SYMBOLS.items():
            try:
                quote = steps.call(
                    "get_quote",
                    lambda s=symbol, e=exchange: broker.get_quote(s, exchange=e),
                    symbol=symbol,
                    exchange=exchange,
                )
                core_quotes[key] = to_serializable(quote)
                summary_verified.append(f"quote {symbol}")
            except Exception as exc:
                findings.append(f"quote failed for {symbol}: {type(exc).__name__}: {exc}")
            sleep_quote_gap()

        for underlying, exchange in INDEX_OPTION_RULES:
            quote_key = "index_nse" if underlying == "NIFTY" else "index_bse"
            spot = extract_positive_spot(core_quotes.get(quote_key, {}))
            if math.isnan(spot):
                findings.append(f"cannot select {underlying} option because spot quote is unavailable")
                continue
            selected = select_nearest_index_call(broker, underlying, exchange, spot)
            if selected is None:
                findings.append(f"no option found for {underlying}")
                continue
            try:
                opt_quote = steps.call(
                    "get_quote",
                    lambda s=selected["symbol"], e=exchange: broker.get_quote(s, exchange=e),
                    symbol=selected["symbol"],
                    exchange=exchange,
                )
                selected["quote"] = to_serializable(opt_quote)
                selected["lot"] = broker.get_min_lot_size(selected["symbol"], exchange)
                selected_options[underlying] = selected
                summary_verified.append(f"selected option {selected['symbol']}")
            except Exception as exc:
                findings.append(f"option quote failed for {selected['symbol']}: {type(exc).__name__}: {exc}")
            sleep_quote_gap()
    report["meta"]["selected_options"] = selected_options

    if GROUP_HISTORICAL in selected_groups:
        historical_symbols = [
            ("INFY_STK___", "NSE"),
            ("NIFTY_IND___", "NSE"),
            ("SENSEX_IND___", "BSE"),
        ]
        for selected in selected_options.values():
            historical_symbols.append((selected["symbol"], selected["exchange"]))
        for symbol, exchange in historical_symbols:
            try:
                daily = steps.call(
                    "get_historical",
                    lambda s=symbol, e=exchange: broker.get_historical(
                        s,
                        date_start=dt.datetime.now() - dt.timedelta(days=5),
                        date_end=dt.datetime.now(),
                        exchange=e,
                        periodicity="1d",
                    ),
                    symbol=symbol,
                    exchange=exchange,
                )
                if steps.steps:
                    steps.steps[-1]["returned"] = summarize_historical(daily)
                summary_verified.append(f"historical {symbol}")
                sleep_quote_gap()
                intraday = steps.call(
                    "get_historical",
                    lambda s=symbol, e=exchange: broker.get_historical(
                        s,
                        date_start=dt.datetime.now() - dt.timedelta(days=1),
                        date_end=dt.datetime.now(),
                        exchange=e,
                        periodicity="1m",
                    ),
                    symbol=symbol,
                    exchange=exchange,
                )
                if steps.steps:
                    steps.steps[-1]["returned"] = summarize_historical(intraday)
                _ = daily, intraday
            except Exception as exc:
                findings.append(f"historical failed for {symbol}: {type(exc).__name__}: {exc}")

    if GROUP_ORDERS in selected_groups:
        order_test_targets: List[Tuple[str, str, int, str]] = [
            (ORDER_TEST_EQUITY[0], ORDER_TEST_EQUITY[1], ORDER_TEST_EQUITY[2], f"{ORDER_STRATEGY_PREFIX}-infy")
        ]
        selected_order_option = selected_options.get(ORDER_TEST_OPTION_UNDERLYING)
        if selected_order_option is not None:
            order_test_targets.append(
                (
                    selected_order_option["symbol"],
                    selected_order_option["exchange"],
                    int(selected_order_option["lot"]),
                    f"{ORDER_STRATEGY_PREFIX}-{ORDER_TEST_OPTION_UNDERLYING.lower()}",
                )
            )

        for symbol, exchange, lot, strategy in order_test_targets:
            steps.steps.append(
                {
                    "function": "order_lifecycle_start",
                    "request_ts": now_local(),
                    "return_ts": now_local(),
                    "symbol": symbol,
                    "exchange": exchange,
                    "strategy": strategy,
                    "returned": {
                        "note": "Starting full lifecycle for this symbol before moving to the next symbol."
                    },
                }
            )
            place_request_ts = now_local()
            try:
                returned = steps.call(
                    "place_combo_order",
                    lambda s=strategy, sym=symbol, ex=exchange, qty=lot: place_combo_order(
                        execution_broker=broker,
                        strategy=s,
                        symbols=[sym],
                        quantities=[qty],
                        entry=True,
                        exchanges=[ex],
                        price_broker=[broker],
                        price_types=["LMT*0.90"],
                        paper=False,
                    ),
                    symbol=symbol,
                    exchange=exchange,
                    strategy=strategy,
                )
                internal_id = returned.get(symbol, "") if isinstance(returned, dict) else ""
            except Exception as exc:
                findings.append(f"order flow failed for {symbol}: {type(exc).__name__}: {exc}")
                internal_id = ""
                returned = {}
            time.sleep(POST_ORDER_SLEEP_SEC)
            try:
                orders_today = steps.call("get_orders_today", broker.get_orders_today, symbol=symbol, strategy=strategy)
                row = find_reconciled_order_row(orders_today, internal_id, symbol, place_request_ts)
                if row:
                    steps.steps.append(
                        {
                            "function": "orders_today_reconcile",
                            "request_ts": now_local(),
                            "return_ts": now_local(),
                            "symbol": symbol,
                            "exchange": exchange,
                            "strategy": strategy,
                            "internal_order_id": row.get("internal_order_id", internal_id),
                            "broker_order_id": row.get("broker_order_id", ""),
                            "returned": row,
                        }
                    )
                    broker_order_id = str(row.get("broker_order_id", "") or "")
                    if broker_order_id:
                        try:
                            info = steps.call(
                                "get_order_info",
                                lambda oid=broker_order_id: broker.get_order_info(broker_order_id=oid),
                                symbol=symbol,
                                broker_order_id=broker_order_id,
                                internal_order_id=row.get("internal_order_id", internal_id),
                            )
                            status_name = getattr(info.status, "name", str(info.status))
                            summary_verified.append(f"independent get_order_info {symbol} -> {status_name}")
                            if status_name in ("PENDING", "OPEN", "UNDEFINED"):
                                new_price = round(float(info.order_price) + get_tick_size(broker, symbol, exchange), 8)
                                steps.call(
                                    "modify_order",
                                    lambda oid=broker_order_id, qty=lot, px=new_price: broker.modify_order(
                                        broker_order_id=oid,
                                        new_price=px,
                                        new_quantity=qty,
                                    ),
                                    symbol=symbol,
                                    broker_order_id=broker_order_id,
                                    internal_order_id=row.get("internal_order_id", internal_id),
                                )
                                time.sleep(POST_MODIFY_SLEEP_SEC)
                                steps.call(
                                    "get_order_info",
                                    lambda oid=broker_order_id: broker.get_order_info(broker_order_id=oid),
                                    symbol=symbol,
                                    broker_order_id=broker_order_id,
                                    internal_order_id=row.get("internal_order_id", internal_id),
                                )
                                steps.call(
                                    "cancel_order",
                                    lambda oid=broker_order_id: broker.cancel_order(broker_order_id=oid),
                                    symbol=symbol,
                                    broker_order_id=broker_order_id,
                                    internal_order_id=row.get("internal_order_id", internal_id),
                                )
                                time.sleep(POST_CANCEL_SLEEP_SEC)
                                steps.call(
                                    "get_order_info",
                                    lambda oid=broker_order_id: broker.get_order_info(broker_order_id=oid),
                                    symbol=symbol,
                                    broker_order_id=broker_order_id,
                                    internal_order_id=row.get("internal_order_id", internal_id),
                                )
                        except Exception as exc:
                            findings.append(f"order reconciliation failed for {symbol}: {type(exc).__name__}: {exc}")
                else:
                    findings.append(f"no reconciled order row found for {symbol}")
            except Exception as exc:
                findings.append(f"get_orders_today failed during reconciliation for {symbol}: {type(exc).__name__}: {exc}")
                steps.steps.append(
                    {
                        "function": "order_lifecycle_end",
                        "request_ts": now_local(),
                        "return_ts": now_local(),
                        "symbol": symbol,
                        "exchange": exchange,
                        "strategy": strategy,
                        "returned": {
                            "note": "Completed lifecycle processing for this symbol."
                        },
                    }
                )

    if GROUP_QUOTES in selected_groups:
        try:
            steps.call("get_position", broker.get_position)
            steps.call("get_trades_today", broker.get_trades_today)
            steps.call("get_available_capital", broker.get_available_capital)
            identifier_pairs: List[Tuple[Any, str, str]] = []
            candidate_pairs: List[Tuple[str, str]] = [
                (CORE_SYMBOLS["equity_nse"][0], "NSE"),
                (CORE_SYMBOLS["index_nse"][0], "NSE"),
                (CORE_SYMBOLS["index_bse"][0], "BSE"),
                (ORDER_TEST_EQUITY[0], ORDER_TEST_EQUITY[1]),
            ] + [(x["symbol"], x["exchange"]) for x in selected_options.values()]

            seen_symbol_exch: set[Tuple[str, str]] = set()
            for symbol, exchange in candidate_pairs:
                key = (str(symbol), str(exchange).upper())
                if key in seen_symbol_exch:
                    continue
                seen_symbol_exch.add(key)
                try:
                    mapped_ex = broker.map_exchange_for_api(symbol, exchange)
                    broker_id = broker.exchange_mappings.get(mapped_ex, {}).get("symbol_map", {}).get(symbol)
                    if broker_id is None:
                        continue
                    ex_char = "B" if str(mapped_ex).upper().startswith("B") else "N"
                    identifier_pairs.append((broker_id, ex_char, symbol))
                except Exception:
                    continue

            if identifier_pairs:
                resolved = steps.call(
                    "get_long_name_from_broker_identifier",
                    lambda ids=identifier_pairs: broker.get_long_name_from_broker_identifier(
                        Scripcode=pd.Series([x[0] for x in ids]),
                        Exchange=pd.Series([x[1] for x in ids]),
                    ),
                    symbols=[x[2] for x in identifier_pairs],
                )
                resolved_list = (
                    resolved.astype(str).tolist() if isinstance(resolved, pd.Series) else list(to_serializable(resolved) or [])
                )
                expected_list = [x[2] for x in identifier_pairs]
                steps.steps.append(
                    {
                        "function": "get_long_name_from_broker_identifier_reconcile",
                        "request_ts": now_local(),
                        "return_ts": now_local(),
                        "returned": {
                            "input_symbols": expected_list,
                            "resolved_symbols": resolved_list,
                            "all_match": resolved_list == expected_list,
                        },
                    }
                )
            summary_verified.append("day-state methods")
        except Exception as exc:
            findings.append(f"day-state verification failed: {type(exc).__name__}: {exc}")

        stream_pairs = [
            (CORE_SYMBOLS["equity_nse"][0], "NSE"),
            (CORE_SYMBOLS["index_nse"][0], "NSE"),
            (CORE_SYMBOLS["index_bse"][0], "BSE"),
        ] + [(x["symbol"], x["exchange"]) for x in selected_options.values()]
        stream_symbols = [symbol for symbol, _ in stream_pairs]
        stream_seen: Dict[str, Any] = {}
        stream_samples: Dict[str, List[Dict[str, Any]]] = {}
        done = threading.Event()

        def callback(price):
            symbol = str(getattr(price, "symbol", "") or "")
            if not symbol:
                return
            serialized = to_serializable(price)
            stream_seen[symbol] = serialized
            samples = stream_samples.setdefault(symbol, [])
            if len(samples) < 5:
                samples.append({"ts": now_local(), "value": serialized})
            if len(stream_seen) >= len(stream_symbols):
                done.set()

        try:
            by_exchange: Dict[str, List[str]] = {}
            for symbol, exchange in stream_pairs:
                by_exchange.setdefault(exchange, []).append(symbol)
            for exchange, symbols in by_exchange.items():
                steps.call(
                    "start_quotes_streaming",
                    lambda ex=exchange, syms=symbols: (
                        broker.start_quotes_streaming("s", syms, ext_callback=callback, exchange=ex),
                        {"exchange": ex, "symbols": syms, "status": "started"},
                    )[1],
                    exchange=exchange,
                    symbols=symbols,
                )
            done.wait(STREAM_WAIT_SEC)
            time.sleep(STREAM_SETTLE_SEC)
            steps.call("stop_streaming", broker.stop_streaming)
            steps.steps.append(
                {
                    "function": "stream_reconcile",
                    "request_ts": now_local(),
                    "return_ts": now_local(),
                    "returned": {
                        "requested_symbols": stream_symbols,
                        "received_symbols": sorted(stream_seen.keys()),
                        "latest_by_symbol": stream_seen,
                        "samples_until_stop": stream_samples,
                    },
                }
            )
            summary_verified.append("streaming")
        except Exception as exc:
            findings.append(f"streaming failed: {type(exc).__name__}: {exc}")

    steps.call("disconnect", broker.disconnect)
    summary_verified.append("disconnect")

    report["meta"]["finished_at"] = now_local()
    report_path = write_report(report)
    print(json.dumps({"report_path": str(report_path), "findings": findings}, indent=2))


if __name__ == "__main__":
    main()
