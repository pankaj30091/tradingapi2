import sys
import threading
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi.broker_base import Brokers
from tradingapi.fivepaisa import FivePaisa


class _HistoricalApi:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def historical_data(self, *_args):
        self.calls += 1
        return self.responses.pop(0)


def _make_broker(responses):
    broker = object.__new__(FivePaisa)
    broker.api = _HistoricalApi(responses)
    broker.broker = Brokers.FIVEPAISA
    broker.exchange_mappings = {
        "N": {
            "symbol_map": {"NIFTY_IND___": 999920000},
            "exchangetype_map": {"NIFTY_IND___": "C"},
        }
    }
    broker.map_exchange_for_api = lambda _symbol, _exchange: "N"
    broker._configure_api_proxy_session = lambda: None
    broker._wait_for_historical_rate_limit = lambda: None
    broker._session_refresh_lock = threading.RLock()
    broker._session_generation = 1
    broker.redis_o = SimpleNamespace(connection_pool=SimpleNamespace(connection_kwargs={"db": 8}))
    return broker


def test_historical_none_retries_current_session_before_reconnect():
    broker = _make_broker([None, pd.DataFrame()])
    connect_calls = []
    broker.connect = lambda redis_db: connect_calls.append(redis_db)

    result = broker.get_historical(
        "NIFTY_IND___",
        date_start="2026-07-30",
        date_end="2026-07-30",
        exchange="N",
    )

    assert result == {"NSENIFTY_IND___": []}
    assert broker.api.calls == 2
    assert connect_calls == []


def test_repeated_historical_none_refreshes_session_once():
    broker = _make_broker([None, None, pd.DataFrame()])
    connect_calls = []

    def connect(redis_db):
        connect_calls.append(redis_db)
        broker._session_generation += 1

    broker.connect = connect

    result = broker.get_historical(
        "NIFTY_IND___",
        date_start="2026-07-30",
        date_end="2026-07-30",
        exchange="N",
    )

    assert result == {"NSENIFTY_IND___": []}
    assert broker.api.calls == 3
    assert connect_calls == [8]
