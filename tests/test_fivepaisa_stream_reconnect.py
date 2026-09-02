import json
import sys
import threading
import time
from unittest.mock import patch

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi.broker_base import Brokers
from tradingapi.fivepaisa import FivePaisa
from tradingapi import fivepaisa as fivepaisa_module


class _SocketState:
    connected = True


class _WebSocket:
    def __init__(self, sent_requests):
        self.sock = _SocketState()
        self.on_open = None
        self.sent_requests = sent_requests

    def send(self, payload):
        self.sent_requests.append(json.loads(payload))


class _DelayedStreamingApi:
    def __init__(self, connect_started, allow_connect, stop_stream):
        self.ws = None
        self.connect_started = connect_started
        self.allow_connect = allow_connect
        self.stop_stream = stop_stream
        self.connect_calls = 0
        self.sent_requests = []

    def Request_Feed(self, feed_type, operation, symbols):
        return {"feed": feed_type, "operation": operation, "symbols": symbols}

    def connect(self, _request):
        self.connect_calls += 1
        self.connect_started.set()
        assert self.allow_connect.wait(timeout=2)
        self.ws = _WebSocket(self.sent_requests)

    def error_data(self, callback):
        self.error_callback = callback

    def receive_data(self, callback):
        callback(self.ws, json.dumps({"Token": 1, "LastRate": 100.0}))
        self.stop_stream.wait(timeout=2)

    def close_data(self):
        if self.ws is not None:
            self.ws.sock.connected = False


def _make_broker(api, fresh_login):
    broker = object.__new__(FivePaisa)
    broker.api = api
    broker.broker = Brokers.FIVEPAISA
    broker.account_key = broker.broker.name
    broker.subscribe_thread = None
    broker.subscribed_symbols = []
    broker._stream_reconnect_lock = threading.Lock()
    broker._stream_reconnect_serial_lock = threading.Lock()
    broker._stream_subscriptions_lock = threading.Lock()
    broker._stream_reconnect_active = False
    broker._suppress_stream_reconnect = False
    broker._last_stream_tick_ts = time.time() - 1
    broker._fp_susertoken_path = "/tmp/fivepaisa-token"
    broker._fp_restore_session_from_token = lambda _path: True
    broker._fp_fresh_login = fresh_login
    broker._wait_for_stream_request_rate_limit = lambda: None
    broker.map_exchange_for_api = lambda _symbol, _exchange: "N"
    broker.map_exchange_for_db = lambda _symbol, exchange: exchange
    broker.exchange_mappings = {
        "N": {
            "symbol_map": {"TEST": 1, "TEST2": 2},
            "symbol_map_reversed": {1: "TEST", 2: "TEST2"},
            "exchangetype_map": {"TEST": "C", "TEST2": "C"},
        }
    }
    return broker


def test_reconnect_waits_for_startup_and_coalesces_queued_callers():
    connect_started = threading.Event()
    allow_connect = threading.Event()
    stop_stream = threading.Event()
    api = _DelayedStreamingApi(connect_started, allow_connect, stop_stream)
    fresh_login_calls = []
    broker = _make_broker(api, lambda path: fresh_login_calls.append(path))
    errors = []

    def start_stream(symbol):
        try:
            broker.start_quotes_streaming("s", [symbol], exchange="NSE")
        except Exception as exc:
            errors.append(exc)

    first_caller = threading.Thread(target=start_stream, args=("TEST",))
    second_caller = threading.Thread(target=start_stream, args=("TEST2",))
    with patch.object(fivepaisa_module.config, "get", return_value="/tmp/fivepaisa-token"):
        first_caller.start()
        assert connect_started.wait(timeout=2)
        second_caller.start()
        time.sleep(0.05)
        allow_connect.set()
        first_caller.join(timeout=2)
        second_caller.join(timeout=2)
        broker.start_quotes_streaming("s", ["TEST"], exchange="NSE")

    stop_stream.set()
    if broker.subscribe_thread is not None:
        broker.subscribe_thread.join(timeout=2)

    assert not first_caller.is_alive()
    assert not second_caller.is_alive()
    assert not errors
    assert api.connect_calls == 1
    assert fresh_login_calls == ["/tmp/fivepaisa-token"]
    assert broker.subscribed_symbols == ["TEST", "TEST2"]
    assert api.sent_requests == [
        {
            "feed": "mf",
            "operation": "s",
            "symbols": [{"Exch": "N", "ExchType": "C", "ScripCode": 2}],
        },
        {
            "feed": "mf",
            "operation": "s",
            "symbols": [{"Exch": "N", "ExchType": "C", "ScripCode": 1}],
        },
    ]
