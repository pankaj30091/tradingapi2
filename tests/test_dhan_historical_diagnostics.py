from types import SimpleNamespace

from tradingapi.dhan import (
    Dhan,
    _extract_dhan_error_details,
    _is_dhan_auth_error,
    _is_dhan_no_data_error,
)


class _Response:
    status_code = 502
    text = "upstream unavailable"


class _Session:
    def post(self, *args, **kwargs):
        return _Response()


def test_historical_failure_includes_http_diagnostics():
    broker = Dhan.__new__(Dhan)
    broker.api = SimpleNamespace(
        base_url="https://example.test",
        header={},
        timeout=1,
        session=_Session(),
        _parse_response=lambda response: {
            "status": "failure",
            "remarks": "Expecting value: line 1 column 1 (char 0)",
            "data": "",
        },
    )
    broker._wait_for_historical_rate_limit = lambda: None
    broker._audit_dhan_call = lambda *args, **kwargs: None

    result = broker._fetch_dhan_historical(
        security_id=123,
        exchange_segment="NSE_FNO",
        instrument_type="OPTSTK",
        from_date="2026-10-06",
        to_date="2026-10-06",
        periodicity="1m",
        include_oi=True,
        symbol="TEST_OPT_20261027_CALL_100",
    )

    details = _extract_dhan_error_details(result)
    assert details["http_status"] == 502
    assert details["response_body"] == "upstream unavailable"


def test_dhan_907_is_expected_no_data():
    details = _extract_dhan_error_details(
        {"status": "failure", "remarks": {"error_code": "DH-907"}}
    )

    assert _is_dhan_no_data_error(details)


def test_auth_error_refreshes_token_and_retries_order():
    auth_failure = {
        "status": "failure",
        "remarks": {
            "error_code": "DH-901",
            "error_type": "Invalid_Authentication",
            "error_message": "Client ID or user generated access token is invalid or expired.",
        },
    }
    calls = []

    class _Api:
        def place_order(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return auth_failure
            return {"status": "success", "data": {"orderId": "1"}}

    broker = Dhan.__new__(Dhan)
    broker.api = _Api()
    broker.account_key = "DHAN"
    refreshed = []
    broker._refresh_access_token = lambda: refreshed.append(True) or setattr(broker, "api", _Api())

    assert _is_dhan_auth_error(_extract_dhan_error_details(auth_failure))
    out = broker._submit_order({"security_id": "1", "quantity": 20, "amo_time": None})
    assert out["status"] == "success"
    assert len(calls) == 2
    assert refreshed == [True]
