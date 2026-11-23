from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from tradingapi.fivepaisa import FivePaisa


@pytest.mark.parametrize("status, content", [
    (404, "404 page not found"), (200, "not json"),
    (200, '{"body":{"Message":"Invalid session"}}'),
])
def test_unavailable_trade_info_returns_none_for_orderbook_fallback(status, content):
    response = httpx.Response(status, text=content, request=httpx.Request("POST", "https://example.test/TradeInformation"))
    api = SimpleNamespace(
        USER_KEY="secret", client_code="client", access_token="token", Jwt_token="",
        APIUID="uid", TRADE_INFO_ROUTE="https://example.test/TradeInformation",
        session=Mock(),
    )
    api.session.post.return_value = response
    broker = SimpleNamespace(api=api)
    assert FivePaisa._fetch_trade_info(broker, []) is None


def test_trade_info_preserves_partial_execution_details_and_uses_local_payload():
    body = {"TradeDetail": [{"Qty": 65, "Rate": 12.5}], "Status": "Pending"}
    api = SimpleNamespace(
        USER_KEY="secret", client_code="client", access_token="token", Jwt_token="",
        APIUID="uid", TRADE_INFO_ROUTE="https://example.test/TradeInformation",
        session=Mock(), payload={"body": {"unrelated": "preserve"}},
    )
    api.session.post.return_value = httpx.Response(
        200, json={"body": body}, request=httpx.Request("POST", api.TRADE_INFO_ROUTE),
    )
    request = [{"ExchOrderID": "123", "Exch": "N", "ExchType": "D", "ScripCode": 1}]
    assert FivePaisa._fetch_trade_info(SimpleNamespace(api=api), request) == body
    assert api.payload == {"body": {"unrelated": "preserve"}}
    assert api.session.post.call_args.kwargs["json"]["body"]["TradeInformationList"] == request
