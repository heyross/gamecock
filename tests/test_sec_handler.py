from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gamecock.sec_handler import SECHandler


def make_response(json_data=None, status=200, text=""):
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


@pytest.fixture()
def handler():
    return SECHandler()


def test_get_company_info_by_ticker_exact_with_exchange(handler, monkeypatch):
    # First request returns tickers mapping containing exact ticker match
    tickers = {
        "0": {"title": "TestCo Inc", "ticker": "TST", "cik_str": 12345}
    }
    # Second request returns exchange data with matching CIK at index 2 and exchange at index 3
    exchange = {
        "data": [
            ["x", "y", 12345, "NASDAQ"],
        ]
    }

    calls = []

    def fake_make_request(url):
        calls.append(url)
        if url.endswith("company_tickers.json"):
            return make_response(tickers)
        elif url.endswith("company_tickers_exchange.json"):
            return make_response(exchange)
        else:
            return None

    monkeypatch.setattr(handler, "_make_request", fake_make_request)

    info = handler.get_company_info("tst")

    assert info is not None
    assert info.name == "TestCo Inc"
    assert info.primary_identifiers.cik == str(12345).zfill(10)
    assert info.primary_identifiers.tickers[0]["symbol"] == "TST"
    assert info.primary_identifiers.tickers[0]["exchange"] == "NASDAQ"


def test_get_company_info_by_name_substring_default_exchange(handler, monkeypatch):
    tickers = {
        "0": {"title": "Alpha Beta Corp", "ticker": "ABC", "cik_str": 999}
    }
    # No exchange data available
    exchange = {"data": []}

    def fake_make_request(url):
        if url.endswith("company_tickers.json"):
            return make_response(tickers)
        elif url.endswith("company_tickers_exchange.json"):
            return make_response(exchange)
        return None

    monkeypatch.setattr(handler, "_make_request", fake_make_request)

    info = handler.get_company_info("beta")  # substring of title

    assert info is not None
    assert info.primary_identifiers.tickers[0]["exchange"] == "NYSE"  # default


def test_get_company_info_no_match_returns_none(handler, monkeypatch):
    tickers = {"0": {"title": "Foo Inc", "ticker": "FOO", "cik_str": 1}}

    def fake_make_request(url):
        if url.endswith("company_tickers.json"):
            return make_response(tickers)
        return None

    monkeypatch.setattr(handler, "_make_request", fake_make_request)

    assert handler.get_company_info("BAR") is None


def test_get_company_info_request_failure_returns_none(handler, monkeypatch):
    # First call returns None simulating http failure
    monkeypatch.setattr(handler, "_make_request", lambda url: None)
    assert handler.get_company_info("ANY") is None


def test_make_request_non_200_and_rate_limit(monkeypatch):
    h = SECHandler()
    # Spy on rate limiter acquire
    h.rate_limiter.acquire = MagicMock()

    class FakeClient:
        def __init__(self, *a, **k):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def get(self, url, headers=None):
            # Return non-200
            return make_response(status=429, text="Too Many Requests")

    monkeypatch.setattr("gamecock.sec_handler.httpx.Client", FakeClient)

    resp = h._make_request("https://example.com")

    h.rate_limiter.acquire.assert_called_once()
    assert resp is None


def test_make_request_exception_returns_none(monkeypatch):
    h = SECHandler()
    h.rate_limiter.acquire = MagicMock()

    class FakeClient:
        def __init__(self, *a, **k):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def get(self, url, headers=None):
            raise RuntimeError("boom")

    monkeypatch.setattr("gamecock.sec_handler.httpx.Client", FakeClient)

    resp = h._make_request("https://example.com")
    assert resp is None
