import asyncio
import pathlib
import sys
from decimal import Decimal

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from spot_runtime import gate as gate_runtime
from spot_runtime import mexc as mexc_runtime


class _DummyResponse:
    def __init__(self, status_code: int = 200, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class _StaticSession:
    def __init__(self, response: _DummyResponse):
        self._response = response

    def get(self, *args, **kwargs):
        return self._response


def test_gate_numeric_helpers_fall_back_cleanly():
    assert gate_runtime._coerce_float("12.5") == 12.5
    assert gate_runtime._coerce_float("bad") is None
    assert gate_runtime._coerce_int("4.0") == 4
    assert gate_runtime._coerce_int("bad") is None
    assert gate_runtime._coerce_decimal("7.5") == Decimal("7.5")
    assert gate_runtime._coerce_decimal("bad") is None
    assert gate_runtime._rate_limit_sleep_from_headers({"X-Gate-RateLimit-Reset-Timestamp": "bad"}, 0.25) == 0.25


def test_gate_preload_balance_background_ignores_non_list_payload(monkeypatch):
    monkeypatch.setattr(gate_runtime.GateUltraTrader, "_warm_up", lambda self: True)
    trader = gate_runtime.GateUltraTrader(test_mode=True)
    trader.session = _StaticSession(_DummyResponse(status_code=200, payload={"balances": "oops"}))

    trader._preload_balance_background()

    assert trader._balance_cache == {}
    assert trader._balance_init_done is False


def test_mexc_get_symbol_info_handles_invalid_payload(monkeypatch):
    monkeypatch.setattr(mexc_runtime.MexcHyperbeastTrader, "_warm_up", lambda self: True)
    trader = mexc_runtime.MexcHyperbeastTrader(test_mode=True)
    trader.session = _StaticSession(_DummyResponse(status_code=200, payload={"symbols": "oops"}))

    result = asyncio.run(trader.get_symbol_info("BTC"))

    assert result is None
    assert "BTC" not in trader._symbol_info_cache


def test_mexc_balance_fetch_handles_invalid_balances_payload(monkeypatch):
    monkeypatch.setattr(mexc_runtime.MexcHyperbeastTrader, "_warm_up", lambda self: True)
    trader = mexc_runtime.MexcHyperbeastTrader(test_mode=False)
    trader.session = _StaticSession(_DummyResponse(status_code=200, payload={"balances": "oops"}))
    monkeypatch.setattr(trader.time_cache, "get_time", lambda: 1234567890)

    balance = trader.get_balance(force=True)

    assert balance == 0.0


def test_mexc_precision_and_quote_constraints_ignore_bad_values(monkeypatch):
    monkeypatch.setattr(mexc_runtime.MexcHyperbeastTrader, "_warm_up", lambda self: True)
    trader = mexc_runtime.MexcHyperbeastTrader(test_mode=True)

    symbol_info = {
        "quantityPrecision": "bad",
        "basePrecision": "4.0",
        "quotePrecision": "bad",
        "quoteAmountPrecision": "5.5",
        "filters": [{"filterType": "MIN_NOTIONAL", "minNotional": "bad"}],
    }

    assert trader.parse_mexc_precision(symbol_info) == 4
    quote_precision, min_notional = trader._extract_quote_constraints(symbol_info)
    assert quote_precision == 2
    assert min_notional == 5.5
    assert trader.format_quantity_for_mexc("bad", 6) is None
