import asyncio
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import trader


class _ForbiddenSession:
    def get(self, *args, **kwargs):
        raise AssertionError("network access should not happen in this test")

    def post(self, *args, **kwargs):
        raise AssertionError("network access should not happen in this test")


class _AsyncTextResponse:
    def __init__(self, *, status: int, text: str):
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _JsonErrorSession:
    def __init__(self, *, status: int = 502, text: str = "not-json"):
        self.status = status
        self.text = text

    def get(self, *args, **kwargs):
        return _AsyncTextResponse(status=self.status, text=self.text)


def test_mexc_test_mode_exchange_info_skips_network():
    api = trader.MEXCAPI(
        api_key="key",
        api_secret="secret",
        session=_ForbiddenSession(),
        test_mode=True,
    )

    result = asyncio.run(api.get_exchange_info("BTCUSDT"))

    assert result is not None
    assert result["symbol"] == "BTCUSDT"
    lot_filter = api.get_lot_size_filter(result)
    assert lot_filter is not None
    assert lot_filter["stepSize"] == 0.000001


def test_mexc_format_quantity_invalid_input_returns_zero():
    api = trader.MEXCAPI(
        api_key="key",
        api_secret="secret",
        session=_ForbiddenSession(),
        test_mode=True,
    )

    assert api.format_quantity("not-a-number", 6) == "0"
    assert api.format_quantity(float("nan"), 6) == "0"


def test_mexc_send_request_invalid_json_returns_parse_error():
    api = trader.MEXCAPI(
        api_key="key",
        api_secret="secret",
        session=_JsonErrorSession(status=502, text="gateway exploded"),
        test_mode=False,
    )

    result = asyncio.run(api._send_request("GET", "/api/v3/time"))

    assert result["error"] == "Invalid JSON response"
    assert "gateway exploded" in result["raw"]
