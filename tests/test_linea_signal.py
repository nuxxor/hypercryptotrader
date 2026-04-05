#!/usr/bin/env python3
"""Quick test harness to simulate a Binance LINEA listing signal.

The real UltraTrader wiring requires live exchange connectivity. For an
offline sanity check we stub the exchange traders and websocket clients so we
can observe the control flow that would normally fan out to MEXC and Gate.
"""

import asyncio
import json
import pathlib
import sys
import time
import types
from typing import Any, Dict, List

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Provide lightweight stand-ins when optional deps are unavailable.
try:  # pragma: no cover - only used in bare test environments
    import psutil as _psutil  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    def _virtual_memory():
        return types.SimpleNamespace(total=16 * 1024**3, percent=12.0)

    class _PsutilProcess:
        def __init__(self, _pid: int | None = None):
            self._rss = 150 * 1024 * 1024

        def cpu_affinity(self, _cores: List[int]):
            return None

        def memory_info(self):
            return types.SimpleNamespace(rss=self._rss)

    _psutil = types.SimpleNamespace(
        virtual_memory=_virtual_memory,
        cpu_count=lambda logical=True: 8,
        Process=_PsutilProcess,
    )
    sys.modules['psutil'] = _psutil

try:  # pragma: no cover
    import colorama as _colorama  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    colorama_stub = types.ModuleType("colorama")

    class _DummyFore:
        GREEN = CYAN = YELLOW = ""

    class _DummyStyle:
        RESET_ALL = ""

    colorama_stub.Fore = _DummyFore
    colorama_stub.Style = _DummyStyle
    colorama_stub.init = lambda: None
    sys.modules['colorama'] = colorama_stub
    _colorama = colorama_stub

import spot_binance


class _DummyResponse:
    """Lightweight stand-in for requests.Response."""

    def __init__(self, status_code: int = 200, payload: Dict[str, Any] | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self) -> Dict[str, Any]:
        return self._payload


class _DummySession:
    """Session wrapper that returns canned responses without network calls."""

    def __init__(self):
        self.headers: Dict[str, str] = {}

    def update(self, headers: Dict[str, str]):
        self.headers.update(headers)

    def mount(self, *_args, **_kwargs):
        return None

    def get(self, url: str, timeout: float | tuple[float, float] = 0.3) -> _DummyResponse:  # noqa: D401
        if "ticker/price" in url:
            return _DummyResponse(200, {"price": "1"})
        if "api/v3/time" in url:
            return _DummyResponse(200, {"serverTime": int(time.time() * 1000)})
        if "spot/time" in url:
            return _DummyResponse(200, {"server_time": int(time.time())})
        return _DummyResponse(200, {})

    def post(self, url: str, *args, **kwargs) -> _DummyResponse:  # noqa: D401
        return _DummyResponse(200, {"url": url, "args": args, "kwargs": kwargs})


class _DummyWebSocket:
    """Capture outbound websocket messages for assertions."""

    def __init__(self):
        self.connected = True
        self.messages: List[Dict[str, Any]] = []

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def send_message(self, message: Dict[str, Any]) -> bool:
        self.messages.append(message)
        return True


class _DummyBinanceTrader:
    """No-op Binance trader. We do not trade on Binance for this signal."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.accounts = [{"name": "primary", "enabled": True}]

    def get_all_balances(self, force: bool = False) -> Dict[str, float]:  # noqa: D401
        return {"primary": 1_000.0}

    async def execute_trade(self, symbol: str, amount: float, preparation_data: Any | None = None) -> Dict[str, Any]:  # noqa: D401
        await asyncio.sleep(0)
        now = int(time.time() * 1000)
        return {
            "multi_account_results": [
                {
                    "success": True,
                    "exchange": "binance",
                    "account": "primary",
                    "symbol": symbol,
                    "amount": amount,
                    "order_result": {"orderId": f"BN-{symbol}-{now}"},
                    "execution_time": 0.001,
                }
            ]
        }


class _DummyMexcTrader:
    def __init__(self, test_mode: bool = False, *_, **__):
        self.test_mode = test_mode

    def get_balance(self, force: bool = False) -> float:  # noqa: D401
        return 1_000.0

    async def execute_trade(self, symbol: str, amount: float, reference_price: float | None = None) -> Dict[str, Any]:  # noqa: D401
        await asyncio.sleep(0)
        now = int(time.time() * 1000)
        return {
            "success": True,
            "exchange": "mexc",
            "symbol": symbol,
            "account": "PRIMARY",
            "amount": amount,
            "order_result": {"id": f"MEXC-{symbol}-{now}"},
            "execution_time": 0.002,
        }

    async def execute_market_slices(self, symbol: str, amount: float, preparation_data: Any | None = None) -> Dict[str, Any]:  # noqa: D401
        return await self.execute_trade(symbol, amount)


class _DummyGateTrader:
    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode

    async def _get_balance(self, force: bool = False) -> float:  # noqa: D401
        await asyncio.sleep(0)
        return 1_000.0

    def get_balance(self, force: bool = False) -> float:  # noqa: D401
        return 1_000.0

    async def execute_trade(self, symbol: str, amount: float, reference_price: float | None = None) -> Dict[str, Any]:  # noqa: D401
        await asyncio.sleep(0)
        now = int(time.time() * 1000)
        return {
            "success": True,
            "exchange": "gate",
            "symbol": symbol,
            "amount": amount,
            "order_result": {"id": f"GATE-{symbol}-{now}"},
            "execution_time": 0.003,
        }

    async def execute_market_slices(self, symbol: str, amount: float, preparation_data: Any | None = None) -> Dict[str, Any]:  # noqa: D401
        return await self.execute_trade(symbol, amount)


def _patch_spot_binance_dependencies() -> None:
    """Monkey-patch heavy dependencies for an offline test run."""

    spot_binance.UbuntuOptimizer.optimize_all = staticmethod(lambda: None)  # type: ignore[attr-defined]

    spot_binance.BINANCE_SESSION = _DummySession()
    spot_binance.MEXC_SESSION = _DummySession()
    spot_binance.GATE_SESSION = _DummySession()

    spot_binance.BinanceHyperbeastTrader = _DummyBinanceTrader  # type: ignore[attr-defined]
    spot_binance.MexcHyperbeastTrader = _DummyMexcTrader  # type: ignore[attr-defined]
    spot_binance.GateUltraTrader = _DummyGateTrader  # type: ignore[attr-defined]

    original_get_market_cap = spot_binance.CMC_API.get_market_cap

    def _fake_get_market_cap(symbol: str, aliases: List[str] | None = None) -> float | None:
        if symbol.upper() == "LINEA":
            return 150_000_000  # under $300M threshold so it is not filtered out
        return original_get_market_cap(symbol, aliases=aliases)

    spot_binance.CMC_API.get_market_cap = _fake_get_market_cap  # type: ignore[assignment]


async def main() -> None:
    _patch_spot_binance_dependencies()

    trader = spot_binance.UltraTrader(test_mode=True)
    trader.allow_binance_listings = True

    # Replace real websocket clients with local recorders.
    trader.signal_ws_client = _DummyWebSocket()
    trader.position_ws_client = _DummyWebSocket()
    trader.prep_ws_client = _DummyWebSocket()

    signal_payload = {
        "type": "signal",
        "exchange": "binance",
        "tokens": ["LINEA"],
        "alert_type": "listing_no_krw",
        "markets": ["LINEA/USDT", "LINEA/BTC"],
        "announcement": "Binance will list LINEA (LINEA)",
        "timestamp": int(time.time()),
    }

    await trader.handle_listing_signal(signal_payload)

    print("Signal processing completed. Captured websocket messages:\n")
    for idx, message in enumerate(trader.signal_ws_client.messages, start=1):
        print(f"[{idx}] {json.dumps(message, indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())
