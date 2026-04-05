import asyncio
import pathlib
import sys
import time
from typing import Any

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import spot_binance


class _DummyResponse:
    def __init__(self, status_code: int = 200, payload: dict[str, Any] | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict[str, Any]:
        return self._payload


class _DummySession:
    def get(self, url: str, timeout: float | tuple[float, float] = 0.3) -> _DummyResponse:
        if "ticker/price" in url:
            return _DummyResponse(200, {"price": "1"})
        if "api/v3/time" in url:
            return _DummyResponse(200, {"serverTime": int(time.time() * 1000)})
        if "spot/time" in url:
            return _DummyResponse(200, {"server_time": int(time.time())})
        return _DummyResponse(200, {})


class _DummyWebSocket:
    def __init__(self):
        self.connected = True
        self.messages: list[dict[str, Any]] = []

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self) -> bool:
        self.connected = False
        return True

    async def send_message(self, message: dict[str, Any]) -> bool:
        self.messages.append(message)
        return True


class _DummyBinanceTrader:
    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.accounts = [{"name": "primary", "enabled": True}]
        self.performance = {"total": {"trade_count": 0, "successful_trades": 0, "total_trade_time": 0}}

    def get_all_balances(self, force: bool = False) -> dict[str, float]:
        return {"primary": 1_000.0}

    async def execute_trade(self, symbol: str, amount: float, preparation_data: Any | None = None) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {
            "multi_account_results": [
                {
                    "success": True,
                    "exchange": "binance",
                    "account": "primary",
                    "symbol": symbol,
                    "amount": amount,
                    "order_result": {"orderId": f"BN-{symbol}"},
                    "execution_time": 0.001,
                }
            ]
        }


class _DummyMexcTrader:
    def __init__(self, test_mode: bool = False, *_, **__):
        self.test_mode = test_mode
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    def get_balance(self, force: bool = False) -> float:
        return 1_000.0

    async def execute_market_slices(self, symbol: str, amount: float, preparation_data: Any | None = None) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {
            "success": True,
            "exchange": "mexc",
            "symbol": symbol,
            "account": "PRIMARY",
            "amount": amount,
            "order_result": {"id": f"MEXC-{symbol}"},
            "execution_time": 0.002,
        }


class _DummyGateTrader:
    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    async def _get_balance(self, force: bool = False) -> float:
        await asyncio.sleep(0)
        return 1_000.0

    def get_balance(self, force: bool = False) -> float:
        return 1_000.0

    async def execute_market_slices(self, symbol: str, amount: float, preparation_data: Any | None = None) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {
            "success": True,
            "exchange": "gate",
            "symbol": symbol,
            "amount": amount,
            "order_result": {"id": f"GATE-{symbol}"},
            "execution_time": 0.003,
        }


def test_spot_binance_facade_points_to_runtime():
    assert spot_binance.UltraTrader.__module__ == "spot_runtime.orchestrator"
    assert spot_binance.main.__module__ == "spot_runtime.orchestrator"


def test_binance_listing_signal_flows_through_modular_router(monkeypatch):
    monkeypatch.setattr(spot_binance.UbuntuOptimizer, "optimize_all", staticmethod(lambda *args, **kwargs: None))
    monkeypatch.setattr(spot_binance, "BINANCE_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "MEXC_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "GATE_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "BinanceHyperbeastTrader", _DummyBinanceTrader)
    monkeypatch.setattr(spot_binance, "MexcHyperbeastTrader", _DummyMexcTrader)
    monkeypatch.setattr(spot_binance, "GateUltraTrader", _DummyGateTrader)
    monkeypatch.setattr(
        spot_binance.CMC_API,
        "get_market_cap",
        lambda symbol, aliases=None: 150_000_000 if symbol.upper() == "LINEA" else None,
    )

    async def _scenario():
        trader = spot_binance.UltraTrader(test_mode=True)
        trader.allow_binance_listings = True
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
        return trader

    trader = asyncio.run(_scenario())
    trade_reports = [message for message in trader.signal_ws_client.messages if message.get("type") == "trade_report"]
    assert trade_reports, "expected a trade report message from the modular router"

    exchanges = {result["exchange"] for result in trade_reports[-1]["results"]}
    assert exchanges == {"gate", "mexc"}


def test_tracked_background_task_logs_and_cleans_up(monkeypatch):
    monkeypatch.setattr(spot_binance.UbuntuOptimizer, "optimize_all", staticmethod(lambda *args, **kwargs: None))
    monkeypatch.setattr(spot_binance, "BinanceHyperbeastTrader", _DummyBinanceTrader)
    monkeypatch.setattr(spot_binance, "MexcHyperbeastTrader", _DummyMexcTrader)
    monkeypatch.setattr(spot_binance, "GateUltraTrader", _DummyGateTrader)

    errors = []
    monkeypatch.setattr(
        spot_binance.logger,
        "error",
        lambda message, *args: errors.append(message % args if args else message),
    )

    async def _scenario():
        trader = spot_binance.UltraTrader(test_mode=True)

        async def failing_task():
            await asyncio.sleep(0)
            raise RuntimeError("boom")

        task = await trader.create_tracked_task(
            failing_task(),
            label="unit-test-failure",
        )
        result = await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        return trader, result[0]

    trader, result = asyncio.run(_scenario())

    assert isinstance(result, RuntimeError)
    assert not trader.tasks
    assert any("unit-test-failure" in message and "boom" in message for message in errors)
