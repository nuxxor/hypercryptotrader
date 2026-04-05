import asyncio
import json
import pathlib
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import spot_binance
import trader
import tree_news


def _extract_base_symbol(market_pair: str) -> str:
    upper = market_pair.upper()
    if upper.endswith("_USDT"):
        return upper[:-5]
    if upper.endswith("USDT"):
        return upper[:-4]
    return upper


@dataclass
class _SharedState:
    prices: dict[str, float] = field(default_factory=lambda: defaultdict(lambda: 1.0))
    balances: dict[str, dict[str, float]] = field(
        default_factory=lambda: {
            "mexc": defaultdict(float, {"USDT": 1_000.0}),
            "gate": defaultdict(float, {"USDT": 1_000.0}),
        }
    )

    def apply_fill(self, exchange: str, symbol: str, quote_amount: float) -> float:
        price = max(self.prices[symbol.upper()], 1e-8)
        qty = quote_amount / price
        self.balances[exchange][symbol.upper()] += qty
        self.balances[exchange]["USDT"] -= quote_amount
        return qty

    def apply_sell(self, exchange: str, symbol: str, quantity: float) -> float:
        symbol_key = symbol.upper()
        available = max(self.balances[exchange][symbol_key], 0.0)
        sold_qty = min(max(quantity, 0.0), available)
        price = max(self.prices[symbol_key], 1e-8)
        self.balances[exchange][symbol_key] -= sold_qty
        self.balances[exchange]["USDT"] += sold_qty * price
        return sold_qty

    def get_balance(self, exchange: str, asset: str) -> float:
        return float(self.balances[exchange][asset.upper()])


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


class _DummySignalWebSocket:
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


class _TreeBroadcastClient:
    def __init__(self):
        self.messages: list[dict[str, Any]] = []

    async def send_str(self, message: str) -> None:
        self.messages.append(json.loads(message))

    async def close(self) -> None:
        return None


class _MemorySignalStore:
    def __init__(self):
        self.signals: dict[str, dict[str, Any]] = {}

    def contains(self, key: str) -> bool:
        return key in self.signals

    def update_many(self, entries: dict[str, dict[str, Any]]) -> None:
        self.signals.update(entries)

    def get_stats(self) -> dict[str, int]:
        stats: dict[str, int] = {}
        for key in self.signals:
            source = key.split(":", 1)[0]
            stats[source] = stats.get(source, 0) + 1
        return stats

    async def close(self) -> None:
        return None


class _DummyBinanceTrader:
    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.accounts = [{"name": "primary", "enabled": True}]
        self.performance = {"total": {"trade_count": 0, "successful_trades": 0, "total_trade_time": 0}}

    def get_all_balances(self, force: bool = False) -> dict[str, float]:
        return {"primary": 1_000.0}

    async def execute_trade(
        self,
        symbol: str,
        amount: float | None = None,
        per_account_amounts: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"multi_account_results": []}


class _DummySpotMexcTrader:
    def __init__(self, shared: _SharedState, test_mode: bool = False, *_, **__):
        self.shared = shared
        self.test_mode = test_mode
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    def get_balance(self, force: bool = False) -> float:
        return self.shared.get_balance("mexc", "USDT")

    async def execute_market_slices(
        self,
        symbol: str,
        amount: float,
        preparation_data: Any | None = None,
    ) -> dict[str, Any]:
        await asyncio.sleep(0)
        qty = self.shared.apply_fill("mexc", symbol, amount)
        return {
            "success": True,
            "exchange": "mexc",
            "symbol": symbol,
            "account": "PRIMARY",
            "amount": amount,
            "executedQty": qty,
            "price": self.shared.prices[symbol.upper()],
            "order_result": {"id": f"MEXC-{symbol}-{int(time.time() * 1000)}"},
            "execution_time": 0.002,
        }


class _DummySpotGateTrader:
    def __init__(self, shared: _SharedState, test_mode: bool = False):
        self.shared = shared
        self.test_mode = test_mode
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    async def _get_balance(self, force: bool = False) -> float:
        await asyncio.sleep(0)
        return self.shared.get_balance("gate", "USDT")

    def get_balance(self, force: bool = False) -> float:
        return self.shared.get_balance("gate", "USDT")

    async def execute_market_slices(
        self,
        symbol: str,
        amount: float,
        preparation_data: Any | None = None,
    ) -> dict[str, Any]:
        await asyncio.sleep(0)
        qty = self.shared.apply_fill("gate", symbol, amount)
        return {
            "success": True,
            "exchange": "gate",
            "symbol": symbol,
            "account": "PRIMARY",
            "amount": amount,
            "executedQty": qty,
            "price": self.shared.prices[symbol.upper()],
            "order_result": {"id": f"GATE-{symbol}-{int(time.time() * 1000)}"},
            "execution_time": 0.003,
        }


class _SharedMexcAPI:
    def __init__(self, shared: _SharedState):
        self.shared = shared

    async def get_balance(self, asset: str, use_cache: bool = True) -> float:
        return self.shared.get_balance("mexc", asset)

    async def get_price(self, market_pair: str) -> float:
        return self.shared.prices[_extract_base_symbol(market_pair)]

    async def get_exchange_info(self, symbol_key: str) -> dict[str, Any]:
        return {"symbol": symbol_key, "quantityPrecision": 6}

    def get_lot_size_filter(self, symbol_info: dict[str, Any] | None):
        return None

    def format_quantity_with_lot_size(self, quantity: float, lot_filter: dict[str, Any]) -> str:
        return self.format_quantity(quantity, 6)

    def format_quantity(self, quantity: float, precision: int) -> str:
        return f"{quantity:.{precision}f}".rstrip("0").rstrip(".")

    async def place_market_sell(self, market_pair: str, amount: float) -> dict[str, Any]:
        symbol = _extract_base_symbol(market_pair)
        sold_qty = self.shared.apply_sell("mexc", symbol, amount)
        return {
            "orderId": f"MEXC-SELL-{symbol}-{int(time.time() * 1000)}",
            "symbol": market_pair,
            "executedQty": sold_qty,
        }

    def subscribe_price(self, symbol: str, callback) -> None:
        return None

    def unsubscribe_price(self, symbol: str, callback) -> None:
        return None


class _SharedGateAPI:
    def __init__(self, shared: _SharedState):
        self.shared = shared

    async def get_balance(self, asset: str) -> float:
        return self.shared.get_balance("gate", asset)

    async def get_balance_snapshot(self, asset: str, use_cache: bool = False):
        value = Decimal(str(self.shared.get_balance("gate", asset)))
        return SimpleNamespace(available=value, locked=Decimal("0"), effective_total=value)

    async def get_price(self, market_pair: str) -> float:
        return self.shared.prices[_extract_base_symbol(market_pair)]

    async def get_currency_pair_info(self, market_pair: str) -> dict[str, Any]:
        return {"id": market_pair, "min_quote_amount": "1", "amount_precision": 6}

    def format_quantity_for_gate(self, quantity: float, pair_info: dict[str, Any] | None) -> str:
        precision = 6
        if pair_info and "amount_precision" in pair_info:
            try:
                precision = max(0, int(pair_info["amount_precision"]))
            except (TypeError, ValueError):
                precision = 6
        return f"{quantity:.{precision}f}".rstrip("0").rstrip(".")

    async def place_market_sell(self, market_pair: str, quantity: float) -> dict[str, Any]:
        symbol = _extract_base_symbol(market_pair)
        sold_qty = self.shared.apply_sell("gate", symbol, quantity)
        return {
            "id": f"GATE-SELL-{symbol}-{int(time.time() * 1000)}",
            "currency_pair": market_pair,
            "amount": sold_qty,
        }

    def subscribe_price(self, symbol: str, callback) -> None:
        return None

    def unsubscribe_price(self, symbol: str, callback) -> None:
        return None


def _patch_spot_dependencies(monkeypatch, shared: _SharedState) -> None:
    monkeypatch.setattr(spot_binance.UbuntuOptimizer, "optimize_all", staticmethod(lambda *args, **kwargs: None))
    monkeypatch.setattr(spot_binance, "BINANCE_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "MEXC_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "GATE_SESSION", _DummySession())
    monkeypatch.setattr(spot_binance, "BinanceHyperbeastTrader", _DummyBinanceTrader)
    monkeypatch.setattr(spot_binance, "MexcHyperbeastTrader", lambda test_mode=False, *args, **kwargs: _DummySpotMexcTrader(shared, test_mode=test_mode))
    monkeypatch.setattr(spot_binance, "GateUltraTrader", lambda test_mode=False, *args, **kwargs: _DummySpotGateTrader(shared, test_mode=test_mode))
    monkeypatch.setattr(
        spot_binance.CMC_API,
        "get_market_cap",
        lambda symbol, aliases=None: 150_000_000,
    )


def _build_position_manager(monkeypatch, tmp_path: pathlib.Path, shared: _SharedState) -> trader.PositionManager:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GATE_API_KEY", "gate-key")
    monkeypatch.setenv("GATE_API_SECRET", "gate-secret")
    monkeypatch.setenv("MEXC_API_KEY", "mexc-key")
    monkeypatch.setenv("MEXC_API_SECRET", "mexc-secret")

    manager = trader.PositionManager(
        test_mode=True,
        enabled_exchanges=["mexc", "gate"],
        position_ws_enabled=False,
    )
    manager.gate_api = _SharedGateAPI(shared)
    manager.mexc_api = _SharedMexcAPI(shared)
    manager.binance_apis = {}
    manager.position_check_delay = 0.02
    manager.position_check_max_retries = 1
    manager.position_check_retry_interval = 0.0
    manager.allow_unconfirmed_positions = False
    manager.cmc_api.get_market_cap = lambda symbol: 150_000_000
    return manager


async def _next_message(messages: list[dict[str, Any]], message_type: str) -> dict[str, Any]:
    for _ in range(50):
        for message in messages:
            if message.get("type") == message_type:
                return message
        await asyncio.sleep(0.01)
    raise AssertionError(f"Expected message type {message_type!r}")


async def _emit_tree_signal(hub: tree_news.SignalHub, token: str) -> dict[str, Any]:
    signal = tree_news.Signal(
        source="BINANCE",
        tokens=[token],
        announcement=f"Binance will list {token} ({token})",
        alert_type="listing",
    )
    client = _TreeBroadcastClient()
    await hub.add_client(client)
    await hub.process_signal(signal)
    payload = await _next_message(client.messages, "signal")
    await hub.remove_client(client)
    return payload


async def _drain_position_messages(
    manager: trader.PositionManager,
    ultra: spot_binance.UltraTrader,
    *,
    start_index: int = 0,
) -> int:
    for message in ultra.signal_ws_client.messages[start_index:]:
        await manager._handle_position_websocket_message(message)
    return len(ultra.signal_ws_client.messages)


async def _await_pipeline_idle(
    manager: trader.PositionManager,
    ultra: spot_binance.UltraTrader,
    *,
    timeout: float = 5.0,
    expect_flat: bool = False,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        active_signal_checks = [task for task in manager.pending_signal_checks.values() if not task.done()]
        active_test_exits = [task for task in manager.test_exit_tasks.values() if not task.done()]
        active_evaluations = [task for task in manager._position_evaluation_tasks.values() if not task.done()]
        active_ultra_tasks = [task for task in ultra.tasks if not task.done()]

        if (
            not active_signal_checks
            and not active_test_exits
            and not active_evaluations
            and not active_ultra_tasks
            and (not expect_flat or not manager.active_positions)
        ):
            return
        await asyncio.sleep(0.01)

    raise AssertionError(
        "Pipeline did not become idle: "
        f"pending_checks={len(active_signal_checks)} "
        f"test_exits={len(active_test_exits)} "
        f"evaluations={len(active_evaluations)} "
        f"ultra_tasks={len(active_ultra_tasks)} "
        f"active_positions={len(manager.active_positions)}"
    )


def _assert_pipeline_runtime_clean(
    manager: trader.PositionManager,
    ultra: spot_binance.UltraTrader,
    shared: _SharedState,
    *,
    expected_signals: int | None = None,
    usdt_tolerance: float = 1e-5,
    asset_tolerance: float = 1e-5,
) -> None:
    assert not manager.active_positions
    assert not manager.symbol_positions
    assert not manager.pending_signal_checks
    assert not manager.test_exit_tasks
    assert not manager._position_evaluation_tasks
    assert not manager._position_evaluation_pending
    assert not manager._position_evaluation_locks
    assert not manager._position_action_locks
    assert not manager._last_position_balance_guard
    assert not ultra.tasks
    assert not manager.trade_confirmations
    assert all(position.status == "closed" for position in manager.positions.values())
    if expected_signals is not None:
        trade_reports = [m for m in ultra.signal_ws_client.messages if m.get("type") == "trade_report"]
        assert len(trade_reports) == expected_signals
    assert abs(shared.get_balance("mexc", "USDT") - 1_000.0) <= usdt_tolerance
    assert abs(shared.get_balance("gate", "USDT") - 1_000.0) <= usdt_tolerance
    for exchange in ("mexc", "gate"):
        for asset, balance in shared.balances[exchange].items():
            if asset == "USDT":
                continue
            assert abs(balance) <= asset_tolerance, f"{exchange} residual balance too large for {asset}: {balance}"


def test_end_to_end_pipeline_opens_positions_without_real_trades(monkeypatch, tmp_path):
    shared = _SharedState()
    shared.prices["LINEA"] = 1.0
    _patch_spot_dependencies(monkeypatch, shared)

    async def scenario():
        hub = tree_news.SignalHub()
        hub.signal_store = _MemorySignalStore()
        broadcast_task = asyncio.create_task(hub.broadcast_loop())
        try:
            payload = await _emit_tree_signal(hub, "LINEA")

            ultra = spot_binance.UltraTrader(test_mode=True)
            ultra.allow_binance_listings = True
            ultra.signal_ws_client = _DummySignalWebSocket()
            ultra.position_ws_client = _DummySignalWebSocket()
            ultra.prep_ws_client = _DummySignalWebSocket()

            manager = _build_position_manager(monkeypatch, tmp_path, shared)

            await manager._handle_position_websocket_message(payload)
            await ultra.handle_listing_signal(payload)

            for message in ultra.signal_ws_client.messages:
                await manager._handle_position_websocket_message(message)

            pending = list(manager.pending_signal_checks.values())
            await asyncio.gather(*pending)
            await asyncio.sleep(0)

            assert manager.trade_confirmations == {}
            assert "LINEA_mexc_PRIMARY" in manager.active_positions
            assert "LINEA_gate_PRIMARY" in manager.active_positions

            trade_report = next(
                message
                for message in ultra.signal_ws_client.messages
                if message.get("type") == "trade_report"
            )
            assert {entry["exchange"] for entry in trade_report["results"]} == {"mexc", "gate"}
        finally:
            broadcast_task.cancel()
            await asyncio.gather(broadcast_task, return_exceptions=True)
            await hub.close()

    asyncio.run(scenario())


def test_pipeline_soak_multiple_signals_stays_stable(monkeypatch, tmp_path):
    shared = _SharedState()
    _patch_spot_dependencies(monkeypatch, shared)

    async def scenario():
        hub = tree_news.SignalHub()
        hub.signal_store = _MemorySignalStore()
        hub.relay_publisher = None
        broadcast_task = asyncio.create_task(hub.broadcast_loop())
        try:
            ultra = spot_binance.UltraTrader(test_mode=True)
            ultra.allow_binance_listings = True
            ultra.signal_ws_client = _DummySignalWebSocket()
            ultra.position_ws_client = _DummySignalWebSocket()
            ultra.prep_ws_client = _DummySignalWebSocket()

            manager = _build_position_manager(monkeypatch, tmp_path, shared)

            for index in range(5):
                token = f"TKN{index}"
                shared.prices[token] = 1.0
                payload = await _emit_tree_signal(hub, token)
                await manager._handle_position_websocket_message(payload)
                await ultra.handle_listing_signal(payload)
                for message in ultra.signal_ws_client.messages[-3:]:
                    await manager._handle_position_websocket_message(message)

            pending = list(manager.pending_signal_checks.values())
            await asyncio.gather(*pending)
            await asyncio.sleep(0)

            assert not manager.pending_signal_checks
            assert len(manager.active_positions) == 10
            assert hub.stats["signals_enqueued"] == 5
            assert hub.stats["queue_drops"] == 0
        finally:
            broadcast_task.cancel()
            await asyncio.gather(broadcast_task, return_exceptions=True)
            await hub.close()

    asyncio.run(scenario())


def test_pipeline_burst_soak_test_signals_return_system_to_flat_state(monkeypatch, tmp_path):
    shared = _SharedState()
    _patch_spot_dependencies(monkeypatch, shared)

    async def scenario():
        hub = tree_news.SignalHub()
        hub.signal_store = _MemorySignalStore()
        hub.relay_publisher = None
        broadcast_task = asyncio.create_task(hub.broadcast_loop())
        try:
            ultra = spot_binance.UltraTrader(test_mode=True)
            ultra.allow_binance_listings = True
            ultra.signal_ws_client = _DummySignalWebSocket()
            ultra.position_ws_client = _DummySignalWebSocket()
            ultra.prep_ws_client = _DummySignalWebSocket()

            manager = _build_position_manager(monkeypatch, tmp_path, shared)
            manager.position_check_delay = 0.30
            manager.position_check_retry_interval = 0.0
            manager.position_check_max_retries = 1
            manager.test_auto_exit_delay = 0.05
            manager.test_auto_exit_timeout = 2.0
            manager.test_auto_exit_qty_factor = 1.0

            message_index = 0
            total_signals = 24
            burst_size = 6

            for burst_start in range(0, total_signals, burst_size):
                burst_tasks = []
                for index in range(burst_start, min(burst_start + burst_size, total_signals)):
                    token = f"SOAK{index:02d}"
                    shared.prices[token] = 1.0
                    payload = await _emit_tree_signal(hub, token)
                    payload["is_test"] = True
                    payload["announcement"] = f"[TEST] {payload['announcement']}"
                    await manager._handle_position_websocket_message(payload)
                    burst_tasks.append(
                        ultra.create_tracked_task(
                            ultra.handle_listing_signal(payload),
                            label=f"soak:{token}",
                        )
                    )

                created_tasks = await asyncio.gather(*burst_tasks)
                await asyncio.gather(*created_tasks, return_exceptions=True)
                message_index = await _drain_position_messages(
                    manager,
                    ultra,
                    start_index=message_index,
                )

            message_index = await _drain_position_messages(
                manager,
                ultra,
                start_index=message_index,
            )
            await _await_pipeline_idle(manager, ultra, timeout=6.0, expect_flat=True)
            await asyncio.sleep(0)

            assert hub.stats["signals_enqueued"] == total_signals
            assert hub.stats["queue_drops"] == 0
            _assert_pipeline_runtime_clean(manager, ultra, shared, expected_signals=total_signals)
        finally:
            await manager.__aexit__(None, None, None)
            await ultra.shutdown()
            broadcast_task.cancel()
            await asyncio.gather(broadcast_task, return_exceptions=True)
            await hub.close()

    asyncio.run(scenario())


def test_pipeline_multi_round_soak_keeps_runtime_clean(monkeypatch, tmp_path):
    shared = _SharedState()
    _patch_spot_dependencies(monkeypatch, shared)

    async def scenario():
        hub = tree_news.SignalHub()
        hub.signal_store = _MemorySignalStore()
        hub.relay_publisher = None
        broadcast_task = asyncio.create_task(hub.broadcast_loop())
        try:
            ultra = spot_binance.UltraTrader(test_mode=True)
            ultra.allow_binance_listings = True
            ultra.signal_ws_client = _DummySignalWebSocket()
            ultra.position_ws_client = _DummySignalWebSocket()
            ultra.prep_ws_client = _DummySignalWebSocket()

            manager = _build_position_manager(monkeypatch, tmp_path, shared)
            manager.position_check_delay = 0.25
            manager.position_check_retry_interval = 0.0
            manager.position_check_max_retries = 1
            manager.test_auto_exit_delay = 0.05
            manager.test_auto_exit_timeout = 2.0
            manager.test_auto_exit_qty_factor = 1.0

            total_signals = 0

            for round_index in range(3):
                message_index = len(ultra.signal_ws_client.messages)
                for burst_index in range(4):
                    burst_tasks = []
                    for token_index in range(4):
                        token = f"ROUND{round_index}_B{burst_index}_T{token_index}"
                        shared.prices[token] = 1.0 + (round_index * 0.02) + (token_index * 0.005)
                        payload = await _emit_tree_signal(hub, token)
                        payload["is_test"] = True
                        payload["announcement"] = f"[ROUND {round_index}] {payload['announcement']}"
                        await manager._handle_position_websocket_message(payload)
                        burst_tasks.append(
                            ultra.create_tracked_task(
                                ultra.handle_listing_signal(payload),
                                label=f"multi-soak:{token}",
                            )
                        )
                        total_signals += 1

                    created_tasks = await asyncio.gather(*burst_tasks)
                    await asyncio.gather(*created_tasks, return_exceptions=True)
                    message_index = await _drain_position_messages(
                        manager,
                        ultra,
                        start_index=message_index,
                    )

                message_index = await _drain_position_messages(
                    manager,
                    ultra,
                    start_index=message_index,
                )
                await _await_pipeline_idle(manager, ultra, timeout=6.0, expect_flat=True)
                _assert_pipeline_runtime_clean(manager, ultra, shared, expected_signals=total_signals)

            assert hub.stats["signals_enqueued"] == total_signals
            assert hub.stats["queue_drops"] == 0
        finally:
            await manager.__aexit__(None, None, None)
            await ultra.shutdown()
            broadcast_task.cancel()
            await asyncio.gather(broadcast_task, return_exceptions=True)
            await hub.close()

    asyncio.run(scenario())
