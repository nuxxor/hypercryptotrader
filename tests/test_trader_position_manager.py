import asyncio
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import trader  # noqa: E402


class DummyExchangeAPI:
    def __init__(self):
        self.sell_calls = 0
        self.unsubscribe_calls = []

    async def place_market_sell(self, market_pair: str, quantity: float):
        self.sell_calls += 1
        await asyncio.sleep(0.02)
        return {"orderId": f"sell-{self.sell_calls}", "symbol": market_pair, "qty": quantity}

    def unsubscribe_price(self, symbol: str, callback):
        self.unsubscribe_calls.append((symbol, callback))


def build_manager() -> trader.PositionManager:
    manager = object.__new__(trader.PositionManager)
    manager.positions = {}
    manager.active_positions = set()
    manager.symbol_positions = {}
    manager.market_conditions = {}
    manager.retry_status = {}
    manager.last_price_update = {}
    manager.last_manual_check = {}
    manager.last_gate_balance_check = {}
    manager._gate_balance_snapshot_cache = {}
    manager._position_evaluation_tasks = {}
    manager._position_evaluation_pending = set()
    manager._position_evaluation_locks = {}
    manager._position_action_locks = {}
    manager.position_balance_guard_interval = 3.0
    manager._last_position_balance_guard = {}
    manager.gate_balance_check_interval = 30.0
    manager.gate_min_order_value = 10.0
    manager.gate_dust_release_margin = 0.02
    manager.gate_dust_buffer = {}
    manager.investment_tasks = {}
    manager.sell_attempts = {}
    manager.trade_stats = {
        "total_trades": 0,
        "successful_trades": 0,
        "total_profit_usdt": 0,
        "average_profit_pct": 0,
        "best_trade": {"symbol": None, "profit_pct": 0},
        "worst_trade": {"symbol": None, "profit_pct": float("inf")},
    }
    manager.binance_apis = {"PRIMARY": DummyExchangeAPI()}
    manager.mexc_api = DummyExchangeAPI()
    manager.gate_api = DummyExchangeAPI()
    manager.is_friend_session = False
    manager.strategies = {}
    return manager


def make_position(symbol: str = "TEST", exchange: str = "binance") -> trader.Position:
    return trader.Position(
        symbol=symbol,
        exchange=exchange,
        entry_price=1.0,
        quantity=10.0,
        entry_time=datetime.now(),
        order_id="order-1",
        listing_source="binance",
        account_name="PRIMARY",
    )


def test_handle_price_update_coalesces_evaluations():
    manager = build_manager()
    position = make_position()
    position_key = manager._position_key(position)
    manager.positions[position_key] = position
    manager.active_positions.add(position_key)
    manager.symbol_positions[position.symbol] = [position_key]

    evaluations = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def fake_evaluate(self, active_position, *, trigger="direct"):
        evaluations.append(trigger)
        if len(evaluations) == 1:
            first_started.set()
            await release_first.wait()

    manager.evaluate_position = fake_evaluate.__get__(manager, trader.PositionManager)

    async def scenario():
        await manager.handle_price_update("TESTUSDT", 1.1)
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await manager.handle_price_update("TESTUSDT", 1.2)
        await manager.handle_price_update("TESTUSDT", 1.3)
        assert manager._position_evaluation_pending == {position_key}
        release_first.set()

        for _ in range(50):
            if not manager._position_evaluation_tasks:
                break
            await asyncio.sleep(0.01)

    asyncio.run(scenario())

    assert len(evaluations) == 2
    assert evaluations[0] == "price_update"


def test_sell_position_is_idempotent_per_position():
    manager = build_manager()
    position = make_position()
    position_key = manager._position_key(position)
    manager.positions[position_key] = position
    manager.active_positions.add(position_key)
    manager.symbol_positions[position.symbol] = [position_key]

    async def fake_refresh(self, active_position, *, allow_increase=False, use_cache=False):
        return active_position.remaining_quantity

    async def fake_finalize(self, active_position):
        return None

    async def fake_check_min_notional(self, active_position, quantity):
        return True, quantity

    manager._refresh_position_balance = fake_refresh.__get__(manager, trader.PositionManager)
    manager._finalize_trade = fake_finalize.__get__(manager, trader.PositionManager)
    manager._check_min_notional = fake_check_min_notional.__get__(manager, trader.PositionManager)

    async def scenario():
        return await asyncio.gather(
            manager.sell_position(position, "first"),
            manager.sell_position(position, "second"),
        )

    results = asyncio.run(scenario())

    assert manager.binance_apis["PRIMARY"].sell_calls == 1
    assert results.count(True) == 1
    assert results.count(False) == 1
    assert position.status == "closed"
    assert position_key not in manager.active_positions
    assert position.symbol not in manager.symbol_positions
    assert len(manager.binance_apis["PRIMARY"].unsubscribe_calls) == 1
