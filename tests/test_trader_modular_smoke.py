from datetime import datetime, timedelta
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import trader


def test_trader_facade_points_to_modular_manager():
    assert trader.PositionManager.__module__ == "trader_runtime.manager"
    assert trader.main.__module__ == "trader_runtime.manager"
    assert trader.BinanceAPI.__name__ == "BinanceAPI"


def test_position_rsi_smoke():
    position = trader.Position(
        symbol="TEST",
        exchange="binance",
        entry_price=1.0,
        quantity=1.0,
        entry_time=datetime.now(),
        order_id="order-1",
        listing_source="binance",
    )

    start = datetime.now() - timedelta(seconds=70)
    for index in range(16):
        position.price_history.append((start + timedelta(seconds=index * 5), 1.0 + (index * 0.01)))

    rsi = position.calculate_rsi(period=14, resample_seconds=5)
    assert isinstance(rsi, float)
    assert 0.0 <= rsi <= 100.0
