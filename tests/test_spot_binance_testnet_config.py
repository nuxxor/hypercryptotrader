import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from spot_runtime import binance as spot_binance_runtime
from spot_runtime import common as spot_common


class _DummyTimeCache:
    def __init__(self, exchange: str):
        self.exchange = exchange
        self.warmup_complete = True

    def get_time(self) -> int:
        return 0


def test_binance_rest_base_url_uses_testnet(monkeypatch):
    monkeypatch.setenv("BINANCE_USE_TESTNET", "true")
    assert spot_common._binance_use_testnet() is True
    assert spot_common._binance_rest_base_url() == "https://testnet.binance.vision"


def test_binance_trader_discovers_testnet_credentials(monkeypatch):
    monkeypatch.setenv("BINANCE_USE_TESTNET", "true")
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "testnet-key")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "testnet-secret")
    monkeypatch.setattr(spot_binance_runtime, "ServerTimeCache", _DummyTimeCache)
    monkeypatch.setattr(spot_binance_runtime.BinanceHyperbeastTrader, "_warm_up", lambda self: True)

    trader = spot_binance_runtime.BinanceHyperbeastTrader(test_mode=False)

    assert trader.use_testnet is True
    assert trader.base_url == "https://testnet.binance.vision"
    assert trader.accounts[0]["api_key"] == "testnet-key"
