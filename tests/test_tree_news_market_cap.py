import asyncio
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover - optional dependency in test env
    import aiohttp  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - provide minimal stub
    aiohttp_stub = types.ModuleType("aiohttp")

    class _DummyClientSession:  # noqa: D401 - simple stub
        def __init__(self, *args, **kwargs):
            pass

        async def close(self):  # pragma: no cover
            return None

    class _DummyConnector:  # noqa: D401 - simple stub
        def __init__(self, *args, **kwargs):
            pass

    class _DummyTimeout:  # noqa: D401 - simple stub
        def __init__(self, *args, **kwargs):
            pass

    class _DummyWebSocketResponse:  # noqa: D401 - simple stub
        pass

    class _DummyWSTimeout:  # noqa: D401 - simple stub
        def __init__(self, *args, **kwargs):
            pass

    aiohttp_stub.ClientSession = _DummyClientSession
    aiohttp_stub.TCPConnector = _DummyConnector
    aiohttp_stub.ClientTimeout = _DummyTimeout
    aiohttp_stub.ClientWSTimeout = _DummyWSTimeout
    aiohttp_stub.ClientWebSocketResponse = _DummyWebSocketResponse
    aiohttp_stub.WSMsgType = types.SimpleNamespace(
        TEXT="TEXT",
        ERROR="ERROR",
        CLOSE="CLOSE",
        CLOSED="CLOSED",
    )
    aiohttp_stub.web = types.SimpleNamespace(WebSocketResponse=_DummyWebSocketResponse)

    sys.modules["aiohttp"] = aiohttp_stub
    sys.modules["aiohttp.web"] = aiohttp_stub.web

import tree_news  # noqa: E402


class DummyStore:
    def __init__(self):
        self._seen = set()

    def contains(self, key: str) -> bool:
        return key in self._seen

    def add(self, key: str, _meta) -> None:
        self._seen.add(key)

    def update_many(self, entries) -> None:
        self._seen.update(entries.keys())


async def _instant_sleep(_delay: float) -> None:
    return None


def test_upbit_prefers_lowest_known_market_cap(monkeypatch):
    hub = tree_news.SignalHub()
    hub.signal_store = DummyStore()
    hub.market_cap_filter.coin_data = {
        "AAA": {"market_cap": 100_000_000},
        "BBB": {"market_cap": 50_000_000},
    }
    hub.market_cap_filter.enabled = True
    hub.market_cap_filter.source_thresholds["UPBIT"] = 300_000_000

    signal = tree_news.Signal(
        source="UPBIT",
        tokens=["AAA", "BBB", "CCC"],
        announcement="Test listing",
    )

    monkeypatch.setattr(tree_news.asyncio, "sleep", _instant_sleep)

    result = asyncio.run(hub.process_signal(signal))
    assert result is True
    assert signal.tokens == ["BBB"]

    queued = hub.signal_queue.get_nowait()
    assert queued.tokens == ["BBB"]


def test_bithumb_prefers_lowest_known_market_cap(monkeypatch):
    hub = tree_news.SignalHub()
    hub.signal_store = DummyStore()
    hub.market_cap_filter.coin_data = {
        "AAA": {"market_cap": 80_000_000},
        "BBB": {"market_cap": 40_000_000},
    }
    hub.market_cap_filter.enabled = True
    hub.market_cap_filter.source_thresholds["BITHUMB"] = 100_000_000

    signal = tree_news.Signal(
        source="BITHUMB",
        tokens=["AAA", "BBB"],
        announcement="Test listing",
    )

    monkeypatch.setattr(tree_news.asyncio, "sleep", _instant_sleep)

    result = asyncio.run(hub.process_signal(signal))
    assert result is True
    assert signal.tokens == ["BBB"]

    queued = hub.signal_queue.get_nowait()
    assert queued.tokens == ["BBB"]
