import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tree_news  # noqa: E402


class DummyStore:
    def __init__(self):
        self.signals = {}

    def contains(self, key: str) -> bool:
        return key in self.signals

    def update_many(self, entries):
        self.signals.update(entries)

    async def close(self):
        return None


async def _instant_sleep(_delay: float) -> None:
    return None


def test_queue_full_does_not_poison_persistent_store(monkeypatch):
    hub = tree_news.SignalHub()
    hub.signal_store = DummyStore()
    monkeypatch.setattr(tree_news.asyncio, "sleep", _instant_sleep)
    hub.signal_queue = asyncio.Queue(maxsize=1)

    blocker = tree_news.Signal(
        source="BITHUMB",
        tokens=["HOLD"],
        announcement="occupy queue",
    )
    hub.signal_queue.put_nowait(blocker)

    signal = tree_news.Signal(
        source="BITHUMB",
        tokens=["TEST"],
        announcement="new listing",
    )

    first_result = asyncio.run(hub.process_signal(signal))
    assert first_result is False
    assert hub.signal_store.signals == {}

    hub.signal_queue.get_nowait()
    second_result = asyncio.run(hub.process_signal(signal))
    assert second_result is True
    assert signal.hash in hub.signal_store.signals


def test_ai_verifier_requires_key_by_default(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("TREE_NEWS_AI_POLICY", raising=False)

    verifier = tree_news.NewsAIVerifier()
    approved, reason = asyncio.run(
        verifier.verify(
            data={"title": "Funding", "body": "Raised $150M", "source": "Reuters"},
            tokens=["ABC"],
            alert_type="investment",
            amount=150.0,
        )
    )

    assert approved is False
    assert "required" in reason.lower()


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = iter(messages)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration:
            raise StopAsyncIteration

    async def close(self):
        self.closed = True

    def exception(self):
        return None


def test_message_loop_enqueues_without_waiting_for_processing():
    async def scenario():
        hub = tree_news.SignalHub()
        client = tree_news.TreeNewsClient("TEST", "wss://example.invalid", hub)
        client.running = False

        started = asyncio.Event()
        release = asyncio.Event()
        processed = []

        async def slow_process(data):
            started.set()
            await release.wait()
            processed.append(data)

        client.process_message = slow_process
        await client._ensure_workers()
        client.ws = FakeWebSocket(
            [
                SimpleNamespace(
                    type=tree_news.WSMsgType.TEXT,
                    data=json.dumps({"title": "hello", "body": "world"}),
                ),
                SimpleNamespace(type=tree_news.WSMsgType.CLOSED, data=""),
            ]
        )

        await asyncio.wait_for(client.message_loop(), timeout=0.1)
        await asyncio.wait_for(started.wait(), timeout=0.1)
        assert processed == []

        release.set()
        await asyncio.wait_for(client.message_queue.join(), timeout=0.2)
        await client.stop()
        assert processed == [{"title": "hello", "body": "world"}]

    asyncio.run(scenario())


def test_handle_disconnect_schedules_only_one_reconnect(monkeypatch):
    async def scenario():
        hub = tree_news.SignalHub()
        client = tree_news.TreeNewsClient("TEST", "wss://example.invalid", hub)

        release_sleep = asyncio.Event()
        connect_calls = []

        async def fake_sleep(_delay: float) -> None:
            await release_sleep.wait()

        async def fake_connect() -> None:
            connect_calls.append("connect")

        monkeypatch.setattr(tree_news.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(client, "connect", fake_connect)

        await client.handle_disconnect()
        first_task = client._reconnect_task
        assert first_task is not None

        await client.handle_disconnect()
        second_task = client._reconnect_task
        assert second_task is first_task

        first_task.cancel()
        await asyncio.gather(first_task, return_exceptions=True)
        await client.stop()
        assert connect_calls == []

    asyncio.run(scenario())
