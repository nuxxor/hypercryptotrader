#!/usr/bin/env python3
"""Tree News tweet sentiment streamer.

Connects to Tree News WebSocket feeds, filters tweets from a curated
list of accounts, and logs coin-related mentions with optional
sentiment analysis powered by LM Studio. Output is streamed to STDOUT
and persisted to ``var/logs/tweet_sentiment.log`` via ``TweetSentimentLogger``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
from collections import deque
from pathlib import Path
from typing import Deque, Dict, Optional, Set

import aiohttp
from aiohttp import ClientTimeout, ClientWSTimeout, WSMsgType

try:  # lazy optional dependency
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    def load_dotenv(*_args, **_kwargs):  # type: ignore[override]
        return False

from tweet_sentiment_logger import TweetSentimentLogger

LOGGER = logging.getLogger("sentiment_cli")

TOKYO_WS_URL = os.getenv("TREE_NEWS_TOKYO_URL", "ws://tokyo.treeofalpha.com:5124")
MAIN_WS_URL = os.getenv("TREE_NEWS_MAIN_URL", "wss://news.treeofalpha.com/ws")
WS_HEARTBEAT = int(os.getenv("TREE_NEWS_WS_HEARTBEAT", "25"))
WS_RECEIVE_TIMEOUT = float(os.getenv("TREE_NEWS_WS_RECEIVE_TIMEOUT", "45"))
RECONNECT_BASE_DELAY = float(os.getenv("TREE_NEWS_RECONNECT_BASE", "4"))
RECONNECT_MAX_DELAY = float(os.getenv("TREE_NEWS_RECONNECT_MAX", "90"))
RECONNECT_JITTER = float(os.getenv("TREE_NEWS_RECONNECT_JITTER", "0.25"))


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def format_entry(entry: Dict) -> str:
    handle = entry.get("handle", "?")
    coins = entry.get("coins") or []
    sentiment = entry.get("sentiment") or {}
    tweet_text = (entry.get("text") or "").replace("\n", " ").strip()
    if len(tweet_text) > 240:
        tweet_text = tweet_text[:240].rstrip() + "..."
    general = sentiment.get("general") if isinstance(sentiment, dict) else None
    final = None
    if isinstance(general, dict):
        final = general.get("final") or general.get("stance")
    if not final:
        final = sentiment.get("status") if isinstance(sentiment, dict) else None
    final = final or "unknown"
    coin_text = ",".join(coins) if coins else "no-coin"

    # Shill detection info
    shill_info = ""
    if isinstance(sentiment, dict):
        shilled_coins = sentiment.get("shilled_coins") or []
        has_shill = sentiment.get("has_shill", False)
        if has_shill and shilled_coins:
            shill_info = f" | SHILL={','.join(shilled_coins)}"
        elif has_shill:
            shill_info = " | SHILL=YES"

    note = ""
    if isinstance(sentiment, dict):
        status = sentiment.get("status")
        reason = sentiment.get("reason")
        if status in {"skipped-no-coin", "skipped-no-text"}:
            default_msg = (
                "Hicbir kripto anahtar kelime tespit edilmedi; LLM calistirilmadi."
                if status == "skipped-no-coin"
                else "Tree News hedef hesabın tweet metnini içermiyor; kayit sadece izleme amaclidir."
            )
            detail = reason or default_msg
            note = f" | note={detail}"
    url = entry.get("tweet_url") or entry.get("url") or ""
    return (
        f"{entry.get('logged_at', 'n/a')} | @{handle} | {coin_text} | "
        f"sentiment={final}{shill_info}{note} | text=\"{tweet_text}\" | {url}"
    )


class SentimentStream:
    """Minimal Tree News consumer that forwards tweets to the logger."""

    def __init__(self, api_key: str, tweet_logger: TweetSentimentLogger):
        self.api_key = api_key
        self.tweet_logger = tweet_logger
        self._running = True
        self._seen_ids: Deque[str] = deque(maxlen=2048)
        self._seen_set: Set[str] = set()
        self._seen_lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._active_ws: Set[aiohttp.ClientWebSocketResponse] = set()
        self._active_sessions: Set[aiohttp.ClientSession] = set()
        self._tasks: Set[asyncio.Task] = set()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        LOGGER.info("Shutdown requested; stopping streams…")
        if self._loop and self._loop.is_running():
            for task in list(self._tasks):
                self._loop.call_soon_threadsafe(task.cancel)
            for ws in list(self._active_ws):
                self._loop.call_soon_threadsafe(lambda w=ws: asyncio.create_task(w.close()))
            for session in list(self._active_sessions):
                self._loop.call_soon_threadsafe(lambda s=session: asyncio.create_task(s.close()))
        else:  # fallback if loop reference yoksa
            for task in list(self._tasks):
                task.cancel()
            for ws in list(self._active_ws):
                try:
                    asyncio.create_task(ws.close())
                except RuntimeError:
                    pass
            for session in list(self._active_sessions):
                try:
                    asyncio.create_task(session.close())
                except RuntimeError:
                    pass

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        tasks = [
            asyncio.create_task(self._run_client("TOKYO", TOKYO_WS_URL)),
            asyncio.create_task(self._run_client("MAIN", MAIN_WS_URL)),
        ]
        self._tasks = set(tasks)
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    LOGGER.warning("Stream task error: %s", result)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()
            LOGGER.info("Streams stopped.")

    async def _run_client(self, name: str, url: str) -> None:
        connector = aiohttp.TCPConnector(limit=32)
        timeout = ClientTimeout(total=30, sock_read=WS_RECEIVE_TIMEOUT)
        backoff = RECONNECT_BASE_DELAY

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self._active_sessions.add(session)
            try:
                while self._running:
                    try:
                        LOGGER.info("[%s] Connecting to %s", name, url)
                        ws = await session.ws_connect(
                            url,
                            heartbeat=WS_HEARTBEAT,
                            timeout=ClientWSTimeout(
                                ws_receive=WS_RECEIVE_TIMEOUT,
                                ws_close=WS_RECEIVE_TIMEOUT,
                            ),
                        )
                        await ws.send_str(f"login {self.api_key}")
                        LOGGER.info("[%s] Logged in", name)
                        backoff = RECONNECT_BASE_DELAY
                        self._active_ws.add(ws)
                        try:
                            await self._consume_ws(name, ws)
                        finally:
                            self._active_ws.discard(ws)
                    except asyncio.CancelledError:  # pragma: no cover - normal during shutdown
                        raise
                    except Exception as exc:
                        LOGGER.warning("[%s] Connection error: %s", name, exc)
                        if not self._running:
                            break
                        jitter = random.uniform(1 - RECONNECT_JITTER, 1 + RECONNECT_JITTER)
                        wait_for = min(backoff * jitter, RECONNECT_MAX_DELAY)
                        LOGGER.info("[%s] Reconnecting in %.1fs", name, wait_for)
                        await asyncio.sleep(wait_for)
                        backoff = min(backoff * 2, RECONNECT_MAX_DELAY)
            finally:
                self._active_sessions.discard(session)

    async def _consume_ws(self, name: str, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                if not self._running:
                    break
                if msg.type == WSMsgType.TEXT:
                    await self._handle_payload(name, msg.data)
                elif msg.type == WSMsgType.ERROR:
                    LOGGER.warning("[%s] WebSocket error: %s", name, ws.exception())
                    break
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    LOGGER.info("[%s] WebSocket closed", name)
                    break
        finally:
            await ws.close()

    async def _handle_payload(self, name: str, payload: str) -> None:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            LOGGER.debug("[%s] Non-JSON payload ignored", name)
            return

        if not isinstance(data, dict):
            return
        source = (data.get("source") or "").upper()
        if source:
            if "TWITTER" not in source and source not in {"SOCIAL", "X"}:
                return

        message_id = str(data.get("_id") or data.get("id") or "")
        if message_id and not await self._mark_seen(message_id):
            return

        self.tweet_logger.submit(data)

    async def _mark_seen(self, message_id: str) -> bool:
        async with self._seen_lock:
            if message_id in self._seen_set:
                return False
            if len(self._seen_ids) == self._seen_ids.maxlen:
                oldest = self._seen_ids.popleft()
                self._seen_set.discard(oldest)
            self._seen_ids.append(message_id)
            self._seen_set.add(message_id)
        return True


def build_tweet_logger(verbose: bool) -> TweetSentimentLogger:
    cmc_path = Path(os.getenv("TREE_NEWS_CMC_PATH", "var/coinmarketcap_data.json")).resolve()
    log_path = Path(os.getenv("TREE_NEWS_TWEET_LOG", "var/logs/tweet_sentiment.log")).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    sentiment_mode = os.getenv("TREE_NEWS_ENABLE_SENTIMENT_ANALYSIS", "true").strip().lower()
    enable_sentiment = sentiment_mode not in {"0", "false", "off", "no"}

    try:
        timeout_env = float(os.getenv("TREE_NEWS_SENTIMENT_INIT_TIMEOUT", "5"))
        sentiment_timeout = max(0.5, timeout_env)
    except ValueError:
        sentiment_timeout = 5.0
        LOGGER.warning("TREE_NEWS_SENTIMENT_INIT_TIMEOUT is invalid; defaulting to 5.0s")

    model_id = os.getenv("LMSTUDIO_MODEL")

    def console_callback(entry: Dict) -> None:
        line = format_entry(entry)
        if verbose:
            sentiment = entry.get("sentiment")
            LOGGER.debug("Sentiment payload: %s", json.dumps(sentiment, ensure_ascii=False))
        print(line, flush=True)

    return TweetSentimentLogger(
        cmc_path=cmc_path,
        log_path=log_path,
        lm_model=model_id,
        enable_sentiment=enable_sentiment,
        sentiment_init_timeout=sentiment_timeout,
        entry_callback=console_callback,
    )


async def async_main(verbose: bool) -> int:
    load_dotenv()
    api_key = os.getenv("TREE_NEWS_API_KEY")
    if not api_key:
        LOGGER.error("TREE_NEWS_API_KEY is missing. Please set it in your environment or .env file.")
        return 1

    tweet_logger = build_tweet_logger(verbose=verbose)
    stream = SentimentStream(api_key=api_key, tweet_logger=tweet_logger)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stream.stop)
        except NotImplementedError:  # Windows fallback
            signal.signal(sig, lambda *_args: stream.stop())

    await stream.run()
    return 0


def main() -> None:
    verbose = "--verbose" in sys.argv
    configure_logging(verbose)

    try:
        exit_code = asyncio.run(async_main(verbose=verbose))
    except KeyboardInterrupt:  # pragma: no cover - user interrupt
        exit_code = 0
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
