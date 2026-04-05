#!/usr/bin/env python3
"""Tree News tweet sentiment logger.

Watches whitelisted accounts, validates mentioned coins against the local
CoinMarketCap cache, and runs cloud-based sentiment analysis for every tweet
that references a tracked asset. Results are appended to
var/logs/tweet_sentiment.log as JSON lines for downstream bots.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
import importlib.util
from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

CASHTAG_RE = re.compile(r"\$([A-Za-z][A-Za-z0-9\-]{1,14})\b")
PAREN_RE = re.compile(r"\(([A-Za-z][A-Za-z0-9\-]{1,14})\)")
HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,20}$")

# Major coins to exclude from shill detection (too big to pump from influencer tweets)
MAJOR_COINS_BLACKLIST: Set[str] = {
    # Top 20 by market cap - these don't pump from tweets
    "BTC", "BITCOIN",
    "ETH", "ETHEREUM",
    "USDT", "TETHER",
    "BNB", "BINANCE",
    "USDC",
    "XRP", "RIPPLE",
    "SOL", "SOLANA",
    "ADA", "CARDANO",
    "DOGE", "DOGECOIN",
    "TRX", "TRON",
    "TON", "TONCOIN",
    "DOT", "POLKADOT",
    "MATIC", "POLYGON",
    "LTC", "LITECOIN",
    "SHIB", "SHIBAINU",
    "AVAX", "AVALANCHE",
    "LINK", "CHAINLINK",
    "XLM", "STELLAR",
    "BCH",
    "UNI", "UNISWAP",
    # Stablecoins
    "DAI", "BUSD", "TUSD", "USDP", "GUSD", "FRAX", "LUSD", "USDD",
    # Wrapped tokens
    "WBTC", "WETH", "STETH", "CBETH",
}
HANDLE_ALIASES = {
    # Farcaster/ENS style handles mapped to their Twitter equivalents
    "vitalik.eth": "vitalikbuterin",
}
TARGET_TWITTER_IDS = {
    # twitterId -> handle for sources that omit screen_name
    "295218901": "vitalikbuterin",  # Vitalik Buterin
}
HANDLE_FROM_TEXT_RE = re.compile(r"@([A-Za-z0-9_.-]{1,20})")


def _normalize_handle(candidate: Optional[str]) -> Optional[str]:
    """Return a sanitized Twitter handle if it looks valid."""
    if not candidate:
        return None
    handle = candidate.replace("@", "").strip().lower()
    if handle in HANDLE_ALIASES:
        return HANDLE_ALIASES[handle]
    if HANDLE_RE.match(handle):
        return handle
    return None


def _extract_handle_from_text(text: str) -> Optional[str]:
    match = HANDLE_FROM_TEXT_RE.search(text)
    if match:
        return _normalize_handle(match.group(1))
    match = re.search(r"\(@([A-Za-z0-9_.-]+)\)", text)
    if match:
        return _normalize_handle(match.group(1))
    return None


def _load_sentiment_module() -> Optional[object]:
    """Dynamically load sentiment-llm/sentiment_cli.py."""
    script_path = Path(__file__).resolve().parent / "sentiment-llm" / "sentiment_cli.py"
    if not script_path.exists():
        LOGGER.warning("sentiment_cli.py not found: %s", script_path)
        return None
    try:
        spec = importlib.util.spec_from_file_location("tweet_sentiment_cli", script_path)
        if spec is None or spec.loader is None:
            LOGGER.warning("Unable to build import spec for sentiment_cli.py")
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[call-arg]
        return module
    except Exception as exc:
        LOGGER.exception("Failed to import sentiment_cli.py: %s", exc)
        return None


class TweetSentimentLogger:
    """Helper that monitors select handles and logs their sentiment metadata."""

    TARGET_HANDLES: Set[str] = {
        "rewkang",
        "cz_binance",
        "vitalikbuterin",
        "naval",
        "pmarca",
        "sama",
        "brian_armstrong",
        "gcrclassic",
        "cobie",
        "hsakatrades",
        "zhusu",
    }

    def __init__(
        self,
        cmc_path: Path,
        log_path: Path = Path("var/logs/tweet_sentiment.log"),
        lm_model: Optional[str] = None,
        *,
        enable_sentiment: bool = True,
        sentiment_init_timeout: float = 5.0,
        entry_callback: Optional[Callable[[Dict], None]] = None,
    ):
        self.cmc_path = cmc_path
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.coin_data: Dict[str, Dict[str, object]] = {}
        self.name_to_symbol: Dict[str, str] = {}
        self._load_coinmarketcap()

        self.sentiment_module = _load_sentiment_module() if enable_sentiment else None
        self.classifier = None
        self._classifier_lock = threading.Lock()
        self._enabled = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._entry_callback = entry_callback

        trace_path_env = os.getenv("TREE_NEWS_TARGET_TRACE_LOG", "var/logs/target_handles.log").strip()
        if trace_path_env:
            trace_path = Path(trace_path_env).resolve()
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            self.trace_log_path: Optional[Path] = trace_path
        else:
            self.trace_log_path = None

        if not enable_sentiment:
            LOGGER.info("Sentiment analysis disabled; tweets will be logged without model output.")
        elif self.sentiment_module is None:
            LOGGER.warning("Sentiment module could not be loaded; logging will continue without it.")

        self._enabled = True

        if self.sentiment_module and enable_sentiment:
            self._init_classifier(lm_model=lm_model, timeout=sentiment_init_timeout)

    def _init_classifier(self, lm_model: Optional[str], timeout: float) -> None:
        module = self.sentiment_module
        if module is None:
            return

        backend = (os.getenv("SENTIMENT_BACKEND") or "bedrock").strip().lower()
        if backend and backend != "bedrock":
            LOGGER.warning("Only the Bedrock backend is supported; forcing backend=bedrock.")

        BedrockClaudeConfig = getattr(module, "BedrockClaudeConfig", None)
        BedrockClaudeClassifier = getattr(module, "BedrockClaudeClassifier", None)
        if BedrockClaudeConfig is None or BedrockClaudeClassifier is None:
            LOGGER.warning("Bedrock backend classes are unavailable; sentiment logging disabled.")
            return

        aws_key = (os.getenv("AWS_ACCESS_KEY_ID") or "").strip() or None
        aws_secret = (os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip() or None
        aws_token = (os.getenv("AWS_SESSION_TOKEN") or "").strip() or None
        api_key = (
            os.getenv("BEDROCK_API_KEY")
            or os.getenv("API_KEY")
            or os.getenv("BEDROCK_API_TOKEN")
            or ""
        ).strip() or None
        bearer_token = (
            os.getenv("BEDROCK_BEARER_TOKEN")
            or os.getenv("AWS_BEARER_TOKEN_BEDROCK")
            or ""
        ).strip() or None
        if not ((aws_key and aws_secret) or api_key or bearer_token):
            LOGGER.error(
                "Bedrock sentiment backend needs AWS creds or BEDROCK_API_KEY / BEDROCK_BEARER_TOKEN."
            )
            return

        model_id = (
            os.getenv("BEDROCK_MODEL")
            or lm_model
            or "anthropic.claude-3-5-sonnet-20241022-v2:0"
        ).strip()
        region = os.getenv("BEDROCK_REGION", "ap-northeast-1").strip() or "ap-northeast-1"
        temp = float(os.getenv("BEDROCK_TEMP", "0.0"))
        top_p = float(os.getenv("BEDROCK_TOP_P", "0.9"))
        retries = max(0, int(os.getenv("BEDROCK_RETRIES", "2")))
        retry_backoff = float(os.getenv("BEDROCK_RETRY_BACKOFF", "0.25"))
        max_output = max(1, int(os.getenv("BEDROCK_MAX_OUTPUT", "32")))
        min_interval = float(os.getenv("BEDROCK_MIN_INTERVAL", "0.08"))
        timeout = float(os.getenv("BEDROCK_TIMEOUT", "15"))
        rps_env = os.getenv("BEDROCK_RPS")
        if rps_env:
            try:
                rps = float(rps_env)
                if rps > 0:
                    min_interval = 1.0 / rps
            except ValueError:
                LOGGER.warning("BEDROCK_RPS is not a valid float; using min_interval=%s", min_interval)

        cfg = BedrockClaudeConfig(
            model_id=model_id,
            region=region,
            aws_access_key=aws_key,
            aws_secret_key=aws_secret,
            aws_session_token=aws_token,
            api_key=api_key,
            bearer_token=bearer_token,
            temp=temp,
            top_p=top_p,
            max_output_tokens=max_output,
            retries=retries,
            retry_backoff=retry_backoff,
            min_interval=min_interval,
            timeout=timeout,
        )

        result_box: Dict[str, object] = {}
        error_box: Dict[str, BaseException] = {}
        done_event = threading.Event()

        def _build_classifier() -> None:
            try:
                classifier = BedrockClaudeClassifier(cfg, set_timeout=None)
                result_box["classifier"] = classifier
            except BaseException as exc:
                error_box["error"] = exc
            finally:
                done_event.set()

        thread = threading.Thread(
            target=_build_classifier,
            name="tweet-sentiment-init",
            daemon=True,
        )
        thread.start()

        wait_timeout = max(0.5, float(timeout or 5.0))
        if not done_event.wait(wait_timeout):
            LOGGER.warning(
                "Bedrock sentiment classifier did not initialize within %.1fs; sentiment disabled.",
                wait_timeout,
            )
        elif error_box:
            LOGGER.warning("Bedrock sentiment classifier failed to start: %s", error_box["error"])
        else:
            self.classifier = result_box["classifier"]  # type: ignore[assignment]
            LOGGER.info(
                "Tweet sentiment logging enabled via Bedrock (model=%s, region=%s).",
                model_id,
                region,
            )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def submit(self, data: Dict) -> None:
        """Enqueue a Tree News message for processing."""
        if not self._enabled:
            return
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                LOGGER.debug("No running event loop; skipping tweet sentiment log.")
                return
        payload = copy.deepcopy(data)
        self._loop.create_task(self._handle(payload))

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    async def _handle(self, data: Dict) -> None:
        try:
            handle = self._extract_handle(data)
            if not handle or handle not in self.TARGET_HANDLES:
                return

            tweet_text = self._extract_text(data)
            coins: Set[str] = set()
            if not tweet_text:
                sentiment = {
                    "status": "skipped-no-text",
                    "reason": "Tree News payload does not include tweet text; recorded for trace only.",
                }
            else:
                coins = self._detect_coins(tweet_text, data)
                if coins:
                    sentiment = await self._classify_async(tweet_text, sorted(coins))
                else:
                    sentiment = {
                        "status": "skipped-no-coin",
                        "reason": "Tweet does not mention a tracked coin/ticker; LLM not executed.",
                    }
            log_entry = self._build_log_entry(data, handle, tweet_text, coins, sentiment)
            if self._entry_callback:
                try:
                    self._entry_callback(copy.deepcopy(log_entry))
                except Exception:
                    LOGGER.exception("Tweet sentiment callback failed")
            await self._write_log(log_entry)
            await self._write_trace_entry(log_entry, sentiment)
        except Exception as exc:
            LOGGER.exception("Tweet sentiment error: %s", exc)

    def _extract_handle(self, data: Dict) -> Optional[str]:
        def _first_valid_handle(*candidates: Optional[str]) -> Optional[str]:
            for candidate in candidates:
                normalized = _normalize_handle(candidate)
                if normalized:
                    return normalized
            return None

        author = data.get("author")
        if isinstance(author, dict):
            handle = _first_valid_handle(
                author.get("screen_name"),
                author.get("username"),
                author.get("handle"),
                author.get("name"),
            )
            if handle:
                return handle
        elif isinstance(author, str):
            handle = _extract_handle_from_text(author)
            if handle:
                return handle

        direct_handle = data.get("handle")
        if isinstance(direct_handle, str):
            handle = _normalize_handle(direct_handle) or _extract_handle_from_text(direct_handle)
            if handle:
                return handle

        title = data.get("title") or ""
        handle = _extract_handle_from_text(title)
        if handle:
            return handle

        info = data.get("info")
        if isinstance(info, dict):
            tid = info.get("twitterId")
            if tid:
                mapped = TARGET_TWITTER_IDS.get(str(tid))
                if mapped:
                    return mapped
            handle = _first_valid_handle(
                info.get("screenName"),
                info.get("screen_name"),
                info.get("twitterHandle"),
                info.get("twitterUsername"),
                info.get("username"),
            )
            if handle:
                return handle


        url = data.get("url") or data.get("tweet_url")
        if isinstance(url, str) and url:
            try:
                parsed = urlparse(url)
                parts = [p for p in parsed.path.split("/") if p]
                if parts:
                    handle = _normalize_handle(parts[0])
                    if handle and handle not in {"i", "status", "statuses"}:
                        return handle
            except Exception:
                pass
        return None

    def _extract_text(self, data: Dict) -> str:
        body = data.get("body") or ""
        text = data.get("text") or ""
        if body and body.strip():
            return str(body).strip()
        if text and text.strip():
            return str(text).strip()
        return ""

    def _detect_coins(self, text: str, data: Dict) -> Set[str]:
        candidates: Set[str] = set()

        suggestions = data.get("suggestions")
        if isinstance(suggestions, list):
            for suggestion in suggestions:
                if isinstance(suggestion, dict):
                    coin = suggestion.get("coin")
                    if isinstance(coin, str):
                        candidates.add(coin.upper())

        for pattern in (CASHTAG_RE, PAREN_RE):
            for match in pattern.findall(text):
                candidates.add(match.upper())

        text_lower = text.lower()
        for name, symbol in self.name_to_symbol.items():
            if symbol in candidates:
                continue
            if name and name in text_lower:
                candidates.add(symbol)

        if not candidates:
            return set()

        # CoinMarketCap aliases (name/slug) already populate `candidates`,
        # so we return the full set without additional filtering here.
        return set(candidates)

    async def _classify_async(self, text: str, tokens: List[str]) -> Dict:
        if not self.classifier:
            return {"status": "classifier-unavailable"}

        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._classify_sync, text, tokens)
        except Exception as exc:  # safeguard if the executor task raises
            LOGGER.error("Sentiment classify async failed: %s", exc)
            return {"status": "classifier-error", "reason": str(exc)}

    def _classify_sync(self, text: str, tokens: List[str]) -> Dict:
        try:
            # Filter out major coins - they don't pump from influencer tweets
            tradeable_tokens = [t for t in tokens if t.upper() not in MAJOR_COINS_BLACKLIST]

            with self._classifier_lock:
                general = self.classifier.classify_general_multihead(text)
                targets = self.classifier.classify_targets(text, tokens) if tokens else {}

                # Shill detection only for non-major coins
                shills = {}
                if tradeable_tokens and hasattr(self.classifier, "classify_shills"):
                    shills = self.classifier.classify_shills(text, tradeable_tokens)

            # Determine which coins are being shilled
            shilled_coins = [
                coin for coin, result in shills.items()
                if result.get("shill") == "SHILL"
            ]

            return {
                "general": general,
                "targets": targets,
                "shills": shills,
                "shilled_coins": shilled_coins,
                "has_shill": len(shilled_coins) > 0,
            }
        except Exception as exc:
            LOGGER.error("Sentiment classify failed: %s", exc)
            return {"status": "classifier-error", "reason": str(exc)}

    def _build_log_entry(
        self,
        data: Dict,
        handle: str,
        tweet_text: str,
        coins: Set[str],
        sentiment: Dict,
    ) -> Dict:
        timestamp = datetime.now(timezone.utc).isoformat()
        coin_details = {
            symbol: {
                "cmc_id": self.coin_data[symbol].get("cmc_id"),
                "name": self.coin_data[symbol].get("name"),
                "slug": self.coin_data[symbol].get("slug"),
                "market_cap": self.coin_data[symbol].get("market_cap"),
            }
            for symbol in coins
            if symbol in self.coin_data
        }
        return {
            "logged_at": timestamp,
            "news_time": data.get("time"),
            "tree_news_id": data.get("_id"),
            "tweet_url": data.get("url"),
            "handle": handle,
            "text": tweet_text,
            "coins": sorted(coins),
            "coin_details": coin_details,
            "sentiment": sentiment,
        }

    async def _write_log(self, entry: Dict) -> None:
        line = json.dumps(entry, ensure_ascii=False)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._append_line, line)

    async def _write_trace_entry(self, entry: Dict, sentiment: Dict) -> None:
        if not self.trace_log_path:
            return
        status = "classified"
        reason = None
        if isinstance(sentiment, dict):
            status = sentiment.get("status", "classified")
            reason = sentiment.get("reason")
        trace_payload = {
            "logged_at": entry.get("logged_at"),
            "handle": entry.get("handle"),
            "status": status,
            "reason": reason,
            "coins": entry.get("coins"),
            "tweet_url": entry.get("tweet_url"),
            "tree_news_id": entry.get("tree_news_id"),
            "text_preview": (entry.get("text") or "")[:200],
        }
        line = json.dumps(trace_payload, ensure_ascii=False)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._append_trace_line, line)

    def _append_line(self, line: str) -> None:
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.write("\n")

    def _append_trace_line(self, line: str) -> None:
        if not self.trace_log_path:
            return
        with open(self.trace_log_path, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.write("\n")

    def _load_coinmarketcap(self) -> None:
        if not self.cmc_path.exists():
            LOGGER.warning("CoinMarketCap cache not found: %s", self.cmc_path)
            return
        try:
            with open(self.cmc_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            LOGGER.warning("CoinMarketCap cache could not be read (%s): %s", self.cmc_path, exc)
            return
        if not isinstance(data, dict):
            LOGGER.warning("CoinMarketCap cache is not in the expected format.")
            return

        for symbol, payload in data.items():
            if not isinstance(symbol, str) or not isinstance(payload, dict):
                continue
            upper_symbol = symbol.upper()
            self.coin_data[upper_symbol] = payload
            name = payload.get("name")
            if isinstance(name, str):
                self.name_to_symbol[name.lower()] = upper_symbol
            slug = payload.get("slug")
            if isinstance(slug, str):
                self.name_to_symbol[slug.lower()] = upper_symbol
