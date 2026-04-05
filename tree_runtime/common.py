#!/usr/bin/env python3
"""
Tree News signal ingestion runtime with optional Telegram backup.

This runtime monitors listing and investment signals, forwards filtered
events to the local WebSocket bus, and optionally refreshes the local
CoinMarketCap cache used by downstream filters.
"""

import asyncio
import json
import re
import os
import signal
import sys
import time
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import deque
import logging
import aiohttp
from aiohttp import web, WSMsgType, ClientTimeout, ClientWebSocketResponse, ClientWSTimeout
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

try:
    from coinmarketcap_loader import refresh_coinmarketcap_cache, CoinMarketCapError
except ImportError:  # pragma: no cover - optional dependency
    refresh_coinmarketcap_cache = None

    class CoinMarketCapError(RuntimeError):
        """Fallback error used when loader module is unavailable."""

# Telegram imports (optional)
try:
    from telethon import TelegramClient, events
    from telethon.tl.functions.channels import JoinChannelRequest
    from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("⚠️  Telethon not installed. Telegram backup disabled. Install with: pip install telethon")

# Load environment variables from the repo root after modular split.
REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:  # pragma: no cover - fallback for unusual launch contexts
    load_dotenv()


def _resolve_repo_path(path_value: str) -> str:
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return str(path)

# ----- Trusted kaynaklar (sosyal kaynakta link zorunluysa) -----
TRUSTED_DOMAINS = (
    'coindesk.com','cointelegraph.com','reuters.com','bloomberg.com',
    'wsj.com','ft.com','financefeeds.com','theblock.co','fortune.com',
    'forbes.com','cnbc.com','seekingalpha.com',
    # Press wires
    'globenewswire.com','businesswire.com','prnewswire.com','newswire.com',
    'sec.gov'
)

# --- Kaynak kara listesi (sosyal/aggregator hesaplar) ---
BLACKLISTED_AUTHORS = {
    'WUBLOCKCHAIN',  # X/Twitter hesabı
}

def _is_blacklisted(data: dict) -> bool:
    """Return True if message should be dropped due to blacklisted origin"""
    try:
        src = (data.get('source') or '').upper()
        title = (data.get('title') or '')
        body = (data.get('body') or '')
        text = (data.get('text') or '')

        # Social media: check author screen_name/name
        if src in ('TWITTER', 'TELEGRAM'):
            author = data.get('author') or {}
            handle = (author.get('screen_name') or author.get('name') or '').upper()
            handle = handle.replace('@', '').replace(' ', '')
            if handle in BLACKLISTED_AUTHORS:
                return True
        # Fallback: title/body/text mentions (covers aggregator forwards)
        blob = f"{title} {body} {text}".upper()
        if 'WUBLOCKCHAIN' in blob.replace(' ', ''):
            return True
        return False
    except Exception:
        return False

# ----- Zaman dilimleri: token sanılmasın -----
TIMEZONES = {'UTC','JST','KST','PST','PDT','EST','EDT','CST','CDT','GMT','BST','CET','CEST','EET','EEST','HKT','SGT'}

ACRONYM_BLACKLIST = {
    'GA', 'AI', 'ML', 'GPU', 'TPU', 'API', 'SDK', 'LLM', 'DB', 'SQL', 'RPC', 'HTTP', 'HTTPS', 'WWW', 'A2A'
}

STOPWORDS = {
    'IN','OF','TO','ON','AT','BY','FOR','AND','THE','ARE','IS','A','AN',
    'WITH','FROM','OVER','UP','OUT','OFF','OR','AS',
    # Common finance acronyms that should never be tokens
    'FDV','TVL'
}

# Certain industry acronyms repeatedly appear in partnership/investment
# announcements (e.g. Zero-Knowledge Proof -> ZKP) and should not be
# treated as tradeable tickers unless they are referenced outside their
# descriptive context.
CONTEXT_TOKEN_FILTERS: Dict[str, Set[str]] = {
    'ZKP': {'ZERO', 'KNOWLEDGE', 'PROOF', 'PROOFS'},
    'AVS': {'ACTIVELY', 'VALIDATED', 'SERVICE', 'SERVICES'},
    'AP2': {'ALTITUDE', 'POINT', 'POINTS'},
    'GA': {'GENERAL', 'AVAILABILITY'},
    'A2A': {'ACCOUNT', 'TO', 'ACCOUNT'},
}

# ----- Hisse/sermaye bağlamı (equity) -----
EQUITY_KEYWORDS = re.compile(
    r'share buyback|buyback|repurchase|repurchasing|board[- ]authorized|'
    r'outstanding shares|common stock|dividend|eps|nasdaq|nyse|sec filing|10b-18',
    re.IGNORECASE
)

# ----- Investment negatif bağlam filtresi -----
NEGATIVE_CONTEXT = re.compile(
    r'hack|exploit|attack|security|breach|cve|npm|malware|supply\s+chain|vulnerability|update|patch|audit',
    re.IGNORECASE
)

# ----- Metrik blacklist (yatırım değil, kullanıcı/indirme sayısı) -----
METRIC_BLACKLIST = re.compile(
    r'\b(users?|downloads?|visitors?|pageviews?|transactions?|accounts?)\b',
    re.IGNORECASE
)


# ----- "Mega marka" anlaşmaları (yalnızca çok büyükler) -----
# Sadece aşağıdaki büyük firmalar partnership sinyali üretebilir.
# Gerekirse genişletilebilir; varsayılanı sıkı tutuyoruz.
MEGABRANDS = {
    'PAYPAL', 'APPLE', 'MICROSOFT', 'NVIDIA', 'GOOGLE', 'ALPHABET', 'META', 'FACEBOOK'
}
DEAL_KEYWORDS = re.compile(
    r'\b('
    r'partners?\s+(?:with|for)|'
    r'partnership\s+(?:with|for)|'
    r'integrat(?:es|ion)\s+(?:with|into)|'
    r'agreement\s+(?:with|for)|'
    r'collaborat(?:es|ion)\s+(?:with|on)|'
    r'strategic\s+(?:alliance|partnership)\s+(?:with|for)|'
    r'launch\s+partner\s+(?:with|for)|'
    r'(?:selected|chosen)\s+as\s+(?:an?\s+)?(?:launch\s+)?partner|'
    r'joins?\s+forces\s+with|'
    r'co-?develops?\s+(?:with|for)|'
    r'co-launch(?:es)?\s+(?:with|alongside)'
    r')\b',
    re.IGNORECASE
)

def _trusted_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        host = urlparse(url).netloc.lower()
        return any(host.endswith(d) for d in TRUSTED_DOMAINS)
    except Exception:
        return False


def _clean_announcement_text(text: str) -> str:
    if not text:
        return ""
    cleaned = HTML_TAG_PATTERN.sub(" ", text)
    cleaned = cleaned.replace("\u200b", " ")
    cleaned = MULTISPACE_PATTERN.sub(" ", cleaned)
    return cleaned.strip()


def _is_verified_social_announcement(data: dict) -> bool:
    """Strict social verification when no trusted URL is present."""
    try:
        source = (data.get('source') or '').upper()
        url = (data.get('url') or '').lower()
        is_social_source = source in {'TWITTER', 'TELEGRAM', 'SOCIAL'}
        is_social_url = any(domain in url for domain in ('twitter.com', 'x.com', 't.me', 'telegram.me', 'telegram.org'))
        if not (is_social_source or is_social_url):
            return False

        author = data.get('author') or {}
        handle = (author.get('screen_name') or author.get('name') or '').strip()
        return bool(handle)
    except Exception:
        return False

    return False

def _has_equity_context(text: str) -> bool:
    return bool(EQUITY_KEYWORDS.search(text or ''))

def _find_megabrand(text: str) -> Optional[str]:
    t = (text or '').upper()
    # X/Twitter özel-casesi kaldırıldı; yalnızca açık marka adları kabul.
    for b in MEGABRANDS:
        # Tam kelime eşleşmesi (substring false positive'larını engelle)
        if re.search(rf"\b{re.escape(b)}\b", t):
            return b
    return None

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Hybrid Signal Server")
    parser.add_argument(
        "--test",
        dest="test_tokens",
        nargs="?",
        action="append",
        const=None,
        help="Enable test mode; optionally pass a token symbol (repeatable).",
    )
    parser.add_argument(
        "--test-token",
        dest="legacy_test_token",
        default=None,
        help="Backwards-compatible token selector when used with --test.",
    )
    parser.add_argument(
        "--test-delay",
        type=float,
        default=30.0,
        help="Seconds to wait before emitting test signal (default: 30)",
    )
    parser.add_argument(
        "--test-spacing",
        type=float,
        default=3.0,
        help="Delay between multiple test tokens (seconds).",
    )
    parser.add_argument(
        "--test-source",
        default="UPBIT_TEST",
        help='Signal "source" value (e.g., UPBIT_TEST or UPBIT to mimic real)',
    )
    parser.add_argument(
        "--test-listing-type",
        default="KRW",
        help="Listing type to mimic (KRW/BTC/USDT) (default: KRW)",
    )
    parser.add_argument(
        "--test-profile",
        choices=["upbit_krw"],
        help="Shortcut profile that applies exchange-specific defaults for tests.",
    )
    parser.add_argument(
        "--test-force-strategy",
        action="store_true",
        help="Flag test signals so trader applies the full strategy logic (no auto exit).",
    )

    args = parser.parse_args()

    tokens: List[str] = []
    if args.test_tokens:
        fallback_token = (args.legacy_test_token or "TAO").strip().upper()
        for raw_token in args.test_tokens:
            token = (raw_token or fallback_token or "TAO").strip().upper()
            if token:
                tokens.append(token)

    if tokens:
        args.test = True
        args.test_tokens = tokens
        args.test_token = tokens[0]
    else:
        args.test = False
        args.test_tokens = []
        args.test_token = (args.legacy_test_token or "TAO").strip().upper()

    profile = (args.test_profile or "").lower()
    default_source = (parser.get_default("test_source") or "").strip().upper()
    default_listing_type = (parser.get_default("test_listing_type") or "").strip().upper()

    # Apply profile-specific defaults and behaviour tweaks
    if profile == "upbit_krw":
        args.test_force_strategy = True
        current_source = (args.test_source or "").strip().upper()
        current_listing = (args.test_listing_type or "").strip().upper()
        if current_source in ("", default_source):
            args.test_source = "UPBIT_TEST"
        if current_listing in ("", default_listing_type):
            args.test_listing_type = "KRW"
    args.test_source = (args.test_source or "").strip().upper()
    args.test_listing_type = (args.test_listing_type or "").strip().upper() or None

    return args

# ==========================
# Configuration
# ==========================

# Tree News API
API_KEY = os.getenv("TREE_NEWS_API_KEY", "")
if not API_KEY:
    print("⚠️  Warning: TREE_NEWS_API_KEY not set in .env file!")
    
TOKYO_WS_URL = "ws://tokyo.treeofalpha.com:5124"
MAIN_WS_URL = "wss://news.treeofalpha.com/ws"

# Tokyo timezone
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

# Telegram BWE (for Upbit backup)
TG_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TG_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TG_PHONE = os.getenv("TELEGRAM_PHONE_NUMBER", "")
TG_2FA = os.getenv("TELEGRAM_2FA_PASSWORD", "")
TG_SESSION = os.getenv("TG_SESSION", "hybrid_bwe_sess")
BWE_CHANNEL = os.getenv("BWE_CHANNEL_USERNAME", "BWEnews")
BWE_GROUP_ID = int(os.getenv("BWE_GROUP_ID", "0"))

# Upbit Official Channel
UPBIT_OFFICIAL_CHANNEL = os.getenv("UPBIT_OFFICIAL_CHANNEL_USERNAME", "upbit_news")
UPBIT_OFFICIAL_GROUP_ID = int(os.getenv("UPBIT_OFFICIAL_GROUP_ID", "0"))

# Server config
WS_SERVER_PORT = int(os.getenv("WS_PORT", "9999"))
# Bind host (default: localhost to avoid internet scanners). Set BIND_HOST=0.0.0.0 only if needed.
BIND_HOST = os.getenv("BIND_HOST", "127.0.0.1")
ENABLE_WS_SERVER = os.getenv("TREE_NEWS_ENABLE_WS_SERVER", "true").lower() != "false"

# External relay publishing (optional)
RELAY_PUBLISH_URL = (
    os.getenv("SIGNAL_RELAY_PUBLISH_URL")
    or os.getenv("RELAY_PUBLISH_URL")
)
RELAY_API_KEY = (
    os.getenv("SIGNAL_RELAY_API_KEY")
    or os.getenv("RELAY_PUBLISH_TOKEN")
    or os.getenv("RELAY_API_KEY")
)
try:
    RELAY_TIMEOUT_SECONDS = max(0.5, float(os.getenv("SIGNAL_RELAY_TIMEOUT", "1.5")))
except ValueError:
    RELAY_TIMEOUT_SECONDS = 1.5

# Debug mode - shows social media messages
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

# ANSI Color codes for terminal
class Colors:
    BLUE = '\033[94m'      # Twitter
    GREEN = '\033[92m'     # Telegram
    YELLOW = '\033[93m'    # Blogs
    MAGENTA = '\033[95m'   # Important/Listings
    CYAN = '\033[96m'      # URLs
    RED = '\033[91m'       # Alerts
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'
    GRAY = '\033[90m'

# Constants
RECONNECT_BASE_DELAY = 2.0
RECONNECT_MAX_DELAY = 60.0
RECONNECT_JITTER = 0.3
WS_HEARTBEAT = 30
WS_RECEIVE_TIMEOUT = 60
BROADCAST_TIMEOUT = 2.0
SIGNAL_QUEUE_SIZE = 1024
PROCESSED_SIGNALS_FILE = _resolve_repo_path(
    os.getenv("TREE_NEWS_PROCESSED_SIGNALS_FILE", "var/tree_news_processed_signals.json")
)
STATS_INTERVAL = 30
TREE_NEWS_STORE_FLUSH_INTERVAL = max(
    0.1, float(os.getenv("TREE_NEWS_STORE_FLUSH_INTERVAL", "1.0"))
)
TREE_NEWS_MESSAGE_QUEUE_SIZE = max(
    32, int(os.getenv("TREE_NEWS_MESSAGE_QUEUE_SIZE", "512"))
)
TREE_NEWS_MESSAGE_WORKERS = max(
    1, int(os.getenv("TREE_NEWS_MESSAGE_WORKERS", "4"))
)
TREE_NEWS_MESSAGE_QUEUE_PUT_TIMEOUT = max(
    0.0, float(os.getenv("TREE_NEWS_MESSAGE_QUEUE_PUT_TIMEOUT", "0.05"))
)

# Büyük market cap/stable coin'ler: partnership sinyallerinden hariç tut
MAJOR_EXCLUDED_COINS = {
    'ADA', 'AVAX', 'BCH', 'BNB', 'BTC', 'CRO', 'DOGE', 'DOT', 'ETH', 'HBAR', 'HYPE',
    'LEO', 'LINK', 'LTC', 'MKR', 'SHIB', 'SOL', 'SOLANA', 'SUI', 'TON', 'TRX', 'USDC',
    'USDT', 'USDE', 'USDX', 'USDD', 'USDP', 'BUSD', 'FDUSD', 'DAI', 'USD1', 'XLM',
    'XMR', 'XMKR', 'XRP'
}

# Partnership/mega-brand exclusions reuse major list and cover stablecoins
PARTNERSHIP_EXCLUDED_COINS = MAJOR_EXCLUDED_COINS | {
    'USDE', 'USDK', 'USDS', 'TUSD', 'GUSD'
}

HTML_TAG_PATTERN = re.compile(r'<[^>]*>')
MULTISPACE_PATTERN = re.compile(r'\s+')
BINANCE_SEED_TAG_PATTERN = re.compile(r'(?i)\bseed\s+tag\s+applied\b')
BINANCE_HODLER_PATTERN = re.compile(r'(?i)\bintroducing\b.+\bon\s+binance\s+hodler\b')
BINANCE_PROMO_TOKEN_EXCLUDES = {'BNB', 'USDT', 'ETH', 'BUSD', 'USDC', 'BTC', 'FDUSD', 'USD', 'EUR'}

LOW_CAP_PRIORITY_SOURCES = {'UPBIT', 'BITHUMB', 'BINANCE'}

ENABLE_PARTNERSHIP_SIGNALS = os.getenv('TREE_NEWS_ENABLE_PARTNERSHIPS', 'false').lower() == 'true'
INVESTMENT_MIN_AMOUNT_MILLIONS = max(0.0, float(os.getenv('TREE_NEWS_INVESTMENT_MIN_AMOUNT', '100')))
ENABLE_UPBIT_LISTINGS = os.getenv('TREE_NEWS_ENABLE_UPBIT_LISTINGS', 'false').lower() == 'true'
ENABLE_BITHUMB_LISTINGS = os.getenv('TREE_NEWS_ENABLE_BITHUMB_LISTINGS', 'true').lower() == 'true'

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
# Reduce aiohttp server parser noise
logging.getLogger('aiohttp.server').setLevel(logging.ERROR)

# ==========================
# Data Structures
# ==========================

@dataclass
class Signal:
    """Signal data structure for important events"""
    source: str  # UPBIT, BITHUMB, INVESTMENT, UPBIT_TELEGRAM
    tokens: List[str]
    announcement: str
    token_address: Optional[str] = None  # Optional EVM contract address (0x...)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    alert_type: str = "listing"
    listing_type: Optional[str] = None
    amount_millions: Optional[float] = None
    amount_formatted: Optional[str] = None
    hash: Optional[str] = None
    telegram_source: bool = False  # Flag for Telegram source
    response_time_ms: Optional[int] = None
    url: Optional[str] = None
    notice_id: Optional[str] = None
    company: Optional[str] = None  # Company/brand metadata when relevant
    body: Optional[str] = None  # For message body
    channel: Optional[str] = None  # For Telegram channel name
    force_strategy: bool = False  # When True, downstream trader should apply full strategy logic

    def to_dict(self) -> Dict:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        data["type"] = "signal"
        return data


class RelayPublisher:
    """Optional helper that pushes signals to an external relay service."""

    def __init__(self, url: str, api_key: Optional[str] = None, timeout_seconds: float = 1.5) -> None:
        self.url = url
        self.api_key = api_key
        self.timeout = ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("relay_publisher")

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(timeout=self.timeout)
            return self._session

    async def publish(self, payload: Dict[str, Any]) -> bool:
        session = await self._get_session()
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            async with session.post(self.url, json=payload, headers=headers) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    self._logger.warning(
                        "Relay publish failed (status=%s): %s",
                        resp.status,
                        text[:200],
                    )
                    return False
        except Exception as exc:  # pragma: no cover - network failure path
            self._logger.error("Relay publish error: %s", exc)
            return False

        return True

    async def close(self) -> None:
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()


def _parse_threshold_env(var_name: str, default: int, *, min_value: int = 0) -> int:
    """Parse integer from environment allowing underscores/commas."""
    raw = os.getenv(var_name)
    if raw is None or raw.strip() == "":
        return default

    try:
        cleaned = re.sub(r"[^0-9]", "", raw)
        if not cleaned:
            raise ValueError
        value = int(cleaned)
        if value < min_value:
            raise ValueError
        return value
    except ValueError:
        logger.warning(
            "Invalid value for %s=%r, using default %s", var_name, raw, default
        )
        return default


class MarketCapFilter:
    """Load CoinMarketCap cache and drop high market-cap listings early."""

    def __init__(self, data_file: Optional[str] = None):
        configured_path = data_file or os.getenv(
            "TREE_NEWS_CMC_FILE", "var/coinmarketcap_data.json"
        )
        self.data_file = _resolve_repo_path(configured_path)
        self.coin_data: Dict[str, Dict[str, Any]] = {}
        self.enabled = False
        self.last_loaded_at: Optional[str] = None
        self.enabled = self._load_cache()

        self.default_threshold = _parse_threshold_env(
            "TREE_NEWS_MARKET_CAP_LIMIT", 400_000_000
        )
        self.source_thresholds = {
            "BITHUMB": _parse_threshold_env(
                "TREE_NEWS_MARKET_CAP_LIMIT_BITHUMB", 50_000_000
            ),
            "UPBIT": _parse_threshold_env(
                "TREE_NEWS_MARKET_CAP_LIMIT_UPBIT", 60_000_000
            ),
        }

    def _load_cache(self) -> bool:
        try:
            with open(self.data_file, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            logger.warning(
                "Market cap cache %s not found; listing filter disabled", self.data_file
            )
            return False
        except json.JSONDecodeError as exc:
            logger.warning(
                "Market cap cache %s is invalid JSON (%s); filter disabled",
                self.data_file,
                exc,
            )
            return False
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Failed to load market cap cache %s (%s); filter disabled",
                self.data_file,
                exc,
            )
            return False

        payload.pop("_metadata", None)
        coin_map: Dict[str, Dict[str, Any]] = {}
        for symbol, data in payload.items():
            if not isinstance(data, dict):
                continue
            coin_map[symbol.upper()] = data

        if not coin_map:
            logger.warning(
                "Market cap cache %s empty; listing filter disabled", self.data_file
            )
            return False

        self.coin_data = coin_map
        self.enabled = True
        self.last_loaded_at = datetime.now(timezone.utc).isoformat()
        logger.info(
            "Market cap filter loaded %d entries from %s",
            len(self.coin_data),
            self.data_file,
        )
        return True

    @staticmethod
    def _normalise_source(source: Optional[str]) -> str:
        if not source:
            return ""
        base = source.upper()
        base = re.sub(r"_TELEGRAM(?:_OFFICIAL)?$", "", base)
        base = re.sub(r"_OFFICIAL$", "", base)
        return base.rstrip("_")

    def should_apply(self, signal: Signal) -> bool:
        if not self.enabled or not signal.tokens:
            return False
        alert = (signal.alert_type or "").lower()
        if "listing" not in alert:
            return False
        base_source = self._normalise_source(signal.source)
        if base_source == "BINANCE_ANNOUNCEMENTS":
            return False
        return True

    def get_market_cap(self, symbol: str) -> Optional[float]:
        entry = self.coin_data.get(symbol.upper())
        if not entry:
            return None
        return entry.get("market_cap")

    def filter_signal(
        self, signal: Signal
    ) -> Tuple[List[str], List[Tuple[str, Optional[float]]], int]:
        base_source = self._normalise_source(signal.source)
        threshold = self.source_thresholds.get(base_source, self.default_threshold)

        filtered: List[str] = []
        skipped: List[Tuple[str, Optional[float]]] = []
        for token in signal.tokens:
            market_cap = self.get_market_cap(token)
            if market_cap is None:
                filtered.append(token)
                continue
            if threshold and market_cap > threshold:
                skipped.append((token, market_cap))
            else:
                filtered.append(token)

        return filtered, skipped, threshold

    def reload(self) -> bool:
        """Reload cache from disk; keep existing data on failure."""
        return self._load_cache()

    def pick_lowest_market_cap(self, tokens: List[str]) -> Optional[str]:
        """Return the lowest known market-cap token, falling back only if all are unknown."""
        known_caps: List[Tuple[float, str]] = []
        unknown_caps: List[str] = []

        for token in tokens:
            market_cap = self.get_market_cap(token)
            if isinstance(market_cap, (int, float)):
                known_caps.append((float(market_cap), token))
            else:
                unknown_caps.append(token)

        if known_caps:
            known_caps.sort(key=lambda item: (item[0], item[1]))
            return known_caps[0][1]
        if unknown_caps:
            return sorted(unknown_caps)[0]
        return None


async def coinmarketcap_refresh_loop(
    filter_obj: MarketCapFilter,
    interval_seconds: int,
) -> None:
    """Background task to refresh the CoinMarketCap cache periodically."""

    if refresh_coinmarketcap_cache is None:
        logger.warning("CoinMarketCap auto-refresh disabled (loader module not available)")
        return

    interval_seconds = max(interval_seconds, 60)
    logger.info(
        "CoinMarketCap auto-refresh enabled every %s seconds (~%.2f hours)",
        interval_seconds,
        interval_seconds / 3600,
    )

    async def _sleep_with_cancel(delay: int) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

    while True:
        try:
            logger.info("Refreshing CoinMarketCap cache via API ...")
            refresh_coinmarketcap_cache(output_path=filter_obj.data_file)
            if filter_obj.reload():
                logger.info(
                    "CoinMarketCap cache refreshed and reloaded (%s)",
                    filter_obj.data_file,
                )
            else:
                logger.warning(
                    "CoinMarketCap cache refresh completed but reload failed; keeping previous data"
                )
        except CoinMarketCapError as exc:
            logger.error("CoinMarketCap auto-refresh failed: %s", exc)
        except asyncio.CancelledError:
            logger.info("CoinMarketCap auto-refresh task cancelled")
            raise
        except Exception:
            logger.exception("Unexpected error during CoinMarketCap auto-refresh")

        await _sleep_with_cancel(interval_seconds)

# ==========================
# Telegram Parsers
# ==========================

def parse_bwe_message(text: str) -> Optional[Signal]:
    """Parse BWEnews message for Upbit KRW listing"""
    if not ENABLE_UPBIT_LISTINGS:
        return None
    if not text:
        return None
        
    text = text.replace("\u200b", "").strip()
    
    # Check for UPBIT + LISTING
    if "UPBIT" not in text.upper() or "LISTING" not in text.upper():
        return None
    
    # Check for KRW market
    markets = []
    if re.search(r'\bKRW\b', text, re.IGNORECASE):
        markets.append("KRW")
    if re.search(r'\bBTC\b', text, re.IGNORECASE):
        markets.append("BTC")
    if re.search(r'\bUSDT\b', text, re.IGNORECASE):
        markets.append("USDT")
        
    if "KRW" not in markets:
        return None  # Only care about KRW listings
    
    # Extract tokens
    tokens = set()
    
    # Pattern 1: (TOKEN)
    for match in re.findall(r'\(([A-Z0-9]{1,15})\)', text):
        if match not in ("KRW", "BTC", "USDT", "USD", "USDC", "BUSD", "FDUSD"):
            tokens.add(match)
    
    # Pattern 2: $TOKEN
    for match in re.findall(r'\$([A-Z0-9]{1,15})\b', text):
        if match not in ("KRW", "BTC", "USDT", "USD", "USDC", "BUSD", "FDUSD"):
            tokens.add(match)
    
    if not tokens:
        return None
    
    # Extract URL and notice ID
    url = None
    notice_id = None
    url_match = re.search(r'https?://upbit\.com/service_center/notice\?id=(\d+)', text)
    if url_match:
        notice_id = url_match.group(1)
        url = url_match.group(0)
    
    # Get title (first non-empty line)
    title = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")[:300]
    
    return Signal(
        source="UPBIT_TELEGRAM",
        tokens=sorted(list(tokens)),
        announcement=title,
        body=text,  # Full message as body
        listing_type="KRW",
        telegram_source=True,
        url=url,
        notice_id=notice_id,
        channel="bwe"
    )

def parse_upbit_official_message(text: str) -> Optional[Signal]:
    """Parse Upbit official channel message for KRW listings"""
    if not ENABLE_UPBIT_LISTINGS:
        return None
    if not text:
        return None
    t = text.replace("\u200b", "").strip()

    # KRW şartı (Korece "원화" veya "KRW")
    has_krw = bool(re.search(r'\bKRW\b', t, re.IGNORECASE) or '원화' in t)
    if not has_krw:
        return None

    # Token çıkar ( (TOKEN) ve $TOKEN )
    tokens = set(re.findall(r'\(([A-Z0-9]{1,15})\)', t))
    tokens.update(re.findall(r'\$([A-Z0-9]{1,15})\b', t))
    # Stable/para birimlerini at
    DROP = {"KRW","BTC","USDT","USD","USDC","BUSD","FDUSD"}
    tokens = sorted([x for x in tokens if x not in DROP])
    if not tokens:
        return None

    title = next((ln.strip() for ln in t.splitlines() if ln.strip()), "")[:300]
    return Signal(
        source="UPBIT_TELEGRAM_OFFICIAL",   # dedup için process_signal içinde _TELEGRAM kaldırılıyor
        tokens=tokens,
        announcement=title,
        listing_type="KRW",
        telegram_source=True,
        body=t,
        channel="upbit_official"
    )

__all__ = [name for name in globals() if not name.startswith("__")]
