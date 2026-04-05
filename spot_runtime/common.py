#!/usr/bin/env python
# coding: utf-8
"""
Spot execution runtime for the hypercryptotrader pipeline.

This module contains exchange clients, orchestration helpers, local cache
management, and runtime logging for the solo spot execution flow.
"""

import asyncio
import time
import hmac
import hashlib
import json
import os
import sys
import base64
import requests
import re
import httpx
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
import psutil
import socket
import gc
import logging
import traceback
import functools
import random
import platform
import threading
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set, Tuple, Union, Callable, Sequence, Mapping
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import websockets
from collections import deque, defaultdict
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN

try:
    import colorama
    from colorama import Fore, Style
except ModuleNotFoundError:
    class _DummyFore:
        GREEN = ""
        CYAN = ""
        YELLOW = ""

    class _DummyStyle:
        RESET_ALL = ""

    class _DummyColorama:
        @staticmethod
        def init():
            return None

    colorama = _DummyColorama()
    Fore = _DummyFore()
    Style = _DummyStyle()

try:
    from mexc_userstream import MexcUserStream
    MEXC_USERSTREAM_AVAILABLE = True
except ModuleNotFoundError:
    MexcUserStream = None  # type: ignore[assignment]
    MEXC_USERSTREAM_AVAILABLE = False


def format_quote_amount(amount: float, decimals: int = 2) -> str:
    """Format quote currency amounts (e.g., USDT) without floating drift."""
    fmt = "0." + ("0" * decimals) if decimals > 0 else "0"
    quantized = Decimal(str(amount)).quantize(Decimal(fmt), rounding=ROUND_DOWN)
    return str(quantized).rstrip('0').rstrip('.')


def _env_flag(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"", "default"}:
        return default
    return lowered not in {"0", "false", "no", "off"}


def _binance_use_testnet() -> bool:
    return _env_flag(os.getenv("BINANCE_USE_TESTNET"), default=False)


def _binance_rest_base_url() -> str:
    if _binance_use_testnet():
        return "https://testnet.binance.vision"
    return "https://api.binance.com"


def _binance_primary_hosts() -> Tuple[str, ...]:
    if _binance_use_testnet():
        return ("testnet.binance.vision",)
    return ("api.binance.com",)


def make_json_safe(value: Any) -> Any:
    """Recursively convert objects into JSON-serializable structures."""
    if isinstance(value, dict):
        return {str(key): make_json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return base64.b64encode(value).decode('ascii')
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def parse_exchange_list(raw: Optional[str]) -> List[str]:
    """Parse comma separated exchange strings into normalized list."""
    if raw is None:
        return []
    items = []
    for part in raw.split(","):
        trimmed = part.strip().lower()
        if trimmed:
            items.append(trimmed)
    return items


REALISTIC_LOOKUP_ATTEMPTS: List[float] = [0.20, 0.35, 0.60, 1.00]

BINANCE_PRIMARY_HOSTS: Tuple[str, ...] = ("api.binance.com",)
GATE_PRIMARY_HOSTS: Tuple[str, ...] = ("api.gateio.ws",)

DEFAULT_BINANCE_HTTP_TIMEOUT = float(os.getenv("BINANCE_HTTP_TIMEOUT", "1.0"))
DEFAULT_MEXC_HTTP_TIMEOUT = float(os.getenv("MEXC_HTTP_TIMEOUT", "1.0"))
DEFAULT_GATE_HTTP_TIMEOUT = float(os.getenv("GATE_HTTP_TIMEOUT", "1.8"))
DEFAULT_GATE_PING_TIMEOUT = float(os.getenv("GATE_PING_TIMEOUT", "1.8"))
DEFAULT_WS_OPEN_TIMEOUT = float(os.getenv("WS_OPEN_TIMEOUT", "3.0"))

BINANCE_ENDPOINT_WEIGHTS: Dict[str, int] = {
    "GET /api/v3/exchangeInfo": 10,
    "POST /api/v3/order": 1,
    "GET /api/v3/account": 10,
    "GET /api/v3/time": 1,
    "GET /api/v3/ticker/price": 1,
}


# Linux için asyncio ayarları
if sys.platform == 'linux':
    # Linux'ta varsayılan event loop zaten optimal
    pass

# Renk desteği başlat
colorama.init()

# Çevresel değişkenleri yükle
BASE_DIR = Path(__file__).resolve().parents[1]

def _load_env_files():
    """Load env files, honoring PRIMARY_ENV_FILE when explicitly set."""
    primary_env_file = os.getenv("PRIMARY_ENV_FILE")
    if primary_env_file:
        if primary_env_file != os.devnull:
            load_dotenv(primary_env_file, override=True)
        return

    home_env = os.path.expanduser("~/.env")
    if os.path.exists(home_env):
        load_dotenv(home_env, override=True)

    local_env = BASE_DIR / ".env"
    if local_env.exists():
        load_dotenv(local_env, override=True)


_load_env_files()

# Test sinyali
TAO_TEST_SIGNAL = {
    "type": "signal",
    "exchange": "coinbase",
    "tokens": ["TAO"],
    "announcement": "Assets added to the roadmap today: Bittensor (TAO)",
    "alert_type": "roadmap",
    "timestamp": datetime.now(timezone.utc).isoformat()
}
RUN_TEST_SIGNAL = False  # Test etmek için True yap

# Küresel session nesneleri (thread-safe access via lock)
BINANCE_SESSION = None
MEXC_SESSION = None
GATE_SESSION = None
_SESSION_LOCK = threading.Lock()  # Thread-safe session access

VAR_ROOT = Path("var")
LOG_ROOT = VAR_ROOT / "logs"
CACHE_ROOT = VAR_ROOT / "cache"
LOG_ROOT.mkdir(parents=True, exist_ok=True)
CACHE_ROOT.mkdir(parents=True, exist_ok=True)
BINANCE_SYMBOL_CACHE_FILE = CACHE_ROOT / "binance_symbol_cache.json"
MEXC_SYMBOL_CACHE_FILE = CACHE_ROOT / "mexc_symbol_cache.json"
GATE_SYMBOL_CACHE_FILE = CACHE_ROOT / "gate_symbol_cache.json"


def _load_json_file(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _atomic_write_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False)
        tmp_path.replace(path)
    except Exception:
        logger = logging.getLogger(__name__)
        logger.debug("Could not persist cache file %s", path, exc_info=True)


class SourceAddressAdapter(HTTPAdapter):
    """HTTP adapter that binds outbound sockets to a specific source IP."""

    def __init__(self, source_ip: Optional[str] = None, **kwargs):
        self._source_ip = source_ip
        super().__init__(**kwargs)

    def init_poolmanager(self, *pool_args, **pool_kwargs):  # type: ignore[override]
        if self._source_ip:
            pool_kwargs.setdefault("source_address", (self._source_ip, 0))
        return super().init_poolmanager(*pool_args, **pool_kwargs)

    def proxy_manager_for(self, *proxy_args, **proxy_kwargs):  # type: ignore[override]
        if self._source_ip:
            proxy_kwargs.setdefault("source_address", (self._source_ip, 0))
        return super().proxy_manager_for(*proxy_args, **proxy_kwargs)


def _create_bound_session(
    *,
    source_ip: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    pool_connections: int = 30,
    pool_maxsize: int = 30,
) -> requests.Session:
    """Create a requests session pinned to the given source IP if provided."""

    session = requests.Session()
    if headers:
        session.headers.update(headers)

    adapter = SourceAddressAdapter(
        source_ip=source_ip,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=0,
        pool_block=False,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_local_ip(env_var: str) -> Optional[str]:
    """Return a trimmed local IP from the environment if one is configured."""

    value = os.getenv(env_var)
    if not value:
        return None
    trimmed = value.strip()
    return trimmed or None

# -----------------------------------------------------------------------------
# SYMBOL CONVERTER - TÜM BORSALAR İÇİN
# -----------------------------------------------------------------------------

class SymbolConverter:
    """Evrensel symbol formatından borsa-spesifik formata dönüştürücü"""
    
    @staticmethod
    def convert(symbol: str, exchange: str, quote: str = "USDT") -> str:
        """
        Symbol'ü borsa formatına dönüştür
        Args:
            symbol: Base currency (BTC, ETH vs)
            exchange: binance, mexc, gate
            quote: Quote currency (varsayılan USDT)
        Returns:
            Borsa-spesifik format
        """
        symbol = symbol.upper()
        quote = quote.upper()
        
        if exchange == "binance":
            return f"{symbol}{quote}"  # BTCUSDT
        elif exchange == "mexc":
            return f"{symbol}{quote}"  # BTCUSDT - MEXC DE ALT ÇİZGİ KULLANMIYOR!
        elif exchange == "gate":
            return f"{symbol}_{quote}"  # BTC_USDT - SADECE GATE ALT ÇİZGİ KULLANIYOR
        else:
            return f"{symbol}{quote}"

# -----------------------------------------------------------------------------
# PRECISION MANAGER - HER BORSA İÇİN HASSAS DECIMAL YÖNETİMİ
# -----------------------------------------------------------------------------

_LISTING_ALIAS_PATTERN = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9\s\-\.'&]{0,64}?)\s*\(\s*([A-Z0-9]{1,10})\s*\)"
)


def _extract_listing_aliases(announcement: Optional[str]) -> Dict[str, List[str]]:
    """Listing duyurusundan sembol isim eşlemelerini çıkar."""
    aliases: Dict[str, List[str]] = defaultdict(list)
    if not announcement:
        return aliases

    for match in _LISTING_ALIAS_PATTERN.finditer(announcement):
        name = (match.group(1) or "").strip()
        symbol = (match.group(2) or "").strip().upper()

        if not symbol or not name:
            continue

        # Fazla kelimeleri temizle ve benzersiz liste oluştur
        candidate_names = [name]
        simplified = re.sub(r"\b(token|coin|fund)\b", "", name, flags=re.IGNORECASE).strip()
        if simplified and simplified not in candidate_names:
            candidate_names.append(simplified)

        existing = aliases[symbol]
        for candidate in candidate_names:
            if candidate and candidate not in existing:
                existing.append(candidate)

    return aliases

_HTTP_RACE_EXECUTOR = ThreadPoolExecutor(max_workers=32)


async def _race_http_requests(
    candidates: Dict[str, Callable[[], requests.Response]],
    timeout: float,
) -> Tuple[Optional[requests.Response], Optional[str], Dict[str, str], float]:
    """
    Execute multiple blocking HTTP callables in parallel and return the first successful response.

    Returns:
        response: The first requests.Response object that completed without raising.
        winner_label: Label of the callable that produced the response.
        errors: Mapping of label -> error string for attempts that failed before a success.
    """
    loop = asyncio.get_running_loop()
    future_to_label = {}
    pending_futures: List[asyncio.Future] = []

    for label, callable_ in candidates.items():
        future = loop.run_in_executor(_HTTP_RACE_EXECUTOR, callable_)
        future_to_label[future] = label
        pending_futures.append(future)

    start = time.perf_counter()
    errors: Dict[str, str] = {}

    result_elapsed = 0.0

    while pending_futures:
        remaining = timeout - (time.perf_counter() - start)
        if remaining <= 0:
            break

        done, pending = await asyncio.wait(
            pending_futures,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=remaining,
        )

        if not done:
            break

        for future in done:
            label = future_to_label.pop(future, "unknown")
            attempt_start = time.perf_counter()
            try:
                response = future.result()
            except Exception as exc:
                errors[label] = f"{type(exc).__name__}: {exc}"
                continue

            # Cancel remaining futures; underlying requests will honour per-call timeout.
            for pending_future in pending:
                pending_future.cancel()
                future_to_label.pop(pending_future, None)

            result_elapsed = time.perf_counter() - attempt_start
            return response, label, errors, result_elapsed

        pending_futures = list(pending)

    for future in pending_futures:
        future.cancel()
        future_to_label.pop(future, None)

    errors["_timeout"] = f">{timeout:.3f}s"
    return None, None, errors, result_elapsed


async def _fast_http_lookup(
    *,
    url: str,
    session: Optional[requests.Session],
    params: Optional[Dict[str, Any]],
    headers: Optional[Dict[str, str]],
    attempt_timeouts: Sequence[float],
    extractor: Callable[[requests.Response], Tuple[Optional[Any], Optional[str]]],
) -> Tuple[Optional[Any], List[str], Optional[Dict[str, Any]]]:
    """
    Retry an HTTP GET using rapid parallel attempts and return the extracted payload.

    extractor(response) should return (result, error_message). If result is not None,
    it is considered success. If error_message is provided, it will be appended to the
    error log for that attempt.
    """
    errors: List[str] = []
    session_available = session is not None

    for attempt, timeout in enumerate(attempt_timeouts, start=1):
        attempt_start = time.perf_counter()
        candidates: Dict[str, Callable[[], requests.Response]] = {}
        request_kwargs: Dict[str, Any] = {'timeout': timeout}
        if params is not None:
            request_kwargs['params'] = params
        if headers is not None:
            request_kwargs['headers'] = headers

        if session_available:
            candidates["session"] = functools.partial(
                session.get,
                url,
                **request_kwargs,
            )

        candidates["fallback"] = functools.partial(
            requests.get,
            url,
            **request_kwargs,
        )

        response, winner_label, race_errors, race_elapsed = await _race_http_requests(candidates, timeout)

        for label, message in race_errors.items():
            if label == "_timeout":
                errors.append(f"attempt {attempt}: {message}")
            else:
                errors.append(f"attempt {attempt} {label}: {message}")
                if label == "session":
                    session_available = False

        if response is None:
            continue

        if response.status_code != 200:
            errors.append(f"attempt {attempt} {winner_label or 'response'}: status {response.status_code}")
            if winner_label == "session":
                session_available = False
            continue

        result, content_error = extractor(response)
        if result is not None:
            total_elapsed = (time.perf_counter() - attempt_start)
            success_meta = {
                "attempt": attempt,
                "winner": winner_label or "unknown",
                "elapsed_ms": total_elapsed * 1000.0,
                "race_elapsed_ms": race_elapsed * 1000.0,
            }
            return result, errors, success_meta

        if content_error:
            errors.append(f"attempt {attempt} {winner_label or 'response'}: {content_error}")

    return None, errors, None

class PrecisionManager:
    """Her borsa ve token için precision yönetimi"""
    
    def __init__(self):
        self.precision_cache = {}
        self.lock = threading.Lock()
        
        # Varsayılan precision değerleri
        self.defaults = {
            "binance": {"price": 8, "quantity": 8},
            "mexc": {"price": 6, "quantity": 6},
            "gate": {"price": 8, "quantity": 8}
        }
    
    def format_quantity(self, exchange: str, symbol: str, quantity: float) -> str:
        """Miktar formatla - borsa kurallarına uygun"""
        precision = self.get_precision(exchange, symbol, "quantity")
        
        # Decimal kullanarak hassas kesme (yuvarlamadan)
        decimal_quantity = Decimal(str(quantity))
        formatted = decimal_quantity.quantize(
            Decimal(f"0.{'0' * precision}"),
            rounding=ROUND_DOWN  # Aşağı yuvarla, böylece limitleri aşmayız
        )
        
        # Gereksiz sıfırları kaldır
        return str(formatted).rstrip('0').rstrip('.')
    
    def format_price(self, exchange: str, symbol: str, price: float) -> str:
        """Fiyat formatla - borsa kurallarına uygun"""
        precision = self.get_precision(exchange, symbol, "price")
        
        decimal_price = Decimal(str(price))
        formatted = decimal_price.quantize(
            Decimal(f"0.{'0' * precision}"),
            rounding=ROUND_DOWN
        )
        
        return str(formatted).rstrip('0').rstrip('.')
    
    def get_precision(self, exchange: str, symbol: str, type: str) -> int:
        """Precision değerini al (cache'den veya varsayılan)"""
        key = f"{exchange}:{symbol}:{type}"
        
        with self.lock:
            if key in self.precision_cache:
                return self.precision_cache[key]
            
            # Cache'de yoksa varsayılan değeri kullan
            return self.defaults[exchange][type]
    
    def update_precision(self, exchange: str, symbol: str, price_precision: int, quantity_precision: int):
        """Precision değerlerini güncelle"""
        with self.lock:
            self.precision_cache[f"{exchange}:{symbol}:price"] = price_precision
            self.precision_cache[f"{exchange}:{symbol}:quantity"] = quantity_precision

# Global precision manager
PRECISION_MANAGER = PrecisionManager()

# -----------------------------------------------------------------------------
# COINMARKETCAP API - LOCAL JSON'DAN OKUMA
# -----------------------------------------------------------------------------

class CoinMarketCapAPI:
    """CoinMarketCap API - Local JSON'dan okuma"""
    def __init__(self):
        self.data_file = os.getenv("TREE_NEWS_CMC_FILE", "var/coinmarketcap_data.json")
        self.coin_data = {}
        self.symbol_index: Dict[str, Dict[str, Any]] = {}
        self.slug_index: Dict[str, str] = {}
        self.compact_slug_index: Dict[str, str] = {}
        self.compact_name_index: Dict[str, str] = {}
        self._load_cached_data()
    
    @staticmethod
    def _compact_key(value: Optional[str]) -> str:
        if not value:
            return ""
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _rebuild_indexes(self) -> None:
        self.symbol_index = {}
        self.slug_index = {}
        self.compact_slug_index = {}
        self.compact_name_index = {}

        for key, entry in self.coin_data.items():
            if not isinstance(entry, dict):
                continue
            symbol = (entry.get("symbol") or key or "").upper()
            if not symbol:
                continue
            self.symbol_index[symbol] = entry

            slug_value = entry.get("slug")
            if slug_value:
                slug_lower = str(slug_value).lower()
                if slug_lower:
                    self.slug_index[slug_lower] = symbol
                    compact_slug = self._compact_key(slug_lower)
                    if compact_slug:
                        self.compact_slug_index[compact_slug] = symbol

            name_value = entry.get("name")
            if name_value:
                compact_name = self._compact_key(str(name_value))
                if compact_name:
                    self.compact_name_index[compact_name] = symbol

    def _load_cached_data(self):
        """Cached coin verisini yükle"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    raw_data = json.load(f)

                metadata_removed = {
                    str(symbol).upper(): data
                    for symbol, data in raw_data.items()
                    if symbol != "_metadata"
                }

                self.coin_data = metadata_removed

                self._rebuild_indexes()

                # Logger yerine print kullan
                print(f"Loaded {len(self.coin_data)} coins from local cache")
        except Exception as e:
            # Logger yerine print kullan
            print(f"Error loading cached coin data: {str(e)}")
            self.coin_data = {}
            self._rebuild_indexes()
    
    def get_market_cap(self, symbol: str, aliases: Optional[Sequence[str]] = None) -> Optional[float]:
        """Bir sembol için market cap değerini döndürür"""
        candidates: List[str] = []
        if symbol:
            candidates.append(symbol.strip().upper())
        if aliases:
            for alias in aliases:
                if alias:
                    candidates.append(str(alias).strip().upper())

        seen: Set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            entry = self.symbol_index.get(candidate)
            if entry:
                market_cap = entry.get("market_cap")
                if isinstance(market_cap, (int, float)):
                    return float(market_cap)
                try:
                    return float(market_cap)
                except (TypeError, ValueError):
                    continue

        for candidate in candidates:
            lower_candidate = candidate.lower()
            if not lower_candidate:
                continue
            symbol_match = self.slug_index.get(lower_candidate)
            if not symbol_match:
                continue
            entry = self.symbol_index.get(symbol_match)
            if entry:
                market_cap = entry.get("market_cap")
                if isinstance(market_cap, (int, float)):
                    return float(market_cap)
                try:
                    return float(market_cap)
                except (TypeError, ValueError):
                    continue

        compact_aliases: List[str] = []
        for item in candidates:
            compact = self._compact_key(item)
            if compact:
                compact_aliases.append(compact)
        if aliases:
            for alias in aliases:
                compact = self._compact_key(str(alias))
                if compact:
                    compact_aliases.append(compact)

        for compact in compact_aliases:
            symbol_match = self.compact_slug_index.get(compact) or self.compact_name_index.get(compact)
            if symbol_match:
                entry = self.symbol_index.get(symbol_match)
                if entry:
                    market_cap = entry.get("market_cap")
                    if isinstance(market_cap, (int, float)):
                        return float(market_cap)
                    try:
                        return float(market_cap)
                    except (TypeError, ValueError):
                        continue

        return None

# Global CoinMarketCap API instance
CMC_API = CoinMarketCapAPI()

# -----------------------------------------------------------------------------
# RATE LIMITER - HER BORSA İÇİN
# -----------------------------------------------------------------------------


@dataclass
class MexcTuning:
    """MEXC REST/WS tuning defaults for low latency execution."""

    rest_connect_timeout: float = 0.10
    rest_read_timeout: float = 0.25
    ws_ack_timeout: float = 0.15
    ws_fill_timeout: float = 0.35
    fast_trades_verify: bool = False
    mytrades_limit: int = 10
    mytrades_window_ms: int = 120_000
    order_recv_window: int = 5000

class UniversalRateLimiter:
    """Tüm borsalar için rate limit yönetimi"""
    
    def __init__(self):
        self.limits = {
            "binance": {
                "weight": 6000,  # Dakikada max weight
                "orders": 100,   # 10 saniyede max order
                "window": 60     # Saniye
            },
            "mexc": {
                "requests": 20,  # Saniyede max request
                "orders": 5,     # Saniyede max order
                "window": 1      # Saniye
            },
            "gate": {
                "requests": 200, # 10 saniyede max request
                "orders": 10,    # Saniyede max order
                "window": 10     # Saniye
            }
        }
        
        # Tracking için
        self.counters = defaultdict(lambda: {"requests": deque(), "orders": deque(), "weight": 0})
        self.lock = threading.Lock()
    
    def can_request(self, exchange: str, weight: int = 1, is_order: bool = False) -> bool:
        """İstek yapılabilir mi kontrol et"""
        with self.lock:
            now = time.time()
            counter = self.counters[exchange]
            limit = self.limits[exchange]
            
            # Eski kayıtları temizle
            window = limit["window"]
            
            # Binance için weight kontrolü
            if exchange == "binance":
                # Weight bazlı kontrol - tuple'ları düzgün işle
                counter["requests"] = deque([req for req in counter["requests"] 
                                        if isinstance(req, tuple) and req[0] > now - window])
            else:
                # Diğer borsalar için normal kontrol
                counter["requests"] = deque([req for req in counter["requests"] 
                                        if (isinstance(req, float) or isinstance(req, int)) and req > now - window])
            
            if is_order:
                # Order için özel kontrol
                order_window = 10 if exchange == "binance" else 1
                counter["orders"] = deque([req for req in counter["orders"] 
                                        if req > now - order_window])
                
                # Order limiti kontrolü
                if exchange == "binance" and len(counter["orders"]) >= limit["orders"]:
                    return False
                elif exchange in ["mexc", "gate"] and len(counter["orders"]) >= limit["orders"]:
                    return False
            
            # Request/weight limiti kontrolü
            if exchange == "binance":
                # Weight bazlı kontrol
                total_weight = sum(w for t, w in counter["requests"])
                return total_weight + weight <= limit["weight"]
            else:
                # Request sayısı bazlı kontrol
                return len(counter["requests"]) < limit["requests"]
    
    def add_request(self, exchange: str, weight: int = 1, is_order: bool = False):
        """İstek kaydı ekle"""
        with self.lock:
            now = time.time()
            counter = self.counters[exchange]
            
            if exchange == "binance":
                counter["requests"].append((now, weight))
            else:
                counter["requests"].append(now)
            
            if is_order:
                counter["orders"].append(now)
    
    def get_wait_time(self, exchange: str, weight: int = 1, is_order: bool = False) -> float:
        """Bekleme süresi hesapla"""
        with self.lock:
            now = time.time()
            counter = self.counters[exchange]
            limit = self.limits[exchange]
            
            wait_time_req = 0.0
            wait_time_order = 0.0

            if exchange == "binance":
                requests = list(counter["requests"])
                if requests:
                    free_capacity = limit["weight"] - weight
                    total_weight = sum(w for _, w in requests)
                    if total_weight > free_capacity:
                        remaining_weight = total_weight
                        wait_until = requests[0][0] + limit["window"]
                        for ts, req_weight in requests:
                            remaining_weight -= req_weight
                            wait_until = ts + limit["window"]
                            if remaining_weight <= free_capacity:
                                break
                        wait_time_req = wait_until - now
            else:
                requests = list(counter["requests"])
                request_limit = limit.get("requests")
                if request_limit is not None and requests and len(requests) >= request_limit:
                    wait_time_req = (requests[0] + limit["window"]) - now

            if is_order:
                orders = list(counter["orders"])
                order_limit = limit.get("orders")
                if order_limit and len(orders) >= order_limit:
                    order_window = 10 if exchange == "binance" else 1
                    wait_time_order = (orders[0] + order_window) - now

            wait_time = max(wait_time_req, wait_time_order, 0.0)
            return wait_time

# Global rate limiter
RATE_LIMITER = UniversalRateLimiter()

class WeightedWindowLimiter:
    """Asenkron ağırlıklı pencere limitleyici."""

    def __init__(
        self,
        window_seconds: float,
        max_weight: float,
        *,
        min_sleep: float = 0.01,
        jitter: float = 0.0,
    ) -> None:
        self.window = max(0.1, float(window_seconds))
        self.max_weight = max(0.1, float(max_weight))
        self.min_sleep = max(0.0, float(min_sleep))
        self.jitter = max(0.0, float(jitter))
        self._records: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, weight: float) -> None:
        weight = max(0.1, float(weight))
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._records and (now - self._records[0][0]) > self.window:
                    self._records.popleft()
                current = sum(record_weight for _, record_weight in self._records)
                if current + weight <= self.max_weight:
                    self._records.append((now, weight))
                    break
                # pencere doluysa ilk kaydın pencere dışına taşmasını bekle
                wait_time = self.window - (now - self._records[0][0])
            await asyncio.sleep(max(wait_time, self.min_sleep))

        if self.jitter > 0.0:
            await asyncio.sleep(random.uniform(0.0, self.jitter))

# -----------------------------------------------------------------------------
# LOGLAMA YAPISI - OPTİMİZE EDİLMİŞ
# -----------------------------------------------------------------------------

def setup_logging():
    """Loglama yapılandırması - optimize edilmiş"""
    # Ana logger
    logger = logging.getLogger('UltraTrader')
    logger.setLevel(logging.INFO)  # DEBUG yerine INFO
    
    # Konsol handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(f'{Fore.CYAN}%(asctime)s - %(name)s - %(levelname)s - %(message)s{Style.RESET_ALL}')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Dosya handler - dönen log dosyaları
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(str(LOG_ROOT / 'ultra_trader.log'), maxBytes=10*1024*1024, backupCount=5)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Performans logger (ayrı dosya)
    perf_logger = logging.getLogger('UltraTrader.Performance')
    perf_handler = RotatingFileHandler(str(LOG_ROOT / 'ultra_performance.log'), maxBytes=10*1024*1024, backupCount=3)
    perf_handler.setFormatter(file_formatter)
    perf_logger.addHandler(perf_handler)
    
    # API logger - işlemler için
    api_logger = logging.getLogger('UltraTrader.API')
    api_handler = RotatingFileHandler(str(LOG_ROOT / 'api_calls.log'), maxBytes=5*1024*1024, backupCount=3)
    api_handler.setFormatter(file_formatter)
    api_logger.addHandler(api_handler)
    api_logger.setLevel(logging.INFO)
    
    # WebSocket logger
    ws_logger = logging.getLogger('UltraTrader.WebSocket')
    ws_handler = RotatingFileHandler(str(LOG_ROOT / 'websocket.log'), maxBytes=5*1024*1024, backupCount=3)
    ws_handler.setFormatter(file_formatter)
    ws_logger.addHandler(ws_handler)
    ws_logger.setLevel(logging.INFO)
    
    return logger, perf_logger, api_logger, ws_logger

# Global loggerlar
logger, perf_logger, api_logger, ws_logger = setup_logging()

# -----------------------------------------------------------------------------
# PERFORMANS ÖLÇÜMÜ İÇİN DEKORATÖR
# -----------------------------------------------------------------------------

def measure_time(func):
    """Fonksiyon çalışma süresini ölçen dekoratör"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        
        # 50ms'den uzun süren işlemleri kaydet
        if elapsed_time > 0.05:
            perf_logger.info(f"{func.__name__} | Süre: {elapsed_time:.6f} saniye")
        
        return result
    return wrapper

# -----------------------------------------------------------------------------
# SYSTEM OPTIMIZER - UBUNTU/LINUX EDITION
# -----------------------------------------------------------------------------

class UbuntuOptimizer:
    """Ubuntu/Linux için sistem optimizasyonları"""
    _optimized = False
    
    @staticmethod
    def optimize_all(force: bool = False):
        """Tüm Ubuntu optimizasyonlarını uygula"""
        if _env_flag(os.getenv("DISABLE_UBUNTU_OPTIMIZER"), default=False):
            logger.info("Ubuntu optimizer disabled via DISABLE_UBUNTU_OPTIMIZER")
            return False

        if UbuntuOptimizer._optimized and not force:
            logger.debug("Ubuntu optimizer already initialized; skipping repeated setup")
            return True

        logger.info(f"{Fore.YELLOW}UBUNTU OPTIMIZATION MODE AKTİVLEŞTİRİLİYOR...{Style.RESET_ALL}")
        
        # Sistem bilgilerini göster
        cpu_info = platform.processor()
        ram_gb = psutil.virtual_memory().total / (1024**3)
        logger.info(f"CPU: {cpu_info}")
        logger.info(f"RAM: {ram_gb:.1f} GB")
        logger.info(f"OS: {platform.system()} {platform.release()}")
        
        # CPU optimizasyonu
        UbuntuOptimizer.optimize_cpu()
        
        # Bellek optimizasyonu
        UbuntuOptimizer.optimize_memory()
        
        # Ağ optimizasyonu
        UbuntuOptimizer.optimize_network()
        
        # HTTP istemcileri hazırla
        UbuntuOptimizer.setup_http_clients()
        
        # DNS önbellekleme
        UbuntuOptimizer.warm_up_dns()
        
        UbuntuOptimizer._optimized = True
        logger.info(f"UBUNTU OPTİMİZASYONLARI TAMAMLANDI!")
        return True
    
    @staticmethod
    def optimize_cpu():
        """CPU optimizasyonu - Linux için"""
        try:
            process = psutil.Process(os.getpid())
            
            # CPU sayısını al
            cpu_count = psutil.cpu_count(logical=True)
            
            # CPU affinity ayarla - tüm çekirdekleri kullan
            if cpu_count > 4:
                # Yarı çekirdek kullan
                cores = list(range(0, cpu_count // 2))
                process.cpu_affinity(cores)
                logger.info(f"CPU affinity set to cores: {cores}")
            
            # Nice değerini düşür (yüksek öncelik)
            try:
                os.nice(-10)  # Yüksek öncelik
                logger.info("Process priority increased")
            except PermissionError:
                logger.warning("Cannot set process priority (need sudo)")
            
            return True
        except Exception as e:
            logger.warning(f"CPU optimization error: {e}")
            return False
    
    @staticmethod
    def optimize_memory():
        """Bellek optimizasyonu"""
        try:
            # Zorla bellek temizliği
            gc.collect()
            
            # Python JIT optimizasyonu için ısınma
            dummy_dict = {}
            for i in range(1000):
                dummy_dict[f"key_{i}"] = i
            
            logger.info("Memory optimization completed")
            return True
        except Exception as e:
            logger.warning(f"Memory optimization error: {e}")
            return False
    
    @staticmethod
    def optimize_network():
        """Ağ optimizasyonu - Linux için"""
        try:
            logger.info("Network optimizations completed (no global socket overrides)")
            return True
        except Exception as e:
            logger.warning(f"Network optimization error: {e}")
            return False
    
    @staticmethod
    def setup_http_clients():
        """HTTP istemcileri hazırla ve optimize et"""
        global BINANCE_SESSION, MEXC_SESSION, GATE_SESSION

        try:
            binance_ip = _get_local_ip("BINANCE_LOCAL_IP")
            mexc_ip = _get_local_ip("MEXC_LOCAL_IP")
            gate_ip = _get_local_ip("GATE_LOCAL_IP")

            common_headers = {'Connection': 'keep-alive'}

            with _SESSION_LOCK:
                BINANCE_SESSION = _create_bound_session(
                    source_ip=binance_ip,
                    headers=common_headers,
                )

                MEXC_SESSION = _create_bound_session(
                    source_ip=mexc_ip,
                    headers=common_headers,
                )

                GATE_SESSION = _create_bound_session(
                    source_ip=gate_ip,
                    headers=common_headers,
                )

            logger.info("HTTP clients prepared and optimized")
            return True
        except Exception as e:
            logger.warning(f"HTTP client preparation error: {e}")
            return False
    
    @staticmethod
    def warm_up_dns():
        """DNS önbellekleme"""
        try:
            endpoints: List[str] = list(_binance_primary_hosts()) + list(GATE_PRIMARY_HOSTS)
            extra_binance = parse_exchange_list(os.getenv("BINANCE_EXTRA_HOSTS"))
            extra_gate = parse_exchange_list(os.getenv("GATE_EXTRA_HOSTS"))
            if extra_binance:
                endpoints.extend(extra_binance)
            if extra_gate:
                endpoints.extend(extra_gate)
            mexc_host = os.getenv("MEXC_API_HOST")
            if mexc_host:
                endpoints.append(mexc_host)

            for endpoint in endpoints:
                try:
                    socket.getaddrinfo(endpoint, 443)
                except:
                    pass

            # İlk API bağlantıları için ısınma (thread-safe)
            try:
                with _SESSION_LOCK:
                    if BINANCE_SESSION:
                        BINANCE_SESSION.get(f"{_binance_rest_base_url()}/api/v3/ping", timeout=DEFAULT_BINANCE_HTTP_TIMEOUT)
                    if MEXC_SESSION:
                        ping_host = mexc_host or "api.mexc.com"
                        MEXC_SESSION.get(f"https://{ping_host}/api/v3/ping", timeout=DEFAULT_BINANCE_HTTP_TIMEOUT)
                    if GATE_SESSION:
                        GATE_SESSION.get("https://api.gateio.ws/api/v4/spot/time", timeout=DEFAULT_GATE_PING_TIMEOUT)
            except:
                pass

            logger.info("DNS cache warming completed")
            return True
        except Exception as e:
            logger.warning(f"DNS cache warming error: {e}")
            return False

# -----------------------------------------------------------------------------
# SERVER TIME CACHE
# -----------------------------------------------------------------------------

class ServerTimeCache:
    """Server time önbellek mekanizması"""
    
    def __init__(self, exchange):
        """
        Args:
            exchange: 'binance', 'mexc' veya 'gate'
        """
        self.exchange = exchange
        self.server_time = None
        self.last_update = 0
        self.offset = 0
        self.update_interval = 5  # 5 saniye
        self.dynamic_interval = self.update_interval
        self.max_interval = max(15, self.update_interval * 6)
        self.error_count = 0
        self.last_warning_time = 0.0
        self.lock = threading.Lock()
        self.warmup_complete = False
        
        # Başlangıçta zamanı al
        self.update_server_time_sync()
        
        # Arka planda güncelleyici başlat
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
        
        # Isınma fazını tamamla
        self._warm_up()
        
        logger.info(f"{exchange.upper()} server time cache initialized")
    
    def _warm_up(self):
        """Isınma fazı"""
        for _ in range(3):
            self.update_server_time_sync()
            time.sleep(0.1)
        
        self.warmup_complete = True
        return True
    
    def get_time(self):
        """Önbellekten server time al veya hesapla"""
        if self.server_time is None:
            return self.update_server_time_sync()
        
        # Son güncellemeden sonraki geçen süreyi ekle
        current_time = int(time.time() * 1000)
        elapsed = current_time - self.last_update
        server_time = self.server_time + elapsed
        
        return server_time
    
    def update_server_time_sync(self):
        """Server time'ı güncelleyen senkron metod"""
        global BINANCE_SESSION, MEXC_SESSION, GATE_SESSION
        response = None
        try:
            # Borsa seç
            if self.exchange == 'binance':
                url = f"{_binance_rest_base_url()}/api/v3/time"
                request_timeout = DEFAULT_BINANCE_HTTP_TIMEOUT
            elif self.exchange == 'mexc':
                url = "https://api.mexc.com/api/v3/time"
                request_timeout = DEFAULT_MEXC_HTTP_TIMEOUT
            else:  # gate
                url = "https://api.gateio.ws/api/v4/spot/time"
                request_timeout = DEFAULT_GATE_PING_TIMEOUT

            timeout_multiplier = 1 + min(self.error_count, 5) * 0.5
            request_timeout *= timeout_multiplier

            # Thread-safe session access
            with _SESSION_LOCK:
                if self.exchange == 'binance':
                    session = BINANCE_SESSION
                elif self.exchange == 'mexc':
                    session = MEXC_SESSION
                else:
                    session = GATE_SESSION

                # Session yoksa bağlanmış yeni bir session oluştur
                if not session:
                    env_key = {
                        'binance': 'BINANCE_LOCAL_IP',
                        'mexc': 'MEXC_LOCAL_IP',
                        'gate': 'GATE_LOCAL_IP',
                    }.get(self.exchange)
                    session = _create_bound_session(
                        source_ip=_get_local_ip(env_key) if env_key else None,
                        headers={'Connection': 'keep-alive'},
                    )
                    if self.exchange == 'binance':
                        BINANCE_SESSION = session
                    elif self.exchange == 'mexc':
                        MEXC_SESSION = session
                    else:
                        GATE_SESSION = session

            # Gerçek zaman al
            local_time = int(time.time() * 1000)

            response = session.get(url, timeout=request_timeout)
            
            if response.status_code == 200:
                with self.lock:
                    data = response.json()
                    if self.exchange == 'gate':
                        if isinstance(data, dict):
                            server_time = int(data.get('server_time', time.time())) * 1000
                        else:
                            server_time = int(data) * 1000
                    else:
                        server_time = data["serverTime"]
                    
                    self.server_time = server_time
                    self.last_update = local_time
                    self.offset = server_time - local_time
                    self.error_count = 0
                    self.dynamic_interval = self.update_interval
                    return server_time
            
            # Hata durumunda mevcut zamanı kullan
            with self.lock:
                if self.server_time:
                    return int(time.time() * 1000) + self.offset
                return int(time.time() * 1000)
        except Exception as e:
            self._handle_time_failure(e)
            with self.lock:
                if self.server_time:
                    return int(time.time() * 1000) + self.offset
                return int(time.time() * 1000)

        self._handle_time_failure(f"HTTP {response.status_code}")
        with self.lock:
            if self.server_time:
                return int(time.time() * 1000) + self.offset
            return int(time.time() * 1000)

    def _handle_time_failure(self, error):
        self.error_count += 1
        self.dynamic_interval = min(
            self.update_interval * (1 + 0.8 * self.error_count),
            self.max_interval,
        )
        now = time.time()
        should_warn = (
            self.error_count <= 3 or (now - self.last_warning_time) >= 30
        )
        message = (
            f"{self.exchange.upper()} server time update error: {error} "
            f"(consecutive={self.error_count}, next_retry={self.dynamic_interval:.1f}s)"
        )
        if should_warn:
            logger.warning(message)
            self.last_warning_time = now
        else:
            logger.debug(message)
    
    def _update_loop(self):
        """Arka planda periyodik güncelleme"""
        while True:
            time.sleep(self.dynamic_interval)
            self.update_server_time_sync()

# -----------------------------------------------------------------------------
# WEBSOCKET İSTEMCİSİ
# -----------------------------------------------------------------------------

class HyperbeastWebSocketClient:
    """Optimize edilmiş WebSocket client"""
    
    def __init__(self, ws_url, api_key=None):
        self.ws_url = ws_url
        self.api_key = api_key
        self.connection = None
        self.connected = False
        self.last_heartbeat = 0
        self.reconnect_interval = 5
        self.logger = ws_logger
        self.warmup_complete = False
        self.recv_lock = asyncio.Lock()
    
    async def connect(self):
        """WebSocket bağlantısı kur"""
        try:
            url = self.ws_url
            if self.api_key:
                url = f"{self.ws_url}?apikey={self.api_key}"
            
            self.connection = await websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
                max_size=10 * 1024 * 1024,
                open_timeout=DEFAULT_WS_OPEN_TIMEOUT
            )
            
            self.connected = True
            self.logger.info(f"WebSocket connection established: {self.ws_url}")
            self.warmup_complete = True
            return True
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {e}")
            self.connected = False
            return False
    
    async def disconnect(self):
        """WebSocket bağlantısını kapat"""
        if self.connection:
            try:
                await self.connection.close()
                self.logger.info("WebSocket connection closed")
            except Exception as e:
                self.logger.error(f"WebSocket close error: {e}")
        self.connected = False
    
    async def send_message(self, message):
        """WebSocket üzerinden mesaj gönder; hata halinde bir kez yeniden bağlanmayı dener."""
        payload = json.dumps(message) if isinstance(message, dict) else message

        for attempt in range(2):
            if not self.connected:
                success = await self.connect()
                if not success:
                    await asyncio.sleep(0.1)
                    continue

            try:
                await self.connection.send(payload)
                return True
            except Exception as e:
                self.logger.error(f"WebSocket send error: {e}")
                self.connected = False
                if attempt == 0:
                    await asyncio.sleep(0.1)
                    continue
        return False
    
    async def receive_message(self, timeout=5):
        """WebSocket üzerinden mesaj al"""
        if not self.connected:
            success = await self.connect()
            if not success:
                return None
        
        async with self.recv_lock:
            try:
                message = await asyncio.wait_for(self.connection.recv(), timeout=timeout)
                
                if message:
                    try:
                        return json.loads(message)
                    except:
                        return message
                return None
            except asyncio.TimeoutError:
                return None
            except Exception as e:
                self.logger.error(f"WebSocket receive error: {e}")
                self.connected = False
                return None
    
    async def send_heartbeat(self, data):
        """Heartbeat mesajı gönder"""
        heartbeat_msg = {
            "type": "heartbeat",
            "client_type": "ultra_trader",
            "timestamp": int(time.time() * 1000),
            "status": data
        }
        
        return await self.send_message(heartbeat_msg)

# -----------------------------------------------------------------------------
# INTELLIGENT RETRY ENGINE
# -----------------------------------------------------------------------------

class IntelligentRetryEngine:
    """Akıllı retry mekanizması"""
    
    def __init__(self):
        self.max_retries = 5
        self.base_delay = 0.1
        self.max_delay = 5.0
        
        # Terminal hatalar
        self.terminal_errors = [
            "Invalid symbol",
            "Unknown order",
            "Invalid api key",
            "Permission denied",
            "Invalid currency pair"
        ]
        
        # Özel delay gerektiren hatalar
        self.special_delays = {
            "TOO_MANY_REQUESTS": 1.0,
            "RATE_LIMIT": 1.0,
            "INSUFFICIENT_BALANCE": 0.5,
            "BALANCE_NOT_ENOUGH": 0.5
        }
    
    async def execute_with_retry(
        self,
        func,
        exchange: str,
        *args,
        weight: int = 1,
        is_order: bool = False,
        max_attempts: Optional[int] = None,
        **kwargs,
    ):
        """Akıllı retry ile fonksiyon çalıştır"""
        attempt_limit = max_attempts if max_attempts is not None else self.max_retries
        attempt_limit = max(1, attempt_limit)

        for attempt in range(attempt_limit):
            try:
                # Rate limit kontrolü
                if not RATE_LIMITER.can_request(exchange, weight=weight, is_order=is_order):
                    wait_time = RATE_LIMITER.get_wait_time(exchange, weight=weight, is_order=is_order)
                    logger.warning(f"{exchange} rate limit - waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                
                # Fonksiyonu çalıştır
                result = await func(*args, **kwargs)
                
                # Başarılıysa rate limiter'a ekle
                RATE_LIMITER.add_request(exchange, weight=weight, is_order=is_order)
                
                return result
                
            except Exception as e:
                error_str = str(e)
                
                # Terminal hata mı kontrol et
                if any(term in error_str for term in self.terminal_errors):
                    logger.error(f"{exchange} terminal error: {error_str}")
                    raise
                
                # Son deneme mi?
                if attempt == attempt_limit - 1:
                    logger.error(f"{exchange} all retries failed: {error_str}")
                    raise
                
                # Delay hesapla
                delay = self.base_delay
                
                # Özel delay varsa kullan
                for error_key, special_delay in self.special_delays.items():
                    if error_key in error_str:
                        delay = special_delay
                        break
                else:
                    # Exponential backoff with jitter
                    delay = min(self.base_delay * (2 ** attempt) + random.uniform(0, 0.1), self.max_delay)
                
                logger.warning(f"{exchange} error (attempt {attempt + 1}/{attempt_limit}): {error_str} - retrying in {delay:.2f}s")
                
                await asyncio.sleep(delay)
        
        raise Exception(f"{exchange} retry limit exceeded")

# Global retry engine
RETRY_ENGINE = IntelligentRetryEngine()

# -----------------------------------------------------------------------------
# BINANCE ULTRA TRADER - MULTI-ACCOUNT
# -----------------------------------------------------------------------------
