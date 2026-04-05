from . import common as _shared_common
from .common import *
from .common import (
    _atomic_write_json,
    _create_bound_session,
    _env_flag,
    _fast_http_lookup,
    _get_local_ip,
    _load_json_file,
)

def _parse_gate_retry_sequence(env_value: Optional[str], default: Sequence[float]) -> Tuple[float, ...]:
    if not env_value:
        return tuple(default)
    values: list[float] = []
    for chunk in env_value.split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            value = float(token)
        except ValueError:
            continue
        if value < 0:
            continue
        values.append(value)
    return tuple(values) if values else tuple(default)


def _retry_delay_from_sequence(sequence: Sequence[float], tail_step: float, attempt: int) -> float:
    if attempt <= 0:
        attempt = 1
    if not sequence:
        return max(0.0, tail_step * (attempt - 1))
    index = attempt - 1
    if index < len(sequence):
        return max(0.0, sequence[index])
    tail_index = index - (len(sequence) - 1)
    return max(0.0, sequence[-1] + tail_step * tail_index)


def _with_jitter(base_delay: float, frac: float = 0.20) -> float:
    if base_delay <= 0:
        return 0.0
    return max(0.0, base_delay * (1.0 + random.uniform(-frac, frac)))


def _decimal_to_str(value: Optional[Decimal]) -> str:
    if value is None:
        return "0"
    return format(value.normalize(), "f")


def _coerce_float(value: Any) -> Optional[float]:
    if value in (None, "", False):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value in (None, "", False):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, "", False):
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None


def _rate_limit_sleep_from_headers(headers: Optional[Mapping[str, str]], fallback: float) -> float:
    if not headers:
        return fallback
    raw = (
        headers.get("X-Gate-RateLimit-Reset-Timestamp")
        or headers.get("X-Gate-Ratelimit-Reset-Timestamp")
    )
    if not raw:
        return fallback
    try:
        ts = float(raw)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        remain = max(0.0, ts - time.time())
        return max(remain, fallback)
    except (TypeError, ValueError, OverflowError):
        return fallback


def _is_retryable_gate_status(status: int, label: str) -> bool:
    label = (label or "").upper()
    if status == 429 or status >= 500 or status == 0:
        return True
    non_retry = {
        "INSUFFICIENT_BALANCE",
        "INVALID_PARAM",
        "MARGIN_NOT_ENOUGH",
        "PRICE_OUT_OF_RANGE",
        "ORDER_NOT_FOUND",
        "FORBIDDEN",
    }
    if label in non_retry:
        return False
    if 400 <= status < 500 and label:
        return False
    return True


class TokenBucket:
    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = max(0.0, rate)
        self.capacity = max(0.0, capacity)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, amount: float = 1.0) -> None:
        amount = max(0.0, amount)
        if amount == 0:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                if self._tokens >= amount:
                    self._tokens -= amount
                    return
                deficit = amount - self._tokens
            wait = deficit / self.rate if self.rate > 0 else 0.5
            await asyncio.sleep(wait)


class GateUltraTrader:
    """Gate.io için ultra hızlı işlem sınıfı"""

    
    def __init__(self, test_mode=False):
        self.api_key = os.getenv("GATE_API_KEY", "")
        self.api_secret = os.getenv("GATE_API_SECRET", "")
        self.base_url = "https://api.gateio.ws"
        self.api_prefix = "/api/v4"
        self.test_mode = test_mode
        
        self.user_agent = "HyperbeastTrader/3.0"
        
        self._balance_cache = {}
        self._balance_time = 0
        
        self.time_cache = ServerTimeCache('gate')
        
        self.local_ip = _get_local_ip("GATE_LOCAL_IP")
        self.session = _shared_common.GATE_SESSION or self._create_session()
        try:
            self.http_timeout = float(os.getenv("GATE_HTTP_TIMEOUT", str(DEFAULT_GATE_HTTP_TIMEOUT)))
        except ValueError:
            self.http_timeout = DEFAULT_GATE_HTTP_TIMEOUT
        try:
            self.balance_timeout = float(os.getenv("GATE_BALANCE_TIMEOUT", str(self.http_timeout)))
        except ValueError:
            self.balance_timeout = self.http_timeout
        try:
            default_connect = min(self.http_timeout, 0.6)
            self.order_connect_timeout = float(os.getenv("GATE_ORDER_CONNECT_TIMEOUT", str(default_connect)))
        except ValueError:
            self.order_connect_timeout = min(self.http_timeout, 0.6)
        try:
            default_read = max(self.http_timeout, 1.2)
            self.order_read_timeout = float(os.getenv("GATE_ORDER_READ_TIMEOUT", str(default_read)))
        except ValueError:
            self.order_read_timeout = max(self.http_timeout, 1.2)
        self.fast_gate_mode = _env_flag(os.getenv("GATE_FAST_MODE", "1"), default=True)
        self.spot_order_max_attempts = max(
            1,
            int(
                os.getenv(
                    "GATE_SPOT_ORDER_MAX_ATTEMPTS",
                    os.getenv("GATE_ORDER_MAX_ATTEMPTS", "20"),
                )
            ),
        )
        default_retry_sequence = (0.05, 0.08, 0.10, 0.12, 0.15) if self.fast_gate_mode else (
            0.3,
            0.45,
            0.60,
            0.80,
            1.00,
        )
        self.spot_order_retry_sequence = _parse_gate_retry_sequence(
            os.getenv("GATE_SPOT_ORDER_RETRY_SEQUENCE") or os.getenv("GATE_ORDER_RETRY_SEQUENCE"),
            default_retry_sequence,
        )
        try:
            tail_base = "0.05" if self.fast_gate_mode else "0.20"
            tail_default = float(os.getenv("GATE_ORDER_RETRY_TAIL_STEP", tail_base))
        except ValueError:
            tail_default = 0.05 if self.fast_gate_mode else 0.20
        try:
            self.spot_order_retry_tail_step = max(
                0.0,
                float(os.getenv("GATE_SPOT_ORDER_RETRY_TAIL_STEP", str(tail_default))),
            )
        except ValueError:
            self.spot_order_retry_tail_step = max(0.0, tail_default)
        try:
            poll_default = "60" if self.fast_gate_mode else "90"
            self.confirm_poll_interval_ms = int(os.getenv("GATE_CONFIRM_POLL_INTERVAL_MS", poll_default))
        except ValueError:
            self.confirm_poll_interval_ms = 60 if self.fast_gate_mode else 90
        self.confirm_poll_interval_ms = max(30, min(self.confirm_poll_interval_ms, 500))
        try:
            self.confirm_max_polls = int(os.getenv("GATE_CONFIRM_MAX_POLLS", "0"))
        except ValueError:
            self.confirm_max_polls = 0
        self.spot_balance_recovery_threshold = self._load_spot_balance_recovery_threshold()
        self.lookup_timeouts = self._load_lookup_timeouts()
        logger.debug(
            "Gate.io timeouts configured: http=%.2fs balance=%.2fs connect=%.2fs read=%.2fs",
            self.http_timeout,
            self.balance_timeout,
            self.order_connect_timeout,
            self.order_read_timeout,
        )
        
        self._signature_cache = {}
        self._pre_encoded_secret = self.api_secret.encode('utf-8') if self.api_secret else b''
        
        self._empty_payload_hash = "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        
        self._balance_init_done = False
        
        self._check_api_credentials()

        # Pair info cache to avoid repeated metadata lookups
        self._pair_info_cache = {}
        self._pair_info_time = {}
        self._pair_info_ttl = 300
        pair_override = os.getenv("GATE_PAIR_CACHE_TTL_OVERRIDE")
        if pair_override:
            try:
                self._pair_info_ttl = float(pair_override)
                logger.info("Gate pair cache TTL override applied: %.1fs", self._pair_info_ttl)
            except ValueError:
                logger.warning("Invalid GATE_PAIR_CACHE_TTL_OVERRIDE=%s", pair_override)
        cache_path_env = os.getenv("GATE_SYMBOL_CACHE_FILE")
        self._pair_cache_path = Path(cache_path_env) if cache_path_env else GATE_SYMBOL_CACHE_FILE
        self._last_pair_cache_persist = 0.0
        self._load_pair_cache_from_disk()

        self.performance = {
            'trade_count': 0,
            'last_trade_time': 0,
            'min_trade_time': float('inf'),
            'max_trade_time': 0,
            'avg_trade_time': 0,
            'total_trade_time': 0,
            'fallback_stats': {
                'level_1_success': 0,
                'level_2_success': 0,
                'level_3_success': 0,
                'level_4_success': 0,
                'total_fallbacks': 0
            }
        }

        # Recent warm-up timestamps to avoid redundant prefetches
        self._recent_warmups: Dict[str, float] = {}

        self._warm_up()

        if not test_mode:
            threading.Thread(target=self._preload_balance_background, daemon=True).start()

    def _record_trade_success(self, trade_time: float) -> None:
        self.performance['trade_count'] += 1
        self.performance['last_trade_time'] = trade_time
        self.performance['min_trade_time'] = min(self.performance['min_trade_time'], trade_time)
        self.performance['max_trade_time'] = max(self.performance['max_trade_time'], trade_time)
        self.performance['total_trade_time'] += trade_time
        self.performance['avg_trade_time'] = (
            self.performance['total_trade_time'] / self.performance['trade_count']
        )
    
    def _generate_order_text(self, tag: str = "ultra") -> str:
        """Gate.io requires order text values to start with 't-'."""
        return f"t-{tag}-{int(time.time() * 1000)}-{random.randint(100, 999)}"
    
    def _check_api_credentials(self):
        """API anahtarlarını kontrol et"""
        if not self.api_key or len(self.api_key) < 10:
            logger.warning("Gate.io API KEY missing or invalid!")
            
        if not self.api_secret or len(self.api_secret) < 10:
            logger.warning("Gate.io API SECRET missing or invalid!")

    async def _warm_up_symbol(self, symbol: str, *, preparation_data: Optional[Dict[str, Any]] = None) -> None:
        """Prime balance and metadata before firing the actual order."""
        if self.test_mode:
            return

        try:
            symbol_with_quote = SymbolConverter.convert(symbol, "gate")
        except (AttributeError, TypeError, ValueError):
            symbol_with_quote = f"{symbol.upper()}_USDT"

        now_ts = time.time()
        last_warm = self._recent_warmups.get(symbol_with_quote)
        if last_warm and (now_ts - last_warm) < 2.0:
            return

        tasks = [
            asyncio.create_task(self._get_balance('USDT')),
            asyncio.create_task(self._get_pair_info(symbol_with_quote)),
        ]

        try:
            await asyncio.gather(*tasks)
        except Exception as exc:
            logger.debug("Gate.io warm-up issue for %s: %s", symbol_with_quote, exc)

        self._recent_warmups[symbol_with_quote] = time.time()

    def _load_pair_cache_from_disk(self) -> None:
        data = _load_json_file(self._pair_cache_path)
        if not isinstance(data, dict):
            return
        now = time.time()
        loaded = 0
        for pair, info in data.items():
            if isinstance(pair, str) and isinstance(info, dict):
                self._pair_info_cache[pair] = info
                self._pair_info_time[pair] = now
                loaded += 1
        if loaded:
            logger.debug(
                "Gate.io pair cache primed with %d entries from %s",
                loaded,
                self._pair_cache_path,
            )

    def _persist_pair_cache_if_needed(self) -> None:
        now = time.time()
        if now - self._last_pair_cache_persist < 2.0:
            return
        payload = {
            pair: info
            for pair, info in self._pair_info_cache.items()
            if isinstance(info, dict)
        }
        if not payload:
            return
        _atomic_write_json(self._pair_cache_path, payload)
        self._last_pair_cache_persist = now
    
    def _create_session(self):
        """Optimize edilmiş HTTP session oluştur"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': self.user_agent,
            'Connection': 'keep-alive'
        }

        session = _create_bound_session(
            source_ip=self.local_ip,
            headers=headers,
            pool_connections=30,
            pool_maxsize=30,
        )
        return session

    @staticmethod
    def _load_lookup_timeouts() -> List[float]:
        raw = os.getenv("GATE_LOOKUP_ATTEMPTS")
        if raw:
            values: List[float] = []
            for item in raw.split(","):
                item = item.strip()
                if not item:
                    continue
                try:
                    parsed = float(item)
                    if parsed > 0:
                        values.append(parsed)
                except ValueError:
                    logger.warning("Invalid GATE_LOOKUP_ATTEMPTS component: %s", item)
            if values:
                return values
        return [0.35, 0.6, 1.0, 1.5, 1.8]

    def _load_spot_balance_recovery_threshold(self) -> Decimal:
        raw = os.getenv("GATE_SPOT_BALANCE_RECOVERY_THRESHOLD", "1")
        value = _coerce_decimal(raw)
        if value is None:
            logger.warning("Invalid GATE_SPOT_BALANCE_RECOVERY_THRESHOLD=%s, using 1", raw)
            value = Decimal("1")
        if value < 0:
            value = Decimal("0")
        return value

    async def _probe_spot_usdt_balance(self) -> Optional[Decimal]:
        """Fetch USDT balance with best-effort conversion to Decimal."""
        try:
            balance = await self._get_balance("USDT", force=True)
        except Exception as exc:
            logger.debug("Gate.io balance probe failed: %s", exc)
            return None
        return _coerce_decimal(balance)
    
    def _warm_up(self):
        """Bağlantıları ve önbelleği hazırla"""
        try:
            socket.getaddrinfo("api.gateio.ws", 443)
        except OSError as exc:
            logger.debug("Gate.io DNS warm-up failed: %s", exc)

        try:
            self.session.get(f"{self.base_url}/api/v4/spot/time", timeout=self.http_timeout)
        except (requests.RequestException, OSError, ValueError) as exc:
            logger.debug("Gate.io REST warm-up failed: %s", exc)

        return True
    
    def _preload_balance_background(self):
        """Arka planda bakiye yükle"""
        try:
            t = int(time.time())
            
            method = "GET"
            endpoint = f"{self.api_prefix}/spot/accounts"
            signature_string = f"{method}\n{endpoint}\n\n{self._empty_payload_hash}\n{t}"
            
            sign = hmac.new(
                self._pre_encoded_secret, 
                signature_string.encode('utf-8'), 
                hashlib.sha512
            ).hexdigest()
            
            headers = {
                'KEY': self.api_key,
                'Timestamp': str(t),
                'SIGN': sign,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.base_url}{endpoint}"
            
            response = self.session.get(url, headers=headers, timeout=self.balance_timeout)
            
            if response.status_code == 200:
                data = response.json()
                current_time = time.time()

                if not isinstance(data, list):
                    logger.debug("Gate.io balance preload returned non-list payload: %s", type(data).__name__)
                    return

                for account in data:
                    if not isinstance(account, dict):
                        continue
                    currency = account.get("currency")
                    if currency:
                        cache_key = f"balance_{currency}"
                        available = _coerce_float(account.get("available", "0"))
                        self._balance_cache[cache_key] = available or 0.0
                
                self._balance_time = current_time
                self._balance_init_done = True
        except (requests.RequestException, OSError, TypeError, ValueError) as exc:
            logger.debug("Gate.io balance preload failed: %s", exc)

    async def _post(self, url, **kw):
        fn = functools.partial(self.session.post, url, **kw)
        return await asyncio.to_thread(fn)

    async def _rapid_get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        attempt_timeouts: Optional[List[float]] = None,
        log_level: int = logging.WARNING,
    ) -> Optional[requests.Response]:
        if attempt_timeouts is None:
            attempt_timeouts = list(self.lookup_timeouts)

        def extractor(response: requests.Response) -> Tuple[Optional[requests.Response], Optional[str]]:
            return response, None

        result, errors, meta = await _fast_http_lookup(
            url=url,
            session=self.session,
            params=params,
            headers=headers,
            attempt_timeouts=attempt_timeouts,
            extractor=extractor,
        )

        if errors and label:
            joined = " | ".join(errors[:6])
            logger.log(log_level, f"{label} failed: {joined}")
        elif meta and label:
            logger.debug(
                f"{label} success: attempt={meta['attempt']} via={meta['winner']} "
                f"elapsed={meta['elapsed_ms']:.1f}ms"
            )

        return result

    async def _get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        attempt_timeouts: Optional[List[float]] = None,
        log_level: int = logging.WARNING,
    ) -> Optional[requests.Response]:
        return await self._rapid_get(
            url,
            params=params,
            headers=headers,
            label=label,
            attempt_timeouts=attempt_timeouts,
            log_level=log_level,
        )

    async def _get_pair_info(self, currency_pair):
        """Fetch Gate.io pair metadata with caching."""
        now = time.time()
        if (
            currency_pair in self._pair_info_cache
            and now - self._pair_info_time.get(currency_pair, 0) < self._pair_info_ttl
        ):
            return self._pair_info_cache[currency_pair]

        url = f"{self.base_url}{self.api_prefix}/spot/currency_pairs/{currency_pair}"
        try:
            resp = await self._get(
                url,
                label=f"Gate.io pair info {currency_pair}",
                log_level=logging.DEBUG,
            )
            if resp and resp.status_code == 200:
                data = resp.json()
                self._pair_info_cache[currency_pair] = data
                self._pair_info_time[currency_pair] = now
                self._persist_pair_cache_if_needed()
                return data
        except Exception:
            pass
        return None

    async def _fetch_gate_ticker_price(self, currency_pair: str) -> Optional[float]:
        """Retrieve last price from Gate ticker endpoint."""
        try:
            url = f"{self.base_url}{self.api_prefix}/spot/tickers?currency_pair={currency_pair}"
            resp = await self._get(
                url,
                label=f"Gate.io ticker {currency_pair}",
                log_level=logging.DEBUG,
            )
            if resp and resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    last_price = data[0].get("last")
                    if last_price is not None:
                        return float(last_price)
        except Exception as exc:
            logger.debug("Gate.io price fetch failed for %s: %s", currency_pair, exc)
        return None

    @staticmethod
    def _extract_price_from_pair_info(pair_info: Optional[Dict[str, Any]]) -> Optional[float]:
        """Attempt to read a usable price from pair metadata."""
        if not isinstance(pair_info, dict):
            return None
        for key in ("last", "last_price", "close", "mark_price", "reference_price"):
            candidate = pair_info.get(key)
            if candidate is None:
                continue
            try:
                price = float(candidate)
            except (TypeError, ValueError):
                continue
            if price > 0:
                return price
        return None

    async def _resolve_gate_price(
        self,
        symbol: str,
        currency_pair: str,
        *,
        pair_info: Optional[Dict[str, Any]] = None,
        attempts: Optional[int] = None,
        delay_ms: Optional[float] = None,
    ) -> Optional[float]:
        """Resolve a tradable price with retry/backoff and optional fallbacks."""
        if attempts is None:
            try:
                attempts = int(os.getenv("GATE_PRICE_RETRY_ATTEMPTS", "4"))
            except ValueError:
                attempts = 4
        attempts = max(1, attempts)

        if delay_ms is None:
            try:
                delay_ms = float(os.getenv("GATE_PRICE_RETRY_DELAY_MS", "150"))
            except ValueError:
                delay_ms = 150.0
        delay_ms = max(0.0, delay_ms)

        resolved_price = None
        for attempt in range(1, attempts + 1):
            resolved_price = await self._fetch_gate_ticker_price(currency_pair)
            if resolved_price and resolved_price > 0:
                if attempt > 1:
                    logger.info(
                        "Gate.io %s price resolved after %d attempts",
                        symbol.upper(),
                        attempt,
                    )
                return resolved_price
            if attempt < attempts and delay_ms > 0.0:
                await asyncio.sleep(min(delay_ms * attempt, 600.0) / 1000.0)

        fallback_price = self._extract_price_from_pair_info(pair_info)
        if fallback_price:
            logger.warning(
                "Gate.io %s price fallback via pair metadata: %.8f",
                symbol.upper(),
                fallback_price,
            )
            return fallback_price

        logger.warning(
            "Gate.io %s price unavailable after %d attempts",
            symbol.upper(),
            attempts,
        )
        return resolved_price

    def _format_gate_amount(self, quantity, precision):
        """Format base quantity according to Gate precision rules."""
        try:
            if precision is None:
                precision = 8
            precision = max(int(precision), 0)
            decimal_quantity = Decimal(str(quantity))
            quantum = Decimal('1') if precision == 0 else Decimal('1').scaleb(-precision)
            quantized = decimal_quantity.quantize(quantum, rounding=ROUND_DOWN)
            if quantized <= 0:
                quantized = quantum
            result = format(quantized, 'f').rstrip('0').rstrip('.')
            return result or "0"
        except Exception as exc:
            logger.error(f"Gate.io format error: {exc}")
            return str(quantity)

    def _generate_signature(self, method, endpoint, query_string="", payload_string=""):
        """Gate.io imzalama"""
        if not endpoint.startswith(self.api_prefix):
            endpoint = f"{self.api_prefix}{endpoint}"

        t = int(time.time())

        if not payload_string:
            hashed_payload = self._empty_payload_hash
        else:
            m = hashlib.sha512()
            m.update(payload_string.encode('utf-8'))
            hashed_payload = m.hexdigest()
        
        signature_string = f"{method}\n{endpoint}\n{query_string}\n{hashed_payload}\n{t}"
        
        sign = hmac.new(
            self._pre_encoded_secret, 
            signature_string.encode('utf-8'), 
            hashlib.sha512
        ).hexdigest()
        
        headers = {
            'KEY': self.api_key,
            'Timestamp': str(t),
            'SIGN': sign,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        return headers
    
    async def _get_balance(self, currency='USDT', force=False):
        """USDT bakiyesini al"""
        current_time = time.time()
        cache_key = f"balance_{currency}"
        
        if self.test_mode:
            return 1000.0
        
        if not force and cache_key in self._balance_cache and (current_time - self._balance_time < 60):
            return self._balance_cache[cache_key]
        
        if self._balance_init_done and not force and cache_key in self._balance_cache:
            return self._balance_cache[cache_key]
        
        try:
            method = "GET"
            endpoint = "/spot/accounts"

            headers = self._generate_signature(method, endpoint)
            url = f"{self.base_url}{self.api_prefix}{endpoint}"

            response = await asyncio.to_thread(
                self.session.get,
                url,
                headers=headers,
                timeout=self.balance_timeout,
            )

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    for account in data:
                        if not isinstance(account, dict):
                            continue
                        acc_currency = account.get("currency")
                        if acc_currency:
                            balance_key = f"balance_{acc_currency}"
                            available = _coerce_float(account.get("available", "0"))
                            self._balance_cache[balance_key] = available or 0.0

                    self._balance_time = current_time
                    self._balance_init_done = True
                    return self._balance_cache.get(cache_key, 0.0)

                api_logger.error("Gate.io balance error: invalid payload type %s", type(data).__name__)
            else:
                api_logger.error(f"Gate.io balance error: HTTP {response.status_code}: {response.text}")

            if cache_key in self._balance_cache:
                return self._balance_cache[cache_key]

            return 0.0

        except (requests.RequestException, OSError, TypeError, ValueError) as exc:
            api_logger.error(f"Gate.io balance error: {exc}")
            if cache_key in self._balance_cache:
                return self._balance_cache[cache_key]
            return 0.0
    
    @measure_time
    async def execute_trade(self, symbol, amount=None, preparation_data=None):
        """Gate.io Market Order ile işlem"""
        trade_start = time.perf_counter()
        attempt_started_ms = int(time.time() * 1000)
        
        if amount is None:
            amount = 10.0

        try:
            reference_price = None
            premium_limit_pct = None
            retry_delay_ms = 0
            requested_base_amount = None
            if isinstance(preparation_data, dict):
                reference_price = _coerce_float(preparation_data.get('reference_price'))
                if reference_price is not None and reference_price <= 0:
                    reference_price = None
                premium_limit_pct = _coerce_float(preparation_data.get('max_premium_pct'))
                retry_delay_ms = _coerce_int(preparation_data.get('retry_delay_ms', 0)) or 0

            observation_price = None
            observed_premium_pct = None

            symbol_with_quote = SymbolConverter.convert(symbol, "gate")

            if self.test_mode:
                await asyncio.sleep(0.001)
                return {"success": True, "exchange": "gate", "symbol": symbol}

            balance_task = asyncio.create_task(self._get_balance('USDT'))
            pair_info_task = asyncio.create_task(self._get_pair_info(symbol_with_quote))

            balance = await balance_task
            try:
                pair_info = await pair_info_task
            except Exception:
                pair_info = None

            if amount > balance:
                amount = min(balance * 0.99, 200.0)  # Cap at $200

            if amount < 10.0:
                logger.error(f"Gate.io amount too small: {amount}")
                return {"success": False, "error": "Amount below minimum (10 USDT)"}

            quote_precision = 2
            amount_precision = None
            base_amount_str = None
            min_base_amount_dec = None

            min_quote_amount = None
            if pair_info:
                trade_status = (pair_info.get('trade_status') or '').lower()
                if trade_status and trade_status not in {'trading', 'tradable'}:
                    logger.warning(f"Gate.io {symbol} trade status {pair_info.get('trade_status')}; skipping")
                    return {
                        "success": False,
                        "skipped": True,
                        "exchange": "gate",
                        "symbol": symbol,
                        "reason": "symbol_not_listed",
                        "execution_time": time.perf_counter() - trade_start,
                    }
                amount_precision = _coerce_int(pair_info.get('amount_precision'))
                raw_price_precision = _coerce_int(pair_info.get('precision'))
                if raw_price_precision is not None and amount_precision is not None:
                    PRECISION_MANAGER.update_precision(
                        'gate',
                        symbol.upper(),
                        raw_price_precision,
                        amount_precision,
                    )
                qp = _coerce_int(pair_info.get('quote_precision'))
                if qp is not None:
                    quote_precision = max(qp, 2)
                min_quote_amount = _coerce_float(pair_info.get('min_quote_amount'))
                min_base_amount_dec = _coerce_decimal(pair_info.get('min_base_amount'))

            price_for_sizing = reference_price
            if not price_for_sizing or price_for_sizing <= 0:
                price_for_sizing = await self._resolve_gate_price(
                    symbol,
                    symbol_with_quote,
                    pair_info=pair_info,
                )

            if min_quote_amount and amount < min_quote_amount:
                amount = min_quote_amount

            spend_amount = amount
            if min_quote_amount:
                spend_amount = max(spend_amount, min_quote_amount)

            safe_cap = min(balance * 0.99, 200.0)  # Cap at $200
            if spend_amount > safe_cap:
                spend_amount = safe_cap

            if spend_amount <= 0:
                logger.error(f"Gate.io spend amount too small after adjustments: {spend_amount}")
                return {
                    "success": False,
                    "exchange": "gate",
                    "symbol": symbol,
                    "error": "insufficient_balance",
                    "execution_time": time.perf_counter() - trade_start,
                }

            if price_for_sizing and price_for_sizing > 0:
                base_quantity = spend_amount / price_for_sizing
                base_amount_str = self._format_gate_amount(base_quantity, amount_precision)
                try:
                    requested_base_amount = float(base_amount_str)
                except (TypeError, ValueError):
                    requested_base_amount = None
                if min_base_amount_dec is not None:
                    try:
                        if Decimal(base_amount_str or '0') < min_base_amount_dec:
                            base_amount_str = self._format_gate_amount(min_base_amount_dec, amount_precision)
                            try:
                                requested_base_amount = float(base_amount_str)
                            except (TypeError, ValueError):
                                requested_base_amount = None
                    except Exception:
                        pass
            else:
                logger.warning(f"Gate.io {symbol} price unavailable; skipping trade")
                return {
                    "success": False,
                    "skipped": True,
                    "exchange": "gate",
                    "symbol": symbol,
                    "reason": "price_unavailable",
                    "execution_time": time.perf_counter() - trade_start,
                }

            if reference_price and premium_limit_pct is not None and premium_limit_pct >= 0:
                attempts = 2 if retry_delay_ms > 0 else 1
                for attempt in range(attempts):
                    observation_price = await self._fetch_gate_ticker_price(symbol_with_quote)
                    if observation_price and reference_price:
                        observed_premium_pct = ((observation_price - reference_price) / reference_price) * 100
                        logger.info(
                            f"Gate.io {symbol} premium check: price={observation_price:.6f} ref={reference_price:.6f} diff={observed_premium_pct:.2f}%"
                        )
                        if observed_premium_pct <= premium_limit_pct:
                            break
                        if attempt == 0 and retry_delay_ms > 0:
                            logger.warning(
                                f"Gate.io {symbol} premium {observed_premium_pct:.2f}%>{premium_limit_pct:.2f}% - waiting {retry_delay_ms}ms before recheck"
                            )
                            await asyncio.sleep(max(retry_delay_ms, 0) / 1000)
                    else:
                        observation_price = None
                        observed_premium_pct = None
                        break

                if observed_premium_pct is not None and observed_premium_pct > premium_limit_pct:
                    logger.warning(
                        f"🚫 Skipping Gate.io buy for {symbol}: premium {observed_premium_pct:.2f}% exceeds limit {premium_limit_pct:.2f}% (ref {reference_price:.6f}, last {observation_price if observation_price else 0:.6f})"
                    )
                    trade_time = time.perf_counter() - trade_start
                    return {
                        "success": False,
                        "skipped": True,
                        "exchange": "gate",
                        "symbol": symbol,
                        "reason": "premium_exceeded",
                        "reference_price": reference_price,
                        "observed_price": observation_price,
                        "premium_pct": observed_premium_pct,
                        "execution_time": trade_time
                    }

            order_data = {
                "currency_pair": symbol_with_quote,
                "side": "buy",
                "type": "market",
                "account": "spot",
                "time_in_force": "ioc",
            }

            quote_amount_str = format_quote_amount(spend_amount, quote_precision)
            order_data["amount"] = quote_amount_str
            order_data["text"] = self._generate_order_text("ultra")
            action_mode = (os.getenv("GATE_ORDER_ACTION_MODE", "ACK") or "").strip().upper()
            if action_mode:
                order_data["action_mode"] = action_mode

            order_json = json.dumps(order_data)

            method = "POST"
            endpoint = "/spot/orders"
            query_string = ""

            exptime_ms_raw = int(os.getenv("GATE_ORDER_EXPTIME_MS", "2000"))
            exptime_ms = max(200, exptime_ms_raw) if exptime_ms_raw > 0 else 0

            url = f"{self.base_url}{self.api_prefix}{endpoint}"

            if base_amount_str:
                logger.info(
                    f"🟢 GATE.IO MARKET BUY: {symbol} spend={quote_amount_str} USDT (≈ {base_amount_str} {symbol.upper()})"
                )
            else:
                logger.info(
                    f"🟢 GATE.IO MARKET BUY: {symbol} spend={quote_amount_str} USDT"
                )

            baseline_balance = await self._probe_spot_usdt_balance()
            max_attempts = self.spot_order_max_attempts
            last_error: Optional[str] = None

            for attempt in range(1, max_attempts + 1):
                headers = self._generate_signature(method, endpoint, query_string, order_json)
                if exptime_ms > 0:
                    headers["X-Gate-Exptime"] = str(int(time.time() * 1000) + exptime_ms)

                if not RATE_LIMITER.can_request('gate', weight=1, is_order=True):
                    wait_time = RATE_LIMITER.get_wait_time('gate', weight=1, is_order=True)
                    logger.warning(f"gate rate limit - waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

                try:
                    resp = await self._post(
                        url,
                        headers=headers,
                        data=order_json,
                        timeout=(self.order_connect_timeout, self.order_read_timeout),
                    )
                    if resp.status_code in [200, 201]:
                        RATE_LIMITER.add_request('gate', weight=1, is_order=True)
                        try:
                            result = resp.json()
                        except ValueError:
                            result = {"raw": resp.text}

                        order_id = result.get("id") or result.get("order_id")
                        if isinstance(order_id, (int, float)):
                            order_id = str(order_id)
                        elif isinstance(order_id, dict):
                            order_id = str(order_id.get("id") or order_id.get("order_id") or "")
                        elif not isinstance(order_id, str):
                            order_id = str(order_id) if order_id is not None else ""

                        confirmation_data = None
                        confirm_default = "200" if self.fast_gate_mode else "250"
                        confirm_window_ms = int(os.getenv("GATE_CONFIRM_WINDOW_MS", confirm_default))
                        if confirm_window_ms > 0 and order_id:
                            confirmation_data = await self._confirm_gate_order(
                                order_id,
                                symbol_with_quote,
                                confirm_window_ms,
                            )

                        def _positive_or_none(value: Optional[float]) -> Optional[float]:
                            if value is None or value <= 0:
                                return None
                            return value

                        filled_quote = _positive_or_none(_coerce_float(result.get("filled_total")))
                        if filled_quote is None:
                            filled_quote = _positive_or_none(_coerce_float(result.get("filled_quote")))
                        if filled_quote is None and confirmation_data:
                            filled_quote = _positive_or_none(_coerce_float(confirmation_data.get("filled_total")))
                        if filled_quote is None and confirmation_data:
                            filled_quote = _positive_or_none(_coerce_float(confirmation_data.get("filled_quote")))

                        base_left = _coerce_float(result.get("left"))
                        if base_left is None and confirmation_data:
                            base_left = _coerce_float(confirmation_data.get("left"))

                        filled_base = None
                        if requested_base_amount is not None and base_left is not None:
                            filled_base = max(requested_base_amount - base_left, 0.0)
                            if filled_base <= 0:
                                filled_base = None
                        if filled_base is None:
                            filled_base = _positive_or_none(_coerce_float(result.get("filled_amount")))
                        if filled_base is None and confirmation_data:
                            filled_base = _positive_or_none(_coerce_float(confirmation_data.get("filled_amount")))

                        price_basis = observation_price or reference_price or price_for_sizing
                        if filled_base is None and filled_quote is not None and price_basis and price_basis > 0:
                            filled_base = filled_quote / price_basis
                        if filled_quote is None and filled_base is not None and price_basis and price_basis > 0:
                            filled_quote = filled_base * price_basis

                        confirmed_fill = bool(filled_quote or filled_base)
                        if not confirmed_fill:
                            recovered = await self._recover_spot_execution(
                                symbol=symbol,
                                symbol_with_quote=symbol_with_quote,
                                spend_amount=spend_amount,
                                baseline_balance=baseline_balance,
                                attempt_started_ms=attempt_started_ms,
                                trade_start=trade_start,
                                reference_price=reference_price,
                                observation_price=observation_price,
                                last_error="unconfirmed_fill",
                            )
                            if recovered:
                                return recovered

                            status = (confirmation_data or {}).get("status")
                            finish_as = (confirmation_data or {}).get("finish_as")
                            logger.warning(
                                "Gate.io order %s not confirmed (filled_total=%s, left=%s, status=%s, finish_as=%s)",
                                order_id,
                                filled_quote,
                                base_left,
                                status,
                                finish_as,
                            )
                            last_error = (
                                f"unconfirmed_fill status={status} finish_as={finish_as} "
                                f"left={base_left} filled_total={filled_quote}"
                            )
                            if attempt < max_attempts:
                                await asyncio.sleep(self._spot_order_retry_delay(attempt))
                                continue
                            break

                        trade_time = time.perf_counter() - trade_start
                        self._record_trade_success(trade_time)
                        if filled_base is not None and filled_quote is not None:
                            logger.info(
                                "✅ Gate.io %s market buy SUCCESS in %.2fms (filled %.2f USDT ≈ %.2f %s)",
                                symbol,
                                trade_time * 1000,
                                filled_quote,
                                filled_base,
                                symbol.upper(),
                            )
                        elif filled_quote is not None:
                            logger.info(
                                "✅ Gate.io %s market buy SUCCESS in %.2fms (filled %.2f USDT)",
                                symbol,
                                trade_time * 1000,
                                filled_quote,
                            )
                        else:
                            logger.info(f"✅ Gate.io {symbol} market buy SUCCESS in {trade_time*1000:.2f}ms")
                        return {
                            "success": True,
                            "exchange": "gate",
                            "symbol": symbol,
                            "amount": filled_quote,
                            "filled_quote": filled_quote,
                            "filled_base": filled_base,
                            "requested_amount": spend_amount,
                            "requested_base": requested_base_amount,
                            "reference_price": reference_price,
                            "observed_price": observation_price,
                            "premium_pct": observed_premium_pct,
                            "order_result": result,
                            "order_id": order_id,
                            "confirmation": confirmation_data,
                            "execution_time": trade_time
                        }

                    last_error = resp.text
                    logger.warning(
                        f"Gate.io order attempt {attempt}/{max_attempts} rejected: {last_error or 'unknown error'}"
                    )

                except Exception as exc:
                    last_error = str(exc)
                    logger.warning(
                        f"Gate.io order attempt {attempt}/{max_attempts} failed: {last_error}"
                    )

                if attempt < max_attempts:
                    await asyncio.sleep(self._spot_order_retry_delay(attempt))

            error = last_error or "unknown gate order error"
            logger.error(f"❌ Gate.io market order failed: {error}")
            recovered = await self._recover_spot_execution(
                symbol=symbol,
                symbol_with_quote=symbol_with_quote,
                spend_amount=spend_amount,
                baseline_balance=baseline_balance,
                attempt_started_ms=attempt_started_ms,
                trade_start=trade_start,
                reference_price=reference_price,
                observation_price=observation_price,
                last_error=error,
            )
            if recovered:
                return recovered
            return {
                "success": False,
                "exchange": "gate",
                "symbol": symbol,
                "reference_price": reference_price,
                "observed_price": observation_price,
                "premium_pct": observed_premium_pct,
                "error": error,
                "execution_time": time.perf_counter() - trade_start
            }

        except Exception as e:
            logger.error(f"Gate.io market order error: {str(e)}")
            return {
                "success": False,
                "exchange": "gate",
                "symbol": symbol,
                "error": str(e),
                "execution_time": time.perf_counter() - trade_start
            }

    async def execute_market_slices(
        self,
        symbol: str,
        total_amount: float,
        *,
        slice_amount: Optional[float] = None,
        preparation_data: Optional[Dict[str, Any]] = None,
        max_slices: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Gate.io için bölünmüş market alışları uygula."""

        try:
            total_target = max(float(total_amount or 0.0), 0.0)
        except (TypeError, ValueError):
            total_target = 0.0

        if total_target <= 0:
            return {
                "success": False,
                "exchange": "gate",
                "symbol": symbol,
                "error": "invalid_total_amount",
                "execution_time": 0.0,
            }

        try:
            default_slice = float(os.getenv("GATE_MARKET_SLICE_USDT", "1000"))
        except ValueError:
            default_slice = 1000.0
        slice_budget = max(slice_amount if slice_amount is not None else default_slice, 10.0)

        try:
            default_max = int(os.getenv("GATE_MAX_SLICE_COUNT", "6"))
        except ValueError:
            default_max = 6
        max_slice_count = max_slices if max_slices is not None else default_max
        max_slice_count = max(1, max_slice_count)

        warmup_payload = preparation_data if isinstance(preparation_data, dict) else None
        try:
            await self._warm_up_symbol(symbol, preparation_data=warmup_payload)
        except Exception as exc:
            logger.debug("Gate.io warm-up skipped for %s: %s", symbol.upper(), exc)

        start_time = time.perf_counter()
        remaining = total_target
        filled_total = 0.0
        slice_results: List[Dict[str, Any]] = []

        for idx in range(max_slice_count):
            if remaining <= 0:
                break

            spend = min(slice_budget, remaining)
            slice_result = await self.execute_trade(symbol, amount=spend, preparation_data=preparation_data)
            slice_results.append(slice_result)

            if slice_result.get("success"):
                filled_value = slice_result.get("amount")
                try:
                    filled = float(filled_value) if filled_value is not None else spend
                except (TypeError, ValueError):
                    filled = spend
                filled_total += max(0.0, filled)
                remaining = max(0.0, total_target - filled_total)
                if remaining <= 0:
                    break
            else:
                # Failure -> kalan tutarı tekrar denemek yerine çık
                break

        total_time = time.perf_counter() - start_time
        success = filled_total > 0.0

        if success:
            logger.info(
                "Gate.io sliced execution summary: target=%.2f filled=%.2f slices=%d remaining=%.2f",
                total_target,
                filled_total,
                len(slice_results),
                remaining,
            )
        else:
            last_error = None
            if slice_results:
                last_error = slice_results[-1].get("error") or slice_results[-1].get("reason")
            logger.warning(
                "Gate.io sliced execution failed: target=%.2f slices=%d last_error=%s",
                total_target,
                len(slice_results),
                last_error or "no_attempt",
            )

        return {
            "success": success,
            "exchange": "gate",
            "symbol": symbol,
            "amount": filled_total,
            "filled_quote": filled_total,
            "requested_amount": total_target,
            "remaining_amount": remaining,
            "slice_amount": slice_budget,
            "slice_count": len(slice_results),
            "slices": slice_results,
            "execution_time": total_time,
        }

    async def _confirm_gate_order(self, order_id: str, currency_pair: str, window_ms: int) -> Optional[Dict[str, Any]]:
        """Gate order durumunu hızlıca poll ederek dolumu doğrula (ACK sonrasında)."""

        try:
            window_ms = int(window_ms)
        except Exception:
            window_ms = 0
        if window_ms <= 0:
            return None

        poll_interval_ms = max(30, getattr(self, "confirm_poll_interval_ms", 80))
        max_polls = getattr(self, "confirm_max_polls", 0)
        if max_polls <= 0:
            max_polls = max(1, int((window_ms + poll_interval_ms - 1) / poll_interval_ms))

        endpoint = f"/spot/orders/{order_id}"
        query_params = {"currency_pair": currency_pair}
        from urllib.parse import urlencode

        query_string = urlencode(query_params)
        headers = self._generate_signature("GET", endpoint, query_string, "")
        url = f"{self.base_url}{self.api_prefix}{endpoint}?{query_string}"

        deadline = time.monotonic() + (window_ms / 1000.0)
        last_confirmation: Optional[Dict[str, Any]] = None

        for poll_index in range(max_polls):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            timeout_sec = min(max(0.05, poll_interval_ms / 1000.0), remaining)

            try:
                response = await self._get(
                    url,
                    headers=headers,
                    label=f"Gate.io order confirm {order_id}",
                    attempt_timeouts=[timeout_sec],
                    log_level=logging.DEBUG,
                )
                if response and response.status_code == 200:
                    data = response.json()
                    filled_total = data.get("filled_total")
                    filled_amount = data.get("filled_amount")
                    finish_as = data.get("finish_as")
                    status = data.get("status")
                    try:
                        filled_total = float(filled_total) if filled_total is not None else None
                    except (TypeError, ValueError):
                        filled_total = None
                    try:
                        filled_amount = float(filled_amount) if filled_amount is not None else None
                    except (TypeError, ValueError):
                        filled_amount = None

                    confirmation = {
                        "filled_total": filled_total,
                        "filled_amount": filled_amount,
                        "left": data.get("left"),
                        "status": status,
                        "finish_as": finish_as,
                    }
                    in_time = response.headers.get("X-In-Time")
                    out_time = response.headers.get("X-Out-Time")
                    if in_time or out_time:
                        confirmation["gateway_timing"] = {
                            "in": in_time,
                            "out": out_time,
                        }
                    last_confirmation = confirmation

                    if (filled_total is not None and filled_total > 0) or (
                        filled_amount is not None and filled_amount > 0
                    ):
                        return confirmation

                    finish_label = str(finish_as or "").lower()
                    status_label = str(status or "").lower()
                    if finish_label in {"cancelled", "canceled", "expired", "ioc"}:
                        return confirmation
                    if status_label in {"cancelled", "canceled", "expired"}:
                        return confirmation
            except Exception as exc:
                logger.debug(f"Gate.io order confirmation failed for {order_id}: {exc}")

            if poll_index < max_polls - 1:
                await asyncio.sleep(max(0.0, min(poll_interval_ms / 1000.0, deadline - time.monotonic())))

        if last_confirmation:
            filled_total = last_confirmation.get("filled_total")
            filled_amount = last_confirmation.get("filled_amount")
            finish_as = last_confirmation.get("finish_as")
            logger.warning(
                "Gate.io order %s confirmation pending (filled_total=%s, filled_amount=%s, finish_as=%s)",
                order_id,
                filled_total,
                filled_amount,
                finish_as,
            )
        return last_confirmation

    async def _fetch_recent_trade(self, currency_pair: str) -> Optional[Dict[str, Any]]:
        from urllib.parse import urlencode

        endpoint = "/spot/my_trades"
        params = {"currency_pair": currency_pair, "limit": 1}
        query_string = urlencode(params)
        headers = self._generate_signature("GET", endpoint, query_string, "")
        url = f"{self.base_url}{self.api_prefix}{endpoint}?{query_string}"
        response = await self._get(
            url,
            headers=headers,
            label=f"Gate.io recent trade {currency_pair}",
            attempt_timeouts=[0.6],
            log_level=logging.DEBUG,
        )
        if response and response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                return None
            if isinstance(data, list) and data:
                return data[0]
        return None

    async def _recover_spot_execution(
        self,
        *,
        symbol: str,
        symbol_with_quote: str,
        spend_amount: float,
        baseline_balance: Optional[Decimal],
        attempt_started_ms: int,
        trade_start: float,
        reference_price: Optional[float],
        observation_price: Optional[float],
        last_error: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if baseline_balance is None:
            return None
        current_balance = await self._probe_spot_usdt_balance()
        if current_balance is None:
            return None
        delta = baseline_balance - current_balance
        if delta <= 0 or delta < self.spot_balance_recovery_threshold:
            return None

        logger.warning(
            "Gate.io spot balance dropped by %s USDT after failure (%s); attempting recovery",
            _decimal_to_str(delta),
            last_error or "unknown_error",
        )

        trade_detail = await self._fetch_recent_trade(symbol_with_quote)
        trade_time_ms: Optional[int] = None
        trade_amount: Optional[Decimal] = None
        trade_price: Optional[Decimal] = None
        if trade_detail:
            raw_ms = trade_detail.get("create_time_ms") or trade_detail.get("create_time") or 0
            try:
                trade_time_ms = int(float(raw_ms))
            except (TypeError, ValueError):
                trade_time_ms = None
            try:
                trade_amount = Decimal(str(trade_detail.get("amount")))
            except Exception:
                trade_amount = None
            try:
                trade_price = Decimal(str(trade_detail.get("price")))
            except Exception:
                trade_price = None

        if trade_time_ms is not None and trade_time_ms + 2000 < attempt_started_ms:
            trade_detail = None
            trade_amount = None
            trade_price = None

        recovered_amount = float(delta)
        observed_price = float(trade_price) if trade_price is not None else observation_price

        return {
            "success": True,
            "exchange": "gate",
            "symbol": symbol,
            "amount": recovered_amount,
            "reference_price": reference_price,
            "observed_price": observed_price,
            "premium_pct": None,
            "order_result": {
                "recovered": True,
                "original_error": last_error,
                "trade_id": (trade_detail or {}).get("id"),
            },
            "order_id": (trade_detail or {}).get("order_id"),
            "confirmation": trade_detail,
            "execution_time": time.perf_counter() - trade_start,
        }

# -----------------------------------------------------------------------------
# ULTRA TRADER - ANA SINIF (MULTI-ACCOUNT + MARKET CAP FILTER)
# -----------------------------------------------------------------------------
