from .common import *
from .common import (
    _atomic_write_json,
    _create_bound_session,
    _env_flag,
    _fast_http_lookup,
    _get_local_ip,
    _load_json_file,
)


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

class MexcHyperbeastTrader:
    """Ultra hızlı MEXC işlem sınıfı"""

    def __init__(
        self,
        test_mode: bool = False,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        label: Optional[str] = None,
        tuning: Optional[MexcTuning] = None,
    ) -> None:
        self.api_key = (api_key or os.getenv("MEXC_API_KEY", "")).strip()
        self.api_secret = (api_secret or os.getenv("MEXC_API_SECRET", "")).strip()
        self.account_label = label if label else "PRIMARY"
        self.base_url = "https://api.mexc.com"
        self.test_mode = test_mode

        self.tuning = tuning or MexcTuning()

        self.headers = {
            'X-MEXC-APIKEY': self.api_key,
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'User-Agent': 'HyperbeastTrader/3.0',
        }
        self._order_headers = dict(self.headers)

        self.local_ip = _get_local_ip("MEXC_LOCAL_IP")
        self.session = self._create_session()
        try:
            logger.info(f"MEXC account in use: {self.account_label}")
        except Exception:
            pass

        self.time_cache = ServerTimeCache('mexc')

        self._pre_encoded_key = self.api_key.encode('utf-8') if self.api_key else b''
        self._pre_encoded_secret = self.api_secret.encode('utf-8') if self.api_secret else b''

        self._balance_cache: Optional[float] = None
        self._balance_updated = 0.0

        self._symbol_info_cache: Dict[str, Dict[str, Any]] = {}
        self._symbol_info_cache_time: Dict[str, float] = {}
        self._symbol_info_cache_ttl = 3600.0
        ttl_override = os.getenv("MEXC_SYMBOL_CACHE_TTL_OVERRIDE")
        if ttl_override:
            try:
                self._symbol_info_cache_ttl = float(ttl_override)
                logger.info("MEXC symbol cache TTL override applied: %.1fs", self._symbol_info_cache_ttl)
            except ValueError:
                logger.warning("Invalid MEXC_SYMBOL_CACHE_TTL_OVERRIDE=%s", ttl_override)
        cache_path_env = os.getenv("MEXC_SYMBOL_CACHE_FILE")
        self._symbol_cache_path = Path(cache_path_env) if cache_path_env else MEXC_SYMBOL_CACHE_FILE
        self._last_symbol_cache_persist = 0.0
        self._load_symbol_cache_from_disk()

        def _read_int_env(name: str, default: int, *, min_value: int, max_value: int) -> int:
            raw_value = os.getenv(name)
            if raw_value is None or raw_value.strip() == "":
                return default
            try:
                parsed = int(float(raw_value))
            except (TypeError, ValueError):
                logger.warning("Invalid %s=%s; using default %d", name, raw_value, default)
                return default
            return max(min_value, min(parsed, max_value))

        def _read_float_env(name: str, default: float, *, min_value: float, max_value: float) -> float:
            raw_value = os.getenv(name)
            if raw_value is None or raw_value.strip() == "":
                return default
            try:
                parsed = float(raw_value)
            except (TypeError, ValueError):
                logger.warning("Invalid %s=%s; using default %.4f", name, raw_value, default)
                return default
            return max(min_value, min(parsed, max_value))

        self.fast_mode = _env_flag(os.getenv("MEXC_FAST_MODE", "1"), default=True)
        self._ws_enabled = _env_flag(os.getenv("MEXC_USER_STREAM_ENABLED", "1"), default=True)
        if not MEXC_USERSTREAM_AVAILABLE:
            if self._ws_enabled:
                logger.info("MEXC user stream module unavailable; disabling WebSocket confirmations")
            self._ws_enabled = False
        self.ws_ack_timeout = max(0.05, self.tuning.ws_ack_timeout)
        self.ws_fill_timeout = max(self.ws_ack_timeout, self.tuning.ws_fill_timeout)
        self._fast_trades_verify = bool(self.tuning.fast_trades_verify)
        self.user_stream: Optional[MexcUserStream] = None
        self._user_stream_lock = asyncio.Lock()
        self._user_stream_start_task: Optional[asyncio.Task] = None
        self._http_client: Optional[httpx.Client] = None
        self._http_client_lock = threading.Lock()
        self._user_stream_failed = False
        self._rest_timeout = (
            max(0.01, self.tuning.rest_connect_timeout),
            max(0.05, self.tuning.rest_read_timeout),
        )
        self.base_recv_window_ms = max(1000, int(self.tuning.order_recv_window))
        logger.info("MEXC fast mode %s", "enabled" if self.fast_mode else "disabled")
        if self.fast_mode and not self._ws_enabled:
            logger.warning("MEXC fast mode enabled but user stream disabled; relying on REST-only confirmations")

        self.fill_confirm_wait_ms = _read_int_env(
            "MEXC_FILL_CONFIRM_WAIT_MS",
            default=max(700, int(self.tuning.rest_read_timeout * 1000)),
            min_value=300,
            max_value=15000,
        )
        self.fill_confirm_interval_ms = _read_int_env(
            "MEXC_FILL_CONFIRM_INTERVAL_MS",
            default=80,
            min_value=20,
            max_value=1000,
        )
        self.delayed_fill_wait_ms = _read_int_env(
            "MEXC_DELAYED_FILL_WAIT_MS",
            default=3500,
            min_value=0,
            max_value=30000,
        )
        self.delayed_fill_min_ratio = _read_float_env(
            "MEXC_DELAYED_FILL_MIN_RATIO",
            default=0.05,
            min_value=0.0,
            max_value=1.0,
        )
        self.order_query_weight = _read_float_env(
            "MEXC_ORDER_QUERY_WEIGHT",
            default=2.0,
            min_value=0.1,
            max_value=10.0,
        )
        self._order_query_limiter = WeightedWindowLimiter(
            window_seconds=_read_float_env("MEXC_ORDER_QUERY_WINDOW", 10.0, min_value=1.0, max_value=60.0),
            max_weight=_read_float_env("MEXC_ORDER_QUERY_BUDGET", 480.0, min_value=10.0, max_value=600.0),
            min_sleep=_read_float_env("MEXC_ORDER_QUERY_MIN_SLEEP", 0.02, min_value=0.0, max_value=0.5),
            jitter=_read_float_env("MEXC_ORDER_QUERY_JITTER", 0.02, min_value=0.0, max_value=0.2),
        )
        logger.debug(
            "MEXC fill confirmation windows: primary=%dms interval=%dms delayed=%dms ratio=%.3f",
            self.fill_confirm_wait_ms,
            self.fill_confirm_interval_ms,
            self.delayed_fill_wait_ms,
            self.delayed_fill_min_ratio,
        )

        self._check_api_credentials()

        self.performance = {
            'trade_count': 0,
            'last_trade_time': 0.0,
            'min_trade_time': float('inf'),
            'max_trade_time': 0.0,
            'avg_trade_time': 0.0,
            'total_trade_time': 0.0,
            'fallback_stats': {
                'level_1_success': 0,
                'level_2_success': 0,
                'level_3_success': 0,
                'level_4_success': 0,
                'total_fallbacks': 0,
            },
        }

        self._signature_cache: Dict[str, str] = {}
        self._warm_up()

        self._ws_fast_nonblocking = _env_flag(os.getenv("MEXC_FAST_NONBLOCKING_WS", "1"), default=True)
        try:
            self._ws_fast_wait = float(os.getenv("MEXC_FAST_WS_WAIT_SEC", "0.05"))
        except ValueError:
            self._ws_fast_wait = 0.05
        self._ws_fast_wait = max(0.0, min(self._ws_fast_wait, 0.5))

        self._symbol_prefetch_tasks: Dict[str, asyncio.Task] = {}
        self._symbol_prefetch_window = 0.3
        self._symbol_prefetch_interval = 0.05

        self.slippage_pct = float(os.getenv("MEXC_SLIPPAGE_PCT", "0.12"))
        self.min_fill_ratio = float(os.getenv("MEXC_MIN_FILL_RATIO", "0.9"))
        self.max_resubmits = int(os.getenv("MEXC_MAX_RESUBMITS", "2"))
        self.min_order_notional = float(os.getenv("MEXC_MIN_ORDER_NOTIONAL", "0.5"))

    def _create_session(self) -> requests.Session:
        """Create a tuned requests session for MEXC REST calls."""
        return _create_bound_session(
            source_ip=self.local_ip,
            headers=self.headers,
            pool_connections=20,
            pool_maxsize=20,
        )

    def _order_was_filled(self, order: dict) -> bool:
        """MEXC sipariş sonucunun gerçekten dolup dolmadığını kontrol et."""
        if not order:
            return False

        def _safe_float(value) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        payload = order.get('data') if isinstance(order, dict) and isinstance(order.get('data'), dict) else order
        status = (
            payload.get('status')
            or payload.get('state')
            or payload.get('origStatus')
            or payload.get('orderStatus')
            or payload.get('execStatus')
            or ''
        ).upper()

        executed_qty = _safe_float(
            payload.get('executedQty')
            or payload.get('executed_qty')
            or payload.get('dealQuantity')
            or payload.get('deal_quantity')
            or payload.get('dealQty')
            or payload.get('filledQty')
        )
        quote_qty = _safe_float(
            payload.get('cummulativeQuoteQty')
            or payload.get('cummulative_quote_qty')
            or payload.get('quoteQuantityFilled')
            or payload.get('fillVolume')
            or payload.get('dealAmount')
        )

        if status in {'FILLED', 'PARTIALLY_FILLED', 'PARTIAL_FILLED', 'PARTIAL_FILL'} and (
            executed_qty > 0 or quote_qty > 0
        ):
            return True

        if executed_qty > 0 and quote_qty > 0:
            return True

        fills = payload.get('fills')
        if isinstance(fills, list) and fills:
            return True

        return False

    def _extract_fill_amounts(self, payload: Any) -> Tuple[float, float]:
        if not isinstance(payload, dict):
            return 0.0, 0.0

        def _safe_float(value) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        qty = 0.0
        for key in (
            'executedQty',
            'executed_qty',
            'filledQty',
            'dealQuantity',
            'deal_quantity',
            'dealQty',
            'qty',
            'quantity',
        ):
            qty = max(qty, _safe_float(payload.get(key)))

        quote = 0.0
        for key in (
            'cummulativeQuoteQty',
            'cummulative_quote_qty',
            'executedQuoteQty',
            'quoteQuantityFilled',
            'fillVolume',
            'dealAmount',
            'filledQuote',
            'filledQuoteQty',
        ):
            quote = max(quote, _safe_float(payload.get(key)))

        return qty, quote

    def _record_trade_success(self, trade_time: float) -> None:
        self.performance['trade_count'] += 1
        self.performance['last_trade_time'] = trade_time
        self.performance['min_trade_time'] = min(self.performance['min_trade_time'], trade_time)
        self.performance['max_trade_time'] = max(self.performance['max_trade_time'], trade_time)
        self.performance['total_trade_time'] += trade_time
        self.performance['avg_trade_time'] = (
            self.performance['total_trade_time'] / self.performance['trade_count']
        )

    def _spot_order_retry_delay(self, attempt: int) -> float:
        return _retry_delay_from_sequence(
            self.spot_order_retry_sequence,
            self.spot_order_retry_tail_step,
            attempt,
        )

        session = _create_bound_session(
            source_ip=self.local_ip,
            headers=self.headers,
            pool_connections=20,
            pool_maxsize=20,
        )
        return session

    def _compute_recv_window(self) -> str:
        """Sunucu drift'ine göre dinamik recvWindow değeri hesapla."""
        drift = abs(int(self.time_cache.offset or 0))
        window = self.base_recv_window_ms + drift + 500
        window = min(60000, max(self.base_recv_window_ms, window))
        return str(int(window))

    async def _throttle_order_query(self) -> None:
        """Order durumu sorgularını kısıtla; rate-limit ve jitter uygula."""
        limiter = getattr(self, "_order_query_limiter", None)
        if limiter:
            await limiter.acquire(self.order_query_weight)

        while not RATE_LIMITER.can_request('mexc', weight=1, is_order=False):
            wait_time = RATE_LIMITER.get_wait_time('mexc', weight=1, is_order=False)
            if wait_time <= 0:
                break
            await asyncio.sleep(max(wait_time, 0.01))
        RATE_LIMITER.add_request('mexc', weight=1, is_order=False)

    def _log_mexc_rejection(self, context: str, response, default_text: str) -> Dict[str, Any]:
        """Hata kodunu ayrıştır ve uygun seviyede logla."""
        status = getattr(response, "status_code", None)
        payload: Optional[Dict[str, Any]] = None
        code: Optional[int] = None
        message = default_text

        if response is not None:
            try:
                payload = response.json()
            except ValueError:
                payload = None

        if isinstance(payload, dict):
            data_block = payload.get("data") if isinstance(payload.get("data"), dict) else payload
            if isinstance(data_block, dict):
                try:
                    code = int(data_block.get("code")) if data_block.get("code") is not None else None
                except (TypeError, ValueError):
                    code = None
                message = (
                    data_block.get("msg")
                    or data_block.get("message")
                    or default_text
                )

        log_suffix = f"[status={status} code={code}]" if status or code else ""
        if status == 403:
            logger.error("MEXC %s rejected by WAF %s %s", context, log_suffix, message)
        elif code == 700006:
            logger.error("MEXC %s rejected: IP not in whitelist %s %s", context, log_suffix, message)
        elif code == 700003:
            logger.error("MEXC %s rejected: timestamp outside recvWindow %s %s", context, log_suffix, message)
        else:
            logger.warning("MEXC %s rejected %s %s", context, log_suffix, message)

        return {
            "status": status,
            "code": code,
            "message": message,
            "payload": payload,
        }

    async def ensure_user_stream(self) -> Optional[MexcUserStream]:
        if self.test_mode or not self._ws_enabled:
            return None
        if not self.api_key or not self.api_secret:
            return None
        if self.user_stream and self.user_stream.is_running:
            return self.user_stream
        async with self._user_stream_lock:
            if self.user_stream and self.user_stream.is_running:
                return self.user_stream
            if not MEXC_USERSTREAM_AVAILABLE:
                if not self._user_stream_failed:
                    logger.info("MEXC user stream disabled; continuing without WebSocket confirmations")
                    self._user_stream_failed = True
                self.user_stream = None
                return None
            self.user_stream = MexcUserStream(
                self.session,
                self.api_key,
                self.api_secret,
                base_url=self.base_url,
                time_provider=self.time_cache.get_time,
            )
            started = await self.user_stream.ensure_started()
            if not started:
                if not self._user_stream_failed:
                    logger.warning("MEXC user stream could not be started; continuing without WebSocket confirmations")
                    self._user_stream_failed = True
                self.user_stream = None
                return None
            self._user_stream_failed = False
            try:
                ready = await self.user_stream.wait_until_ready(timeout=max(self.ws_ack_timeout, 0.8))
                if not ready:
                    logger.debug("MEXC user stream readiness wait timed out; proceeding anyway")
            except Exception as exc:
                logger.debug("MEXC user stream wait_until_ready error: %s", exc)
            return self.user_stream

    def _get_http_client(self) -> httpx.Client:
        client = self._http_client
        if client and not client.is_closed:
            return client
        with self._http_client_lock:
            client = self._http_client
            if client and getattr(client, "is_closed", False) is False:
                return client
            timeout = httpx.Timeout(
                connect=self._rest_timeout[0],
                read=self._rest_timeout[1],
                write=0.20,
                pool=1.0,
            )
            self._http_client = httpx.Client(
                http2=False,
                headers=self._order_headers,
                timeout=timeout,
            )
            return self._http_client

    async def _acquire_user_stream(self) -> Optional[MexcUserStream]:
        """Return an active user stream without blocking startup for long."""
        if not self._ws_enabled:
            return None

        stream = self.user_stream
        if stream and stream.is_running:
            return stream

        if not self._ws_fast_nonblocking:
            started = await self.ensure_user_stream()
            return (
                self.user_stream
                if started and self.user_stream and self.user_stream.is_running
                else None
            )

        start_task = self._user_stream_start_task
        if start_task is None or start_task.done():
            try:
                self._user_stream_start_task = asyncio.create_task(self.ensure_user_stream())

                def _suppress_task_exception(task: asyncio.Task) -> None:
                    try:
                        exc = task.exception()
                    except asyncio.CancelledError:
                        return
                    if exc is not None:
                        logger.debug("MEXC user stream start task failed: %s", exc)

                self._user_stream_start_task.add_done_callback(_suppress_task_exception)
            except RuntimeError:
                self._user_stream_start_task = None

        deadline = time.perf_counter() + self._ws_fast_wait
        while time.perf_counter() < deadline:
            stream = self.user_stream
            if stream and stream.is_running:
                return stream
            await asyncio.sleep(0.005)

        return None

    async def prewarm(self) -> None:
        """Warm REST and WebSocket paths so the first trade avoids cold-start costs."""
        if self.test_mode:
            return

        async def _call_session(url: str) -> None:
            try:
                await asyncio.to_thread(
                    self.session.get,
                    url,
                    timeout=self._rest_timeout,
                )
            except (requests.RequestException, OSError, ValueError) as exc:
                logger.debug("MEXC prewarm session call failed (%s): %s", url, exc)

        await _call_session(f"{self.base_url}/api/v3/ping")
        await _call_session(f"{self.base_url}/api/v3/time")

        http_client: Optional[httpx.Client] = None
        try:
            http_client = self._get_http_client()
        except (RuntimeError, ValueError, OSError) as exc:
            logger.debug("MEXC prewarm http client error: %s", exc)

        if http_client:
            try:
                await asyncio.to_thread(
                    http_client.get,
                    f"{self.base_url}/api/v3/ping",
                    timeout=self._rest_timeout[1],
                )
            except (httpx.HTTPError, OSError, ValueError) as exc:
                logger.debug("MEXC httpx prewarm call failed: %s", exc)

        user_stream = await self.ensure_user_stream()
        if user_stream:
            try:
                ready = await user_stream.wait_until_ready(timeout=max(self.ws_ack_timeout, 0.8))
                if not ready:
                    logger.debug("MEXC user stream prewarm ready wait timed out")
            except (asyncio.TimeoutError, RuntimeError) as exc:
                logger.debug("MEXC user stream prewarm wait error: %s", exc)

    async def shutdown(self) -> None:
        start_task = self._user_stream_start_task
        if start_task and not start_task.done():
            start_task.cancel()
            try:
                await start_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug("MEXC user stream start task shutdown error: %s", exc)
        self._user_stream_start_task = None
        if self.user_stream:
            try:
                await self.user_stream.stop()
            except Exception as exc:
                logger.debug("MEXC user stream stop error: %s", exc)
            self.user_stream = None
        if self._http_client:
            try:
                self._http_client.close()
            except (httpx.HTTPError, RuntimeError, OSError) as exc:
                logger.debug("MEXC http client close error: %s", exc)
            self._http_client = None

    def _warm_up(self):
        """MEXC trader ısınma"""
        try:
            if not self.time_cache.warmup_complete:
                time.sleep(0.1)

            _ = self.time_cache.get_time()
            _ = self._get_balance(force=True)

            if not self.test_mode and self.session:
                self.session.get(
                    f"{self.base_url}/api/v3/ping",
                    timeout=self._timeout_tuple(self.tuning.rest_read_timeout),
                )

            return True
        except (requests.RequestException, OSError, RuntimeError, ValueError) as exc:
            logger.debug("MEXC warm-up failed: %s", exc)
            return False

    def _check_api_credentials(self):
        """API anahtarlarını kontrol et"""
        if not self.api_key or len(self.api_key) < 10:
            logger.warning("MEXC API KEY missing or invalid!")

        if not self.api_secret or len(self.api_secret) < 10:
            logger.warning("MEXC API SECRET missing or invalid!")

    def _load_symbol_cache_from_disk(self) -> None:
        data = _load_json_file(self._symbol_cache_path)
        if not isinstance(data, dict):
            return
        loaded = 0
        now = time.time()
        for symbol, info in data.items():
            if isinstance(symbol, str) and isinstance(info, dict):
                self._symbol_info_cache[symbol.upper()] = info
                self._symbol_info_cache_time[symbol.upper()] = now
                loaded += 1
        if loaded:
            logger.debug(
                "MEXC symbol cache primed with %d entries from %s",
                loaded,
                self._symbol_cache_path,
            )

    def _persist_symbol_cache_if_needed(self) -> None:
        now = time.time()
        if now - self._last_symbol_cache_persist < 2.0:
            return
        payload = {
            symbol: info
            for symbol, info in self._symbol_info_cache.items()
            if isinstance(info, dict)
        }
        if not payload:
            return
        _atomic_write_json(self._symbol_cache_path, payload)
        self._last_symbol_cache_persist = now

    def _schedule_symbol_prefetch(self, symbol: str) -> None:
        symbol = symbol.upper()
        if symbol in self._symbol_info_cache:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        existing = self._symbol_prefetch_tasks.get(symbol)
        if existing and not existing.done():
            return

        task = loop.create_task(self._prefetch_symbol_info(symbol))
        self._symbol_prefetch_tasks[symbol] = task

    async def _prefetch_symbol_info(self, symbol: str) -> None:
        deadline = time.perf_counter() + self._symbol_prefetch_window
        attempt_timeouts = REALISTIC_LOOKUP_ATTEMPTS
        symbol = symbol.upper()

        while time.perf_counter() < deadline:
            if symbol in self._symbol_info_cache:
                return

            request_symbol = SymbolConverter.convert(symbol, "mexc")
            url = f"{self.base_url}/api/v3/exchangeInfo?symbol={request_symbol}"

            result, _, _ = await _fast_http_lookup(
                url=url,
                session=self.session,
                params=None,
                headers=None,
                attempt_timeouts=attempt_timeouts,
                extractor=self._extract_mexc_symbol_info,
            )

            if result:
                current_time = time.time()
                self._symbol_info_cache[symbol] = result
                self._symbol_info_cache_time[symbol] = current_time
                return

            await asyncio.sleep(self._symbol_prefetch_interval)

    @staticmethod
    def _extract_mexc_symbol_info(response: requests.Response) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            data = response.json()
        except ValueError as exc:
            return None, f"invalid json {exc}"

        symbols = data.get('symbols') or []
        if symbols:
            return symbols[0], None
        return None, "empty symbols"

    def _canonicalize_params(self, params):
        """Normalize params into a sorted (key, str(value)) list."""
        if isinstance(params, dict):
            items = list(params.items())
        else:
            items = list(params)

        normalized = []
        for key, value in items:
            if value is None:
                continue
            if isinstance(value, str):
                str_value = value
            elif isinstance(value, int):
                str_value = str(value)
            elif isinstance(value, float):
                str_value = f"{value:.15f}".rstrip('0').rstrip('.')
                if not str_value:
                    str_value = "0"
            else:
                str_value = str(value)
            normalized.append((key, str_value))

        normalized.sort(key=lambda item: item[0])
        return normalized

    def _encode_request_body(self, body: Any) -> str:
        if isinstance(body, str):
            return body
        if isinstance(body, bytes):
            return body.decode('utf-8')
        return json.dumps(body, separators=(',', ':'), ensure_ascii=False)

    def _generate_signature(self, params, body: Optional[Any] = None):
        """Return signature and canonical query string for params and optional body."""
        from urllib.parse import urlencode

        canonical_params = self._canonicalize_params(params)
        query_string = urlencode(canonical_params, doseq=False, safe="")
        if body is not None:
            body_payload = self._encode_request_body(body)
            signing_payload = f"{query_string}{body_payload}"
        else:
            body_payload = ""
            signing_payload = query_string

        signature = hmac.new(
            self._pre_encoded_secret,
            signing_payload.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()
        return signature, query_string, body_payload

    async def _signed_post(
        self,
        url,
        params,
        timeout: Optional[float] = None,
        json_body: Optional[Any] = None,
    ):
        """Execute a signed POST request asynchronously."""
        signature, query_string, body_payload = self._generate_signature(params, json_body)
        if query_string:
            full_url = f"{url}?{query_string}&signature={signature}"
        else:
            full_url = f"{url}?signature={signature}"

        timeout_tuple = self._timeout_tuple(timeout)
        if json_body is not None:
            if not body_payload:
                body_payload = self._encode_request_body(json_body)
            headers = dict(self._order_headers)
            headers.setdefault("Content-Type", "application/json")
            fn = functools.partial(
                self.session.post,
                full_url,
                headers=headers,
                data=body_payload,
                timeout=timeout_tuple,
            )
        else:
            fn = functools.partial(
                self.session.post,
                full_url,
                headers=self._order_headers,
                timeout=timeout_tuple,
            )
        return await asyncio.to_thread(fn)

    async def _signed_get(self, url, params, timeout: Optional[float] = None):
        """Execute a signed GET request asynchronously."""
        signature, query_string, _ = self._generate_signature(params)
        if query_string:
            full_url = f"{url}?{query_string}&signature={signature}"
        else:
            full_url = f"{url}?signature={signature}"
        timeout_tuple = self._timeout_tuple(timeout)
        fn = functools.partial(
            self.session.get,
            full_url,
            headers=self.headers,
            timeout=timeout_tuple,
        )
        return await asyncio.to_thread(fn)

    def _get_balance(self, force: bool = False) -> float:
        """USDT bakiyesini al"""
        current_time = time.time()

        if self.test_mode:
            return 1000.0

        if not force and self._balance_cache is not None and (current_time - self._balance_updated < 60):
            return self._balance_cache

        try:
            timestamp = self.time_cache.get_time()
            params = [
                ('timestamp', str(timestamp)),
                ('recvWindow', '60000'),
            ]
            signature, query_string, _ = self._generate_signature(params)
            full_url = f"{self.base_url}/api/v3/account?{query_string}&signature={signature}"
            response = self.session.get(
                full_url,
                headers=self.headers,
                timeout=self._timeout_tuple(self.tuning.rest_read_timeout),
            )
            if response and response.status_code == 200:
                data = response.json()
                balances = data.get('balances') if isinstance(data, dict) else []
                if not isinstance(balances, list):
                    logger.error("MEXC balance fetch error: invalid balances payload type %s", type(balances).__name__)
                    balances = []
                for item in balances:
                    if not isinstance(item, dict):
                        continue
                    if item.get('asset') == 'USDT':
                        free = _coerce_float(item.get('free')) or 0.0
                        self._balance_cache = free
                        self._balance_updated = current_time
                        return free
        except (requests.RequestException, OSError, TypeError, ValueError) as exc:
            logger.error(f"MEXC balance fetch error: {exc}")

        if self._balance_cache is not None:
            return self._balance_cache
        return 0.0

    def get_balance(self, force: bool = False) -> float:
        return self._get_balance(force=force)

    async def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol = symbol.upper()
        cached = self._symbol_info_cache.get(symbol)
        if cached and (time.time() - self._symbol_info_cache_time.get(symbol, 0)) < self._symbol_info_cache_ttl:
            return cached

        request_symbol = SymbolConverter.convert(symbol, "mexc")
        url = f"{self.base_url}/api/v3/exchangeInfo"
        try:
            response = await asyncio.to_thread(
                self.session.get,
                url,
                params={'symbol': request_symbol},
                timeout=self._timeout_tuple(self.tuning.rest_read_timeout),
            )
            if response and response.status_code == 200:
                payload = response.json()
                symbols = payload.get('symbols') if isinstance(payload, dict) else []
                if not isinstance(symbols, list):
                    logger.error("MEXC get_symbol_info error: invalid symbols payload type %s", type(symbols).__name__)
                    return None
                if symbols:
                    info = symbols[0]
                    self._symbol_info_cache[symbol] = info
                    self._symbol_info_cache_time[symbol] = time.time()
                    self._persist_symbol_cache_if_needed()
                    return info
        except (requests.RequestException, OSError, TypeError, ValueError) as exc:
            logger.error(f"MEXC get_symbol_info error: {exc}")
        return None

    def parse_mexc_precision(self, symbol_info: Dict[str, Any]) -> int:
        bsp = symbol_info.get('baseSizePrecision')
        if isinstance(bsp, str) and '.' in bsp:
            fractional = bsp.split('.')[1].rstrip('0')
            return len(fractional) if fractional else 0

        qp = symbol_info.get('quantityPrecision')
        parsed_qp = _coerce_int(qp)
        if parsed_qp is not None:
            return parsed_qp

        for key in ('baseAssetPrecision', 'basePrecision'):
            parsed_value = _coerce_int(symbol_info.get(key))
            if parsed_value is not None:
                return parsed_value

        return 3

    def _extract_quote_constraints(self, symbol_info: Dict[str, Any]) -> Tuple[int, Optional[float]]:
        raw_precision = (
            symbol_info.get('quoteAssetPrecision')
            if symbol_info.get('quoteAssetPrecision') is not None
            else symbol_info.get('quotePrecision')
        )
        parsed_quote_precision = _coerce_int(raw_precision)
        quote_precision = max(parsed_quote_precision, 2) if parsed_quote_precision is not None else 2

        min_notional: Optional[float] = None
        for filt in symbol_info.get('filters') or []:
            ftype = str(filt.get('filterType') or "").upper()
            if ftype in {'MIN_NOTIONAL', 'MIN_NOTIONAL_FILTER'}:
                candidate = filt.get('minNotional') or filt.get('min_notional')
                parsed_candidate = _coerce_float(candidate)
                if parsed_candidate is not None:
                    min_notional = parsed_candidate
                    break

        if min_notional is None:
            for key in ('quoteAmountPrecisionMarket', 'quoteAmountPrecision'):
                candidate = symbol_info.get(key)
                parsed_candidate = _coerce_float(candidate)
                if parsed_candidate is not None:
                    min_notional = parsed_candidate
                    break

        return quote_precision, min_notional

    def format_quantity_for_mexc(self, quantity: float, precision: int) -> Optional[str]:
        try:
            decimal_qty = Decimal(str(quantity))
            step = Decimal('1').scaleb(-precision)
            formatted = decimal_qty.quantize(step, rounding=ROUND_DOWN)
            if formatted <= 0:
                return None
            return format(formatted, 'f').rstrip('0').rstrip('.') or "0"
        except (ArithmeticError, TypeError, ValueError) as exc:
            logger.error(f"MEXC quantity format error: {exc}")
            return None

    def _timeout_tuple(self, read_override: Optional[float] = None) -> Tuple[float, float]:
        connect_timeout, read_timeout = self._rest_timeout
        if read_override is None:
            return connect_timeout, read_timeout
        return connect_timeout, max(0.05, float(read_override))

    async def _debug_fetch_order(
        self,
        symbol_with_quote: str,
        order_id: Optional[str],
        client_order_id: Optional[str],
    ):
        """Debug amaçlı olarak verilen siparişi sorgula."""
        if not order_id and not client_order_id:
            return None

        params = [
            ('symbol', symbol_with_quote),
            ('timestamp', str(self.time_cache.get_time())),
            ('recvWindow', self._compute_recv_window()),
        ]

        if order_id:
            params.append(('orderId', str(order_id)))
        elif client_order_id:
            params.append(('origClientOrderId', client_order_id))

        try:
            await self._throttle_order_query()
            response = await self._signed_get(
                f"{self.base_url}/api/v3/order",
                params,
                timeout=self.tuning.rest_read_timeout,
            )
        except Exception as exc:
            logger.error(f"MEXC order lookup error: {exc}")
            return None

        if response is None:
            return None

        try:
            payload = response.json()
        except ValueError:
            payload = {"raw": response.text}

        return response.status_code, payload

    async def _fetch_order_trades(self, symbol_with_quote: str, order_id: Optional[str]):
        """Siparişe ait trade kayıtlarını getir."""
        if not order_id:
            return None

        params = [
            ('symbol', symbol_with_quote),
            ('timestamp', str(self.time_cache.get_time())),
            ('recvWindow', self._compute_recv_window()),
            ('orderId', str(order_id)),
        ]
        params.append(('limit', str(max(1, int(self.tuning.mytrades_limit)))))
        try:
            window_start = int(self.time_cache.get_time()) - max(1000, int(self.tuning.mytrades_window_ms))
            params.append(('startTime', str(max(0, window_start))))
        except Exception:
            pass

        try:
            response = await self._signed_get(
                f"{self.base_url}/api/v3/myTrades",
                params,
                timeout=self.tuning.rest_read_timeout,
            )
        except Exception as exc:
            logger.error(f"MEXC trade lookup error: {exc}")
            return None

        if response is None:
            return None

        try:
            payload = response.json()
        except ValueError:
            payload = {"raw": response.text}

        return response.status_code, payload

    async def _sum_trades_fill(self, symbol_with_quote: str, order_id: Optional[str]) -> Tuple[float, float]:
        """Belirli bir sipariş için gerçekleşen toplam miktarları myTrades üzerinden topla."""
        if not order_id:
            return 0.0, 0.0

        trades_result = await self._fetch_order_trades(symbol_with_quote, order_id)
        if not trades_result:
            return 0.0, 0.0

        status_code, payload = trades_result
        if status_code != 200 or not isinstance(payload, list):
            return 0.0, 0.0

        filled_qty = 0.0
        filled_quote = 0.0
        for trade in payload:
            try:
                filled_qty += float(trade.get('qty') or trade.get('quantity') or 0.0)
            except (TypeError, ValueError):
                pass
            try:
                filled_quote += float(trade.get('quoteQty') or trade.get('quote_quantity') or 0.0)
            except (TypeError, ValueError):
                pass

        return filled_qty, filled_quote

    async def wait_for_private_deal_or_order_close(
        self,
        client_order_id: str,
        ws_future: Optional[asyncio.Future],
        timeout: Optional[float] = None,
        *,
        user_stream: Optional['MexcUserStream'] = None,
    ) -> Optional[Dict[str, Any]]:
        """Wait until private WebSocket confirms the order fill or closure."""
        if not client_order_id or not ws_future:
            return None
        stream = user_stream or await self.ensure_user_stream()
        if not stream:
            return None
        effective_timeout = timeout if timeout is not None else self.ws_fill_timeout
        try:
            return await stream.wait_for(client_order_id, ws_future, timeout=effective_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("MEXC wait_for_private_deal_or_order_close error: %s", exc)
            return None

    async def _execute_trade_fast(
        self,
        symbol: str,
        symbol_with_quote: str,
        spend_amount: float,
        quote_precision: int,
        trade_start: float,
    ) -> Optional[Dict[str, Any]]:
        formatted_amount = format_quote_amount(spend_amount, quote_precision)
        client_order_id = self._build_client_order_id()
        timestamp = str(self.time_cache.get_time())

        logger.info(
            "⚡ MEXC FAST MARKET BUY %s %s USDT (clientOrderId=%s)",
            symbol,
            formatted_amount,
            client_order_id,
        )

        user_stream = await self._acquire_user_stream()
        ws_future: Optional[asyncio.Future] = None
        if user_stream:
            try:
                ws_future = await user_stream.register_order(client_order_id)
            except Exception as exc:
                logger.debug("MEXC user stream register_order error: %s", exc)
                ws_future = None

        try:
            http_client = self._get_http_client()
            order_params = [
                ('symbol', symbol_with_quote),
                ('side', 'BUY'),
                ('type', 'MARKET'),
                ('quoteOrderQty', formatted_amount),
                ('timestamp', timestamp),
                ('recvWindow', self._compute_recv_window()),
                ('newClientOrderId', client_order_id),
            ]
            signature, query_string, body_payload = self._generate_signature(order_params, None)
            request_url = f"{self.base_url}/api/v3/order"
            if query_string:
                request_url = f"{request_url}?{query_string}&signature={signature}"
            else:
                request_url = f"{request_url}?signature={signature}"
            headers = dict(self._order_headers)
            body_payload = None
        except Exception as exc:
            if user_stream and ws_future:
                user_stream.unregister_order(client_order_id)
            logger.error("MEXC fast order preparation failed: %s", exc)
            return None

        async def _send_fast_order():
            try:
                return await asyncio.to_thread(
                    http_client.post,
                    request_url,
                    headers=headers,
                    content=body_payload,
                )
            except Exception as exc:
                raise RuntimeError(f"MEXC HTTP error: {exc}") from exc

        try:
            response = await RETRY_ENGINE.execute_with_retry(
                _send_fast_order,
                'mexc',
                weight=1,
                is_order=True,
                max_attempts=1,
            )
        except Exception as exc:
            if user_stream and ws_future:
                user_stream.unregister_order(client_order_id)
            logger.warning("MEXC fast order request failed: %s", exc)
            return None

        if not response or response.status_code not in (200, 201):
            if user_stream and ws_future:
                user_stream.unregister_order(client_order_id)
            error_text = response.text if response else "no_response"
            self._log_mexc_rejection("fast order", response, error_text)
            return None

        try:
            rest_payload = response.json()
        except ValueError:
            rest_payload = {"raw": response.text}

        rest_data = rest_payload.get('data') if isinstance(rest_payload, dict) and isinstance(rest_payload.get('data'), dict) else rest_payload
        order_id = rest_data.get('orderId') or rest_data.get('order_id') if isinstance(rest_data, dict) else None
        response_client_order_id = (
            rest_data.get('clientOrderId')
            or rest_data.get('origClientOrderId')
            or rest_data.get('clientId')
            if isinstance(rest_data, dict)
            else None
        )
        if not response_client_order_id:
            response_client_order_id = client_order_id

        ws_ack_payload: Optional[Dict[str, Any]] = None
        lookup_tuple: Optional[Tuple[int, Any]] = None
        tasks: Dict[str, asyncio.Task[Any]] = {}

        tasks["rest_lookup"] = asyncio.create_task(
            self._debug_fetch_order(
                symbol_with_quote,
                order_id,
                response_client_order_id,
            )
        )

        if ws_future:
            tasks["ws_ack"] = asyncio.create_task(
                self.wait_for_private_deal_or_order_close(
                    client_order_id=client_order_id,
                    ws_future=ws_future,
                    timeout=self.ws_fill_timeout,
                    user_stream=user_stream,
                )
            )

        done: Set[asyncio.Task[Any]]
        pending: Set[asyncio.Task[Any]]
        done, pending = await asyncio.wait(
            set(tasks.values()),
            timeout=self.ws_fill_timeout,
            return_when=asyncio.ALL_COMPLETED,
        )

        for finished in done:
            label = next((name for name, task in tasks.items() if task is finished), None)
            try:
                result_value = finished.result()
            except Exception as exc:  # pragma: no cover - defensive logging
                result_value = None
                if label == "rest_lookup":
                    logger.debug("MEXC rest lookup error: %s", exc)
                elif label == "ws_ack":
                    logger.debug("MEXC ws ack wait error: %s", exc)

            if label == "rest_lookup":
                lookup_tuple = result_value
            elif label == "ws_ack":
                ws_ack_payload = result_value

        for pending_task in pending:
            label = next((name for name, task in tasks.items() if task is pending_task), None)
            pending_task.cancel()
            if label == "rest_lookup":
                lookup_tuple = None

        if user_stream and ws_future:
            user_stream.unregister_order(client_order_id)

        rest_qty, rest_quote = self._extract_fill_amounts(rest_data if isinstance(rest_data, dict) else {})
        filled_qty = rest_qty
        filled_quote = rest_quote
        confirmation_source = "rest" if rest_quote > 0 else None

        ack_payload = ws_ack_payload.get("payload") if isinstance(ws_ack_payload, dict) else None
        ack_qty, ack_quote = self._extract_fill_amounts(ack_payload) if ack_payload else (0.0, 0.0)

        if isinstance(ws_ack_payload, dict):
            try:
                ack_qty = max(ack_qty, float(ws_ack_payload.get("filled_qty") or 0.0))
            except (TypeError, ValueError):
                pass
            try:
                ack_quote = max(ack_quote, float(ws_ack_payload.get("filled_quote") or 0.0))
            except (TypeError, ValueError):
                pass

        if ack_quote > 0 and ack_quote >= filled_quote:
            filled_quote = ack_quote
            filled_qty = max(filled_qty, ack_qty)
        elif ack_qty > 0 and filled_qty <= 0:
            filled_qty = ack_qty

        ack_channel = (ws_ack_payload.get("channel") or "").lower() if isinstance(ws_ack_payload, dict) else ""

        if ws_ack_payload and not order_id:
            ack_order_id = ws_ack_payload.get("order_id")
            if ack_order_id:
                order_id = ack_order_id
        if isinstance(ack_payload, dict) and not order_id:
            ack_order_id = ack_payload.get("orderId") or ack_payload.get("order_id")
            if ack_order_id:
                order_id = ack_order_id

        lookup_data: Optional[Dict[str, Any]] = None
        if lookup_tuple and isinstance(lookup_tuple, tuple) and lookup_tuple[0] == 200:
            raw_payload = lookup_tuple[1]
            if isinstance(raw_payload, dict):
                candidate = raw_payload.get('data')
                lookup_data = candidate if isinstance(candidate, dict) else raw_payload

        if not order_id and not lookup_data:
            fallback_lookup = await self._debug_fetch_order(
                symbol_with_quote,
                None,
                response_client_order_id,
            )
            if fallback_lookup and isinstance(fallback_lookup, tuple) and fallback_lookup[0] == 200:
                payload = fallback_lookup[1]
                if isinstance(payload, dict):
                    candidate = payload.get('data')
                    lookup_data = candidate if isinstance(candidate, dict) else payload

        if lookup_data:
            maybe_order_id = lookup_data.get('orderId') or lookup_data.get('order_id')
            if maybe_order_id and not order_id:
                order_id = str(maybe_order_id)
            lookup_qty, lookup_quote = self._extract_fill_amounts(lookup_data)
            if lookup_quote > filled_quote:
                filled_quote = lookup_quote
                filled_qty = lookup_qty
                confirmation_source = "rest_lookup"

        trades_checked = False
        if filled_quote <= 0 and self._fast_trades_verify and order_id:
            trades_checked = True
            trades_qty, trades_quote = await self._sum_trades_fill(symbol_with_quote, order_id)
            if trades_quote > 0:
                filled_quote = trades_quote
                filled_qty = trades_qty
                confirmation_source = "trades"

        ack_status = ""
        if isinstance(ws_ack_payload, dict):
            ack_status = str(
                ws_ack_payload.get("status")
                or ws_ack_payload.get("state")
                or ""
            ).upper()
        if not ack_status and isinstance(ack_payload, dict):
            ack_status = str(
                ack_payload.get("status")
                or ack_payload.get("state")
                or ack_payload.get("orderStatus")
                or ""
            ).upper()

        if isinstance(rest_data, dict):
            rest_status = str(rest_data.get("status") or rest_data.get("state") or "").upper()
        else:
            rest_status = ""

        terminal_statuses = {
            "FILLED",
            "PARTIALLY_FILLED",
            "PARTIAL_FILLED",
            "PARTIAL_FILL",
            "CANCELED",
            "CANCELLED",
            "REJECTED",
            "EXPIRED",
        }

        if ws_ack_payload and (
            ack_quote > 0
            or ack_qty > 0
            or "spot@private" in ack_channel
            or ack_status in terminal_statuses
        ):
            confirmation_source = "private_ws"

        success = filled_quote > 0
        if not success:
            if self._order_was_filled(rest_payload):
                success = True
                filled_quote = filled_quote or spend_amount
            elif ack_payload and self._order_was_filled(ack_payload):
                success = True
                filled_quote = filled_quote or spend_amount

        trade_time = time.perf_counter() - trade_start

        if success:
            self._record_trade_success(trade_time)
        else:
            self.performance['trade_count'] += 1
            self.performance['last_trade_time'] = trade_time
            self.performance['min_trade_time'] = min(self.performance['min_trade_time'], trade_time)
            self.performance['max_trade_time'] = max(self.performance['max_trade_time'], trade_time)
            self.performance['total_trade_time'] += trade_time
            self.performance['avg_trade_time'] = (
                self.performance['total_trade_time'] / self.performance['trade_count']
                if self.performance['trade_count'] > 0 else 0.0
            )

        result: Dict[str, Any] = {
            "success": success,
            "exchange": "mexc",
            "symbol": symbol,
            "amount": filled_quote if success else spend_amount,
            "requested_amount": spend_amount,
            "filled_quote": filled_quote,
            "filled_qty": filled_qty,
            "order_result": rest_payload,
            "client_order_id": response_client_order_id,
            "order_id": order_id,
            "ws_ack": ws_ack_payload is not None,
            "ws_payload": ws_ack_payload,
            "confirmation_source": confirmation_source,
            "trades_checked": trades_checked,
            "rest_status": rest_status,
            "ws_status": ack_status,
            "execution_time": trade_time,
            "fast_mode": True,
        }

        if not success:
            if ack_status:
                result["error"] = f"ack_status={ack_status}"
            elif rest_status:
                result["error"] = f"rest_status={rest_status}"
            else:
                result["error"] = "fill_not_confirmed"
            logger.warning(
                "❌ MEXC fast order incomplete | orderId=%s | clientId=%s | rest=%s | ws=%s | filled=%.4f/%.4f | confirmation=%s",
                order_id or "N/A",
                response_client_order_id or client_order_id,
                rest_status or "N/A",
                ack_status or "N/A",
                filled_quote,
                spend_amount,
                confirmation_source or "none",
            )

        return result

    async def _poll_fill_until(
        self,
        symbol_with_quote: str,
        order_id: Optional[str],
        client_order_id: Optional[str],
        target_quote: float,
        min_ratio: Optional[float] = None,
        max_wait_ms: int = 700,
        interval_ms: int = 60,
    ) -> Tuple[bool, float, float, Optional[str]]:
        """Sipariş dolumunu kısa süreli poll ederek takip et."""
        min_ratio = min_ratio if min_ratio is not None else self.min_fill_ratio
        resolved_order_id = order_id
        best_qty = 0.0
        best_quote = 0.0

        if target_quote <= 0:
            return False, best_qty, best_quote, resolved_order_id

        deadline = time.perf_counter() + (max_wait_ms / 1000.0)
        poll_interval = max(interval_ms, 20) / 1000.0

        while time.perf_counter() < deadline:
            if resolved_order_id:
                qty, quote = await self._sum_trades_fill(symbol_with_quote, resolved_order_id)
                best_qty = max(best_qty, qty)
                best_quote = max(best_quote, quote)

                if best_quote >= target_quote * min_ratio:
                    return True, best_qty, best_quote, resolved_order_id

            lookup_result = await self._debug_fetch_order(symbol_with_quote, resolved_order_id, client_order_id)
            if lookup_result:
                status_code, payload = lookup_result
                if status_code == 200 and isinstance(payload, dict):
                    raw_data = payload.get('data') if isinstance(payload.get('data'), dict) else payload
                    maybe_order_id = raw_data.get('orderId') or raw_data.get('order_id')
                    if maybe_order_id and not resolved_order_id:
                        resolved_order_id = str(maybe_order_id)

                    try:
                        cumulative_quote = float(
                            raw_data.get('cummulativeQuoteQty')
                            or raw_data.get('cumQuote')
                            or raw_data.get('executedQuoteQty')
                            or 0.0
                        )
                        best_quote = max(best_quote, cumulative_quote)
                    except (TypeError, ValueError):
                        pass

                    try:
                        executed_qty = float(
                            raw_data.get('executedQty')
                            or raw_data.get('executed_qty')
                            or raw_data.get('dealQuantity')
                            or 0.0
                        )
                        best_qty = max(best_qty, executed_qty)
                    except (TypeError, ValueError):
                        pass

                    if best_quote >= target_quote * min_ratio:
                        return True, best_qty, best_quote, resolved_order_id

            await asyncio.sleep(poll_interval + random.uniform(0.0, 0.02))

        return False, best_qty, best_quote, resolved_order_id

    async def _await_delayed_fill(
        self,
        symbol_with_quote: str,
        order_id: Optional[str],
        client_order_id: Optional[str],
        target_quote: float,
        *,
        min_ratio: Optional[float] = None,
    ) -> Tuple[bool, float, float, Optional[str], Optional[str]]:
        """Extended follow-up check for fills that may arrive after the initial confirmation window."""
        try:
            target_value = float(target_quote or 0.0)
        except (TypeError, ValueError):
            target_value = 0.0
        if target_value <= 0:
            return False, 0.0, 0.0, order_id, None

        wait_ms = max(0, int(self.delayed_fill_wait_ms))
        if wait_ms <= 0:
            return False, 0.0, 0.0, order_id, None

        ratio = self.delayed_fill_min_ratio if min_ratio is None else min_ratio
        try:
            ratio = float(ratio)
        except (TypeError, ValueError):
            ratio = self.delayed_fill_min_ratio
        if ratio < 0.0:
            ratio = 0.0
        if ratio > 1.0:
            ratio = 1.0

        ws_qty = 0.0
        ws_quote = 0.0
        confirmation_source: Optional[str] = None
        stream: Optional[MexcUserStream] = None
        ws_future: Optional[asyncio.Future] = None

        if client_order_id:
            stream = await self._acquire_user_stream()
            if stream:
                try:
                    ws_future = await stream.register_order(client_order_id)
                except Exception as exc:
                    logger.debug("MEXC delayed fill WS register error: %s", exc)
                    ws_future = None

        if ws_future and stream:
            try:
                ws_payload = await self.wait_for_private_deal_or_order_close(
                    client_order_id,
                    ws_future,
                    timeout=wait_ms / 1000.0,
                    user_stream=stream,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("MEXC delayed WS wait error: %s", exc)
                ws_payload = None
            finally:
                try:
                    stream.unregister_order(client_order_id)
                except Exception:
                    pass

            if isinstance(ws_payload, dict):
                ws_qty, ws_quote = self._extract_fill_amounts(ws_payload)
                if ws_quote > 0 or ws_qty > 0:
                    confirmation_source = "private_ws"
                ack_order_id = (
                    ws_payload.get("orderId")
                    or ws_payload.get("order_id")
                    or ws_payload.get("data", {}).get("orderId")
                )
                if ack_order_id and not order_id:
                    order_id = str(ack_order_id)

        try:
            success, qty, quote, resolved_order_id = await self._poll_fill_until(
                symbol_with_quote,
                order_id,
                client_order_id,
                target_quote=target_value,
                min_ratio=ratio,
                max_wait_ms=wait_ms,
                interval_ms=max(self.fill_confirm_interval_ms, 60),
            )
        except Exception as exc:
            logger.debug("MEXC delayed fill check error: %s", exc)
            resolved_order_id = order_id
            success = False
            qty = 0.0
            quote = 0.0

        final_qty = max(qty, ws_qty)
        final_quote = max(quote, ws_quote)
        final_order_id = resolved_order_id or order_id

        if final_quote > 0 and confirmation_source is None:
            confirmation_source = "rest_poll" if success else "mixed_poll"

        return final_quote > 0, final_qty, final_quote, final_order_id, confirmation_source

    @measure_time
    async def execute_trade(self, symbol, amount=None, preparation_data=None):
        """MEXC Market Order ile işlem"""
        trade_start = time.perf_counter()

        if amount is None:
            amount = 10.0

        try:
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                logger.warning(f"MEXC {symbol} not listed; skipping trade")
                return {
                    "success": False,
                    "skipped": True,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "reason": "symbol_not_listed",
                    "execution_time": time.perf_counter() - trade_start,
                }

            precision = self.parse_mexc_precision(symbol_info)
            quote_precision, min_notional = self._extract_quote_constraints(symbol_info)
            logger.info(f"🔵 MEXC {symbol} precision: base={precision}, quote={quote_precision}, min_notional={min_notional}")

            symbol_with_quote = SymbolConverter.convert(symbol, "mexc")
            order_url = f"{self.base_url}/api/v3/order"

            balance = self._get_balance()
            if balance is None:
                balance = 0.0

            if amount > balance and balance > 0:
                amount = min(balance * 0.99, 200.0)  # Cap at $200

            effective_min_notional = max(min_notional or 0.0, self.min_order_notional)

            if balance < effective_min_notional:
                logger.warning(
                    f"MEXC insufficient balance {balance:.2f} < min notional {effective_min_notional:.2f}; skipping"
                )
                return {
                    "success": False,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "error": "insufficient_balance_for_min_notional",
                    "execution_time": time.perf_counter() - trade_start,
                }

            spend_amount = float(amount)
            if spend_amount <= 0:
                spend_amount = effective_min_notional

            if spend_amount > balance:
                spend_amount = balance

            if spend_amount < effective_min_notional:
                spend_amount = min(balance, effective_min_notional)

            if spend_amount <= 0:
                return {
                    "success": False,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "error": "insufficient_balance",
                    "execution_time": time.perf_counter() - trade_start,
                }

            if self.test_mode:
                await asyncio.sleep(0.001)
                return {"success": True, "exchange": "mexc", "symbol": symbol}

            if self.fast_mode:
                fast_result = await self._execute_trade_fast(
                    symbol=symbol,
                    symbol_with_quote=symbol_with_quote,
                    spend_amount=spend_amount,
                    quote_precision=quote_precision,
                    trade_start=trade_start,
                )
                if fast_result is not None:
                    return fast_result
                logger.warning("MEXC fast mode fallback to legacy path for %s", symbol)

            original_target_quote = spend_amount
            target_quote = original_target_quote
            partial_filled_quote = 0.0
            partial_filled_qty = 0.0
            server_time = self.time_cache.get_time()
            quote_str = format_quote_amount(target_quote, quote_precision)
            try:
                quote_value = float(quote_str) if quote_str else 0.0
            except ValueError:
                quote_value = 0.0
            if quote_value < effective_min_notional:
                target_quote = max(target_quote, effective_min_notional)
                quote_str = format_quote_amount(target_quote, quote_precision)

            client_order_id = self._build_client_order_id()
            quote_params = [
                ('symbol', symbol_with_quote),
                ('side', 'BUY'),
                ('type', 'MARKET'),
                ('quoteOrderQty', quote_str),
                ('timestamp', str(server_time)),
                ('recvWindow', self._compute_recv_window()),
                ('newClientOrderId', client_order_id),
            ]

            logger.info(
                f"🔵 MEXC MARKET BUY (quoteOrderQty): {symbol} for {quote_str} USDT (requested {target_quote:.2f})"
            )

            quote_failed = False
            quote_error = None
            order_result_payload = {"initial": None, "resubmits": []}

            async def _place_mexc_quote():
                return await self._signed_post(order_url, quote_params, timeout=1.2)

            try:
                response = await RETRY_ENGINE.execute_with_retry(
                    _place_mexc_quote,
                    'mexc',
                    weight=1,
                    is_order=True,
                )
            except Exception as exc:
                logger.warning(f"MEXC quoteOrderQty request failed: {exc}")
                response = None

            if response and response.status_code in (200, 201):
                try:
                    initial_result = response.json()
                except ValueError:
                    initial_result = {"raw": response.text}
                order_result_payload["initial"] = initial_result

                order_id = (
                    initial_result.get('orderId')
                    or (initial_result.get('data') or {}).get('orderId')
                )
                order_id = str(order_id).strip() if order_id else None
                response_client_order_id = (
                    initial_result.get('clientOrderId')
                    or initial_result.get('origClientOrderId')
                    or (initial_result.get('data') or {}).get('clientOrderId')
                    or client_order_id
                )

                if not order_id:
                    quote_failed = True
                    quote_error = "missing_order_id"
                    logger.warning("MEXC quoteOrderQty response missing orderId; will attempt quantity fallback")
                else:
                    success, filled_qty, filled_quote, resolved_order_id = await self._poll_fill_until(
                        symbol_with_quote,
                        order_id,
                        response_client_order_id,
                        target_quote=target_quote,
                        min_ratio=self.min_fill_ratio,
                        max_wait_ms=self.fill_confirm_wait_ms,
                        interval_ms=self.fill_confirm_interval_ms,
                    )
                    if resolved_order_id:
                        order_id = resolved_order_id

                    total_filled_qty = filled_qty
                    total_filled_quote = filled_quote
                    partial_filled_qty = total_filled_qty
                    partial_filled_quote = total_filled_quote

                    resubmits = 0
                    while (
                        total_filled_quote < target_quote * self.min_fill_ratio
                        and resubmits < self.max_resubmits
                    ):
                        remaining_quote = max(0.0, target_quote - total_filled_quote)
                        if remaining_quote < effective_min_notional:
                            break

                        remaining_str = format_quote_amount(remaining_quote, quote_precision)
                        try:
                            remaining_value = float(remaining_str) if remaining_str else 0.0
                        except ValueError:
                            remaining_value = 0.0
                        if remaining_value < effective_min_notional:
                            remaining_quote = max(remaining_quote, effective_min_notional)
                            remaining_str = format_quote_amount(remaining_quote, quote_precision)

                        resubmit_client_order_id = self._build_client_order_id()
                        resubmit_params = [
                            ('symbol', symbol_with_quote),
                            ('side', 'BUY'),
                            ('type', 'MARKET'),
                            ('quoteOrderQty', remaining_str),
                            ('timestamp', str(self.time_cache.get_time())),
                            ('recvWindow', self._compute_recv_window()),
                            ('newClientOrderId', resubmit_client_order_id),
                        ]

                        try:
                            resubmit_response = await self._signed_post(order_url, resubmit_params, timeout=1.2)
                        except Exception as exc:
                            logger.error(f"MEXC quoteOrderQty retry failed: {exc}")
                            break

                        if not resubmit_response or resubmit_response.status_code not in (200, 201):
                            error_text = resubmit_response.text if resubmit_response else "no_response"
                            self._log_mexc_rejection("quoteOrderQty retry", resubmit_response, error_text)
                            break

                        try:
                            resubmit_result = resubmit_response.json()
                        except ValueError:
                            resubmit_result = {"raw": resubmit_response.text}
                        order_result_payload["resubmits"].append(resubmit_result)

                        resubmit_order_id = (
                            resubmit_result.get('orderId')
                            or (resubmit_result.get('data') or {}).get('orderId')
                        )
                        resubmit_order_id = str(resubmit_order_id).strip() if resubmit_order_id else None
                        resubmit_client_order_id = (
                            resubmit_result.get('clientOrderId')
                            or resubmit_result.get('origClientOrderId')
                            or (resubmit_result.get('data') or {}).get('clientOrderId')
                            or resubmit_client_order_id
                        )

                        resubmit_success, resubmit_qty, resubmit_quote, resolved_resubmit_id = await self._poll_fill_until(
                            symbol_with_quote,
                            resubmit_order_id,
                            resubmit_client_order_id,
                            target_quote=remaining_quote,
                            min_ratio=self.min_fill_ratio,
                            max_wait_ms=self.fill_confirm_wait_ms,
                            interval_ms=self.fill_confirm_interval_ms,
                        )

                        total_filled_qty += resubmit_qty
                        total_filled_quote += resubmit_quote
                        if resolved_resubmit_id and not resubmit_order_id:
                            resubmit_order_id = resolved_resubmit_id
                        resubmits += 1

                        if not resubmit_success:
                            logger.warning(
                                f"MEXC quoteOrderQty retry partial fill {resubmit_quote:.4f}/{remaining_quote:.4f} USDT"
                            )

                    if total_filled_quote >= original_target_quote * self.min_fill_ratio:
                        trade_time = time.perf_counter() - trade_start
                        self._record_trade_success(trade_time)
                        return {
                            "success": True,
                            "exchange": "mexc",
                            "symbol": symbol,
                            "amount": total_filled_quote,
                            "filled_quote": total_filled_quote,
                            "filled_qty": total_filled_qty,
                            "order_result": order_result_payload,
                            "execution_time": trade_time,
                            "order_id": order_id,
                            "client_order_id": response_client_order_id,
                            "requested_amount": original_target_quote,
                            "remaining_quote": max(0.0, original_target_quote - total_filled_quote),
                            "partial_fill": total_filled_quote < original_target_quote,
                            "delayed_confirmation": False,
                            "confirmation_source": "rest_poll",
                        }

                    fill_ratio = total_filled_quote / original_target_quote if original_target_quote else 0.0
                    logger.error(
                        f"❌ MEXC quoteOrderQty fill below threshold {total_filled_quote:.4f}/{original_target_quote:.4f} USDT (ratio={fill_ratio:.3f})"
                    )

                    (
                        delayed_success,
                        delayed_qty,
                        delayed_quote,
                        resolved_delayed_id,
                        delayed_source,
                    ) = await self._await_delayed_fill(
                        symbol_with_quote,
                        order_id,
                        response_client_order_id,
                        original_target_quote,
                    )
                    final_quote = max(total_filled_quote, delayed_quote)
                    final_qty = max(total_filled_qty, delayed_qty)
                    if final_quote > 0:
                        order_id = resolved_delayed_id or order_id
                        trade_time = time.perf_counter() - trade_start
                        self._record_trade_success(trade_time)
                        delayed_ratio = (
                            final_quote / original_target_quote if original_target_quote else 0.0
                        )
                        log_msg = (
                            "MEXC delayed fill confirmed: %.4f/%.4f USDT (ratio=%.3f)"
                            if delayed_ratio >= self.min_fill_ratio
                            else "MEXC delayed fill accepted with partial ratio %.3f (filled %.4f/%.4f USDT)"
                        )
                        if delayed_ratio >= self.min_fill_ratio:
                            logger.info(
                                log_msg,
                                delayed_quote,
                                original_target_quote,
                                delayed_ratio,
                            )
                        else:
                            logger.warning(
                                log_msg,
                                delayed_ratio,
                                final_quote,
                                original_target_quote,
                            )
                        return {
                            "success": True,
                            "exchange": "mexc",
                            "symbol": symbol,
                            "amount": final_quote,
                            "filled_quote": final_quote,
                            "filled_qty": final_qty,
                            "order_result": order_result_payload,
                            "execution_time": trade_time,
                            "order_id": order_id,
                            "client_order_id": response_client_order_id,
                            "requested_amount": original_target_quote,
                            "remaining_quote": max(0.0, original_target_quote - final_quote),
                            "partial_fill": delayed_ratio < 1.0,
                            "delayed_confirmation": True,
                            "confirmation_source": delayed_source or "delayed_poll",
                        }

                    remaining_quote = max(0.0, original_target_quote - total_filled_quote)
                    if remaining_quote < effective_min_notional:
                        if total_filled_quote > 0:
                            trade_time = time.perf_counter() - trade_start
                            self._record_trade_success(trade_time)
                            logger.warning(
                                "MEXC partial fill accepted: remaining amount below minimum notional "
                                f"{remaining_quote:.4f} < {effective_min_notional:.4f}"
                            )
                            return {
                                "success": True,
                                "exchange": "mexc",
                                "symbol": symbol,
                                "amount": total_filled_quote,
                                "filled_quote": total_filled_quote,
                                "filled_qty": total_filled_qty,
                            "order_result": order_result_payload,
                            "partial_fill": True,
                            "execution_time": trade_time,
                            "order_id": order_id,
                            "client_order_id": response_client_order_id,
                            "requested_amount": original_target_quote,
                            "remaining_quote": max(0.0, original_target_quote - total_filled_quote),
                            "delayed_confirmation": False,
                            "confirmation_source": "rest_poll",
                        }
                        quote_failed = True
                        quote_error = f"insufficient_fill_below_min_notional:{remaining_quote:.4f}"
                    else:
                        quote_failed = True
                        quote_error = f"insufficient_fill:{total_filled_quote:.2f}/{original_target_quote:.2f}"
                        target_quote = remaining_quote
                        if balance > 0:
                            remaining_balance = max(0.0, balance - total_filled_quote)
                            if remaining_balance > 0:
                                target_quote = min(target_quote, remaining_balance)
                        if target_quote <= 0:
                            trade_time = time.perf_counter() - trade_start
                            self._record_trade_success(trade_time)
                            logger.warning(
                                "MEXC partial fill accepted: no free balance left for fallback attempt"
                            )
                            return {
                                "success": True,
                                "exchange": "mexc",
                                "symbol": symbol,
                                "amount": partial_filled_quote,
                                "filled_quote": partial_filled_quote,
                                "filled_qty": partial_filled_qty,
                                "order_result": order_result_payload,
                                "partial_fill": True,
                                "execution_time": trade_time,
                                "order_id": order_id,
                                "client_order_id": response_client_order_id,
                                "requested_amount": original_target_quote,
                                "remaining_quote": max(0.0, original_target_quote - partial_filled_quote),
                                "delayed_confirmation": False,
                            }
                        quote_str = format_quote_amount(target_quote, quote_precision)
                        try:
                            quote_value = float(quote_str) if quote_str else 0.0
                        except ValueError:
                            quote_value = 0.0
                        if quote_value < effective_min_notional:
                            quote_str = format_quote_amount(effective_min_notional, quote_precision)
                            target_quote = effective_min_notional

            elif response is not None:
                quote_failed = True
                rejection = self._log_mexc_rejection("quoteOrderQty", response, response.text)
                quote_error = rejection.get("message") or response.text
            else:
                quote_failed = True
                quote_error = "no_response"
                logger.warning("MEXC quoteOrderQty call returned no response; attempting fallback")

            if not quote_failed:
                return {
                    "success": False,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "error": "unexpected_quote_flow",
                    "execution_time": time.perf_counter() - trade_start,
                    "order_id": order_id,
                    "client_order_id": response_client_order_id,
                    "requested_amount": original_target_quote,
                    "confirmation_source": "quote_flow",
                }

            self.performance['fallback_stats']['total_fallbacks'] += 1

            if partial_filled_quote > 0:
                logger.info(
                    f"MEXC quoteOrderQty partial fill preserved: {partial_filled_quote:.4f} USDT, "
                    f"retrying for remaining {target_quote:.4f} USDT"
                )

            current_price: Optional[float] = None
            if isinstance(preparation_data, dict):
                try:
                    ref_price = preparation_data.get("reference_price")
                    if ref_price is not None:
                        current_price = float(ref_price)
                except (TypeError, ValueError):
                    current_price = None
            if isinstance(preparation_data, (int, float, str)):
                try:
                    current_price = float(preparation_data)
                except (TypeError, ValueError):
                    current_price = None

            if not current_price:
                price_url = f"{self.base_url}/api/v3/ticker/price?symbol={symbol_with_quote}"
                try:
                    price_response = await asyncio.to_thread(
                        self.session.get,
                        price_url,
                        timeout=0.3,
                    )
                    if price_response.status_code == 200:
                        current_price = float(price_response.json()['price'])
                except (requests.RequestException, OSError, TypeError, ValueError, KeyError) as exc:
                    logger.error(f"MEXC couldn't get price for {symbol}: {exc}")

            if not current_price:
                return {
                    "success": False,
                    "skipped": True,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "reason": "price_unavailable",
                    "execution_time": time.perf_counter() - trade_start,
                }

            adjusted_price = current_price * (1 + self.slippage_pct)
            token_quantity = target_quote / adjusted_price
            formatted_quantity = self.format_quantity_for_mexc(token_quantity, precision)

            if formatted_quantity is None:
                logger.error(f"MEXC quantity formatting failed for {symbol}")
                return {"success": False, "error": "Quantity formatting failed"}

            logger.info(
                f"🔵 MEXC {symbol}: Price=${current_price:.8f}, Raw={token_quantity:.8f} -> Formatted={formatted_quantity} (precision={precision})"
            )

            fallback_client_order_id = self._build_client_order_id()
            quantity_params = [
                ('symbol', symbol_with_quote),
                ('side', 'BUY'),
                ('type', 'MARKET'),
                ('quantity', formatted_quantity),
                ('timestamp', str(self.time_cache.get_time())),
                ('recvWindow', self._compute_recv_window()),
                ('newClientOrderId', fallback_client_order_id),
            ]

            async def _place_mexc_quantity():
                return await self._signed_post(order_url, quantity_params)

            try:
                response = await RETRY_ENGINE.execute_with_retry(
                    _place_mexc_quantity,
                    'mexc',
                    weight=1,
                    is_order=True,
                )
            except Exception as exc:
                logger.error(f"❌ MEXC market order failed (quantity method): {exc}")
                return {
                    "success": False,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "error": str(exc),
                    "execution_time": time.perf_counter() - trade_start
                }

            if response and response.status_code in (200, 201):
                try:
                    result = response.json()
                except ValueError:
                    result = {"raw": response.text}
                try:
                    logger.debug(f"🔵 MEXC quantity raw response: {json.dumps(result, ensure_ascii=False)}")
                except (TypeError, ValueError):
                    logger.debug(f"🔵 MEXC quantity raw response (non-JSON): {result}")

                quantity_order_id = (
                    result.get('orderId')
                    or result.get('orderIdStr')
                    or (result.get('data') or {}).get('orderId')
                    or (result.get('data') or {}).get('orderIdStr')
                )
                quantity_order_id = str(quantity_order_id).strip() if quantity_order_id else None
                quantity_client_order_id = (
                    result.get('clientOrderId')
                    or result.get('origClientOrderId')
                    or result.get('newClientOrderId')
                    or (result.get('data') or {}).get('clientOrderId')
                    or fallback_client_order_id
                )

                success, filled_qty, filled_quote, resolved_order_id = await self._poll_fill_until(
                    symbol_with_quote,
                    quantity_order_id,
                    quantity_client_order_id,
                    target_quote=target_quote,
                    min_ratio=self.min_fill_ratio,
                    max_wait_ms=self.fill_confirm_wait_ms,
                    interval_ms=self.fill_confirm_interval_ms,
                )
                if resolved_order_id:
                    quantity_order_id = resolved_order_id

                    if success:
                        combined_quote = partial_filled_quote + filled_quote
                        combined_qty = partial_filled_qty + filled_qty
                        trade_time = time.perf_counter() - trade_start
                        self._record_trade_success(trade_time)
                        combined_results = {
                            "initial": order_result_payload.get("initial"),
                            "resubmits": order_result_payload.get("resubmits"),
                            "fallback": result,
                        }
                        return {
                            "success": True,
                            "exchange": "mexc",
                            "symbol": symbol,
                            "amount": combined_quote,
                            "filled_quote": combined_quote,
                            "filled_qty": combined_qty,
                            "order_result": combined_results,
                            "partial_fill": partial_filled_quote > 0,
                            "execution_time": trade_time,
                            "order_id": quantity_order_id,
                            "client_order_id": quantity_client_order_id,
                            "requested_amount": original_target_quote,
                            "remaining_quote": max(0.0, original_target_quote - combined_quote),
                            "confirmation_source": "quantity_fallback",
                            "delayed_confirmation": False,
                        }

                logger.error(
                    f"❌ MEXC quantity fill below threshold {filled_quote:.4f}/{target_quote:.4f} USDT (quote failure reason={quote_error}) orderId={quantity_order_id}"
                )
                (
                    delayed_success,
                    delayed_qty,
                    delayed_quote,
                    resolved_delayed_id,
                    delayed_source,
                ) = await self._await_delayed_fill(
                    symbol_with_quote,
                    quantity_order_id,
                    quantity_client_order_id,
                    target_quote,
                )
                candidate_quotes = [
                    q for q in (delayed_quote, partial_filled_quote, filled_quote) if q and q > 0
                ]
                final_quote = max(candidate_quotes) if candidate_quotes else 0.0
                final_qty = max(
                    qty for qty in (delayed_qty, partial_filled_qty, filled_qty) if qty and qty > 0
                ) if candidate_quotes else 0.0
                if final_quote > 0:
                    trade_time = time.perf_counter() - trade_start
                    self._record_trade_success(trade_time)
                    resolved_id = resolved_delayed_id or quantity_order_id
                    ratio = (
                        final_quote / original_target_quote if original_target_quote else 0.0
                    )
                    if ratio >= self.min_fill_ratio:
                        logger.info(
                            "MEXC delayed quantity fill confirmed %.4f/%.4f USDT (ratio=%.3f)",
                            final_quote,
                            original_target_quote,
                            ratio,
                        )
                    else:
                        logger.warning(
                            "MEXC delayed quantity fill accepted with partial ratio %.3f (filled %.4f/%.4f USDT)",
                            ratio,
                            final_quote,
                            original_target_quote,
                        )
                    return {
                        "success": True,
                        "exchange": "mexc",
                        "symbol": symbol,
                        "amount": final_quote,
                        "filled_quote": final_quote,
                        "filled_qty": final_qty,
                        "order_result": {
                            "initial": order_result_payload.get("initial"),
                            "resubmits": order_result_payload.get("resubmits"),
                            "fallback": result,
                            "delayed_check": True,
                        },
                        "partial_fill": ratio < 1.0,
                        "execution_time": trade_time,
                        "order_id": resolved_id,
                        "client_order_id": quantity_client_order_id,
                        "requested_amount": original_target_quote,
                        "remaining_quote": max(0.0, original_target_quote - final_quote),
                        "delayed_confirmation": True,
                        "confirmation_source": delayed_source or "delayed_poll",
                    }
                final_failure_quote = max(partial_filled_quote, filled_quote, delayed_quote, 0.0)
                final_failure_qty = max(partial_filled_qty, filled_qty, delayed_qty, 0.0)
                return {
                    "success": False,
                    "exchange": "mexc",
                    "symbol": symbol,
                    "error": f"insufficient_fill: {filled_quote:.2f}/{target_quote:.2f} USDT",
                    "filled_quote": final_failure_quote,
                    "filled_qty": final_failure_qty,
                    "order_result": result,
                    "execution_time": time.perf_counter() - trade_start,
                    "order_id": quantity_order_id,
                    "client_order_id": quantity_client_order_id,
                    "requested_amount": original_target_quote,
                    "remaining_quote": max(0.0, original_target_quote - combined_failure_quote),
                    "confirmation_source": "rest_quantity",
                }

            error = response.text if response else "Unknown error"
            rejection = self._log_mexc_rejection("quantity order", response, error)

            return {
                "success": False,
                "exchange": "mexc",
                "symbol": symbol,
                "error": rejection.get("message", error),
                "execution_time": time.perf_counter() - trade_start,
                "error_code": rejection.get("code"),
                "status_code": rejection.get("status"),
            }

        except Exception as e:
            logger.error(f"MEXC market order error: {str(e)}")
            return {
                "success": False,
                "exchange": "mexc",
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
        preparation_data: Optional[Any] = None,
        max_slices: Optional[int] = None,
    ) -> Dict[str, Any]:
        """MEXC için toplam USDT tutarını küçük parçalara bölerek market alış yap."""

        try:
            target_total = max(float(total_amount or 0.0), 0.0)
        except (TypeError, ValueError):
            target_total = 0.0

        if target_total <= 0:
            return {
                "success": False,
                "exchange": "mexc",
                "symbol": symbol,
                "error": "invalid_total_amount",
                "execution_time": 0.0,
            }

        if self.fast_mode:
            fast_start = time.perf_counter()
            single_result = await self.execute_trade(symbol, amount=target_total, preparation_data=preparation_data)
            total_time = time.perf_counter() - fast_start
            filled_value = single_result.get("filled_quote") or single_result.get("amount") or 0.0
            try:
                filled_total = float(filled_value)
            except (TypeError, ValueError):
                filled_total = 0.0
            remaining = max(0.0, target_total - filled_total)
            result_summary = {
                "success": single_result.get("success", False),
                "exchange": "mexc",
                "symbol": symbol,
                "amount": filled_total,
                "filled_quote": filled_total,
                "requested_amount": target_total,
                "remaining_amount": remaining,
                "slice_amount": target_total,
                "slice_count": 1,
                "slices": [single_result],
                "execution_time": single_result.get("execution_time", total_time),
                "fast_mode": True,
            }
            for key in (
                "error",
                "reason",
                "rest_status",
                "ws_status",
                "confirmation_source",
                "order_id",
                "client_order_id",
                "filled_qty",
                "delayed_confirmation",
                "partial_fill",
                "remaining_quote",
            ):
                if key in single_result and single_result.get(key) is not None:
                    result_summary[key] = single_result.get(key)
            return result_summary

        try:
            concurrency = int(os.getenv("MEXC_SLICE_CONCURRENCY", "3"))
        except ValueError:
            concurrency = 3
        concurrency = max(1, min(concurrency, 5))

        try:
            default_slice = float(os.getenv("MEXC_MARKET_SLICE_USDT", "1000"))
        except ValueError:
            default_slice = 1000.0

        if slice_amount is not None:
            slice_budget = max(slice_amount, self.min_order_notional)
        else:
            adaptive_slice = target_total / max(1, concurrency)
            slice_budget = min(default_slice, adaptive_slice)
            slice_budget = max(self.min_order_notional, slice_budget)

        try:
            default_max = int(os.getenv("MEXC_MAX_SLICE_COUNT", "12"))
        except ValueError:
            default_max = 12
        max_slice_count = max_slices if max_slices is not None else default_max
        max_slice_count = max(1, max_slice_count)

        start_time = time.perf_counter()
        remaining = target_total
        filled_total = 0.0
        slice_results: List[Dict[str, Any]] = []
        last_error: Optional[str] = None

        try:
            batch_delay = float(os.getenv("MEXC_SLICE_BATCH_DELAY_SEC", "0.0"))
        except ValueError:
            batch_delay = 0.0

        # Slice plan oluştur
        slice_plan: List[float] = []
        temp_remaining = target_total
        for index in range(max_slice_count):
            if temp_remaining <= 0:
                break
            spend = min(slice_budget, temp_remaining)
            # Last permitted slice carries any remaining amount to avoid truncation
            if index == max_slice_count - 1 and temp_remaining > slice_budget:
                spend = temp_remaining
            slice_plan.append(spend)
            temp_remaining = max(0.0, temp_remaining - spend)

        if temp_remaining > 0:
            logger.warning(
                "MEXC slice plan exhausted at %d slices; remaining %.2f USDT will be attempted in fallback",
                max_slice_count,
                temp_remaining,
            )

        index = 0
        total_slices = len(slice_plan)

        while index < total_slices:
            if remaining <= 0:
                break

            batch = slice_plan[index : index + concurrency]
            tasks = [
                asyncio.create_task(
                    self.execute_trade(symbol, amount=amt, preparation_data=preparation_data)
                )
                for amt in batch
            ]

            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            slice_results.extend(batch_results)

            for result in batch_results:
                filled_value = None
                if result.get("success"):
                    filled_value = result.get("filled_quote") or result.get("amount")
                elif result.get("partial_fill"):
                    filled_value = result.get("filled_quote") or result.get("amount")

                if filled_value is not None:
                    try:
                        filled = float(filled_value)
                    except (TypeError, ValueError):
                        filled = 0.0
                else:
                    filled = 0.0

                filled_total += max(0.0, filled)
                remaining = max(0.0, target_total - filled_total)

                if not result.get("success") and not result.get("partial_fill"):
                    last_error = result.get("error") or result.get("reason")
                    # başarısız sipariş -> kalan tutar için yavaşlamamak adına döngüden çık
                    index = total_slices
                    break

            index += len(batch)

            if remaining <= 0 or index >= total_slices:
                break

            if batch_delay > 0:
                await asyncio.sleep(batch_delay)

        total_time = time.perf_counter() - start_time
        success = filled_total > 0.0

        if success:
            logger.info(
                "MEXC sliced execution summary: target=%.2f filled=%.2f slices=%d remaining=%.2f",
                target_total,
                filled_total,
                len(slice_results),
                remaining,
            )
        else:
            fallback_error = last_error
            if not fallback_error and slice_results:
                fallback_error = slice_results[-1].get("error") or slice_results[-1].get("reason")
            last_error = fallback_error
            logger.warning(
                "MEXC sliced execution failed: target=%.2f slices=%d last_error=%s",
                target_total,
                len(slice_results),
                last_error or "no_attempt",
            )

        return {
            "success": success,
            "exchange": "mexc",
            "symbol": symbol,
            "amount": filled_total,
            "filled_quote": filled_total,
            "requested_amount": target_total,
            "remaining_amount": remaining,
            "slice_amount": slice_budget,
            "slice_count": len(slice_results),
            "slices": slice_results,
            "execution_time": total_time,
            "error": None if success else (last_error or "slice_execution_failed"),
        }

    @staticmethod
    def _build_client_order_id() -> str:
        return f"ubnt-mexc-{int(time.time() * 1000)}-{random.randint(100, 999)}"



# -----------------------------------------------------------------------------
# GATE.IO ULTRA TRADER
# -----------------------------------------------------------------------------
