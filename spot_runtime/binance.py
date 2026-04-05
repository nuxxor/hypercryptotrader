from . import common as _shared_common
from .common import *
from .common import _atomic_write_json, _fast_http_lookup, _load_json_file

class BinanceHyperbeastTrader:
    """Ultra hızlı Binance işlem sınıfı - Multi-Account"""
    
    def __init__(self, test_mode=False):
        self.test_mode = test_mode
        self.use_testnet = _shared_common._binance_use_testnet()
        self.base_url = _shared_common._binance_rest_base_url()
        
        # Hesapları keşfet ve yükle
        self.accounts = self._discover_accounts()
        
        # Ana hesap referansı
        self.primary_account = next((acc for acc in self.accounts if acc['name'] == 'primary'), None)
        if self.primary_account:
            self.api_key = self.primary_account['api_key']
            self.api_secret = self.primary_account['api_secret']
        else:
            self.api_key = ""
            self.api_secret = ""
        
        self.headers = {
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        self.session = _shared_common.BINANCE_SESSION
        self.time_cache = ServerTimeCache('binance')
        
        self._pre_encoded_key = self.api_key.encode('utf-8') if self.api_key else b''
        self._pre_encoded_secret = self.api_secret.encode('utf-8') if self.api_secret else b''
        
        # Her hesap için pre-encoded secret'lar
        self._account_secrets = {}
        for acc in self.accounts:
            self._account_secrets[acc['name']] = acc['api_secret'].encode('utf-8')
        
        self._balance_cache = {}
        self._balance_updated = {}
        self._balance_cache_ttl = float(os.getenv("BINANCE_BALANCE_CACHE_TTL", "3600"))
        
        self.performance = {
            'total': {
                'trade_count': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'total_trade_time': 0
            }
        }
        
        for acc in self.accounts:
            self.performance[acc['name']] = {
                'trade_count': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'min_trade_time': float('inf'),
                'max_trade_time': 0,
                'avg_trade_time': 0,
                'total_trade_time': 0
            }
        
        self._signature_cache = {}

        self._listing_cache: Dict[str, Tuple[bool, float]] = {}
        self._listing_cache_ttl = 0.1
        self._symbol_info_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._symbol_info_cache_ttl = 1.0
        self._listing_prefetch_tasks: Dict[str, asyncio.Task] = {}
        self._listing_prefetch_window = 0.3
        self._listing_prefetch_interval = 0.05

        # Cache eviction settings
        self._last_cache_eviction = 0.0
        self._cache_eviction_interval = 60.0  # Evict every 60 seconds
        self._max_cache_size = 500  # Max entries per cache

        override_ttl = os.getenv("BINANCE_SYMBOL_CACHE_TTL_OVERRIDE")
        if override_ttl:
            try:
                self._symbol_info_cache_ttl = float(override_ttl)
                logger.info("Binance symbol cache TTL override applied: %.1fs", self._symbol_info_cache_ttl)
            except ValueError:
                logger.warning("Invalid BINANCE_SYMBOL_CACHE_TTL_OVERRIDE=%s", override_ttl)
        override_listing_ttl = os.getenv("BINANCE_LISTING_CACHE_TTL_OVERRIDE")
        if override_listing_ttl:
            try:
                self._listing_cache_ttl = float(override_listing_ttl)
                logger.info("Binance listing cache TTL override applied: %.1fs", self._listing_cache_ttl)
            except ValueError:
                logger.warning("Invalid BINANCE_LISTING_CACHE_TTL_OVERRIDE=%s", override_listing_ttl)

        cache_path_env = os.getenv("BINANCE_SYMBOL_CACHE_FILE")
        self._symbol_cache_path = Path(cache_path_env) if cache_path_env else BINANCE_SYMBOL_CACHE_FILE
        self._last_symbol_cache_persist = 0.0
        self._load_symbol_info_cache_from_disk()
        
        logger.info(f"Binance Multi-Account Trader started - {len(self.accounts)} accounts found")
        logger.info("Binance environment: %s", "testnet" if self.use_testnet else "live")
        for acc in self.accounts:
            logger.info("  - %s", acc["name"])
        
        self._warm_up()
    
    def _discover_accounts(self):
        """Tüm Binance hesaplarını otomatik keşfet"""
        if self.use_testnet:
            api_key = os.getenv("BINANCE_TESTNET_API_KEY") or os.getenv("BINANCE_API_KEY")
            api_secret = os.getenv("BINANCE_TESTNET_API_SECRET") or os.getenv("BINANCE_API_SECRET")
        else:
            api_key = os.getenv("BINANCE_API_KEY")
            api_secret = os.getenv("BINANCE_API_SECRET")

        if api_key and api_secret:
            return [{
                'name': 'primary',
                'api_key': api_key,
                'api_secret': api_secret,
                'enabled': True,
            }]

        return []
    
    def _warm_up(self):
        """Binance trader ısınma"""
        try:
            if not self.time_cache.warmup_complete:
                time.sleep(0.1)
            
            _ = self.time_cache.get_time()
            
            if not self.test_mode and self.session:
                self.session.get(f"{self.base_url}/api/v3/ping", timeout=1)
            
            return True
        except:
            return False

    def _load_symbol_info_cache_from_disk(self) -> None:
        data = _load_json_file(self._symbol_cache_path)
        if not isinstance(data, dict):
            return

        now = time.time()
        loaded = 0
        for symbol, info in data.items():
            if isinstance(symbol, str) and isinstance(info, dict):
                self._symbol_info_cache[symbol.upper()] = (info, now)
                loaded += 1
        if loaded:
            logger.debug(
                "Binance symbol cache primed with %d entries from %s",
                loaded,
                self._symbol_cache_path,
            )

    def _persist_symbol_cache_if_needed(self) -> None:
        now = time.time()
        if now - self._last_symbol_cache_persist < 2.0:
            return

        payload = {
            symbol: info
            for symbol, (info, _) in self._symbol_info_cache.items()
            if isinstance(info, dict)
        }
        if not payload:
            return

        _atomic_write_json(self._symbol_cache_path, payload)
        self._last_symbol_cache_persist = now

    def _schedule_listing_prefetch(self, symbol: str) -> None:
        symbol = symbol.upper()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        existing = self._listing_prefetch_tasks.get(symbol)
        if existing and not existing.done():
            return

        self._listing_prefetch_tasks[symbol] = loop.create_task(self._prefetch_listing_status(symbol))

    async def _prefetch_listing_status(self, symbol: str) -> None:
        deadline = time.perf_counter() + self._listing_prefetch_window

        while time.perf_counter() < deadline:
            cache_entry = self._listing_cache.get(symbol)
            if cache_entry and (time.time() - cache_entry[1]) < self._listing_cache_ttl and cache_entry[0]:
                return
            cached_info = self._symbol_info_cache.get(symbol)
            if cached_info and (time.time() - cached_info[1]) < self._symbol_info_cache_ttl:
                self._listing_cache[symbol] = (True, time.time())
                return

            result, _, _ = await self._fetch_symbol_metadata(symbol)

            if result:
                self._listing_cache[symbol] = (True, time.time())
                return

            await asyncio.sleep(self._listing_prefetch_interval)

    def _extract_binance_symbol_info(self, symbol: str, response: requests.Response) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            data = response.json()
        except ValueError as exc:
            return None, f"invalid json {exc}"

        symbols = data.get('symbols') or []
        for entry in symbols:
            entry_symbol = entry.get('symbol')
            if entry_symbol != symbol:
                continue

            status = (entry.get('status') or '').upper()
            if status in {'TRADING', 'PRE_TRADING'}:
                return entry, None
            return None, f"status {status or 'unknown'}"

        if symbols:
            return None, "symbol missing or inactive"
        return None, "empty symbols"

    async def _fetch_symbol_metadata(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[Dict[str, Any]]]:
        params = {'symbol': symbol}
        extractor = functools.partial(self._extract_binance_symbol_info, symbol)

        weight = BINANCE_ENDPOINT_WEIGHTS.get("GET /api/v3/exchangeInfo", 10)
        while not RATE_LIMITER.can_request('binance', weight=weight, is_order=False):
            wait_time = RATE_LIMITER.get_wait_time('binance', weight=weight, is_order=False)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            else:
                break
        RATE_LIMITER.add_request('binance', weight=weight, is_order=False)

        result, errors, meta = await _fast_http_lookup(
            url=f"{self.base_url}/api/v3/exchangeInfo",
            session=self.session,
            params=params,
            headers=None,
            attempt_timeouts=REALISTIC_LOOKUP_ATTEMPTS,
            extractor=extractor,
        )
        if result:
            self._symbol_info_cache[symbol] = (result, time.time())
            self._persist_symbol_cache_if_needed()
        return result, errors, meta

    async def _get_symbol_metadata(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[Dict[str, Any]]]:
        symbol = symbol.upper()
        cached = self._symbol_info_cache.get(symbol)
        if cached and (time.time() - cached[1]) < self._symbol_info_cache_ttl:
            return cached[0], [], None
        return await self._fetch_symbol_metadata(symbol)

    async def is_symbol_listed(self, symbol: str) -> bool:
        symbol = symbol.upper()
        now = time.time()

        # Periodic cache eviction
        self._evict_stale_cache()

        cache_entry = self._listing_cache.get(symbol)
        if cache_entry and (now - cache_entry[1]) < self._listing_cache_ttl:
            return cache_entry[0]

        self._schedule_listing_prefetch(symbol)

        result, errors, meta = await self._get_symbol_metadata(symbol)

        listed = bool(result)
        self._listing_cache[symbol] = (listed, time.time())

        if listed and meta:
            logger.info(
                f"Binance exchangeInfo success for {symbol}: attempt={meta['attempt']} via={meta['winner']} "
                f"elapsed={meta['elapsed_ms']:.1f}ms"
            )
        if not listed and errors:
            joined = " | ".join(errors[:6])
            logger.warning(f"Binance symbol lookup failed for {symbol}: {joined}")

        return listed

    def _evict_stale_cache(self):
        """Periodically evict stale cache entries to prevent memory leaks"""
        now = time.time()
        if now - self._last_cache_eviction < self._cache_eviction_interval:
            return

        self._last_cache_eviction = now

        # Evict stale listing cache entries
        stale_listing_keys = [
            k for k, (_, ts) in self._listing_cache.items()
            if now - ts > self._listing_cache_ttl * 10  # 10x TTL before eviction
        ]
        for k in stale_listing_keys:
            del self._listing_cache[k]

        # Evict stale symbol info cache entries
        stale_symbol_keys = [
            k for k, (_, ts) in self._symbol_info_cache.items()
            if now - ts > self._symbol_info_cache_ttl * 2  # 2x TTL before eviction
        ]
        for k in stale_symbol_keys:
            del self._symbol_info_cache[k]

        # Enforce max cache size (LRU-style eviction)
        if len(self._listing_cache) > self._max_cache_size:
            sorted_items = sorted(self._listing_cache.items(), key=lambda x: x[1][1])
            for k, _ in sorted_items[:len(self._listing_cache) - self._max_cache_size]:
                del self._listing_cache[k]

        if len(self._symbol_info_cache) > self._max_cache_size:
            sorted_items = sorted(self._symbol_info_cache.items(), key=lambda x: x[1][1])
            for k, _ in sorted_items[:len(self._symbol_info_cache) - self._max_cache_size]:
                del self._symbol_info_cache[k]

        evicted = len(stale_listing_keys) + len(stale_symbol_keys)
        if evicted > 0:
            logger.debug(f"Cache eviction: removed {evicted} stale entries")

    def _generate_signature(self, params, account_name='primary'):
        """Ultra hızlı imza oluşturma"""
        query_string = '&'.join(f"{k}={v}" for k, v in params.items())

        secret = self._account_secrets.get(account_name, self._pre_encoded_secret)
        
        signature = hmac.new(
            secret,
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature

    def _build_client_order_id(self, account_name: str) -> str:
        timestamp_ms = int(time.time() * 1000)
        return f"ubnt-{account_name}-{timestamp_ms}-{random.randint(100, 999)}"
    
    def _get_balance(self, account_name='primary', force=False):
        """Belirtilen hesabın USDT bakiyesini al"""
        current_time = time.time()
        
        if self.test_mode:
            return 1000.0
        
        cache_key = f"{account_name}_balance"
        if (
            not force
            and cache_key in self._balance_cache
            and (current_time - self._balance_updated.get(cache_key, 0) < self._balance_cache_ttl)
        ):
            return self._balance_cache[cache_key]
        
        account = next((acc for acc in self.accounts if acc['name'] == account_name), None)
        if not account:
            return 0.0
        
        try:
            timestamp = self.time_cache.get_time()
            
            params = {
                'timestamp': timestamp,
                'recvWindow': 60000
            }
            
            signature = self._generate_signature(params, account_name)
            params['signature'] = signature
            
            headers = {'X-MBX-APIKEY': account['api_key']}
            
            url = f"{self.base_url}/api/v3/account"
            weight = BINANCE_ENDPOINT_WEIGHTS.get("GET /api/v3/account", 10)
            if not RATE_LIMITER.can_request('binance', weight=weight, is_order=False):
                wait_time = RATE_LIMITER.get_wait_time('binance', weight=weight, is_order=False)
                if wait_time > 0:
                    time.sleep(wait_time)
            RATE_LIMITER.add_request('binance', weight=weight, is_order=False)
            response = self.session.get(url, params=params, headers=headers, timeout=1)
            
            if response.status_code == 200:
                data = response.json()
                
                for balance in data.get('balances', []):
                    if balance['asset'] == 'USDT':
                        usdt_balance = float(balance['free'])
                        
                        self._balance_cache[cache_key] = usdt_balance
                        self._balance_updated[cache_key] = current_time
                        
                        return usdt_balance
                
                self._balance_cache[cache_key] = 0.0
                self._balance_updated[cache_key] = current_time
                return 0.0
            else:
                return self._balance_cache.get(cache_key, 0.0)
        
        except Exception as e:
            logger.error(f"Binance {account_name} balance error: {e}")
            return self._balance_cache.get(cache_key, 0.0)
    
    def get_balance(self, force=False):
        """Ana hesap bakiyesi"""
        return self._get_balance('primary', force)
    
    def get_all_balances(self, force=False):
        """Tüm hesapların bakiyelerini al"""
        balances = {}
        for account in self.accounts:
            if account['enabled']:
                balance = self._get_balance(account['name'], force)
                balances[account['name']] = balance
        return balances

    @staticmethod
    def _precision_from_step(step_value: Any) -> Optional[int]:
        if step_value in (None, '', '0', 0):
            return None
        try:
            decimal_value = Decimal(str(step_value))
        except Exception:
            return None
        if decimal_value == 0:
            return None
        normalized = decimal_value.normalize()
        return max(-normalized.as_tuple().exponent, 0)

    async def _execute_single_trade(self, account, symbol, amount, preparation_data=None):
        """Tek bir hesap için işlem yap"""
        trade_start = time.perf_counter()
        account_name = account['name']
        base_symbol = symbol.upper()

        try:
            symbol_with_quote = SymbolConverter.convert(symbol, "binance")

            symbol_info, info_errors, _ = await self._get_symbol_metadata(symbol_with_quote)
            quote_precision = 2
            min_notional = None
            price_precision: Optional[int] = None
            quantity_precision: Optional[int] = None

            if symbol_info:
                raw_quote_precision = symbol_info.get('quotePrecision', symbol_info.get('quoteAssetPrecision'))
                try:
                    if raw_quote_precision is not None:
                        quote_precision = max(int(raw_quote_precision), 2)
                except (TypeError, ValueError):
                    quote_precision = 2

                if 'pricePrecision' in symbol_info:
                    try:
                        price_precision = int(symbol_info['pricePrecision'])
                    except (TypeError, ValueError):
                        price_precision = None

                if 'baseAssetPrecision' in symbol_info:
                    try:
                        quantity_precision = int(symbol_info['baseAssetPrecision'])
                    except (TypeError, ValueError):
                        quantity_precision = None

                filters = symbol_info.get('filters', [])
                for filter_entry in filters:
                    filter_type = filter_entry.get('filterType')
                    if filter_type == 'MIN_NOTIONAL':
                        raw_min = filter_entry.get('minNotional') or filter_entry.get('notional')
                        if raw_min is not None:
                            try:
                                min_notional = float(raw_min)
                            except (TypeError, ValueError):
                                min_notional = None
                    elif filter_type == 'LOT_SIZE':
                        precision = self._precision_from_step(filter_entry.get('stepSize'))
                        if precision is not None:
                            quantity_precision = precision
                    elif filter_type == 'PRICE_FILTER':
                        precision = self._precision_from_step(filter_entry.get('tickSize'))
                        if precision is not None:
                            price_precision = precision

                if price_precision is not None and quantity_precision is not None:
                    try:
                        PRECISION_MANAGER.update_precision(
                            'binance',
                            base_symbol,
                            int(price_precision),
                            int(quantity_precision),
                        )
                    except Exception:
                        pass
            else:
                if info_errors:
                    logger.warning(
                        f"Binance metadata lookup failed for {symbol_with_quote}: "
                        + " | ".join(info_errors[:6])
                    )

            amount = float(amount)
            requested_amount = amount
            balance = self._get_balance(account_name)

            if amount > balance:
                amount = min(balance * 0.99, 200.0)  # Cap at $200

            if amount <= 0:
                amount = min(10.0, balance)

            spend_amount = amount
            min_spend_requirement = 10.0
            if min_notional is not None:
                min_spend_requirement = max(min_spend_requirement, min_notional)

            if balance < min_spend_requirement:
                raise RuntimeError(
                    f"insufficient balance {balance:.4f} < minimum trade notional {min_spend_requirement:.4f}"
                )

            if spend_amount < min_spend_requirement:
                spend_amount = min_spend_requirement

            if spend_amount > balance:
                spend_amount = balance

            step = Decimal('1') if quote_precision == 0 else Decimal('1').scaleb(-quote_precision)
            spend_decimal = Decimal(str(spend_amount))
            min_decimal = Decimal(str(min_spend_requirement))
            balance_decimal = Decimal(str(balance))
            required_steps = (min_decimal / step).to_integral_value(rounding=ROUND_CEILING)
            requested_steps = (spend_decimal / step).to_integral_value(rounding=ROUND_CEILING)
            normalized_spend = max(required_steps, requested_steps) * step

            if normalized_spend > balance_decimal:
                normalized_spend = balance_decimal.quantize(step, rounding=ROUND_DOWN)

            if normalized_spend < required_steps * step:
                raise RuntimeError(
                    f"unable to satisfy minimum notional {min_spend_requirement:.4f} after precision rounding"
                )

            spend_amount = float(normalized_spend)
            formatted_amount = format_quote_amount(spend_amount, quote_precision)
            client_order_id = self._build_client_order_id(account_name)

            if self.test_mode:
                await asyncio.sleep(0.001)
                result = {
                    "symbol": symbol_with_quote,
                    "orderId": int(time.time() * 1000),
                    "clientOrderId": client_order_id,
                    "transactTime": int(time.time() * 1000),
                    "status": "TEST_SUCCESS"
                }
            else:
                url = f"{self.base_url}/api/v3/order"
                headers = {'X-MBX-APIKEY': account['api_key']}

                async def _place_binance_order():
                    params = {
                        'symbol': symbol_with_quote,
                        'side': 'BUY',
                        'type': 'MARKET',
                        'quoteOrderQty': formatted_amount,
                        'timestamp': self.time_cache.get_time(),
                        'recvWindow': 60000,
                        'newOrderRespType': 'ACK',
                        'newClientOrderId': client_order_id,
                    }
                    signature = self._generate_signature(params, account_name)
                    params['signature'] = signature

                    response = await asyncio.to_thread(
                        self.session.post,
                        url,
                        params=params,
                        headers=headers,
                        timeout=(0.3, 1.2),
                    )

                    if response.status_code in [200, 201]:
                        return response.json()
                    raise Exception(f"HTTP {response.status_code}: {response.text}")

                result = await RETRY_ENGINE.execute_with_retry(
                    _place_binance_order,
                    'binance',
                    weight=BINANCE_ENDPOINT_WEIGHTS["POST /api/v3/order"],
                    is_order=True,
                )

            trade_time = time.perf_counter() - trade_start

            perf = self.performance[account_name]
            perf['trade_count'] += 1
            perf['successful_trades'] += 1
            perf['min_trade_time'] = min(perf['min_trade_time'], trade_time)
            perf['max_trade_time'] = max(perf['max_trade_time'], trade_time)
            perf['total_trade_time'] += trade_time
            perf['avg_trade_time'] = perf['total_trade_time'] / perf['trade_count']

            self.performance['total']['trade_count'] += 1
            self.performance['total']['successful_trades'] += 1
            self.performance['total']['total_trade_time'] += trade_time

            logger.info(
                f"Binance {account_name} {symbol} bought: {formatted_amount} USDT (requested {requested_amount:.2f}), "
                f"time: {trade_time*1000:.2f}ms"
            )

            return {
                "success": True,
                "exchange": "binance",
                "account": account_name,
                "symbol": symbol,
                "amount": spend_amount,
                "order_result": result,
                "execution_time": trade_time
            }

        except Exception as e:
            trade_time = time.perf_counter() - trade_start

            self.performance[account_name]['trade_count'] += 1
            self.performance[account_name]['failed_trades'] += 1
            self.performance['total']['trade_count'] += 1
            self.performance['total']['failed_trades'] += 1

            logger.error(f"Binance {account_name} trade error ({symbol}): {e}")

            return {
                "success": False,
                "exchange": "binance",
                "account": account_name,
                "symbol": symbol,
                "error": str(e),
                "execution_time": trade_time
            }

    @measure_time
    async def execute_trade(
        self,
        symbol: str,
        amount: Optional[float] = None,
        per_account_amounts: Optional[Dict[str, float]] = None,
        preparation_data: Optional[Any] = None,
    ):
        """Binance ana giriş noktası - tek hesap veya çoklu hesap akışını yönetir."""
        start = time.perf_counter()
        enabled_accounts = [acc for acc in self.accounts if acc.get('enabled', True)]

        if not enabled_accounts:
            return {
                "success": False,
                "exchange": "binance",
                "symbol": symbol,
                "error": "no_enabled_accounts",
                "execution_time": time.perf_counter() - start,
            }

        if per_account_amounts:
            tasks: List[Tuple[str, asyncio.Task]] = []
            for account in enabled_accounts:
                trade_amount = per_account_amounts.get(account['name'])
                if trade_amount is None or trade_amount <= 0:
                    continue
                task = asyncio.create_task(
                    self._execute_single_trade(account, symbol, trade_amount, preparation_data)
                )
                tasks.append((account['name'], task))

            multi_results: List[Dict[str, Any]] = []
            if tasks:
                raw_results = await asyncio.gather(
                    *[task for _, task in tasks],
                    return_exceptions=True,
                )
                for idx, result in enumerate(raw_results):
                    account_name = tasks[idx][0]
                    if isinstance(result, Exception):
                        logger.error(f"Binance {account_name} trade error ({symbol}): {result}")
                        multi_results.append({
                            "success": False,
                            "exchange": "binance",
                            "account": account_name,
                            "symbol": symbol,
                            "error": str(result),
                            "execution_time": 0.0,
                        })
                    else:
                        multi_results.append(result)

            success_any = any(r.get('success') for r in multi_results)
            return {
                "success": success_any,
                "exchange": "binance",
                "symbol": symbol,
                "multi_account_results": multi_results,
                "execution_time": time.perf_counter() - start,
            }

        # Tek hesap (varsayılan) - primary öncelikli
        target_account = self.primary_account
        if not target_account or not target_account.get('enabled', True):
            target_account = next((acc for acc in enabled_accounts if acc.get('enabled', True)), None)

        if not target_account:
            return {
                "success": False,
                "exchange": "binance",
                "symbol": symbol,
                "error": "no_active_account_available",
                "execution_time": time.perf_counter() - start,
            }

        trade_amount = amount if amount is not None else 10.0
        return await self._execute_single_trade(target_account, symbol, trade_amount, preparation_data)
