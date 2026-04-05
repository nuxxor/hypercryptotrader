from . import common as _shared_common
from .common import *
from .common import _atomic_write_json, _extract_listing_aliases, _load_json_file
from .binance import BinanceHyperbeastTrader
from .mexc import MexcHyperbeastTrader
from .gate import GateUltraTrader

class UltraTrader:
    """Ana Ultra Hızlı İşlem Sınıfı - Market Cap Filtreli"""
    
    def __init__(self, test_mode=False):
        self.test_mode = test_mode
        
        # Ubuntu optimizasyonlarını uygula
        UbuntuOptimizer.optimize_all()
        
        # Borsa işlemcileri
        self.binance_trader = BinanceHyperbeastTrader(test_mode)
        self.mexc_trader = MexcHyperbeastTrader(test_mode)
        self.gate_trader = GateUltraTrader(test_mode=test_mode)

        def _env_flag(name: str, default: bool) -> bool:
            value = os.getenv(name)
            if value is None:
                return default
            return value.strip().lower() not in {"false", "0", "no", "off"}

        self.allow_upbit_listings = _env_flag("ALLOW_UPBIT_LISTINGS", False)
        self.allow_upbit_krw_listings = _env_flag("ALLOW_UPBIT_KRW_LISTINGS", False)
        self.allow_bithumb_listings = _env_flag("ALLOW_BITHUMB_LISTINGS", False)
        self.allow_binance_listings = _env_flag("ALLOW_BINANCE_LISTINGS", True)
        self.allow_investment_signals = _env_flag("ALLOW_INVESTMENT_SIGNALS", True)
        self.allow_binance_alpha_signals = _env_flag("ALLOW_BINANCE_ALPHA_SIGNALS", False)

        def _flag_label(flag: bool) -> str:
            return "ON" if flag else "OFF"

        logger.info(
            "Listing permissions -> Upbit: %s | Upbit KRW: %s | Bithumb: %s | Binance: %s | Investment: %s | Binance Alpha: %s",
            _flag_label(self.allow_upbit_listings),
            _flag_label(self.allow_upbit_krw_listings),
            _flag_label(self.allow_bithumb_listings),
            _flag_label(self.allow_binance_listings),
            _flag_label(self.allow_investment_signals),
            _flag_label(self.allow_binance_alpha_signals),
        )

        # Signal Service endpoint
        self.signal_ws_url = os.getenv("SIGNAL_WS_URL", "ws://localhost:9999/ws")
        self.signal_api_key = os.getenv("SIGNAL_API_KEY", "")
        self.signal_ws_client = None

        # Hazırlık API endpoint
        self.prep_ws_url = os.getenv("PREP_WS_URL", "ws://localhost:9999/ws")
        self.prep_api_key = os.getenv("PREP_API_KEY", "")
        self.prep_ws_client = None
        self.prep_ws_enabled = _env_flag("ENABLE_PREP_WS", True)

        # Position/trade notification channel
        self.position_ws_url = os.getenv("POSITION_WS_URI", "ws://localhost:9999/ws")
        self.position_ws_client = None
        self.position_ws_enabled = _env_flag("ENABLE_POSITION_WS", True)
        prep_cache_env = os.getenv("PREPARED_DATA_CACHE_FILE")
        self.prepared_cache_path = Path(prep_cache_env) if prep_cache_env else CACHE_ROOT / "prepared_data_cache.json"
        self.prepared_cache: Dict[str, Any] = {}
        self._last_prepared_cache_persist = 0.0
        self._load_prepared_cache_from_disk()
        self._last_signal_payload = None
        try:
            jitter_default = float(os.getenv("EXECUTION_JITTER_MS", "0"))
        except ValueError:
            jitter_default = 0.0
        self.execution_jitter_ms = max(0.0, jitter_default)
        
        # İstatistikler
        self.stats = {
            "total_trades": 0,
            "successful_trades": 0,
            "failed_trades": 0,
            "avg_binance_time": 0,
            "avg_gate_time": 0,
            "avg_mexc_time": 0,
            "total_binance_trades": 0,
            "total_gate_trades": 0,
            "total_mexc_trades": 0
        }
        
        # KRW istatistikleri
        self.krw_stats = {
            "krw_trades": 0,
            "krw_successful": 0,
            "krw_failed": 0,
            "non_krw_trades": 0,
            "non_krw_successful": 0,
            "non_krw_failed": 0,
            "krw_total_amount": 0.0,
            "non_krw_total_amount": 0.0
        }
        
        # Multi-account istatistikleri
        self.multi_account_stats = {
            "binance_accounts": len(self.binance_trader.accounts),
            "total_binance_trades": 0,
            "successful_binance_trades": 0,
            "by_account": {}
        }
        
        for acc in self.binance_trader.accounts:
            self.multi_account_stats["by_account"][acc['name']] = {
                "trades": 0,
                "successful": 0,
                "failed": 0,
                "total_amount": 0.0
            }
        
        self.running = True
        self.last_heartbeat_time = time.time()
        self.heartbeat_interval = 15
        self.status_check_time = time.time()
        self.status_check_interval = 60
        self.idle_status_interval = 3600
        self.last_status_log_time = 0.0
        self.last_status_log_trade_count = 0
        self.tasks = set()
        self.ws_loop_task = None
        self.processed_signals: Dict[str, float] = {}
        self._processed_signal_cache_max = max(
            20,
            int(os.getenv("SPOT_PROCESSED_SIGNAL_CACHE_MAX", "200")),
        )
        self._processed_signal_cache_trim = max(
            10,
            min(
                self._processed_signal_cache_max,
                int(os.getenv("SPOT_PROCESSED_SIGNAL_CACHE_TRIM", "100")),
            ),
        )
        try:
            self._signal_gc_threshold_mb = float(os.getenv("SPOT_SIGNAL_GC_THRESHOLD_MB", "1500"))
        except ValueError:
            self._signal_gc_threshold_mb = 1500.0
        try:
            self._signal_gc_min_interval = float(os.getenv("SPOT_SIGNAL_GC_MIN_INTERVAL_SEC", "30"))
        except ValueError:
            self._signal_gc_min_interval = 30.0
        self._last_signal_gc = 0.0
        self.preload_trade_params = {}
        
        self.balances = {
            "binance": {},
            "mexc": 0.0,
            "gate": 0.0
        }
        
        self.balances_loaded = False
        self.last_balance_refresh = time.time()
        self.balance_refresh_interval = 3600
        
        logger.info(f"UltraTrader started - {len(self.binance_trader.accounts)} Binance accounts active")
    
    async def initialize(self):
        """Sistemi başlat"""
        logger.info("Ultra Trading System starting - UBUNTU EDITION V3.2 (Multi-Account + Market Cap Filter)...")
        
        self.signal_ws_client = HyperbeastWebSocketClient(self.signal_ws_url, self.signal_api_key)
        signal_connected = await self.signal_ws_client.connect()

        prep_connected = False
        if self.prep_ws_enabled:
            self.prep_ws_client = HyperbeastWebSocketClient(self.prep_ws_url, self.prep_api_key)
            prep_connected = await self.prep_ws_client.connect()
        else:
            self.prep_ws_client = None

        position_connected = False
        if self.position_ws_enabled:
            self.position_ws_client = HyperbeastWebSocketClient(self.position_ws_url)
            position_connected = await self.position_ws_client.connect()
        else:
            self.position_ws_client = None

        if signal_connected:
            logger.info("Signal service WebSocket connection established")
        else:
            logger.warning("Signal service WebSocket connection failed")
            
        if self.prep_ws_enabled and prep_connected:
            logger.info("Preparation service WebSocket connection established")
        elif self.prep_ws_enabled:
            logger.warning("Preparation service WebSocket connection failed")

        if self.position_ws_enabled and position_connected:
            logger.info("Position notification WebSocket connection established")
        elif self.position_ws_enabled:
            logger.warning("Position notification WebSocket connection failed")
        
        self.ws_loop_task = await self.create_tracked_task(
            self.ws_listen_loop(),
            label="ws-listen-loop",
        )
        
        await self._preload_common_tokens()
        await self._load_initial_balances()
        if self.mexc_trader.fast_mode and not self.mexc_trader.test_mode:
            try:
                user_stream = await self.mexc_trader.ensure_user_stream()
                if user_stream:
                    logger.info("MEXC user stream initialized for fast mode confirmations")
                else:
                    logger.warning("MEXC user stream unavailable; fast mode will fall back to REST confirmation")
            except Exception as exc:
                logger.warning("Failed to initialize MEXC user stream: %s", exc)
        
        logger.info("Ultra Trading System ready - UBUNTU EDITION V3.2")
        logger.info(f"Market Cap Filter: $400M (tokens above this will be skipped)")
        logger.info(f"Target Latency: <200ms total")
        logger.info(f"Binance Multi-Account: {len(self.binance_trader.accounts)} accounts active")
        
        return self

    def _load_prepared_cache_from_disk(self) -> None:
        data = _load_json_file(self.prepared_cache_path)
        if not isinstance(data, dict):
            return
        loaded = {k.upper(): v for k, v in data.items() if isinstance(k, str) and isinstance(v, dict)}
        if loaded:
            self.prepared_cache.update(loaded)
            logger.debug(
                "Prepared-data cache primed with %d entries from %s",
                len(loaded),
                self.prepared_cache_path,
            )

    def _persist_prepared_cache_if_needed(self) -> None:
        now = time.time()
        if now - self._last_prepared_cache_persist < 2.0:
            return
        if not self.prepared_cache:
            return
        payload = {k: v for k, v in self.prepared_cache.items() if isinstance(v, dict)}
        if not payload:
            return
        _atomic_write_json(self.prepared_cache_path, payload)
        self._last_prepared_cache_persist = now
    
    async def _preload_common_tokens(self):
        """Sık kullanılan tokenler için ön hazırlık yap"""
        if not self.prep_ws_enabled:
            logger.info("Preparation service disabled - skipping token preload")
            return 0

        common_tokens = ["BTC", "ETH", "XRP", "BNB", "ADA", "SOL", "DOGE", "AVAX", "DOT", "SHIB", "MATIC", "LTC"]
        
        logger.info(f"Preloading common tokens...")
        
        for token in common_tokens:
            try:
                result = await self.get_prepared_data(token)
                if result:
                    self.preload_trade_params[token] = result
            except Exception as e:
                logger.warning(f"Could not prepare data for {token}: {e}")
        
        logger.info(f"Preloaded {len(self.preload_trade_params)} tokens")
        return len(self.preload_trade_params)
    
    async def _load_initial_balances(self):
        """Başlangıçta tüm borsaların bakiyelerini yükle"""
        if self.test_mode:
            self.balances = {
                "binance": {"primary": 1000.0},
                "mexc": 1000.0,
                "gate": 1000.0,
            }
            self.balances_loaded = True
            logger.info("Test mode active - using default balances")
            return
            
        try:
            binance_balances = self.binance_trader.get_all_balances(force=True)
            self.balances["binance"] = binance_balances
            
            tasks = [
                self._get_balance_from_trader("mexc"),
                self._get_balance_from_trader("gate")
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, exchange in enumerate(["mexc", "gate"]):
                result = results[i]
                if isinstance(result, Exception):
                    logger.warning(f"{exchange.upper()} balance could not be retrieved: {str(result)}")
                    self.balances[exchange] = 0.0
                else:
                    self.balances[exchange] = result
            
            self.balances_loaded = True
            
            logger.info("Initial balances loaded:")
            
            total_binance = 0
            for acc_name, balance in self.balances["binance"].items():
                logger.info(f"  Binance {acc_name}: {balance:.2f} USDT")
                total_binance += balance
            logger.info(f"  Binance TOTAL: {total_binance:.2f} USDT")
            
            logger.info(f"  MEXC: {self.balances['mexc']:.2f} USDT")
            logger.info(f"  Gate.io: {self.balances['gate']:.2f} USDT")
            
        except Exception as e:
            logger.error(f"Error loading balances: {str(e)}")
            self.balances = {
                "binance": {},
                "mexc": 0.0,
                "gate": 0.0
            }
    
    async def _get_balance_from_trader(self, exchange, force=False):
        """Belirtilen borsadan bakiye al"""
        try:
            if exchange == "binance":
                return self.binance_trader.get_all_balances(force=force)
            elif exchange == "mexc":
                mexc_balance = self.mexc_trader.get_balance(force=force)
                return mexc_balance
            elif exchange == "gate":
                gate_balance = await self.gate_trader._get_balance(force=force)
                return gate_balance
            else:
                return 0.0
        except Exception as e:
            logger.error(f"{exchange.upper()} balance error: {str(e)}")
            return 0.0 if exchange != "binance" else {}
    
    async def _refresh_all_balances(self):
        """Tüm borsaların bakiyelerini güncelle"""
        if self.test_mode:
            logger.info("Balance refresh skipped in test mode")
            return 0
            
        logger.info("Hourly balance refresh starting...")
        
        try:
            refresh_tasks = [
                self._get_balance_from_trader("binance", force=True),
                self._get_balance_from_trader("mexc", force=True),
                self._get_balance_from_trader("gate", force=True)
            ]
            
            results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
            
            updated_count = 0
            
            # Binance multi-account
            if not isinstance(results[0], Exception):
                old_balances = self.balances.get("binance", {})
                new_balances = results[0]
                self.balances["binance"] = new_balances
                
                for acc_name, new_balance in new_balances.items():
                    old_balance = old_balances.get(acc_name, 0)
                    change = new_balance - old_balance
                    change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
                    logger.info(f"Binance {acc_name} balance updated: {new_balance:.2f} USDT ({change_str} USDT)")
                    updated_count += 1
            
            # MEXC
            if not isinstance(results[1], Exception):
                old_balance = self.balances.get("mexc", 0)
                self.balances["mexc"] = results[1]
                change = results[1] - old_balance
                change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
                logger.info(f"MEXC balance updated: {results[1]:.2f} USDT ({change_str} USDT)")
                updated_count += 1
            
            # Gate
            if not isinstance(results[2], Exception):
                old_balance = self.balances.get("gate", 0)
                self.balances["gate"] = results[2]
                change = results[2] - old_balance
                change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
                logger.info(f"Gate.io balance updated: {results[2]:.2f} USDT ({change_str} USDT)")
                updated_count += 1
            
            self.balances_loaded = True
            logger.info(f"Balance refresh completed - {updated_count} updates made")
            
            return updated_count
        except Exception as e:
            logger.error(f"Error refreshing balances: {str(e)}")
            return 0

    async def _broadcast_trade_results(self, tokens, summary):
        if not self.position_ws_client:
            return

        results = summary.get('results') or []
        if not results:
            return

        payload = {
            "type": "trade_result",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tokens": [t.upper() for t in tokens],
            "success": summary.get('success', False),
            "total": summary.get('total', 0),
            "successful": summary.get('successful', 0),
            "failed": summary.get('failed', 0),
            "results": results,
            "signal": self._last_signal_payload or {},
        }

        try:
            safe_payload = make_json_safe(payload)
            sent = await self.position_ws_client.send_message(safe_payload)
            if not sent:
                logger.warning("Failed to send trade_result payload over position WebSocket")
        except Exception as exc:
            logger.error(f"Error broadcasting trade results: {exc}")
    
    async def shutdown(self):
        """Sistemi düzgün şekilde kapat"""
        logger.info("Ultra Trading System shutting down...")
        
        self.running = False

        if self.signal_ws_client:
            await self.signal_ws_client.disconnect()
        
        if self.prep_ws_client:
            await self.prep_ws_client.disconnect()

        if self.position_ws_client:
            await self.position_ws_client.disconnect()

        tracked_tasks = [
            task for task in self.tasks
            if task is not asyncio.current_task() and not task.done()
        ]
        for task in tracked_tasks:
            task.cancel()
        if tracked_tasks:
            await asyncio.gather(*tracked_tasks, return_exceptions=True)
        self.ws_loop_task = None

        if hasattr(self.mexc_trader, "shutdown"):
            try:
                await self.mexc_trader.shutdown()
            except Exception as exc:
                logger.debug("MEXC trader shutdown error: %s", exc)
        
        gc.enable()
        
        logger.info("Ultra Trading System shut down")
    
    async def ws_listen_loop(self):
        """WebSocket dinleme döngüsü"""
        try:
            while self.running:
                if self.signal_ws_client and self.signal_ws_client.connected:
                    try:
                        message = await self.signal_ws_client.receive_message(timeout=0.5)
                        if message:
                            if isinstance(message, dict) and message.get('type') == 'signal':
                                src = message.get('exchange') or message.get('source') or 'unknown'
                                tokens_list = message.get('tokens') or []
                                timestamp = message.get('timestamp') or message.get('notice_id') or ''
                                signal_id = f"{src}_{','.join(tokens_list)}_{timestamp}"
                                if signal_id in self.processed_signals:
                                    logger.info(f"Signal already processed, skipping: {signal_id}")
                                    continue
                                
                                self._remember_processed_signal(signal_id)
                                logger.info(f"NEW SIGNAL RECEIVED: {message}")
                                
                                await self.create_tracked_task(
                                    self.handle_listing_signal(message),
                                    label=f"handle-listing-signal:{signal_id}",
                                )
                            elif isinstance(message, dict) and message.get('type') == 'heartbeat':
                                self.last_heartbeat_time = time.time()
                    except Exception as e:
                        logger.error(f"WebSocket listen error: {e}")
                        self.signal_ws_client.connected = False
                else:
                    if self.signal_ws_client:
                        await self.signal_ws_client.connect()
                    await asyncio.sleep(1)
                    
                current_time = time.time()
                if current_time - self.last_heartbeat_time > self.heartbeat_interval:
                    await self.send_heartbeat()
                
                await asyncio.sleep(0.05)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"WebSocket listen loop error: {e}")
            logger.debug(traceback.format_exc())
    
    async def send_heartbeat(self):
        """Signal service'e heartbeat gönder"""
        try:
            if self.signal_ws_client and self.signal_ws_client.connected:
                status_data = {
                    "trades_completed": self.stats['total_trades'],
                    "successful_trades": self.stats['successful_trades'],
                    "binance_accounts": len(self.binance_trader.accounts),
                    "multi_account_stats": self.multi_account_stats
                }
                
                result = await self.signal_ws_client.send_heartbeat(status_data)
                if result:
                    self.last_heartbeat_time = time.time()
                    return True
                else:
                    logger.warning("Heartbeat send error")
                    return False
            else:
                logger.warning("Heartbeat could not be sent: No WebSocket connection")
                return False
        except Exception as e:
            logger.warning(f"Heartbeat send error: {e}")
            return False
    
    async def get_prepared_data(self, token):
        """Hazırlık sisteminden veri al"""
        token_key = token.upper()
        if token in self.preload_trade_params:
            return self.preload_trade_params[token]

        cached_prepared = self.prepared_cache.get(token_key)
        if cached_prepared:
            return cached_prepared

        if not self.prep_ws_enabled:
            return {}
        
        try:
            if not self.prep_ws_client:
                return {}

            if not self.prep_ws_client.connected:
                connected = await self.prep_ws_client.connect()
                if not connected:
                    logger.warning(f"Preparation service connection could not be established")
                    return {}
            
            request = {
                "type": "request_data",
                "coin": token,
                "exchange": "all",
                "timestamp": int(time.time() * 1000)
            }
            
            sent = await self.prep_ws_client.send_message(request)
            if not sent:
                logger.warning(f"Preparation data request could not be sent")
                return {}
                
            try:
                response = await asyncio.wait_for(self.prep_ws_client.receive_message(), timeout=0.05)
                if response and isinstance(response, dict) and response.get('type') == 'prepared_data':
                    data = response.get('data', {}) or {}
                    self.preload_trade_params[token] = data
                    if data:
                        self.prepared_cache[token_key] = data
                        self._persist_prepared_cache_if_needed()
                    return data
                else:
                    return {}
            except asyncio.TimeoutError:
                return {}
                
        except Exception as e:
            logger.debug("Preparation lookup failed for %s: %s", token, e)
            return {}
    
    async def _refresh_affected_balances(self, exchanges):
        """İşlem yapılan borsaların bakiyelerini güncelle"""
        refresh_tasks = []
        
        for exchange in exchanges:
            refresh_tasks.append(self._get_balance_from_trader(exchange, force=True))
        
        if refresh_tasks:
            results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
            
            updated_count = 0
            for i, exchange in enumerate(exchanges):
                if i < len(results) and not isinstance(results[i], Exception):
                    if exchange == "binance":
                        old_balances = self.balances.get(exchange, {})
                        new_balances = results[i]
                        self.balances[exchange] = new_balances
                        
                        for acc_name, new_balance in new_balances.items():
                            old_balance = old_balances.get(acc_name, 0)
                            change = new_balance - old_balance
                            change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
                            logger.info(f"Binance {acc_name} balance updated: {new_balance:.2f} USDT ({change_str} USDT)")
                        updated_count += 1
                    else:
                        old_balance = self.balances.get(exchange, 0)
                        self.balances[exchange] = results[i]
                        change = results[i] - old_balance
                        change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
                        logger.info(f"{exchange.upper()} balance updated: {results[i]:.2f} USDT ({change_str} USDT)")
                        updated_count += 1
            
            if updated_count > 0:
                logger.info(f"{updated_count} exchange balances successfully updated")
        
        return len(refresh_tasks)

    def _remember_processed_signal(self, signal_id: str) -> None:
        self.processed_signals.pop(signal_id, None)
        self.processed_signals[signal_id] = time.time()
        self._prune_processed_signals(self._processed_signal_cache_max)

    def _prune_processed_signals(self, target_size: Optional[int] = None) -> None:
        if not self.processed_signals:
            return

        target = target_size if target_size is not None else self._processed_signal_cache_trim
        target = max(10, min(target, self._processed_signal_cache_max))

        while len(self.processed_signals) > target:
            oldest_signal_id = next(iter(self.processed_signals))
            self.processed_signals.pop(oldest_signal_id, None)

    def _maybe_collect_after_signal(self) -> None:
        if self._signal_gc_threshold_mb <= 0:
            return

        now = time.time()
        if now - self._last_signal_gc < self._signal_gc_min_interval:
            return

        try:
            process_memory_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception:
            return

        if process_memory_mb < self._signal_gc_threshold_mb:
            return

        gc.collect()
        self._last_signal_gc = now

    async def check_connection_status(self):
        """Bağlantı durumunu kontrol et ve gerekirse yeniden bağlan"""
        current_time = time.time()
        total_trades = self.stats.get('total_trades', 0)

        should_log = False
        if self.last_status_log_time == 0.0:
            should_log = True
        elif total_trades != self.last_status_log_trade_count:
            should_log = True
        elif current_time - self.last_status_log_time >= self.idle_status_interval:
            should_log = True
        
        memory_usage = psutil.virtual_memory().percent
        process_memory = psutil.Process().memory_info().rss / (1024 * 1024)
        
        if should_log:
            logger.info(
                "System status: Running | Memory: %.1f%% | Process: %.1f MB | Trades: %d",
                memory_usage,
                process_memory,
                total_trades,
            )
        
        if self.signal_ws_client and not self.signal_ws_client.connected:
            logger.warning("Signal service connection lost, reconnecting...")
            await self.signal_ws_client.connect()
        
        if self.prep_ws_enabled and self.prep_ws_client and not self.prep_ws_client.connected:
            logger.warning("Preparation service connection lost, reconnecting...")
            await self.prep_ws_client.connect()
        
        if memory_usage > 80 or process_memory > 2000:
            if should_log:
                logger.warning("High memory usage: %.1f%% - Running GC", memory_usage)
            gc.collect()
        
        if len(self.processed_signals) > self._processed_signal_cache_max:
            if should_log:
                logger.info("Cleaning processed signal cache (%d records)", len(self.processed_signals))
            self._prune_processed_signals()
        
        if should_log:
            self._show_performance_stats()
            self.last_status_log_time = current_time
            self.last_status_log_trade_count = total_trades
    
    def _show_performance_stats(self):
        """Performans istatistiklerini göster"""
        logger.info("=== BINANCE MULTI-ACCOUNT PERFORMANCE ===")
        
        for acc_name, perf in self.binance_trader.performance.items():
            if acc_name == 'total':
                continue
                
            if perf['trade_count'] > 0:
                avg_time = perf['avg_trade_time'] * 1000
                min_time = perf['min_trade_time'] * 1000
                max_time = perf['max_trade_time'] * 1000
                success_rate = (perf['successful_trades'] / perf['trade_count']) * 100
                
                logger.info(f"Binance {acc_name}: Avg={avg_time:.2f}ms, Min={min_time:.2f}ms, Max={max_time:.2f}ms, "
                          f"Count={perf['trade_count']}, Success={success_rate:.1f}%")
        
        total_perf = self.binance_trader.performance['total']
        if total_perf['trade_count'] > 0:
            total_time = total_perf['total_trade_time'] * 1000
            avg_time = total_time / total_perf['trade_count']
            success_rate = (total_perf['successful_trades'] / total_perf['trade_count']) * 100
            
            logger.info(f"Binance TOTAL: {total_perf['trade_count']} trades, "
                      f"Avg={avg_time:.2f}ms, Success={success_rate:.1f}%")
        
        if self.mexc_trader.performance['trade_count'] > 0:
            avg_time = self.mexc_trader.performance['avg_trade_time'] * 1000
            min_time = self.mexc_trader.performance['min_trade_time'] * 1000
            max_time = self.mexc_trader.performance['max_trade_time'] * 1000
            
            logger.info(f"MEXC Perf: Avg={avg_time:.2f}ms, Min={min_time:.2f}ms, Max={max_time:.2f}ms, Count={self.mexc_trader.performance['trade_count']}")
        
        if self.gate_trader.performance['trade_count'] > 0:
            avg_time = self.gate_trader.performance['avg_trade_time'] * 1000
            min_time = self.gate_trader.performance['min_trade_time'] * 1000
            max_time = self.gate_trader.performance['max_trade_time'] * 1000
            
            logger.info(f"Gate.io Perf: Avg={avg_time:.2f}ms, Min={min_time:.2f}ms, Max={max_time:.2f}ms, Count={self.gate_trader.performance['trade_count']}")
    
    def _finalize_tracked_task(self, task: asyncio.Task, label: str) -> None:
        self.tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Background task failed (%s): %s", label, exc)
            logger.debug(traceback.format_exc())

    async def create_tracked_task(self, coro, *, label: str = "background-task"):
        """Takip edilen görev oluştur"""
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(
            lambda t, task_label=label: self._finalize_tracked_task(t, task_label)
        )
        return task
    
    @measure_time
    async def execute_trades(
        self,
        tokens,
        exchange=None,
        amount=None,
        exchanges_filter=None,
        balance_percentage: Union[float, Dict[str, float]] = 0.99,
    ):
        """Belirlenen tokenleri al - Market cap filtreli"""
        execute_start_time = time.perf_counter()
        
        try:
            if not tokens:
                logger.warning("Token list is empty")
                return {"success": False, "error": "Token list is empty", "results": []}
            
            if exchanges_filter:
                exchanges_to_use = exchanges_filter
            else:
                exchanges_to_use = ["binance", "mexc", "gate"]
                    
            logger.info(
                "Trade request | tokens=%s | exchanges=%s | test=%s",
                ','.join(tokens),
                ','.join(exchanges_to_use),
                "ON" if self.test_mode else "OFF",
            )
            
            exchange_amounts: Dict[str, float] = {}
            binance_account_amounts = {}
            binance_plan_segments: List[str] = []
            other_plan_segments: List[str] = []
            
            token_count = len(tokens)

            if isinstance(balance_percentage, dict):
                default_percentage = float(balance_percentage.get("default", 0.0))
                exchange_percentages = {
                    key: float(value)
                    for key, value in balance_percentage.items()
                    if key != "default"
                }
            else:
                default_percentage = float(balance_percentage)
                exchange_percentages = {}

            def _resolve_percentage(exchange_name: str) -> float:
                pct = exchange_percentages.get(exchange_name, default_percentage)
                if pct < 0.0:
                    return 0.0
                if pct > 1.0:
                    return 1.0
                return pct

            if amount is None:
                if not self.balances_loaded:
                    await self._load_initial_balances()
                
                for ex in exchanges_to_use:
                    if ex == "binance":
                        binance_balances = self.balances.get("binance", {})
                        resolved_percentage = _resolve_percentage("binance")
                        for acc_name, acc_balance in binance_balances.items():
                            available = min(acc_balance * resolved_percentage, 200.0)  # Cap at $200
                            per_token_amount = available / token_count if token_count > 0 else 0.0
                            binance_account_amounts[acc_name] = per_token_amount
                            binance_plan_segments.append(
                                f"{acc_name} {per_token_amount:.2f} (avail {available:.2f})"
                            )
                    else:
                        resolved_percentage = _resolve_percentage(ex)
                        available = min(self.balances.get(ex, 0.0) * resolved_percentage, 200.0)  # Cap at $200
                        per_token_amount = available / token_count if token_count > 0 else 0.0
                        exchange_amounts[ex] = per_token_amount
                        other_plan_segments.append(
                            f"{ex.upper()} {per_token_amount:.2f} (avail {available:.2f})"
                        )
            else:
                for ex in exchanges_to_use:
                    if ex == "binance":
                        for acc in self.binance_trader.accounts:
                            if acc['enabled']:
                                binance_account_amounts[acc['name']] = amount
                                binance_plan_segments.append(f"{acc['name']} {amount:.2f}")
                    else:
                        exchange_amounts[ex] = amount
                        other_plan_segments.append(f"{ex.upper()} {amount:.2f}")

            plan_segments: List[str] = []
            if binance_plan_segments:
                plan_segments.append("Binance: " + "; ".join(binance_plan_segments))
            if other_plan_segments:
                plan_segments.extend(other_plan_segments)
            if plan_segments:
                logger.info("Allocation per token | " + " | ".join(plan_segments))
            
            binance_price = None
            shared_binance_session = _shared_common.BINANCE_SESSION
            if len(tokens) == 1 and shared_binance_session:
                try:
                    url = f"{_shared_common._binance_rest_base_url()}/api/v3/ticker/price?symbol={tokens[0]}USDT"
                    weight = BINANCE_ENDPOINT_WEIGHTS.get("GET /api/v3/ticker/price", 1)
                    while not RATE_LIMITER.can_request('binance', weight=weight, is_order=False):
                        wait_time = RATE_LIMITER.get_wait_time('binance', weight=weight, is_order=False)
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                        else:
                            break
                    RATE_LIMITER.add_request('binance', weight=weight, is_order=False)
                    response = await asyncio.to_thread(
                        shared_binance_session.get,
                        url,
                        timeout=0.3,
                    )
                    if response.status_code == 200:
                        binance_price = float(response.json()['price'])
                        logger.info(f"Binance {tokens[0]} price: {binance_price} USDT")
                except Exception as e:
                    logger.warning(f"Could not get Binance price: {e}")
            
            tasks = []
            task_info = []
            
            price_payload = None
            if binance_price is not None:
                price_payload = {"reference_price": binance_price}
            
            for token in tokens:
                if "binance" in exchanges_to_use:
                    task = asyncio.create_task(
                        self.binance_trader.execute_trade(
                            token,
                            amount=amount,
                            per_account_amounts=binance_account_amounts if binance_account_amounts else None,
                        )
                    )
                    tasks.append(task)
                    task_info.append(("binance_multi", token))
                
                if "mexc" in exchanges_to_use:
                    ex_amount = exchange_amounts.get("mexc", 10.0)
                    if ex_amount > 0:
                        task = asyncio.create_task(
                            self.mexc_trader.execute_market_slices(
                                token,
                                ex_amount,
                                preparation_data=price_payload,
                            )
                        )
                        tasks.append(task)
                        task_info.append(("mexc", token))

                if "gate" in exchanges_to_use:
                    ex_amount = exchange_amounts.get("gate", 10.0)
                    if ex_amount > 0:
                        task = asyncio.create_task(
                            self.gate_trader.execute_market_slices(
                                token,
                                ex_amount,
                                preparation_data=price_payload,
                            )
                        )
                        tasks.append(task)
                        task_info.append(("gate", token))
            
            if tasks:
                raw_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                results = []
                for i, result in enumerate(raw_results):
                    ex, token = task_info[i]
                    
                    if isinstance(result, Exception):
                        logger.error(f"{ex} {token} trade error: {str(result)}")
                        results.append({
                            "success": False,
                            "exchange": ex,
                            "symbol": token,
                            "error": str(result),
                            "execution_time": 0
                        })
                    else:
                        if ex == "binance_multi" and 'multi_account_results' in result:
                            multi_results = result.get('multi_account_results', [])
                            for acc_result in multi_results:
                                results.append(acc_result)
                                
                                acc_name = acc_result.get('account', 'unknown')
                                if acc_name in self.multi_account_stats["by_account"]:
                                    stats = self.multi_account_stats["by_account"][acc_name]
                                    stats["trades"] += 1
                                    if acc_result.get('success'):
                                        stats["successful"] += 1
                                        stats["total_amount"] += acc_result.get('amount', 0)
                                    else:
                                        stats["failed"] += 1
                        else:
                            results.append(result)
                            if not result.get('success'):
                                failure_snapshot = {
                                    "error": result.get("error"),
                                    "reason": result.get("reason"),
                                    "rest_status": result.get("rest_status"),
                                    "ws_status": result.get("ws_status"),
                                    "confirmation": result.get("confirmation_source"),
                                    "client_order_id": result.get("client_order_id"),
                                    "order_id": result.get("order_id"),
                                    "amount": result.get("amount"),
                                    "filled_quote": result.get("filled_quote"),
                                    "filled_qty": result.get("filled_qty"),
                                    "remaining_quote": result.get("remaining_quote"),
                                    "delayed_confirmation": result.get("delayed_confirmation"),
                                    "execution_time_ms": (result.get("execution_time") or 0.0) * 1000.0,
                                }
                                logger.warning(
                                    "Trade failure detail (%s %s): %s",
                                    ex.upper(),
                                    token,
                                    json.dumps(make_json_safe(failure_snapshot)),
                                )
            else:
                results = []
            
            execute_time = time.perf_counter() - execute_start_time
            
            success_count = sum(1 for r in results if r.get('success', False))
            
            self.stats['total_trades'] += len(results)
            self.stats['successful_trades'] += success_count
            self.stats['failed_trades'] += (len(results) - success_count)
            
            self.multi_account_stats["total_binance_trades"] += len([r for r in results if r.get('exchange') == 'binance'])
            self.multi_account_stats["successful_binance_trades"] += len([r for r in results if r.get('exchange') == 'binance' and r.get('success')])
            
            max_exec_time = 0.0
            if results:
                max_exec_time = max((r.get('execution_time') or 0.0) for r in results)

            total_failures = len(results) - success_count
            logger.info(
                "Trade summary | tokens=%d | trades=%d (%d ok / %d fail) | wall %.2fms | slowest leg %.2fms",
                len(tokens),
                len(results),
                success_count,
                total_failures,
                execute_time * 1000,
                max_exec_time * 1000,
            )
            
            if results:
                logger.info("Exchange breakdown:")
                exchange_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for entry in results:
                    exchange_groups[entry.get('exchange', 'unknown')].append(entry)
                
                for exchange in sorted(exchange_groups.keys()):
                    group = exchange_groups[exchange]
                    success_entries = [g for g in group if g.get('success')]
                    failure_entries = [g for g in group if not g.get('success')]
                    logger.info(
                        "  %s | legs=%d | success=%d | fail=%d",
                        exchange.upper(),
                        len(group),
                        len(success_entries),
                        len(failure_entries),
                    )
                    for idx, entry in enumerate(group, 1):
                        symbol = entry.get('symbol', 'unknown')
                        account = entry.get('account')
                        amount = entry.get('amount', entry.get('filled_quote', 0.0))
                        try:
                            amount_val = float(amount) if amount is not None else 0.0
                        except (TypeError, ValueError):
                            amount_val = 0.0
                        exec_time_ms = (entry.get('execution_time') or 0.0) * 1000.0
                        status_icon = "✓" if entry.get('success') else "✗"
                        account_label = f" ({account})" if account else ""
                        if entry.get('success'):
                            logger.info(
                                "    %s #%d %s%s %.2f USDT @ %.2fms",
                                status_icon,
                                idx,
                                symbol,
                                account_label,
                                amount_val,
                                exec_time_ms,
                            )
                        else:
                            reason = (
                                entry.get('error')
                                or entry.get('reason')
                                or entry.get('rest_status')
                                or entry.get('ws_status')
                                or "unknown_error"
                            )
                            extra_bits = []
                            if entry.get('rest_status'):
                                extra_bits.append(f"rest={entry.get('rest_status')}")
                            if entry.get('ws_status'):
                                extra_bits.append(f"ws={entry.get('ws_status')}")
                            if entry.get('confirmation_source'):
                                extra_bits.append(f"confirmation={entry.get('confirmation_source')}")
                            if entry.get('client_order_id'):
                                extra_bits.append(f"client={entry.get('client_order_id')}")
                            if entry.get('order_id'):
                                extra_bits.append(f"order={entry.get('order_id')}")
                            extra_suffix = f" [{', '.join(extra_bits)}]" if extra_bits else ""
                            logger.info(
                                "    %s #%d %s%s error=%s%s",
                                status_icon,
                                idx,
                                symbol,
                                account_label,
                                reason,
                                extra_suffix,
                            )
            
            if success_count > 0 and not self.test_mode:
                affected_exchanges = set([result.get('exchange') for result in results if result.get('success', False)])
                if affected_exchanges:
                    await self._refresh_affected_balances(affected_exchanges)
            
            summary = {
                "success": success_count > 0,
                "total": len(results),
                "successful": success_count,
                "failed": len(results) - success_count,
                "results": results,
                "execution_time": execute_time,
                "longest_leg": max_exec_time,
            }
            await self._broadcast_trade_results(tokens, summary)
            return summary
        except Exception as e:
            execute_time = time.perf_counter() - execute_start_time
            logger.error(f"Trade error: {str(e)}")
            logger.debug(traceback.format_exc())
            
            return {
                "success": False,
                "error": str(e),
                "execution_time": execute_time
            }
    
    async def handle_listing_signal(self, signal_data):
        """Listing sinyalini işle ve işlem yap - MARKET CAP FİLTRELİ"""
        start_time = time.perf_counter()
        
        logger.info(f"LISTING SIGNAL PROCESSING: {signal_data}")
        self._last_signal_payload = dict(signal_data)
        
        try:
            source_value = signal_data.get("exchange") or signal_data.get("source") or ""
            exchange = source_value.lower()
            is_binance_alpha = exchange == "binance_alpha"
            is_upbit_source = exchange.startswith("upbit")
            tokens = signal_data.get("tokens", [])

            upbit_krw_restriction = False

            if is_binance_alpha and not self.allow_binance_alpha_signals:
                logger.info("⏸ Binance Alpha signal received but disabled; skipping.")
                return

            if is_upbit_source:
                if not self.allow_upbit_listings:
                    logger.info("⏸ Upbit listing received but ALLOW_UPBIT_LISTINGS=false; skipping.")
                    return
                else:
                    logger.info("✅ Upbit listing allowed by configuration (ALLOW_UPBIT_LISTINGS=true).")
                    upbit_krw_restriction = not self.allow_upbit_krw_listings
            
            if exchange == "bithumb" and not self.allow_bithumb_listings:
                logger.info("⏸ Bithumb listing received but ALLOW_BITHUMB_LISTINGS=false; skipping.")
                return

            if exchange == "binance" and not self.allow_binance_listings:
                logger.info("⏸ Binance listing received but ALLOW_BINANCE_LISTINGS=false; skipping.")
                return

            if exchange == "investment" and not self.allow_investment_signals:
                logger.info("⏸ Investment signal received but ALLOW_INVESTMENT_SIGNALS=false; skipping.")
                return

            if not tokens:
                logger.warning("Token list is empty, no trades will be made")
                return
            
            alert_type = (signal_data.get("alert_type") or "").lower()
            markets = signal_data.get("markets", [])
            announcement = signal_data.get("announcement", "")
            listing_type = (signal_data.get("listing_type") or "").upper()
            has_krw_market = bool(
                signal_data.get("has_krw_market", False)
                or listing_type == "KRW"
                or any(str(m or "").upper() == "KRW" for m in markets)
                or "KRW" in announcement.upper()
            )

            if is_upbit_source and not has_krw_market:
                logger.info("⏸ Upbit listing without KRW market; only KRW listings are enabled; skipping.")
                return

            if is_upbit_source and upbit_krw_restriction and has_krw_market:
                logger.info("⏸ Upbit KRW listing received but ALLOW_UPBIT_KRW_LISTINGS=false; skipping.")
                return
            
            announcement_aliases = _extract_listing_aliases(announcement)

            # MARKET CAP KONTROLÜ - $400M ÜSTÜYSEYSEYİŞLEM AÇMA
            filtered_tokens = []
            for token in tokens:
                alias_candidates = announcement_aliases.get(token.upper(), [])
                market_cap = CMC_API.get_market_cap(token, aliases=alias_candidates)
                
                if market_cap is not None and market_cap > 400_000_000:
                    logger.warning(f"🚫 SKIPPING {token} - Market cap ${market_cap/1_000_000:.0f}M > $400M threshold")
                    continue
                else:
                    if market_cap is not None:
                        logger.info(f"✅ {token} market cap ${market_cap/1_000_000:.0f}M - below threshold, proceeding")
                    else:
                        logger.info(f"✅ {token} market cap unknown - proceeding with trade")
                    filtered_tokens.append(token)
            
            # Eğer hiç token kalmadıysa işlem yapma
            if not filtered_tokens:
                logger.warning(f"All tokens filtered out due to market cap > $400M")
                return
            
            # Filtrelenmiş token listesini kullan
            tokens = filtered_tokens
            
            # Strateji belirleme
            if is_binance_alpha:
                logger.info("🛰️ Binance Alpha signal detected - allocating up to 50% on MEXC & Gate")

                if not self.balances_loaded:
                    await self._load_initial_balances()

                trade_amount = 10.0 if self.test_mode else None
                exchanges_to_use = ["mexc", "gate"]
                balance_percentage = 0.20

            elif is_upbit_source and has_krw_market:
                logger.info(f"🔥 KRW MARKET TOKEN DETECTED! Markets: {markets}")

                if not self.balances_loaded:
                    await self._load_initial_balances()

                trade_amount = 50.0 if self.test_mode else None

                # UPBIT KRW İÇİN SADECE GATE (HARDCODE)
                # Binance ve MEXC kapalı - Gate daha hızlı listing yapıyor
                exchanges_to_use = ["gate"]
                logger.info("KRW exchange plan: GATE ONLY (hardcoded for Upbit KRW)")
                balance_percentage = 0.99

                logger.info(f"KRW token strategy: MAXIMUM AMOUNT (99%) ON GATE ONLY")
                
            elif alert_type in {"listing_no_krw", "listing"}:
                logger.info(f"💡 Non-KRW market token. Markets: {markets}")
                
                if self.test_mode:
                    trade_amount = 20.0
                else:
                    trade_amount = None
                
                if exchange == "binance":
                    exchanges_to_use = ["mexc", "gate"]
                    logger.info("💡 Binance signal detected - No trades on Binance")
                else:
                    exchanges_to_use = ["binance", "mexc", "gate"]
                
                balance_percentage = 0.95
                
                logger.info("Non-KRW token strategy: All exchanges 95% balance target")
                
            elif alert_type in ["roadmap", "pre_announcement"]:
                trade_amount = 10.0 if self.test_mode else 15.0
                
                if exchange == "binance":
                    exchanges_to_use = ["mexc", "gate"]
                    logger.info("🔧 Binance signal detected - No trades on Binance")
                else:
                    exchanges_to_use = ["binance"]
                
                balance_percentage = 0.99
                logger.info(f"Roadmap/Pre-announcement signal - minimal amount")
                
            else:
                logger.info(f"Unknown signal type: {alert_type}, applying default strategy")
                trade_amount = None
                
                if exchange == "binance":
                    exchanges_to_use = ["mexc", "gate"]
                    logger.info("⚠️ Binance signal detected - No trades on Binance")
                else:
                    exchanges_to_use = ["binance", "mexc", "gate"]
                
                balance_percentage = 0.99
            
            # Saatlik bakiye yenileme kontrolü
            current_time = time.time()
            if current_time - self.last_balance_refresh > self.balance_refresh_interval:
                logger.info("Hourly balance refresh time has come")
                await self._refresh_all_balances()
                self.last_balance_refresh = current_time
            
            # İşlemleri başlat
            logger.info(f"Starting trades - Tokens: {tokens}, Exchanges: {exchanges_to_use}")

            if self.execution_jitter_ms > 0:
                delay = random.uniform(0, self.execution_jitter_ms) / 1000.0
                logger.debug("Execution jitter: sleeping %.3f seconds before trades", delay)
                await asyncio.sleep(delay)
            
            result = await self.execute_trades(
                tokens, 
                exchange=None,
                amount=trade_amount,
                exchanges_filter=exchanges_to_use,
                balance_percentage=balance_percentage
            )
            
            # KRW istatistiklerini güncelle
            if has_krw_market or alert_type == "listing_krw":
                self.krw_stats["krw_trades"] += result.get("total", 0)
                self.krw_stats["krw_successful"] += result.get("successful", 0)
                self.krw_stats["krw_failed"] += result.get("failed", 0)
                
                for r in result.get("results", []):
                    if r.get("success"):
                        self.krw_stats["krw_total_amount"] += r.get("amount", 0)
            else:
                self.krw_stats["non_krw_trades"] += result.get("total", 0)
                self.krw_stats["non_krw_successful"] += result.get("successful", 0)
                self.krw_stats["non_krw_failed"] += result.get("failed", 0)
                
                for r in result.get("results", []):
                    if r.get("success"):
                        self.krw_stats["non_krw_total_amount"] += r.get("amount", 0)
            
            # Sonuçları logla
            signal_time = time.perf_counter() - start_time
            
            if result.get("success"):
                krw_status = "KRW" if has_krw_market else "NO-KRW"
                source_info = f"[{exchange.upper()}]" if exchange else "[UNKNOWN]"
                logger.info(f"✅ {source_info} [{krw_status}] LISTING SIGNAL SUCCESSFULLY PROCESSED - {result.get('successful', 0)}/{result.get('total', 0)} trades successful, total time: {signal_time*1000:.2f}ms")
                
                if "binance" in exchanges_to_use:
                    binance_results = [r for r in result.get('results', []) if r.get('exchange') == 'binance']
                    if binance_results:
                        logger.info(f"Binance Multi-Account Detail: {len(binance_results)} trades")
                        for acc_name in self.binance_trader.accounts:
                            acc_results = [r for r in binance_results if r.get('account') == acc_name['name']]
                            if acc_results:
                                success = len([r for r in acc_results if r.get('success')])
                                total = len(acc_results)
                                logger.info(f"  - {acc_name['name']}: {success}/{total} successful")
            else:
                source_info = f"[{exchange.upper()}]" if exchange else "[UNKNOWN]"
                logger.error(f"❌ {source_info} LISTING SIGNAL PROCESSING ERROR - Error: {result.get('error', 'Unknown error')}")
            
            # Signal service'e sonuç bildirimi gönder
            if self.signal_ws_client and self.signal_ws_client.connected:
                report_msg = {
                    "type": "trade_report",
                    "signal_id": signal_data.get("id", ""),
                    "tokens": tokens,
                    "source_exchange": exchange,
                    "has_krw_market": has_krw_market,
                    "markets": markets,
                    "results": result.get("results", []),
                    "total_execution_time": signal_time,
                    "timestamp": int(time.time() * 1000),
                    "multi_account_stats": self.multi_account_stats
                }
                await self.signal_ws_client.send_message(make_json_safe(report_msg))
            
            # Başarılı işlemler için pozisyon bildirimi
            if result.get("success"):
                await self.notify_new_positions(result, signal_data)
            
            # KRW istatistiklerini göster
            logger.info(f"KRW Statistics - KRW: {self.krw_stats['krw_successful']}/{self.krw_stats['krw_trades']} (${self.krw_stats['krw_total_amount']:.2f}), Non-KRW: {self.krw_stats['non_krw_successful']}/{self.krw_stats['non_krw_trades']} (${self.krw_stats['non_krw_total_amount']:.2f})")
            
        except Exception as e:
            logger.error(f"Listing signal processing error: {str(e)}")
            logger.debug(traceback.format_exc())
        finally:
            self._maybe_collect_after_signal()
            
            signal_time = time.perf_counter() - start_time
            logger.info(f"Signal processing time: {signal_time*1000:.2f}ms")

    async def notify_new_positions(self, trade_results, signal_data):
        """İşlem sonuçlarını bildir"""
        try:
            if not self.signal_ws_client or not self.signal_ws_client.connected:
                await self.signal_ws_client.connect()
                
            for result in trade_results.get('results', []):
                if result.get('success'):
                    try:
                        position_data = {
                            "type": "position_update",
                            "symbol": result.get('symbol'),
                            "exchange": result.get('exchange'),
                            "account": result.get('account', ''),
                            "listing_source": signal_data.get('source') or signal_data.get('exchange', 'unknown'),
                            "timestamp": datetime.now().isoformat()
                        }
                        
                        if 'order_result' in result:
                            if result.get('exchange') == 'binance':
                                position_data["order_id"] = result['order_result'].get('orderId', '')
                            else:
                                position_data["order_id"] = result['order_result'].get('id', '')
                        
                        position_data["amount"] = result.get('amount', 0)
                        
                        await self.signal_ws_client.send_message(make_json_safe(position_data))
                    except Exception as e:
                        logger.error(f"Position notification error: {str(e)}")
        except Exception as e:
            logger.error(f"Position notification general error: {str(e)}")

    async def run(self):
        """Ana döngü - sinyal dinle ve işlem yap"""
        logger.info("Ultra Trading System running, waiting for signals...")
        logger.info(f"Market Cap Filter: Active - Tokens > $400M will be SKIPPED")
        logger.info(f"Binance Multi-Account: {len(self.binance_trader.accounts)} accounts active")
        
        if RUN_TEST_SIGNAL:
            logger.info("Running test signal for TAO...")
            await self.handle_listing_signal(TAO_TEST_SIGNAL)
            logger.info("Test signal executed, continuing normal operation")
        
        try:
            while self.running:
                try:
                    if time.time() - self.status_check_time > self.status_check_interval:
                        await self.check_connection_status()
                        self.status_check_time = time.time()
                    
                    current_time = time.time()
                    if current_time - self.last_balance_refresh > self.balance_refresh_interval:
                        logger.info("Starting hourly balance update...")
                        await self._refresh_all_balances()
                        self.last_balance_refresh = current_time
                    
                    await asyncio.sleep(0.05)
                    
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt detected, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Main loop unexpected error: {str(e)}")
                    logger.debug(traceback.format_exc())
                    await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected, shutting down...")
        except Exception as e:
            logger.error(f"Main loop unexpected error: {str(e)}")
            logger.debug(traceback.format_exc())
        finally:
            await self.shutdown()

# -----------------------------------------------------------------------------
# ANA PROGRAM BLOĞU - UBUNTU EDITION V3.2
# -----------------------------------------------------------------------------

async def main():
    """Ana işlev - UBUNTU EDITION V3.2 - Multi-Account + Market Cap Filter"""
    print(f"\n{Fore.GREEN}{'=' * 80}")
    print(f"Ultra Fast Crypto Trading System - UBUNTU EDITION V3.2")
    print(f"Multi-Account Support + Market Cap Filter ($300M)")
    print(f"Optimized for Ubuntu/Linux Systems")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target Latency: <200ms total (with all optimizations)")
    print(f"Market Cap Filter: Tokens > $400M will be SKIPPED")
    print(f"{'=' * 80}{Style.RESET_ALL}\n")
    
    # Market cap data kontrolü
    if not CMC_API.coin_data:
        print(f"{Fore.YELLOW}WARNING: No market cap data loaded. var/coinmarketcap_data.json file missing or empty.")
        print(f"Market cap filtering will be disabled.{Style.RESET_ALL}")
    else:
        print(f"{Fore.CYAN}Market cap data loaded: {len(CMC_API.coin_data)} coins tracked{Style.RESET_ALL}")
    
    # UltraTrader örneği oluştur
    trader = UltraTrader(test_mode=False)  # Test modu varsayılan olarak kapalı
    
    try:
        # Sistemi başlat
        await trader.initialize()
        
        # Multi-account bilgilerini göster
        if trader.binance_trader.accounts:
            print(f"\n{Fore.CYAN}=== BINANCE MULTI-ACCOUNT CONFIGURATION ==={Style.RESET_ALL}")
            for acc in trader.binance_trader.accounts:
                print(f"  • {acc['name']}: {Fore.GREEN}ACTIVE{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{'=' * 45}{Style.RESET_ALL}\n")
        
        # Çalıştır
        await trader.run()
        
    except Exception as e:
        logger.error(f"Program error: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Sistemi temiz bir şekilde kapat
        if hasattr(trader, 'running') and trader.running:
            await trader.shutdown()

# -----------------------------------------------------------------------------
# PROGRAM ENTRY POINT
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Linux için event loop ayarları (Ubuntu için optimize edilmiş)
    if sys.platform == 'linux':
        # Linux'ta varsayılan event loop zaten optimal
        # Sadece uvloop varsa kullan (daha hızlı)
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print(f"{Fore.GREEN}Using uvloop for better performance{Style.RESET_ALL}")
        except ImportError:
            # uvloop yoksa varsayılan event loop kullan
            pass
    
    # Ana program döngüsünü başlat
    try:
        # Çalışma zamanı istatistikleri için başlangıç zamanı
        start_time = time.time()
        
        # Ana programı çalıştır
        asyncio.run(main())
        
        # Çalışma süresi istatistiği
        runtime = time.time() - start_time
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        seconds = int(runtime % 60)
        
        print(f"\n{Fore.CYAN}{'=' * 80}")
        print(f"Program terminated successfully")
        print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")
        print(f"{'=' * 80}{Style.RESET_ALL}")
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Keyboard interrupt detected, shutting down gracefully...{Style.RESET_ALL}")
    except Exception as e:
        print(f"\n{Fore.RED}Unexpected error: {str(e)}{Style.RESET_ALL}")
        traceback.print_exc()
    finally:
        # Temizlik işlemleri
        try:
            # Event loop'u temizle
            loop = asyncio.get_event_loop()
            if loop and not loop.is_closed():
                loop.close()
        except:
            pass
        
        print(f"\n{Fore.GREEN}Shutdown complete. Goodbye!{Style.RESET_ALL}")
