from .common import *
from .position import Position
from .exchanges import BalanceSnapshot, BinanceAPI, GateAPI, GateAPIHardError, GateAPITemporaryError, MEXCAPI
from .strategies import (
    AggressiveMomentumBinanceStrategy,
    BinanceAlphaListingStrategy,
    BinanceListingStrategy,
    BithumbListingStrategy,
    UpbitListingStrategy,
)
from .market import CoinMarketCapAPI


class PositionManagerCoreMixin:
    def __init__(
        self,
        test_mode: bool = False,
        *,
        enabled_exchanges: Optional[Sequence[str]] = None,
        binance_use_testnet: Optional[bool] = None,
        position_ws_enabled: Optional[bool] = None,
    ):
        self.enabled_exchanges = self._parse_enabled_exchanges(enabled_exchanges)
        self.binance_use_testnet = (
            self._env_truthy(os.getenv("BINANCE_USE_TESTNET"))
            if binance_use_testnet is None
            else bool(binance_use_testnet)
        )
        self.binance_key_type = self._resolve_binance_key_type(self.binance_use_testnet)
        self.position_ws_enabled = (
            not self._env_truthy(os.getenv("POSITION_WS_DISABLED"))
            if position_ws_enabled is None
            else bool(position_ws_enabled)
        )

        # Primary Binance API kimlik bilgileri
        self.binance_api_key = None
        self.binance_api_secret = None
        self.binance_testnet_credential_source = None
        if "binance" in self.enabled_exchanges:
            if self.binance_use_testnet:
                testnet_key = os.getenv("BINANCE_TESTNET_API_KEY")
                testnet_secret = os.getenv("BINANCE_TESTNET_API_SECRET")
                default_key = os.getenv("BINANCE_API_KEY")
                default_secret = os.getenv("BINANCE_API_SECRET")
                if testnet_key and (self.binance_key_type != "hmac" or testnet_secret):
                    self.binance_api_key = testnet_key
                    self.binance_api_secret = testnet_secret or ""
                    self.binance_testnet_credential_source = "BINANCE_TESTNET_*"
                else:
                    self.binance_api_key = default_key
                    self.binance_api_secret = default_secret or ""
                    self.binance_testnet_credential_source = "BINANCE_*"
                if self.binance_key_type != "hmac":
                    self.binance_api_secret = self.binance_api_secret or ""
            else:
                self.binance_api_key = os.getenv("BINANCE_API_KEY")
                self.binance_api_secret = os.getenv("BINANCE_API_SECRET")
                if self.binance_key_type != "hmac":
                    self.binance_api_secret = self.binance_api_secret or ""

        self.active_account_label = (
            os.getenv("ACCOUNT_LABEL")
            or "PRIMARY"
        )

        # Diğer borsa API bilgileri (varsayılan olarak ortamdan)
        self.gate_api_key = os.getenv("GATE_API_KEY") if "gate" in self.enabled_exchanges else None
        self.gate_api_secret = os.getenv("GATE_API_SECRET") if "gate" in self.enabled_exchanges else None
        self.mexc_api_key = os.getenv("MEXC_API_KEY") if "mexc" in self.enabled_exchanges else None
        self.mexc_api_secret = os.getenv("MEXC_API_SECRET") if "mexc" in self.enabled_exchanges else None

        self.exchange_local_ips: Dict[str, Optional[str]] = {
            "binance": self._normalise_ip(os.getenv("BINANCE_LOCAL_IP")),
            "gate": self._normalise_ip(os.getenv("GATE_LOCAL_IP")),
            "mexc": self._normalise_ip(os.getenv("MEXC_LOCAL_IP")),
        }

        self.primary_local_ip = self._select_local_ip()
        if self.primary_local_ip:
            logger.info(
                "Using configured local IP %s for outbound exchange traffic (%s)",
                self.primary_local_ip,
                self.active_account_label,
            )
        distinct_local_ips = {ip for ip in self.exchange_local_ips.values() if ip}
        if len(distinct_local_ips) > 1 and self.primary_local_ip:
            logger.warning(
                "Multiple local IP assignments detected (%s); HTTP session will bind to %s",
                distinct_local_ips,
                self.primary_local_ip,
            )

        # Diğer Binance hesapları devre dışı bırakıldı; yalnızca aktif hesap kullanılacak
        self.binance_accounts = {}
        if self.binance_api_key and self.binance_api_secret:
            self.binance_accounts[self.active_account_label] = {
                "api_key": self.binance_api_key,
                "api_secret": self.binance_api_secret,
            }
        else:
            logger.warning("Binance API credentials missing for Position Manager")

        missing = []
        if "binance" in self.enabled_exchanges:
            if not self.binance_api_key:
                missing.append("BINANCE_API_KEY")
            if self._binance_secret_required() and not self.binance_api_secret:
                missing.append("BINANCE_API_SECRET")
        if "gate" in self.enabled_exchanges and (not self.gate_api_key or not self.gate_api_secret):
            missing.extend(["GATE_API_KEY", "GATE_API_SECRET"])
        if "mexc" in self.enabled_exchanges and (not self.mexc_api_key or not self.mexc_api_secret):
            missing.extend(["MEXC_API_KEY", "MEXC_API_SECRET"])

        if missing:
            raise ValueError(f"Missing API credentials: {', '.join(missing)}")

        self.session = None
        self.binance_apis = {}  # account_name -> BinanceAPI instance
        self.gate_api = None
        self.mexc_api = None
        self.cmc_api = None
        
        # Test modu kontrolü
        self.test_mode = test_mode
        
        # Pozisyon yönetimi
        self.positions = {}  # "symbol_exchange_account" -> Position
        self.active_positions = set()  # Set of "symbol_exchange_account" keys
        self.symbol_positions = {}  # symbol -> ["symbol_exchange_account", ...]
        self.market_conditions = {}  # Piyasa koşulları değerlendirmesi

        self.retry_status = {}
        
        # ÖNEMLİ: last_price_update dictionary'sini ekle
        self.last_price_update = {}  # {exchange_symbol: timestamp}
        self.last_manual_check = {}  # Manuel fiyat kontrolü için
        self.gate_balance_check_interval = float(os.getenv("GATE_BALANCE_CHECK_INTERVAL", "30"))
        self.last_gate_balance_check: Dict[str, float] = {}
        self._gate_balance_snapshot_cache: Dict[str, BalanceSnapshot] = {}
        try:
            self.gate_balance_retry_attempts = max(1, int(os.getenv("GATE_BALANCE_RETRY_ATTEMPTS", "3")))
        except ValueError:
            self.gate_balance_retry_attempts = 3
        try:
            self.gate_balance_retry_delay = float(os.getenv("GATE_BALANCE_RETRY_DELAY", "0.75"))
        except ValueError:
            self.gate_balance_retry_delay = 0.75

        # WebSocket yapılandırması (pozisyon bildirimi için)
        default_position_ws = os.getenv("POSITION_WS_URI") or os.getenv("SIGNAL_WS_URL") or "ws://localhost:9999/ws"
        self.position_ws_uri = default_position_ws
        self.position_ws = None
        self.position_ws_connected = False
        self.position_ws_reconnect_delay = 5
        
        # İzleme durumu
        self.position_check_interval = 0.1  # Daha hızlı kontrol için 100ms
        self.market_check_interval = 60     # saniye
        
        # Fiyat kontrolü optimizasyonu
        self.price_cache = {}  # {symbol: (price, timestamp)}
        self.price_cache_ttl = 0.5  # saniye

        # Trade confirmation cache (symbol, exchange, account) -> {timestamp, amount}
        self.trade_confirmations: Dict[tuple, dict] = {}
        self.trade_confirmation_ttl = int(os.getenv("TRADE_CONFIRMATION_TTL", "180"))

        # Allow creating positions even if no trade confirmation arrived (fallback when relay fails)
        self.allow_unconfirmed_positions = os.getenv("ALLOW_UNCONFIRMED_POSITIONS", "true").lower() != "false"

        # Investment signal handling parameters (seconds / percent)
        self.investment_trailing_delay = float(os.getenv("INVESTMENT_TRAILING_DELAY", "30"))
        self.investment_trailing_percent = float(os.getenv("INVESTMENT_TRAILING_PERCENT", "5"))
        self.investment_force_exit_delay = float(os.getenv("INVESTMENT_FORCE_EXIT", "180"))
        self.investment_tasks: Dict[str, Dict[str, asyncio.Task]] = {}

        # Stratejiler
        self.strategies = {
            "binance": AggressiveMomentumBinanceStrategy(),
            "binance_legacy": BinanceListingStrategy(),
            "binance_alpha": BinanceAlphaListingStrategy(),
            "bithumb": BithumbListingStrategy(),
            "upbit": UpbitListingStrategy(),
        }

        self._source_aliases = {
            "upbit_test": "upbit",
            "upbit_krw": "upbit",
            "upbit": "upbit",
            "binance_announcements": "binance",
            "binance_announcement": "binance",
            "binance_us": "binance",
            "binanceus": "binance",
            "binance_alpha": "binance_alpha",
            "binance_alpha_test": "binance_alpha",
            "binancealpha": "binance_alpha",
            "binancealpha_test": "binance_alpha",
            "binance-alpha": "binance_alpha",
            "binance-alpha_test": "binance_alpha",
            "bithumb_test": "bithumb",
            "cb": "coinbase",
            "coinbase_pro": "coinbase",
        }
        
        # İstatistik takibi
        self.trade_stats = {
            "total_trades": 0,
            "successful_trades": 0,  # Kârla kapatılan
            "total_profit_usdt": 0,
            "average_profit_pct": 0,
            "best_trade": {"symbol": None, "profit_pct": 0},
            "worst_trade": {"symbol": None, "profit_pct": float('inf')},
        }
        
        # Trade sonuçları için klasör yapısı
        self.results_dir = "var/trading_results"
        self.trade_counter_file = os.path.join(self.results_dir, "trade_counter.json")
        self.processed_signals_file = os.path.join(self.results_dir, "processed_signals.json")
        self.current_trade_id = 0
        
        # Processed signals cache - (coin, exchange) -> timestamp (float)
        self.processed_signals: Dict[Tuple[str, str], float] = {}
        self.processed_signals_ttl = 86400 * 7  # 7 days TTL
        self._last_processed_signals_prune = 0.0

        # Test signal handling
        self.test_auto_exit_delay = float(os.getenv("TEST_AUTO_EXIT_DELAY", "15"))
        self.test_auto_exit_timeout = float(os.getenv("TEST_AUTO_EXIT_TIMEOUT", "90"))
        self.test_auto_exit_qty_factor = float(os.getenv("TEST_AUTO_EXIT_QTY_FACTOR", "0.995"))
        self.test_exit_tasks: Dict[str, asyncio.Task] = {}
        self.gate_balance_guard_cooldown_seconds = float(os.getenv("GATE_BALANCE_GUARD_COOLDOWN", "15"))
        self.pending_signal_checks: Dict[Tuple[str, str], asyncio.Task] = {}
        self.position_check_delay = float(os.getenv("POSITION_CHECK_DELAY_SECONDS", "4.5"))
        self.position_check_max_retries = max(1, int(os.getenv("POSITION_CHECK_MAX_RETRIES", "3")))
        self.position_check_retry_interval = float(os.getenv("POSITION_CHECK_RETRY_INTERVAL_SECONDS", "1.2"))
        
        # Klasörü oluştur ve trade sayacını yükle
        self._setup_results_directory()
        self._load_trade_counter()
        self._load_processed_signals()
        
        # Fiyat akışı başlatma görevleri
        self.price_stream_tasks = []
        
        # CoinMarketCap API
        self.cmc_api = CoinMarketCapAPI()
        
        # Satış denemeleri takibi
        self.sell_attempts: Dict[str, List[dict]] = {}
        self._sell_attempts_max_per_symbol = 50  # Max entries per symbol
        self._sell_attempts_ttl = 3600  # 1 hour TTL
        self._last_sell_attempts_cleanup = 0.0

        # Gate.io toz pozisyon yönetimi
        self.gate_min_order_value = float(os.getenv("GATE_MIN_ORDER_VALUE", "10"))
        self.gate_dust_release_margin = float(os.getenv("GATE_DUST_RELEASE_MARGIN", "0.02"))
        self.gate_dust_buffer: Dict[str, dict] = {}
        self._position_evaluation_tasks: Dict[str, asyncio.Task] = {}
        self._position_evaluation_pending: Set[str] = set()
        self._position_evaluation_locks: Dict[str, asyncio.Lock] = {}
        self._position_action_locks: Dict[str, asyncio.Lock] = {}
        try:
            self.position_balance_guard_interval = max(
                0.0, float(os.getenv("POSITION_BALANCE_GUARD_INTERVAL", "3.0"))
            )
        except ValueError:
            self.position_balance_guard_interval = 3.0
        self._last_position_balance_guard: Dict[str, float] = {}

        logger.info(
            "Position Manager config: exchanges=%s, binance_testnet=%s, position_ws=%s, simulated_test_mode=%s",
            ",".join(sorted(self.enabled_exchanges)),
            self.binance_use_testnet,
            self.position_ws_enabled,
            self.test_mode,
        )
        if self.binance_use_testnet and self.binance_testnet_credential_source == "BINANCE_*":
            logger.warning(
                "Binance testnet mode is using default BINANCE_* credentials. "
                "If auth fails, set BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET."
            )

    @staticmethod
    def _normalise_ip(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        trimmed = value.strip()
        return trimmed or None

    @staticmethod
    def _env_truthy(value: Optional[str], default: bool = False) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _parse_enabled_exchanges(
        enabled_exchanges: Optional[Sequence[str]],
    ) -> Set[str]:
        supported = {"binance", "gate", "mexc"}

        if enabled_exchanges is None:
            raw = os.getenv("TRADER_ENABLED_EXCHANGES") or os.getenv("ENABLED_EXCHANGES")
            if not raw:
                return set(supported)
            candidates = [part.strip().lower() for part in raw.split(",")]
        elif isinstance(enabled_exchanges, str):
            candidates = [part.strip().lower() for part in enabled_exchanges.split(",")]
        else:
            candidates = [str(part).strip().lower() for part in enabled_exchanges]

        selected = {candidate for candidate in candidates if candidate in supported}
        if not selected:
            return set(supported)
        return selected

    @staticmethod
    def _resolve_binance_key_type(use_testnet: bool) -> str:
        raw = (
            os.getenv("BINANCE_TESTNET_KEY_TYPE")
            if use_testnet
            else None
        ) or os.getenv("BINANCE_KEY_TYPE") or "hmac"
        mode = raw.strip().lower()
        aliases = {
            "hmac-sha-256": "hmac",
            "hmac_sha_256": "hmac",
            "rsa_pkcs1v15": "rsa",
            "ed25519_api": "ed25519",
            "ed25519-key": "ed25519",
        }
        return aliases.get(mode, mode)

    def _binance_secret_required(self) -> bool:
        return self.binance_key_type == "hmac"

    def _select_local_ip(self) -> Optional[str]:
        preferred_order = [exchange for exchange in ("binance", "gate", "mexc") if exchange in self.enabled_exchanges]
        for exchange in preferred_order:
            ip = self.exchange_local_ips.get(exchange)
            if ip:
                return ip
        return None

    async def _check_min_notional(self, position: Position, quantity: float) -> tuple[bool, float]:
        """Minimum işlem değeri kontrolü ve düzeltmesi
        Returns: (is_valid, adjusted_quantity)
        """
        try:
            current_price = position.current_price
            order_value = quantity * current_price
            
            # Mevcut bakiyeyi ve min notional gereksinimini karşılamayı deneyelim
            if position.exchange == "binance":
                account_name = getattr(position, "account_name", None) or "PRIMARY"
                binance_api = self.binance_apis.get(account_name) or self.binance_apis.get("PRIMARY")
                if not binance_api:
                    logger.warning(f"No Binance API instance available for account {account_name}")
                    return True, quantity

                symbol = position.market_pair
                symbol_info = await binance_api.get_exchange_info(symbol)
                notional_filter = binance_api.get_notional_filter(symbol_info) if symbol_info else None

                if notional_filter and notional_filter.get('minNotional'):
                    min_notional = float(notional_filter['minNotional'])
                    if order_value < min_notional:
                        logger.warning(
                            f"Order value ${order_value:.2f} below Binance NOTIONAL minimum ${min_notional:.2f} ({symbol})"
                        )
                        required_quantity = (min_notional / current_price) * 1.05  # %5 güvenlik payı

                        if required_quantity <= position.remaining_quantity:
                            logger.info(f"Adjusting quantity from {quantity:.8f} to {required_quantity:.8f}")
                            return True, required_quantity

                        # Tüm bakiyeyi kontrol et
                        total_balance = await self._get_exchange_token_balance(position.exchange, position.symbol, account_name)
                        if total_balance > 0:
                            total_value = total_balance * current_price
                            if total_value >= min_notional * 0.95:
                                logger.info(f"Using full balance {total_balance:.8f} to satisfy min notional")
                                return True, total_balance

                        logger.error(
                            f"Insufficient balance for Binance minimum notional requirement (min={min_notional:.2f}, "
                            f"avail_value={position.remaining_quantity * current_price:.2f})"
                        )
                        position.strategy_state.setdefault("min_notional_blocked", 0)
                        position.strategy_state["min_notional_blocked"] += 1
                        position.strategy_state["min_notional_last_price"] = current_price
                        position.strategy_state["min_notional_blocked_until"] = datetime.now() + timedelta(minutes=5)
                        position.strategy_state.pop("min_notional_warning_state", None)
                        return False, 0.0
                else:
                    logger.debug(f"No NOTIONAL filter found for {symbol} on Binance; skipping min notional enforcement")
            
            elif position.exchange == "gate":
                # Gate.io currency pair'den al
                pair_info = await self.gate_api.get_currency_pair_info(position.market_pair)
                if pair_info:
                    min_quote_amount = float(pair_info.get('min_quote_amount', '1'))
                    if order_value < min_quote_amount:
                        logger.warning(f"Order value ${order_value:.2f} below Gate.io minimum ${min_quote_amount}")
                        # Gereken miktarı hesapla
                        required_quantity = (min_quote_amount / current_price) * 1.05
                        
                        if required_quantity <= position.remaining_quantity:
                            logger.info(f"Adjusting quantity from {quantity:.8f} to {required_quantity:.8f}")
                            self._clear_gate_dust(position)
                            return True, required_quantity
                        else:
                            # Tüm bakiyeyi kontrol et
                            total_balance = await self._get_exchange_token_balance(position.exchange, position.symbol, position.account_name)
                            if total_balance > 0:
                                total_value = total_balance * current_price
                                if total_value >= min_quote_amount * 0.95:
                                    logger.info(f"Using full Gate.io balance {total_balance:.8f} to satisfy minimum")
                                    self._clear_gate_dust(position)
                                    return True, total_balance

                            logger.error(
                                f"Insufficient balance for Gate.io minimum order value (min={min_quote_amount:.2f}, "
                                f"avail_value={position.remaining_quantity * current_price:.2f})"
                            )
                            position.strategy_state.setdefault("min_notional_blocked", 0)
                            position.strategy_state["min_notional_blocked"] += 1
                            position.strategy_state["min_notional_last_price"] = current_price
                            position.strategy_state["min_notional_blocked_until"] = datetime.now() + timedelta(minutes=5)
                            position.strategy_state.pop("min_notional_warning_state", None)
                            self._record_gate_dust(position, min_quote_amount)
                            return False, 0.0
                    else:
                        self._clear_gate_dust(position)
            
            # MEXC veya kontrol gerekmiyorsa orijinal değeri döndür
            return True, quantity
            
        except Exception as e:
            logger.error(f"Error checking min notional: {str(e)}")
            return True, quantity

    def _load_processed_signals(self):
        """İşlenmiş sinyalleri dosyadan yükle"""
        if os.path.exists(self.processed_signals_file):
            try:
                with open(self.processed_signals_file, 'r') as f:
                    data = json.load(f)
                    now = time.time()
                    # JSON'da tuple key'ler string olarak saklanır, geri çevir
                    # Support both old format (True) and new format (timestamp)
                    for key, value in data.items():
                        parts = key.split('|')
                        if len(parts) == 2:
                            tuple_key = (parts[0], parts[1])
                            if isinstance(value, (int, float)):
                                self.processed_signals[tuple_key] = float(value)
                            else:
                                # Legacy format (True/bool) - assign current time
                                self.processed_signals[tuple_key] = now

                    # Prune expired entries on load
                    self._prune_processed_signals()
                    logger.info(f"Loaded {len(self.processed_signals)} processed signals from cache")

            except Exception as e:
                logger.error(f"Error loading processed signals: {str(e)}")
                self.processed_signals = {}
        else:
            logger.info("No processed signals cache found, starting fresh")
            self.processed_signals = {}

    def _save_processed_signals(self):
        """İşlenmiş sinyalleri dosyaya kaydet"""
        try:
            # Prune before saving
            self._prune_processed_signals()

            # Tuple key'leri string'e çevir (JSON için)
            data = {f"{coin}|{exchange}": ts for (coin, exchange), ts in self.processed_signals.items()}

            with open(self.processed_signals_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Saved {len(self.processed_signals)} processed signals to cache")
        except Exception as e:
            logger.error(f"Error saving processed signals: {str(e)}")

    def _prune_processed_signals(self):
        """Remove expired processed signals entries"""
        now = time.time()
        # Only prune every 60 seconds to avoid overhead
        if now - self._last_processed_signals_prune < 60:
            return

        self._last_processed_signals_prune = now
        expired_keys = [
            k for k, ts in self.processed_signals.items()
            if now - ts > self.processed_signals_ttl
        ]
        for k in expired_keys:
            del self.processed_signals[k]

        if expired_keys:
            logger.info(f"Pruned {len(expired_keys)} expired processed signals")

    def _cleanup_sell_attempts(self):
        """Cleanup old sell_attempts entries to prevent memory leak"""
        now = time.time()
        # Only cleanup every 60 seconds
        if now - self._last_sell_attempts_cleanup < 60:
            return

        self._last_sell_attempts_cleanup = now
        symbols_to_remove = []
        total_removed = 0

        for symbol, attempts in self.sell_attempts.items():
            # Remove entries older than TTL
            original_count = len(attempts)
            attempts[:] = [
                a for a in attempts
                if 'time' in a and (now - a['time'].timestamp() if hasattr(a['time'], 'timestamp') else now) < self._sell_attempts_ttl
            ]

            # Limit entries per symbol
            if len(attempts) > self._sell_attempts_max_per_symbol:
                attempts[:] = attempts[-self._sell_attempts_max_per_symbol:]

            total_removed += original_count - len(attempts)

            # Mark empty symbols for removal
            if not attempts:
                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self.sell_attempts[symbol]

        if total_removed > 0 or symbols_to_remove:
            logger.debug(f"Cleaned up sell_attempts: removed {total_removed} entries, {len(symbols_to_remove)} symbols")

    def _normalize_source(self, source: str) -> str:
        """Normalize raw listing source strings to strategy keys."""
        if not source:
            return "unknown"
        normalized = source.strip().lower()
        if not normalized:
            return "unknown"
        return self._source_aliases.get(normalized, normalized)

    def _is_signal_processed(self, coin: str, exchange: str) -> bool:
        """Bu coin-exchange çifti daha önce işlendi mi?"""
        key = (coin.upper(), self._normalize_source(exchange))
        ts = self.processed_signals.get(key)
        if ts is None:
            return False
        # Check if expired
        if time.time() - ts > self.processed_signals_ttl:
            del self.processed_signals[key]
            return False
        return True

    def _mark_signal_processed(self, coin: str, exchange: str):
        """Bu coin-exchange çiftini işlenmiş olarak işaretle"""
        key = (coin.upper(), self._normalize_source(exchange))
        self.processed_signals[key] = time.time()
        self._save_processed_signals()
        logger.info(f"🔒 Marked {coin} from {exchange} as processed - will not open again")  

    def _record_trade_confirmations(self, payload: dict):
        results = payload.get("results") or []
        if not results:
            return

        timestamp = datetime.now(timezone.utc)

        for result in results:
            if not result.get("success") or result.get("skipped"):
                continue

            symbol = (result.get("symbol") or "").upper()
            exchange = (result.get("exchange") or "").lower()
            account = (result.get("account") or "PRIMARY").upper()

            if not symbol or not exchange:
                continue

            key = (symbol, exchange, account)
            self.trade_confirmations[key] = {
                "timestamp": timestamp,
                "amount": result.get("amount", 0.0),
                "raw": result,
            }

            logger.info(
                f"Trade confirmation recorded: {symbol} on {exchange.upper()} ({account}) amount={result.get('amount', 0.0):.2f}"
            )

    def _has_recent_trade_confirmation(self, symbol: str, exchange: str, account: str = "PRIMARY") -> bool:
        key = (symbol.upper(), exchange.lower(), account.upper())
        info = self.trade_confirmations.get(key)
        if not info:
            return False

        timestamp: datetime = info.get("timestamp", datetime.now(timezone.utc))
        age = datetime.now(timezone.utc) - timestamp
        if age.total_seconds() > self.trade_confirmation_ttl:
            self.trade_confirmations.pop(key, None)
            return False

        return True

    def _consume_trade_confirmation(self, symbol: str, exchange: str, account: str = "PRIMARY") -> None:
        key = (symbol.upper(), exchange.lower(), account.upper())
        self.trade_confirmations.pop(key, None)

    def _extract_confirmation_metrics(self, confirmation: Optional[dict]) -> Tuple[Optional[float], Optional[float]]:
        """Trade onayından ortalama fiyat ve toplam miktarı ayıkla."""
        if not confirmation or not isinstance(confirmation, dict):
            return None, None

        price: Optional[float] = None
        quantity: Optional[float] = None

        # Doğrudan alanları kontrol et
        direct_price_keys = ["avgPrice", "averagePrice", "price", "fillPrice", "executed_price"]
        direct_qty_keys = [
            "executedQty",
            "executed_quantity",
            "filledSize",
            "amount",
            "quantity",
            "filledQuantity",
        ]

        for key in direct_price_keys:
            value = confirmation.get(key)
            if value not in (None, ""):
                try:
                    price = float(value)
                    break
                except (TypeError, ValueError):
                    continue

        for key in direct_qty_keys:
            value = confirmation.get(key)
            if value not in (None, ""):
                try:
                    quantity = float(value)
                    break
                except (TypeError, ValueError):
                    continue

        # Fills varsa weighted average hesapla
        fills = confirmation.get("fills") or confirmation.get("tradeList")
        if isinstance(fills, list) and fills:
            total_qty = 0.0
            total_cost = 0.0
            for fill in fills:
                if not isinstance(fill, dict):
                    continue
                fill_price = fill.get("price") or fill.get("tradePrice")
                fill_qty = fill.get("qty") or fill.get("quantity") or fill.get("tradeQuantity")
                try:
                    if fill_price is None or fill_qty is None:
                        continue
                    f_price = float(fill_price)
                    f_qty = float(fill_qty)
                    if f_qty <= 0:
                        continue
                    total_qty += f_qty
                    total_cost += f_price * f_qty
                except (TypeError, ValueError):
                    continue

            if total_qty > 0:
                price = total_cost / total_qty
                if quantity is None:
                    quantity = total_qty

        return price, quantity

    async def _prepare_sell_quantity(self, position: Position, quantity: float) -> float:
        """Normalize sell quantity according to exchange precision rules."""
        qty = max(float(quantity), 0.0)
        if qty <= 0:
            return 0.0

        try:
            if position.exchange == "mexc" and self.mexc_api:
                symbol_key = position.market_pair
                symbol_info = await self.mexc_api.get_exchange_info(symbol_key)
                lot_filter = self.mexc_api.get_lot_size_filter(symbol_info) if symbol_info else None
                if lot_filter:
                    qty_str = self.mexc_api.format_quantity_with_lot_size(qty, lot_filter)
                else:
                    precision = 6
                    if symbol_info:
                        quantity_precision = symbol_info.get("quantityPrecision")
                        if quantity_precision is not None:
                            try:
                                precision = max(0, int(quantity_precision))
                            except (TypeError, ValueError):
                                precision = 6
                        else:
                            base_precision = symbol_info.get("baseSizePrecision")
                            if base_precision is not None:
                                try:
                                    if isinstance(base_precision, str):
                                        if '.' in base_precision:
                                            decimals = len(base_precision.split('.')[1].rstrip('0'))
                                            precision = max(0, decimals)
                                        else:
                                            precision = max(0, int(base_precision))
                                    elif isinstance(base_precision, (int, float)):
                                        base_precision_str = f"{base_precision}"
                                        if '.' in base_precision_str:
                                            decimals = len(base_precision_str.split('.')[1].rstrip('0'))
                                            precision = max(0, decimals)
                                        else:
                                            precision = max(0, int(base_precision))
                                except (TypeError, ValueError):
                                    precision = 6
                    qty_str = self.mexc_api.format_quantity(qty, precision)
                if qty_str is None:
                    return qty
                return float(qty_str)

            if position.exchange == "gate" and self.gate_api:
                pair_info = await self.gate_api.get_currency_pair_info(position.market_pair)
                qty_str = None
                if pair_info:
                    qty_str = self.gate_api.format_quantity_for_gate(qty, pair_info)

                if qty_str is None:
                    return qty

                try:
                    return float(qty_str)
                except (TypeError, ValueError):
                    logger.warning(
                        f"Failed to convert Gate quantity '{qty_str}' to float for {position.market_pair}"
                    )
                    return qty
        except Exception as exc:
            logger.warning(
                f"Auto-exit quantity normalization failed for {position.exchange} {position.symbol}: {exc}"
            )

        return qty

    def _is_test_signal(self, raw_source: str, signal_data: dict) -> bool:
        """Determine whether the incoming signal should trigger test handling."""
        force_flag = signal_data.get("force_strategy")
        if isinstance(force_flag, str):
            force_flag = force_flag.strip().lower() in {"1", "true", "yes", "on"}
        if force_flag:
            return False

        source_label = (raw_source or "").lower()
        if "test" in source_label:
            return True

        announcement = (signal_data.get("announcement") or "").lower()
        if announcement.startswith("[test]") or "simulated" in announcement:
            return True

        if signal_data.get("test_mode") or signal_data.get("is_test"):
            return True

        return False

    def _should_bypass_processed(self, signal_data: dict, is_test_signal: bool) -> bool:
        """Return True if duplicate-signal guard should be skipped."""

        if is_test_signal:
            return True

        force_flag = signal_data.get("force_strategy")
        if isinstance(force_flag, str):
            force_flag = force_flag.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(force_flag, bool) and force_flag:
            return True

        notice_id = str(signal_data.get("notice_id") or "")
        if notice_id.upper().startswith("TEST-"):
            return True

        announcement = (signal_data.get("announcement") or "").upper()
        if "[TEST]" in announcement or "SIMULATED" in announcement:
            return True

        source_label = (signal_data.get("source") or "").lower()
        if "test" in source_label:
            return True

        return False

    def _schedule_test_auto_exit(self, token: str) -> None:
        """Ensure a background task will flatten test positions back to USDT."""
        token = token.upper()
        existing = self.test_exit_tasks.get(token)
        if existing and not existing.done():
            logger.debug(f"Test auto-exit already scheduled for {token}")
            return

        try:
            task = asyncio.create_task(self._auto_exit_test_token(token))
        except RuntimeError as exc:
            logger.error(f"Unable to schedule test auto-exit for {token}: {exc}")
            return

        self.test_exit_tasks[token] = task
        logger.info(
            f"🧪 Scheduled test auto-exit for {token} in {self.test_auto_exit_delay:.1f}s "
            f"(timeout {self.test_auto_exit_timeout:.1f}s)"
        )

    async def _auto_exit_test_token(self, token: str) -> None:
        """Sell any active positions for the given test token back to USDT."""
        try:
            delay = max(0.0, self.test_auto_exit_delay)
            if delay:
                await asyncio.sleep(delay)

            deadline = time.time() + max(0.0, self.test_auto_exit_timeout)
            active_keys: List[str] = []

            while time.time() < deadline:
                active_keys = [
                    key
                    for key in self.symbol_positions.get(token, [])
                    if (key in self.positions) and self.positions[key].status == "active"
                ]

                if active_keys:
                    break

                await asyncio.sleep(1.0)

            if not active_keys:
                logger.info(f"🧪 Test auto-exit: no active {token} positions detected within timeout")
                return

            positions = [
                self.positions[key]
                for key in active_keys
                if key in self.positions and self.positions[key].status == "active"
            ]

            if not positions:
                logger.info(f"🧪 Test auto-exit: {token} positions already flat")
                return

            logger.info(
                f"🧪 Test auto-exit triggered for {token}: flattening {len(positions)} position(s) back to USDT"
            )

            for position in positions:
                try:
                    override_qty = max(0.0, position.remaining_quantity * self.test_auto_exit_qty_factor)
                    override_qty = await self._prepare_sell_quantity(position, override_qty)
                    if override_qty <= 0:
                        logger.info(
                            f"🧪 Test auto-exit: quantity for {position.symbol} on {position.exchange} is zero; skipping"
                        )
                        continue

                    sell_success = False
                    qty_attempt = override_qty
                    for attempt in range(3):
                        if qty_attempt <= 0:
                            break
                        success = await self.sell_position(
                            position,
                            "Test auto-exit to USDT",
                            override_quantity=qty_attempt,
                        )
                        if success:
                            sell_success = True
                            break
                        qty_attempt *= 0.9
                        qty_attempt = await self._prepare_sell_quantity(position, qty_attempt)
                        logger.warning(
                            f"🧪 Test auto-exit retry for {token} on {position.exchange} ({position.account_name}) with quantity {qty_attempt:.8f}"
                        )
                        await asyncio.sleep(0.2)

                    if not sell_success:
                        logger.warning(
                            f"🧪 Test auto-exit pending for {token} on {position.exchange} ({position.account_name})"
                        )
                except Exception as exc:
                    logger.error(
                        f"🧪 Test auto-exit error for {token} on {position.exchange} ({position.account_name}): {exc}"
                    )

            logger.info(f"🧪 Test auto-exit complete for {token}")
        finally:
            self.test_exit_tasks.pop(token, None)

    def _setup_results_directory(self):
        """Trading sonuçları için klasör yapısını oluştur"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.info(f"Created trading results directory: {self.results_dir}")
    
    def _load_trade_counter(self):
        """Trade sayacını dosyadan yükle"""
        if os.path.exists(self.trade_counter_file):
            try:
                with open(self.trade_counter_file, 'r') as f:
                    data = json.load(f)
                    self.current_trade_id = data.get('last_trade_id', 0)
                logger.info(f"Loaded trade counter: last_trade_id={self.current_trade_id}")
            except Exception as e:
                logger.error(f"Error loading trade counter: {str(e)}")
                self.current_trade_id = 0
        else:
            # Dosya yoksa yeni oluştur
            self._save_trade_counter()
    
    def _save_trade_counter(self):
        """Trade sayacını dosyaya kaydet"""
        try:
            with open(self.trade_counter_file, 'w') as f:
                json.dump({'last_trade_id': self.current_trade_id}, f)
            logger.info(f"Saved trade counter: last_trade_id={self.current_trade_id}")
        except Exception as e:
            logger.error(f"Error saving trade counter: {str(e)}")
    
    def _get_next_trade_id(self):
        """Bir sonraki trade ID'sini al"""
        self.current_trade_id += 1
        self._save_trade_counter()
        return f"trade{self.current_trade_id}"

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(
            total=DEFAULT_POSITION_HTTP_TOTAL_TIMEOUT,
            connect=DEFAULT_POSITION_HTTP_CONNECT_TIMEOUT,
            sock_connect=DEFAULT_POSITION_HTTP_SOCK_CONNECT_TIMEOUT,
            sock_read=DEFAULT_POSITION_HTTP_SOCK_READ_TIMEOUT,
        )
        connector_kwargs: Dict[str, Any] = {"limit": 100}
        if self.primary_local_ip:
            connector_kwargs["local_addr"] = (self.primary_local_ip, 0)
            logger.info("Binding HTTP client session to local IP %s", self.primary_local_ip)

        connector = aiohttp.TCPConnector(**connector_kwargs)
        logger.debug(
            "HTTP client timeout configured: total=%.2fs connect=%.2fs sock_connect=%.2fs sock_read=%.2fs",
            timeout.total if timeout.total is not None else -1,
            timeout.connect if timeout.connect is not None else -1,
            timeout.sock_connect if timeout.sock_connect is not None else -1,
            timeout.sock_read if timeout.sock_read is not None else -1,
        )
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        # Tüm Binance hesaplarını başlat
        for account_name, credentials in self.binance_accounts.items():
            self.binance_apis[account_name] = BinanceAPI(
                credentials["api_key"],
                credentials["api_secret"],
                self.session,
                self.test_mode,
                account_name,
                local_ip=self.exchange_local_ips.get("binance"),
                use_testnet=self.binance_use_testnet,
            )
            
            # Her hesap için bakiye kontrolü
            balance = await self.binance_apis[account_name].get_balance("USDT")
            logger.info(f"{account_name} USDT Balance: ${balance:.2f}")
        
        # Diğer borsaları başlat
        if "gate" in self.enabled_exchanges:
            self.gate_api = GateAPI(
                self.gate_api_key,
                self.gate_api_secret,
                self.session,
                self.test_mode,
                local_ip=self.exchange_local_ips.get("gate"),
            )
            gate_balance = await self.gate_api.get_balance("USDT")
            logger.info(f"GATE.IO USDT Balance: ${gate_balance:.2f}")
        
        if "mexc" in self.enabled_exchanges:
            self.mexc_api = MEXCAPI(
                self.mexc_api_key,
                self.mexc_api_secret,
                self.session,
                self.test_mode,
                local_ip=self.exchange_local_ips.get("mexc"),
            )
            mexc_balance = await self.mexc_api.get_balance("USDT")
            logger.info(f"MEXC USDT Balance: ${mexc_balance:.2f}")
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Temiz bir şekilde kaynakları serbest bırak"""
        logger.info("Shutting down Position Manager...")
        
        # Önce WebSocket bağlantısını kapat
        if self.position_ws and self.position_ws_connected:
            try:
                await self.position_ws.close()
                logger.info("Closed position WebSocket connection")
            except Exception as e:
                logger.error(f"Error closing position WebSocket: {str(e)}")
        
        # Tüm fiyat akışı görevlerini temizle
        logger.info("Cancelling all price stream tasks...")
        tasks_to_cancel = []
        
        # Tüm Binance API görevlerini ekle
        for binance_api in self.binance_apis.values():
            for task in binance_api.active_ws_tasks:
                if not task.done():
                    tasks_to_cancel.append(task)
        
        # Gate API görevlerini ekle
        if self.gate_api:
            for task in getattr(self.gate_api, 'active_ws_tasks', set()):
                if not task.done():
                    tasks_to_cancel.append(task)
                    
        # MEXC API görevlerini ekle
        if self.mexc_api:
            for task in getattr(self.mexc_api, 'active_ws_tasks', set()):
                if not task.done():
                    tasks_to_cancel.append(task)
                    
        # Diğer görevlerimizi ekle
        tasks_to_cancel.extend([task for task in self.price_stream_tasks if not task.done()])
        tasks_to_cancel.extend(
            [task for task in self._position_evaluation_tasks.values() if not task.done()]
        )
        tasks_to_cancel.extend(
            [task for task in self.pending_signal_checks.values() if task and not task.done()]
        )
        tasks_to_cancel.extend(
            [task for task in self.test_exit_tasks.values() if task and not task.done()]
        )
        for position_tasks in self.investment_tasks.values():
            tasks_to_cancel.extend(
                [task for task in position_tasks.values() if task and not task.done()]
            )

        unique_tasks = []
        seen_task_ids = set()
        for task in tasks_to_cancel:
            if id(task) in seen_task_ids:
                continue
            seen_task_ids.add(id(task))
            unique_tasks.append(task)
        tasks_to_cancel = unique_tasks
        
        # Tüm görevleri iptal et
        for task in tasks_to_cancel:
            task.cancel()
        
        # Tüm görevlerin sonlanmasını bekle
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info(f"Cancelled {len(tasks_to_cancel)} WebSocket tasks")
        self.pending_signal_checks.clear()
        self.test_exit_tasks.clear()
        self._position_evaluation_tasks.clear()
        self._position_evaluation_pending.clear()
        self._position_evaluation_locks.clear()
        self._position_action_locks.clear()
        self._last_position_balance_guard.clear()
        self.active_positions.clear()
        self.symbol_positions.clear()
        
        # Son olarak HTTP oturumunu kapat
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("Closed HTTP session")
    
    async def start(self):
        """Pozisyon yönetim sistemini başlat"""
        logger.info("Starting Position Manager System")
        
        try:
            # Fiyat akışı görevlerini başlat
            binance_stream_tasks = []
            for account_name in self.binance_apis:
                task = asyncio.create_task(self.start_binance_price_streams(account_name))
                binance_stream_tasks.append(task)
                self.price_stream_tasks.append(task)
            
            if self.gate_api:
                gate_stream_task = asyncio.create_task(self.start_gate_price_streams())
                self.price_stream_tasks.append(gate_stream_task)
            if self.mexc_api:
                mexc_stream_task = asyncio.create_task(self.start_mexc_price_streams())
                self.price_stream_tasks.append(mexc_stream_task)
            
            # Pozisyon izleme görevini başlat
            monitor_task = asyncio.create_task(self.monitor_positions())
            
            # Piyasa koşullarını değerlendirme görevini başlat
            market_task = asyncio.create_task(self.evaluate_market_conditions())
            
            # Pozisyon bildirimleri için WebSocket'e bağlan
            websocket_task = None
            if self.position_ws_enabled:
                websocket_task = asyncio.create_task(self.connect_to_position_websocket())
            
            # Tüm görevleri bekle
            all_tasks = [monitor_task, market_task] + self.price_stream_tasks
            if websocket_task is not None:
                all_tasks.append(websocket_task)
            await asyncio.gather(*all_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Position Manager tasks cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in start method: {str(e)}")
            logger.debug(traceback.format_exc())
    
    async def start_binance_price_streams(self, account_name: str):
        """Belirli bir Binance hesabı için fiyat akışlarını başlat"""
        try:
            logger.info(f"Starting Binance price stream monitor for {account_name}")
            
            # Son aktif sembolleri takip etmek için bir set
            last_active_symbols = set()
            binance_api = self.binance_apis[account_name]
            
            while True:
                # Bu hesaba ait aktif pozisyonları bul
                active_symbols = []
                for pos in self.positions.values():
                    if (pos.exchange == 'binance' and 
                        pos.status == 'active' and 
                        pos.account_name == account_name):
                        active_symbols.append(pos.symbol)
                
                # Set'e dönüştür
                active_symbols_set = set(active_symbols)
                
                # Semboller değişti mi?
                if active_symbols_set != last_active_symbols:
                    if active_symbols:
                        logger.info(f"Active symbols changed for {account_name}. "
                                   f"Starting/restarting Binance WebSocket for {len(active_symbols)} symbols: {active_symbols}")
                        
                        # WebSocket'i başlat/yeniden başlat
                        await binance_api.start_price_streams(list(active_symbols))
                        last_active_symbols = active_symbols_set.copy()
                    else:
                        # Aktif sembol yok
                        if last_active_symbols:
                            logger.info(f"No active symbols for {account_name}. Stopping Binance WebSocket.")
                            # Mevcut WebSocket'leri iptal et
                            for task in list(binance_api.ws_connections.values()):
                                if not task.done():
                                    task.cancel()
                            binance_api.ws_connections.clear()
                            last_active_symbols = set()
                        else:
                            logger.debug(f"No active symbols for {account_name}, waiting...")
                
                # 3 saniye bekle
                await asyncio.sleep(3)
                
        except asyncio.CancelledError:
            logger.info(f"{account_name} Binance price stream monitor cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in {account_name} Binance price stream monitor: {str(e)}")
            logger.debug(traceback.format_exc())
            # Hata durumunda biraz bekle ve devam et
            await asyncio.sleep(10)

    async def start_gate_price_streams(self):
        """Gate.io fiyat akışlarını başlat"""
        try:
            # Son aktif sembolleri takip etmek için bir set tutalım
            last_active_symbols = set()
            
            # Aktif sürüm - hiçbir sembol yoksa beklet
            while True:
                active_symbols = [pos.symbol for pos in self.positions.values() 
                                if pos.exchange == 'gate' and pos.status == 'active']
                
                # Set'e dönüştür - böylece karşılaştırma yapabiliriz
                active_symbols_set = set(active_symbols)
                
                # Aktif semboller değiştiyse veya bağlantı yoksa yeni bağlantı kur
                if active_symbols_set != last_active_symbols:
                    if active_symbols:
                        logger.info(f"Aktif semboller değişti. Gate.io fiyat akışlarını yeniden başlatılıyor. Aktif sembol sayısı: {len(active_symbols)}")
                        await self.gate_api.start_price_streams(active_symbols)
                        last_active_symbols = active_symbols_set.copy()
                    else:
                        # Aktif semboller temizlendiyse, son aktif sembolleri de sıfırla
                        if last_active_symbols:
                            logger.info("Tüm aktif semboller temizlendi. Gate.io fiyat akışları durduruluyor.")
                            last_active_symbols = set()
                
                # Bekle
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            logger.info("Gate.io price streams cancelled")
        except Exception as e:
            logger.error(f"Error in Gate.io price streams: {str(e)}")
            logger.debug(traceback.format_exc())
    
    async def start_mexc_price_streams(self):
        """MEXC fiyat akışlarını başlat"""
        try:
            # Son aktif sembolleri takip etmek için bir set tutalım
            last_active_symbols = set()
            
            # Aktif sürüm - hiçbir sembol yoksa beklet
            while True:
                active_symbols = [pos.symbol for pos in self.positions.values() 
                                if pos.exchange == 'mexc' and pos.status == 'active']
                
                # Set'e dönüştür - böylece karşılaştırma yapabiliriz
                active_symbols_set = set(active_symbols)
                
                # Aktif semboller değiştiyse veya bağlantı yoksa yeni bağlantı kur
                if active_symbols_set != last_active_symbols:
                    if active_symbols:
                        logger.info(f"Aktif semboller değişti. MEXC fiyat akışlarını yeniden başlatılıyor. Aktif sembol sayısı: {len(active_symbols)}")
                        await self.mexc_api.start_price_streams(active_symbols)
                        last_active_symbols = active_symbols_set.copy()
                    else:
                        # Aktif semboller temizlendiyse, son aktif sembolleri de sıfırla
                        if last_active_symbols:
                            logger.info("Tüm aktif semboller temizlendi. MEXC fiyat akışları durduruluyor.")
                            last_active_symbols = set()
                
                # Bekle
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            logger.info("MEXC price streams cancelled")
        except Exception as e:
            logger.error(f"Error in MEXC price streams: {str(e)}")
            logger.debug(traceback.format_exc())
    
