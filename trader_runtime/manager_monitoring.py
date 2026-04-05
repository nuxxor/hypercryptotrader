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


class PositionManagerMonitoringMixin:
    async def handle_position_update(self, data: dict):
        """Pozisyon güncelleme mesajını işle"""
        try:
            raw_symbol = data.get("symbol")
            if not raw_symbol:
                return

            symbol = raw_symbol.upper()
            updated = False

            if symbol in self.symbol_positions:
                for position_key in list(self.symbol_positions[symbol]):
                    position = self.positions.get(position_key)
                    if not position or position.status == "closed":
                        continue

                    if "price" in data:
                        new_price = float(data["price"])
                        volume = float(data.get("volume", 0) or 0)
                        position.update_price(new_price, volume)
                        logger.debug(
                            f"Updated {position_key} price to {new_price} "
                            f"(profit: {position.current_profit_pct:.2f}%)"
                        )

                    self._schedule_position_evaluation(position, trigger="position_update")
                    updated = True

            if not updated:
                logger.debug("Ignoring position update for %s: no active tracked position", symbol)
        except Exception as e:
            logger.error(f"Error handling position update: {str(e)}")
    
    async def _get_exchange_balance(self, exchange: str, account_name: str = "PRIMARY", force_refresh: bool = False) -> float:
        """Borsadaki USDT bakiyesini al"""
        try:
            use_cache = not force_refresh
            if exchange == "binance":
                if account_name in self.binance_apis:
                    return await self.binance_apis[account_name].get_balance("USDT", use_cache=use_cache)
                else:
                    logger.error(f"Unknown Binance account: {account_name}")
                    return 0.0
            elif exchange == "mexc":
                return await self.mexc_api.get_balance("USDT", use_cache=use_cache)
            else:  # gate
                if force_refresh:
                    snapshot = await self.gate_api.get_balance_snapshot("USDT", use_cache=False)
                    return float(snapshot.available)
                return await self.gate_api.get_balance("USDT")
        except Exception as e:
            logger.error(f"Error getting balance from {exchange}: {str(e)}")
            return 0.0

    async def _get_exchange_token_balance(
        self,
        exchange: str,
        symbol: str,
        account_name: str = "PRIMARY",
        *,
        use_cache: bool = True,
    ) -> float:
        """Belirli bir varlık bakiyesini getir, hata durumunda mevcut pozisyonu koru."""
        try:
            if exchange == "binance":
                if account_name in self.binance_apis:
                    return await self.binance_apis[account_name].get_balance(symbol, use_cache=use_cache)
                logger.error(f"Unknown Binance account: {account_name}")
                return 0.0
            if exchange == "mexc":
                return await self.mexc_api.get_balance(symbol, use_cache=use_cache)
            if exchange == "gate":
                snapshot = await self.gate_api.get_balance_snapshot(symbol)
                return float(snapshot.effective_total)
        except GateAPITemporaryError as exc:
            logger.warning(
                f"Gate.io balance temporarily unavailable for {symbol} ({account_name}): {exc}"
            )
        except GateAPIHardError as exc:
            logger.error(
                f"Gate.io balance error for {symbol} ({account_name}): {exc}"
            )
        except Exception as exc:
            logger.error(f"Error getting {symbol} balance on {exchange} ({account_name}): {exc}")
        # Hata veya bilinmeyen senaryoda negatif döndürme; pozisyonu tutmak için -1
        return -1.0
    
    def _close_position_immediately(self, position: Position) -> None:
        """Remove position from active tracking structures without triggering sells."""
        self._mark_position_closed(position)

    def _reconcile_position_quantity(
        self,
        position: Position,
        live_balance: float,
        *,
        allow_increase: bool = False,
        increase_cap: float = 1.2,
    ) -> None:
        """Adjust stored position quantity to align with live balance and partial sells."""
        if live_balance < 0:
            return

        sold_qty = sum(qty for _, qty, _ in position.partial_sells)
        new_total = sold_qty + max(live_balance, 0.0)
        tolerance = max(1e-8, position.quantity * 0.001)

        if not allow_increase and new_total > position.quantity + tolerance:
            logger.warning(
                f"Live balance for {position.symbol} exceeds tracked quantity "
                f"({live_balance:.8f} vs remaining {position.remaining_quantity:.8f}); "
                "keeping tracked quantity to avoid oversell"
            )
            new_total = position.quantity
        elif allow_increase and new_total > position.quantity * increase_cap:
            logger.warning(
                f"Live balance jump for {position.symbol} looks suspicious "
                f"(tracked total {position.quantity:.8f}, live-backed total {new_total:.8f}); "
                "skipping automatic increase"
            )
            return

        if new_total < sold_qty:
            new_total = sold_qty

        if abs(new_total - position.quantity) > tolerance:
            logger.info(
                f"Synchronizing {position.symbol} quantity with live balance: "
                f"{position.quantity:.8f} -> {new_total:.8f}"
            )
            position.quantity = new_total

    async def _refresh_position_balance(
        self,
        position: Position,
        *,
        allow_increase: bool = False,
        use_cache: bool = False,
    ) -> float:
        """Fetch live balance and reconcile local quantity."""
        live_balance = await self._get_exchange_token_balance(
            position.exchange,
            position.symbol,
            position.account_name,
            use_cache=use_cache,
        )
        if live_balance >= 0:
            self._reconcile_position_quantity(position, live_balance, allow_increase=allow_increase)
        return live_balance

    def _record_gate_dust(self, position: Position, threshold: float) -> None:
        """Track Gate.io dust that cannot be sold yet."""
        symbol = position.symbol.upper()
        entry = self.gate_dust_buffer.get(symbol) or {
            "quantity": 0.0,
            "threshold": threshold,
            "updated": datetime.now(),
        }

        new_quantity = position.remaining_quantity
        previous_quantity = entry.get("quantity", 0.0)

        entry["quantity"] = new_quantity
        entry["threshold"] = threshold
        entry["updated"] = datetime.now()
        self.gate_dust_buffer[symbol] = entry
        position.strategy_state["gate_dust_hold"] = {
            "threshold": threshold,
            "updated": datetime.now(),
        }

        if abs(previous_quantity - new_quantity) > 1e-9:
            logger.info(
                f"Gate.io dust tracking enabled for {symbol}: remaining={new_quantity:.8f}, "
                f"threshold={threshold:.2f}"
            )

    def _clear_gate_dust(self, position: Position) -> None:
        symbol = position.symbol.upper()
        self.gate_dust_buffer.pop(symbol, None)
        position.strategy_state.pop("gate_dust_hold", None)

    def _should_release_gate_dust(self, position: Position) -> bool:
        """Determine if accumulated Gate.io dust can now be sold."""
        hold_info = position.strategy_state.get("gate_dust_hold")
        if not hold_info:
            return True
        if position.current_price <= 0:
            logger.debug(
                f"Skipping Gate.io dust release check for {position.symbol}: "
                "current price unavailable"
            )
            return False
        threshold = hold_info.get("threshold", self.gate_min_order_value)
        current_value = position.current_price * position.remaining_quantity
        release_margin = 1 + self.gate_dust_release_margin

        if current_value >= threshold * release_margin:
            logger.info(
                f"Gate.io dust release condition met for {position.symbol}: "
                f"value=${current_value:.2f}, threshold=${threshold:.2f}"
            )
            self._clear_gate_dust(position)
            return True

        return False

    def _should_defer_gate_guard(self, position: Position, snapshot: BalanceSnapshot) -> bool:
        """Return True if Gate balance check should be deferred due to settlement window."""
        try:
            locked_positive = snapshot.locked > Decimal("0")
        except Exception:
            locked_positive = False

        if not locked_positive:
            return False

        elapsed = (datetime.now() - position.entry_time).total_seconds()
        if elapsed < self.gate_balance_guard_cooldown_seconds:
            logger.info(
                f"[Gate guard] Deferring balance check for {position.symbol}: "
                f"elapsed={elapsed:.1f}s < cooldown={self.gate_balance_guard_cooldown_seconds:.1f}s, "
                f"avail={float(snapshot.available):.8f}, locked={float(snapshot.locked):.8f}"
            )
            return True
        return False

    async def _log_all_exchange_balances(self, context: Optional[str] = None) -> None:
        """Fetch and log up-to-date USDT balances across all exchanges."""
        context_suffix = f" ({context})" if context else ""
        logger.info(f"🔄 Refreshing USDT balances{context_suffix}")

        # Binance accounts
        binance_total = 0.0
        for account_name, api in self.binance_apis.items():
            try:
                balance = await api.get_balance("USDT")
                binance_total += balance
                logger.info(f"  Binance {account_name.lower()}: {balance:.2f} USDT")
            except Exception as exc:
                logger.warning(f"  Binance {account_name.lower()}: balance unavailable ({exc})")
        if self.binance_apis:
            logger.info(f"  Binance TOTAL: {binance_total:.2f} USDT")

        # Gate.io
        if self.gate_api:
            try:
                gate_snapshot = await self.gate_api.get_balance_snapshot("USDT", use_cache=False)
                logger.info(f"  Gate.io: {float(gate_snapshot.available):.2f} USDT")
            except GateAPITemporaryError as exc:
                logger.warning(f"  Gate.io: balance temporarily unavailable ({exc})")
            except GateAPIHardError as exc:
                logger.error(f"  Gate.io: balance error ({exc})")
            except Exception as exc:
                logger.error(f"  Gate.io: unexpected balance error ({exc})")

        # MEXC
        if self.mexc_api:
            try:
                mexc_balance = await self.mexc_api.get_balance("USDT")
                logger.info(f"  MEXC: {mexc_balance:.2f} USDT")
            except Exception as exc:
                logger.warning(f"  MEXC: balance unavailable ({exc})")

    def _has_open_positions(self) -> bool:
        """Return True if any position is still active or partially open."""
        return any(
            pos.status in ("active", "partial_sold")
            for pos in self.positions.values()
        )

    async def handle_price_update(self, symbol: str, price: float, volume: float = None):
        """WebSocket'ten gelen fiyat güncellemesini işle - Sessiz mod"""
        try:
            # Binance'den gelen sembol formatını düzelt (BTCUSDT -> BTC)
            if symbol.endswith('USDT'):
                symbol = symbol[:-4]
            
            # Debug log
            logger.debug(f"Price update received for {symbol}: ${price}")
            
            # Symbol pozisyonları varsa işle
            if symbol in self.symbol_positions:
                for position_key in list(self.symbol_positions[symbol]):
                    if position_key in self.positions:
                        position = self.positions[position_key]
                        
                        # WebSocket güncelleme zamanını kaydet
                        self.last_price_update[f"{position.exchange}_{symbol}"] = time.time()
                        
                        # KAPALI POZİSYONLARI KONTROL ET
                        if position.status == "closed":
                            logger.debug(f"Ignoring price update for closed position {symbol}")
                            self._close_position_immediately(position)
                            continue
                        
                        old_price = position.current_price
                        old_profit = position.current_profit_pct
                        
                        position.update_price(price, volume)
                        
                        # Sadece önemli değişiklikleri logla (%1'den fazla)
                        profit_change = abs(position.current_profit_pct - old_profit)
                        if profit_change >= 1.0:
                            logger.debug(f"📊 {symbol} profit update: {old_profit:.2f}% -> {position.current_profit_pct:.2f}%")
                        
                        # Hot path'i bloklamadan tek evaluation akışına yönlendir
                        self._schedule_position_evaluation(position, trigger="price_update")
                
        except Exception as e:
            logger.error(f"Error in handle_price_update for {symbol}: {str(e)}")

    
    async def get_current_price(self, position: Position) -> float:
        """Bir pozisyon için mevcut fiyatı al (önbellekleme ile)"""
        cache_key = f"{position.exchange}_{position.symbol}"
        
        # Taze önbellek fiyatımız var mı diye kontrol et
        if cache_key in self.price_cache:
            cached_price, timestamp = self.price_cache[cache_key]
            if time.time() - timestamp < self.price_cache_ttl:
                return cached_price
        
        # Yeni fiyat al
        try:
            if position.exchange == "binance":
                if position.account_name in self.binance_apis:
                    price = await self.binance_apis[position.account_name].get_price(position.market_pair)
                else:
                    logger.error(f"Unknown account name: {position.account_name}")
                    price = 0
            elif position.exchange == "mexc":
                price = await self.mexc_api.get_price(position.market_pair)
            else:  # gate
                price = await self.gate_api.get_price(position.market_pair)
            
            # Fiyatı önbellekle
            if price > 0:
                self.price_cache[cache_key] = (price, time.time())
                return price
            
            return 0
        except Exception as e:
            logger.error(f"Error getting price for {position.symbol}: {str(e)}")
            return 0
    
    async def check_position_price(self, position: Position) -> bool:
        """Bir pozisyon için fiyat kontrolü yap"""
        try:
            current_price = await self.get_current_price(position)
            if current_price > 0:
                # Manuel güncelleme olduğunu belirt
                old_price = position.current_price
                position.update_price(current_price)
                
                # Manuel güncelleme zamanını kaydet
                self.last_price_update[f"{position.exchange}_{position.symbol}"] = time.time()
                
                # İlk kontrol veya önemli değişiklik varsa logla
                if position.age_seconds < 5:
                    logger.debug(f"Initial price check for {position.symbol}: ${current_price:.8f} (profit: {position.current_profit_pct:.2f}%)")
                elif abs(current_price - old_price) / old_price > 0.01:  # %1'den fazla değişim
                    logger.debug(f"Manual price update for {position.symbol}: ${old_price:.8f} -> ${current_price:.8f} ({position.current_profit_pct:+.2f}%)")
                
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking position price for {position.symbol}: {str(e)}")
            return False

    async def _generate_summary_report(self):
        """Periyodik özet rapor oluştur"""
        try:
            if not self.positions:
                return
            
            report = []
            report.append("\n" + "="*80)
            report.append(f"📊 POSITION SUMMARY REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("="*80)
            
            total_value = 0
            total_profit_usdt = 0
            positions_by_exchange = {}
            
            for position_key, position in self.positions.items():
                if position.status == "closed":
                    continue
                    
                # Exchange'e göre grupla
                if position.exchange not in positions_by_exchange:
                    positions_by_exchange[position.exchange] = []
                
                position_value = position.current_price * position.remaining_quantity
                entry_value = position.entry_price * position.remaining_quantity
                profit_usdt = position_value - entry_value
                
                positions_by_exchange[position.exchange].append({
                    "symbol": position.symbol,
                    "account": position.account_name if position.exchange == "binance" else "N/A",
                    "age_min": position.age_minutes,
                    "profit_pct": position.current_profit_pct,
                    "profit_usdt": profit_usdt,
                    "value": position_value,
                    "strategy": position.listing_source
                })
                
                total_value += position_value
                total_profit_usdt += profit_usdt
            
            # Exchange bazlı rapor
            for exchange, positions in positions_by_exchange.items():
                report.append(f"\n{exchange.upper()} POSITIONS ({len(positions)}):")
                report.append("-" * 40)
                
                for pos in sorted(positions, key=lambda x: x['profit_pct'], reverse=True):
                    account_str = f"/{pos['account']}" if pos['account'] != "N/A" else ""
                    report.append(
                        f"  {pos['symbol']}{account_str}: "
                        f"{pos['age_min']:.1f}min | "
                        f"{pos['profit_pct']:+.2f}% | "
                        f"${pos['profit_usdt']:+.2f} | "
                        f"Strategy: {pos['strategy']}"
                    )
            
            # Satış denemeleri özeti
            if hasattr(self, 'sell_attempts') and self.sell_attempts:
                report.append(f"\n\nRECENT SELL ATTEMPTS:")
                report.append("-" * 40)
                
                for symbol, attempts in self.sell_attempts.items():
                    recent_attempts = attempts[-5:]  # Son 5 deneme
                    success_count = sum(1 for a in recent_attempts if a['success'])
                    fail_count = len(recent_attempts) - success_count
                    
                    report.append(f"\n{symbol}: {success_count} success, {fail_count} failed")
                    for attempt in recent_attempts[-3:]:  # Son 3 deneme detayı
                        status = "✅" if attempt['success'] else "❌"
                        time_str = attempt['time'].strftime('%H:%M:%S')
                        error_str = f" - {attempt['error']}" if 'error' in attempt else ""
                        report.append(f"  {status} {time_str}: {attempt['reason']}{error_str}")
            
            # Genel özet
            report.append(f"\n\nTOTAL SUMMARY:")
            report.append("-" * 40)
            report.append(f"Active Positions: {len([p for p in self.positions.values() if p.status != 'closed'])}")
            report.append(f"Total Value: ${total_value:.2f}")
            report.append(f"Total Profit: ${total_profit_usdt:+.2f}")
            report.append("="*80 + "\n")
            
            # Raporu yazdır
            logger.info('\n'.join(report))
            
        except Exception as e:
            logger.error(f"Error generating summary report: {str(e)}")
    
    async def monitor_positions(self):
        """Aktif pozisyonları sürekli izle ve ticaret kararları ver"""
        last_summary_time = datetime.now()
        summary_interval_minutes = 5  # Her 5 dakikada özet rapor
        
        while True:
            try:
                # Aktif pozisyonumuz var mı diye kontrol et
                if not self.active_positions:
                    await asyncio.sleep(5)
                    continue
                
                # Her 5 dakikada bir özet rapor ver
                current_time = datetime.now()
                if (current_time - last_summary_time).total_seconds() >= summary_interval_minutes * 60:
                    await self._generate_summary_report()
                    last_summary_time = current_time

                # Periodic cleanup of memory structures
                self._cleanup_sell_attempts()
                self._prune_processed_signals()
                
                # SADECE PRIMARY HESAP POZİSYONLARINI DEĞERLENDİR
                for position_key in list(self.active_positions):
                    if position_key not in self.positions:
                        self.active_positions.remove(position_key)
                        continue
                    
                    position = self.positions[position_key]
                    
                    # PRIMARY HESAP DEĞİLSE ATLA - ÖNEMLİ!
                    if (
                        not self.is_friend_session
                        and position.exchange == "binance"
                        and position.account_name != "PRIMARY"
                    ):
                        continue  # Sadece PRIMARY değerlendirilir
                    
                    # Kapalı pozisyonları atla
                    if position.status == "closed":
                        self._close_position_immediately(position)
                        continue
                    
                    # WebSocket kontrolü ve fiyat güncellemesi
                    exchange_symbol_key = f"{position.exchange}_{position.symbol}"
                    last_update = self.last_price_update.get(exchange_symbol_key, 0)
                    current_time_seconds = time.time()
                    
                    # Son güncelleme 3 saniyeden eski mi?
                    if current_time_seconds - last_update > 3:
                        # Manuel kontrol zamanı geldi mi?
                        last_manual = self.last_manual_check.get(exchange_symbol_key, 0)
                        
                        if current_time_seconds - last_manual > 3:  # 3 saniyede bir manuel kontrol
                            logger.debug(
                                f"⚡ Manual price check for {position.symbol} on {position.exchange} "
                                f"(last update: {int(current_time_seconds - last_update)}s ago)"
                            )
                            
                            # Manuel fiyat kontrolü yap
                            updated = await self.check_position_price(position)
                            
                            if updated:
                                # Manuel kontrol zamanını kaydet
                                self.last_manual_check[exchange_symbol_key] = current_time_seconds
                    
                    # Pozisyonu değerlendir
                    self._schedule_position_evaluation(position, trigger="monitor")
                
                # Bir sonraki kontrolden önce bekle
                await asyncio.sleep(max(1.0, self.position_check_interval))
            
            except Exception as e:
                logger.error(f"Error monitoring positions: {str(e)}")
                logger.debug(traceback.format_exc())
                await asyncio.sleep(5)

    async def evaluate_position(self, position: Position, *, trigger: str = "direct"):
        """Bir pozisyonu değerlendir ve satıp satmamaya karar ver - MASTER/FOLLOWER YAPISI"""
        position_key = self._position_key(position)
        evaluation_lock = self._get_position_evaluation_lock(position_key)
        try:
            async with evaluation_lock:
                position = self.positions.get(position_key, position)

                # Kapalı pozisyonu değerlendirme
                if position.status == "closed":
                    return

                if position.exchange == "gate":
                    # Dust birikimini kontrol et; koşul sağlanırsa otomatik temizle
                    self._should_release_gate_dust(position)

                # PRIMARY OLMAYAN HESAPLARI DEĞERLENDİRME - ÖNEMLİ!
                if (
                    not self.is_friend_session
                    and position.exchange == "binance"
                    and position.account_name != "PRIMARY"
                ):
                    return  # Sadece PRIMARY değerlendirilir, diğerleri onu takip eder

                if self._should_run_balance_guard(position):
                    try:
                        if position.exchange == "binance":
                            if position.account_name in self.binance_apis:
                                api = self.binance_apis[position.account_name]
                                actual_balance = await api.get_balance(position.symbol)
                            else:
                                logger.error(f"Unknown account name: {position.account_name}")
                                return
                            if actual_balance < position.remaining_quantity * 0.1:
                                logger.warning(
                                    f"🧹 Closing {position.symbol} on {position.exchange} - insufficient balance "
                                    f"(have: {actual_balance:.8f}, expected: {position.remaining_quantity:.8f})"
                                )
                                self._close_position_immediately(position)
                                return
                        elif position.exchange == "mexc":
                            actual_balance = await self.mexc_api.get_balance(position.symbol)
                            if actual_balance < position.remaining_quantity * 0.1:
                                logger.warning(
                                    f"🧹 Closing {position.symbol} on {position.exchange} - insufficient balance "
                                    f"(have: {actual_balance:.8f}, expected: {position.remaining_quantity:.8f})"
                                )
                                self._close_position_immediately(position)
                                return
                        else:  # gate
                            try:
                                gate_symbol = position.symbol.upper()
                                now = time.time()
                                last_check = self.last_gate_balance_check.get(gate_symbol)
                                force_refresh = (
                                    last_check is None
                                    or (now - last_check) >= self.gate_balance_check_interval
                                )

                                if force_refresh:
                                    gate_snapshot = await self.gate_api.get_balance_snapshot(
                                        position.symbol, use_cache=False
                                    )
                                    self._gate_balance_snapshot_cache[gate_symbol] = gate_snapshot
                                    self.last_gate_balance_check[gate_symbol] = now
                                else:
                                    gate_snapshot = self._gate_balance_snapshot_cache.get(gate_symbol)
                                    if gate_snapshot is None:
                                        gate_snapshot = await self.gate_api.get_balance_snapshot(
                                            position.symbol, use_cache=False
                                        )
                                        self._gate_balance_snapshot_cache[gate_symbol] = gate_snapshot
                                        self.last_gate_balance_check[gate_symbol] = now
                                    else:
                                        logger.debug(
                                            f"[Gate rate limit] Using cached balance snapshot for {position.symbol}"
                                        )
                            except GateAPITemporaryError as exc:
                                logger.warning(
                                    f"Gate.io balance temporarily unavailable for {position.symbol}: {exc}; deferring evaluation"
                                )
                                return
                            except GateAPIHardError as exc:
                                logger.error(f"Gate.io balance error for {position.symbol}: {exc}")
                                return

                            if self._should_defer_gate_guard(position, gate_snapshot):
                                return

                            effective_total = float(gate_snapshot.effective_total)
                            if effective_total < position.remaining_quantity * 0.1:
                                logger.warning(
                                    f"🧹 Closing {position.symbol} on {position.exchange} - insufficient effective balance "
                                    f"(avail={float(gate_snapshot.available):.8f}, locked={float(gate_snapshot.locked):.8f}, "
                                    f"expected={position.remaining_quantity:.8f})"
                                )
                                self._close_position_immediately(position)
                                return

                    except Exception as e:
                        logger.error(
                            "Error checking balance for %s during %s evaluation: %s",
                            position.symbol,
                            trigger,
                            str(e),
                        )

                # Pozisyonun borsa kaynağına uygun stratejiyi seç
                listing_source = position.listing_source.lower() if position.listing_source else "unknown"

                if self._is_investment_position(position):
                    if (
                        position.trailing_stop_activated
                        and position.check_trailing_stop()
                        and not position.strategy_state.get("investment_trailing_triggered")
                    ):
                        position.strategy_state["investment_trailing_triggered"] = True
                        logger.info(
                            f"🛑 Investment trailing stop hit for {position.symbol} "
                            f"({position.exchange}); executing exit"
                        )
                        await self.sell_position_master_follower(
                            position,
                            "Investment trailing stop hit",
                        )
                        if position.status != "closed":
                            position.strategy_state.pop("investment_trailing_triggered", None)
                    return

                # Desteklenen bir kaynaksa o stratejiyi kullan
                if listing_source in self.strategies:
                    strategy = self.strategies[listing_source]
                else:
                    # Bilinmeyen kaynaklar için en hızlı çıkışı tercih et
                    strategy = self.strategies["bithumb"]
                    logger.critical(f"Unknown listing source '{listing_source}' for {position.symbol}, falling back to Bithumb strategy")

                # İlk kez strateji seçimi yapılıyorsa logla
                if not hasattr(position, '_strategy_logged'):
                    logger.info(f"Using {strategy.name} for {position.symbol} from {listing_source}")
                    position._strategy_logged = True

                # Stratejiyi çalıştır
                sell_now, reason, percentage_to_sell, target_profit = await strategy.evaluate(
                    position, self.market_conditions
                )

                # Satış kararı verdiyse ve satılacak miktar varsa
                if sell_now and percentage_to_sell > 0 and position.remaining_quantity > 0:

                    # Kritik satış kontrolü (süre dolmuş veya kar hedefi)
                    is_timeout = False
                    is_profit_target = False
                    age_minutes = position.age_minutes
                    age_seconds = position.age_seconds

                    if position.listing_source in {"binance", "binance_alpha"} and age_minutes >= 3.0:
                        is_timeout = True
                    elif position.listing_source == "bithumb" and age_minutes >= 10.0:
                        is_timeout = True
                    elif position.listing_source == "upbit" and age_seconds >= 7200.0:
                        is_timeout = True  # 2 saat max hold

                    if "Target" in reason and "reached" in reason:
                        is_profit_target = True

                    # Strateji değişikliğini logla
                    logger.debug(f"💡 Strategy {strategy.name} decision for {position.symbol}: {reason}")

                    sell_qty = position.remaining_quantity * percentage_to_sell

                    # Eğer çok küçük bir miktar kaldıysa, tamamını sat
                    if position.remaining_quantity - sell_qty < position.quantity * 0.05:
                        sell_qty = position.remaining_quantity
                        percentage_to_sell = 1.0

                    # MASTER/FOLLOWER SATIŞI - RETRY MEKANİZMASI İLE
                    if is_timeout or is_profit_target:
                        # Kritik satışlar için agresif retry
                        await self.sell_with_retry(position, percentage_to_sell, reason, max_retries=30)
                    else:
                        # Normal satış
                        if sell_qty >= position.remaining_quantity * 0.99:  # Tam satış
                            await self.sell_position_master_follower(position, reason)
                        else:  # Kısmi satış
                            await self.partial_sell_position_master_follower(position, percentage_to_sell, reason)

        except Exception as e:
            logger.error(f"Error evaluating position {position.symbol}: {str(e)}")
            logger.debug(traceback.format_exc())

