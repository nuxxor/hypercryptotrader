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


class PositionManagerExecutionMixin:
    async def sell_with_retry(self, position: Position, percentage: float, reason: str, max_retries: int = 30):
        """Satışı retry mekanizması ile dene"""
        if position.status == "closed":
            return

        symbol = position.symbol
        
        # Retry tracking için dictionary
        if not hasattr(self, 'retry_status'):
            self.retry_status = {}
        
        # Bu pozisyon için retry durumu
        position_key = f"{position.symbol}_{position.exchange}_{position.account_name}"
        
        # Daha önce başarılı olduysa tekrar deneme
        if position_key in self.retry_status and self.retry_status[position_key]['success']:
            return
        
        # İlk deneme mi kontrol et
        if position_key not in self.retry_status:
            self.retry_status[position_key] = {
                'attempts': 0,
                'success': False,
                'last_attempt': None
            }
        
        retry_info = self.retry_status[position_key]
        
        # Max retry kontrolü
        if retry_info['attempts'] >= max_retries:
            logger.critical(
                f"❌ Max retries ({max_retries}) reached for {symbol} on {position.exchange}. "
                "Closing position locally and requiring manual review."
            )
            self._close_position_immediately(position)
            retry_info['success'] = False
            return
        
        # Son denemeden bu yana 5 saniye geçti mi?
        if retry_info['last_attempt']:
            time_since_last = (datetime.now() - retry_info['last_attempt']).total_seconds()
            if time_since_last < 5:
                return  # Henüz 5 saniye geçmedi, bekle
        
        blocked_until = position.strategy_state.get("min_notional_blocked_until")
        if blocked_until:
            now = datetime.now()
            if now < blocked_until:
                wait_seconds = (blocked_until - now).total_seconds()
                warning_state = position.strategy_state.setdefault("min_notional_warning_state", {})
                last_logged = warning_state.get("last_logged")
                last_bucket = warning_state.get("last_bucket")
                current_bucket = int(wait_seconds // 30)

                should_log = False
                if not last_logged:
                    should_log = True
                elif (now - last_logged).total_seconds() >= 30:
                    should_log = True
                elif last_bucket != current_bucket:
                    should_log = True

                if should_log:
                    logger.warning(
                        f"Minimum notional block active for {symbol} on {position.exchange}; "
                        f"skipping retry for another {wait_seconds:.0f}s"
                    )
                    warning_state["last_logged"] = now
                    warning_state["last_bucket"] = current_bucket
                return

            # Blok süresi dolduysa throttle durumunu sıfırla
            position.strategy_state.pop("min_notional_warning_state", None)
            position.strategy_state.pop("min_notional_blocked_until", None)

        if position.exchange == "gate" and position.strategy_state.get("gate_dust_hold"):
            threshold = position.strategy_state["gate_dust_hold"].get("threshold", self.gate_min_order_value)
            logger.info(
                f"Gate.io dust hold active for {symbol}; waiting until value exceeds ${threshold:.2f}"
            )
            return

        # Retry attempt
        retry_info['attempts'] += 1
        retry_info['last_attempt'] = datetime.now()
        
        logger.warning(f"🔄 Retry #{retry_info['attempts']}/{max_retries} for {symbol} on {position.exchange}")
        
        # Master position satışı dene
        success = False
        try:
            # last_sell_attempt'i sıfırla (hemen deneyebilmek için)
            if hasattr(position, 'last_sell_attempt'):
                delattr(position, 'last_sell_attempt')
            
            if percentage >= 0.99:  # Tam satış
                # Önce master'ı sat
                master_success = await self.sell_position(position, reason)
                
                if master_success:
                    # Master başarılıysa follower'ları sat
                    await self.sell_followers(position, reason)
                    success = True
            else:  # Kısmi satış
                # Önce master'ı kısmi sat
                master_success = await self.partial_sell_position(position, percentage, reason)
                
                if master_success:
                    # Master başarılıysa follower'ları da kısmi sat
                    await self.sell_followers_partial(position, percentage, reason)
                    success = True
                    
        except Exception as e:
            logger.error(f"Error in retry attempt: {str(e)}")
        
        if success:
            retry_info['success'] = True
            logger.info(f"✅ Retry successful for {symbol} on attempt #{retry_info['attempts']}")
        else:
            logger.warning(f"⚠️ Retry #{retry_info['attempts']} failed for {symbol}, will retry in 5 seconds...")

    async def sell_followers(self, master_position: Position, reason: str):
        """Sadece follower pozisyonları sat (master zaten satıldı)"""
        symbol = master_position.symbol
        followers_to_sell = []

        # Symbol pozisyonlarını kontrol et (copy to avoid mutation during iteration)
        if symbol in self.symbol_positions:
            for position_key in list(self.symbol_positions[symbol]):
                if position_key in self.positions:
                    pos = self.positions[position_key]
                    
                    # Master değilse ve aktifse follower listesine ekle
                    if pos != master_position and pos.status == "active":
                        # Bakiye doğrulayarak gerçekten açık olup olmadığını kontrol et
                        actual_balance = await self._get_exchange_token_balance(pos.exchange, pos.symbol, pos.account_name)
                        if actual_balance == -1.0:
                            logger.warning(
                                f"🔍 Unable to confirm follower balance for {pos.exchange} ({pos.account_name}) "
                                f"{symbol}; will attempt sell regardless"
                            )
                        elif actual_balance <= 0:
                            logger.warning(
                                f"🔍 Skipping follower {pos.exchange} ({pos.account_name}) for {symbol}: "
                                f"zero balance detected during sell_followers"
                            )
                            self._close_position_immediately(pos)
                            continue

                        followers_to_sell.append(pos)
        
        if followers_to_sell:
            logger.info(f"📢 Selling {len(followers_to_sell)} follower positions for {symbol}")
            
            # Tüm follower'ları paralel olarak sat
            sell_tasks = []
            for follower_pos in followers_to_sell:
                logger.info(f"   - {follower_pos.exchange} ({follower_pos.account_name})")
                sell_tasks.append(self.sell_position(follower_pos, f"Following master: {reason}"))
            
            # Tüm satışları bekle
            results = await asyncio.gather(*sell_tasks, return_exceptions=True)
            
            # Sonuçları rapor et
            success_count = sum(1 for r in results if r is True)
            fail_count = len(results) - success_count
            
            logger.info(f"📊 Follower sell results: {success_count} success, {fail_count} failed")

    async def sell_followers_partial(self, master_position: Position, percentage: float, reason: str):
        """Sadece follower pozisyonları kısmi sat (master zaten satıldı)"""
        symbol = master_position.symbol
        followers_to_sell = []

        # Symbol pozisyonlarını kontrol et (copy to avoid mutation during iteration)
        if symbol in self.symbol_positions:
            for position_key in list(self.symbol_positions[symbol]):
                if position_key in self.positions:
                    pos = self.positions[position_key]
                    
                    # Master değilse ve aktifse follower listesine ekle
                    if pos != master_position and pos.status == "active":
                        balance_check = await self._get_exchange_token_balance(pos.exchange, pos.symbol, pos.account_name)
                        if balance_check == -1.0:
                            logger.warning(
                                f"🔍 Unable to confirm follower balance for {pos.exchange} ({pos.account_name}) "
                                f"{symbol}; will attempt partial sell regardless"
                            )
                        elif balance_check <= 0:
                            logger.warning(
                                f"🔍 Skipping follower {pos.exchange} ({pos.account_name}) for {symbol}: "
                                f"zero balance detected prior to partial sell"
                            )
                            self._close_position_immediately(pos)
                            continue
                        followers_to_sell.append(pos)
        
        if followers_to_sell:
            logger.info(f"📢 Partially selling {len(followers_to_sell)} follower positions ({percentage:.1f}%)")
            
            # Tüm follower'ları paralel olarak sat
            sell_tasks = []
            for follower_pos in followers_to_sell:
                check_balance = await self._get_exchange_token_balance(follower_pos.exchange, follower_pos.symbol, follower_pos.account_name)
                if check_balance == -1.0:
                    logger.warning(
                        f"🔍 Unable to confirm follower balance for {follower_pos.exchange} "
                        f"({follower_pos.account_name}) {symbol}; proceeding with partial sell"
                    )
                elif check_balance <= 0:
                    logger.warning(
                        f"🔍 Skipping follower {follower_pos.exchange} ({follower_pos.account_name}) for {symbol}: "
                        f"zero balance detected during sell_followers_partial"
                    )
                    self._close_position_immediately(follower_pos)
                    continue

                logger.info(f"   - {follower_pos.exchange} ({follower_pos.account_name})")
                sell_tasks.append(self.partial_sell_position(follower_pos, percentage, f"Following master: {reason}"))
            
            # Tüm satışları bekle
            results = await asyncio.gather(*sell_tasks, return_exceptions=True)
            
            # Sonuçları rapor et
            success_count = sum(1 for r in results if r is True)
            fail_count = len(results) - success_count
            
            logger.info(f"📊 Follower partial sell results: {success_count} success, {fail_count} failed")

    async def sell_position_master_follower(self, master_position: Position, reason: str):
        """Master pozisyonu sat ve tüm follower'ları da sat"""
        try:
            symbol = master_position.symbol
            logger.info(f"🚨 MASTER/FOLLOWER SELL INITIATED for {symbol}")
            logger.info(f"   Master: {master_position.exchange} ({master_position.account_name})")
            logger.info(f"   Reason: {reason}")
            
            # 1. ÖNCE MASTER POZİSYONU SAT (PRIMARY veya GATE/MEXC)
            master_result = await self.sell_position(master_position, reason)
            
            if not master_result:
                logger.error(f"❌ Master sell failed for {symbol}, aborting follower sells")
                return
            
            logger.info(f"✅ Master position sold successfully for {symbol}")
            
            # 2. TÜM FOLLOWER POZİSYONLARI BUL VE SAT
            followers_to_sell = []

            # Symbol pozisyonlarını kontrol et (copy to avoid mutation during iteration)
            if symbol in self.symbol_positions:
                for position_key in list(self.symbol_positions[symbol]):
                    if position_key in self.positions:
                        pos = self.positions[position_key]

                        # Master değilse ve aktifse follower listesine ekle
                        if pos != master_position and pos.status == "active":
                            balance_check = await self._get_exchange_token_balance(pos.exchange, pos.symbol, pos.account_name)
                            if balance_check == -1.0:
                                logger.warning(
                                    f"🔍 Unable to confirm follower balance for {pos.exchange} ({pos.account_name}) "
                                    f"{symbol}; will attempt sell regardless"
                                )
                            elif balance_check <= 0:
                                logger.warning(
                                    f"🔍 Skipping follower {pos.exchange} ({pos.account_name}) for {symbol}: "
                                    f"zero balance detected during master sell propagation"
                                )
                                self._close_position_immediately(pos)
                                continue
                            followers_to_sell.append(pos)
            
            if followers_to_sell:
                logger.info(f"📢 Found {len(followers_to_sell)} follower positions to sell")
                
                # Tüm follower'ları paralel olarak sat
                sell_tasks = []
                for follower_pos in followers_to_sell:
                    logger.info(f"   - {follower_pos.exchange} ({follower_pos.account_name})")
                    sell_tasks.append(self.sell_position(follower_pos, f"Following master: {reason}"))
                
                # Tüm satışları bekle
                results = await asyncio.gather(*sell_tasks, return_exceptions=True)
                
                # Sonuçları rapor et
                success_count = sum(1 for r in results if r is True)
                fail_count = len(results) - success_count
                
                logger.info(f"📊 Follower sell results: {success_count} success, {fail_count} failed")
            else:
                logger.info(f"No follower positions found for {symbol}")
                
        except Exception as e:
            logger.error(f"Error in master/follower sell: {str(e)}")
            logger.debug(traceback.format_exc())

    async def partial_sell_position_master_follower(self, master_position: Position, percentage: float, reason: str):
        """Master pozisyonunun bir kısmını sat ve tüm follower'ları da aynı oranda sat"""
        try:
            symbol = master_position.symbol
            logger.info(f"🚨 MASTER/FOLLOWER PARTIAL SELL ({percentage:.1f}%) for {symbol}")
            logger.info(f"   Master: {master_position.exchange} ({master_position.account_name})")
            logger.info(f"   Reason: {reason}")
            
            # 1. ÖNCE MASTER POZİSYONU SAT
            master_result = await self.partial_sell_position(master_position, percentage, reason)
            
            if not master_result:
                logger.error(f"❌ Master partial sell failed for {symbol}, aborting follower sells")
                return
            
            logger.info(f"✅ Master position partially sold ({percentage:.1f}%) for {symbol}")
            
            # 2. TÜM FOLLOWER POZİSYONLARI BUL VE SAT
            followers_to_sell = []

            # Symbol pozisyonlarını kontrol et (copy to avoid mutation during iteration)
            if symbol in self.symbol_positions:
                for position_key in list(self.symbol_positions[symbol]):
                    if position_key in self.positions:
                        pos = self.positions[position_key]

                        # Master değilse ve aktifse follower listesine ekle
                        if pos != master_position and pos.status == "active":
                            balance_check = await self._get_exchange_token_balance(pos.exchange, pos.symbol, pos.account_name)
                            if balance_check == -1.0:
                                logger.warning(
                                    f"🔍 Unable to confirm follower balance for {pos.exchange} ({pos.account_name}) "
                                    f"{symbol}; will attempt sell regardless"
                                )
                            elif balance_check <= 0:
                                logger.warning(
                                    f"🔍 Skipping follower {pos.exchange} ({pos.account_name}) for {symbol}: "
                                    f"zero balance detected during master sell propagation"
                                )
                                self._close_position_immediately(pos)
                                continue

                            followers_to_sell.append(pos)
            
            if followers_to_sell:
                logger.info(f"📢 Found {len(followers_to_sell)} follower positions to partially sell")
                
                # Tüm follower'ları paralel olarak sat
                sell_tasks = []
                for follower_pos in followers_to_sell:
                    logger.info(f"   - {follower_pos.exchange} ({follower_pos.account_name})")
                    sell_tasks.append(self.partial_sell_position(follower_pos, percentage, f"Following master: {reason}"))
                
                # Tüm satışları bekle
                results = await asyncio.gather(*sell_tasks, return_exceptions=True)
                
                # Sonuçları rapor et
                success_count = sum(1 for r in results if r is True)
                fail_count = len(results) - success_count
                
                logger.info(f"📊 Follower partial sell results: {success_count} success, {fail_count} failed")
            else:
                logger.info(f"No follower positions found for {symbol}")
                
        except Exception as e:
            logger.error(f"Error in master/follower partial sell: {str(e)}")
            logger.debug(traceback.format_exc())

    async def partial_sell_position(self, position: Position, percentage: float, reason: str) -> bool:
        """Pozisyonun belirli bir yüzdesini sat"""
        position_key = self._position_key(position)
        action_lock = self._get_position_action_lock(position_key)
        try:
            async with action_lock:
                position = self.positions.get(position_key, position)
                if position.status == "closed":
                    logger.debug("Skipping partial sell for closed position %s", position_key)
                    return False

                # Satılacak miktarı hesapla
                sell_qty = position.remaining_quantity * percentage
                
                if sell_qty <= 0:
                    logger.warning(f"Cannot sell zero or negative quantity for {position.symbol}")
                    return False

                if position.exchange == "gate" and not self._should_release_gate_dust(position):
                    threshold = position.strategy_state.get("gate_dust_hold", {}).get(
                        "threshold", self.gate_min_order_value
                    )
                    logger.info(
                        f"Deferring Gate.io partial sell for {position.symbol}: "
                        f"value ${position.current_price * sell_qty:.2f} below ${threshold:.2f}"
                    )
                    return False
                
                # MIN_NOTIONAL kontrolü
                if position.exchange in ["binance", "gate"]:
                    is_valid, adjusted_qty = await self._check_min_notional(position, sell_qty)
                    if not is_valid:
                        logger.error(f"Cannot meet minimum order requirements for {position.symbol}")
                        return False
                    sell_qty = adjusted_qty
                
                logger.info(f"🔸 Partial selling {percentage:.1f}% ({sell_qty:.8f}) of {position.symbol} on {position.exchange}. Reason: {reason}")
                
                # Satış emrini gerçekleştir
                if position.exchange == "binance":
                    # Doğru Binance API'yi seç
                    if position.account_name in self.binance_apis:
                        binance_api = self.binance_apis[position.account_name]
                    else:
                        logger.error(f"Unknown account name: {position.account_name}")
                        return False
                    
                    result = await binance_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    success = "orderId" in result
                
                elif position.exchange == "mexc":
                    result = await self.mexc_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    success = "orderId" in result
                
                else:  # gate
                    result = await self.gate_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    success = "id" in result
                
                if success:
                    # Kısmi satış bilgisini kaydet
                    position.partial_sells.append((datetime.now(), sell_qty, position.current_price))
                    
                    # Tüm miktar satıldıysa pozisyonu kapat
                    if position.remaining_quantity < position.quantity * 0.01:
                        self._mark_position_closed(position)
                        await self._finalize_trade(position)
                    else:
                        position.status = "partial_sold"
                    
                    # Satış başarılı log
                    profit_pct = position.current_profit_pct
                    sold_value = sell_qty * position.current_price
                    bought_value = sell_qty * position.entry_price
                    profit_usdt = sold_value - bought_value
                    
                    logger.info(f"✅ PARTIAL SOLD {percentage:.1f}% of {position.symbol} on {position.exchange} "
                            f"for ${profit_usdt:.2f} ({profit_pct:.2f}%)")
                    
                    # İstatistik güncelle
                    self.trade_stats["total_trades"] += 1
                    if profit_pct > 0:
                        self.trade_stats["successful_trades"] += 1
                    self.trade_stats["total_profit_usdt"] += profit_usdt
                    
                    return True
                else:
                    error_msg = result.get('msg') or result.get('message') or result.get('error') or 'Unknown error'
                    logger.error(f"❌ Failed partial sell on {position.exchange}: {error_msg}")
                    
                    # Satış denemesi kaydet
                    if position.symbol not in self.sell_attempts:
                        self.sell_attempts[position.symbol] = []
                    
                    self.sell_attempts[position.symbol].append({
                        "time": datetime.now(),
                        "reason": f"Partial {percentage:.1f}% - {reason}",
                        "success": False,
                        "error": error_msg
                    })
                    
                    return False
        
        except Exception as e:
            logger.error(f"Error selling partial position: {str(e)}")
            logger.debug(traceback.format_exc())
            return False

    async def sell_position(
        self,
        position: Position,
        reason: str,
        *,
        override_quantity: Optional[float] = None,
    ) -> bool:
        """Pozisyonu tamamen sat"""
        position_key = self._position_key(position)
        action_lock = self._get_position_action_lock(position_key)
        try:
            async with action_lock:
                position = self.positions.get(position_key, position)
                if position.status == "closed":
                    logger.debug("Skipping sell for closed position %s", position_key)
                    return False

                # Satış denemesi takibi için
                if position.symbol not in self.sell_attempts:
                    self.sell_attempts[position.symbol] = []
                
                # Son satış denemesinden bu yana geçen süreyi kontrol et
                if hasattr(position, 'last_sell_attempt'):
                    time_since_last_attempt = (datetime.now() - position.last_sell_attempt).total_seconds()
                    if time_since_last_attempt < 30: # 30 saniyeden az ise bekle
                        logger.debug(f"Skipping sell attempt for {position.symbol}, last attempt was {time_since_last_attempt:.1f}s ago")
                        return False
                
                # Satış denemesini kaydet
                position.last_sell_attempt = datetime.now()
                sell_attempt = {
                    "time": datetime.now(),
                    "reason": reason,
                    "success": False,
                    "error": None
                }
                # Satılacak miktar
                sell_qty = override_quantity if override_quantity is not None else position.remaining_quantity

                if override_quantity is None:
                    live_balance = await self._refresh_position_balance(
                        position,
                        allow_increase=True,
                        use_cache=False,
                    )

                    if live_balance >= 0:
                        tolerance = max(1e-8, position.quantity * 0.005)

                        if live_balance <= tolerance:
                            logger.warning(
                                f"No live balance detected for {position.symbol} on {position.exchange}; "
                                "will skip market sell"
                            )
                            sell_qty = 0.0
                        elif live_balance + tolerance < sell_qty:
                            logger.warning(
                                f"Adjusted sell quantity for {position.symbol} on {position.exchange} "
                                f"from {sell_qty:.8f} to live balance {live_balance:.8f}"
                            )
                            sell_qty = max(live_balance, 0.0)
                        elif live_balance > sell_qty + tolerance:
                            logger.warning(
                                f"Live balance ({live_balance:.8f}) exceeds tracked remaining quantity "
                                f"({sell_qty:.8f}) for {position.symbol}; using tracked amount to avoid oversell"
                            )
                            sell_qty = max(sell_qty, 0.0)
                            # Re-align without increasing tracked totals
                            self._reconcile_position_quantity(position, sell_qty, allow_increase=False)
                        else:
                            sell_qty = max(min(live_balance, sell_qty), 0.0)
                    else:
                        logger.warning(
                            f"Could not refresh live balance for {position.symbol} on {position.exchange}; "
                            "proceeding with tracked quantity"
                        )

                if sell_qty <= 0:
                    logger.info(f"No available quantity to sell for {position.symbol}; marking position closed")
                    sell_attempt["success"] = True
                    self._close_position_immediately(position)
                    self.sell_attempts[position.symbol].append(sell_attempt)
                    return True

                if position.exchange == "gate" and not self._should_release_gate_dust(position):
                    threshold = position.strategy_state.get("gate_dust_hold", {}).get(
                        "threshold", self.gate_min_order_value
                    )
                    logger.info(
                        f"Deferring Gate.io sell for {position.symbol}: value ${position.current_price * sell_qty:.2f} "
                        f"is below configured threshold ${threshold:.2f}"
                    )
                    sell_attempt["error"] = "Gate dust accumulation"
                    self.sell_attempts[position.symbol].append(sell_attempt)
                    return False

                # MIN_NOTIONAL kontrolü
                if position.exchange in ["binance", "gate"]:
                    is_valid, adjusted_qty = await self._check_min_notional(position, sell_qty)
                    if not is_valid:
                        logger.error(f"Cannot meet minimum order requirements for {position.symbol}")
                        sell_attempt["error"] = "Below minimum order value"
                        self.sell_attempts[position.symbol].append(sell_attempt)
                        return False
                    sell_qty = adjusted_qty
                
                logger.info(f"🔴 FULL SELL: {position.symbol} on {position.exchange} ({position.account_name}). Reason: {reason}")
                
                # Satış emrini gerçekleştir
                if position.exchange == "binance":
                    # Doğru Binance API'yi seç
                    if position.account_name in self.binance_apis:
                        binance_api = self.binance_apis[position.account_name]
                    else:
                        logger.error(f"Unknown account name: {position.account_name}")
                        sell_attempt["error"] = "Unknown account"
                        self.sell_attempts[position.symbol].append(sell_attempt)
                        return False
                        
                    result = await binance_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    success = "orderId" in result
                    
                elif position.exchange == "mexc":
                    result = await self.mexc_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    if isinstance(result, dict):
                        message_text = str(result.get("msg") or "").lower()
                        if result.get("code") == 30005 or "oversold" in message_text:
                            logger.error(
                                f"❌ MEXC reported oversold for {position.symbol}; attempting to resync balance before retry"
                            )
                            sell_attempt["error"] = "Oversold"
                            await self._refresh_position_balance(position, allow_increase=False, use_cache=False)
                            self.sell_attempts[position.symbol].append(sell_attempt)
                            return False
                    
                    success = "orderId" in result
                    
                else:  # gate
                    result = await self.gate_api.place_market_sell(
                        position.market_pair, 
                        sell_qty
                    )
                    
                    success = "id" in result
                
                if success:
                    sell_attempt["success"] = True

                    # Son kısmi satışı ekle
                    position.partial_sells.append((datetime.now(), sell_qty, position.current_price))
                    self._mark_position_closed(position)
                    
                    # Başarılı satışı logla
                    profit_pct = position.current_profit_pct
                    entry_value = position.entry_price * position.quantity
                    exit_value = sum(qty * price for _, qty, price in position.partial_sells)
                    profit_usdt = exit_value - entry_value
                    
                    logger.info(f"✅ SOLD {position.symbol} | {position.exchange} | "
                            f"Profit: ${profit_usdt:.2f} ({profit_pct:.2f}%) | "
                            f"Duration: {position.age_minutes:.1f}min")
                    
                    # İstatistik güncelle
                    self.trade_stats["total_trades"] += 1
                    if profit_pct > 0:
                        self.trade_stats["successful_trades"] += 1
                    self.trade_stats["total_profit_usdt"] += profit_usdt
                    
                    # Ortalama kârı güncelle
                    if self.trade_stats["total_trades"] > 0:
                        self.trade_stats["average_profit_pct"] = (
                            self.trade_stats["average_profit_pct"] * (self.trade_stats["total_trades"] - 1) + profit_pct
                        ) / self.trade_stats["total_trades"]
                    
                    # En iyi/en kötü ticareti güncelle
                    if profit_pct > self.trade_stats["best_trade"]["profit_pct"]:
                        self.trade_stats["best_trade"]["symbol"] = position.symbol
                        self.trade_stats["best_trade"]["profit_pct"] = profit_pct
                    
                    if profit_pct < self.trade_stats["worst_trade"]["profit_pct"] and profit_pct > 0:
                        self.trade_stats["worst_trade"]["symbol"] = position.symbol
                        self.trade_stats["worst_trade"]["profit_pct"] = profit_pct
                    
                    # Trade tamamlandı, sonuçları kaydet
                    await self._finalize_trade(position)
                    
                    self.sell_attempts[position.symbol].append(sell_attempt)
                    return True
                else:
                    error_msg = result.get('msg') or result.get('message') or result.get('error') or 'Unknown error'
                    logger.error(f"❌ Failed to sell on {position.exchange}: {error_msg}")
                    sell_attempt["error"] = error_msg
                    
                    # Detaylı hata bilgisi
                    if position.exchange == "binance":
                        # Binance hata kodları
                        if 'code' in result:
                            if result['code'] == -1013:
                                logger.error(f"LOT_SIZE error for {position.symbol}, disabling position temporarily")
                                position.last_sell_attempt = datetime.now() - timedelta(seconds=270)
                                sell_attempt["error"] = "LOT_SIZE error - retry in 5 min"
                            elif result['code'] == -2010:
                                logger.error(f"Insufficient balance for {position.symbol}")
                                sell_attempt["error"] = "Insufficient balance"
                    elif position.exchange == "gate":
                        # Gate.io hata kontrolleri
                        if 'label' in result:
                            if result['label'] == 'INVALID_AMOUNT':
                                logger.error(f"Invalid amount for {position.symbol} on Gate.io")
                                sell_attempt["error"] = "Invalid amount"
                            elif result['label'] == 'BALANCE_NOT_ENOUGH':
                                logger.error(f"Insufficient balance for {position.symbol} on Gate.io")
                                sell_attempt["error"] = "Insufficient balance"
                    
                    self.sell_attempts[position.symbol].append(sell_attempt)
                    return False
        
        except Exception as e:
            logger.error(f"Error selling position: {str(e)}")
            logger.debug(traceback.format_exc())
            
            if position.symbol in self.sell_attempts:
                self.sell_attempts[position.symbol].append({
                    "time": datetime.now(),
                    "reason": reason,
                    "success": False,
                    "error": str(e)
                })
            
            return False

    async def _finalize_trade(self, position: Position):
        """Trade'i sonlandır ve sonuçları kaydet"""
        try:
            # Son bakiyeyi kontrol et
            final_balance = await self._get_exchange_balance(
                position.exchange, position.account_name, force_refresh=True
            )
            
            # Bakiye değişimi ve kâr hesapla
            balance_change = final_balance - position.initial_balance
            
            # Entry ve exit değerleri
            entry_value = position.entry_price * position.quantity
            exit_value = sum(qty * price for _, qty, price in position.partial_sells)
            profit_usdt = exit_value - entry_value
            profit_pct = ((exit_value / entry_value) - 1) * 100 if entry_value > 0 else 0
            
            # Debug log
            logger.debug(f"Trade {position.trade_id} calculations:")
            logger.debug(f"  Initial balance: ${position.initial_balance:.2f}")
            logger.debug(f"  Final balance: ${final_balance:.2f}")
            logger.debug(f"  Balance change: ${balance_change:.2f}")
            logger.debug(f"  Entry value: ${entry_value:.2f}")
            logger.debug(f"  Exit value: ${exit_value:.2f}")
            logger.debug(f"  Profit USDT: ${profit_usdt:.2f}")
            logger.debug(f"  Profit %: {profit_pct:.2f}%")
            
            # Strateji detayları
            listing_source = position.listing_source.lower() if position.listing_source else "unknown"
            strategy_name = self.strategies[listing_source].name if listing_source in self.strategies else "unknown"
            
            # Market cap kategorisi
            market_cap_str = "Unknown"
            if position.market_cap is not None:
                if position.market_cap > 100_000_000:
                    market_cap_str = f"High (${position.market_cap/1_000_000:.2f}M)"
                elif position.market_cap > 70_000_000:
                    market_cap_str = f"Medium (${position.market_cap/1_000_000:.2f}M)"
                else:
                    market_cap_str = f"Low (${position.market_cap/1_000_000:.2f}M)"
            
            # Sonuçları kaydet
            trade_result = {
                "trade_id": position.trade_id,
                "symbol": position.symbol,
                "exchange": position.exchange,
                "account_name": position.account_name,
                "listing_source": position.listing_source,
                "strategy": strategy_name,
                "entry_time": position.entry_time.isoformat(),
                "exit_time": datetime.now().isoformat(),
                "entry_price": position.entry_price,
                "highest_price": position.highest_price,
                "quantity": position.quantity,
                "entry_value": entry_value,
                "exit_value": exit_value,
                "profit_usdt": profit_usdt,
                "profit_pct": profit_pct,
                "initial_balance": position.initial_balance,
                "final_balance": final_balance,
                "balance_change": balance_change,
                "market_cap": position.market_cap,
                "market_cap_category": market_cap_str,
                "is_krw_pair": position.is_krw_pair,
                "partial_sells": [
                    {"time": timestamp.isoformat(), "quantity": qty, "price": price}
                    for timestamp, qty, price in position.partial_sells
                ]
            }
            
            # Dosyaya kaydet
            trade_file = os.path.join(self.results_dir, f"{position.trade_id}.json")
            with open(trade_file, 'w') as f:
                json.dump(trade_result, f, indent=2)
            
            logger.info(f"Trade {position.trade_id} finalized and results saved to {trade_file}")
            
            # Özet rapor - DÜZGÜN HESAPLAMALARLA
            summary = f"""
            -----------------------------------------
            TRADE {position.trade_id} COMPLETED
            -----------------------------------------
            Symbol: {position.symbol} ({position.exchange}/{position.account_name})
            Strategy: {strategy_name}
            Market Cap: {market_cap_str}
            Is KRW Pair: {position.is_krw_pair}
            
            Initial Balance: ${position.initial_balance:.2f}
            Final Balance: ${final_balance:.2f}
            Balance Change: ${balance_change:.2f}
            
            Position Profit: ${profit_usdt:.2f} ({profit_pct:.2f}%)
            -----------------------------------------
            """
            logger.info(summary)
            
            if not self._has_open_positions():
                await self._log_all_exchange_balances(context=f"after {position.trade_id}")
            
        except Exception as e:
            logger.error(f"Error finalizing trade {position.trade_id}: {str(e)}")
            logger.debug(traceback.format_exc())

    async def evaluate_market_conditions(self):
        """Piyasa koşullarını periyodik olarak değerlendir"""
        while True:
            try:
                # Burada daha kapsamlı piyasa analizi eklenebilir
                # Örneğin BTC ve ETH gibi büyük coinlerin fiyat hareketlerini izle
                # Korku-açgözlülük endeksi, toplam market hacmi, trend analizleri vs.
                
                # Şimdilik basit bir veri yapısı döndür
                self.market_conditions = {
                    "overall_trend": "neutral",  # "bullish", "bearish", "neutral"
                    "volatility": "medium",      # "high", "medium", "low"
                    "timestamp": datetime.now().isoformat(),
                    "last_update": int(time.time())
                }
                
                await asyncio.sleep(self.market_check_interval)
            except Exception as e:
                logger.error(f"Error evaluating market conditions: {str(e)}")
                await asyncio.sleep(60)
