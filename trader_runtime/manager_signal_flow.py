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


class PositionManagerSignalMixin:
    async def _handle_position_websocket_message(
        self,
        data: dict,
        *,
        websocket=None,
    ) -> None:
        """Dispatch a decoded upstream WebSocket payload."""
        message_type = data.get("type", "unknown")

        if message_type == "new_position":
            logger.info(f"New position notification received: {data}")
            await self.handle_new_position(data)
        elif message_type == "ping":
            if websocket is not None:
                pong_message = {"type": "pong", "time": int(time.time())}
                await websocket.send(json.dumps(pong_message))
        elif message_type == "position_update":
            await self.handle_position_update(data)
        elif message_type in {"trade_result", "trade_report"}:
            self._record_trade_confirmations(data)
        elif message_type == "error":
            logger.error(
                f"Error message from WebSocket: {data.get('message', 'Unknown error')}"
            )
        elif message_type == "welcome":
            logger.info("Received welcome message from position WebSocket server")
        elif message_type == "connection_ack":
            logger.info("Connection acknowledged by WebSocket server")
        elif message_type == "status_update":
            logger.debug(f"Status update from server: {data}")
        elif message_type == "heartbeat":
            logger.debug("Heartbeat received from server")
        elif message_type == "signal":
            logger.info(f"Signal received: {data}")
            await self.handle_signal(data)
        else:
            logger.warning(f"Unknown message type: {message_type}")

    async def connect_to_position_websocket(self):
        """Pozisyon bildirimleri için WebSocket sunucusuna bağlan"""
        if not getattr(self, "position_ws_enabled", True):
            logger.info("Position WebSocket disabled; skipping upstream connection")
            return

        retry_count = 0
        max_retries = 100  # Uzun süre yeniden deneme yapmak için
        refused_consecutive = 0
        base_delay = self.position_ws_reconnect_delay
        max_delay = 60.0  # Max 60 seconds between retries
        current_delay = base_delay

        while retry_count < max_retries:
            try:
                logger.info(f"Connecting to position WebSocket at {self.position_ws_uri}...")
                
                async with websockets.connect(
                    self.position_ws_uri,
                    open_timeout=DEFAULT_POSITION_WS_OPEN_TIMEOUT,
                    close_timeout=DEFAULT_POSITION_WS_CLOSE_TIMEOUT,
                    ping_interval=DEFAULT_POSITION_WS_PING_INTERVAL,
                    ping_timeout=DEFAULT_POSITION_WS_PING_TIMEOUT,
                ) as websocket:
                    self.position_ws = websocket
                    self.position_ws_connected = True
                    refused_consecutive = 0
                    logger.info("Connected to position WebSocket")
                    
                    # Bağlantı başarılı mesajı gönder
                    register_message = {
                        "type": "register",
                        "client_type": "position_manager",
                        "version": "1.0.0"
                    }
                    await websocket.send(json.dumps(register_message))
                    
                    async for message in websocket:
                        try:
                            # Gelen mesajı logla - DEBUG seviyesinde
                            logger.debug(f"Received WebSocket message: {message}")
                            
                            data = json.loads(message)
                            await self._handle_position_websocket_message(
                                data,
                                websocket=websocket,
                            )
                                
                        except json.JSONDecodeError:
                            logger.error(f"Invalid JSON received: {message}")
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                            logger.debug(traceback.format_exc())
            
            except (websockets.exceptions.ConnectionClosed, InvalidStatus, ConnectionRefusedError) as e:
                logger.warning(
                    f"WebSocket connection error: {str(e)}. Reconnecting in {current_delay:.1f}s..."
                )
                self.position_ws_connected = False
                if isinstance(e, ConnectionRefusedError):
                    refused_consecutive += 1
                    if refused_consecutive >= 5:
                        logger.critical(
                            "Position WebSocket server still unreachable after 5 consecutive refusals; "
                            "please verify the upstream broadcaster"
                        )
                else:
                    refused_consecutive = 0
                    # Reset backoff on non-refused errors (connection was successful but dropped)
                    current_delay = base_delay

                # Exponential backoff with jitter
                jitter = random.uniform(0.5, 1.5)
                await asyncio.sleep(current_delay * jitter)
                current_delay = min(current_delay * 2, max_delay)
                retry_count += 1
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {str(e)}")
                logger.debug(traceback.format_exc())
                self.position_ws_connected = False
                # Exponential backoff with jitter
                jitter = random.uniform(0.5, 1.5)
                await asyncio.sleep(current_delay * 2 * jitter)
                current_delay = min(current_delay * 2, max_delay)
                retry_count += 1

        logger.critical(f"Failed to maintain WebSocket connection after {max_retries} attempts")
    
    async def handle_signal(self, signal_data: dict):
        """Sinyal mesajını işle ve 3 saniye sonra pozisyon kontrolü yap"""
        try:
            # Signal data'yı sakla (KRW pair bilgisi için)
            self._last_signal_data = signal_data
            
            # Token bilgilerini çıkart
            tokens = signal_data.get("tokens", [])
            if isinstance(tokens, str):
                tokens = [tokens]
                
            # Token bilgisi yoksa announcement'tan çıkarmayı dene
            if not tokens:
                announcement = signal_data.get("announcement", "")
                # Parantez içindeki token adlarını bul
                token_matches = re.findall(r'\(([A-Z0-9]+)\)', announcement)
                if token_matches:
                    tokens = token_matches
                    
            # Kaynak borsayı belirle (API sağlayıcılarına göre birden fazla alanı dene)
            source_candidates = [
                signal_data.get("source"),
                signal_data.get("exchange"),
                signal_data.get("origin"),
                signal_data.get("provider"),
            ]

            raw_source = "unknown"
            for candidate in source_candidates:
                if isinstance(candidate, str) and candidate.strip():
                    value = candidate.strip()
                    if value.lower() != "unknown":
                        raw_source = value
                        break
                    raw_source = value  # fallback if nothing better appears

            source_exchange = self._normalize_source(raw_source)
            is_test_signal = self._is_test_signal(raw_source, signal_data)
            allow_repeat = self._should_bypass_processed(signal_data, is_test_signal)

            if not tokens:
                logger.warning(f"No tokens found in signal: {signal_data}")
                return
                
            logger.info(f"Signal for tokens: {tokens} from {source_exchange}")
            
            # Her token için işlem başlat
            for token in tokens:
                # CACHE KONTROLÜ - YENİ!
                if not allow_repeat and self._is_signal_processed(token, source_exchange):
                    logger.warning(f"🔒 SKIPPING {token} from {source_exchange} - already processed before!")
                    continue

                pending_key = (token.upper(), source_exchange)
                if not allow_repeat:
                    pending_task = self.pending_signal_checks.get(pending_key)
                    if pending_task and not pending_task.done():
                        logger.debug(
                            f"Skipping duplicate position check for {token} from {source_exchange}; "
                            "previous task still running"
                        )
                        continue
                    
                logger.info(f"✅ Received NEW token signal: {token} from {source_exchange}")

                if is_test_signal:
                    self._schedule_test_auto_exit(token)
                
                # Pozisyon kontrolü için asenkron görev başlat
                task = asyncio.create_task(
                    self.check_position_after_delay(
                        token,
                        raw_source,
                        source_exchange,
                        signal_payload=signal_data,
                        is_test_signal=is_test_signal,
                        allow_repeat=allow_repeat,
                    )
                )
                self.pending_signal_checks[pending_key] = task

                def _finalize_pending_signal_check(
                    done: asyncio.Task,
                    key=pending_key,
                    ref=task,
                    token_symbol=token.upper(),
                    exchange_name=source_exchange,
                ):
                    if self.pending_signal_checks.get(key) is ref:
                        self.pending_signal_checks.pop(key, None)
                    try:
                        done.result()
                    except asyncio.CancelledError:
                        pass
                    except Exception as exc:
                        logger.error(
                            "Signal follow-up failed for %s on %s: %s",
                            token_symbol,
                            exchange_name,
                            exc,
                        )
                        logger.debug(traceback.format_exc())

                task.add_done_callback(_finalize_pending_signal_check)
                
        except Exception as e:
            logger.error(f"Error handling signal: {str(e)}")
            logger.debug(traceback.format_exc())

    async def check_position_after_delay(
        self,
        token: str,
        raw_source: str,
        listing_source: str,
        *,
        signal_payload: Optional[dict] = None,
        is_test_signal: bool = False,
        allow_repeat: bool = False,
    ):
        """Belirli bir token için konfigüre edilen gecikme sonrasında pozisyon kontrolü yap"""
        try:
            delay = max(self.position_check_delay, 0.0)
            if delay > 0:
                logger.info(f"Will check for {token} position in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
            else:
                logger.info(f"Will check for {token} position immediately (delay={delay})")

            payload: Dict[str, Any] = {}
            if isinstance(signal_payload, dict):
                payload = signal_payload
            elif hasattr(self, "_last_signal_data") and isinstance(self._last_signal_data, dict):
                payload = self._last_signal_data

            signal_alert_type = payload.get("alert_type")
            signal_amount_millions = payload.get("amount_millions")

            if signal_amount_millions is None and isinstance(payload.get("amount"), (int, float)):
                signal_amount_millions = float(payload["amount"])

            if signal_amount_millions is None and isinstance(payload.get("amount_formatted"), str):
                try:
                    numeric_part = re.sub(r"[^\d\.]", "", payload["amount_formatted"])
                    if numeric_part:
                        signal_amount_millions = float(numeric_part)
                except ValueError:
                    signal_amount_millions = None
            
            # Signal data'dan KRW pair bilgisini al (varsa)
            is_krw_pair = bool(payload.get("has_krw", False))
            listing_type = payload.get("listing_type")
            if isinstance(listing_type, str) and listing_type.upper() == "KRW":
                is_krw_pair = True
            
            # TÜM BORSALARI KONTROL ET VE RAPOR VER
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 CHECKING ALL EXCHANGES FOR {token}")
            logger.info(f"{'='*60}")
            normalized_source = listing_source or "unknown"
            display_source = normalized_source.upper()
            if raw_source and raw_source.lower() != normalized_source:
                display_source = f"{display_source} (raw: {raw_source})"
            logger.info(f"Signal source: {display_source}")
            logger.info(f"Is KRW pair: {is_krw_pair}")
            logger.info(f"{'='*60}\n")
            
            positions_found = []
            
            # 1. MEXC KONTROLÜ
            if self.mexc_api:
                logger.info(f"1️⃣ Checking MEXC...")
                mexc_market_pair = f"{token}USDT"
                mexc_token_balance = 0.0
                max_checks = max(1, self.position_check_max_retries)
                for attempt in range(max_checks):
                    mexc_token_balance = await self.mexc_api.get_balance(token, use_cache=False)
                    logger.info(f"   MEXC {token} balance: {mexc_token_balance:.8f}")
                    if mexc_token_balance > 0:
                        break
                    if attempt < max_checks - 1:
                        retry_sleep = max(self.position_check_retry_interval, 0.0)
                        if retry_sleep > 0:
                            logger.info(
                                "   ⏳ No %s found on MEXC yet (attempt %d/%d); retrying in %.1fs",
                                token,
                                attempt + 1,
                                max_checks,
                                retry_sleep,
                            )
                            await asyncio.sleep(retry_sleep)
                        else:
                            logger.info(
                                "   ⏳ No %s found on MEXC yet (attempt %d/%d); retrying immediately",
                                token,
                                attempt + 1,
                                max_checks,
                            )
                if mexc_token_balance > 0:
                    confirmation_ok = self._has_recent_trade_confirmation(token, "mexc")
                    if not confirmation_ok and not (is_test_signal or self.allow_unconfirmed_positions):
                        logger.info(
                            "   ⚪ MEXC balance detected but no confirmed trade; skipping auto-position"
                        )
                    else:
                        if not confirmation_ok:
                            logger.warning(
                                "   ⚠️ MEXC balance detected without trade confirmation; auto-positioning anyway"
                            )
                        confirmation_key = (token.upper(), "mexc", "PRIMARY")
                        confirmation_info = self.trade_confirmations.get(confirmation_key)
                        confirmed_price, confirmed_qty = self._extract_confirmation_metrics(
                            (confirmation_info or {}).get("raw")
                        )
                        if confirmed_qty is None and confirmation_info:
                            try:
                                confirmed_qty = float(confirmation_info.get("amount", 0.0))
                            except (TypeError, ValueError):
                                confirmed_qty = None

                        token_price = await self.mexc_api.get_price(mexc_market_pair)
                        logger.info(f"   MEXC {token} price: ${token_price:.8f}")
                        value = mexc_token_balance * token_price
                        logger.info(f"   ✅ Position value: ${value:.2f}")
                        
                        positions_found.append({
                            "exchange": "mexc",
                            "balance": mexc_token_balance,
                            "price": token_price,
                            "entry_price": confirmed_price or token_price,
                            "confirmed_quantity": confirmed_qty,
                            "value": value,
                            "account": "PRIMARY"
                        })
                else:
                    logger.info(
                        "   ❌ No %s found on MEXC after %d balance checks",
                        token,
                        max_checks,
                    )
            else:
                logger.info("1️⃣ Skipping MEXC (disabled)")
            
            # 2. BINANCE HESAPLARINI KONTROL ET
            logger.info(f"\n2️⃣ Checking Binance accounts...")
            for account_name, binance_api in self.binance_apis.items():
                market_pair = f"{token}USDT"
                binance_token_balance = await binance_api.get_balance(token, use_cache=False)
                
                if binance_token_balance > 0:
                    account_key = account_name.upper()
                    confirmation_ok = self._has_recent_trade_confirmation(token, "binance", account_key)
                    if not confirmation_ok and not (is_test_signal or self.allow_unconfirmed_positions):
                        logger.info(
                            f"   ⚪ {account_name}: balance present but no confirmed trade; skipping"
                        )
                        continue

                    if not confirmation_ok:
                        logger.warning(
                            f"   ⚠️ {account_name}: balance detected without trade confirmation; auto-positioning anyway"
                        )

                    confirmation_key = (token.upper(), "binance", account_key)
                    confirmation_info = self.trade_confirmations.get(confirmation_key)
                    confirmed_price, confirmed_qty = self._extract_confirmation_metrics(
                        (confirmation_info or {}).get("raw")
                    )
                    if confirmed_qty is None and confirmation_info:
                        try:
                            confirmed_qty = float(confirmation_info.get("amount", 0.0))
                        except (TypeError, ValueError):
                            confirmed_qty = None

                    logger.info(f"   {account_name}: {binance_token_balance:.8f} {token}")
                    token_price = await binance_api.get_price(market_pair)
                    value = binance_token_balance * token_price
                    logger.info(f"   ✅ Price: ${token_price:.8f}, Value: ${value:.2f}")
                    
                    positions_found.append({
                        "exchange": "binance",
                        "balance": binance_token_balance,
                        "price": token_price,
                        "entry_price": confirmed_price or token_price,
                        "confirmed_quantity": confirmed_qty,
                        "value": value,
                        "account": account_name
                    })
                else:
                    logger.info(f"   {account_name}: No {token} found")
            
            # 3. GATE.IO KONTROLÜ
            if self.gate_api:
                logger.info(f"\n3️⃣ Checking Gate.io...")
                gate_market_pair = f"{token}_USDT"
                gate_snapshot: Optional[BalanceSnapshot] = None
                gate_token_balance = 0.0
                for attempt in range(1, self.gate_balance_retry_attempts + 1):
                    try:
                        gate_snapshot = await self.gate_api.get_balance_snapshot(token)
                        gate_token_balance = float(gate_snapshot.effective_total)
                        if attempt > 1:
                            logger.info(
                                f"   Gate.io {token} balance recovered on retry {attempt}"
                            )
                        logger.info(
                            f"   Gate.io {token} balance: {gate_token_balance:.8f} "
                            f"(avail={float(gate_snapshot.available):.8f}, locked={float(gate_snapshot.locked):.8f})"
                        )
                        break
                    except GateAPIHardError as exc:
                        logger.error(f"   Gate.io balance error for {token}: {exc}")
                        gate_token_balance = 0.0
                        gate_snapshot = None
                        break
                    except GateAPITemporaryError as exc:
                        if attempt >= self.gate_balance_retry_attempts:
                            logger.warning(
                                f"   Gate.io balance check deferred for {token}: {exc}"
                            )
                        else:
                            logger.warning(
                                f"   Gate.io balance attempt {attempt}/{self.gate_balance_retry_attempts} failed for {token}: {exc}; "
                                f"retrying in {self.gate_balance_retry_delay:.1f}s"
                            )
                            await asyncio.sleep(self.gate_balance_retry_delay)

                if gate_token_balance > 0:
                    confirmation_ok = self._has_recent_trade_confirmation(token, "gate")
                    if not confirmation_ok and not (is_test_signal or self.allow_unconfirmed_positions):
                        logger.info(
                            "   ⚪ Gate.io balance detected but no confirmed trade; skipping auto-position"
                        )
                    else:
                        if not confirmation_ok:
                            logger.warning(
                                "   ⚠️ Gate.io balance detected without trade confirmation; auto-positioning anyway"
                            )
                        token_price = await self.gate_api.get_price(gate_market_pair)
                        logger.info(f"   Gate.io {token} price: ${token_price:.8f}")
                        value = gate_token_balance * token_price
                        logger.info(f"   ✅ Position value: ${value:.2f}")

                        confirmation_key = (token.upper(), "gate", "PRIMARY")
                        confirmation_info = self.trade_confirmations.get(confirmation_key)
                        confirmed_price, confirmed_qty = self._extract_confirmation_metrics(
                            (confirmation_info or {}).get("raw")
                        )
                        if confirmed_qty is None and confirmation_info:
                            try:
                                confirmed_qty = float(confirmation_info.get("amount", 0.0))
                            except (TypeError, ValueError):
                                confirmed_qty = None
                        
                        positions_found.append({
                            "exchange": "gate",
                            "balance": gate_token_balance,
                            "price": token_price,
                            "entry_price": confirmed_price or token_price,
                            "confirmed_quantity": confirmed_qty,
                            "value": value,
                            "account": "PRIMARY"
                        })
                else:
                    logger.info(f"   ❌ No {token} found on Gate.io")
            else:
                logger.info("\n3️⃣ Skipping Gate.io (disabled)")
            
            # ÖZET RAPOR
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 SUMMARY FOR {token}")
            logger.info(f"{'='*60}")
            
            if positions_found:
                total_value = sum(p['value'] for p in positions_found)
                logger.info(f"Total positions found: {len(positions_found)}")
                logger.info(f"Total value: ${total_value:.2f}")
                
                # TÜM POZİSYONLARI OLUŞTUR - ÖNEMLİ DEĞİŞİKLİK!
                logger.info(f"\n✅ Creating positions on ALL exchanges...")
                created_position = False

                for pos in positions_found:
                    logger.info(f"\n🎯 Creating position on {pos['exchange'].upper()} ({pos['account']})...")
                    position_data = {
                        "symbol": token,
                        "exchange": pos['exchange'],
                        "entry_price": pos.get('entry_price') or pos['price'],
                        "quantity": pos['balance'],
                        "order_id": "auto_detected",
                        "listing_source": listing_source,
                        "account_name": pos['account'],
                        "is_krw_pair": is_krw_pair,
                        "confirmed_quantity": pos.get('confirmed_quantity'),
                        "alert_type": signal_alert_type,
                        "amount_millions": signal_amount_millions,
                    }
                    await self.handle_new_position(position_data)
                    self._consume_trade_confirmation(token, pos['exchange'], pos['account'])
                    created_position = True

                if created_position and not allow_repeat:
                    self._mark_signal_processed(token, listing_source)
            else:
                existing_position_keys: List[str] = []
                token_upper = token.upper()
                for symbol_key, position_keys in self.symbol_positions.items():
                    if symbol_key.upper() == token_upper:
                        existing_position_keys.extend(
                            key for key in position_keys if key in self.active_positions
                        )

                if existing_position_keys:
                    logger.info(
                        f"⚪ No new {token} balances detected in this scan; "
                        f"{len(existing_position_keys)} position(s) already active"
                    )
                else:
                    logger.info(f"❌ No {token} positions found on any exchange")
                    logger.info(f"ℹ️ {token} signal handled with no trades (awaiting future listings)")
            
            logger.info(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"Error checking positions for {token}: {str(e)}")
            logger.debug(traceback.format_exc())

    async def handle_new_position(self, position_data: dict):
        """Yeni bir pozisyon bildirimi al ve kaydet"""
        try:
            logger.debug(f"Processing new position: {position_data}")
            
            symbol = position_data.get("symbol")
            exchange = position_data.get("exchange")
            listing_source = position_data.get("listing_source", "unknown").lower()
            entry_price = float(position_data.get("entry_price", 0))
            quantity = float(position_data.get("quantity", 0))
            order_id = position_data.get("order_id", "unknown")
            account_name = position_data.get("account_name", "PRIMARY")
            confirmed_quantity = position_data.get("confirmed_quantity")
            alert_type_raw = position_data.get("alert_type")
            alert_type_normalized = alert_type_raw.lower() if isinstance(alert_type_raw, str) else None
            amount_millions = position_data.get("amount_millions")
            try:
                amount_millions = float(amount_millions) if amount_millions is not None else None
            except (TypeError, ValueError):
                amount_millions = None
            try:
                if confirmed_quantity is not None:
                    confirmed_quantity = float(confirmed_quantity)
                    if confirmed_quantity > 0:
                        quantity = max(quantity, confirmed_quantity)
            except (TypeError, ValueError):
                confirmed_quantity = None
            
            # Upbit için KRW pair kontrolü
            is_krw_pair = position_data.get("is_krw_pair", False)
            
            # Pozisyon geçerli mi kontrol et
            if not symbol or not exchange or entry_price <= 0 or quantity <= 0:
                logger.error(f"Invalid position data: {position_data}")
                return
            
            # Benzersiz pozisyon anahtarı oluştur - ÖNEMLİ DEĞİŞİKLİK!
            position_key = f"{symbol}_{exchange}_{account_name}"
            
            # Eğer bu anahtar zaten varsa, güncelle
            if position_key in self.positions:
                logger.debug(f"Updating existing position for {position_key}")
                existing_position = self.positions[position_key]
                
                # Gerçek bakiye üzerinden pozisyonu senkronize et
                prev_total_qty = existing_position.quantity
                prev_remaining = existing_position.remaining_quantity
                sold_qty = max(prev_total_qty - prev_remaining, 0.0)

                new_balance = quantity
                delta = new_balance - prev_remaining
                new_total_quantity = max(new_balance + sold_qty, 0.0)

                existing_position.quantity = new_total_quantity

                confirmed_entry_price = position_data.get("entry_price")
                try:
                    confirmed_entry_price = float(confirmed_entry_price) if confirmed_entry_price else 0.0
                except (TypeError, ValueError):
                    confirmed_entry_price = 0.0

                if delta > 1e-12 and confirmed_entry_price > 0:
                    existing_cost = existing_position.entry_price * prev_total_qty
                    additional_cost = confirmed_entry_price * delta
                    total_after_delta = prev_total_qty + delta
                    if total_after_delta > 0:
                        existing_position.entry_price = (existing_cost + additional_cost) / total_after_delta
                elif prev_total_qty <= 1e-12 and confirmed_entry_price > 0:
                    existing_position.entry_price = confirmed_entry_price

                logger.debug(
                    f"Updated position {position_key}: balance={new_balance}, total_qty={existing_position.quantity}, "
                    f"delta={delta}, entry_price={existing_position.entry_price}"
                )
                return
            
            # Başlangıç bakiyesini sipariş sonrası snapshot'tan tahmin et
            post_entry_balance = await self._get_exchange_balance(
                exchange, account_name, force_refresh=True
            )
            if post_entry_balance is None:
                post_entry_balance = 0.0
            entry_value = entry_price * quantity if entry_price > 0 and quantity > 0 else 0.0
            initial_balance_estimate = post_entry_balance + entry_value
            logger.debug(
                "Initial %s balance estimate for %s: pre-trade=%.2f (post-entry %.2f, entry_value %.2f)",
                exchange,
                account_name,
                initial_balance_estimate,
                post_entry_balance,
                entry_value,
            )
            
            # Yeni pozisyon oluştur
            entry_time = datetime.now()
            position = Position(
                symbol=symbol,
                exchange=exchange,
                entry_price=entry_price,
                quantity=quantity,
                entry_time=entry_time,
                order_id=order_id,
                listing_source=listing_source,
                account_name=account_name,
                alert_type=alert_type_raw,
                amount_millions=amount_millions,
            )
            if alert_type_normalized:
                position.strategy_state["alert_type"] = alert_type_normalized
            if amount_millions is not None:
                position.strategy_state["amount_millions"] = amount_millions
            
            # Trade ID ve başlangıç bakiyesi ata
            position.trade_id = self._get_next_trade_id()
            position.initial_balance = initial_balance_estimate
            position.post_entry_balance = post_entry_balance
            
            # Upbit KRW pair bilgisini ayarla
            position.is_krw_pair = is_krw_pair
            
            # Market cap'i CoinMarketCap'ten al
            market_cap_display = "n/a"
            if self.cmc_api:
                market_cap = self.cmc_api.get_market_cap(symbol)
                if market_cap is not None:
                    position.market_cap = market_cap
                    market_cap_display = f"${market_cap/1_000_000:.2f}M"
                else:
                    logger.warning(f"Market cap not found for {symbol}")
            elif position.market_cap is not None:
                market_cap_display = f"${position.market_cap/1_000_000:.2f}M"
            
            # Pozisyonu sakla - ÖNEMLİ DEĞİŞİKLİK!
            self.positions[position_key] = position
            self.active_positions.add(position_key)
            
            # Symbol bazlı pozisyon listesini güncelle
            if symbol not in self.symbol_positions:
                self.symbol_positions[symbol] = []
            self.symbol_positions[symbol].append(position_key)
            
            # Fiyat akışına abone ol
            if exchange == "binance":
                if account_name in self.binance_apis:
                    self.binance_apis[account_name].subscribe_price(symbol, self.handle_price_update)
            elif exchange == "mexc":
                if self.mexc_api:
                    self.mexc_api.subscribe_price(symbol, self.handle_price_update)
            else:  # gate
                if self.gate_api:
                    self.gate_api.subscribe_price(symbol, self.handle_price_update)
            
            logger.info(
                f"🟢 Opened {symbol} on {exchange.upper()} ({account_name}) | "
                f"entry {entry_price:.8f} | qty {quantity:.6f} | trade {position.trade_id} | "
                f"market cap {market_cap_display}"
            )
            
            # İlk fiyat kontrolü yap
            await self.check_position_price(position)

            if position.exchange == "gate":
                self._should_release_gate_dust(position)

            if self._is_investment_position(position):
                self._schedule_investment_tasks(position)
            
        except Exception as e:
            logger.error(f"Error handling new position: {str(e)}")
            logger.debug(traceback.format_exc())
    
    def _position_key(self, position: Position) -> str:
        return f"{position.symbol}_{position.exchange}_{position.account_name}"

    def _get_async_lock(self, store: Dict[str, asyncio.Lock], key: str) -> asyncio.Lock:
        lock = store.get(key)
        if lock is None:
            lock = asyncio.Lock()
            store[key] = lock
        return lock

    def _get_position_evaluation_lock(self, position_key: str) -> asyncio.Lock:
        return self._get_async_lock(self._position_evaluation_locks, position_key)

    def _get_position_action_lock(self, position_key: str) -> asyncio.Lock:
        return self._get_async_lock(self._position_action_locks, position_key)

    def _has_tracked_position(
        self,
        symbol: str,
        *,
        exchange: Optional[str] = None,
        account_name: Optional[str] = None,
        exclude_key: Optional[str] = None,
    ) -> bool:
        for position_key in self.symbol_positions.get(symbol, []):
            if position_key == exclude_key:
                continue
            position = self.positions.get(position_key)
            if not position or position.status == "closed":
                continue
            if exchange and position.exchange != exchange:
                continue
            if account_name and position.account_name != account_name:
                continue
            return True
        return False

    def _remove_symbol_position_reference(self, position_key: str, symbol: str) -> None:
        references = self.symbol_positions.get(symbol)
        if not references:
            return
        try:
            references.remove(position_key)
        except ValueError:
            return
        if not references:
            self.symbol_positions.pop(symbol, None)

    def _unsubscribe_price_updates(self, position: Position) -> None:
        try:
            if position.exchange == "binance":
                api = self.binance_apis.get(position.account_name)
                if api:
                    api.unsubscribe_price(position.symbol, self.handle_price_update)
            elif position.exchange == "mexc" and self.mexc_api:
                self.mexc_api.unsubscribe_price(position.symbol, self.handle_price_update)
            elif position.exchange == "gate" and self.gate_api:
                self.gate_api.unsubscribe_price(position.symbol, self.handle_price_update)
        except Exception as exc:
            logger.warning(
                "Failed to unsubscribe %s price stream for %s (%s): %s",
                position.symbol,
                position.exchange,
                position.account_name,
                exc,
            )

    def _mark_position_closed(self, position: Position, *, unsubscribe_price: bool = True) -> None:
        position_key = self._position_key(position)
        position.status = "closed"
        self._cancel_investment_tasks_by_position(position)
        self.active_positions.discard(position_key)
        self._position_evaluation_pending.discard(position_key)
        self._last_position_balance_guard.pop(position_key, None)
        self.retry_status.pop(position_key, None)
        self._remove_symbol_position_reference(position_key, position.symbol)

        if position.exchange == "gate":
            gate_symbol = position.symbol.upper()
            self.gate_dust_buffer.pop(gate_symbol, None)
            position.strategy_state.pop("gate_dust_hold", None)
            if not self._has_tracked_position(position.symbol, exchange="gate"):
                self.last_gate_balance_check.pop(gate_symbol, None)
                self._gate_balance_snapshot_cache.pop(gate_symbol, None)

        exchange_symbol_key = f"{position.exchange}_{position.symbol}"
        if not self._has_tracked_position(
            position.symbol,
            exchange=position.exchange,
            account_name=position.account_name,
        ):
            self.last_price_update.pop(exchange_symbol_key, None)
            self.last_manual_check.pop(exchange_symbol_key, None)
            if unsubscribe_price:
                self._unsubscribe_price_updates(position)

        current_task = asyncio.current_task()
        task = self._position_evaluation_tasks.get(position_key)
        if task and task is not current_task and not task.done():
            task.cancel()
        elif task is None or task.done() or task is current_task:
            self._position_evaluation_tasks.pop(position_key, None)

        self._position_evaluation_locks.pop(position_key, None)
        self._position_action_locks.pop(position_key, None)

    def _schedule_position_evaluation(
        self,
        position: Position,
        *,
        trigger: str,
    ) -> Optional[asyncio.Task]:
        if not position or position.status == "closed":
            return None

        position_key = self._position_key(position)
        existing = self._position_evaluation_tasks.get(position_key)
        if existing and not existing.done():
            self._position_evaluation_pending.add(position_key)
            return existing

        task = asyncio.create_task(self._evaluate_position_task(position_key, trigger))
        self._position_evaluation_tasks[position_key] = task
        task.add_done_callback(
            lambda completed, key=position_key: self._finish_position_evaluation_task(key, completed)
        )
        return task

    async def _evaluate_position_task(self, position_key: str, trigger: str) -> None:
        position = self.positions.get(position_key)
        if not position or position.status == "closed":
            return
        await self.evaluate_position(position, trigger=trigger)

    def _finish_position_evaluation_task(self, position_key: str, task: asyncio.Task) -> None:
        if self._position_evaluation_tasks.get(position_key) is task:
            self._position_evaluation_tasks.pop(position_key, None)

        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Position evaluation task failed for %s: %s", position_key, exc)
            logger.debug(traceback.format_exc())

        if position_key in self._position_evaluation_pending:
            self._position_evaluation_pending.discard(position_key)
            position = self.positions.get(position_key)
            if position and position.status != "closed":
                self._schedule_position_evaluation(position, trigger="coalesced")

    def _should_run_balance_guard(self, position: Position) -> bool:
        position_key = self._position_key(position)
        interval = (
            self.gate_balance_check_interval
            if position.exchange == "gate"
            else self.position_balance_guard_interval
        )
        now = time.time()
        last_check = self._last_position_balance_guard.get(position_key)
        if last_check is not None and now - last_check < interval:
            return False
        self._last_position_balance_guard[position_key] = now
        return True

    def _is_investment_position(self, position: Position) -> bool:
        source = (position.listing_source or "").lower()
        alert = (position.alert_type or "").lower() if position.alert_type else ""
        return source == "investment" or alert in {"investment", "partnership"}

    def _register_investment_task(self, position_key: str, task_name: str, task: asyncio.Task) -> None:
        tasks = self.investment_tasks.setdefault(position_key, {})
        existing = tasks.get(task_name)
        if existing and not existing.done():
            existing.cancel()
        tasks[task_name] = task

        def _cleanup(done: asyncio.Task):
            entry = self.investment_tasks.get(position_key)
            if not entry:
                return
            if entry.get(task_name) is done:
                entry.pop(task_name, None)
                if not entry:
                    self.investment_tasks.pop(position_key, None)

        task.add_done_callback(_cleanup)

    def _cancel_investment_tasks_by_key(self, position_key: str) -> None:
        tasks = self.investment_tasks.pop(position_key, {})
        for task in tasks.values():
            task.cancel()

    def _cancel_investment_tasks_by_position(self, position: Position) -> None:
        self._cancel_investment_tasks_by_key(self._position_key(position))

    def _schedule_investment_tasks(self, position: Position) -> None:
        position_key = self._position_key(position)
        self._cancel_investment_tasks_by_key(position_key)

        trailing_delay = max(0.0, self.investment_trailing_delay)
        trailing_percent = max(0.0, self.investment_trailing_percent)
        exit_delay = max(0.0, self.investment_force_exit_delay)

        if trailing_percent > 0 and trailing_delay >= 0:
            trailing_task = asyncio.create_task(
                self._activate_investment_trailing_stop(position_key, trailing_percent, trailing_delay)
            )
            self._register_investment_task(position_key, "trailing", trailing_task)
            logger.info(
                f"⏱ Scheduled {trailing_percent:.1f}% trailing stop for {position.symbol} "
                f"({position.exchange}) in {trailing_delay:.1f}s"
            )

        exit_task = asyncio.create_task(
            self._force_exit_investment_position(position_key, exit_delay)
        )
        self._register_investment_task(position_key, "force_exit", exit_task)
        position.strategy_state["investment_force_exit_at"] = position.entry_time + timedelta(seconds=exit_delay)
        logger.info(
            f"⏰ Scheduled forced exit for {position.symbol} ({position.exchange}) in {exit_delay/60:.1f} min"
        )

    async def _activate_investment_trailing_stop(
        self,
        position_key: str,
        percent: float,
        delay_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            position = self.positions.get(position_key)
            if not position or position.status == "closed":
                return
            if not self._is_investment_position(position):
                return
            await self.check_position_price(position)
            position.set_trailing_stop(percent)
            position.strategy_state["investment_trailing_percent"] = percent
            position.strategy_state["investment_trailing_activated_at"] = datetime.now()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                f"Error activating investment trailing stop for {position_key}: {exc}"
            )

    async def _force_exit_investment_position(self, position_key: str, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            position = self.positions.get(position_key)
            if not position or position.status == "closed":
                return
            if not self._is_investment_position(position):
                return
            logger.info(
                f"🏁 Investment timeout reached for {position.symbol} ({position.exchange}); forcing exit"
            )
            await self.sell_position_master_follower(
                position,
                f"Investment timeout {delay_seconds/60:.1f}min",
            )
            if position.status != "closed":
                logger.warning(
                    f"Investment forced exit did not close {position.symbol}; initiating retry sequence"
                )
                await self.sell_with_retry(
                    position,
                    1.0,
                    f"Investment timeout retry {delay_seconds/60:.1f}min",
                    max_retries=30,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                f"Error forcing investment exit for {position_key}: {exc}"
            )
