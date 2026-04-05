from .common import *
from .store import PersistentSignalStore


class SignalHub:
    """Central hub for signal management and broadcasting."""

    def __init__(self):
        self.clients: Set[web.WebSocketResponse] = set()
        self.signal_queue: asyncio.Queue = asyncio.Queue(maxsize=SIGNAL_QUEUE_SIZE)
        self.signal_store = PersistentSignalStore(PROCESSED_SIGNALS_FILE)
        self.signal_cache: deque = deque(maxlen=100)
        self.stats = {
            "signals_enqueued": 0,
            "signals_blocked": 0,
            "clients_connected": 0,
            "queue_drops": 0,
            "broadcast_success": 0,
            "broadcast_errors": 0,
            "relay_published": 0,
            "relay_errors": 0,
            "total_news_received": 0,
            "news_filtered": 0,
            "telegram_signals": 0,
            "tree_news_signals": 0,
            "market_cap_filtered": 0,
            "message_queue_enqueued": 0,
            "message_queue_drops": 0,
            "message_queue_processed": 0,
        }
        self.relay_publisher: Optional[RelayPublisher] = None
        if RELAY_PUBLISH_URL:
            self.relay_publisher = RelayPublisher(
                RELAY_PUBLISH_URL,
                api_key=RELAY_API_KEY,
                timeout_seconds=RELAY_TIMEOUT_SECONDS,
            )
            logger.info("Relay publishing enabled -> %s", RELAY_PUBLISH_URL)

        try:
            ttl_minutes = int(os.getenv("INVESTMENT_DEDUP_MINUTES", "360"))
        except Exception:
            ttl_minutes = 360

        self.recent_investment_ttl = 60 * ttl_minutes
        self.recent_investment_seen: Dict[Tuple[str, Tuple[str, ...], int], float] = {}
        self._pending_investment_keys: Set[Tuple[str, Tuple[str, ...], int]] = set()
        self._pending_signal_hashes: Set[str] = set()
        self._investment_lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self.market_cap_filter = MarketCapFilter()

    async def add_client(self, ws: web.WebSocketResponse):
        self.clients.add(ws)
        self.stats["clients_connected"] = len(self.clients)
        logger.info("Client connected. Total: %d", len(self.clients))

    async def remove_client(self, ws: web.WebSocketResponse):
        self.clients.discard(ws)
        self.stats["clients_connected"] = len(self.clients)
        logger.info("Client disconnected. Remaining: %d", len(self.clients))

    def create_signal_hash(
        self,
        source: str,
        tokens: List[str],
        amount: Optional[float] = None,
        notice_id: Optional[str] = None,
        company: Optional[str] = None,
    ) -> str:
        if notice_id:
            return f"{source}:notice:{notice_id}"
        if source == "INVESTMENT" and company and amount is None:
            return f"{source}:{company}:{':'.join(sorted(tokens))}"
        if source == "INVESTMENT" and amount:
            return f"{source}:{':'.join(sorted(tokens))}:{amount}"
        return f"{source}:{':'.join(sorted(tokens))}"

    def get_detailed_timestamp(self) -> str:
        now = datetime.now(timezone.utc)
        return now.strftime("%H:%M:%S.%f")[:-3]

    def add_to_cache(self, signal: Signal, status: str):
        now_tokyo = datetime.now(timezone.utc).astimezone(TOKYO_TZ)
        tokyo_hms_ms = now_tokyo.strftime("%H:%M:%S.%f")[:-3]

        cache_entry = {
            "time": self.get_detailed_timestamp(),
            "time_tokyo": tokyo_hms_ms,
            "timestamp": signal.timestamp,
            "source": signal.source,
            "tokens": signal.tokens,
            "status": status,
            "hash": signal.hash,
            "telegram_source": signal.telegram_source,
            "announcement": (
                signal.announcement[:100] + "..."
                if len(signal.announcement) > 100
                else signal.announcement
            ),
        }

        if signal.amount_millions:
            cache_entry["amount"] = signal.amount_formatted
        if signal.company:
            cache_entry["company"] = signal.company
        if signal.channel:
            cache_entry["channel"] = signal.channel

        self.signal_cache.append(cache_entry)

        if status == "SENT":
            source_str = (
                f"{signal.source} (Telegram)"
                if signal.telegram_source
                else signal.source
            )
            if (
                signal.source == "INVESTMENT"
                and signal.company
                and (signal.alert_type or "").lower() == "partnership"
            ):
                source_str = f"{source_str} (partnership: {signal.company})"
            if signal.channel:
                source_str = f"{source_str} [{signal.channel}]"
            print(
                f"{Colors.GREEN}✅ [{tokyo_hms_ms} TOKYO] Signal sent: "
                f"{source_str} - {signal.tokens}{Colors.RESET}"
            )
        elif status == "BLOCKED":
            channel_str = f" [{signal.channel}]" if signal.channel else ""
            print(
                f"{Colors.YELLOW}⚠️  [{tokyo_hms_ms} TOKYO] Signal blocked "
                f"(duplicate): {signal.source}{channel_str} - {signal.tokens}{Colors.RESET}"
            )
        elif status == "DROPPED":
            print(
                f"{Colors.RED}⛔ [{tokyo_hms_ms} TOKYO] Signal dropped "
                f"(queue full): {signal.source} - {signal.tokens}{Colors.RESET}"
            )

    def _normalize_source(self, source: Optional[str]) -> str:
        return self.market_cap_filter._normalise_source(source)

    def _should_bypass_upbit_dedup(self, signal: Signal) -> bool:
        tokens = signal.tokens or []
        tokens_upper = {token.upper() for token in tokens}
        if "BNB" in tokens_upper:
            logger.info(
                "Bypassing Upbit duplicate guard for tokens=%s (BNB priority)",
                tokens,
            )
            return True

        announcement = (signal.announcement or "").upper()
        if announcement.startswith("[TEST]") or "SIMULATED" in announcement:
            return True

        notice_id = (signal.notice_id or "").upper()
        if notice_id.startswith("TEST-"):
            return True

        source_label = (signal.source or "").upper()
        if "TEST" in source_label:
            return True

        return False

    def _build_signal_meta(self, signal: Signal) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        now_tokyo = now_utc.astimezone(TOKYO_TZ)
        meta: Dict[str, Any] = {
            "received_utc_iso": now_utc.isoformat(),
            "received_tokyo_iso": now_tokyo.isoformat(),
            "received_tokyo_hms_ms": now_tokyo.strftime("%H:%M:%S.%f")[:-3],
            "signal_timestamp_iso": signal.timestamp,
            "source": signal.source,
            "tokens": list(signal.tokens),
            "notice_id": signal.notice_id,
            "url": signal.url,
            "alert_type": signal.alert_type,
            "listing_type": signal.listing_type,
            "channel": signal.channel,
        }
        if signal.amount_millions is not None:
            meta["amount_millions"] = signal.amount_millions
        if signal.announcement:
            meta["announcement"] = signal.announcement[:300]
        if signal.company:
            meta["company"] = signal.company
        return meta

    def _persist_keys_for_signal(
        self,
        signal: Signal,
        normalized_source: str,
        signal_hash: str,
    ) -> List[str]:
        keys = [signal_hash]
        if normalized_source == "UPBIT" and signal.tokens:
            base_key = f"UPBIT:{':'.join(sorted(signal.tokens))}"
            if base_key != signal_hash:
                keys.append(base_key)
        return keys

    async def _reserve_signal_keys(
        self,
        signal: Signal,
        persist_keys: List[str],
    ) -> bool:
        normalized_source = self._normalize_source(signal.source)
        bypass_upbit = normalized_source == "UPBIT" and self._should_bypass_upbit_dedup(
            signal
        )

        async with self._lock:
            if not bypass_upbit:
                duplicate = any(
                    self.signal_store.contains(key) or key in self._pending_signal_hashes
                    for key in persist_keys
                )
                if duplicate:
                    self.stats["signals_blocked"] += 1
                    self.add_to_cache(signal, "BLOCKED")
                    return False

            self._pending_signal_hashes.update(persist_keys)
            return True

    async def _release_signal_keys(self, persist_keys: List[str]) -> None:
        async with self._lock:
            for key in persist_keys:
                self._pending_signal_hashes.discard(key)

    async def _commit_signal_keys(
        self,
        persist_keys: List[str],
        meta: Dict[str, Any],
    ) -> None:
        payload = {key: dict(meta) for key in persist_keys}
        async with self._lock:
            self.signal_store.update_many(payload)
            for key in persist_keys:
                self._pending_signal_hashes.discard(key)

    def _investment_dedup_key(
        self, signal: Signal
    ) -> Optional[Tuple[str, Tuple[str, ...], int]]:
        if signal.source != "INVESTMENT" or not signal.tokens:
            return None
        amt = signal.amount_millions or 0.0
        bucket_step = max(1.0, INVESTMENT_MIN_AMOUNT_MILLIONS)
        bucket = int(round(amt / bucket_step) * bucket_step) if amt > 0 else 0
        return ("INVESTMENT", tuple(sorted(signal.tokens)), bucket)

    async def _reserve_investment_key(
        self, signal: Signal
    ) -> Tuple[bool, Optional[Tuple[str, Tuple[str, ...], int]]]:
        key = self._investment_dedup_key(signal)
        if key is None:
            return False, None

        now_ts = time.time()
        new_set = set(key[1])
        bucket = key[2]

        async with self._investment_lock:
            expired = [
                existing_key
                for existing_key, ts in self.recent_investment_seen.items()
                if now_ts - ts >= self.recent_investment_ttl
            ]
            for expired_key in expired:
                self.recent_investment_seen.pop(expired_key, None)

            last_ts = self.recent_investment_seen.get(key, 0)
            if now_ts - last_ts < self.recent_investment_ttl or key in self._pending_investment_keys:
                self.stats["signals_blocked"] += 1
                self.add_to_cache(signal, "BLOCKED")
                return True, None

            active_keys = list(self.recent_investment_seen.keys()) + list(
                self._pending_investment_keys
            )
            for existing_key in active_keys:
                if existing_key[0] != "INVESTMENT" or existing_key[2] != bucket:
                    continue
                old_set = set(existing_key[1])
                if new_set.issubset(old_set) or old_set.issubset(new_set):
                    self.stats["signals_blocked"] += 1
                    self.add_to_cache(signal, "BLOCKED")
                    return True, None

            self._pending_investment_keys.add(key)
            return False, key

    async def _release_investment_key(
        self, key: Optional[Tuple[str, Tuple[str, ...], int]]
    ) -> None:
        if key is None:
            return
        async with self._investment_lock:
            self._pending_investment_keys.discard(key)

    async def _commit_investment_key(
        self, key: Optional[Tuple[str, Tuple[str, ...], int]]
    ) -> None:
        if key is None:
            return
        async with self._investment_lock:
            self._pending_investment_keys.discard(key)
            self.recent_investment_seen[key] = time.time()

    async def process_signal(self, signal: Signal) -> bool:
        """Process and queue signal if not duplicate."""
        if signal.tokens and len(signal.tokens) > 1:
            base_source = self._normalize_source(signal.source)
            alert = (signal.alert_type or "").lower()
            if (
                any(base_source.startswith(src) for src in LOW_CAP_PRIORITY_SOURCES)
                and "listing" in alert
            ):
                lowest = self.market_cap_filter.pick_lowest_market_cap(signal.tokens)
                if lowest and lowest in signal.tokens:
                    if len(signal.tokens) != 1 or signal.tokens[0] != lowest:
                        logger.info(
                            "Selecting lowest market cap token for %s listing: %s -> %s",
                            base_source,
                            signal.tokens,
                            lowest,
                        )
                    signal.tokens = [lowest]

        if self.market_cap_filter.should_apply(signal):
            filtered_tokens, skipped, threshold = self.market_cap_filter.filter_signal(
                signal
            )
            if skipped:
                source_name = self._normalize_source(signal.source) or "default"
                for token, market_cap in skipped:
                    logger.warning(
                        "🚫 Market cap filter removing %s - Market cap $%.0fM > $%.0fM threshold for %s",
                        token,
                        (market_cap or 0) / 1_000_000,
                        (threshold or 0) / 1_000_000,
                        source_name,
                    )
                self.stats["market_cap_filtered"] += len(skipped)

            if not filtered_tokens:
                self.stats["signals_blocked"] += 1
                print(
                    f"{Colors.YELLOW}⚠️  Market cap filter dropped {signal.source} signal "
                    f"entirely (>{threshold/1_000_000:.0f}M){Colors.RESET}"
                )
                return False

            if filtered_tokens != signal.tokens:
                print(
                    f"{Colors.YELLOW}⚠️  Market cap filter trimmed {signal.source} tokens -> "
                    f"{filtered_tokens} (threshold ${threshold/1_000_000:.0f}M){Colors.RESET}"
                )
                signal.tokens = filtered_tokens

        normalized_source = self._normalize_source(signal.source)
        signal_hash = self.create_signal_hash(
            normalized_source,
            signal.tokens,
            signal.amount_millions,
            signal.notice_id,
            signal.company,
        )
        signal.hash = signal_hash
        persist_keys = self._persist_keys_for_signal(signal, normalized_source, signal_hash)
        meta = self._build_signal_meta(signal)

        investment_blocked, investment_key = await self._reserve_investment_key(signal)
        if investment_blocked:
            return False

        keys_reserved = await self._reserve_signal_keys(signal, persist_keys)
        if not keys_reserved:
            await self._release_investment_key(investment_key)
            return False

        committed = False
        try:
            if normalized_source == "UPBIT":
                await asyncio.sleep(0.7)

            self.signal_queue.put_nowait(signal)
            self.stats["signals_enqueued"] += 1

            await self._commit_signal_keys(persist_keys, meta)
            await self._commit_investment_key(investment_key)
            committed = True

            if signal.telegram_source:
                self.stats["telegram_signals"] += 1
            else:
                self.stats["tree_news_signals"] += 1

            self.add_to_cache(signal, "SENT")
            return True
        except asyncio.QueueFull:
            self.stats["queue_drops"] += 1
            self.add_to_cache(signal, "DROPPED")
            logger.error("Signal queue full, dropping: %s", signal_hash)
            return False
        finally:
            if not committed:
                await self._release_signal_keys(persist_keys)
                await self._release_investment_key(investment_key)

    async def broadcast_loop(self):
        """Continuously broadcast signals from queue."""
        while True:
            signal = await self.signal_queue.get()
            try:
                await self._broadcast_signal(signal)
            except Exception as exc:
                logger.error("Broadcast loop error: %s", exc)
                await asyncio.sleep(0.1)
            finally:
                self.signal_queue.task_done()

    async def _broadcast_signal(self, signal: Signal):
        payload = signal.to_dict()
        message = json.dumps(payload)
        relay_task: Optional[asyncio.Task] = None
        if self.relay_publisher:
            relay_task = asyncio.create_task(self.relay_publisher.publish(payload))

        if not self.clients:
            if relay_task:
                try:
                    if await relay_task:
                        self.stats["relay_published"] += 1
                    else:
                        self.stats["relay_errors"] += 1
                except Exception as exc:
                    self.stats["relay_errors"] += 1
                    logger.warning("Relay task error: %s", exc)
            return

        send_tasks = []
        for client in tuple(self.clients):
            wrapped = asyncio.wait_for(
                self._send_to_client(client, message),
                timeout=BROADCAST_TIMEOUT,
            )
            task = asyncio.create_task(wrapped)
            send_tasks.append((client, task))

        disconnected = set()
        results = await asyncio.gather(
            *(task for _, task in send_tasks),
            return_exceptions=True,
        )

        for (client, _task), result in zip(send_tasks, results):
            if isinstance(result, Exception):
                if isinstance(result, asyncio.TimeoutError):
                    logger.warning("Client send timeout")
                else:
                    logger.debug("Client send error: %s", result)
                disconnected.add(client)
                self.stats["broadcast_errors"] += 1
            else:
                self.stats["broadcast_success"] += 1

        for client in disconnected:
            await self.remove_client(client)
            try:
                await client.close()
            except Exception:
                pass

        if relay_task:
            try:
                if await relay_task:
                    self.stats["relay_published"] += 1
                else:
                    self.stats["relay_errors"] += 1
            except Exception as exc:  # pragma: no cover - defensive logging
                self.stats["relay_errors"] += 1
                logger.warning("Relay task error: %s", exc)

    async def _send_to_client(self, client: web.WebSocketResponse, message: str):
        await client.send_str(message)

    async def close(self) -> None:
        if self.relay_publisher:
            await self.relay_publisher.close()
        if hasattr(self.signal_store, "close"):
            await self.signal_store.close()
