from .common import *
from .hub import SignalHub

class TelegramChannelClient:
    """
    Generic Telegram client for a single channel.
    You pass: channel (username or id), a parser function that returns Signal|None,
    and a label for logging.
    """
    def __init__(self, hub: SignalHub, label: str, channel_username: Optional[str], channel_id: Optional[int], parser_fn):
        self.hub = hub
        self.client = None
        self.running = True
        self.connected = False
        self.label = label
        self.channel_username = channel_username
        self.channel_id = channel_id
        self.parser_fn = parser_fn

    async def start(self):
        if not TELEGRAM_AVAILABLE:
            logger.warning(f"[{self.label}] Telegram disabled (telethon missing)")
            return
        if not TG_API_ID or not TG_API_HASH or not TG_PHONE:
            logger.warning(f"[{self.label}] Telegram credentials not configured")
            return

        try:
            self.client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
            await self.client.connect()

            if not await self.client.is_user_authorized():
                logger.error(f"[{self.label}] Not authorized! Run telegram_bootstrap.py first")
                self.connected = False
                return

            chat = self.channel_id if self.channel_id else self.channel_username
            if self.channel_username and not self.channel_id:
                try:
                    await self.client(JoinChannelRequest(self.channel_username))
                except Exception:
                    pass

            @self.client.on(events.NewMessage(chats=chat))
            async def on_message(event):
                try:
                    arrival = time.time()
                    text = event.raw_text or ""
                    sig = self.parser_fn(text)
                    if sig:
                        # response time
                        msg_time = event.message.date.replace(tzinfo=timezone.utc).timestamp()
                        sig.response_time_ms = int((arrival - msg_time) * 1000)
                        # işaretle
                        sig.telegram_source = True
                        # kaynağı daha belirgin yap
                        if not sig.channel:
                            sig.channel = self.label
                        await self.hub.process_signal(sig)
                        print(f"\n{Colors.MAGENTA}📱 [{self.label}] signal: {sig.tokens} ({sig.listing_type}) {sig.response_time_ms}ms{Colors.RESET}")
                except Exception as e:
                    logger.error(f"[{self.label}] handler error: {e}")

            self.connected = True
            logger.info(f"[{self.label}] Connected & listening")
            await self.client.run_until_disconnected()

        except Exception as e:
            logger.error(f"[{self.label}] Connection error: {e}")
            self.connected = False

# ==========================
# Tree News Client
# ==========================

