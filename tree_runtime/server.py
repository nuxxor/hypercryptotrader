from .common import *
from .hub import SignalHub
from .telegram_client import TelegramChannelClient
from .tree_client import TreeNewsClient

class WebSocketServer:
    """WebSocket server for signal distribution"""
    
    def __init__(self, hub: SignalHub):
        self.hub = hub
        self.app = web.Application()
        self.runner = None
        self.setup_routes()
    
    def setup_routes(self):
        """Setup HTTP routes"""
        self.app.router.add_get('/ws', self.websocket_handler)
        self.app.router.add_get('/', self.index_handler)
        self.app.router.add_get('/favicon.ico', self.favicon_handler)
        self.app.router.add_get('/health', self.health_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)

        # Add simple error middleware to avoid noisy stacktraces
        @web.middleware
        async def error_middleware(request, handler):
            try:
                return await handler(request)
            except web.HTTPException as ex:
                # Return as is
                return ex
            except Exception as e:
                logger.warning(f"Unhandled request error: {e}")
                return web.Response(status=400, text='Bad Request')

        self.app.middlewares.append(error_middleware)
    
    async def index_handler(self, request):
        """Landing page"""
        telegram_status = "✅ ENABLED" if (TELEGRAM_AVAILABLE and TG_API_ID) else "❌ DISABLED"
        upbit_source_line = "    • Upbit (KRW listings)"
        if not ENABLE_UPBIT_LISTINGS:
            upbit_source_line += " [DISABLED]"
        bwe_channel_line = "    • BWEnews (Upbit backup)"
        if not ENABLE_UPBIT_LISTINGS:
            bwe_channel_line += " [SUPPRESSED]"
        upbit_channel_line = "    • upbit_news (Official channel)"
        if not ENABLE_UPBIT_LISTINGS:
            upbit_channel_line += " [SUPPRESSED]"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Hybrid Signal Server</title>
            <style>
                body {{
                    font-family: monospace;
                    background: #1a1a1a;
                    color: #0f0;
                    padding: 20px;
                }}
                pre {{ 
                    background: #000;
                    padding: 15px;
                    border: 1px solid #0f0;
                    border-radius: 5px;
                }}
                a {{ color: #0ff; }}
                .warning {{ color: #ff0; }}
                .success {{ color: #0f0; }}
            </style>
        </head>
        <body>
            <h1>🌲 Hybrid Exchange Signal Server</h1>
            <pre>
Status: <span class="success">● RUNNING</span>
Version: 3.0-hybrid

Endpoints:
  WebSocket: ws://localhost:{WS_SERVER_PORT}/ws
  Health:    <a href="/health">/health</a>
  Metrics:   <a href="/metrics">/metrics</a>

Sources:
  <b>Tree News (All exchanges):</b>
{upbit_source_line}
    • Bithumb (All listings)
    • Investments/Treasury buys ($100M+)
  
  <b>Telegram Channels:</b>
    Status: {telegram_status}
{bwe_channel_line}
{upbit_channel_line}

Features:
  • Deduplication across sources
  • Persistent signal storage
  • Color-coded console output
  • Debug mode: {DEBUG_MODE}
  • Multi-channel Telegram support
            </pre>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')

    async def favicon_handler(self, request):
        """Return empty favicon to avoid 404 spam"""
        return web.Response(status=204)
    
    async def websocket_handler(self, request):
        """Handle WebSocket connections"""
        try:
            ws = web.WebSocketResponse(heartbeat=30)
            await ws.prepare(request)
            await self.hub.add_client(ws)
        except Exception as e:
            logger.exception("WS handshake failed")
            return web.Response(status=426, text=f"WebSocket handshake error: {e}")
        
        telegram_enabled = TELEGRAM_AVAILABLE and TG_API_ID
        tree_news_sources = ["INVESTMENT"]
        if ENABLE_BITHUMB_LISTINGS:
            tree_news_sources.insert(0, "BITHUMB")
        if ENABLE_UPBIT_LISTINGS:
            tree_news_sources.insert(0, "UPBIT")
        upbit_filter_text = "KRW listings only"
        if not ENABLE_UPBIT_LISTINGS:
            upbit_filter_text += " (disabled)"
        
        welcome = {
            "type": "welcome",
            "message": "Connected to Hybrid Signal Server",
            "version": "3.0-hybrid",
            "sources": {
                "tree_news": tree_news_sources,
                "telegram_channels": ["BWEnews", "upbit_official"] if telegram_enabled else []
            },
            "filters": {
                "upbit": upbit_filter_text,
                "bithumb": "All listings",
                "investment": "$100M+ funding/purchase"
            },
            "telegram_backup": telegram_enabled,
            "upbit_listings_enabled": ENABLE_UPBIT_LISTINGS,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            await ws.send_str(json.dumps(welcome))
        except Exception:
            logger.exception("Failed to send welcome message")
            await self.hub.remove_client(ws)
            return web.Response(status=500, text="Failed to send welcome")
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    # Accept external signal injections over WS
                    try:
                        payload = json.loads(msg.data)
                        if isinstance(payload, dict) and (payload.get('type') == 'signal'):
                            src = str(payload.get('source', 'EXTERNAL')).upper()
                            tokens = payload.get('tokens') or []
                            if isinstance(tokens, str):
                                tokens = [tokens]
                            # Basic token cleanup
                            cleaned = []
                            for t in tokens:
                                t = str(t).upper().strip()
                                if 2 <= len(t) <= 15 and re.match(r'^[A-Z0-9\-]+$', t):
                                    cleaned.append(t)
                            cleaned = sorted(set(cleaned))
                            if not cleaned:
                                continue

                            announcement = str(payload.get('announcement', ''))[:300]
                            alert_type = str(payload.get('alert_type', 'listing'))
                            listing_type = payload.get('listing_type')
                            url = payload.get('url')
                            notice_id = payload.get('notice_id')
                            company = payload.get('company')
                            token_address = payload.get('token_address')
                            if token_address:
                                token_address = str(token_address).strip()
                                if not re.match(r"^0x[a-fA-F0-9]{40}$", token_address):
                                    token_address = None

                            sig = Signal(
                                source=src,
                                tokens=cleaned,
                                token_address=token_address,
                                announcement=announcement or f"External {src} signal",
                                alert_type=alert_type,
                                listing_type=listing_type,
                                url=url,
                                notice_id=str(notice_id) if notice_id else None,
                                company=str(company).upper() if company else None,
                            )
                            await self.hub.process_signal(sig)
                    except json.JSONDecodeError:
                        # ignore plain text from clients
                        pass
                elif msg.type == WSMsgType.ERROR:
                    logger.error(f'WebSocket error: {ws.exception()}')
                    break
        except Exception as e:
            logger.exception("WebSocket handler loop error")
        finally:
            await self.hub.remove_client(ws)
        
        return ws
    
    async def health_handler(self, request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def metrics_handler(self, request):
        """Metrics endpoint"""
        metrics = {
            **self.hub.stats,
            "processed_signals": len(self.hub.signal_store.signals),
            "signal_breakdown": self.hub.signal_store.get_stats(),
            "queue_size": self.hub.signal_queue.qsize(),
            "uptime_seconds": int(time.time() - globals().get("start_time", time.time())),
            "telegram_enabled": TELEGRAM_AVAILABLE and bool(TG_API_ID),
        }
        return web.Response(
            text=json.dumps(metrics, indent=2),
            content_type='application/json'
        )
    
    async def start(self):
        """Start the server"""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, BIND_HOST, WS_SERVER_PORT)
        await site.start()
        logger.info(f"WebSocket server started on {BIND_HOST}:{WS_SERVER_PORT}")
    
    async def stop(self):
        """Stop the server"""
        if self.runner:
            await self.runner.cleanup()

# ==========================
# Stats Reporter
# ==========================

async def stats_reporter(hub: SignalHub):
    """Report statistics periodically"""
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        
        stats = {
            "clients": hub.stats["clients_connected"],
            "signals_enqueued": hub.stats["signals_enqueued"],
            "signals_blocked": hub.stats["signals_blocked"],
            "market_cap_filtered": hub.stats["market_cap_filtered"],
            "total_news": hub.stats["total_news_received"],
            "news_filtered": hub.stats["news_filtered"],
            "tree_news_signals": hub.stats["tree_news_signals"],
            "telegram_signals": hub.stats["telegram_signals"],
            "queue_size": hub.signal_queue.qsize(),
            "processed_total": len(hub.signal_store.signals),
            "relay_success": hub.stats["relay_published"],
            "relay_errors": hub.stats["relay_errors"],
        }
        
        breakdown = hub.signal_store.get_stats()
        
        logger.info(f"📊 Stats: {stats}")
        if breakdown:
            logger.info(f"   Signal breakdown: {breakdown}")

# ==========================
# Test Injector
# ==========================

async def test_injector(
    hub: SignalHub,
    tokens: List[str],
    source: str,
    delay: float,
    listing_type: str,
    spacing: float,
    force_strategy: bool,
):
    """Inject one or more test signals after a delay."""
    try:
        normalized_source = (source or "").strip().upper() or "TEST"
        normalized_listing_type = (listing_type or "").strip().upper() or None
        cleaned_tokens = [
            (tok or "").strip().upper()
            for tok in tokens
            if (tok or "").strip()
        ]
        token_list = ", ".join(cleaned_tokens) if cleaned_tokens else "(none)"
        strategy_note = " (forcing full strategy)" if force_strategy else ""
        print(
            f"{Colors.YELLOW}🧪 Test mode enabled. Emitting {token_list} after {delay:.1f}s "
            f"(source={normalized_source}, type={normalized_listing_type or 'N/A'}) with {spacing:.1f}s spacing{strategy_note}.{Colors.RESET}"
        )
        await asyncio.sleep(delay)

        for idx, token in enumerate(cleaned_tokens):
            if idx > 0 and spacing > 0:
                await asyncio.sleep(spacing)

            fake_notice = f"TEST-{int(time.time()*1000)}-{idx}"
            clean_token = token
            listing_descriptor = f"{normalized_listing_type} " if normalized_listing_type else ""
            sig = Signal(
                source=normalized_source,
                tokens=[clean_token],
                announcement=f"[TEST] Simulated {normalized_source} {listing_descriptor}listing: {clean_token}",
                listing_type=normalized_listing_type,
                url=None,
                notice_id=fake_notice,
                force_strategy=force_strategy,
            )
            await hub.process_signal(sig)
            print(
                f"{Colors.GREEN}🧪 Test signal emitted for {clean_token} "
                f"({normalized_source}, {normalized_listing_type or 'N/A'}).{Colors.RESET}"
            )
    except Exception as e:
        logger.error(f"Test injector error: {e}")

# ==========================
# Main Function
# ==========================

async def main(args):
    """Main async function"""
    global start_time
    start_time = time.time()
    
    print("\n" + "="*70)
    print(f"{Colors.CYAN}🌲 HYBRID MULTI-EXCHANGE SIGNAL SERVER v3.0{Colors.RESET}")
    print("="*70)
    print("📡 Signal Sources:")
    sources_display = []
    if ENABLE_UPBIT_LISTINGS:
        sources_display.append("Upbit")
    if ENABLE_BITHUMB_LISTINGS:
        sources_display.append("Bithumb")
    sources_display.append("Investments")
    tree_news_line = "   • Tree News: " + ", ".join(sources_display)
    disabled_notes = []
    if not ENABLE_UPBIT_LISTINGS:
        disabled_notes.append("Upbit disabled")
    if not ENABLE_BITHUMB_LISTINGS:
        disabled_notes.append("Bithumb disabled")
    if disabled_notes:
        tree_news_line += f" ({'; '.join(disabled_notes)})"
    print(tree_news_line)
    
    telegram_status = "ENABLED" if (TELEGRAM_AVAILABLE and TG_API_ID) else "DISABLED"
    telegram_color = Colors.GREEN if telegram_status == "ENABLED" else Colors.YELLOW
    print(f"   • Telegram Channels: [{telegram_color}{telegram_status}{Colors.RESET}]")
    if telegram_status == "ENABLED":
        backup_suffix = " [SUPPRESSED]" if not ENABLE_UPBIT_LISTINGS else ""
        print(f"     - BWEnews (Upbit backup){backup_suffix}")
        print(f"     - upbit_news (Official){backup_suffix}")
    
    print("🎯 Filters:")
    upbit_filter_line = "   • Upbit: KRW listings only"
    if not ENABLE_UPBIT_LISTINGS:
        upbit_filter_line += " (disabled)"
    print(upbit_filter_line)
    if ENABLE_BITHUMB_LISTINGS:
        print("   • Bithumb: All listings")
    else:
        print("   • Bithumb: Listings disabled")
    print("   • Investments/Treasury buys: $100M+ funding or purchases")
    if ENABLE_WS_SERVER:
        print(f"🌐 Signal server: ws://localhost:{WS_SERVER_PORT}/ws")
        print(f"📊 Metrics: http://localhost:{WS_SERVER_PORT}/metrics")
        print(f"❤️  Health: http://localhost:{WS_SERVER_PORT}/health")
        print(f"🏠 Landing: http://localhost:{WS_SERVER_PORT}/")
    else:
        print(f"🌐 Signal server: disabled (TREE_NEWS_ENABLE_WS_SERVER=false)")
    print(f"🎨 Console: ALL news displayed with colors")
    print(f"🐛 Debug mode: {DEBUG_MODE}")
    if args.test:
        tokens_display = ", ".join(args.test_tokens)
        print(
            f"{Colors.YELLOW}🧪 Test: Will emit {tokens_display} after {args.test_delay:.1f}s "
            f"(source={args.test_source}, type={args.test_listing_type}, spacing={args.test_spacing:.1f}s)"
            f"{Colors.RESET}"
        )
    print("="*70)
    if not ENABLE_UPBIT_LISTINGS:
        print(f"{Colors.YELLOW}⏸ Upbit listings suppressed. Set TREE_NEWS_ENABLE_UPBIT_LISTINGS=true to resume.{Colors.RESET}")
    if not ENABLE_BITHUMB_LISTINGS:
        print(f"{Colors.YELLOW}⏸ Bithumb listings suppressed. Set TREE_NEWS_ENABLE_BITHUMB_LISTINGS=true to resume.{Colors.RESET}")
    
    # Check API credentials
    if not API_KEY:
        print(f"{Colors.RED}❌ Error: TREE_NEWS_API_KEY not set in .env file!{Colors.RESET}")
        print("Please set your Tree News API key to continue.")
        sys.exit(1)
    
    if not TELEGRAM_AVAILABLE:
        print(f"{Colors.YELLOW}⚠️  Telethon not installed. Telegram backup disabled.{Colors.RESET}")
        print(f"   Install with: pip install telethon")
    elif not TG_API_ID or not TG_API_HASH or not TG_PHONE:
        print(f"{Colors.YELLOW}⚠️  Telegram credentials not configured. Telegram channels disabled.{Colors.RESET}")
        print(f"   Set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_PHONE_NUMBER in .env")
    else:
        print(f"{Colors.GREEN}✅ Telegram configured. Run 'python telegram_bootstrap.py' first if not done.{Colors.RESET}")
    
    # Initialize components
    hub = SignalHub()
    server = WebSocketServer(hub)

    # Show existing processed signals
    if hub.signal_store.signals:
        print(f"📂 Loaded {len(hub.signal_store.signals)} permanently blocked signals")
        breakdown = hub.signal_store.get_stats()
        if breakdown:
            print(f"   Breakdown: {breakdown}")
    else:
        print("📂 Starting with clean signal store")
    print("="*70 + "\n")
    
    # Create Tree News clients
    tokyo_client = TreeNewsClient("TOKYO", TOKYO_WS_URL, hub)
    main_client = TreeNewsClient("MAIN", MAIN_WS_URL, hub)

    # Create Telegram clients
    telegram_clients = []
    if TELEGRAM_AVAILABLE and TG_API_ID and TG_API_HASH and TG_PHONE:
        # BWE Client
        bwe_client = TelegramChannelClient(
            hub, 
            "telegram_bwe",
            BWE_CHANNEL if not BWE_GROUP_ID else None,
            BWE_GROUP_ID if BWE_GROUP_ID else None,
            parse_bwe_message
        )
        telegram_clients.append(bwe_client)
        
        # Upbit Official Client
        upbit_official_client = TelegramChannelClient(
            hub, 
            "telegram_upbit_official",
            UPBIT_OFFICIAL_CHANNEL if not UPBIT_OFFICIAL_GROUP_ID else None,
            UPBIT_OFFICIAL_GROUP_ID if UPBIT_OFFICIAL_GROUP_ID else None,
            parse_upbit_official_message
        )
        telegram_clients.append(upbit_official_client)
    
    # Start all tasks
    tasks = [
        asyncio.create_task(hub.broadcast_loop()),
        asyncio.create_task(tokyo_client.run()),
        asyncio.create_task(main_client.run()),
        asyncio.create_task(stats_reporter(hub))
    ]

    if ENABLE_WS_SERVER:
        tasks.append(asyncio.create_task(server.start()))

    cmc_auto_refresh = os.getenv("TREE_NEWS_CMC_AUTO_REFRESH", "true").lower() != "false"
    if cmc_auto_refresh:
        refresh_interval = _parse_threshold_env(
            "TREE_NEWS_CMC_REFRESH_INTERVAL_SECONDS", 86_400, min_value=60
        )
        tasks.append(
            asyncio.create_task(
                coinmarketcap_refresh_loop(hub.market_cap_filter, refresh_interval)
            )
        )
    else:
        logger.info("CoinMarketCap auto-refresh disabled via TREE_NEWS_CMC_AUTO_REFRESH")

    # Add Telegram tasks
    for client in telegram_clients:
        tasks.append(asyncio.create_task(client.start()))
    
    # 🧪 Test mode task
    if args.test:
        tasks.append(
            asyncio.create_task(
                test_injector(
                    hub,
                    args.test_tokens,
                    args.test_source,
                    args.test_delay,
                    args.test_listing_type,
                    args.test_spacing,
                    args.test_force_strategy,
                )
            )
        )
    
    # Setup graceful shutdown
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Shutdown signal received")
        for task in tasks:
            task.cancel()
        asyncio.create_task(
            shutdown(hub, tokyo_client, main_client, telegram_clients, server, ENABLE_WS_SERVER)
        )
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass

async def shutdown(hub, tokyo_client, main_client, telegram_clients, server, ws_enabled):
    """Graceful shutdown"""
    logger.info("Starting graceful shutdown...")

    await tokyo_client.stop()
    await main_client.stop()
    for client in telegram_clients:
        if client.client:
            await client.client.disconnect()
    if ws_enabled:
        await server.stop()
    await hub.close()

    logger.info("Shutdown complete")
    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    try:
        args = parse_args()
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}👋 Shutting down...{Colors.RESET}")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
