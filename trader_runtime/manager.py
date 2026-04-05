from .common import *
from .market import CoinMarketCapAPI
from .persistence import AtomicPersistenceMixin
from .manager_core import PositionManagerCoreMixin
from .manager_signal_flow import PositionManagerSignalMixin
from .manager_monitoring import PositionManagerMonitoringMixin
from .manager_execution import PositionManagerExecutionMixin


class PositionManager(
    AtomicPersistenceMixin,
    PositionManagerExecutionMixin,
    PositionManagerMonitoringMixin,
    PositionManagerSignalMixin,
    PositionManagerCoreMixin,
):
    pass


async def main():
    """Ana giriş noktası"""
    try:
        # Komut satırı argümanlarını parse et
        parser = argparse.ArgumentParser(description='Position Manager')
        parser.add_argument('--test', action='store_true', help='Run in test mode')
        parser.add_argument(
            '--exchanges',
            default=os.getenv("TRADER_ENABLED_EXCHANGES", "binance,gate,mexc"),
            help='Comma-separated exchanges to enable (default: binance,gate,mexc)',
        )
        parser.add_argument(
            '--binance-testnet',
            action='store_true',
            default=(os.getenv("BINANCE_USE_TESTNET") or "").strip().lower() in {"1", "true", "yes", "on"},
            help='Use Binance Spot Testnet instead of live Binance endpoints',
        )
        parser.add_argument(
            '--disable-position-ws',
            action='store_true',
            default=(os.getenv("POSITION_WS_DISABLED") or "").strip().lower() in {"1", "true", "yes", "on"},
            help='Skip upstream position WebSocket connection during local smoke tests',
        )
        args = parser.parse_args()
        enabled_exchanges = {
            part.strip().lower()
            for part in str(args.exchanges).split(",")
            if part.strip()
        } or {"binance", "gate", "mexc"}
        
        # .env dosyasının bulunduğundan emin ol
        if not os.path.exists('.env'):
            logger.error("No .env file found. Please create one with your API keys.")
            return
        
        # Env değişkenlerini yükle
        load_dotenv()

        binance_key_type = (
            (os.getenv("BINANCE_TESTNET_KEY_TYPE") if args.binance_testnet else None)
            or os.getenv("BINANCE_KEY_TYPE")
            or "hmac"
        ).strip().lower()
        
        # Gerekli API anahtarlarını kontrol et
        missing_vars = []
        if "binance" in enabled_exchanges:
            if args.binance_testnet:
                if binance_key_type == "hmac":
                    has_testnet_pair = bool(os.getenv("BINANCE_TESTNET_API_KEY")) and bool(os.getenv("BINANCE_TESTNET_API_SECRET"))
                    has_default_pair = bool(os.getenv("BINANCE_API_KEY")) and bool(os.getenv("BINANCE_API_SECRET"))
                    if not (has_testnet_pair or has_default_pair):
                        missing_vars.extend(["BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET"])
                else:
                    has_testnet_key = bool(os.getenv("BINANCE_TESTNET_API_KEY"))
                    has_default_key = bool(os.getenv("BINANCE_API_KEY"))
                    private_key_path = os.getenv("BINANCE_TESTNET_PRIVATE_KEY_PATH") or os.getenv("BINANCE_PRIVATE_KEY_PATH")
                    if not (has_testnet_key or has_default_key):
                        missing_vars.append("BINANCE_TESTNET_API_KEY")
                    if not private_key_path:
                        missing_vars.append("BINANCE_TESTNET_PRIVATE_KEY_PATH")
            else:
                if binance_key_type == "hmac" and (not os.getenv("BINANCE_API_KEY") or not os.getenv("BINANCE_API_SECRET")):
                    missing_vars.extend(["BINANCE_API_KEY", "BINANCE_API_SECRET"])
                elif binance_key_type != "hmac":
                    if not os.getenv("BINANCE_API_KEY"):
                        missing_vars.append("BINANCE_API_KEY")
                    if not os.getenv("BINANCE_PRIVATE_KEY_PATH"):
                        missing_vars.append("BINANCE_PRIVATE_KEY_PATH")
        if "gate" in enabled_exchanges:
            if not os.getenv("GATE_API_KEY") or not os.getenv("GATE_API_SECRET"):
                missing_vars.extend(["GATE_API_KEY", "GATE_API_SECRET"])
        if "mexc" in enabled_exchanges:
            if not os.getenv("MEXC_API_KEY") or not os.getenv("MEXC_API_SECRET"):
                missing_vars.extend(["MEXC_API_KEY", "MEXC_API_SECRET"])

        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return
        
        # Test modu kontrolü
        if args.test:
            logger.info("Running in TEST MODE - no real trades will be executed")
        if args.binance_testnet:
            logger.info("Binance Spot Testnet mode enabled")
        
        # Normal çalışma modu
        logger.info("Starting Position Manager...")
        
        # Pozisyon yöneticisini oluştur ve başlat
        manager = PositionManager(
            test_mode=args.test,
            enabled_exchanges=sorted(enabled_exchanges),
            binance_use_testnet=args.binance_testnet,
            position_ws_enabled=not args.disable_position_ws,
        )
        async with manager as active_manager:
            await active_manager.start()
    
    except KeyboardInterrupt:
        logger.info("Position Manager stopped by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())
