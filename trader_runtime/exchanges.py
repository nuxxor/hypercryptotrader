from .common import *
import base64

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding as asym_padding, rsa


class BinanceAPI:
    """Binance API etkileşimleri - Çoklu hesap desteği ile"""
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        session: aiohttp.ClientSession,
        test_mode: bool = False,
        account_name: str = "PRIMARY",
        local_ip: Optional[str] = None,
        use_testnet: bool = False,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = session
        self.local_ip = (local_ip or "").strip() or None
        self.price_subscribers = {}  # {symbol: [callback1, callback2, ...]}
        self.last_price_update = {}  # {symbol: timestamp}
        self.ws_connections = {}  # {symbol_group: websocket_task}
        self.active_ws_tasks = set()  # Aktif WebSocket görevlerini izle
        self.test_mode = test_mode
        self.use_testnet = use_testnet
        self.simulated_mode = bool(self.test_mode and not self.use_testnet)
        self.account_name = account_name

        if self.use_testnet:
            self.base_url = "https://testnet.binance.vision"
            self.stream_url_base = "wss://stream.testnet.binance.vision"
            self.ws_url = "wss://ws-api.testnet.binance.vision/ws-api/v3"
        else:
            self.base_url = "https://api.binance.com"
            self.stream_url_base = "wss://stream.binance.com:9443"
            self.ws_url = "wss://ws-api.binance.com:9443/ws-api/v3"

        self.signature_mode = self._resolve_signature_mode()
        self.private_key_path = self._resolve_private_key_path()
        self.private_key = self._load_private_key()
        
        # Exchange info cache - LOT_SIZE için kritik
        self.exchange_info_cache = {}
        self.exchange_info_last_update = 0
        self.exchange_info_cache_ttl = 3600  # 1 saat
        self.balance_cache_ttl = float(os.getenv("MEXC_BALANCE_CACHE_TTL", "30"))
        self._balance_cache: Dict[str, Tuple[float, float]] = {}
        self.balance_cache_ttl = float(os.getenv("BINANCE_BALANCE_CACHE_TTL", "3600"))
        self._balance_cache: Dict[str, Tuple[float, float]] = {}
        self.order_retry_sequence = _parse_retry_sequence(
            os.getenv("BINANCE_ORDER_RETRY_SEQUENCE"),
            (0.3, 0.45, 0.60, 0.80, 1.00),
        )
        try:
            self.order_retry_tail_step = max(
                0.0, float(os.getenv("BINANCE_ORDER_RETRY_TAIL_STEP", "0.20"))
            )
        except ValueError:
            self.order_retry_tail_step = 0.20
        try:
            self.max_order_attempts = max(
                1, int(os.getenv("BINANCE_ORDER_MAX_ATTEMPTS", "20"))
            )
        except ValueError:
            self.max_order_attempts = 20
        self.balance_recovery_threshold = self._load_binance_balance_recovery_threshold()
    
    async def _generate_signature(self, params: dict) -> str:
        """Binance API isteği için imza oluştur"""
        query_string = '&'.join([f"{key}={params[key]}" for key in params])
        payload = query_string.encode('utf-8')

        if self.signature_mode == "hmac":
            return hmac.new(
                self.api_secret.encode('utf-8'),
                payload,
                hashlib.sha256
            ).hexdigest()

        if self.private_key is None:
            raise RuntimeError(
                f"Binance signature mode '{self.signature_mode}' requires a private key file"
            )

        if self.signature_mode == "rsa":
            signature_bytes = self.private_key.sign(
                payload,
                asym_padding.PKCS1v15(),
                hashes.SHA256(),
            )
        elif self.signature_mode == "ed25519":
            signature_bytes = self.private_key.sign(payload)
        else:
            raise RuntimeError(f"Unsupported Binance signature mode: {self.signature_mode}")

        return base64.b64encode(signature_bytes).decode("ascii")

    def _resolve_signature_mode(self) -> str:
        if self.use_testnet:
            raw = (
                os.getenv("BINANCE_TESTNET_KEY_TYPE")
                or os.getenv("BINANCE_KEY_TYPE")
                or "hmac"
            )
        else:
            raw = os.getenv("BINANCE_KEY_TYPE") or "hmac"

        mode = raw.strip().lower()
        aliases = {
            "hmac-sha-256": "hmac",
            "hmac_sha_256": "hmac",
            "rsa_pkcs1v15": "rsa",
            "ed25519_api": "ed25519",
            "ed25519-key": "ed25519",
        }
        mode = aliases.get(mode, mode)
        if mode not in {"hmac", "rsa", "ed25519"}:
            logger.warning(
                "Unknown Binance key type '%s' for %s; falling back to hmac",
                raw,
                self.account_name,
            )
            return "hmac"
        return mode

    def _resolve_private_key_path(self) -> Optional[str]:
        if self.signature_mode == "hmac":
            return None

        if self.use_testnet:
            raw_path = (
                os.getenv("BINANCE_TESTNET_PRIVATE_KEY_PATH")
                or os.getenv("BINANCE_PRIVATE_KEY_PATH")
            )
        else:
            raw_path = os.getenv("BINANCE_PRIVATE_KEY_PATH")

        if not raw_path:
            return None
        return os.path.expanduser(raw_path.strip())

    def _load_private_key(self):
        if self.signature_mode == "hmac":
            return None
        if not self.private_key_path:
            logger.warning(
                "Binance key type '%s' selected for %s but no private key path configured",
                self.signature_mode,
                self.account_name,
            )
            return None

        try:
            with open(self.private_key_path, "rb") as handle:
                key_data = handle.read()
            private_key = serialization.load_pem_private_key(key_data, password=None)
        except FileNotFoundError:
            logger.error(
                "Binance private key file not found for %s: %s",
                self.account_name,
                self.private_key_path,
            )
            return None
        except Exception as exc:
            logger.error(
                "Failed to load Binance private key for %s from %s: %s",
                self.account_name,
                self.private_key_path,
                exc,
            )
            return None

        if self.signature_mode == "rsa" and not isinstance(private_key, rsa.RSAPrivateKey):
            logger.error(
                "Configured Binance RSA key for %s is not an RSA private key: %s",
                self.account_name,
                self.private_key_path,
            )
            return None

        if self.signature_mode == "ed25519" and not isinstance(private_key, ed25519.Ed25519PrivateKey):
            logger.error(
                "Configured Binance Ed25519 key for %s is not an Ed25519 private key: %s",
                self.account_name,
                self.private_key_path,
            )
            return None

        logger.info(
            "Loaded Binance %s private key for %s from %s",
            self.signature_mode,
            self.account_name,
            self.private_key_path,
        )
        return private_key
    
    async def _send_request(self, method: str, endpoint: str, params: dict = None, 
                        signed: bool = False) -> dict:
        """Binance API'ye istek gönder - Sessiz mod"""
        if self.simulated_mode:
            # Test modunda API çağrısı yapma, sabit değerler döndür
            if endpoint == '/api/v3/ticker/price':
                return {"price": 1.0}
            elif endpoint == '/api/v3/account':
                return {"balances": [{"asset": params.get("asset", "USDT"), "free": 1000.0}]}
            elif endpoint == '/api/v3/order':
                return {"orderId": "test_order_123", "status": "FILLED"}
            elif endpoint == '/api/v3/exchangeInfo':
                return self._get_test_exchange_info(params.get("symbol"))
            else:
                return {"success": True}
        
        if params is None:
            params = {}
            
        url = f"{self.base_url}{endpoint}"
        headers = {'X-MBX-APIKEY': self.api_key}
        
        try:
            if signed:
                # Önce sunucu zamanını al
                try:
                    time_response = await self._send_request('GET', '/api/v3/time')
                    server_time = time_response.get('serverTime', int(time.time() * 1000))
                except Exception:
                    # Sunucu zamanı alınamazsa yerel zamanı kullan ama güvenlik payıyla
                    server_time = int((time.time() - 1) * 1000)
                
                params['timestamp'] = server_time
                params['recvWindow'] = 5000
                params['signature'] = await self._generate_signature(params)
            
            # API çağrılarını DEBUG seviyesine indir
            logger.debug(f"Binance API {method} {endpoint} ({self.account_name})")
            
            if method == 'GET':
                async with self.session.get(url, params=params, headers=headers) as response:
                    response_data = await response.json()
                    if response.status != 200:
                        logger.error(f"Binance API error ({self.account_name}): {response.status} - {response_data}")
                    return response_data
            elif method == 'POST':
                async with self.session.post(url, params=params, headers=headers) as response:
                    response_data = await response.json()
                    if response.status != 200:
                        logger.error(f"Binance API error ({self.account_name}): {response.status} - {response_data}")
                    else:
                        # Başarılı order'ları logla
                        if endpoint == '/api/v3/order':
                            logger.info(f"✅ Binance order success ({self.account_name}): {response_data.get('orderId')}")
                    return response_data
            elif method == 'DELETE':
                async with self.session.delete(url, params=params, headers=headers) as response:
                    response_data = await response.json()
                    if response.status != 200:
                        logger.error(f"Binance API error ({self.account_name}): {response.status} - {response_data}")
                    return response_data
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Connection error to Binance API ({self.account_name}): {str(e)}")
            return {"error": f"Connection error: {str(e)}"}
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to Binance API ({self.account_name})")
            return {"error": "Request timeout"}
        except Exception as e:
            logger.error(f"Binance API request error ({self.account_name}): {str(e)}")
            logger.debug(traceback.format_exc())
            return {"error": str(e)}

    def _binance_order_retry_delay(self, attempt: int) -> float:
        return _retry_delay_for_attempt(self.order_retry_sequence, self.order_retry_tail_step, attempt)

    def _load_binance_balance_recovery_threshold(self) -> Decimal:
        raw = os.getenv("BINANCE_BALANCE_RECOVERY_THRESHOLD", "0.0001")
        try:
            value = Decimal(str(raw))
        except Exception:
            value = Decimal("0.0001")
        if value < 0:
            value = Decimal("0")
        return value

    def _build_client_order_id(self, symbol: str) -> str:
        """Generate a Binance client order id that fits the 36-char constraint."""
        safe_account = re.sub(r"[^a-z0-9]", "", (self.account_name or "").lower()) or "acct"
        safe_symbol = re.sub(r"[^a-z0-9]", "", (symbol or "").lower()) or "sym"
        safe_account = safe_account[:6]
        safe_symbol = safe_symbol[:6]

        timestamp_fragment = format(int(time.time() * 1000), "x")[-8:]
        random_fragment = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))

        client_order_id = f"pm-{safe_account}-{safe_symbol}-{timestamp_fragment}{random_fragment}"
        if len(client_order_id) > 36:
            client_order_id = client_order_id[:36]
        return client_order_id

    def _extract_base_asset(self, symbol: str) -> str:
        symbol = (symbol or "").upper()
        quote_assets = ("USDT", "BUSD", "USDC", "BTC", "ETH")
        for quote in quote_assets:
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[: -len(quote)]
        if len(symbol) > 4:
            return symbol[:-4]
        return symbol

    def _get_test_exchange_info(self, symbol=None):
        """Test modu için exchange info"""
        test_info = {
            "symbols": [{
                "symbol": symbol or "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00001000",
                        "maxQty": "9000.00000000",
                        "stepSize": "0.00001000"
                    },
                    {
                        "filterType": "MARKET_LOT_SIZE",
                        "minQty": "0.00000000",
                        "maxQty": "100.00000000",
                        "stepSize": "0.00001000"
                    },
                    {
                        "filterType": "NOTIONAL",
                        "minNotional": "10.00000000",
                        "applyMinToMarket": True
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": "10.00000000"
                    }
                ]
            }]
        }
        return test_info
    
    async def get_exchange_info(self, symbol: str = None):
        """Exchange info'yu al (LOT_SIZE kuralları için)"""
        # Cache kontrolü
        current_time = time.time()
        if (self.exchange_info_cache and 
            current_time - self.exchange_info_last_update < self.exchange_info_cache_ttl):
            if symbol:
                return self.exchange_info_cache.get(symbol)
            return self.exchange_info_cache
        
        try:
            # Exchange info'yu çek
            params = {}
            if symbol:
                params['symbol'] = symbol
                
            response = await self._send_request('GET', '/api/v3/exchangeInfo', params)
            
            if 'symbols' in response:
                # Cache'i güncelle
                self.exchange_info_cache = {}
                for symbol_info in response['symbols']:
                    symbol_name = symbol_info['symbol']
                    self.exchange_info_cache[symbol_name] = symbol_info
                self.exchange_info_last_update = current_time
                
                if symbol:
                    return self.exchange_info_cache.get(symbol)
                return self.exchange_info_cache
            
            return None
        except Exception as e:
            logger.error(f"Error getting exchange info ({self.account_name}): {str(e)}")
            return None
    
    def get_lot_size_filter(self, symbol_info: dict):
        """Symbol info'dan LOT_SIZE filtresini al"""
        if not symbol_info or 'filters' not in symbol_info:
            return None
            
        for filter_item in symbol_info['filters']:
            if filter_item['filterType'] == 'LOT_SIZE':
                return {
                    'minQty': float(filter_item['minQty']),
                    'maxQty': float(filter_item['maxQty']),
                    'stepSize': float(filter_item['stepSize'])
                }
        return None
    
    def get_market_lot_size_filter(self, symbol_info: dict):
        """Market orders için MARKET_LOT_SIZE filtresini al"""
        if not symbol_info or 'filters' not in symbol_info:
            return None
            
        for filter_item in symbol_info['filters']:
            if filter_item['filterType'] == 'MARKET_LOT_SIZE':
                return {
                    'minQty': float(filter_item['minQty']),
                    'maxQty': float(filter_item['maxQty']),
                    'stepSize': float(filter_item['stepSize'])
                }
        return None

    def get_notional_filter(self, symbol_info: dict):
        """Symbol info'dan NOTIONAL veya MIN_NOTIONAL filtresini al"""
        if not symbol_info or 'filters' not in symbol_info:
            return None

        for filter_item in symbol_info['filters']:
            if filter_item.get('filterType') == 'NOTIONAL':
                return {
                    'minNotional': float(filter_item.get('minNotional', 0)),
                    'applyMinToMarket': filter_item.get('applyMinToMarket'),
                    'maxNotional': float(filter_item.get('maxNotional', 0)) if filter_item.get('maxNotional') else None
                }

        for filter_item in symbol_info['filters']:
            if filter_item.get('filterType') == 'MIN_NOTIONAL':
                return {
                    'minNotional': float(filter_item.get('minNotional', 0))
                }

        return None
    
    def round_step_size(self, quantity: float, step_size: float) -> str:
        """Quantity'yi step size'a göre yuvarla - Araştırmadan öğrenilen yöntem"""
        try:
            # Import kontrolü
            from decimal import Decimal, ROUND_DOWN, InvalidOperation
            
            # Önce quantity'nin geçerli bir sayı olduğundan emin ol
            if not isinstance(quantity, (int, float)):
                logger.error(f"Invalid quantity type: {type(quantity)}, value: {quantity}")
                return None
                
            quantity = float(quantity)
            if math.isnan(quantity) or math.isinf(quantity):
                logger.error(f"Invalid quantity value: {quantity}")
                return None
                
            if quantity <= 0:
                logger.error(f"Quantity must be positive: {quantity}")
                return None
                
            # Step size kontrolü
            if not isinstance(step_size, (int, float)) or step_size <= 0:
                logger.error(f"Invalid step_size value: {step_size}")
                return None
                
            if step_size == 1.0:
                # Integer only
                return str(int(math.floor(quantity)))
            elif step_size < 1.0:
                # Decimal kullan - string'den başlat!
                try:
                    # String formatını düzelt
                    quantity_str = f"{quantity:.10f}"  # Maksimum 10 basamak
                    step_size_str = f"{step_size:.10f}"
                    
                    quantity_dec = Decimal(quantity_str)
                    step_size_dec = Decimal(step_size_str)
                    
                    # TRUNCATE, round değil! (araştırmadan öğrenilen kritik nokta)
                    result = quantity_dec - (quantity_dec % step_size_dec)
                    
                    # String'e çevir ve gereksiz sıfırları temizle
                    result_str = str(result)
                    if '.' in result_str:
                        result_str = result_str.rstrip('0').rstrip('.')
                    
                    return result_str
                except (InvalidOperation, ValueError) as e:
                    logger.error(f"Decimal operation error: {e}, quantity={quantity}, step_size={step_size}")
                    # Fallback to simple formatting
                    precision = len(str(step_size).split('.')[-1]) if '.' in str(step_size) else 0
                    return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
            else:
                # step_size > 1.0 durumu
                steps = math.floor(quantity / step_size)
                result = steps * step_size
                return str(int(result)) if result == int(result) else str(result)
        except Exception as e:
            logger.error(f"Error in round_step_size: {e}, quantity={quantity}, step_size={step_size}")
            logger.debug(traceback.format_exc())
            return None
    
    def format_quantity_with_lot_size(self, quantity: float, lot_size_filter: dict, is_market_order: bool = True) -> str:
        """Miktarı LOT_SIZE kurallarına göre formatla - Production ready"""
        try:
            if not lot_size_filter:
                # Filter yoksa varsayılan formatı kullan
                return self.format_quantity(quantity, 8)
            
            # Lot size filter değerlerini kontrol et
            try:
                min_qty = float(lot_size_filter.get('minQty', 0))
                max_qty = float(lot_size_filter.get('maxQty', 9000))
                step_size = float(lot_size_filter.get('stepSize', 0.00001))
                
                # Step size 0 veya çok küçükse varsayılan değer kullan
                if step_size <= 0 or step_size < 0.00000001:
                    logger.warning(f"Invalid step_size {step_size}, using default 0.00001")
                    step_size = 0.00001
                    
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid lot_size_filter values: {lot_size_filter}, error: {e}")
                return None
            
            # Quantity'yi float'a çevir ve kontrol et
            try:
                quantity = float(quantity)
            except (ValueError, TypeError):
                logger.error(f"Cannot convert quantity to float: {quantity}")
                return None
            
            # Sıfır veya negatif kontrol
            if quantity <= 0 or math.isnan(quantity) or math.isinf(quantity):
                logger.error(f"Invalid quantity: {quantity}")
                return None
            
            # Maximum kontrolü
            if quantity > max_qty:
                logger.warning(f"Quantity {quantity} exceeds maxQty {max_qty}, using maxQty")
                quantity = max_qty
            
            # Step size'a göre formatla
            formatted = self.round_step_size(quantity, step_size)
            
            if formatted is None:
                logger.error(f"round_step_size returned None for quantity={quantity}, step_size={step_size}")
                # Fallback to default formatting
                return self.format_quantity(quantity, 8)
                
            try:
                formatted_float = float(formatted)
            except (ValueError, TypeError):
                logger.error(f"Cannot convert formatted value to float: {formatted}")
                return None
            
            # Minimum kontrolü - formatted değer üzerinden
            if formatted_float < min_qty:
                # Eğer minimum'un altındaysa, minimum'u kullan
                # Ama önce minimum'u da step size'a göre formatla
                min_formatted = self.round_step_size(min_qty, step_size)
                if min_formatted is None:
                    logger.error(f"Cannot format minimum quantity: {min_qty}")
                    return self.format_quantity(min_qty, 8)
                logger.warning(f"Quantity {formatted} is less than minQty {min_qty}, using {min_formatted}")
                return min_formatted
            
            return formatted
        except Exception as e:
            logger.error(f"Error in format_quantity_with_lot_size: {e}")
            logger.debug(traceback.format_exc())
            return None
    
    def format_quantity(self, quantity: float, precision: int = 8) -> str:
        """Miktarı Binance kurallarına göre formatla (eski method, uyumluluk için)"""
        try:
            # Önce quantity'nin geçerli olduğunu kontrol et
            if not isinstance(quantity, (int, float)):
                logger.error(f"Invalid quantity type: {type(quantity)}")
                return "0"
                
            quantity = float(quantity)
            if quantity <= 0 or math.isnan(quantity) or math.isinf(quantity):
                logger.error(f"Invalid quantity value: {quantity}")
                return "0"
            
            # Decimal kullanarak hassas kesme (yuvarlamadan)
            decimal_quantity = Decimal(str(quantity))
            formatted = decimal_quantity.quantize(
                Decimal(f"0.{'0' * precision}"),
                rounding=ROUND_DOWN  # Aşağı yuvarla, böylece limitleri aşmayız
            )
            
            # Gereksiz sıfırları kaldır
            result = str(formatted).rstrip('0').rstrip('.')
            
            # Çok küçük değerler için bilimsel notasyonu önle
            if 'e' in result.lower():
                return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
            
            return result
        except Exception as e:
            logger.error(f"Error in format_quantity: {e}, quantity={quantity}")
            # Hata durumunda basit format
            return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
    
    async def get_price(self, symbol: str) -> float:
        """Bir sembol için mevcut fiyatı al"""
        if self.simulated_mode:
            return 1.0  # Test modunda sabit fiyat
            
        try:
            response = await self._send_request('GET', '/api/v3/ticker/price', {'symbol': symbol})
            if 'price' in response:
                return float(response['price'])
            logger.error(f"Failed to get price for {symbol} ({self.account_name}): {response}")
            return 0
        except Exception as e:
            logger.error(f"Error in get_price for {symbol} ({self.account_name}): {str(e)}")
            return 0

    async def place_market_sell(self, symbol: str, quantity: float) -> dict:
        """Market satış emri ver - LOT_SIZE desteği ile"""
        if self.simulated_mode:
            logger.info(f"TEST MODE: Simulating market sell for {symbol}, quantity: {quantity} ({self.account_name})")
            return {"orderId": "test_order_123", "status": "FILLED"}
            
        try:
            # Quantity kontrolü
            try:
                quantity = float(quantity)
                if quantity <= 0 or math.isnan(quantity) or math.isinf(quantity):
                    logger.error(f"Invalid quantity for {symbol}: {quantity}")
                    return {"error": f"Invalid quantity: {quantity}"}
            except (ValueError, TypeError) as e:
                logger.error(f"Cannot convert quantity to float for {symbol}: {quantity}, error: {e}")
                return {"error": f"Invalid quantity type: {type(quantity)}"}
            
            # Exchange info'yu al
            symbol_info = await self.get_exchange_info(symbol)
            if not symbol_info:
                logger.warning(f"Could not get exchange info for {symbol} ({self.account_name}), using default formatting")
                # Varsayılan formatı kullan
                quantity_str = self.format_quantity(quantity, 8)
            else:
                # MARKET_LOT_SIZE'ın stepSize'ı 0 ise normal LOT_SIZE kullan
                market_lot_filter = self.get_market_lot_size_filter(symbol_info)
                lot_size_filter = self.get_lot_size_filter(symbol_info)
                
                # Hangi filter'ı kullanacağımıza karar ver
                if market_lot_filter and market_lot_filter.get('stepSize', 0) > 0:
                    # MARKET_LOT_SIZE geçerliyse onu kullan
                    active_filter = market_lot_filter
                    logger.debug(f"Using MARKET_LOT_SIZE filter for {symbol}")
                elif lot_size_filter and lot_size_filter.get('stepSize', 0) > 0:
                    # Normal LOT_SIZE kullan
                    active_filter = lot_size_filter
                    logger.debug(f"Using LOT_SIZE filter for {symbol} (MARKET_LOT_SIZE stepSize=0)")
                else:
                    # Hiçbir filter yoksa varsayılan
                    logger.warning(f"No valid LOT_SIZE filter found for {symbol}, using default formatting")
                    quantity_str = self.format_quantity(quantity, 8)
                    active_filter = None
                
                if active_filter:
                    # Miktarı formatla
                    quantity_str = self.format_quantity_with_lot_size(quantity, active_filter, is_market_order=True)
                    
                    if quantity_str is None:
                        # Fallback to simple formatting with stepSize
                        step_size = active_filter.get('stepSize', 0.0001)
                        if step_size > 0:
                            # Step size'a göre basit yuvarlama
                            precision = len(str(step_size).split('.')[-1]) if '.' in str(step_size) else 0
                            quantity_str = f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
                            
                            # Step size'a göre truncate
                            if '.' in quantity_str:
                                parts = quantity_str.split('.')
                                if len(parts[1]) > precision:
                                    quantity_str = f"{parts[0]}.{parts[1][:precision]}"
                            
                            logger.debug(f"Using simple formatting for {symbol}: {quantity} -> {quantity_str} (precision={precision})")
                        else:
                            logger.warning(f"format_quantity_with_lot_size failed for {symbol}, using default formatting")
                            quantity_str = self.format_quantity(quantity, 8)
                    else:
                        logger.debug(
                            f"Formatted {symbol} quantity: {quantity} -> {quantity_str} "
                            f"(minQty: {active_filter.get('minQty', 'N/A')}, "
                            f"stepSize: {active_filter.get('stepSize', 'N/A')})"
                        )
            
            # Son kontrol
            try:
                qty_float = float(quantity_str)
                if qty_float <= 0:
                    logger.error(f"Quantity too small after formatting: {quantity} -> {quantity_str}")
                    return {"error": "Quantity too small after formatting"}
            except (TypeError, ValueError):
                logger.error(f"Invalid quantity format: {quantity_str}")
                return {"error": "Invalid quantity format"}
            
            # Pre-flight MIN_NOTIONAL kontrolü
            try:
                qty_for_notional = float(quantity_str)
            except Exception:
                qty_for_notional = quantity

            notional_filter = self.get_notional_filter(symbol_info) if symbol_info else None
            if notional_filter and notional_filter.get("minNotional"):
                min_notional = float(notional_filter["minNotional"])
                current_price = await self.get_price(symbol)
                if current_price > 0:
                    projected_value = qty_for_notional * current_price
                    if projected_value < min_notional:
                        logger.warning(
                            f"Pre-flight MIN_NOTIONAL check failed for {symbol} ({self.account_name}): "
                            f"value=${projected_value:.2f}, required=${min_notional:.2f}"
                        )
                        return {
                            "error": "MIN_NOTIONAL precheck failed",
                            "code": -1013,
                            "minNotional": min_notional,
                            "projectedValue": projected_value,
                        }

            base_asset = self._extract_base_asset(symbol)
            baseline_balance: Optional[Decimal] = None
            if base_asset:
                try:
                    baseline_balance = Decimal(str(await self.get_balance(base_asset, use_cache=False)))
                except Exception:
                    baseline_balance = None

            last_error: Optional[str] = None
            attempted_client_ids: List[str] = []

            for attempt in range(1, self.max_order_attempts + 1):
                client_order_id = self._build_client_order_id(symbol)
                attempted_client_ids.append(client_order_id)
                params = {
                    'symbol': symbol,
                    'side': 'SELL',
                    'type': 'MARKET',
                    'quantity': quantity_str,
                    'newClientOrderId': client_order_id,
                }
                
                logger.info(
                    f"🔴 MARKET SELL ORDER: {symbol} quantity={quantity_str} "
                    f"({self.account_name}) attempt {attempt}/{self.max_order_attempts}"
                )
                try:
                    response = await self._send_request('POST', '/api/v3/order', params, signed=True)
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        f"Binance order attempt {attempt}/{self.max_order_attempts} failed "
                        f"({self.account_name}): {last_error}"
                    )
                    if attempt < self.max_order_attempts:
                        await asyncio.sleep(self._binance_order_retry_delay(attempt))
                    continue

                if 'orderId' in response:
                    logger.info(
                        f"✅ Binance market sell order placed successfully "
                        f"({self.account_name}): {response['orderId']} attempt {attempt}"
                    )
                    return response

                last_error = json.dumps(response)
                logger.error(
                    f"❌ Failed to place Binance market sell order ({self.account_name}) attempt {attempt}: {response}"
                )

                if 'code' in response and response['code'] == -1013 and symbol_info:
                    lot_filter = self.get_lot_size_filter(symbol_info) or {}
                    market_lot_filter = self.get_market_lot_size_filter(symbol_info) or {}
                    logger.error(
                        f"LOT_SIZE details for {symbol}: "
                        f"LOT_SIZE={lot_filter}, MARKET_LOT_SIZE={market_lot_filter}, "
                        f"Attempted quantity={quantity_str}"
                    )
                    break  # structural issue, do not keep retrying

                if attempt < self.max_order_attempts:
                    await asyncio.sleep(self._binance_order_retry_delay(attempt))

            recovered = await self._recover_binance_order(
                symbol=symbol,
                base_asset=base_asset,
                baseline_balance=baseline_balance,
                client_order_ids=attempted_client_ids,
            )
            if recovered:
                return recovered

            return {
                "error": last_error or "order_failed",
                "symbol": symbol,
                "account": self.account_name,
            }
        except Exception as e:
            logger.error(f"Error in place_market_sell for {symbol} ({self.account_name}): {type(e).__name__}: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"error": f"{type(e).__name__}: {str(e)}"}
    
    async def get_balance(self, asset: str = "USDT", use_cache: bool = True) -> float:
        """Belirli bir varlık için hesap bakiyesini al"""
        if self.simulated_mode:
            return 1000.0  # Test modunda sabit bakiye
            
        try:
            asset_key = (asset or "").upper()
            now = time.time()

            if use_cache:
                cached = self._balance_cache.get(asset_key)
                if cached and (now - cached[1]) <= self.balance_cache_ttl:
                    return cached[0]

            response = await self._send_request('GET', '/api/v3/account', {}, signed=True)
            
            if isinstance(response, dict) and 'balances' in response:
                timestamp = time.time()
                result = 0.0
                for balance in response['balances']:
                    asset_name = (balance.get('asset') or "").upper()
                    if not asset_name:
                        continue
                    try:
                        free_amount = float(balance.get('free', 0))
                    except (TypeError, ValueError):
                        free_amount = 0.0
                    self._balance_cache[asset_name] = (free_amount, timestamp)
                    if asset_name == asset_key:
                        result = free_amount
                if asset_key not in self._balance_cache and asset_key:
                    self._balance_cache[asset_key] = (0.0, timestamp)
                    logger.info(f"No {asset_key} balance found on Binance ({self.account_name})")
                return result

            if isinstance(response, dict) and response.get("code") == -1003:
                cached = self._balance_cache.get(asset_key)
                if cached:
                    logger.warning(
                        f"Binance rate limit hit for {asset_key} ({self.account_name}); using cached balance"
                    )
                    return cached[0]
                logger.error(f"Binance API rate limit without cache ({self.account_name}): {response}")
            else:
                logger.info(f"No {asset} balance found on Binance ({self.account_name})")
            return 0.0
        except Exception as e:
            logger.error(f"Error in get_balance for {asset} ({self.account_name}): {str(e)}")
            return 0
    
    async def _recover_binance_order(
        self,
        *,
        symbol: str,
        base_asset: Optional[str],
        baseline_balance: Optional[Decimal],
        client_order_ids: List[str],
    ) -> Optional[dict]:
        base_asset = (base_asset or self._extract_base_asset(symbol)).upper() if symbol else None
        balance_drop = Decimal("0")
        if base_asset and baseline_balance is not None:
            try:
                current_balance = Decimal(str(await self.get_balance(base_asset, use_cache=False)))
                balance_drop = baseline_balance - current_balance
            except Exception:
                balance_drop = Decimal("0")

        for client_id in reversed(client_order_ids[-5:]):
            params = {
                "symbol": symbol,
                "origClientOrderId": client_id,
            }
            try:
                order_status = await self._send_request('GET', '/api/v3/order', params, signed=True)
            except Exception:
                continue
            if isinstance(order_status, dict) and order_status.get("status") in {"FILLED", "PARTIALLY_FILLED"}:
                logger.warning(
                    f"Recovered Binance order via status lookup ({self.account_name}): "
                    f"{order_status.get('orderId')} status={order_status.get('status')}"
                )
                order_status["recovered"] = True
                return order_status

        if balance_drop >= self.balance_recovery_threshold:
            logger.warning(
                f"Binance base balance for {base_asset} decreased by {float(balance_drop):.8f} "
                f"after failures ({self.account_name}); assuming order filled"
            )
            return {
                "orderId": f"recovered-{symbol}-{int(time.time() * 1000)}",
                "status": "FILLED",
                "recovered": True,
            }
        return None
        
    # WebSocket metodları
    async def start_price_streams(self, symbols: List[str]):
        """Tüm sembollerin fiyat akışını başlat"""
        if not symbols or self.simulated_mode:
            logger.info(f"Not starting price streams ({self.account_name}): empty symbols or simulated mode")
            return
        
        logger.info(f"Starting Binance WebSocket for {len(symbols)} symbols ({self.account_name}): {symbols}")
        
        # Mevcut görevleri temizle
        for group_id, task in list(self.ws_connections.items()):
            if not task.done():
                logger.info(f"Cancelling existing WebSocket task {group_id} ({self.account_name})")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling WebSocket task {group_id} ({self.account_name}): {str(e)}")
        
        self.ws_connections = {}  
        
        # Sembolleri 200'er gruplar halinde böl (Binance sınırlaması)
        symbol_groups = [symbols[i:i+200] for i in range(0, len(symbols), 200)]
        
        logger.info(f"Creating {len(symbol_groups)} WebSocket connections ({self.account_name})")
        
        # Yeni görevleri başlat
        for group_index, symbol_group in enumerate(symbol_groups):
            # Her grup için ayrı bir WebSocket bağlantısı başlat
            group_key = f"group_{group_index}"
            task = asyncio.create_task(self._start_group_price_stream(symbol_group, group_index))
            self.ws_connections[group_key] = task
            self.active_ws_tasks.add(task)
            # Görev bitince active_ws_tasks'tan çıkarma için callback ekle
            task.add_done_callback(lambda t, gk=group_key: self._task_done_callback(t, gk))
            
            logger.info(f"Started WebSocket task for {group_key} with {len(symbol_group)} symbols ({self.account_name})")
    
    def _task_done_callback(self, task, group_key):
        """WebSocket görevi bittiğinde yapılacak temizlik"""
        if task in self.active_ws_tasks:
            self.active_ws_tasks.remove(task)
        if group_key in self.ws_connections:
            del self.ws_connections[group_key]
    
    async def _start_group_price_stream(self, symbols: List[str], group_index: int):
        """Binance için bir grup sembol için fiyat akışını başlat"""
        if not symbols:
            return
            
        logger.info(f"Starting Binance price stream for group {group_index} with {len(symbols)} symbols ({self.account_name})")
        
        retry_count = 0
        max_retries = 10
        ws = None
        
        try:
            while retry_count < max_retries:
                try:
                    # Stream listesini oluştur
                    streams = []
                    for symbol in symbols:
                        # miniTicker stream kullan (daha hafif)
                        streams.append(f"{symbol.lower()}usdt@miniTicker")
                    
                    # WebSocket URL'ini oluştur
                    stream_names = '/'.join(streams)
                    ws_url = f"{self.stream_url_base}/stream?streams={stream_names}"
                    
                    logger.info(f"Connecting to Binance WebSocket ({self.account_name}): {len(streams)} streams")
                    
                    ws_kwargs = {
                        "ping_interval": DEFAULT_POSITION_WS_PING_INTERVAL,
                        "ping_timeout": DEFAULT_POSITION_WS_PING_TIMEOUT,
                        "close_timeout": DEFAULT_POSITION_WS_CLOSE_TIMEOUT,
                        "open_timeout": DEFAULT_POSITION_WS_OPEN_TIMEOUT,
                    }
                    if self.local_ip:
                        ws_kwargs["local_addr"] = (self.local_ip, 0)

                    async with websockets.connect(ws_url, **ws_kwargs) as ws:
                        logger.info(f"Connected to Binance price stream for group {group_index} ({self.account_name})")
                        
                        # Ping görevi başlat
                        ping_task = asyncio.create_task(self._websocket_ping(ws))
                        
                        try:
                            async for message in ws:
                                try:
                                    data = json.loads(message)
                                    
                                    # Stream data formatı
                                    if 'stream' in data and 'data' in data:
                                        stream_data = data['data']
                                        
                                        # miniTicker formatı
                                        if 's' in stream_data and 'c' in stream_data:
                                            symbol = stream_data['s']  # BTCUSDT
                                            
                                            if symbol.endswith('USDT'):
                                                symbol = symbol[:-4]  # BTC
                                                
                                            price = float(stream_data['c'])  # close price
                                            volume = float(stream_data.get('v', 0))  # volume
                                            
                                            # Timestamp kaydet
                                            self.last_price_update[symbol] = time.time()
                                            
                                            # Debug log
                                            logger.debug(f"Binance {self.account_name} {symbol}: ${price}")
                                            
                                            # Subscribers'ları bilgilendir
                                            if symbol in self.price_subscribers:
                                                for callback in self.price_subscribers[symbol]:
                                                    try:
                                                        await callback(symbol, price, volume)
                                                    except Exception as e:
                                                        logger.error(f"Price callback error ({self.account_name}): {str(e)}")
                                    
                                except json.JSONDecodeError as e:
                                    logger.error(f"Binance JSON parsing error ({self.account_name}): {str(e)}")
                                except Exception as e:
                                    logger.error(f"Binance processing error ({self.account_name}): {str(e)}")
                        finally:
                            if ping_task and not ping_task.done():
                                ping_task.cancel()
                                try:
                                    await ping_task
                                except asyncio.CancelledError:
                                    pass
                        
                        # Normal çıkış
                        logger.warning(f"Binance stream closed ({self.account_name}). Reconnecting...")
                        await asyncio.sleep(1)
                        retry_count += 1
                            
                except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
                    logger.warning(f"Binance disconnected ({self.account_name}): {str(e)}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Binance error ({self.account_name}): {str(e)}. Reconnecting in 10s...")
                    logger.debug(traceback.format_exc())
                    await asyncio.sleep(10)
                    retry_count += 1
                except asyncio.CancelledError:
                    logger.info(f"Binance task cancelled ({self.account_name})")
                    raise
            
            logger.critical(f"Failed to maintain Binance stream after {max_retries} retries ({self.account_name})")
        
        finally:
            logger.info(f"Binance WebSocket task exiting ({self.account_name})")
            if ws and not ws.closed:
                try:
                    await ws.close()
                except Exception:
                    pass
    
    async def _websocket_ping(self, ws):
        """Binance WebSocket bağlantısı için ek ping gerekmiyor; bağlantıyı izlemek için boş döngü."""
        try:
            while True:
                await asyncio.sleep(45)
                # websockets kütüphanesi kendi ping/pong mekanizmasını yönetiyor
                pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"WebSocket ping error: {str(e)}")
        
    def subscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerine abone ol"""
        if symbol not in self.price_subscribers:
            self.price_subscribers[symbol] = []
        self.price_subscribers[symbol].append(callback)
    
    def unsubscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerinden aboneliği kaldır"""
        if symbol in self.price_subscribers and callback in self.price_subscribers[symbol]:
            self.price_subscribers[symbol].remove(callback)


class GateAPITemporaryError(Exception):
    """Transient network/service issue; caller can retry without closing positions."""
    pass


class GateAPIHardError(Exception):
    """Non-retryable Gate.io error (business rule, auth etc.)."""
    pass


@dataclass
class BalanceSnapshot:
    currency: str
    available: Decimal
    locked: Decimal
    effective_total: Decimal
    last_update_time: Optional[float] = None
    stale: bool = False


class GateAPI:
    """Gate.io API etkileşimleri"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        session: aiohttp.ClientSession,
        test_mode: bool = False,
        local_ip: Optional[str] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.gateio.ws/api/v4"
        self.session = session
        self.local_ip = (local_ip or "").strip() or None
        self.ws_url = "wss://api.gateio.ws/ws/v4/"
        self.price_subscribers = {}
        self.last_price_update = {}
        self.ws_connections = {}  # {symbol_group: websocket_task}
        self.active_ws_tasks = set()  # Aktif WebSocket görevlerini izle
        self.test_mode = test_mode
        self._balance_cache: Dict[str, Tuple[BalanceSnapshot, float]] = {}
        self._balance_cache_ttl = 3.0  # seconds
        
        # Currency pair info cache
        self.currency_pairs_cache = {}
        self.currency_pairs_last_update = 0
        self.currency_pairs_cache_ttl = 3600  # 1 saat
        self.max_order_attempts = max(
            1,
            int(
                os.getenv(
                    "GATE_SELL_ORDER_MAX_ATTEMPTS",
                    os.getenv("GATE_SHORT_MAX_ORDER_ATTEMPTS", "20"),
                )
            ),
        )
        self.order_retry_sequence = _parse_retry_sequence(
            os.getenv("GATE_SELL_ORDER_RETRY_SEQUENCE")
            or os.getenv("GATE_SHORT_ORDER_RETRY_SEQUENCE"),
            (0.3, 0.45, 0.60, 0.80, 1.00),
        )
        try:
            default_tail = float(
                os.getenv("GATE_SHORT_ORDER_RETRY_TAIL_STEP", "0.20")
            )
        except ValueError:
            default_tail = 0.20
        try:
            self.order_retry_tail_step = max(
                0.0,
                float(
                    os.getenv(
                        "GATE_SELL_ORDER_RETRY_TAIL_STEP",
                        str(default_tail),
                    )
                ),
            )
        except ValueError:
            self.order_retry_tail_step = max(0.0, default_tail)
        self.balance_recovery_threshold = self._load_gate_balance_recovery_threshold()
    
    def _generate_signature(self, method: str, url_path: str, query_string: str, 
                           payload_string: str, timestamp: int) -> str:
        """Gate.io API isteği için imza oluştur"""
        message = f"{method}\n{url_path}\n{query_string}\n{payload_string}\n{timestamp}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()
        return signature
    
    async def _send_request(self, method: str, endpoint: str, params: dict = None,
                        data: dict = None) -> dict:
        """Gate.io API isteği gönder ve hataları anlamlı şekilde işaretle."""
        if self.test_mode:
            if endpoint == '/spot/tickers':
                return [{"last": "1.0", "base_volume": "1000000.0"}]
            if endpoint == '/spot/accounts':
                return [{"available": "1000.0", "locked": "0"}]
            if endpoint == '/spot/orders':
                return {"id": "test_order_123", "status": "filled"}
            if endpoint == '/spot/currency_pairs':
                return self._get_test_currency_pairs()
            return {"success": True}

        url = f"{self.base_url}{endpoint}"
        timestamp = int(time.time())

        payload = ""
        if data is not None:
            logger.debug(f"Gate.io API request data: {data}")
            payload = json.dumps(data)

        hashed_payload = hashlib.sha512(payload.encode('utf-8')).hexdigest()
        query_string = ""
        if params:
            query_string = '&'.join([f"{key}={params[key]}" for key in params])

        string_to_sign = f"{method}\n/api/v4{endpoint}\n{query_string}\n{hashed_payload}\n{timestamp}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()

        headers = {
            "KEY": self.api_key,
            "Timestamp": str(timestamp),
            "SIGN": signature,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        request_kwargs = {
            "params": params,
            "headers": headers,
        }
        if method.upper() in {"POST", "DELETE"} and payload:
            request_kwargs["data"] = payload

        try:
            logger.debug(f"Gate.io API {method} {endpoint} params={params}")
            async with self.session.request(method.upper(), url, **request_kwargs) as response:
                xin = response.headers.get("X-In-Time")
                xout = response.headers.get("X-Out-Time")
                trace_id = response.headers.get("X-Gate-Trace-ID")
                if trace_id:
                    logger.debug(
                        f"Gate.io trace_id={trace_id} xin={xin} xout={xout} endpoint={endpoint}"
                    )

                try:
                    response_data = await response.json()
                except aiohttp.ContentTypeError:
                    text = await response.text()
                    if response.status >= 400:
                        raise GateAPIHardError(f"HTTP {response.status}: {text}")
                    raise GateAPITemporaryError(
                        f"Unexpected response format for {endpoint}: {text[:200]}"
                    )

                if response.status >= 400:
                    if isinstance(response_data, dict):
                        label = response_data.get("label") or response_data.get("error") or "HTTP"
                        message = response_data.get("message") or response_data
                        raise GateAPIHardError(f"{label}: {message}")
                    raise GateAPIHardError(f"HTTP {response.status}: {response_data}")

                if isinstance(response_data, dict) and response_data.get("error"):
                    raise GateAPITemporaryError(response_data.get("error"))

                return response_data

        except asyncio.TimeoutError as exc:
            raise GateAPITemporaryError("Timeout connecting to Gate.io API") from exc
        except aiohttp.ClientError as exc:
            raise GateAPITemporaryError(f"Connection error to Gate.io API: {exc}") from exc
        raise GateAPITemporaryError(f"Gate.io API returned empty response for {endpoint}")

    def _gate_order_retry_delay(self, attempt: int) -> float:
        return _retry_delay_for_attempt(self.order_retry_sequence, self.order_retry_tail_step, attempt)

    def _load_gate_balance_recovery_threshold(self) -> Decimal:
        raw = os.getenv("GATE_SELL_BALANCE_RECOVERY_THRESHOLD", "0.0001")
        try:
            value = Decimal(str(raw))
        except Exception:
            value = Decimal("0.0001")
        if value < 0:
            value = Decimal("0")
        return value
    
    def _get_test_currency_pairs(self):
        """Test modu için currency pairs"""
        return [{
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "fee": "0.002",
            "min_quote_amount": "1",
            "min_base_amount": "0.0001",
            "amount_precision": 4,
            "precision": 2,
            "trade_status": "tradable"
        }]

    async def get_balance_snapshot(self, currency: str, use_cache: bool = True) -> BalanceSnapshot:
        """Return available, locked and effective balance for a currency."""
        currency_key = (currency or "").upper()
        now = time.time()

        cached = self._balance_cache.get(currency_key)
        if use_cache and cached:
            snapshot, cached_at = cached
            if now - cached_at <= self._balance_cache_ttl:
                return BalanceSnapshot(
                    currency=snapshot.currency,
                    available=snapshot.available,
                    locked=snapshot.locked,
                    effective_total=snapshot.effective_total,
                    last_update_time=snapshot.last_update_time,
                    stale=True,
                )

        response = await self._send_request('GET', '/spot/accounts', {'currency': currency_key})

        if isinstance(response, dict) and response.get("error"):
            raise GateAPITemporaryError(response["error"])

        account_info = None
        if isinstance(response, list) and response:
            account_info = response[0]

        if not account_info:
            snapshot = BalanceSnapshot(
                currency=currency_key,
                available=Decimal("0"),
                locked=Decimal("0"),
                effective_total=Decimal("0"),
                last_update_time=None,
                stale=False,
            )
            self._balance_cache[currency_key] = (snapshot, now)
            return snapshot

        try:
            available = Decimal(str(account_info.get("available", "0")))
            locked_str = account_info.get("locked") or account_info.get("freeze") or "0"
            locked = Decimal(str(locked_str))
        except Exception as exc:
            raise GateAPITemporaryError(f"Malformed balance payload for {currency_key}: {exc}") from exc

        last_update_raw = account_info.get("update_time") or account_info.get("last_update_time")
        last_update_ts: Optional[float] = None
        if last_update_raw not in (None, ""):
            try:
                last_update_ts = float(last_update_raw)
            except (TypeError, ValueError):
                last_update_ts = None

        snapshot = BalanceSnapshot(
            currency=currency_key,
            available=available,
            locked=locked,
            effective_total=available + locked,
            last_update_time=last_update_ts,
            stale=False,
        )
        self._balance_cache[currency_key] = (snapshot, now)
        return snapshot

    async def get_effective_total(self, currency: str, use_cache: bool = True) -> Decimal:
        snapshot = await self.get_balance_snapshot(currency, use_cache=use_cache)
        return snapshot.effective_total
    
    async def get_currency_pair_info(self, currency_pair: str = None):
        """Currency pair bilgilerini al"""
        current_time = time.time()
        if (self.currency_pairs_cache and 
            current_time - self.currency_pairs_last_update < self.currency_pairs_cache_ttl):
            if currency_pair:
                return self.currency_pairs_cache.get(currency_pair)
            return self.currency_pairs_cache
        
        try:
            # Tüm currency pair bilgilerini çek
            response = await self._send_request('GET', '/spot/currency_pairs')
            
            if isinstance(response, list):
                self.currency_pairs_cache = {}
                for pair_info in response:
                    pair_name = pair_info.get('id', '')
                    if pair_name:
                        self.currency_pairs_cache[pair_name] = pair_info
                self.currency_pairs_last_update = current_time
                
                if currency_pair:
                    return self.currency_pairs_cache.get(currency_pair)
                return self.currency_pairs_cache
            
            return None
        except Exception as e:
            logger.error(f"Error getting currency pair info: {str(e)}")
            return None
    
    def calculate_step_size_from_precision(self, amount_precision: int) -> float:
        """Precision'dan step size hesapla"""
        return 10 ** (-amount_precision)

    def _generate_order_text(self, tag: str = "pm-sell") -> str:
        """Gate.io order metni 't-' ile başlamalı."""
        return f"t-{tag}-{int(time.time() * 1000)}-{random.randint(100, 999)}"

    def format_quantity_for_gate(self, quantity: float, pair_info: dict) -> str:
        """Gate.io için miktarı formatla - Araştırmadan öğrenilen yöntemle"""
        if not pair_info:
            return f"{quantity:.8f}".rstrip('0').rstrip('.')
        
        # Gate.io'da min_base_amount ve amount_precision kullanılır
        min_amount = float(pair_info.get('min_base_amount', '0'))
        amount_precision = int(pair_info.get('amount_precision', 8))
        
        # Step size'ı precision'dan hesapla
        step_size = self.calculate_step_size_from_precision(amount_precision)
        
        # Minimum kontrolü
        if quantity < min_amount:
            logger.warning(f"Quantity {quantity} is less than min_amount {min_amount}, using min_amount")
            quantity = min_amount
        
        # Step size'a göre truncate et (round değil!)
        quantity_dec = Decimal(str(quantity))
        step_size_dec = Decimal(str(step_size))
        
        # Truncate işlemi
        truncated = quantity_dec - (quantity_dec % step_size_dec)
        
        # Precision'a göre formatla
        formatted = truncated.quantize(
            Decimal(f"0.{'0' * amount_precision}"),
            rounding=ROUND_DOWN
        )
        
        result = str(formatted).rstrip('0').rstrip('.')
        
        # Son kontrol - minimum'un altına düşmedik mi?
        if float(result) < min_amount:
            # Minimum'u kullan ve onu da formatla
            min_dec = Decimal(str(min_amount))
            min_formatted = min_dec.quantize(
                Decimal(f"0.{'0' * amount_precision}"),
                rounding=ROUND_DOWN
            )
            result = str(min_formatted).rstrip('0').rstrip('.')
        
        return result
    
    async def get_price(self, market_pair: str) -> float:
        """Bir market çifti için mevcut fiyatı al"""
        if self.test_mode:
            return 1.0  # Test modunda sabit fiyat
            
        try:
            response = await self._send_request('GET', '/spot/tickers', {'currency_pair': market_pair})
            
            if isinstance(response, list) and len(response) > 0 and 'last' in response[0]:
                return float(response[0]['last'])
            
            logger.error(f"Failed to get price for {market_pair}: {response}")
            return 0
        except Exception as e:
            logger.error(f"Error in get_price for {market_pair}: {str(e)}")
            return 0
    
    async def place_market_sell(self, market_pair: str, amount: float) -> dict:
        """Market satış emri ver - Gate.io kurallarına uygun"""
        if self.test_mode:
            logger.info(f"TEST MODE: Simulating market sell for {market_pair}, amount: {amount}")
            return {"id": "test_order_123", "status": "filled"}
            
        try:
            logger.info(f"🟢 GATE.IO place_market_sell called for {market_pair}, amount={amount}")
            
            # Currency pair bilgilerini al
            pair_info = await self.get_currency_pair_info(market_pair)
            if pair_info:
                logger.debug(f"🟢 GATE.IO pair info for {market_pair}: {json.dumps(pair_info, indent=2)}")
                
                # Miktarı Gate.io kurallarına göre formatla
                amount_str = self.format_quantity_for_gate(amount, pair_info)
                logger.debug(
                    f"🟢 GATE.IO Formatted {market_pair} amount: {amount} -> {amount_str} "
                    f"(min: {pair_info.get('min_base_amount', 'N/A')}, "
                    f"precision: {pair_info.get('amount_precision', 'N/A')})"
                )
                
                # MIN_NOTIONAL kontrolü - Gate.io için kritik!
                min_quote_amount = float(pair_info.get('min_quote_amount', '0'))
                if min_quote_amount > 0:
                    # Güncel fiyatı al
                    current_price = await self.get_price(market_pair)
                    if current_price > 0:
                        order_value = float(amount_str) * current_price
                        logger.debug(f"🟢 GATE.IO Order value: {order_value:.2f} USDT (min required: {min_quote_amount} USDT)")
                        
                        if order_value < min_quote_amount:
                            # Minimum değeri karşılayacak miktarı hesapla
                            required_amount = min_quote_amount / current_price * 1.05  # %5 fazla güvenlik payı
                            required_amount_str = self.format_quantity_for_gate(required_amount, pair_info)
                            
                            logger.warning(f"🟡 GATE.IO Order value {order_value:.2f} USDT is below minimum {min_quote_amount} USDT")
                            logger.warning(f"🟡 GATE.IO Adjusting amount from {amount_str} to {required_amount_str}")
                            
                            # Bakiye kontrolü
                            base_currency = market_pair.split('_')[0]
                            balance = await self.get_balance(base_currency)
                            
                            if balance >= float(required_amount_str):
                                amount_str = required_amount_str
                                order_value = float(amount_str) * current_price
                                logger.debug(f"🟢 GATE.IO New order value: {order_value:.2f} USDT")
                            else:
                                # Bakiye yetmiyorsa tüm bakiyeyi sat
                                balance_str = self.format_quantity_for_gate(balance, pair_info)
                                balance_value = balance * current_price
                                
                                if balance_value >= min_quote_amount:
                                    amount_str = balance_str
                                    logger.debug(f"🟢 GATE.IO Using full balance: {amount_str} (value: {balance_value:.2f} USDT)")
                                else:
                                    logger.error(f"🔴 GATE.IO Insufficient balance. Have {balance} {base_currency} "
                                            f"(value: {balance_value:.2f} USDT), need at least {min_quote_amount} USDT")
                                    return {"error": f"Insufficient balance to meet minimum order value of {min_quote_amount} USDT"}
            else:
                logger.warning(f"🟡 GATE.IO Currency pair info not found for {market_pair}, using default formatting")
                amount_str = f"{amount:.8f}".rstrip('0').rstrip('.')
            
            # Son kontrol
            try:
                amount_float = float(amount_str)
                if amount_float <= 0:
                    logger.error(f"🔴 GATE.IO Amount too small after formatting: {amount} -> {amount_str}")
                    return {"error": "Amount too small after formatting"}
            except (TypeError, ValueError):
                logger.error(f"🔴 GATE.IO Invalid amount format: {amount_str}")
                return {"error": "Invalid amount format"}
            
            order_template = {
                "currency_pair": market_pair,
                "side": "sell",
                "type": "market",
                "time_in_force": "ioc",
            }
            base_currency = market_pair.split("_")[0].upper()
            baseline_snapshot: Optional[BalanceSnapshot] = None
            try:
                baseline_snapshot = await self.get_balance_snapshot(base_currency, use_cache=False)
            except GateAPITemporaryError:
                baseline_snapshot = None

            last_error: Optional[str] = None

            for attempt in range(1, self.max_order_attempts + 1):
                payload = dict(order_template)
                payload["amount"] = amount_str
                payload["text"] = self._generate_order_text("pm-sell")

                logger.debug(f"🟢 GATE.IO place_market_sell data: {json.dumps(payload, indent=2)}")
                logger.debug(
                    f"🟢 GATE.IO Sending sell request at "
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} attempt {attempt}/{self.max_order_attempts}"
                )

                try:
                    response = await self._send_request('POST', '/spot/orders', data=payload)
                except GateAPITemporaryError as exc:
                    last_error = str(exc)
                    logger.warning(
                        f"Gate.io sell attempt {attempt}/{self.max_order_attempts} temporary failure "
                        f"for {market_pair}: {exc}"
                    )
                    if attempt < self.max_order_attempts:
                        await asyncio.sleep(self._gate_order_retry_delay(attempt))
                    continue
                except GateAPIHardError as exc:
                    last_error = str(exc)
                    logger.error(
                        f"Gate.io sell attempt {attempt}/{self.max_order_attempts} hard error "
                        f"for {market_pair}: {exc}"
                    )
                    break

                logger.debug(f"🟢 GATE.IO Response received: {json.dumps(response, indent=2)}")

                if 'id' in response:
                    logger.info(
                        f"✅ Gate.io market sell order placed successfully: {response['id']} "
                        f"attempt {attempt}/{self.max_order_attempts}"
                    )
                    return response

                last_error = json.dumps(response)
                logger.error(f"❌ Failed to place Gate.io market sell order: {response}")

                if (
                    isinstance(response, dict)
                    and response.get('label') == 'INVALID_AMOUNT'
                    and pair_info
                ):
                    logger.error(
                        f"🔴 GATE.IO amount details for {market_pair}: "
                        f"min_base_amount={pair_info.get('min_base_amount')}, "
                        f"amount_precision={pair_info.get('amount_precision')}, "
                        f"Attempted amount={amount_str}"
                    )
                    break

                if attempt < self.max_order_attempts:
                    await asyncio.sleep(self._gate_order_retry_delay(attempt))

            recovered = await self._recover_gate_sell(
                market_pair=market_pair,
                base_currency=base_currency,
                baseline_snapshot=baseline_snapshot,
                last_error=last_error,
            )
            if recovered:
                return recovered

            return {
                "error": last_error or "gate_order_failed",
                "market_pair": market_pair,
            }
        except Exception as e:
            logger.error(f"🔴 Error in GATE.IO place_market_sell for {market_pair}: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"error": str(e)}

    async def get_balance(self, asset: str = "USDT") -> float:
        """Belirli bir varlık için hesap bakiyesini al (yalnızca available)."""
        if self.test_mode:
            return 1000.0

        try:
            snapshot = await self.get_balance_snapshot(asset)
            return float(snapshot.available)
        except GateAPITemporaryError as exc:
            logger.warning(f"Gate.io balance temporarily unavailable for {asset}: {exc}")
            return 0.0
        except GateAPIHardError as exc:
            logger.error(f"Gate.io balance error for {asset}: {exc}")
            return 0.0

    async def _fetch_recent_trade(self, market_pair: str) -> Optional[Dict[str, Any]]:
        params = {"currency_pair": market_pair, "limit": 1}
        try:
            response = await self._send_request('GET', '/spot/my_trades', params=params)
        except GateAPITemporaryError:
            return None
        except GateAPIHardError:
            return None
        if isinstance(response, list) and response:
            return response[0]
        return None

    async def _recover_gate_sell(
        self,
        *,
        market_pair: str,
        base_currency: str,
        baseline_snapshot: Optional[BalanceSnapshot],
        last_error: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        baseline_available = baseline_snapshot.available if baseline_snapshot else None
        current_available: Optional[Decimal] = None
        if baseline_available is not None:
            try:
                current_snapshot = await self.get_balance_snapshot(base_currency, use_cache=False)
                current_available = current_snapshot.available
            except GateAPITemporaryError:
                current_available = None

        delta = None
        if baseline_available is not None and current_available is not None:
            delta = baseline_available - current_available

        if delta is not None and delta >= self.balance_recovery_threshold:
            logger.warning(
                f"Gate.io base balance for {base_currency} decreased by {delta} after failures "
                f"({last_error or 'unknown'}); attempting trade recovery"
            )
            trade_detail = await self._fetch_recent_trade(market_pair)
            recovered_id = None
            if trade_detail:
                recovered_id = trade_detail.get("id")
            if not recovered_id:
                recovered_id = f"recovered-{market_pair}-{int(time.time() * 1000)}"
            return {
                "id": recovered_id,
                "status": "filled",
                "recovered": True,
                "trade": trade_detail,
            }
        return None
    
    # WebSocket metodları aynı kalacak...
    async def start_price_streams(self, symbols: List[str]):
        """Tüm sembollerin fiyat akışını başlat"""
        if not symbols or self.test_mode:
            return
        
        # Mevcut görevleri temizle
        for group_id, task in list(self.ws_connections.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling Gate.io WebSocket task {group_id}: {str(e)}")
        
        self.ws_connections = {}  # Bağlantı koleksiyonunu sıfırla
        
        # Sembolleri 50'şer gruplar halinde böl (Gate.io sınırlaması için)
        symbol_groups = [symbols[i:i+50] for i in range(0, len(symbols), 50)]
        
        # Yeni görevleri başlat
        for group_index, symbol_group in enumerate(symbol_groups):
            # Her grup için ayrı bir WebSocket bağlantısı başlat
            group_key = f"group_{group_index}"
            task = asyncio.create_task(self._start_group_price_stream(symbol_group, group_index))
            self.ws_connections[group_key] = task
            self.active_ws_tasks.add(task)
            # Görev bitince active_ws_tasks'tan çıkarma için callback ekle
            task.add_done_callback(lambda t, gk=group_key: self._task_done_callback(t, gk))
    
    def _task_done_callback(self, task, group_key):
        """WebSocket görevi bittiğinde yapılacak temizlik"""
        if task in self.active_ws_tasks:
            self.active_ws_tasks.remove(task)
        if group_key in self.ws_connections:
            del self.ws_connections[group_key]
    
    async def _start_group_price_stream(self, symbols: List[str], group_index: int):
        """Bir grup sembol için fiyat akışını başlat"""
        if not symbols:
            return
            
        market_pairs = [f"{symbol}_USDT" for symbol in symbols]
        
        logger.info(f"Starting Gate.io price stream for group {group_index} with {len(symbols)} symbols")
        
        retry_count = 0
        max_retries = 10
        ws = None
        
        try:
            while retry_count < max_retries:
                try:
                    ws_kwargs = {
                        "ping_interval": DEFAULT_POSITION_WS_PING_INTERVAL,
                        "ping_timeout": DEFAULT_POSITION_WS_PING_TIMEOUT,
                        "close_timeout": DEFAULT_POSITION_WS_CLOSE_TIMEOUT,
                        "open_timeout": DEFAULT_POSITION_WS_OPEN_TIMEOUT,
                    }
                    if self.local_ip:
                        ws_kwargs["local_addr"] = (self.local_ip, 0)

                    async with websockets.connect(self.ws_url, **ws_kwargs) as ws:
                        logger.info(f"Connected to Gate.io price stream for group {group_index}")
                        
                        # Abone ol
                        for pair in market_pairs:
                            subscribe_message = {
                                "time": int(time.time()),
                                "channel": "spot.tickers",
                                "event": "subscribe",
                                "payload": [pair]
                            }
                            await ws.send(json.dumps(subscribe_message))
                        
                        # Ping görevi başlat
                        ping_task = asyncio.create_task(self._websocket_ping(ws))
                        
                        try:
                            async for message in ws:
                                try:
                                    data = json.loads(message)
                                    if data.get('event') == 'update' and data.get('channel') == 'spot.tickers':
                                        ticker_data = data.get('result', {})
                                        if ticker_data:
                                            pair = ticker_data.get('currency_pair', '')
                                            if '_' in pair:
                                                symbol = pair.split('_')[0]
                                                price = float(ticker_data.get('last', 0))
                                                volume = float(ticker_data.get('base_volume', 0))
                                                
                                                self.last_price_update[symbol] = time.time()
                                                
                                                # Kayıtlı geri çağırmaları bilgilendir
                                                if symbol in self.price_subscribers:
                                                    for callback in self.price_subscribers[symbol]:
                                                        try:
                                                            await callback(symbol, price, volume)
                                                        except Exception as e:
                                                            logger.error(f"Price subscriber callback error: {str(e)}")
                                except json.JSONDecodeError as e:
                                    logger.error(f"Price stream JSON parsing error: {str(e)}")
                                except Exception as e:
                                    logger.error(f"Price stream processing error: {str(e)}")
                        finally:
                            if ping_task and not ping_task.done():
                                ping_task.cancel()
                                try:
                                    await ping_task
                                except asyncio.CancelledError:
                                    pass
                        
                        # Normal çıkış - yeniden dene
                        logger.warning(f"Gate.io price stream for group {group_index} closed. Reconnecting...")
                        await asyncio.sleep(1)
                        retry_count += 1
                            
                except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
                    logger.warning(f"Gate.io price stream for group {group_index} disconnected: {str(e)}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Gate.io price stream error for group {group_index}: {str(e)}. Reconnecting in 10s...")
                    await asyncio.sleep(10)
                    retry_count += 1
                except asyncio.CancelledError:
                    logger.info(f"Gate.io price stream task for group {group_index} cancelled")
                    raise  # Re-raise to properly exit
            
            logger.critical(f"Failed to maintain Gate.io price stream for group {group_index} after {max_retries} retries")
        
        finally:
            logger.info(f"WebSocket task for group {group_index} exiting")
            # Bağlantıyı kapattığımızdan emin ol
            if ws and not ws.closed:
                try:
                    await ws.close()
                except Exception:
                    pass
    
    async def _websocket_ping(self, ws):
        """WebSocket bağlantısını canlı tutmak için düzenli ping gönder"""
        try:
            while True:
                await asyncio.sleep(30)
                ping_message = {"time": int(time.time()), "channel": "spot.ping", "event": "ping"}
                await ws.send(json.dumps(ping_message))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"WebSocket ping error: {str(e)}")
    
    def subscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerine abone ol"""
        if symbol not in self.price_subscribers:
            self.price_subscribers[symbol] = []
        self.price_subscribers[symbol].append(callback)
    
    def unsubscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerinden aboneliği kaldır"""
        if symbol in self.price_subscribers and callback in self.price_subscribers[symbol]:
            self.price_subscribers[symbol].remove(callback)


class MEXCAPI:
    """MEXC API etkileşimleri"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        session: aiohttp.ClientSession,
        test_mode: bool = False,
        local_ip: Optional[str] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.mexc.com"
        self.session = session
        self.local_ip = (local_ip or "").strip() or None
        self.ws_url = "wss://wbs-api.mexc.com/ws"
        self.price_subscribers = {}  # {symbol: [callback1, callback2, ...]}
        self.last_price_update = {}  # {symbol: timestamp}
        self.ws_connections = {}  # {symbol_group: websocket_task}
        self.active_ws_tasks = set()  # Aktif WebSocket görevlerini izle
        self.test_mode = test_mode

        # HEADERS EKLE
        self.headers = {
            'X-MEXC-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        }
        
        # Exchange info cache - MEXC de Binance benzeri exchange info kullanıyor
        self.exchange_info_cache = {}
        self.exchange_info_last_update = 0
        self.exchange_info_cache_ttl = 3600  # 1 saat
        self.balance_cache_ttl = float(os.getenv("MEXC_BALANCE_CACHE_TTL", "30"))
        self._balance_cache: Dict[str, Tuple[float, float]] = {}
        
    async def _generate_signature(self, params: dict) -> str:
        """MEXC API isteği için imza oluştur"""
        # Parametreleri sırala - trader.py ile aynı yöntem
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        
        # İmza oluştur
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    async def _send_request(self, method: str, endpoint: str, params: dict = None, 
                        signed: bool = False) -> dict:
        """MEXC API'ye istek gönder - Sessiz mod"""
        if self.test_mode:
            if endpoint == '/api/v3/time':
                return {"serverTime": int(time.time() * 1000)}
            if endpoint == '/api/v3/ticker/price':
                return {"price": "1.0"}
            if endpoint == '/api/v3/account':
                requested_asset = (params or {}).get("asset", "USDT")
                return {
                    "balances": [
                        {"asset": str(requested_asset).upper(), "free": "1000.0"},
                        {"asset": "USDT", "free": "1000.0"},
                    ]
                }
            if endpoint == '/api/v3/order':
                return {"orderId": "test_order_123", "status": "FILLED"}
            if endpoint == '/api/v3/exchangeInfo':
                return self._get_test_exchange_info((params or {}).get("symbol"))
            return {"success": True}
            
        if params is None:
            params = {}
            
        url = f"{self.base_url}{endpoint}"
        headers = {
            'X-MEXC-APIKEY': self.api_key,
            'Content-Type': 'application/json'  # BU SATIR KRİTİK!
        }
        
        try:
            if signed:
                # Sunucu zamanını al
                try:
                    time_response = await self._send_request('GET', '/api/v3/time')
                    server_time = time_response.get('serverTime', int(time.time() * 1000))
                except Exception:
                    server_time = int((time.time() - 1) * 1000)
                
                params['timestamp'] = str(server_time)
                params['recvWindow'] = '60000'
                
                # İmza oluştur
                query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
                signature = await self._generate_signature(params)
                
                # DEBUG loglarını kaldır
                logger.debug(f"MEXC API {method} {endpoint}")
                
                # POST/DELETE için URL'de parametreler
                if method in ['POST', 'DELETE']:
                    url = f"{url}?{query_string}&signature={signature}"
                    params = None  # Body'de parametre gönderme
                else:
                    params['signature'] = signature
            
            # Request gönder
            if method == 'GET':
                async with self.session.get(url, params=params, headers=headers) as response:
                    response_text = await response.text()
                    
                    try:
                        response_data = json.loads(response_text)
                    except json.JSONDecodeError:
                        response_data = {
                            "error": "Invalid JSON response",
                            "raw": response_text[:500],
                        }
                        
                    if response.status != 200:
                        logger.error(f"MEXC API error: {response.status} - {response_data}")
                    return response_data
                    
            elif method == 'POST':
                # POST için parametreler URL'de
                async with self.session.post(url, headers=headers) as response:
                    response_text = await response.text()
                    
                    try:
                        response_data = json.loads(response_text)
                    except json.JSONDecodeError:
                        response_data = {
                            "error": "Invalid JSON response",
                            "raw": response_text[:500],
                        }
                        
                    if response.status != 200:
                        logger.error(f"MEXC API error: {response.status} - {response_data}")
                    else:
                        # Başarılı order'ları logla
                        if endpoint == '/api/v3/order':
                            logger.info(f"✅ MEXC order success: {response_data.get('orderId')}")
                    return response_data
                    
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Connection error to MEXC API: {str(e)}")
            return {"error": f"Connection error: {str(e)}"}
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to MEXC API")
            return {"error": "Request timeout"}
        except Exception as e:
            logger.error(f"MEXC API request error: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"error": str(e)}

    def _get_test_exchange_info(self, symbol=None):
        """Test modu için exchange info"""
        test_info = {
            "symbols": [{
                "symbol": symbol or "BTCUSDT",
                "status": "ENABLED",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "baseSizePrecision": "0.000001",
                "quotePrecision": 2,
                "filters": [{
                    "filterType": "LOT_SIZE",
                    "minQty": "0.000001",
                    "maxQty": "9000.00000000",
                    "stepSize": "0.000001"
                }]
            }]
        }
        return test_info
    
    async def get_exchange_info(self, symbol: str = None):
        """Exchange info'yu al (LOT_SIZE kuralları için)"""
        current_time = time.time()
        if (self.exchange_info_cache and 
            current_time - self.exchange_info_last_update < self.exchange_info_cache_ttl):
            if symbol:
                return self.exchange_info_cache.get(symbol)
            return self.exchange_info_cache
        
        try:
            params = {}
            if symbol:
                params['symbol'] = symbol
                
            response = await self._send_request('GET', '/api/v3/exchangeInfo', params)
            
            if 'symbols' in response:
                self.exchange_info_cache = {}
                for symbol_info in response['symbols']:
                    symbol_name = symbol_info['symbol']
                    self.exchange_info_cache[symbol_name] = symbol_info
                self.exchange_info_last_update = current_time
                
                if symbol:
                    return self.exchange_info_cache.get(symbol)
                return self.exchange_info_cache
            
            return None
        except Exception as e:
            logger.error(f"Error getting MEXC exchange info: {str(e)}")
            return None
    
    def parse_mexc_precision(self, precision_str):
        """MEXC'in özel precision formatını parse et"""
        if isinstance(precision_str, str) and '.' in precision_str:
            # "0.000001" -> 0.000001 step size
            return float(precision_str)
        elif isinstance(precision_str, (int, float)):
            # 6 -> 0.000001 step size
            return 10 ** (-int(precision_str))
        return 0.000001  # Varsayılan
    
    def get_lot_size_filter(self, symbol_info: dict):
        """Symbol info'dan LOT_SIZE filtresini al - MEXC için özel"""
        if not symbol_info:
            return None
            
        # Önce normal filters içinde ara
        if 'filters' in symbol_info:
            for filter_item in symbol_info['filters']:
                if filter_item.get('filterType') == 'LOT_SIZE':
                    return {
                        'minQty': float(filter_item.get('minQty', '0.000001')),
                        'maxQty': float(filter_item.get('maxQty', '9000')),
                        'stepSize': float(filter_item.get('stepSize', '0.000001'))
                    }
        
        # MEXC'in alternatif formatı - baseSizePrecision kullan
        if 'baseSizePrecision' in symbol_info:
            step_size = self.parse_mexc_precision(symbol_info['baseSizePrecision'])
            return {
                'minQty': step_size,  # Genelde step size minimum'dur
                'maxQty': 9000.0,  # Varsayılan maksimum
                'stepSize': step_size
            }
            
        return None
    
    def round_step_size_mexc(self, quantity: float, step_size: float) -> str:
        """MEXC için quantity'yi step size'a göre yuvarla"""
        if step_size == 1.0:
            # Integer only
            return str(int(math.floor(quantity)))
        elif step_size < 1.0:
            # Decimal kullan - MEXC için özel handling
            quantity_dec = Decimal(str(quantity))
            step_size_dec = Decimal(str(step_size))
            
            # TRUNCATE işlemi
            result = quantity_dec - (quantity_dec % step_size_dec)
            
            # MEXC'in beklediği formata çevir
            result_str = str(result)
            if '.' in result_str:
                # Step size'ın decimal basamak sayısını bul
                step_str = f"{step_size:.10f}".rstrip('0')
                if '.' in step_str:
                    decimals = len(step_str.split('.')[1])
                    # O kadar basamakla sınırla
                    parts = result_str.split('.')
                    if len(parts[1]) > decimals:
                        result_str = f"{parts[0]}.{parts[1][:decimals]}"
                
                # Sondaki sıfırları temizle
                result_str = result_str.rstrip('0').rstrip('.')
            
            return result_str
        else:
            # step_size > 1.0
            steps = math.floor(quantity / step_size)
            result = steps * step_size
            return str(int(result)) if result == int(result) else str(result)
    
    def format_quantity_with_lot_size(self, quantity: float, lot_size_filter: dict) -> str:
        """Miktarı LOT_SIZE kurallarına göre formatla - MEXC için"""
        if not lot_size_filter:
            return self.format_quantity(quantity, 6)
        
        min_qty = lot_size_filter['minQty']
        max_qty = lot_size_filter['maxQty']
        step_size = lot_size_filter['stepSize']
        
        # Sıfır veya negatif kontrol
        if quantity <= 0:
            logger.error(f"Invalid quantity: {quantity}")
            return None
        
        # Maximum kontrolü
        if quantity > max_qty:
            logger.warning(f"Quantity {quantity} exceeds maxQty {max_qty}, using maxQty")
            quantity = max_qty
        
        # Step size'a göre formatla
        formatted = self.round_step_size_mexc(quantity, step_size)
        formatted_float = float(formatted)
        
        # Minimum kontrolü
        if formatted_float < min_qty:
            # Minimum'u kullan ve onu da formatla
            min_formatted = self.round_step_size_mexc(min_qty, step_size)
            logger.warning(f"Quantity {formatted} is less than minQty {min_qty}, using {min_formatted}")
            return min_formatted
        
        return formatted
    
    def format_quantity(self, quantity: float, precision: int = 6) -> str:
        """Miktarı MEXC kurallarına göre formatla (eski method)"""
        try:
            numeric_quantity = float(quantity)
        except (TypeError, ValueError) as exc:
            logger.error(f"Invalid MEXC quantity value: {quantity!r} ({exc})")
            return "0"

        if numeric_quantity <= 0 or math.isnan(numeric_quantity) or math.isinf(numeric_quantity):
            logger.error(f"Invalid MEXC quantity numeric value: {numeric_quantity}")
            return "0"

        try:
            decimal_quantity = Decimal(str(numeric_quantity))
            formatted = decimal_quantity.quantize(
                Decimal(f"0.{'0' * precision}"),
                rounding=ROUND_DOWN
            )
            
            result = str(formatted).rstrip('0').rstrip('.')
            
            if 'e' in result.lower():
                return f"{numeric_quantity:.{precision}f}".rstrip('0').rstrip('.')
            
            return result
        except (ArithmeticError, ValueError) as exc:
            logger.error(
                f"Failed to format MEXC quantity {numeric_quantity} with precision {precision}: {exc}"
            )
            return f"{numeric_quantity:.{precision}f}".rstrip('0').rstrip('.')
    
    async def get_price(self, symbol: str) -> float:
        """Bir sembol için mevcut fiyatı al"""
        if self.test_mode:
            return 1.0  # Test modunda sabit fiyat
            
        try:
            response = await self._send_request('GET', '/api/v3/ticker/price', {'symbol': symbol})
            if 'price' in response:
                return float(response['price'])
            logger.error(f"Failed to get price for {symbol}: {response}")
            return 0
        except Exception as e:
            logger.error(f"Error in get_price for {symbol}: {str(e)}")
            return 0
    
    async def place_market_sell(self, symbol: str, quantity: float) -> dict:
        """MEXC Market satış emri ver - LOT_SIZE desteği ile"""
        if self.test_mode:
            logger.info(f"TEST MODE: Simulating market sell for {symbol}, quantity: {quantity}")
            return {"orderId": "test_order_123", "status": "FILLED"}
            
        try:
            logger.info(f"🔵 MEXC place_market_sell called for {symbol}, quantity={quantity}")
            
            # Exchange info'yu al
            symbol_info = await self.get_exchange_info(symbol)
            if symbol_info:
                logger.debug(f"🔵 MEXC symbol info for {symbol}: {json.dumps(symbol_info, indent=2)}")
                # LOT_SIZE filtresini al
                lot_size_filter = self.get_lot_size_filter(symbol_info)
                if lot_size_filter:
                    logger.debug(f"🔵 MEXC LOT_SIZE filter: {lot_size_filter}")
                    quantity_str = self.format_quantity_with_lot_size(quantity, lot_size_filter)
                    
                    if quantity_str is None:
                        logger.error(f"🔴 MEXC: Cannot format quantity {quantity} for {symbol}")
                        return {"error": f"Cannot format quantity {quantity} for {symbol}"}
                    
                    logger.debug(
                        f"🔵 MEXC Formatted {symbol} quantity: {quantity} -> {quantity_str} "
                        f"(minQty: {lot_size_filter['minQty']}, stepSize: {lot_size_filter['stepSize']})"
                    )
                else:
                    logger.warning(f"🟡 MEXC LOT_SIZE filter not found for {symbol}, using default formatting")
                    quantity_str = self.format_quantity(quantity, 6)
            else:
                logger.warning(f"🟡 MEXC Exchange info not found for {symbol}, using default formatting")
                quantity_str = self.format_quantity(quantity, 6)
            
            # Son kontrol
            try:
                qty_float = float(quantity_str)
                if qty_float <= 0:
                    logger.error(f"🔴 MEXC Quantity too small after formatting: {quantity} -> {quantity_str}")
                    return {"error": "Quantity too small after formatting"}
            except (TypeError, ValueError):
                logger.error(f"🔴 MEXC Invalid quantity format: {quantity_str}")
                return {"error": "Invalid quantity format"}
            
            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': quantity_str
            }
            
            logger.info(f"🔴 MEXC MARKET SELL ORDER: {symbol} quantity={quantity_str}")
            logger.debug(f"🔵 MEXC order params: {params}")
            
            # İstek öncesi timestamp log
            logger.debug(f"🔵 MEXC Sending sell request at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
            
            response = await self._send_request('POST', '/api/v3/order', params, signed=True)
            
            # Response detaylı log
            logger.debug(f"🔵 MEXC Response received: {json.dumps(response, indent=2)}")
            
            if 'orderId' in response:
                logger.info(f"✅ MEXC market sell order placed successfully: {response['orderId']}")
            else:
                logger.error(f"❌ Failed to place MEXC market sell order: {response}")
                
                # LOT_SIZE hatası ise detaylı bilgi ver
                if 'msg' in response and 'LOT_SIZE' in response['msg']:
                    if symbol_info and lot_size_filter:
                        logger.error(f"🔴 MEXC LOT_SIZE details for {symbol}: "
                                f"minQty={lot_size_filter['minQty']}, "
                                f"maxQty={lot_size_filter['maxQty']}, "
                                f"stepSize={lot_size_filter['stepSize']}, "
                                f"Attempted quantity={quantity_str}")
            
            return response
        except Exception as e:
            logger.error(f"🔴 Error in MEXC place_market_sell for {symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"error": str(e)}
    
    async def get_balance(self, asset: str = "USDT", use_cache: bool = True) -> float:
        """Belirli bir varlık için hesap bakiyesini al"""
        if self.test_mode:
            return 1000.0  # Test modunda sabit bakiye
            
        try:
            # Server time al
            try:
                time_response = await self._send_request('GET', '/api/v3/time')
                server_time = time_response.get('serverTime', int(time.time() * 1000))
            except Exception:
                server_time = int((time.time() - 1) * 1000)
            
            # Parametreleri hazırla
            params = {
                'timestamp': str(server_time),
                'recvWindow': '60000'
            }
            
            # İmza oluştur
            signature = await self._generate_signature(params)
            query_string = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
            asset_key = (asset or "").upper()
            now = time.time()

            if use_cache:
                cached = self._balance_cache.get(asset_key)
                if cached and (now - cached[1]) <= self.balance_cache_ttl:
                    return cached[0]
            
            # URL oluştur (trader.py ile aynı yöntem)
            url = f"{self.base_url}/api/v3/account?{query_string}&signature={signature}"
            
            # İsteği gönder
            async with self.session.get(url, headers=self.headers) as response:
                response_data = await response.json()
                
                if response.status == 200 and 'balances' in response_data:
                    timestamp = time.time()
                    result = 0.0
                    for balance in response_data['balances']:
                        asset_name = (balance.get('asset') or "").upper()
                        if not asset_name:
                            continue
                        try:
                            free_amount = float(balance.get('free', 0))
                        except (TypeError, ValueError):
                            free_amount = 0.0
                        self._balance_cache[asset_name] = (free_amount, timestamp)
                        if asset_name == asset_key:
                            result = free_amount

                    if asset_key not in self._balance_cache and asset_key:
                        self._balance_cache[asset_key] = (0.0, timestamp)
                        logger.info(f"MEXC: {asset_key} balance not found (likely not listed yet)")
                    return result
                else:
                    if isinstance(response_data, dict) and response.status == 429:
                        cached = self._balance_cache.get(asset_key)
                        if cached:
                            logger.warning("MEXC rate limit; using cached balance for %s", asset_key)
                            return cached[0]
                    logger.error(f"MEXC bakiye hatası: {response.status} - {response_data}")
                    return 0
                    
        except Exception as e:
            logger.error(f"Error in get_balance for {asset}: {str(e)}")
            return 0
        
    # WebSocket metodları aynı kalacak...
    async def start_price_streams(self, symbols: List[str]):
        """Tüm sembollerin fiyat akışını başlat"""
        if not symbols or self.test_mode:
            return
        
        # Mevcut görevleri temizle
        for group_id, task in list(self.ws_connections.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling MEXC WebSocket task {group_id}: {str(e)}")
        
        self.ws_connections = {}  # Bağlantı koleksiyonunu sıfırla
        
        # MEXC limitine göre sembolleri 30'luk gruplara böl
        symbol_groups = [symbols[i:i+30] for i in range(0, len(symbols), 30)]
        
        # Yeni görevleri başlat
        for group_index, symbol_group in enumerate(symbol_groups):
            # Her grup için ayrı bir WebSocket bağlantısı başlat
            group_key = f"group_{group_index}"
            task = asyncio.create_task(self._start_group_price_stream(symbol_group, group_index))
            self.ws_connections[group_key] = task
            self.active_ws_tasks.add(task)
            # Görev bitince active_ws_tasks'tan çıkarma için callback ekle
            task.add_done_callback(lambda t, gk=group_key: self._task_done_callback(t, gk))
    
    def _task_done_callback(self, task, group_key):
        """WebSocket görevi bittiğinde yapılacak temizlik"""
        if task in self.active_ws_tasks:
            self.active_ws_tasks.remove(task)
        if group_key in self.ws_connections:
            del self.ws_connections[group_key]
    
    async def _start_group_price_stream(self, symbols: List[str], group_index: int):
        """MEXC için bir grup sembol için fiyat akışını başlat"""
        if not symbols:
            return
            
        market_pairs = [f"{symbol}USDT" for symbol in symbols]
        
        logger.info(f"Starting MEXC price stream for group {group_index} with {len(symbols)} symbols: {symbols}")
        
        retry_count = 0
        max_retries = 10
        refused_streak = 0
        circuit_open_until = 0.0
        failure_timestamps: deque[float] = deque(maxlen=12)
        circuit_breaker_cooldown = 60.0
        jitter_range = (0.0, 1.5)

        while True:
            if retry_count >= max_retries:
                logger.critical(f"Failed to maintain MEXC stream after {max_retries} retries")
                break
            
            now_ts = time.time()
            if circuit_open_until and now_ts < circuit_open_until:
                sleep_remaining = circuit_open_until - now_ts
                logger.warning(
                    f"MEXC circuit breaker active for another {sleep_remaining:.1f}s; "
                    "skipping reconnect attempt"
                )
                await asyncio.sleep(min(sleep_remaining, 30))
                continue

            ws = None
            ping_task = None
            connected_since = None
            subscription_confirmed = False
            
            try:
                ws_url = self.ws_url
                logger.info(f"Connecting to MEXC WebSocket: {ws_url}")
                
                ws_kwargs = {
                    "ping_interval": DEFAULT_POSITION_WS_PING_INTERVAL,
                    "ping_timeout": DEFAULT_POSITION_WS_PING_TIMEOUT,
                    "close_timeout": DEFAULT_POSITION_WS_CLOSE_TIMEOUT,
                    "open_timeout": DEFAULT_POSITION_WS_OPEN_TIMEOUT,
                }
                if self.local_ip:
                    ws_kwargs["local_addr"] = (self.local_ip, 0)

                async with websockets.connect(ws_url, **ws_kwargs) as ws:
                    logger.info(f"Connected to MEXC price stream for group {group_index}")
                    retry_count = 0  # Sağlam bağlantıda sayaç sıfırlansın
                    connected_since = time.time()
                    refused_streak = 0
                    failure_timestamps.clear()
                    circuit_open_until = 0.0
                    
                    subscribe_msg = {
                        "method": "SUBSCRIPTION",
                        "params": [f"spot@public.miniTicker.v3.api@{pair}" for pair in market_pairs],
                    }
                    
                    logger.info(f"MEXC Subscribe message: {subscribe_msg}")
                    await ws.send(json.dumps(subscribe_msg))
                    
                    ping_task = asyncio.create_task(self._websocket_ping(ws))
                    ack_deadline = time.time() + 5
                    
                    try:
                        async for message in ws:
                            try:
                                data = self._decode_mexc_ws_message(message)
                                if data is None:
                                    continue
                            except json.JSONDecodeError as exc:
                                logger.error(f"MEXC JSON parsing error: {exc}")
                                continue
                            except Exception as exc:
                                logger.error(f"MEXC processing error: {exc}")
                                logger.debug(traceback.format_exc())
                                continue
                            
                            if isinstance(data, dict):
                                if 'd' in data:
                                    ticker_data = data['d']
                                    if isinstance(ticker_data, list) and ticker_data:
                                        ticker_data = ticker_data[0]
                                    
                                    if isinstance(ticker_data, dict):
                                        symbol_pair = ticker_data.get('s') or ticker_data.get('S')
                                        if symbol_pair and symbol_pair.endswith('USDT'):
                                            symbol = symbol_pair[:-4]
                                            last_price = 0.0
                                            if 'p' in ticker_data:
                                                last_price = float(ticker_data['p'])
                                            elif 'c' in ticker_data:
                                                last_price = float(ticker_data['c'])
                                            
                                            volume = float(ticker_data.get('v', ticker_data.get('q', 0) or 0))
                                            
                                            if last_price > 0:
                                                logger.debug(f"📈 MEXC {symbol}: ${last_price:.8f} (vol: {volume:.2f})")
                                                self.last_price_update[symbol] = time.time()
                                                
                                                if symbol in self.price_subscribers:
                                                    for callback in self.price_subscribers[symbol]:
                                                        try:
                                                            await callback(symbol, last_price, volume)
                                                        except Exception as exc:
                                                            logger.error(f"Price callback error: {exc}")
                                elif 'c' in data and isinstance(data.get('c'), str) and 'd' in data:
                                    ticker_data = data['d']
                                    symbol_pair = ticker_data.get('s') if isinstance(ticker_data, dict) else None
                                    if symbol_pair and symbol_pair.endswith('USDT'):
                                        symbol = symbol_pair[:-4]
                                        last_price = float(ticker_data.get('c', 0) or 0)
                                        volume = float(ticker_data.get('v', 0) or 0)
                                        if last_price > 0:
                                            logger.debug(f"📈 MEXC {symbol}: ${last_price:.8f}")
                                            self.last_price_update[symbol] = time.time()
                                            if symbol in self.price_subscribers:
                                                for callback in self.price_subscribers[symbol]:
                                                    try:
                                                        await callback(symbol, last_price, volume)
                                                    except Exception as exc:
                                                        logger.error(f"Price callback error: {exc}")
                                elif 'code' in data:
                                    if data['code'] == 0:
                                        logger.info("MEXC subscription confirmed")
                                        subscription_confirmed = True
                                    else:
                                        logger.error(f"MEXC subscription error: {data}")
                                elif 'ping' in data:
                                    pong_msg = {"pong": data['ping']}
                                    await ws.send(json.dumps(pong_msg))
                                    logger.debug("Sent pong to MEXC")
                                elif data.get('method', '').upper() == 'PING':
                                    pong_msg = {"method": "PONG", "id": data.get('id', int(time.time() * 1000))}
                                    await ws.send(json.dumps(pong_msg))
                                    logger.debug("Responded to MEXC method ping")
                                else:
                                    logger.debug(f"MEXC unknown format: {data}")
                            else:
                                logger.debug(f"MEXC non-dict message: {data}")

                            if not subscription_confirmed and time.time() > ack_deadline:
                                logger.warning(
                                    "MEXC subscription ACK not received within 5s; monitoring stream cautiously"
                                )
                                subscription_confirmed = True  # avoid repeated warnings
                    finally:
                        if ping_task and not ping_task.done():
                            ping_task.cancel()
                            try:
                                await ping_task
                            except asyncio.CancelledError:
                                pass
                        if ws:
                            logger.debug("WebSocket connection context exited")
                        ping_task = None
                        ws = None
                
                logger.warning(f"MEXC price stream for group {group_index} closed. Reconnecting...")
                uptime = (time.time() - connected_since) if connected_since else 0
                if uptime >= 60:
                    retry_count = 0
                else:
                    retry_count += 1
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info("MEXC task cancelled")
                raise
            except (websockets.ConnectionClosed, ConnectionRefusedError) as exc:
                long_lived = connected_since is not None and (time.time() - connected_since) >= 60
                failure_timestamps.append(time.time())
                if long_lived:
                    retry_count = 0
                    delay = 1
                    attempt = 1
                else:
                    retry_count += 1
                    attempt = retry_count
                    delay = min(30, 2 ** min(retry_count, 5))
                if isinstance(exc, ConnectionRefusedError):
                    refused_streak += 1
                    if refused_streak >= 5 and not circuit_open_until:
                        circuit_open_until = time.time() + circuit_breaker_cooldown
                        retry_count = 0
                        logger.critical(
                            f"MEXC WebSocket refused {refused_streak} times consecutively; "
                            f"opening circuit for {circuit_breaker_cooldown:.0f}s"
                        )
                else:
                    refused_streak = 0

                if (
                    len(failure_timestamps) == failure_timestamps.maxlen
                    and failure_timestamps[-1] - failure_timestamps[0] <= 120
                    and not circuit_open_until
                ):
                    circuit_open_until = time.time() + circuit_breaker_cooldown
                    retry_count = 0
                    logger.critical(
                        "MEXC WebSocket experienced rapid consecutive failures; "
                        f"opening circuit for {circuit_breaker_cooldown:.0f}s"
                    )
                logger.warning(
                    f"MEXC disconnected: {exc}. Reconnecting in {delay:.1f}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                await asyncio.sleep(delay + random.uniform(*jitter_range))
            except Exception as exc:
                long_lived = connected_since is not None and (time.time() - connected_since) >= 60
                failure_timestamps.append(time.time())
                if long_lived:
                    retry_count = 0
                    delay = 1
                    attempt = 1
                else:
                    retry_count += 1
                    attempt = retry_count
                    delay = min(30, 2 ** min(retry_count + 1, 5))
                refused_streak = 0

                if (
                    len(failure_timestamps) == failure_timestamps.maxlen
                    and failure_timestamps[-1] - failure_timestamps[0] <= 120
                    and not circuit_open_until
                ):
                    circuit_open_until = time.time() + circuit_breaker_cooldown
                    retry_count = 0
                    logger.critical(
                        "MEXC WebSocket experienced rapid consecutive errors; "
                        f"opening circuit for {circuit_breaker_cooldown:.0f}s"
                    )
                logger.error(
                    f"MEXC error: {exc}. Reconnecting in {delay:.1f}s (attempt {attempt}/{max_retries})"
                )
                logger.debug(traceback.format_exc())
                await asyncio.sleep(delay + random.uniform(*jitter_range))
            finally:
                if ping_task and not ping_task.done():
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
                if ws:
                    logger.debug("WebSocket connection context exited")
        
        logger.info(f"MEXC WebSocket task exiting")
    
    def _decode_mexc_ws_message(self, message):
        """Decode incoming WebSocket payload, supporting future protobuf frames."""
        if isinstance(message, dict):
            return message

        if isinstance(message, (bytes, bytearray)):
            try:
                text = message.decode("utf-8")
                return json.loads(text)
            except UnicodeDecodeError:
                logger.warning(
                    "Received non-UTF8 binary payload from MEXC; protobuf decoding not implemented yet"
                )
                return None
            except json.JSONDecodeError:
                logger.warning(
                    "Binary payload from MEXC not valid JSON; protobuf decoder stub will skip this frame"
                )
                return None

        if isinstance(message, str):
            return json.loads(message)

        try:
            return json.loads(message)
        except Exception:
            logger.warning(f"Unexpected payload type from MEXC WebSocket: {type(message).__name__}")
            return None

    async def _websocket_ping(self, ws):
        """WebSocket bağlantısını canlı tutmak için düzenli ping gönder"""
        try:
            while True:
                await asyncio.sleep(30)
                timestamp = int(time.time() * 1000)
                method_ping = {"method": "PING", "id": timestamp}
                legacy_ping = {"ping": timestamp}
                await ws.send(json.dumps(method_ping))
                await ws.send(json.dumps(legacy_ping))
                logger.debug("Sent heartbeat ping to MEXC")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"WebSocket ping error: {str(e)}")
    
    def subscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerine abone ol"""
        if symbol not in self.price_subscribers:
            self.price_subscribers[symbol] = []
        self.price_subscribers[symbol].append(callback)
    
    def unsubscribe_price(self, symbol: str, callback):
        """Bir sembol için fiyat güncellemelerinden aboneliği kaldır"""
        if symbol in self.price_subscribers and callback in self.price_subscribers[symbol]:
            self.price_subscribers[symbol].remove(callback)
