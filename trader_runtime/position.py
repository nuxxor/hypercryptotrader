from .common import *


class Position:
    """Açık pozisyonları temsil eden geliştirilmiş sınıf"""
    def __init__(
        self,
        symbol: str,
        exchange: str,
        entry_price: float,
        quantity: float,
        entry_time: datetime,
        order_id: str,
        listing_source: str = None,
        account_name: str = "PRIMARY",
        alert_type: Optional[str] = None,
        amount_millions: Optional[float] = None,
    ):
        self.symbol = symbol
        self.exchange = exchange  # 'binance', 'gate' veya 'mexc'
        self.entry_price = entry_price
        self.quantity = quantity
        self.entry_time = entry_time
        self.order_id = order_id
        self.current_price = entry_price
        self.highest_price = entry_price
        self.last_checked = entry_time
        self.status = "active"  # active, partial_sold, closed
        
        # Yeni özellikler
        self.listing_source = listing_source  # 'coinbase', 'binance', 'upbit', 'bithumb'
        self.price_history = []  # Fiyat tarihçesi [(timestamp, price), ...]
        self.volume_history = []  # Hacim tarihçesi [(timestamp, volume), ...]
        self.partial_sells = []  # Kısmi satışlar [(timestamp, quantity, price), ...]
        self.strategy_state = {}  # Strateji durumu
        
        # Trailing stop bilgileri
        self.trailing_stop_activated = False
        self.trailing_stop_price = 0
        self.trailing_stop_percent = 0
        self.alert_type = alert_type.lower() if isinstance(alert_type, str) and alert_type else None
        self.invest_amount_millions = (
            float(amount_millions) if isinstance(amount_millions, (int, float)) else None
        )
        
        # Trade ID ve başlangıç bakiyesi
        self.trade_id = None
        self.initial_balance = 0
        self.post_entry_balance = 0
        
        # Market Cap bilgisi
        self.market_cap = None
        
        # Hangi hesaptan alındığı bilgisi
        self.account_name = account_name
        
        # Upbit KRW pair bilgisi
        self.is_krw_pair = False
        
    @property
    def age_seconds(self) -> float:
        """Pozisyonun yaşını saniye cinsinden alır"""
        return (datetime.now() - self.entry_time).total_seconds()
    
    @property
    def age_minutes(self) -> float:
        """Pozisyonun yaşını dakika cinsinden alır"""
        return self.age_seconds / 60
    
    @property
    def remaining_quantity(self) -> float:
        """Kalan miktarı alır (kısmi satışları hesaba katar)"""
        sold_qty = sum(qty for _, qty, _ in self.partial_sells)
        return self.quantity - sold_qty
    
    @property
    def current_profit_pct(self) -> float:
        """Mevcut kâr yüzdesini alır"""
        if self.entry_price <= 0:
            return 0
        return ((self.current_price - self.entry_price) / self.entry_price) * 100
    
    @property
    def total_realized_profit_usdt(self) -> float:
        """Gerçekleşen toplam kârı USDT cinsinden alır"""
        return sum((price - self.entry_price) * qty for _, qty, price in self.partial_sells)
    
    @property
    def market_pair(self) -> str:
        """Borsa formatına uygun market çifti alır"""
        if self.exchange == 'binance':
            return f"{self.symbol}USDT"
        elif self.exchange == 'mexc':
            return f"{self.symbol}USDT"  
        else:  # gate
            return f"{self.symbol}_USDT"
    
    def update_price(self, new_price: float, volume: float = None):
        """Mevcut fiyatı güncelle ve en yüksek fiyatı takip et"""
        timestamp = datetime.now()
        
        # Fiyat bilgisini güncelle
        self.current_price = new_price
        if new_price > self.highest_price:
            self.highest_price = new_price
        self.last_checked = timestamp
        
        # Fiyat ve hacim geçmişini güncelle
        self.price_history.append((timestamp, new_price))
        if volume is not None:
            self.volume_history.append((timestamp, volume))
        
        # Son 30 fiyat kaydını tut
        if len(self.price_history) > 30:
            self.price_history.pop(0)
        if len(self.volume_history) > 30:
            self.volume_history.pop(0)
            
        # Trailing stop varsa kontrol et
        if self.trailing_stop_activated:
            if new_price > self.trailing_stop_price / (1 - self.trailing_stop_percent / 100):
                # Trailing stop fiyatını güncelle
                self.trailing_stop_price = new_price * (1 - self.trailing_stop_percent / 100)
                logger.debug(f"Updated trailing stop for {self.symbol} to {self.trailing_stop_price:.8f}")
    
    def set_trailing_stop(self, percent: float):
        """Trailing stop ayarla"""
        self.trailing_stop_activated = True
        self.trailing_stop_percent = percent
        self.trailing_stop_price = self.current_price * (1 - percent / 100)
        logger.info(f"Set {percent}% trailing stop for {self.symbol} at price {self.trailing_stop_price:.8f}")
    
    def check_trailing_stop(self) -> bool:
        """Trailing stop tetiklendi mi?"""
        if not self.trailing_stop_activated:
            return False
        
        return self.current_price <= self.trailing_stop_price
    
    def is_momentum_lost(self) -> bool:
        """Momentum kaybı var mı? (Son 5 fiyat hareketine bakarak)"""
        if len(self.price_history) < 5:
            return False
            
        recent_prices = [price for _, price in self.price_history[-5:]]
        
        # Son 5 hareketin kaçı düşüş?
        drops = 0
        for i in range(1, len(recent_prices)):
            if recent_prices[i] < recent_prices[i-1]:
                drops += 1
                
        # 5 hareketin 3'ü düşüş = momentum lost
        return drops >= 3
    
    def calculate_rsi(self, period: int = 14, resample_seconds: int = 5) -> Optional[float]:
        """Belirli bir periyot için RSI hesapla (pandas kullanmadan)."""
        if len(self.price_history) < period + 1:
            return None

        if resample_seconds and resample_seconds > 0:
            closes = []
            current_bucket = None
            current_close = None
            for timestamp, price in self.price_history:
                bucket = int(timestamp.timestamp() // int(resample_seconds))
                if current_bucket is None:
                    current_bucket = bucket
                    current_close = float(price)
                    continue
                if bucket != current_bucket:
                    closes.append(current_close)
                    current_bucket = bucket
                current_close = float(price)
            if current_close is not None:
                closes.append(current_close)
        else:
            closes = [float(price) for _, price in self.price_history]

        if len(closes) <= period:
            return None

        deltas = [current - previous for previous, current in zip(closes, closes[1:])]
        if len(deltas) < period:
            return None

        relevant_deltas = deltas[-period:]
        gains = [delta for delta in relevant_deltas if delta > 0]
        losses = [-delta for delta in relevant_deltas if delta < 0]

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return float(100 - (100 / (1 + rs)))

    def get_price_momentum(self, lookback: int = 5) -> float:
        """Son N fiyattan momentumu hesapla"""
        if len(self.price_history) < lookback:
            return 0
            
        # Son fiyatları al
        recent_prices = [price for _, price in self.price_history[-lookback:]]
        
        # Momentum hesapla (fiyat değişim eğilimi)
        if len(recent_prices) >= 2:
            first_price = recent_prices[0]
            last_price = recent_prices[-1]
            return ((last_price / first_price) - 1) * 100
        return 0
    
    def get_volume_trend(self, lookback: int = 5) -> float:
        """Son N hacimden hacim trendini hesapla"""
        if len(self.volume_history) < lookback:
            return 0
            
        # Son hacimleri al
        recent_volumes = [vol for _, vol in self.volume_history[-lookback:]]
        
        # Trend hesapla (ilk ve son arasındaki fark)
        if len(recent_volumes) >= 2:
            first_volume = recent_volumes[0]
            last_volume = recent_volumes[-1]
            if first_volume > 0:
                return ((last_volume / first_volume) - 1) * 100
        return 0
    
    def to_dict(self) -> dict:
        """Pozisyonu sözlük formatına dönüştür"""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "listing_source": self.listing_source,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "highest_price": self.highest_price,
            "quantity": self.quantity,
            "remaining_quantity": self.remaining_quantity,
            "entry_time": self.entry_time.isoformat(),
            "age_minutes": self.age_minutes,
            "current_profit_pct": self.current_profit_pct,
            "realized_profit_usdt": self.total_realized_profit_usdt,
            "status": self.status,
            "trailing_stop_active": self.trailing_stop_activated,
            "trailing_stop_price": self.trailing_stop_price if self.trailing_stop_activated else 0,
            "trade_id": self.trade_id,
            "market_cap": self.market_cap,
            "account_name": self.account_name,
            "is_krw_pair": self.is_krw_pair,
        }


