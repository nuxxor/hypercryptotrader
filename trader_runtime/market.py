from .common import *



class CoinMarketCapAPI:
    """CoinMarketCap API - Local JSON'dan okuma"""
    def __init__(self):
        self.data_file = os.getenv("TREE_NEWS_CMC_FILE", "var/coinmarketcap_data.json")
        self.coin_data = {}
        self._load_cached_data()
    
    def _load_cached_data(self):
        """Cached coin verisini yükle"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    self.coin_data = json.load(f)
                    
                # Metadata'yı çıkar
                if "_metadata" in self.coin_data:
                    del self.coin_data["_metadata"]
                    
                logger.info(f"Loaded {len(self.coin_data)} coins from local cache")
        except Exception as e:
            logger.error(f"Error loading cached coin data: {str(e)}")
            self.coin_data = {}
    
    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Bir sembol için market cap değerini döndürür"""
