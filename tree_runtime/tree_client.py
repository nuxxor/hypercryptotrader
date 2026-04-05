import traceback

from .common import *
from .ai_verifier import NewsAIVerifier
from .hub import SignalHub

class TreeNewsClient:
    """Async Tree News WebSocket client"""
    
    def __init__(
        self,
        name: str,
        url: str,
        hub: SignalHub,
    ):
        self.name = name
        self.url = url
        self.hub = hub
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[ClientWebSocketResponse] = None
        self.running = True
        self.reconnect_delay = RECONNECT_BASE_DELAY
        self.logged_in = False
        self.ai_verifier = NewsAIVerifier()
        self.message_queue: asyncio.Queue = asyncio.Queue(
            maxsize=TREE_NEWS_MESSAGE_QUEUE_SIZE
        )
        self.message_workers = TREE_NEWS_MESSAGE_WORKERS
        self.worker_tasks: List[asyncio.Task] = []
        self._background_tasks: Set[asyncio.Task] = set()
        self._reconnect_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()

    def _track_background_task(
        self,
        task: asyncio.Task,
        label: str,
        *,
        is_reconnect: bool = False,
    ) -> asyncio.Task:
        self._background_tasks.add(task)

        def _finalize(done: asyncio.Task) -> None:
            self._background_tasks.discard(done)
            if is_reconnect and self._reconnect_task is done:
                self._reconnect_task = None
            try:
                done.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error("[%s] Background task failed (%s): %s", self.name, label, exc)
                logger.debug(traceback.format_exc())

        task.add_done_callback(_finalize)
        return task

    def _spawn_background_task(
        self,
        coro,
        label: str,
        *,
        is_reconnect: bool = False,
    ) -> asyncio.Task:
        if is_reconnect and self._reconnect_task and not self._reconnect_task.done():
            return self._reconnect_task

        task = asyncio.create_task(coro)
        tracked = self._track_background_task(task, label, is_reconnect=is_reconnect)
        if is_reconnect:
            self._reconnect_task = tracked
        return tracked
    
    def get_detailed_timestamp(self) -> str:
        """Get timestamp with milliseconds"""
        now = datetime.now(timezone.utc)
        return now.strftime("%H:%M:%S.%f")[:-3]
    
    def display_social_message(self, data: dict):
        """Display social media messages in a clean format"""
        if not DEBUG_MODE:
            return
            
        timestamp = self.get_detailed_timestamp()
        source = data.get('source', 'Unknown')
        
        # Choose color and icon based on source
        if source == 'TWITTER':
            color = Colors.BLUE
            icon = "🐦"
        elif source == 'TELEGRAM':
            color = Colors.GREEN
            icon = "💬"
        else:
            color = Colors.GRAY
            icon = "📝"
        
        # Extract only essential info
        text = data.get('text', '')
        author = data.get('author', {})
        
        if author:
            username = author.get('screen_name', author.get('name', 'Unknown'))
        else:
            username = 'Unknown'
        
        # Clean display
        print(f"\n{color}{icon} [{self.name}] [{timestamp}] {source}{Colors.RESET}")
        if username != 'Unknown':
            print(f"{Colors.GRAY}@{username}:{Colors.RESET} {text[:200]}")
        else:
            print(f"{Colors.GRAY}Message:{Colors.RESET} {text[:200]}")
    
    def display_news(self, data: dict, important: bool = False):
        """Display news in console with colors and richer metadata"""
        timestamp = self.get_detailed_timestamp()

        title = data.get('title') or ''
        body = data.get('body') or ''
        text = data.get('text') or ''
        en_text = data.get('en') or ''
        url = data.get('url') or ''

        raw_source = (data.get('source') or '').strip()
        display_source = raw_source

        host = ""
        if url:
            try:
                host = urlparse(url).netloc
            except Exception:
                host = ""

        info = data.get('info') if isinstance(data.get('info'), dict) else {}
        author = data.get('author') if isinstance(data.get('author'), dict) else {}
        icon_url = data.get('icon') or ''
        image_url = data.get('image') or ''

        def _is_unknown(value: Optional[str]) -> bool:
            return not value or value.strip().upper() == 'UNKNOWN'

        if _is_unknown(display_source) and host:
            host_lower = host.lower()
            if host_lower in ('twitter.com', 'www.twitter.com', 'x.com', 'www.x.com'):
                display_source = 'Twitter'
            elif host_lower in ('t.me', 'telegram.me', 'telegram.org', 'www.t.me', 'web.telegram.org'):
                display_source = 'Telegram'
            else:
                display_source = host

        if _is_unknown(display_source) and info.get('twitterId'):
            display_source = 'Twitter'

        if _is_unknown(display_source):
            if any(key.lower().startswith('telegram') for key in info.keys()):
                display_source = 'Telegram'

        if _is_unknown(display_source) and icon_url:
            if 'pbs.twimg.com' in icon_url:
                display_source = 'Twitter'
            elif 'telegram' in icon_url:
                display_source = 'Telegram'

        if _is_unknown(display_source) and image_url:
            if 'pbs.twimg.com' in image_url:
                display_source = 'Twitter'
            elif 'telegram' in image_url:
                display_source = 'Telegram'

        if _is_unknown(display_source) and author:
            handle = (author.get('screen_name') or author.get('name') or '').strip()
            if handle and ('@' in title or '@' in text):
                display_source = 'Twitter'

        if _is_unknown(display_source) and host:
            display_source = host

        if _is_unknown(display_source):
            display_source = 'Unknown'

        if display_source and display_source.lower() in ('twitter.com', 'www.twitter.com', 'x.com', 'www.x.com'):
            display_source = 'Twitter'
        elif display_source and display_source.lower() in ('t.me', 'telegram.me', 'telegram.org', 'www.t.me', 'web.telegram.org'):
            display_source = 'Telegram'

        important_keywords = [
            'listing', 'list', 'launchpad', 'launchpool', 'binance',
            'bybit', 'okx', 'launch', 'airdrop', 'pump', 'dump',
            'hack', 'exploit', 'upbit', 'bithumb', 'files s-1'
        ]
        try:
            message_text = json.dumps(data, ensure_ascii=False).lower()
        except (TypeError, ValueError):
            message_text = str(data).lower()
        is_important = important or any(keyword in message_text for keyword in important_keywords)

        neg_pr = ['already listed', 'has been listed', 'is listed', 'listed on']
        if any(p in message_text for p in neg_pr):
            is_important = False

        source_colors = {
            'TWITTER': Colors.BLUE,
            'TELEGRAM': Colors.GREEN,
            'BLOGS': Colors.YELLOW,
            'EXCHANGE': Colors.MAGENTA,
            'UPBIT': Colors.MAGENTA,
            'BITHUMB': Colors.MAGENTA,
            'BINANCE': Colors.MAGENTA,
            'TERMINAL': Colors.MAGENTA,
            'PROPOSALS': Colors.YELLOW,
            'UNKNOWN': Colors.GRAY,
        }
        source_key = (display_source or '').upper()
        source_color = source_colors.get(source_key, Colors.RESET)

        if is_important:
            print(f"\n{Colors.RED}⚠️  [{self.name}] [{timestamp}] IMPORTANT NEWS:{Colors.RESET}")
            print(Colors.RED + "-" * 60 + Colors.RESET)
        else:
            print(f"\n{source_color}📰 [{self.name}] [{timestamp}]{Colors.RESET}")

        if title:
            print(f"{Colors.BOLD}Title:{Colors.RESET} {title}")

        if body:
            body_display = body[:400] + "..." if len(body) > 400 else body
            print(f"{Colors.BOLD}Body:{Colors.RESET} {body_display}")
        elif en_text and en_text != title:
            en_display = en_text[:400] + "..." if len(en_text) > 400 else en_text
            print(f"{Colors.BOLD}Content:{Colors.RESET} {en_display}")

        if text and text not in (body, en_text):
            text_display = text[:400] + "..." if len(text) > 400 else text
            print(f"{Colors.BOLD}Text:{Colors.RESET} {text_display}")

        if display_source:
            print(f"{Colors.GRAY}Source: {source_color}{display_source}{Colors.RESET}")
        if url:
            print(f"{Colors.GRAY}URL: {Colors.CYAN}{url}{Colors.RESET}")

        event_time_ms = data.get('time')
        if isinstance(event_time_ms, (int, float)):
            try:
                dt = datetime.fromtimestamp(event_time_ms / 1000.0, timezone.utc)
                print(f"{Colors.GRAY}Event time (UTC):{Colors.RESET} {dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except (OSError, ValueError):
                print(f"{Colors.GRAY}Event time (raw):{Colors.RESET} {event_time_ms}")

        symbols = data.get('symbols')
        if symbols:
            if isinstance(symbols, (list, tuple)):
                formatted = ', '.join(str(sym) for sym in symbols)
            else:
                formatted = str(symbols)
            print(f"{Colors.BOLD}Symbols:{Colors.RESET} {formatted}")

        first_price = data.get('firstPrice')
        if isinstance(first_price, dict) and first_price:
            formatted_pairs = ', '.join(f"{k}: {v}" for k, v in first_price.items())
            print(f"{Colors.BOLD}First prices:{Colors.RESET} {formatted_pairs}")

        coin = data.get('coin')
        if coin:
            print(f"{Colors.BOLD}Coin:{Colors.RESET} {Colors.MAGENTA}{coin}{Colors.RESET}")

        suggestions = data.get('suggestions') or []
        if suggestions:
            print(f"{Colors.BOLD}Suggestions:{Colors.RESET}")
            for idx, suggestion in enumerate(suggestions, start=1):
                coin_name = suggestion.get('coin')
                header = f"  {idx}."
                if coin_name:
                    header += f" Coin: {Colors.MAGENTA}{coin_name}{Colors.RESET}"
                print(header)
                found = suggestion.get('found')
                if found:
                    found_list = ', '.join(str(item) for item in found)
                    print(f"     Found: {found_list}")
                suggestion_symbols = suggestion.get('symbols')
                if suggestion_symbols:
                    formatted_symbols = []
                    for item in suggestion_symbols:
                        if isinstance(item, dict):
                            exch = item.get('exchange')
                            sym = item.get('symbol')
                            if exch and sym:
                                formatted_symbols.append(f"{exch}:{sym}")
                            elif sym:
                                formatted_symbols.append(sym)
                        else:
                            formatted_symbols.append(str(item))
                    if formatted_symbols:
                        print(f"     Symbols: {', '.join(formatted_symbols)}")
                if suggestion.get('supply'):
                    print(f"     Supply: {suggestion['supply']}")

        author = data.get('author')
        if isinstance(author, dict) and author:
            handle = author.get('screen_name') or author.get('name')
            if handle:
                print(f"{Colors.BOLD}Author:{Colors.RESET} @{handle}")

        info = data.get('info')
        if isinstance(info, dict) and info:
            print(f"{Colors.BOLD}Info:{Colors.RESET} " + ', '.join(f"{k}={v}" for k, v in info.items()))

        if data.get('icon'):
            print(f"{Colors.BOLD}Icon:{Colors.RESET} {data['icon']}")
        if data.get('image'):
            print(f"{Colors.BOLD}Image:{Colors.RESET} {data['image']}")

        if is_important:
            print(Colors.RED + "-" * 60 + Colors.RESET)
    
    def extract_amount(self, text: str) -> float:
        """
        Yalnızca açık para birimi bağlamı olan sayıları kabul et:
          - $1.65B, $1.65 billion, USD 1.65B, USD 500M, 1.65 billion USD
        Çıplak sayılar (unit yoksa) REDDEDİLİR.
        Çıkarım: milyon dolar cinsinden float döner.
        """
        if not text:
            return 0.0

        # Negatif bağlam kontrolü
        if NEGATIVE_CONTEXT.search(text):
            return 0.0
        
        # Metrik blacklist kontrolü
        if METRIC_BLACKLIST.search(text):
            return 0.0

        # Normalize spaces
        t = re.sub(r'\s+', ' ', text)

        # Sadece $ veya USD ile başlayan tutarlar
        patterns = [
            r'(?:\$|USD\s+)(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(K|M|B|T|MN|BN|TN|million|billion|trillion)\b'
        ]

        candidates = []
        for pat in patterns:
            for m in re.finditer(pat, t, flags=re.IGNORECASE):
                num_str = m.group(1).replace(',', '')
                unit = m.group(2).lower()
                try:
                    num = float(num_str)
                except:
                    continue

                # Unit → millions
                if unit in ('k',):
                    num = num / 1000.0
                elif unit in ('m', 'mn', 'million'):
                    num = num
                elif unit in ('b', 'bn', 'billion'):
                    num = num * 1000.0
                elif unit in ('t', 'tn', 'trillion'):
                    num = num * 1_000_000.0
                else:
                    continue

                # Hard bounds / sanity checks
                if not (0 < num < 1_000_000):  # < $1T (in millions)
                    continue

                candidates.append(num)

        if not candidates:
            return 0.0

        # En büyük tutarı dön (aynı metinde farklı sayılar olabilir)
        return max(candidates)
    
    def _has_investment_context_near_amount(self, text: str) -> bool:
        """Check if investment context exists near amount"""
        txt = re.sub(r'\s+', ' ', text)
        
        # Negatif bağlam kontrolü
        if NEGATIVE_CONTEXT.search(txt):
            return False
            
        amt_re = re.compile(
            r'(?:\$|USD\s+)\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*(?:K|M|B|T|MN|BN|TN|million|billion|trillion)\b',
            re.IGNORECASE
        )
        ctx_re = re.compile(
            r'\b('
            r'raise[sd]?|funding|investment|financing|round|'
            r'series\s*[abcdef]|seed\s+round|pre-?seed|strategic\s+round|'
            r'private\s+placement|pipe|treasury\s+(?:strategy|allocation|reserve|plan)|'
            r'buy\s*back|repurchas(?:e|ing)|purchase[sd]?|purchasing|buy(?:ing|s)?|bought|'
            r'acquire[sd]?|acquisition|'
            r'valuation|led\s+by|co-?led|invests?|backs?|'
            r'commit(?:s|ment)?|allocat(?:e|es|ed|ion)|'
            r'debt\s+financing|convertible\s+notes?|senior\s+notes?'
            r')\b',
            re.IGNORECASE
        )
        for m in amt_re.finditer(txt):
            window = txt[max(0, m.start()-80): m.end()+80]
            if ctx_re.search(window):
                return True
        return False
    
    def normalize_tokens(self, tokens: List[str], raw_text: str = "", exclude_majors: bool = False) -> List[str]:
        """
        Normalize and validate tokens.
        - Varsayılan: en az 3 karakter (2 harfli sadece $PREFIX veya (TOKEN) ile yakalandıysa kabul)
        - Common stopwords/bağlaçlar (IN, OF, TO, ON, AT, BY, FOR ...) auto-drop
        - Büyük stable/majors whitelist zaten hariç
        """
        base_stables = {'USD', 'USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI', 'USD1', 'USDE'}
        if exclude_majors:
            EXCLUDE = PARTNERSHIP_EXCLUDED_COINS | base_stables
        else:
            EXCLUDE = base_stables
        
        raw_upper = (raw_text or '').upper()
        normalized = []

        for token in tokens:
            t = token.strip().upper()
            if not re.match(r'^[A-Z0-9\-]{1,15}$', t):
                continue
            if t in EXCLUDE or t in STOPWORDS or t in TIMEZONES or t in ACRONYM_BLACKLIST:
                continue

            if len(t) <= 2:
                if exclude_majors:
                    continue
                strong = (
                    re.search(rf'\${re.escape(t)}\b', raw_upper) is not None or
                    re.search(rf'\({re.escape(t)}\)', raw_upper) is not None
                )
                if not strong:
                    continue

            # Filter out common industry acronyms when they only describe a
            # concept (e.g. "zero-knowledge proof (ZKP)") instead of a
            # tradable asset. We examine the words immediately preceding the
            # raw token mention; if every occurrence is surrounded by
            # descriptive context, skip the token.
            context_words = CONTEXT_TOKEN_FILTERS.get(t)
            if context_words and raw_upper:
                pattern = re.compile(r'\(?\$?' + re.escape(t) + r'\)?')
                matches = list(pattern.finditer(raw_upper))
                if matches:
                    contextual_only = True
                    for match in matches:
                        window_start = max(0, match.start() - 80)
                        window_text = raw_upper[window_start:match.start()]
                        words = re.findall(r'[A-Z0-9]+', window_text)
                        recent_words = set(words[-5:])  # focus on last few
                        if not recent_words.intersection(context_words):
                            contextual_only = False
                            break
                    if contextual_only:
                        continue

            normalized.append(t)

        return sorted(set(normalized))
    
    def check_megabrand_deal(self, data: dict) -> Tuple[bool, List[str], Optional[str]]:
        """Check for mega-brand partnership/integration (disabled by default)."""
        if not ENABLE_PARTNERSHIP_SIGNALS:
            return False, [], None

        title = data.get('title', '') or ''
        body  = data.get('body', '') or ''
        full  = f"{title} {body}"

        if not DEAL_KEYWORDS.search(full):
            return False, [], None

        brand = _find_megabrand(full)
        if not brand:
            return False, [], None

        # Tüm partnership haberleri için güvenilir URL şartı, ancak doğrulanmış resmi sosyal duyurular istisna
        url = data.get('url')
        if not _trusted_url(url):
            if not _is_verified_social_announcement(data):
                return False, [], None

        # Marka etrafında token ifadesi ($TOKEN veya (TOKEN)) aralığı şartı (yakın bağlam)
        near_ok = False
        m = re.search(rf"\b{re.escape(brand)}\b", full, re.IGNORECASE)
        if m:
            window = full[max(0, m.start()-160): m.end()+160]
            if re.search(r'\$[A-Z][A-Z0-9\-]{1,14}\b|\([A-Z][A-Z0-9\-]{1,14}\)', window):
                near_ok = True

        explicit_dollars = set(re.findall(r'\$([A-Z][A-Z0-9\-]{1,14})\b', full))
        paren_tokens = set(re.findall(r'\(([A-Z][A-Z0-9\-]{1,14})\)', full))
        strong_evidence = bool(data.get('coin') or data.get('suggestions') or explicit_dollars)
        if not strong_evidence:
            return False, [], None

        tokens = set()
        if data.get('coin'):
            tokens.update(self.normalize_tokens([data['coin']], raw_text=full, exclude_majors=True))

        if data.get('suggestions'):
            for s in data['suggestions']:
                c = s.get('coin')
                if c:
                    tokens.update(self.normalize_tokens([c], raw_text=full, exclude_majors=True))

        if explicit_dollars:
            tokens.update(self.normalize_tokens(list(explicit_dollars), raw_text=full, exclude_majors=True))

        if paren_tokens:
            tokens.update(self.normalize_tokens(list(paren_tokens), raw_text=full, exclude_majors=True))

        if not tokens:
            return False, [], None

        explicit_found = bool(explicit_dollars)
        if not explicit_found and not data.get('coin') and not near_ok:
            return False, [], None

        return True, sorted(tokens), brand
    
    def check_upbit_listing(self, data: dict) -> Tuple[bool, List[str], Optional[str]]:
        title = (data.get('title') or '')
        body  = (data.get('body') or '')
        low   = (title + ' ' + body).lower()
        src   = (data.get('source') or '').upper()

        # Upbit kaynağını metindeki 'upbit' şartına alternatif olarak kabul et
        has_upbit = ('upbit' in low) or (src == 'UPBIT')

        # KRW sinyali: 'krw' ya da Korece '원화'
        has_krw = ('krw' in low) or ('원화' in low)

        if not (has_upbit and has_krw):
            return False, [], None

        # Notice id (varsa)
        notice_id = None
        url = data.get('url') or ''
        if 'upbit.com' in url:
            m = re.search(r'id=(\d+)', url)
            if m:
                notice_id = m.group(1)

        # YENİ: Sadece resmi kaynak/duyuru kabul
        # - src doğrudan 'UPBIT' ise (Tree News exchange kaynağı)
        # - veya resmi Upbit notice linki (notice_id) varsa
        if not (src == 'UPBIT' or notice_id):
            return False, [], None

        full_text = f"{title} {body}"

        # Önce coin alanı/suggestions
        tokens: List[str] = []
        if data.get('coin'):
            tokens = self.normalize_tokens([data['coin']], raw_text=full_text)
        if not tokens:
            # Başlık/gövde içinden (TOKEN) ve $TOKEN yakala
            paren = re.findall(r'\(([A-Z0-9]{1,15})\)', full_text)
            dollar = re.findall(r'\$([A-Z0-9]{1,15})\b', full_text)
            candidates = paren + dollar
            tokens = self.normalize_tokens(candidates, raw_text=full_text)

        return (len(tokens) > 0), sorted(tokens), notice_id

    
    def check_bithumb_listing(self, data: dict) -> Tuple[bool, List[str]]:
        """Check for Bithumb listing"""
        if not ENABLE_BITHUMB_LISTINGS:
            return False, []
        title = data.get('title', '') or ''
        body = data.get('body', '') or ''
        full_text = f"{title} {body}"
        src = (data.get('source') or '').upper()
        url = data.get('url') or ''

        # Yalnızca resmi kaynak kabul: Tree News kaynağı BITHUMB veya resmi Bithumb URL'si
        if not (src == 'BITHUMB' or 'bithumb.com' in url.lower()):
            return False, []

        # Zorunlu kalıp: 원화 마켓 추가 (KRW market addition)
        if not re.search(r'원화\s*마켓\s*추가', full_text):
            return False, []

        # Birden çok (TOKEN) olabilir: hepsini çek
        paren = re.findall(r'\(([A-Z0-9]{1,15})\)', full_text)
        tokens = self.normalize_tokens(paren, raw_text=full_text)
        if tokens:
            return True, tokens

        # Yapısal coin alanı (varsa son çare)
        coin = data.get('coin', '')
        if coin:
            tokens = self.normalize_tokens([coin], raw_text=full_text)
            if tokens:
                return True, tokens

        return False, []
    
    def _extract_binance_seed_tokens(self, text: str) -> List[str]:
        if not text:
            return []

        candidates = []
        candidates.extend(re.findall(r'\(([A-Z0-9]{2,12})\)', text))
        candidates.extend(re.findall(r'\$([A-Z0-9]{2,12})\b', text))

        if not candidates:
            return []

        tokens = self.normalize_tokens(candidates, raw_text=text)
        tokens = [tok for tok in tokens if tok not in BINANCE_PROMO_TOKEN_EXCLUDES]
        return sorted(set(tokens))

    def check_binance_seed_hodler(self, data: dict) -> Tuple[bool, List[str]]:
        """Detect Binance seed-tag applications or HODLer introductions."""
        source = (data.get('source') or '').upper()
        url = (data.get('url') or '').lower()

        if 'binance.com' not in url and 'BINANCE' not in source:
            return False, []

        title_clean = _clean_announcement_text(data.get('title') or '')
        body_clean = _clean_announcement_text(data.get('body') or '')
        combined = f"{title_clean} {body_clean}".strip()

        if not combined:
            return False, []

        if not (BINANCE_SEED_TAG_PATTERN.search(combined) or BINANCE_HODLER_PATTERN.search(combined)):
            return False, []

        tokens_set = set()

        if data.get('coin'):
            tokens_set.update(
                tok for tok in self.normalize_tokens([data['coin']], raw_text=combined)
                if tok not in BINANCE_PROMO_TOKEN_EXCLUDES
            )

        if data.get('symbols'):
            tokens_set.update(
                tok for tok in self.normalize_tokens(data.get('symbols', []), raw_text=combined)
                if tok not in BINANCE_PROMO_TOKEN_EXCLUDES
            )

        if data.get('suggestions'):
            for suggestion in data.get('suggestions', []):
                coin = suggestion.get('coin')
                if coin:
                    tokens_set.update(
                        tok for tok in self.normalize_tokens([coin], raw_text=combined)
                        if tok not in BINANCE_PROMO_TOKEN_EXCLUDES
                    )

        extracted_title = self._extract_binance_seed_tokens(title_clean)
        if extracted_title:
            tokens_set.update(extracted_title)

        if not extracted_title:
            tokens_set.update(self._extract_binance_seed_tokens(body_clean))

        tokens = sorted(tokens_set)

        if not tokens:
            return False, []

        return True, tokens
    
    def check_investment(self, data: dict) -> Tuple[bool, List[str], float]:
        """
        $100M+ yatırım/treasury alımı için sıkı filtre:
          - Zorunlu: extract_amount(text) >= INVESTMENT_MIN_AMOUNT_MILLIONS
          - Token çıkarımı: önce $TOKEN veya (TOKEN), sonra coin/suggestions
          - 2 harfli token sadece güçlü bağlamla (normalize_tokens içinde denetlenir)
          - Major coinler (BTC, ETH vb.) exclude edilir
        """
        title = data.get('title', '') or ''
        body = data.get('body', '') or ''
        full = f"{title} {body}"

        # Negatif bağlam kontrolü
        if NEGATIVE_CONTEXT.search(full):
            return False, [], 0.0

        # 1) Yatırım anahtar kelime sinyali (genişletilmiş çekirdek + destekleyici)
        keywords = [
            # ağır çekirdek
            'raises', 'raised', 'funding', 'investment',
            'led by', 'co-led', 'seed round', 'series a', 'series b', 'series c',
            'series d', 'series e', 'series f', 'strategic round',
            'private placement', 'pipe', 'treasury strategy', 'treasury allocation',
            'treasury reserve', 'treasury plan',
            # destekleyici bağlamlar
            'valuation', 'commit', 'commits', 'commitment',
            'backs', 'invests', 'investment from', 'allocation', 'allocates', 'allocated',
            'debt financing', 'convertible notes', 'senior notes',
            'closes funding', 'closed funding', 'closes round', 'closed round',
            # treasury/token purchases
            'purchases', 'purchase', 'purchased', 'buying', 'buys', 'bought',
            'acquires', 'acquired', 'acquisition', 'treasury purchase'
        ]
        if not any(kw in full.lower() for kw in keywords):
            return False, [], 0.0

        # 2) Tutar (milyon $)
        amount = self.extract_amount(full)
        if amount < INVESTMENT_MIN_AMOUNT_MILLIONS:
            return False, [], 0.0

        # 3) Yatırım bağlamı (miktar çevresi)
        if not self._has_investment_context_near_amount(full):
            return False, [], 0.0

        # 4) Equity bağlamı? (buyback vs.)
        equity = _has_equity_context(full)

        # 5) Token çıkarımı (sıkı)
        tokens = set()
        explicit_found = False
        # Explicit mentions
        explicit_dollar = re.findall(r'\$([A-Z][A-Z0-9\-]{1,14})\b', full)
        explicit_paren  = re.findall(r'\(([A-Z][A-Z0-9\-]{1,14})\)', full)
        for m in explicit_dollar + explicit_paren:
            tokens.add(m.upper())
        if tokens:
            explicit_found = True

        raw_upper = full.upper()
        # Structured coin field allowed
        if data.get('coin'):
            tokens.update(self.normalize_tokens([data['coin']], raw_text=raw_upper, exclude_majors=True))

        # Equity bağlamındaysa hisse olma ihtimali yüksek $XXXX sembollerini sil
        # (Hisseyi anlatan tipik kelimeler + "shares/outstanding" vs. yakalandıysa)
        if equity and tokens:
            # "token/coin" gibi kripto bağlamı yoksa tüm $XXXX'leri temizle
            if not re.search(r'\btoken|coin|cryptocurrency|chain|blockchain|protocol\b', raw_upper):
                tokens.clear()

        # Majors ve zaman dilimlerini ayıkla
        tokens = self.normalize_tokens(list(tokens), raw_text=raw_upper, exclude_majors=True)

        # Emniyet: zaman dilimleri ve para birimleri gibi kaçakları temizle
        DROP_EXTRA = {'UTC','KST','JST','KRW','USD','USDT','USDC','BUSD','FDUSD'}
        tokens = [t for t in tokens if t not in DROP_EXTRA]
        # Zaman dilimi/boş kaldıysa gönderme
        if not tokens:
            return False, [], 0.0

        # 6) Güvenilir URL zorunlu (tüm kaynaklar)
        if not _trusted_url(data.get('url','')):
            return False, [], 0.0

        # 7) Sadece suggestions ile gelen tokenları reddet (explicit/coin şart)
        if not explicit_found and not data.get('coin'):
            return False, [], 0.0

        return True, sorted(tokens), amount
    
    async def process_message(self, data: dict):
        """Process Tree News message"""
        self.hub.stats["total_news_received"] += 1
        
        # Check if login success message
        if "success" in data and "Logged in" in data.get("message", ""):
            print(f"{Colors.GREEN}[{self.name}] ✅ {data['message']}{Colors.RESET}")
            self.logged_in = True
            return
        
        # Check if status message
        if "user" in data and "username" in data.get("user", {}):
            username = data['user']['username']
            status = data['user'].get('highestRole', 'Unknown')
            sub_status = "Active" if data['user'].get('isSub') else "Inactive"
            print(f"{Colors.CYAN}[{self.name}] 👤 User: {username}, Status: {status}, Subscription: {sub_status}{Colors.RESET}")
            return
        
        # Global blacklist (e.g., WuBlockchain aggregator)
        if _is_blacklisted(data):
            self.hub.stats["news_filtered"] += 1
            return

        # Skip if no title
        if not data.get('title'):
            return
        
        # Display news
        self.display_news(data)

        # Check for signals
        signal_created = False
        
        # --- Mega-brand partnership ---
        is_deal, deal_tokens, brand = self.check_megabrand_deal(data)
        if is_deal and deal_tokens:
            approved, reason = await self.ai_verifier.verify(
                data=data,
                tokens=deal_tokens,
                alert_type="partnership",
                company=brand,
            )

            if not approved:
                reason_text = reason or "Model rejected"
                tokens_str = ', '.join(deal_tokens)
                logger.info("AI rejected partnership signal (%s): %s", tokens_str, reason_text)
                print(
                    f"{Colors.YELLOW}🤖 [{self.name}] AI rejected partnership signal {tokens_str}: {reason_text}{Colors.RESET}"
                )
            else:
                signal = Signal(
                    source="INVESTMENT",
                    tokens=deal_tokens,
                    announcement=data.get('title', ''),
                    alert_type="partnership",   # partnership olarak işaretle
                    company=brand,
                    url=data.get('url')
                )
                await self.hub.process_signal(signal)
                signal_created = True

        # Check Upbit
        if ENABLE_UPBIT_LISTINGS:
            is_upbit, upbit_tokens, notice_id = self.check_upbit_listing(data)
            if is_upbit and upbit_tokens:
                signal = Signal(
                    source="UPBIT",
                    tokens=upbit_tokens,
                    announcement=data.get('title', ''),
                    listing_type="KRW",
                    notice_id=notice_id,
                    url=data.get('url')
                )
                await self.hub.process_signal(signal)
                signal_created = True

        is_binance_seed, binance_tokens = self.check_binance_seed_hodler(data)
        if is_binance_seed and binance_tokens:
            signal = Signal(
                source="BINANCE_ANNOUNCEMENTS",
                tokens=binance_tokens,
                announcement=data.get('title', ''),
                listing_type="SEED_HODLER",
                url=data.get('url')
            )
            await self.hub.process_signal(signal)
            signal_created = True
        
        # Check Bithumb
        if ENABLE_BITHUMB_LISTINGS:
            is_bithumb, bithumb_tokens = self.check_bithumb_listing(data)
            if is_bithumb and bithumb_tokens:
                signal = Signal(
                    source="BITHUMB",
                    tokens=bithumb_tokens,
                    announcement=data.get('title', ''),
                    listing_type="KRW",
                    url=data.get('url')
                )
                await self.hub.process_signal(signal)
                signal_created = True
        
        # Check investment
        is_inv, inv_tokens, amount = self.check_investment(data)
        if is_inv and inv_tokens:
            approved, reason = await self.ai_verifier.verify(
                data=data,
                tokens=inv_tokens,
                alert_type="investment",
                amount=amount,
            )

            if not approved:
                reason_text = reason or "Model rejected"
                tokens_str = ', '.join(inv_tokens)
                logger.info("AI rejected investment signal (%s): %s", tokens_str, reason_text)
                print(
                    f"{Colors.YELLOW}🤖 [{self.name}] AI rejected investment signal {tokens_str}: {reason_text}{Colors.RESET}"
                )
            else:
                signal = Signal(
                    source="INVESTMENT",
                    tokens=inv_tokens,
                    announcement=data.get('title', ''),
                    alert_type="investment",
                    amount_millions=amount,
                    amount_formatted=f"${amount:.0f}M",
                    url=data.get('url')
                )
                await self.hub.process_signal(signal)
                signal_created = True
        
        if not signal_created:
            self.hub.stats["news_filtered"] += 1

    async def _message_worker(self, worker_id: int):
        while True:
            data = await self.message_queue.get()
            try:
                if data is None:
                    return
                await self.process_message(data)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[%s] Worker %s error: %s", self.name, worker_id, exc)
            finally:
                self.hub.stats["message_queue_processed"] += 1
                self.message_queue.task_done()

    async def _ensure_workers(self):
        alive_tasks = [task for task in self.worker_tasks if not task.done()]
        self.worker_tasks = alive_tasks
        missing = self.message_workers - len(self.worker_tasks)
        for index in range(missing):
            worker_id = len(self.worker_tasks) + index + 1
            task = self._spawn_background_task(
                self._message_worker(worker_id),
                f"message-worker-{worker_id}",
            )
            self.worker_tasks.append(task)

    async def _stop_workers(self):
        alive_tasks = [task for task in self.worker_tasks if not task.done()]
        if not alive_tasks:
            self.worker_tasks = []
            return

        for _ in alive_tasks:
            try:
                self.message_queue.put_nowait(None)
            except asyncio.QueueFull:
                await self.message_queue.put(None)

        await asyncio.gather(*alive_tasks, return_exceptions=True)
        self.worker_tasks = []

    async def _submit_message(self, data: dict) -> bool:
        try:
            self.message_queue.put_nowait(data)
            self.hub.stats["message_queue_enqueued"] += 1
            return True
        except asyncio.QueueFull:
            if TREE_NEWS_MESSAGE_QUEUE_PUT_TIMEOUT > 0:
                try:
                    await asyncio.wait_for(
                        self.message_queue.put(data),
                        timeout=TREE_NEWS_MESSAGE_QUEUE_PUT_TIMEOUT,
                    )
                    self.hub.stats["message_queue_enqueued"] += 1
                    return True
                except (asyncio.QueueFull, asyncio.TimeoutError):
                    pass

        self.hub.stats["message_queue_drops"] += 1
        logger.warning(
            "[%s] Message queue full, dropping inbound Tree News payload",
            self.name,
        )
        return False
    
    async def connect(self):
        """Connect to Tree News WebSocket"""
        async with self._connect_lock:
            if not self.running:
                return

            if self.ws and not self.ws.closed:
                logger.debug("[%s] Connect skipped; WebSocket already active", self.name)
                return

            if not self.session or self.session.closed:
                connector = aiohttp.TCPConnector(limit=128)
                timeout = ClientTimeout(total=30, sock_read=WS_RECEIVE_TIMEOUT)
                self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            await self._ensure_workers()

            try:
                logger.info(f"[{self.name}] Connecting to {self.url}")

                self.ws = await self.session.ws_connect(
                    self.url,
                    heartbeat=WS_HEARTBEAT,
                    timeout=ClientWSTimeout(
                        ws_receive=WS_RECEIVE_TIMEOUT,
                        ws_close=WS_RECEIVE_TIMEOUT
                    )
                )

                await self.ws.send_str(f"login {API_KEY}")
                logger.info(f"[{self.name}] Connected and logging in")

                self.reconnect_delay = RECONNECT_BASE_DELAY

                await self.message_loop()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[{self.name}] Connection error: {e}")
                await self.handle_disconnect()
    
    async def message_loop(self):
        """Process incoming messages"""
        try:
            async for msg in self.ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._submit_message(data)
                    except json.JSONDecodeError:
                        if msg.data and len(msg.data) > 2:
                            timestamp = self.get_detailed_timestamp()
                            if DEBUG_MODE:
                                print(f"\n{Colors.GRAY}💬 [{self.name}] [{timestamp}] Plain text: {msg.data}{Colors.RESET}")
                    except Exception as e:
                        logger.error(f"[{self.name}] Message processing error: {e}")
                        
                elif msg.type == WSMsgType.ERROR:
                    logger.error(f"[{self.name}] WebSocket error: {self.ws.exception()}")
                    break
                    
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    logger.warning(f"[{self.name}] WebSocket closed")
                    break
                    
        except Exception as e:
            logger.error(f"[{self.name}] Message loop error: {e}")
        
        await self.handle_disconnect()
    
    async def handle_disconnect(self):
        """Handle disconnection and schedule reconnect"""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as exc:
                logger.debug("[%s] WebSocket close error during disconnect: %s", self.name, exc)
            self.ws = None
        
        self.logged_in = False
        
        if not self.running:
            return

        if self._reconnect_task and not self._reconnect_task.done():
            logger.debug("[%s] Reconnect already scheduled", self.name)
            return
        
        jitter_factor = random.uniform(1 - RECONNECT_JITTER, 1 + RECONNECT_JITTER)
        wait_time = min(self.reconnect_delay * jitter_factor, RECONNECT_MAX_DELAY)
        
        logger.info(f"[{self.name}] Reconnecting in {wait_time:.1f}s")
        self._spawn_background_task(
            self._reconnect_after_delay(wait_time),
            f"reconnect-{wait_time:.1f}s",
            is_reconnect=True,
        )

    async def _reconnect_after_delay(self, wait_time: float) -> None:
        await asyncio.sleep(wait_time)
        self.reconnect_delay = min(self.reconnect_delay * 2, RECONNECT_MAX_DELAY)
        if self.running:
            await self.connect()
    
    async def run(self):
        """Run the client"""
        await self.connect()
    
    async def stop(self):
        """Stop the client"""
        self.running = False
        reconnect_task = self._reconnect_task
        if reconnect_task and not reconnect_task.done():
            reconnect_task.cancel()
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
        await self._stop_workers()
        background = [
            task for task in self._background_tasks
            if task is not asyncio.current_task() and task not in self.worker_tasks
        ]
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        self._reconnect_task = None
        await self.ai_verifier.close()

# ==========================
# WebSocket Server
# ==========================
