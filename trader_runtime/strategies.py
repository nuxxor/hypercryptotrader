from .common import *
from .position import Position


class TradingStrategy:
    """Temel strateji sınıfı - tüm stratejilerin temel sınıfı"""
    def __init__(self, name: str):
        self.name = name
        
    async def evaluate(self, position: Position, market_conditions: dict = None) -> Tuple[bool, str, float, float]:
        """
        Bir pozisyonu değerlendir ve satış kararı ver
        Returns: (sell_now, reason, percentage_to_sell, target_profit)
        """
        # Alt sınıflar bu metodu uygulamalı
        raise NotImplementedError("Evaluate method must be implemented by strategy subclasses")


class BinanceListingStrategy(TradingStrategy):
    """Binance listeleme stratejisi - 3 dakikalık bekleme sonrası trailing stop"""

    TRAILING_STATE_KEY = "binance_trailing"

    def __init__(self):
        super().__init__("Binance Listing Strategy")

        # Market cap eşiği (bilgi amaçlı)
        self.market_cap_threshold = 100_000_000  # $100M

        # Risk yönetimi guard'ları
        self.mae_guard_percent = -7.0  # İlk dakikalarda -%7 kayıp görülürse kes
        self.break_even_activation_percent = 2.5  # Daha geniş bir marj ile break-even arm
        self.break_even_buffer_percent = 0.15     # Break-even stop'u entry'nin hafif üstüne koy
        self.break_even_delay_seconds = 45.0      # Break-even öncesi minimum bekleme

        # Kademeli kâr alma
        self.first_tp_percent = 20.0
        self.first_tp_share = 0.35

        # Trailing stop parametreleri
        self.trailing_activation_seconds = 180        # Spike yoksa 3 dakika bekle
        self.trailing_update_interval_seconds = 5     # Stop güncelleme frekansı
        self.trailing_config = {
            "GEM": {"base": 8.0, "after_tp": 7.0, "spike_cap": 6.5, "max": 10.0},
            "LARGE": {"base": 5.0, "after_tp": 4.5, "spike_cap": 4.5, "max": 6.0},
        }
        self.trailing_min_floor = 4.0

        # Spike moduna erken geçiş
        self.spike_activation_profit_percent = 15.0
        self.spike_min_age_seconds = 20.0

    async def evaluate(self, position: Position, market_conditions: dict = None) -> Tuple[bool, str, float, float]:
        """Binance stratejisi - erken koruma, kademeli kâr ve dinamik trailing"""

        profit_pct = position.current_profit_pct
        age_seconds = position.age_seconds
        current_price = position.current_price or position.entry_price
        market_cap = getattr(position, "market_cap", None)

        if market_cap is None:
            market_cap_info = "Unknown (GEM)"
            category = "GEM"
        elif market_cap < self.market_cap_threshold:
            market_cap_info = f"${market_cap/1_000_000:.1f}M (GEM)"
            category = "GEM"
        else:
            market_cap_info = (
                f"${market_cap/1_000_000:.0f}M" if market_cap < 1_000_000_000 else f"${market_cap/1_000_000_000:.1f}B"
            )
            category = "LARGE"

        if not hasattr(position, "_binance_strategy_logged"):
            trailing_base = self.trailing_config.get(category, self.trailing_config["GEM"])["base"]
            logger.info(
                "Binance Listing Strategy v2 for %s: Market Cap=%s, category=%s, "
                "TP %.0f%%/%.0f%%, MAE %.1f%%, break-even %.1f%% @ %.2fs, trailing base %.1f%%",
                position.symbol,
                market_cap_info,
                category,
                self.first_tp_percent,
                self.first_tp_share * 100,
                self.mae_guard_percent,
                self.break_even_activation_percent,
                self.break_even_delay_seconds,
                trailing_base,
            )
            position._binance_strategy_logged = True

        state = position.strategy_state.setdefault(
            self.TRAILING_STATE_KEY,
            {
                "trailing_active": False,
                "activation_time": None,
                "last_update": None,
                "highest_price": position.entry_price,
                "trailing_price": None,
                "trailing_offset": None,
                "break_even_armed": False,
                "break_even_price": None,
                "break_even_logged": False,
                "partial_taken": False,
                "partial_logged": False,
                "category": category,
                "spike_activated": False,
            },
        )
        state["category"] = category

        now = datetime.now()
        observed_high = max(
            state.get("highest_price") or position.entry_price,
            position.highest_price,
            current_price or 0.0,
        )
        state["highest_price"] = observed_high

        # 1. MAE guard – ilk dakikalarda sert düşüşte çık
        if (
            not state.get("partial_taken")
            and profit_pct <= self.mae_guard_percent
        ):
            return True, (
                f"MAE guard {self.mae_guard_percent:.1f}% breached "
                f"(current {profit_pct:.2f}%) - cutting trade"
            ), 1.0, profit_pct

        # 2. Break-even koruması: anlamlı hareket ve minimum süre sonrası
        if (
            not state["break_even_armed"]
            and position.entry_price > 0
            and profit_pct >= self.break_even_activation_percent
            and age_seconds >= self.break_even_delay_seconds
        ):
            state["break_even_armed"] = True
            state["break_even_price"] = position.entry_price * (1 + self.break_even_buffer_percent / 100)
            if not state["break_even_logged"]:
                logger.info(
                    "Break-even guard armed for %s @ %.8f (buffer %.2f%%)",
                    position.symbol,
                    state["break_even_price"],
                    self.break_even_buffer_percent,
                )
                state["break_even_logged"] = True

        if state["break_even_armed"] and state.get("break_even_price") and current_price <= state["break_even_price"]:
            return True, (
                f"Break-even guard triggered ({current_price:.8f} <= {state['break_even_price']:.8f})"
            ), 1.0, profit_pct

        # 3. Kısmi kâr al
        if (
            not state["partial_taken"]
            and profit_pct >= self.first_tp_percent
            and position.remaining_quantity > 0
        ):
            state["partial_taken"] = True
            message = (
                f"TP {self.first_tp_percent:.0f}% reached - scaling {self.first_tp_share*100:.0f}% "
                f"(profit {profit_pct:.2f}%)"
            )
            return True, message, self.first_tp_share, profit_pct

        # 4. Spike modunu kontrol et (erken trailing için)
        if (
            not state["trailing_active"]
            and not state.get("spike_activated")
            and profit_pct >= self.spike_activation_profit_percent
            and age_seconds >= self.spike_min_age_seconds
        ):
            state["spike_activated"] = True
            logger.info(
                "Spike activation detected for %s (profit %.2f%% @ %.0fs)",
                position.symbol,
                profit_pct,
                age_seconds,
            )

        # 5. Trailing aktivasyonu
        trailing_ready = state.get("spike_activated") or age_seconds >= self.trailing_activation_seconds

        if not state["trailing_active"]:
            if not trailing_ready:
                remaining = max(self.trailing_activation_seconds - age_seconds, 0.0)
                status = (
                    f"Awaiting trailing ({remaining:.0f}s) | profit {profit_pct:.2f}% "
                    f"| TP{self.first_tp_percent:.0f} "
                    f"{'done' if state['partial_taken'] else 'pending'}"
                )
                return False, status, 0.0, 0.0

            if current_price <= 0:
                return False, "Awaiting valid price to arm trailing stop", 0.0, 0.0

            offset = self._determine_trailing_offset(state)
            trailing_price = current_price * (1 - offset / 100)
            state.update(
                {
                    "trailing_active": True,
                    "activation_time": now,
                    "last_update": now,
                    "highest_price": current_price,
                    "trailing_price": trailing_price,
                    "trailing_offset": offset,
                }
            )
            position.trailing_stop_activated = True
            position.trailing_stop_price = trailing_price
            position.trailing_stop_percent = offset

            mode = " (spike)" if state.get("spike_activated") and age_seconds < self.trailing_activation_seconds else ""
            status = (
                f"Trailing armed{mode} @ {trailing_price:.8f} (-{offset:.1f}%) | profit {profit_pct:.2f}%"
            )
            logger.info(
                "Activated %.1f%% trailing for %s (category %s, price %.8f)",
                offset,
                position.symbol,
                category,
                current_price,
            )
            return False, status, 0.0, 0.0

        # 6. Trailing aktif – güncelle ve tetiklenme kontrolü
        offset = self._determine_trailing_offset(state)
        state["trailing_offset"] = offset

        highest_price = state.get("highest_price") or current_price
        if current_price > highest_price:
            highest_price = current_price
            state["highest_price"] = highest_price

        desired_trailing = highest_price * (1 - offset / 100)
        trailing_price = state.get("trailing_price") or 0.0
        if desired_trailing > trailing_price:
            state["trailing_price"] = desired_trailing
            state["last_update"] = now
            position.trailing_stop_price = desired_trailing
            position.trailing_stop_percent = offset
            logger.debug(
                "Trailing tightened for %s -> %.8f (offset %.1f%%, high %.8f)",
                position.symbol,
                desired_trailing,
                offset,
                highest_price,
            )
        else:
            last_update = state.get("last_update") or now
            if (now - last_update).total_seconds() >= self.trailing_update_interval_seconds:
                state["last_update"] = now

        trailing_price = state.get("trailing_price") or 0.0
        if trailing_price > 0 and current_price <= trailing_price:
            return True, (
                f"Trailing stop hit at {current_price:.8f} (stop {trailing_price:.8f}, offset {offset:.1f}%)"
            ), 1.0, profit_pct

        status = (
            f"Trailing @ {trailing_price:.8f} (-{offset:.1f}%) | profit {profit_pct:.2f}% "
            f"| high {highest_price:.8f}"
        )
        return False, status, 0.0, 0.0

    def _determine_trailing_offset(self, state: dict) -> float:
        category = state.get("category", "GEM")
        config = self.trailing_config.get(category, self.trailing_config["GEM"])
        offset = config.get("base", 8.0)
        if state.get("partial_taken"):
            offset = config.get("after_tp", offset)
        if state.get("spike_activated"):
            offset = min(offset, config.get("spike_cap", offset))
        offset = max(self.trailing_min_floor, min(offset, config.get("max", offset)))
        return offset


class AggressiveMomentumBinanceStrategy(TradingStrategy):
    """
    Binance listelemeleri için hibrit kademeli satış + adaptif trailing + momentum çıkışı
    stratejisi. Geniş volatiliteyi tolere ederken erken dump'larda sermayeyi korumayı hedefler.
    """

    STATE_KEY = "binance_aggressive_momentum"

    def __init__(self):
        super().__init__("Binance Aggressive Momentum Strategy")

        # Kademeli satış seviyeleri (% cinsinden)
        self.tp_levels = [20.0, 45.0, 90.0]
        self.tp_shares = [0.30, 0.22, 0.18]
        self.runner_share = 0.30

        # Slippage varsayımları (yüzde cinsinden)
        self.slippage_normal_pct = 0.15
        self.slippage_low_volume_multiplier = 2.0
        self.slippage_spike_pct = 0.40
        self.slippage_spike_low_pct = 0.80

        # Koruyucu parametreler
        self.mae_guard_pct = -7.0           # TP1 öncesi maksimum tolerans
        self.break_even_activation_pct = 0.8
        self.break_even_buffer_pct = 0.8
        self.break_even_fee_pct = 0.1
        self.break_even_min_age_seconds = 15.0

        # Trailing parametreleri
        self.trailing_multiplier = 1.15
        self.trailing_pct_min = 0.08
        self.trailing_pct_max = 0.24
        self.tight_trailing_max = 0.12
        self.fallback_drawdown = 0.12
        self.trailing_cooldown_seconds = 120
        self.max_drawdowns_tracked = 20

        # Spike modu (erken trailing aktivasyonu) eşikleri
        self.spike_price_change = 0.15
        self.spike_time_limit = 30
        self.spike_volume_ratio = 6.0
        self.spike_volume_time_limit = 45

        # Momentum çıkışı parametreleri
        self.momentum_exit_threshold = 1.0
        self.momentum_volume_drop_threshold = 0.45
        self.momentum_rsi_threshold = 35.0
        self.momentum_vwap_ratio = 0.95
        self.momentum_red_candle_threshold = 3
        self.momentum_min_age_seconds = 60

        # Runner yönetimi
        self.runner_max_seconds = 420

    def _init_state(self, position: Position) -> dict:
        entry_price = position.entry_price if position.entry_price > 0 else 1.0
        current_price = position.current_price if position.current_price > 0 else entry_price
        return {
            "initialized": False,
            "tp_done": [False] * len(self.tp_levels),
            "runner_active": False,
            "runner_start_age": None,
            "first_scale_out_age": None,
            "spike_mode": False,
            "tight_trailing": False,
            "break_even_armed": False,
            "break_even_price": None,
            "highest_price": current_price,
            "trailing_price": None,
            "drawdowns": [],
            "current_peak": current_price,
            "current_trough": current_price,
            "max_profit": 0.0,
            "max_effective_profit": 0.0,
            "last_status": None,
        }

    @staticmethod
    def _price_change(entry: float, current: float) -> float:
        if entry <= 0:
            return 0.0
        return (current / entry) - 1.0

    def _volume_ratio(self, position: Position) -> float:
        volumes = [vol for _, vol in position.volume_history[-10:] if vol]
        if len(volumes) < 2:
            return 1.0
        avg_volume = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
        current_volume = volumes[-1]
        if avg_volume <= 0:
            return 1.0
        return current_volume / avg_volume

    def _estimate_slippage_pct(self, state: dict, volume_ratio: float) -> float:
        if state.get("spike_mode"):
            return self.slippage_spike_low_pct if volume_ratio < 0.5 else self.slippage_spike_pct
        base = self.slippage_normal_pct
        if volume_ratio < 0.5:
            base *= self.slippage_low_volume_multiplier
        return base

    def _effective_profit(self, profit_pct: float, slippage_pct: float) -> float:
        return profit_pct - slippage_pct

    def _is_spike(self, position: Position, state: dict, age_seconds: float) -> bool:
        price_change = self._price_change(position.entry_price, position.current_price)
        if age_seconds <= self.spike_time_limit and price_change >= self.spike_price_change:
            return True

        if (
            age_seconds <= self.spike_volume_time_limit
            and len(position.volume_history) >= 10
        ):
            recent_volumes = [vol for _, vol in position.volume_history[-10:]]
            if recent_volumes:
                avg_volume = sum(recent_volumes[:-1]) / max(len(recent_volumes) - 1, 1)
                last_volume = recent_volumes[-1]
                if avg_volume > 0 and last_volume >= self.spike_volume_ratio * avg_volume:
                    return True
        return False

    def _update_drawdowns(self, state: dict, price: float) -> None:
        peak = state["current_peak"]
        trough = state["current_trough"]

        if price > peak:
            if peak > 0 and trough < peak:
                drawdown = (peak - trough) / peak
                if drawdown > 0:
                    state["drawdowns"].append(drawdown)
                    if len(state["drawdowns"]) > self.max_drawdowns_tracked:
                        state["drawdowns"].pop(0)
            state["current_peak"] = price
            state["current_trough"] = price
        elif price < trough:
            state["current_trough"] = price

    def _compute_trailing_pct(self, state: dict) -> float:
        drawdowns = state.get("drawdowns", [])
        q90 = self.fallback_drawdown
        if len(drawdowns) >= 3:
            try:
                q90 = float(np.percentile(drawdowns, 90))
            except (IndexError, ValueError):
                q90 = max(drawdowns)
        elif drawdowns:
            q90 = max(drawdowns)

        trailing = self.trailing_multiplier * q90
        trailing = max(self.trailing_pct_min, min(trailing, self.trailing_pct_max))
        if state.get("tight_trailing"):
            trailing = min(trailing, self.tight_trailing_max)
            trailing = max(self.trailing_pct_min, trailing)
        return trailing

    def _activate_trailing(self, position: Position, state: dict) -> Optional[str]:
        if position.current_price <= 0:
            return None

        trailing_pct = self._compute_trailing_pct(state)
        trailing_price = position.current_price * (1 - trailing_pct)
        state["trailing_price"] = trailing_price
        state["highest_price"] = max(state.get("highest_price", 0.0), position.current_price)
        position.trailing_stop_activated = True
        position.trailing_stop_price = trailing_price
        position.trailing_stop_percent = trailing_pct * 100
        return (
            f"Trailing armed @{trailing_price:.8f} (offset {trailing_pct*100:.1f}%) | "
            f"profit {position.current_profit_pct:.2f}%"
        )

    def _update_trailing(self, position: Position, state: dict) -> Optional[str]:
        if state.get("trailing_price") is None or position.current_price <= 0:
            return None

        trailing_pct = self._compute_trailing_pct(state)
        highest_price = state.get("highest_price") or position.current_price
        if position.current_price > highest_price:
            highest_price = position.current_price
            state["highest_price"] = highest_price

        desired_trailing = highest_price * (1 - trailing_pct)
        if desired_trailing > state.get("trailing_price", 0):
            state["trailing_price"] = desired_trailing
            position.trailing_stop_price = desired_trailing
            position.trailing_stop_percent = trailing_pct * 100
            return (
                f"Trailing updated @{desired_trailing:.8f} (offset {trailing_pct*100:.1f}%) | "
                f"high {highest_price:.8f}"
            )
        return None

    def _calculate_vwap(self, position: Position, window: int = 10) -> Optional[float]:
        if not position.price_history or not position.volume_history:
            return None
        prices = [price for _, price in position.price_history[-window:]]
        volumes = [vol for _, vol in position.volume_history[-window:]]
        if not prices or not volumes:
            return None
        length = min(len(prices), len(volumes))
        prices = prices[-length:]
        volumes = volumes[-length:]
        total_volume = sum(volumes)
        if total_volume <= 0:
            return None
        return sum(p * v for p, v in zip(prices, volumes)) / total_volume

    def _momentum_score(self, position: Position) -> float:
        score = 0.0

        if len(position.volume_history) >= 10:
            recent_volumes = [vol for _, vol in position.volume_history[-10:]]
            current_volume = recent_volumes[-1]
            avg_volume = sum(recent_volumes[:-1]) / max(len(recent_volumes) - 1, 1)
            if avg_volume > 0:
                volume_drop = (avg_volume - current_volume) / avg_volume
                if volume_drop > self.momentum_volume_drop_threshold:
                    score += 1.0

        rsi = position.calculate_rsi(period=14)
        if rsi is not None and rsi < self.momentum_rsi_threshold:
            score += 0.5

        vwap = self._calculate_vwap(position)
        if vwap is not None and vwap > 0:
            price_ratio = position.current_price / vwap
            if price_ratio < self.momentum_vwap_ratio:
                score += 0.5

        if position.is_momentum_lost():
            score += 0.5

        return score

    def _should_activate_trailing(self, state: dict, age_seconds: float) -> bool:
        if state.get("trailing_price") is not None:
            return False
        if state.get("spike_mode"):
            return True
        return age_seconds >= self.trailing_cooldown_seconds

    async def evaluate(
        self,
        position: Position,
        market_conditions: dict = None,
    ) -> Tuple[bool, str, float, float]:
        profit_pct = position.current_profit_pct
        age_seconds = position.age_seconds
        current_price = position.current_price if position.current_price > 0 else position.entry_price

        state = position.strategy_state.setdefault(
            self.STATE_KEY,
            self._init_state(position),
        )

        if not state["initialized"]:
            logger.info(
                "Aggressive Binance strategy activated for %s | tp_levels=%s runner=%s%% "
                "trailing_cooldown=%ss momentum_exit=%.1f break-even>=%.2f%% @ %ds",
                position.symbol,
                self.tp_levels,
                int(self.runner_share * 100),
                self.trailing_cooldown_seconds,
                self.momentum_exit_threshold,
                self.break_even_activation_pct,
                int(self.break_even_min_age_seconds),
            )
            state["initialized"] = True

        if current_price <= 0:
            return False, "Waiting for valid price data", 0.0, 0.0

        volume_ratio = self._volume_ratio(position)
        slippage_pct = self._estimate_slippage_pct(state, volume_ratio)
        effective_profit = self._effective_profit(profit_pct, slippage_pct)
        state["last_slippage_pct"] = slippage_pct

        # Spike modu kontrolü
        if not state.get("spike_mode") and self._is_spike(position, state, age_seconds):
            state["spike_mode"] = True
            state["tight_trailing"] = True
            logger.info("Spike mode detected for %s (age %.0fs)", position.symbol, age_seconds)

        self._update_drawdowns(state, current_price)

        # En yüksek fiyat ve kâr takibi
        state["highest_price"] = max(state.get("highest_price", 0.0), current_price)
        state["max_profit"] = max(state.get("max_profit", 0.0), profit_pct)
        state["max_effective_profit"] = max(
            state.get("max_effective_profit", 0.0),
            effective_profit,
        )

        # MAE guard
        if not state["tp_done"][0] and profit_pct <= self.mae_guard_pct:
            return True, (
                f"MAE guard {self.mae_guard_pct:.1f}% breached (current {profit_pct:.2f}%) - exiting"
            ), 1.0, profit_pct

        # Break-even guard
        if (
            not state["break_even_armed"]
            and effective_profit >= self.break_even_activation_pct
            and position.entry_price > 0
            and age_seconds >= self.break_even_min_age_seconds
        ):
            state["break_even_armed"] = True
            state["break_even_price"] = position.entry_price * (1 - self.break_even_fee_pct / 100)
            logger.info(
                "Break-even guard armed for %s @ %.8f (fee buffer %.2f%%, age %.0fs)",
                position.symbol,
                state["break_even_price"],
                self.break_even_fee_pct,
                age_seconds,
            )

        if state.get("break_even_armed") and state.get("break_even_price"):
            if current_price <= state["break_even_price"]:
                return True, (
                    f"Break-even guard triggered ({current_price:.8f} <= {state['break_even_price']:.8f})"
                ), 1.0, profit_pct

        # Kademeli satışlar
        for idx, level in enumerate(self.tp_levels):
            if not state["tp_done"][idx] and effective_profit >= level:
                state["tp_done"][idx] = True
                state["first_scale_out_age"] = state["first_scale_out_age"] or age_seconds
                share = self.tp_shares[idx]
                message = f"TP{idx + 1} {level:.0f}% reached - scaling {share*100:.0f}%"

                if idx == len(self.tp_levels) - 1:
                    state["runner_active"] = True
                    state["tight_trailing"] = False
                    state["runner_start_age"] = age_seconds
                    message += " | runner activated"
                return True, message, share, profit_pct

        trailing_message = None
        if self._should_activate_trailing(state, age_seconds):
            trailing_message = self._activate_trailing(position, state)
        else:
            trailing_message = self._update_trailing(position, state)

        trailing_price = state.get("trailing_price")
        if trailing_price and current_price <= trailing_price:
            return True, (
                f"Adaptive trailing hit @ {current_price:.8f} (stop {trailing_price:.8f})"
            ), 1.0, profit_pct

        # Runner maksimum süre kontrolü
        if state.get("runner_active") and state.get("runner_start_age") is not None:
            runner_age = age_seconds - state["runner_start_age"]
            if runner_age >= self.runner_max_seconds:
                return True, (
                    f"Runner max hold {self.runner_max_seconds}s exceeded (age {runner_age:.0f}s)"
                ), 1.0, profit_pct

        # Momentum çıkışı
        if age_seconds >= self.momentum_min_age_seconds:
            score = self._momentum_score(position)
            first_scale_age = state.get("first_scale_out_age")
            scale_age_ok = (
                first_scale_age is None
                or age_seconds - first_scale_age >= self.momentum_min_age_seconds / 2
            )
            if score >= self.momentum_exit_threshold and scale_age_ok:
                return True, (
                    f"Momentum score {score:.1f} reached threshold ({self.momentum_exit_threshold:.1f})"
                ), 1.0, profit_pct

        next_target = None
        for idx, done in enumerate(state["tp_done"]):
            if not done:
                next_target = self.tp_levels[idx]
                break
        if next_target is None:
            next_target = max(state.get("max_effective_profit", 0.0), 0.0)

        status_parts = []
        if trailing_message:
            status_parts.append(trailing_message)
        else:
            status_parts.append(f"Holding | profit {profit_pct:.2f}%")
        if next_target:
            status_parts.append(f"next target {next_target:.1f}%")
        status = " | ".join(status_parts)
        state["last_status"] = status
        return False, status, 0.0, next_target

        status = (
            f"Trailing active | stop {trailing_price:.8f} | high {state.get('highest_price', 0):.8f} | "
            f"price {current_price:.8f} ({profit_pct:.2f}%)"
        )
        return False, status, 0.0, 0.0


class BinanceAlphaListingStrategy(TradingStrategy):
    """Binance Alpha sinyalleri için hızlı TP merdiveni ve runner yönetimi."""

    STATE_KEY = "binance_alpha"

    def __init__(self):
        super().__init__("Binance Alpha Quick TP")
        self.tp1_threshold = 15.0
        self.tp2_threshold = 35.0
        self.tp3_threshold = 80.0

        self.tp1_percentage = 0.20
        self.tp2_percentage = 0.30
        self.tp3_percentage = 0.20

        self.initial_stop_pct = -7.0
        self.post_tp1_stop_pct = 0.0

        self.runner_trail_offset_pct = 12.0
        self.momentum_window_seconds = 120
        self.momentum_profit_threshold = 10.0

    def _next_target(self, state) -> float:
        if not state["tp1_done"]:
            return self.tp1_threshold
        if not state["tp2_done"]:
            return self.tp2_threshold
        if not state["tp3_done"]:
            return self.tp3_threshold
        return state.get("highest_profit", 0.0)

    async def evaluate(self, position: Position, market_conditions: dict = None) -> Tuple[bool, str, float, float]:
        profit_pct = position.current_profit_pct
        age_seconds = position.age_seconds

        state = position.strategy_state.setdefault(
            self.STATE_KEY,
            {
                "tp1_done": False,
                "tp2_done": False,
                "tp3_done": False,
                "runner_active": False,
                "stop_loss_pct": self.initial_stop_pct,
                "highest_profit": profit_pct,
                "highest_price": position.current_price if position.current_price > 0 else None,
                "trailing_price": None,
                "momentum_locked": False,
            },
        )

        if profit_pct > state["highest_profit"]:
            state["highest_profit"] = profit_pct

        current_price = position.current_price
        if current_price > 0:
            highest_price = state.get("highest_price")
            if highest_price is None or current_price > highest_price:
                state["highest_price"] = current_price

        stop_level = state.get("stop_loss_pct", self.initial_stop_pct)
        if profit_pct <= stop_level:
            return True, f"Hard stop hit at {stop_level:.2f}% (current {profit_pct:.2f}%)", 1.0, profit_pct

        if (
            not state["momentum_locked"]
            and age_seconds >= self.momentum_window_seconds
            and state.get("highest_profit", 0.0) < self.momentum_profit_threshold
        ):
            state["momentum_locked"] = True
            return True, (
                f"No momentum in first {self.momentum_window_seconds}s "
                f"(max {state.get('highest_profit', 0.0):.2f}%), flattening"
            ), 1.0, profit_pct

        if not state["tp1_done"] and profit_pct >= self.tp1_threshold:
            state["tp1_done"] = True
            state["stop_loss_pct"] = self.post_tp1_stop_pct
            return True, (
                f"TP1 {self.tp1_threshold}% reached - scaling 20%, stop to breakeven"
            ), self.tp1_percentage, profit_pct

        if state["tp1_done"] and not state["tp2_done"] and profit_pct >= self.tp2_threshold:
            state["tp2_done"] = True
            return True, f"TP2 {self.tp2_threshold}% reached - locking additional 30%", self.tp2_percentage, profit_pct

        if state["tp2_done"] and not state["tp3_done"] and profit_pct >= self.tp3_threshold:
            state["tp3_done"] = True
            state["runner_active"] = True
            if position.current_price > 0:
                state["highest_price"] = position.current_price
                state["trailing_price"] = position.current_price * (1 - self.runner_trail_offset_pct / 100)
            return True, (
                f"TP3 {self.tp3_threshold}% reached - scaling 20%, runner activated"
            ), self.tp3_percentage, profit_pct

        if state.get("runner_active") and position.current_price > 0:
            highest_price = state.get("highest_price") or position.current_price
            if position.current_price > highest_price:
                highest_price = position.current_price
                state["highest_price"] = highest_price

            trailing_price = highest_price * (1 - self.runner_trail_offset_pct / 100)
            previous_trailing = state.get("trailing_price")
            if previous_trailing is None or trailing_price > previous_trailing:
                state["trailing_price"] = trailing_price
            else:
                trailing_price = previous_trailing

            if position.current_price <= trailing_price:
                return True, (
                    f"Runner trailing stop hit @ {profit_pct:.2f}% (trail {trailing_price:.8f})"
                ), 1.0, profit_pct

            status = (
                f"Runner trailing {trailing_price:.8f} | "
                f"high {state.get('highest_price', 0):.8f} | current {position.current_price:.8f} "
                f"({profit_pct:.2f}%)"
            )
            return False, status, 0.0, state["highest_profit"]

        next_target = self._next_target(state)
        status = (
            f"Holding for next ladder target {next_target:.1f}% | "
            f"profit {profit_pct:.2f}%"
        )
        return False, status, 0.0, next_target


class BithumbListingStrategy(TradingStrategy):
    """Bithumb listeleme stratejisi - ULTRA HIZLI"""
    def __init__(self):
        super().__init__("Bithumb Listing Strategy")
        # 3 dakika içinde tamamen çık ne olursa olsun
        self.exit_duration_minutes = 3.0  # 3 dakikada tamamen çık (daha dengeli)
        
        # %12 kar görürse ne olursa olsun çık (aynı kaldı)
        self.profit_target = 12.0  
        
        # 30 saniye sonunda aldığımız yere stop koy (break-even stop)
        self.stop_loss_delay_minutes = 0.5   # 30 saniye (3 dakikadan 0.5'e düşürdük)
        self.stop_loss_percent = 0.0         # %0 stop loss (tam aldığımız yer, %5'ten 0'a düşürdük)
    
    async def evaluate(self, position: Position, market_conditions: dict = None) -> Tuple[bool, str, float, float]:
        """Bithumb strateji mantığı - Ultra hızlı scalping"""
        profit_pct = position.current_profit_pct
        age_minutes = position.age_minutes
        
        # 1. %12 kar görürse ne olursa olsun çık (aynı kaldı)
        if profit_pct >= self.profit_target:
            return True, f"%{self.profit_target} kar hedefine ulaşıldı, tamamen çık", 1.0, profit_pct
        
        # 2. Break-even stop loss kontrolü - 30 saniye sonra aktif
        if age_minutes >= self.stop_loss_delay_minutes:
            # Tam aldığımız yerin altına düşerse stop loss
            stop_loss_price = position.entry_price  # %0 altı = tam entry price
            if position.current_price <= stop_loss_price:
                return True, f"Break-even stop tetiklendi (30s sonra entry altı)", 1.0, profit_pct
        
        # 3. Süre doldu mu - ne olursa olsun çık
        if age_minutes >= self.exit_duration_minutes:
            return True, f"{self.exit_duration_minutes:.0f} dakika tamamlandı, tamamen çık: {profit_pct:.2f}%", 1.0, profit_pct
        
        # Satmama kararı - süre dolmasını bekle
        time_left = self.exit_duration_minutes - age_minutes
        time_left_seconds = time_left * 60
        
        if time_left_seconds > 30:
            status = f"Bithumb ultra-fast: {time_left_seconds:.0f}s kaldı (hedef: %{self.profit_target})"
        else:
            status = f"⚠️ CLOSING SOON: {time_left_seconds:.0f}s kaldı! (mevcut: {profit_pct:.2f}%)"
        
        return False, status, 0, self.profit_target


class UpbitListingStrategy(TradingStrategy):
    """Upbit KRW listing stratejisi v2 - Kademeli satış, spike capture, 2 saat max hold

    Özellikler:
    - Kademeli satış: %8→%25, %15→%35, %25→%25, %35→%15
    - Spike capture: İlk 20s içinde %30+ → %50 sat
    - Max hold: 2 saat (7200s)
    - TRAILING STOP YOK (volatilite)
    - KAYIP KESME YOK (risk göze alınıyor)
    - MOMENTUM/VOLUME EXIT YOK (%20-30 düşüp toparlanıyor)
    - Market cap filtresi: $80M altı (tree_news.py'de)
    """

    def __init__(self):
        super().__init__("Upbit Listing Strategy v2")

        # Kademeli satış hedefleri (max %30-40 beklenti)
        self.profit_targets = {
            8: 0.25,    # %8 kar → %25 sat
            15: 0.35,   # %15 kar → %35 sat
            25: 0.25,   # %25 kar → %25 sat
            35: 0.15,   # %35 kar → kalan %15 sat
        }

        # Spike ve max hold parametreleri
        self.spike_window_seconds = 20      # İlk 20s içinde %30+ ise spike
        self.spike_profit_threshold = 30    # %30 kar eşiği
        self.spike_sell_percentage = 0.5    # Spike'ta %50 sat
        self.max_hold_seconds = 7200        # Maksimum 2 SAAT tut

    async def evaluate(self, position: Position, market_conditions: dict = None) -> Tuple[bool, str, float, float]:
        """Upbit stratejisi v2 - Kademeli satış ve spike capture"""
        profit_pct = position.current_profit_pct
        age_seconds = position.age_seconds

        state = position.strategy_state.setdefault("upbit_v2", {
            "partial_sells": {},  # profit_level -> satılan oran (float)
            "spike_captured": False,
        })

        # Log strateji başlangıcını
        if not hasattr(position, '_upbit_v2_logged'):
            logger.info(
                f"Upbit Strategy v2 for {position.symbol}: "
                f"targets={list(self.profit_targets.keys())}%, "
                f"spike={self.spike_profit_threshold}%@{self.spike_window_seconds}s, "
                f"max_hold={self.max_hold_seconds}s"
            )
            position._upbit_v2_logged = True

        # Toplam satılan oranı hesapla
        total_sold = sum(state["partial_sells"].values())

        # 1. SPIKE CAPTURE (ilk 20s içinde %30+ kar)
        if (
            age_seconds < self.spike_window_seconds
            and profit_pct >= self.spike_profit_threshold
            and not state["spike_captured"]
        ):
            state["spike_captured"] = True
            sell_pct = min(self.spike_sell_percentage, 1.0 - total_sold)
            if sell_pct > 0.05:
                state["partial_sells"]["spike"] = sell_pct  # Toplama dahil et!
                logger.info(
                    f"🚀 SPIKE CAPTURE {position.symbol}: {profit_pct:.1f}% in {age_seconds:.0f}s → selling {sell_pct*100:.0f}%"
                )
                return True, f"Spike capture at {profit_pct:.1f}% in {age_seconds:.0f}s", sell_pct, profit_pct

        # 2. KADEMELİ SATIŞ
        # Spike sonrası total_sold güncel olsun diye yeniden hesapla
        total_sold = sum(state["partial_sells"].values())
        for target, percentage in sorted(self.profit_targets.items()):
            if profit_pct >= target and target not in state["partial_sells"]:
                remaining = 1.0 - total_sold
                sell_pct = min(percentage, remaining)
                if sell_pct > 0.05:
                    state["partial_sells"][target] = sell_pct  # Float olarak tut!
                    logger.info(
                        f"📊 KADEMELI SATIŞ {position.symbol}: {target}% hedef → {sell_pct*100:.0f}% satılıyor"
                    )
                    return True, f"Profit target {target}% reached", sell_pct, profit_pct

        # 3. MAX HOLD TIME (2 SAAT)
        if age_seconds >= self.max_hold_seconds:
            remaining = 1.0 - total_sold
            if remaining > 0.01:
                logger.info(
                    f"⏰ MAX HOLD {position.symbol}: 2h doldu, kalan {remaining*100:.0f}% satılıyor (profit: {profit_pct:.1f}%)"
                )
                return True, f"Max hold time 2h reached (profit: {profit_pct:.1f}%)", remaining, profit_pct

        # Bekle - sonraki hedefi göster
        unsold_targets = [t for t in self.profit_targets.keys() if t not in state["partial_sells"]]
        next_target = min(unsold_targets) if unsold_targets else 35
        remaining = 1.0 - total_sold
        time_left = self.max_hold_seconds - age_seconds
        time_left_str = f"{time_left/60:.0f}m" if time_left > 60 else f"{time_left:.0f}s"

        status = (
            f"Holding {remaining*100:.0f}% → next {next_target}% "
            f"(now: {profit_pct:.1f}%, {time_left_str} left)"
        )
        return False, status, 0.0, 0.0


