import asyncio
import contextlib
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .common import logger, traceback


class AtomicPersistenceMixin:
    def __init__(self, *args, **kwargs):
        self._processed_signals_dirty = False
        self._processed_signals_flush_task: Optional[asyncio.Task] = None
        try:
            self.processed_signals_flush_delay = max(
                0.0, float(os.getenv("PROCESSED_SIGNALS_FLUSH_DELAY", "0.75"))
            )
        except ValueError:
            self.processed_signals_flush_delay = 0.75
        super().__init__(*args, **kwargs)

    def _setup_results_directory(self):
        results_path = Path(self.results_dir)
        created = not results_path.exists()
        results_path.mkdir(parents=True, exist_ok=True)
        if created:
            logger.info(f"Created trading results directory: {self.results_dir}")

    def _write_json_atomic(self, file_path: str, payload: dict, *, indent: Optional[int] = None) -> None:
        target_path = Path(file_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        fd, temp_path = tempfile.mkstemp(
            prefix=f".{target_path.name}.",
            suffix=".tmp",
            dir=str(target_path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=indent)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, target_path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temp_path)

    def _save_trade_counter(self):
        try:
            self._write_json_atomic(
                self.trade_counter_file,
                {"last_trade_id": self.current_trade_id},
            )
            logger.info(f"Saved trade counter: last_trade_id={self.current_trade_id}")
        except Exception as e:
            logger.error(f"Error saving trade counter: {str(e)}")

    def _save_processed_signals(self):
        try:
            self._prune_processed_signals()
            data = {f"{coin}|{exchange}": ts for (coin, exchange), ts in self.processed_signals.items()}
            self._write_json_atomic(self.processed_signals_file, data, indent=2)
            self._processed_signals_dirty = False
            logger.debug(f"Saved {len(self.processed_signals)} processed signals to cache")
        except Exception as e:
            logger.error(f"Error saving processed signals: {str(e)}")

    def _schedule_processed_signals_flush(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._save_processed_signals()
            return

        task = self._processed_signals_flush_task
        if task and not task.done():
            return

        self._processed_signals_flush_task = loop.create_task(
            self._flush_processed_signals_after_delay()
        )

    async def _flush_processed_signals_after_delay(self) -> None:
        try:
            if self.processed_signals_flush_delay > 0:
                await asyncio.sleep(self.processed_signals_flush_delay)
            if self._processed_signals_dirty:
                self._save_processed_signals()
        except asyncio.CancelledError:
            raise
        finally:
            self._processed_signals_flush_task = None

    async def _flush_processed_signals_before_shutdown(self) -> None:
        task = self._processed_signals_flush_task
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._processed_signals_dirty:
            self._save_processed_signals()

    def _mark_signal_processed(self, coin: str, exchange: str):
        key = (coin.upper(), self._normalize_source(exchange))
        self.processed_signals[key] = time.time()
        self._processed_signals_dirty = True
        if self.processed_signals_flush_delay <= 0:
            self._save_processed_signals()
        else:
            self._schedule_processed_signals_flush()
        logger.info(f"🔒 Marked {coin} from {exchange} as processed - will not open again")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._flush_processed_signals_before_shutdown()
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def _finalize_trade(self, position):
        try:
            final_balance = await self._get_exchange_balance(
                position.exchange, position.account_name, force_refresh=True
            )
            balance_change = final_balance - position.initial_balance

            entry_value = position.entry_price * position.quantity
            exit_value = sum(qty * price for _, qty, price in position.partial_sells)
            profit_usdt = exit_value - entry_value
            profit_pct = ((exit_value / entry_value) - 1) * 100 if entry_value > 0 else 0

            logger.debug(f"Trade {position.trade_id} calculations:")
            logger.debug(f"  Initial balance: ${position.initial_balance:.2f}")
            logger.debug(f"  Final balance: ${final_balance:.2f}")
            logger.debug(f"  Balance change: ${balance_change:.2f}")
            logger.debug(f"  Entry value: ${entry_value:.2f}")
            logger.debug(f"  Exit value: ${exit_value:.2f}")
            logger.debug(f"  Profit USDT: ${profit_usdt:.2f}")
            logger.debug(f"  Profit %: {profit_pct:.2f}%")

            listing_source = position.listing_source.lower() if position.listing_source else "unknown"
            strategy_name = self.strategies[listing_source].name if listing_source in self.strategies else "unknown"

            market_cap_str = "Unknown"
            if position.market_cap is not None:
                if position.market_cap > 100_000_000:
                    market_cap_str = f"High (${position.market_cap/1_000_000:.2f}M)"
                elif position.market_cap > 70_000_000:
                    market_cap_str = f"Medium (${position.market_cap/1_000_000:.2f}M)"
                else:
                    market_cap_str = f"Low (${position.market_cap/1_000_000:.2f}M)"

            trade_result = {
                "trade_id": position.trade_id,
                "symbol": position.symbol,
                "exchange": position.exchange,
                "account_name": position.account_name,
                "listing_source": position.listing_source,
                "strategy": strategy_name,
                "entry_time": position.entry_time.isoformat(),
                "exit_time": datetime.now().isoformat(),
                "entry_price": position.entry_price,
                "highest_price": position.highest_price,
                "quantity": position.quantity,
                "entry_value": entry_value,
                "exit_value": exit_value,
                "profit_usdt": profit_usdt,
                "profit_pct": profit_pct,
                "initial_balance": position.initial_balance,
                "final_balance": final_balance,
                "balance_change": balance_change,
                "market_cap": position.market_cap,
                "market_cap_category": market_cap_str,
                "is_krw_pair": position.is_krw_pair,
                "partial_sells": [
                    {"time": timestamp.isoformat(), "quantity": qty, "price": price}
                    for timestamp, qty, price in position.partial_sells
                ],
            }

            trade_file = os.path.join(self.results_dir, f"{position.trade_id}.json")
            self._write_json_atomic(trade_file, trade_result, indent=2)
            logger.info(f"Trade {position.trade_id} finalized and results saved to {trade_file}")

            summary = f"""
            -----------------------------------------
            TRADE {position.trade_id} COMPLETED
            -----------------------------------------
            Symbol: {position.symbol} ({position.exchange}/{position.account_name})
            Strategy: {strategy_name}
            Market Cap: {market_cap_str}
            Is KRW Pair: {position.is_krw_pair}

            Initial Balance: ${position.initial_balance:.2f}
            Final Balance: ${final_balance:.2f}
            Balance Change: ${balance_change:.2f}

            Position Profit: ${profit_usdt:.2f} ({profit_pct:.2f}%)
            -----------------------------------------
            """
            logger.info(summary)

            if not self._has_open_positions():
                await self._log_all_exchange_balances(context=f"after {position.trade_id}")

        except Exception as e:
            logger.error(f"Error finalizing trade {position.trade_id}: {str(e)}")
            logger.debug(traceback.format_exc())
