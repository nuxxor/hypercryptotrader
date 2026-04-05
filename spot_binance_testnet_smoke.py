import argparse
import asyncio
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

load_dotenv()
os.environ.setdefault("BINANCE_USE_TESTNET", "true")

import spot_binance


class _NoopMexcTrader:
    def __init__(self, test_mode: bool = False, *_, **__):
        self.test_mode = test_mode
        self.fast_mode = False
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    def get_balance(self, force: bool = False) -> float:
        return 0.0

    async def shutdown(self) -> None:
        return None


class _NoopGateTrader:
    def __init__(self, test_mode: bool = False, *_, **__):
        self.test_mode = test_mode
        self.performance = {"trade_count": 0, "avg_trade_time": 0, "min_trade_time": 0, "max_trade_time": 0}

    async def _get_balance(self, force: bool = False) -> float:
        return 0.0

    def get_balance(self, force: bool = False) -> float:
        return 0.0


async def run(symbol: str, amount: float) -> int:
    spot_binance.MexcHyperbeastTrader = _NoopMexcTrader
    spot_binance.GateUltraTrader = _NoopGateTrader

    trader = spot_binance.UltraTrader(test_mode=False)
    binance_trader = trader.binance_trader

    print(f"binance-env: {'testnet' if getattr(binance_trader, 'use_testnet', False) else 'live'}")
    print(f"binance-base-url: {binance_trader.base_url}")
    print(f"account-count: {len(binance_trader.accounts)}")

    before_balances = binance_trader.get_all_balances(force=True)
    before_usdt = sum(before_balances.values())
    pair = f"{symbol}USDT"
    listed = await binance_trader.is_symbol_listed(pair)

    print(f"symbol: {symbol}")
    print(f"pair: {pair}")
    print(f"listed: {listed}")
    print(f"usdt-before: {before_usdt}")

    if not before_balances:
        print("auth: failed")
        return 2

    print("auth: ok")

    if not listed:
        print("trade: skipped-listing-missing")
        return 3

    result = await trader.execute_trades(
        [symbol],
        exchanges_filter=["binance"],
        amount=amount,
    )

    after_balances = binance_trader.get_all_balances(force=True)
    after_usdt = sum(after_balances.values())

    print(f"trade-success: {result.get('success')}")
    print(f"trade-total: {result.get('total')}")
    print(f"trade-successful: {result.get('successful')}")
    print(f"usdt-after: {after_usdt}")
    print(f"usdt-delta: {after_usdt - before_usdt}")

    for entry in result.get("results", []):
        if entry.get("exchange") != "binance":
            continue
        order_result = entry.get("order_result") or {}
        print(
            "binance-leg: "
            f"success={entry.get('success')} "
            f"account={entry.get('account')} "
            f"amount={entry.get('amount')} "
            f"order_id={order_result.get('orderId')}"
        )

    print(f"timestamp: {datetime.now().isoformat()}")
    return 0 if result.get("success") else 4


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Spot Testnet smoke for modular spot runtime")
    parser.add_argument("--symbol", default="BTC", help="Base symbol to buy on Binance Spot Testnet")
    parser.add_argument("--amount", type=float, default=11.0, help="USDT quote amount to spend")
    args = parser.parse_args()
    return asyncio.run(run(args.symbol.upper(), args.amount))


if __name__ == "__main__":
    raise SystemExit(main())
