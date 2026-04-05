import argparse
import asyncio
from datetime import datetime

from dotenv import load_dotenv

import trader


async def run(symbol: str) -> int:
    load_dotenv()

    manager = trader.PositionManager(
        test_mode=False,
        enabled_exchanges=["binance"],
        binance_use_testnet=True,
        position_ws_enabled=False,
    )

    async with manager as active_manager:
        api = active_manager.binance_apis.get("PRIMARY")
        if api is None:
            print("binance-api: unavailable")
            return 1

        auth_probe = await api._send_request("GET", "/api/v3/account", {}, signed=True)
        auth_ok = isinstance(auth_probe, dict) and "balances" in auth_probe
        auth_error = None
        if not auth_ok:
            auth_error = {
                "code": auth_probe.get("code") if isinstance(auth_probe, dict) else None,
                "msg": auth_probe.get("msg") if isinstance(auth_probe, dict) else str(auth_probe),
            }

        price = await api.get_price(f"{symbol}USDT")
        usdt_balance = await api.get_balance("USDT", use_cache=False)
        asset_balance = await api.get_balance(symbol, use_cache=False)
        symbol_info = await api.get_exchange_info(f"{symbol}USDT")

        print(f"binance-testnet-auth: {'ok' if auth_ok else 'failed'}")
        if auth_error:
            print(f"auth-error: code={auth_error['code']} msg={auth_error['msg']}")
        print(f"symbol: {symbol}")
        print(f"price: {price}")
        print(f"usdt-balance: {usdt_balance}")
        print(f"{symbol.lower()}-balance: {asset_balance}")
        print(f"exchange-info: {'ok' if symbol_info else 'missing'}")

        if not auth_ok or price <= 0 or asset_balance <= 0:
            print("manager-eval: skipped")
            return 0 if auth_ok else 2

        sell_calls = []

        async def fake_full_sell(position, reason):
            sell_calls.append(("full", position.symbol, reason))
            return True

        async def fake_partial_sell(position, percentage, reason):
            sell_calls.append(("partial", position.symbol, percentage, reason))
            return True

        async def fake_retry(position, percentage, reason, max_retries=30):
            sell_calls.append(("retry", position.symbol, percentage, reason, max_retries))
            return True

        active_manager.sell_position_master_follower = fake_full_sell
        active_manager.partial_sell_position_master_follower = fake_partial_sell
        active_manager.sell_with_retry = fake_retry

        tracked_qty = max(min(asset_balance * 0.01, asset_balance), 0.00001)
        await active_manager.handle_new_position(
            {
                "symbol": symbol,
                "exchange": "binance",
                "entry_price": price,
                "quantity": tracked_qty,
                "order_id": "binance-testnet-smoke",
                "listing_source": "binance",
                "account_name": "PRIMARY",
            }
        )

        position_key = f"{symbol}_binance_PRIMARY"
        position = active_manager.positions.get(position_key)
        if position is None:
            print("manager-eval: no-position-created")
            return 3

        await active_manager.evaluate_position(position, trigger="binance_testnet_smoke")

        print("manager-eval: ok")
        print(f"tracked-qty: {position.quantity}")
        print(f"profit-pct: {position.current_profit_pct}")
        print(f"strategy-state-keys: {sorted(position.strategy_state.keys())}")
        print(f"synthetic-sell-calls: {len(sell_calls)}")
        print(f"timestamp: {datetime.now().isoformat()}")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Spot Testnet smoke for modular trader runtime")
    parser.add_argument("--symbol", default="BTC", help="Symbol to probe on Binance testnet")
    args = parser.parse_args()
    return asyncio.run(run(args.symbol.upper()))


if __name__ == "__main__":
    raise SystemExit(main())
