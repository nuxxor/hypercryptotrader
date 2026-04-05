# hypercryptotrader

Solo-focused public copy of an event-driven crypto trading stack.

This repository is organized around three runtime stages:

1. `tree_news.py`
   Ingests listing and news signals from Tree News, optional Telegram backups, and a local WebSocket bus.
2. `spot_binance.py`
   Consumes signals and orchestrates spot execution attempts across Binance, Gate, and MEXC.
3. `trader.py`
   Tracks open positions, monitors trade state, and executes exit logic.

Pipeline flow:

`tree_news -> spot_binance -> trader`

## Public copy scope

This public copy intentionally keeps the solo-trader core and excludes:

- follower launchers and multi-account orchestration files
- private `.env` files, account maps, whitelist files, and proxy assignments
- runtime logs, caches, market dumps, and trade result artefacts
- legacy monolithic snapshots and unrelated side tools

## Quick start

Install dependencies:

```bash
pip install -r requirements.txt
```

Create your local environment file:

```bash
cp .env.example .env
```

Optional but recommended: refresh the CoinMarketCap cache before starting the pipeline:

```bash
python coinmarketcap_loader.py --run-once
```

Start the local pipeline in three terminals:

```bash
python tree_news.py
python spot_binance.py
python trader.py
```

The default local bus is `ws://localhost:9999/ws`, so the simplest deployment is a single machine.

## Testnet and smoke checks

Run the trader runtime against Binance Spot Testnet:

```bash
python binance_testnet_smoke.py --symbol BTC
```

Run the spot execution runtime against Binance Spot Testnet:

```bash
python spot_binance_testnet_smoke.py --symbol BTC --amount 11
```

Run a local synthetic signal without waiting for live news:

```bash
python tree_news.py --test TAO --test-source UPBIT_TEST --test-listing-type KRW
```

Run the trader in simulated mode without live positions:

```bash
python trader.py --test --exchanges gate,mexc --disable-position-ws
```

## Solo setup notes

- The normal solo setup uses the primary exchange credentials from `.env`.
- `signal_relay.py` is optional and only needed if you want a remote publish/subscribe hop instead of the default local WebSocket flow.
- Runtime data is written under `var/`, so the repo root stays clean.

## Repository layout

```text
hypercryptotrader/
├── tree_news.py
├── spot_binance.py
├── trader.py
├── signal_relay.py
├── coinmarketcap_loader.py
├── tree_runtime/
├── spot_runtime/
├── trader_runtime/
├── sentiment-llm/
├── tests/
└── var/
```

## Useful checks

Run the focused regression suite:

```bash
pytest -q \
  tests/test_tree_news_runtime.py \
  tests/test_tree_news_market_cap.py \
  tests/test_spot_exchange_runtime.py \
  tests/test_spot_binance_modular_smoke.py \
  tests/test_trader_modular_smoke.py \
  tests/test_trader_position_manager.py \
  tests/test_system_pipeline.py
```
