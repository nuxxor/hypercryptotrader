"""Utility to refresh local CoinMarketCap cache.

Fetches market data from the CoinMarketCap Pro API using the
`CMC_API_KEY` environment variable and materialises a lightweight
`var/coinmarketcap_data.json` file consumed by the trading services.

This module can be imported and its `refresh_coinmarketcap_cache`
function invoked programmatically (preferred), or executed as a script
for manual refreshes:

    python coinmarketcap_loader.py --limit 500

Symbols can be provided explicitly via `--symbols BTC,ETH` if only a
subset is required. When no symbols are supplied the loader pulls the
top-N listings ranked by market cap.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


CMC_BASE_URL = "https://pro-api.coinmarketcap.com/v1"
DEFAULT_OUTPUT = "var/coinmarketcap_data.json"
DEFAULT_LIMIT = 500


class CoinMarketCapError(RuntimeError):
    """Raised when the CoinMarketCap API call fails."""


def _load_api_key() -> str:
    """Load CMC API key from environment (supporting .env files)."""

    load_dotenv()  # Allow users to keep the key in .env
    api_key = os.getenv("CMC_API_KEY", "").strip()
    if not api_key:
        raise CoinMarketCapError("CMC_API_KEY is not defined in the environment")
    return api_key


def _request(endpoint: str, params: dict) -> dict:
    api_key = _load_api_key()
    headers = {"X-CMC_PRO_API_KEY": api_key}

    url = f"{CMC_BASE_URL}{endpoint}"
    logger.debug("Requesting %s params=%s", url, params)

    response = requests.get(url, headers=headers, params=params, timeout=10)
    if response.status_code != 200:
        raise CoinMarketCapError(
            f"CoinMarketCap request failed ({response.status_code}): {response.text}"
        )

    payload = response.json()
    status = payload.get("status", {})
    if status.get("error_code") not in (0, None):
        raise CoinMarketCapError(
            f"CoinMarketCap returned error {status.get('error_code')}: {status.get('error_message')}"
        )

    return payload


def _normalise_entry(entry: dict) -> dict:
    quote = entry.get("quote", {}).get("USD", {})
    return {
        "cmc_id": entry.get("id"),
        "symbol": entry.get("symbol"),
        "name": entry.get("name"),
        "slug": entry.get("slug"),
        "cmc_rank": entry.get("cmc_rank"),
        "market_cap": quote.get("market_cap"),
        "price": quote.get("price"),
        "volume_24h": quote.get("volume_24h"),
        "percent_change_1h": quote.get("percent_change_1h"),
        "percent_change_24h": quote.get("percent_change_24h"),
        "percent_change_7d": quote.get("percent_change_7d"),
        "last_updated": quote.get("last_updated"),
    }


def refresh_coinmarketcap_cache(
    *,
    output_path: str | Path = DEFAULT_OUTPUT,
    symbols: Optional[Iterable[str]] = None,
    limit: int = DEFAULT_LIMIT,
) -> Path:
    """Refresh the local CoinMarketCap cache file.

    Args:
        output_path: destination json path (default `var/coinmarketcap_data.json`).
        symbols: optional iterable of symbols to query explicitly.
        limit: number of top market-cap entries when `symbols` is None.

    Returns:
        Path to the written cache file.
    """

    if symbols:
        symbol_list = sorted({sym.upper().strip() for sym in symbols if sym})
        payload = _request(
            "/cryptocurrency/quotes/latest",
            {
                "symbol": ",".join(symbol_list),
                "convert": "USD",
            },
        )
        data_iter = payload.get("data", {}).values()
    else:
        safe_limit = max(1, min(int(limit), 5000))  # CMC limit safeguard
        payload = _request(
            "/cryptocurrency/listings/latest",
            {
                "convert": "USD",
                "limit": safe_limit,
                "sort": "market_cap",
            },
        )
        data_iter = payload.get("data", [])

    coin_map = {}
    for entry in data_iter:
        normalised = _normalise_entry(entry)
        symbol = normalised.get("symbol")
        if symbol:
            coin_map[symbol.upper()] = normalised

    if not coin_map:
        raise CoinMarketCapError("No data received from CoinMarketCap")

    metadata = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "symbols": sorted(coin_map.keys()),
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"_metadata": metadata, **coin_map}, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    logger.info("Updated CoinMarketCap cache with %d entries -> %s", len(coin_map), output_path)
    return output_path


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Refresh CoinMarketCap cache")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path")
    parser.add_argument(
        "--symbols",
        help="Comma-separated list of symbols to fetch (default: top market-cap listings)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Number of top listings to pull when symbols are omitted (default: 500)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable debug logging"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=24 * 3600,
        help="Refresh interval in seconds when running continuously (default: 86400)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Refresh a single time and exit (matches legacy behaviour)",
    )
    return parser.parse_args()


def _main():
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    interval = max(60, int(args.interval))

    while True:
        start = time.monotonic()
        try:
            refresh_coinmarketcap_cache(
                output_path=args.output,
                symbols=symbols,
                limit=args.limit,
            )
        except CoinMarketCapError as exc:
            logger.error("Failed to refresh CoinMarketCap cache: %s", exc)
            if args.run_once:
                raise SystemExit(1) from exc
        else:
            if args.run_once:
                break

        if args.run_once:
            break

        elapsed = time.monotonic() - start
        sleep_for = max(0.0, interval - elapsed)
        hours = sleep_for / 3600.0
        logger.info("Sleeping %.2f hours (%.0f seconds) before next refresh", hours, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    _main()
