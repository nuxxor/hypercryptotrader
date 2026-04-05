import asyncio

from trader_runtime import (
    AggressiveMomentumBinanceStrategy,
    BalanceSnapshot,
    BinanceAPI,
    BinanceAlphaListingStrategy,
    BinanceListingStrategy,
    BithumbListingStrategy,
    CoinMarketCapAPI,
    GateAPI,
    GateAPIHardError,
    GateAPITemporaryError,
    MEXCAPI,
    Position,
    PositionManager,
    UpbitListingStrategy,
    logger,
    main,
)

__all__ = [
    "AggressiveMomentumBinanceStrategy",
    "BalanceSnapshot",
    "BinanceAPI",
    "BinanceAlphaListingStrategy",
    "BinanceListingStrategy",
    "BithumbListingStrategy",
    "CoinMarketCapAPI",
    "GateAPI",
    "GateAPIHardError",
    "GateAPITemporaryError",
    "MEXCAPI",
    "Position",
    "PositionManager",
    "UpbitListingStrategy",
    "logger",
    "main",
]


if __name__ == "__main__":
    asyncio.run(main())
