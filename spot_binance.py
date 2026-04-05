import asyncio
import sys
import types

from spot_runtime import (
    CMC_API,
    CoinMarketCapAPI,
    HyperbeastWebSocketClient,
    UltraTrader,
    UbuntuOptimizer,
    api_logger,
    logger,
    main,
    perf_logger,
    ws_logger,
)
from spot_runtime import binance as _binance_runtime
from spot_runtime import common as _common_runtime
from spot_runtime import gate as _gate_runtime
from spot_runtime import mexc as _mexc_runtime
from spot_runtime import orchestrator as _orchestrator_runtime

_MIRRORED_SOURCES = {
    "BINANCE_SESSION": _common_runtime,
    "MEXC_SESSION": _common_runtime,
    "GATE_SESSION": _common_runtime,
    "BinanceHyperbeastTrader": _orchestrator_runtime,
    "MexcHyperbeastTrader": _orchestrator_runtime,
    "GateUltraTrader": _orchestrator_runtime,
    "CMC_API": _common_runtime,
    "UbuntuOptimizer": _common_runtime,
}

_MIRRORED_TARGETS = {
    "BINANCE_SESSION": (_common_runtime, _binance_runtime, _orchestrator_runtime),
    "MEXC_SESSION": (_common_runtime, _mexc_runtime),
    "GATE_SESSION": (_common_runtime, _gate_runtime),
    "BinanceHyperbeastTrader": (_orchestrator_runtime,),
    "MexcHyperbeastTrader": (_orchestrator_runtime,),
    "GateUltraTrader": (_orchestrator_runtime,),
    "CMC_API": (
        _common_runtime,
        _binance_runtime,
        _mexc_runtime,
        _gate_runtime,
        _orchestrator_runtime,
    ),
    "UbuntuOptimizer": (_common_runtime, _orchestrator_runtime),
}


class _SpotFacadeModule(types.ModuleType):
    def __getattribute__(self, name):
        source = _MIRRORED_SOURCES.get(name)
        if source is not None:
            return getattr(source, name)
        return types.ModuleType.__getattribute__(self, name)

    def __setattr__(self, name, value):
        types.ModuleType.__setattr__(self, name, value)
        for module in _MIRRORED_TARGETS.get(name, ()):
            setattr(module, name, value)


sys.modules[__name__].__class__ = _SpotFacadeModule

__all__ = [
    "BINANCE_SESSION",
    "CMC_API",
    "CoinMarketCapAPI",
    "GATE_SESSION",
    "MEXC_SESSION",
    "BinanceHyperbeastTrader",
    "GateUltraTrader",
    "HyperbeastWebSocketClient",
    "MexcHyperbeastTrader",
    "UltraTrader",
    "UbuntuOptimizer",
    "api_logger",
    "logger",
    "main",
    "perf_logger",
    "ws_logger",
]


if __name__ == "__main__":
    asyncio.run(main())
