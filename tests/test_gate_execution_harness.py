import asyncio
import time
from dataclasses import dataclass

import pytest


@dataclass
class MockDelayConfig:
    """Keep the artificial delays together so tests stay readable."""

    balance: float = 0.12
    pair_info: float = 0.08
    price_lookup: float = 0.10
    order_submit: float = 0.05
    confirm: float = 0.04


class MockGateAPI:
    """Async mock that mimics the behaviour of the live Gate client."""

    def __init__(self, delays: MockDelayConfig | None = None) -> None:
        self.delays = delays or MockDelayConfig()
        self._balance_cache: float | None = None
        self._pair_cache: dict | None = None
        self.balance_calls = 0
        self.pair_calls = 0
        self.price_calls = 0
        self.order_calls = 0
        self.confirm_calls = 0

    async def fetch_balance(self, *, force: bool = False) -> float:
        self.balance_calls += 1
        if self._balance_cache is not None and not force:
            return self._balance_cache
        await asyncio.sleep(self.delays.balance)
        self._balance_cache = 500.0
        return self._balance_cache

    async def fetch_pair_info(self) -> dict:
        self.pair_calls += 1
        if self._pair_cache is not None:
            return self._pair_cache
        await asyncio.sleep(self.delays.pair_info)
        self._pair_cache = {"precision": 8, "amount_precision": 6}
        return self._pair_cache

    async def fetch_last_price(self) -> float:
        self.price_calls += 1
        await asyncio.sleep(self.delays.price_lookup)
        return 0.25

    async def submit_order(self) -> dict:
        self.order_calls += 1
        await asyncio.sleep(self.delays.order_submit)
        return {"id": "mock-order-1"}

    async def confirm_order(self) -> dict:
        self.confirm_calls += 1
        await asyncio.sleep(self.delays.confirm)
        return {"filled_total": 312.0}


class BaselineGateFlow:
    """Simplified version of the current Gate execution path."""

    def __init__(self, api: MockGateAPI) -> None:
        self.api = api

    async def execute(self, *, preparation_data):
        balance = await self.api.fetch_balance()
        _pair = await self.api.fetch_pair_info()
        reference_price = preparation_data if isinstance(preparation_data, dict) else None
        if not reference_price:
            reference_price = {"reference_price": await self.api.fetch_last_price()}

        _ = reference_price["reference_price"]  # re-uses computed price for sizing
        await self.api.submit_order()
        await self.api.confirm_order()
        return balance


class OptimizedGateFlow:
    """Applies the optimisations we plan to port to the production trader."""

    def __init__(self, api: MockGateAPI) -> None:
        self.api = api
        self._prewarmed = False

    async def prewarm(self) -> None:
        if self._prewarmed:
            return
        # Warm up balance + pair metadata concurrently.
        await asyncio.gather(
            self.api.fetch_balance(),
            self.api.fetch_pair_info(),
        )
        self._prewarmed = True

    async def execute(self, *, preparation_data: dict):
        if not isinstance(preparation_data, dict):
            raise ValueError("preparation_data must provide reference info in dict form")

        await self.prewarm()
        # Cached results mean these return instantly for the live order.
        balance = await self.api.fetch_balance()
        _pair = await self.api.fetch_pair_info()
        _ = preparation_data["reference_price"]
        await self.api.submit_order()
        await self.api.confirm_order()
        return balance


def test_gate_optimisation_cuts_execution_time():
    baseline_api = MockGateAPI()
    baseline_flow = BaselineGateFlow(baseline_api)

    optimized_api = MockGateAPI()
    optimized_flow = OptimizedGateFlow(optimized_api)

    baseline_start = time.perf_counter()
    asyncio.run(baseline_flow.execute(preparation_data=0.25))
    baseline_duration = time.perf_counter() - baseline_start

    optimized_start = time.perf_counter()
    asyncio.run(optimized_flow.execute(preparation_data={"reference_price": 0.25}))
    optimized_duration = time.perf_counter() - optimized_start

    assert optimized_duration < baseline_duration * 0.6
    assert baseline_api.price_calls == 1  # float preparation forces price lookup
    assert optimized_api.price_calls == 0  # dict prep injects reference price


def test_prewarm_runs_metadata_and_balance_once():
    api = MockGateAPI()
    flow = OptimizedGateFlow(api)

    asyncio.run(flow.prewarm())
    asyncio.run(flow.execute(preparation_data={"reference_price": 0.25}))

    assert api.balance_calls == 2  # prewarm + live fetch using cache
    assert api.pair_calls == 2
    assert api.price_calls == 0
    assert api.order_calls == 1
    assert api.confirm_calls == 1
