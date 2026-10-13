"""
GFW-owned tests for the IntegrationStateManager methods this integration added
(quiet periods, AOI cache, pending jobs). Kept in a separate file so the
template-owned test_state_manager.py merges cleanly on upstream syncs.
"""
import asyncio
import inspect
import json
from unittest.mock import MagicMock

import pytest
import redis.asyncio as redis

from app.services import state as state_module
from app.services.state import IntegrationStateManager


@pytest.fixture
def fast_redis_retry(mocker):
    mocker.patch.object(
        state_module, "REDIS_RETRY",
        {**state_module.REDIS_RETRY, "wait_initial": 0.05, "wait_max": 0.05, "wait_jitter": 0},
    )


def flaky_once(return_value):
    """An async redis call that raises RedisError on the first call, then succeeds."""
    calls = {"n": 0}

    async def call(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise redis.RedisError("blip")
        return return_value

    return call


async def run_with_ticker(coro):
    """Run coro while counting how often the event loop got to schedule another task."""
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.005)

    task = asyncio.create_task(ticker())
    try:
        result = await coro
    finally:
        task.cancel()
    return result, ticks


@pytest.mark.asyncio
async def test_get_aoi_data_retries_without_blocking_the_event_loop(fast_redis_retry):
    manager = IntegrationStateManager()
    manager.db_client = MagicMock()
    manager.db_client.get = flaky_once(json.dumps({"id": "aoi-1"}))

    result, ticks = await run_with_ticker(manager.get_aoi_data("int-1"))

    assert result == {"id": "aoi-1"}
    assert ticks >= 2, "retry back-off blocked the event loop"


@pytest.mark.asyncio
async def test_is_quiet_period_retries_without_blocking_the_event_loop(fast_redis_retry):
    manager = IntegrationStateManager()
    manager.db_client = MagicMock()
    manager.db_client.exists = flaky_once(1)

    result, ticks = await run_with_ticker(manager.is_quiet_period("int-1", "fire_alerts"))

    assert result == 1
    assert ticks >= 2, "retry back-off blocked the event loop"


def test_no_state_method_iterates_stamina_synchronously():
    # stamina's sync iterator sleeps with time.sleep, which stalls the loop
    # (see app/services/retry_policies.py). Every method must use `async for`.
    source = inspect.getsource(state_module)
    sync_loops = [
        line for line in source.splitlines()
        if "for attempt in stamina.retry_context(" in line and "async for" not in line
    ]
    assert sync_loops == []
