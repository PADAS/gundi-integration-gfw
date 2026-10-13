"""
GFW-owned tests for the IntegrationStateManager methods this integration added
(quiet periods, AOI cache, pending jobs). Kept in a separate file so the
template-owned test_state_manager.py merges cleanly on upstream syncs.
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio as redis

from app.services import state as state_module
from app.services.activity_logger import ephemeral_run
from app.services.state import IntegrationStateManager


@pytest.fixture
def fast_redis_retry(mocker):
    mocker.patch.object(
        state_module, "REDIS_RETRY",
        {**state_module.REDIS_RETRY, "wait_initial": 0.05, "wait_max": 0.05, "wait_jitter": 0},
    )


class FlakyRedis:
    """A redis client whose every call raises RedisError once, then succeeds."""

    def __init__(self):
        self.failed_once = set()
        self.pipe = MagicMock()
        self.pipe.execute = self._call("execute", [True, 1])

    def _call(self, name, value):
        async def call(*args, **kwargs):
            if name not in self.failed_once:
                self.failed_once.add(name)
                raise redis.RedisError("blip")
            return value
        return call

    def __getattr__(self, name):
        values = {
            "get": json.dumps({"id": "aoi-1"}),
            "exists": 1,
            "setex": True,
            "delete": 1,
            "smembers": {b"job-1"},
            "srem": 1,
        }
        if name == "pipeline":
            return lambda **kwargs: self.pipe
        if name in values:
            return self._call(name, values[name])
        raise AttributeError(name)


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


GFW_METHODS = {
    "set_quiet_period": lambda m: m.set_quiet_period("int-1", "fire_alerts", 60),
    "is_quiet_period": lambda m: m.is_quiet_period("int-1", "fire_alerts"),
    "set_aoi_data": lambda m: m.set_aoi_data("int-1", {"id": "aoi-1"}),
    "get_aoi_data": lambda m: m.get_aoi_data("int-1"),
    "add_pending_job": lambda m: m.add_pending_job("int-1", "get_gfw_integrated_alerts", {"job_id": "job-1"}),
    "get_pending_jobs": lambda m: m.get_pending_jobs("int-1", "get_gfw_integrated_alerts"),
    "remove_pending_job": lambda m: m.remove_pending_job("int-1", "get_gfw_integrated_alerts", "job-1"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", GFW_METHODS)
async def test_gfw_state_methods_retry_without_blocking_the_event_loop(fast_redis_retry, method):
    manager = IntegrationStateManager()
    manager.db_client = FlakyRedis()

    _, ticks = await run_with_ticker(GFW_METHODS[method](manager))

    assert ticks >= 2, f"{method}: retry back-off blocked the event loop"


@pytest.mark.asyncio
async def test_get_aoi_data_returns_cached_value_after_a_retry(fast_redis_retry):
    manager = IntegrationStateManager()
    manager.db_client = FlakyRedis()

    assert await manager.get_aoi_data("int-1") == {"id": "aoi-1"}


MUTATING_METHODS = ["set_quiet_period", "set_aoi_data", "add_pending_job", "remove_pending_job"]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", MUTATING_METHODS)
async def test_gfw_mutating_methods_noop_on_ephemeral_run(method):
    # Ephemeral runs use a synthetic integration id; any write is a permanent orphan.
    manager = IntegrationStateManager()
    manager.db_client = MagicMock()
    manager.db_client.pipeline = MagicMock()
    token = ephemeral_run.set(True)
    try:
        await GFW_METHODS[method](manager)
    finally:
        ephemeral_run.reset(token)

    assert manager.db_client.method_calls == []
