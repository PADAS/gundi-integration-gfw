from unittest.mock import AsyncMock

import httpx
import pytest
from gundi_client_v2.errors import GundiAPIError

from app.actions.handlers import handle_transformed_data


@pytest.mark.asyncio
async def test_gundi_api_error_is_reported_not_raised(mocker):
    # gundi-client-v2 3.x raises GundiAPIError (not an httpx error) for 4xx/5xx.
    mocker.patch(
        "app.actions.handlers.send_events_to_gundi",
        AsyncMock(side_effect=GundiAPIError(400, "event rejected")),
    )

    result = await handle_transformed_data([{"title": "x"}], "int-1", "pull_events")

    assert "error" in result
    assert "event rejected" in result["error"]


@pytest.mark.asyncio
async def test_transport_error_is_still_reported_not_raised(mocker):
    mocker.patch(
        "app.actions.handlers.send_events_to_gundi",
        AsyncMock(side_effect=httpx.ConnectError("boom")),
    )

    result = await handle_transformed_data([{"title": "x"}], "int-1", "pull_events")

    assert "error" in result
