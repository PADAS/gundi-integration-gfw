import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from gundi_core.schemas.v2 import Integration

from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.actions.handlers import action_auth, action_pull_events


INTEGRATION_ID = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
PASSWORD = "hunter2-not-for-logs"


@pytest.fixture
def gfw_integration(integration_v2_as_dict):
    data = dict(integration_v2_as_dict)
    data["configurations"] = [
        {
            "id": "30f8878c-4a98-4c95-88eb-79f73c40fb2f",
            "integration": INTEGRATION_ID,
            "action": {"id": "80448d1c-4696-4b32-a59f-f3494fc949ac", "type": "auth", "name": "Authenticate", "value": "auth"},
            "data": {"email": "ranger@example.com", "password": PASSWORD},
        },
    ]
    return Integration.parse_obj(data)


@pytest.mark.asyncio
async def test_auth_does_not_write_the_access_token_to_the_log(mocker, caplog, integration_v2):
    api = MagicMock()
    api.get_access_token = AsyncMock(return_value="SECRET-TOKEN-XYZ")
    mocker.patch("app.actions.handlers.DataAPI", return_value=api)
    config = AuthenticateConfig(email="ranger@example.com", password="pw")

    with caplog.at_level(logging.DEBUG, logger="app.actions.handlers"):
        result = await action_auth(integration=integration_v2, action_config=config)

    assert result == {"valid_credentials": True}
    assert "SECRET-TOKEN-XYZ" not in caplog.text


@pytest.mark.asyncio
async def test_auth_does_not_write_the_gfw_password_to_the_log(mocker, caplog, gfw_integration):
    # The Integration's configuration data is a plain dict, so logging the
    # whole object prints the password in clear.
    api = MagicMock()
    api.get_access_token = AsyncMock(return_value="tok")
    mocker.patch("app.actions.handlers.DataAPI", return_value=api)
    config = AuthenticateConfig(email="ranger@example.com", password=PASSWORD)

    with caplog.at_level(logging.DEBUG, logger="app.actions.handlers"):
        await action_auth(integration=gfw_integration, action_config=config)

    assert PASSWORD not in caplog.text


@pytest.mark.asyncio
async def test_pull_events_does_not_write_the_gfw_password_to_the_log(mocker, caplog, gfw_integration, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.DataAPI", return_value=MagicMock())
    state = MagicMock()
    state.get_aoi_data = AsyncMock(return_value=None)
    mocker.patch("app.actions.handlers.state_manager", state)
    # No AOI and the share link lookup is a MagicMock (not awaitable), so the
    # action returns early with an error; the log line under test runs first.
    config = PullEventsConfig(gfw_share_link_url="https://www.globalforestwatch.org/dashboards/aoi/abc123")

    with caplog.at_level(logging.DEBUG, logger="app.actions.handlers"):
        await action_pull_events(integration=gfw_integration, action_config=config)

    assert PASSWORD not in caplog.text
