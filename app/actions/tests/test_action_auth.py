import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.actions.configurations import AuthenticateConfig
from app.actions.handlers import action_auth


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
