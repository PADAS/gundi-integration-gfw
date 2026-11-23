from unittest.mock import AsyncMock, MagicMock

import pytest

from app.actions import action_handlers
from app.actions.configurations import ClearCacheConfig
from app.actions.handlers import action_clear_cache


@pytest.mark.asyncio
@pytest.mark.parametrize("cleared,message", [
    (True, "Cleared the saved area of interest. The next run reads it again from the share link."),
    (False, "There was no saved area of interest to clear."),
])
async def test_clear_cache_forgets_the_saved_area(mocker, integration_v2, mock_publish_event, cleared, message):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    state = MagicMock()
    state.delete_aoi_data = AsyncMock(return_value=cleared)
    mocker.patch("app.actions.handlers.state_manager", state)
    log = mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())

    result = await action_clear_cache(integration=integration_v2, action_config=ClearCacheConfig())

    state.delete_aoi_data.assert_awaited_once_with(str(integration_v2.id))
    assert result == {"cleared": cleared, "message": message}
    assert log.await_args.kwargs["title"] == message
    assert log.await_args.kwargs["action_id"] == "clear_cache"


def test_clear_cache_can_be_run_from_the_portal():
    _, config_model, _ = action_handlers["clear_cache"]

    assert config_model is ClearCacheConfig
    assert config_model.schema()["properties"] == {}


@pytest.mark.parametrize("action_id,title", [
    ("auth", "Authentication"),
    ("pull_events", "Fetch Alerts from Global Nature Watch"),
    ("sample_data", "Preview Alerts in the Area"),
    ("clear_cache", "Clear Saved Area of Interest"),
])
def test_actions_register_with_friendly_names(action_id, title):
    func, _, _ = action_handlers[action_id]

    assert func.action_title == title
