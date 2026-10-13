from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from gundi_core.schemas.v2 import Integration

from app.actions.configurations import GetIntegratedAlertsDatasetAndGeostoresConfig, PullEventsConfig
from app.actions.gfwclient import AOIData, DatasetResponseItem
from app.actions.handlers import action_get_gfw_integrated_alerts


INTEGRATION_ID = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"


@pytest.fixture
def gfw_integration(integration_v2_as_dict):
    data = dict(integration_v2_as_dict)
    data["configurations"] = [
        {
            "id": "30f8878c-4a98-4c95-88eb-79f73c40fb2f",
            "integration": INTEGRATION_ID,
            "action": {"id": "80448d1c-4696-4b32-a59f-f3494fc949ac", "type": "auth", "name": "Authenticate", "value": "auth"},
            "data": {"email": "ranger@example.com", "password": "secret"},
        },
    ]
    return Integration.parse_obj(data)


@pytest.fixture
def action_config():
    return GetIntegratedAlertsDatasetAndGeostoresConfig(
        integration_id=INTEGRATION_ID,
        pull_events_config=PullEventsConfig(gfw_share_link_url="https://www.globalforestwatch.org/dashboards/aoi/abc123"),
        aoi_data=AOIData.parse_obj({
            "type": "area", "id": "aoi-1",
            "attributes": {
                "name": "Test AOI", "geostore": "geo-1",
                "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
                "use": {}, "env": "production", "status": "saved", "public": False,
            },
        }),
    )


@pytest.fixture
def mock_dataapi(mocker):
    api = MagicMock()
    api.get_job_status = AsyncMock(return_value=SimpleNamespace(
        status="success", download_link="https://gfw.example/dl/job-1",
        failed_geometries_link=None, progress=None, message=None,
    ))
    api.download_job_results = AsyncMock(return_value=[{
        "latitude": 1.0, "longitude": 2.0,
        "gfw_integrated_alerts__confidence": "highest",
        "gfw_integrated_alerts__date": "2026-10-01",
    }])
    api.get_dataset_metadata = AsyncMock(return_value=DatasetResponseItem(
        created_on="2026-01-01T00:00:00Z", updated_on="2026-10-11T00:00:00Z",
        dataset="gfw_integrated_alerts", version="v1", is_latest=True, is_mutable=True,
    ))
    mocker.patch("app.actions.handlers.DataAPI", return_value=api)
    return api


@pytest.fixture
def mock_state(mocker):
    state = MagicMock()
    state.get_pending_jobs = AsyncMock(return_value=[{"job_id": "job-1", "job_link": "https://gfw.example/job/job-1"}])
    state.remove_pending_job = AsyncMock()
    state.add_pending_job = AsyncMock()
    # Dataset already processed at its current updated_on, so no new batch job is created.
    state.get_state = AsyncMock(return_value={
        "dataset": "gfw_integrated_alerts", "version": "v1", "latest_updated_on": "2026-10-11T00:00:00+00:00",
    })
    state.set_state = AsyncMock()
    state.set_quiet_period = AsyncMock()
    mocker.patch("app.actions.handlers.state_manager", state)
    return state


@pytest.fixture
def mock_activity_log(mocker):
    return mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())


@pytest.mark.asyncio
async def test_completed_job_is_removed_after_a_successful_send(gfw_integration, action_config, mock_dataapi, mock_state, mock_activity_log, mocker):
    mocker.patch("app.actions.handlers.handle_transformed_data", AsyncMock(return_value={"status": "ok"}))

    result = await action_get_gfw_integrated_alerts(integration=gfw_integration, action_config=action_config)

    assert result["jobs_completed"] == 1
    assert result["total_alerts_processed"] == 1
    mock_state.remove_pending_job.assert_awaited_once_with(INTEGRATION_ID, "get_gfw_integrated_alerts", "job-1")


@pytest.mark.asyncio
async def test_job_stays_pending_when_the_send_to_gundi_fails(gfw_integration, action_config, mock_dataapi, mock_state, mock_activity_log, mocker):
    mocker.patch("app.actions.handlers.handle_transformed_data", AsyncMock(return_value={"error": "Sensors API returned error"}))

    result = await action_get_gfw_integrated_alerts(integration=gfw_integration, action_config=action_config)

    mock_state.remove_pending_job.assert_not_called()
    assert result["jobs_completed"] == 0
    assert result["total_alerts_processed"] == 0
    assert result["jobs_pending"] == 1


@pytest.mark.asyncio
async def test_failed_send_is_recorded_on_the_pending_job(gfw_integration, action_config, mock_dataapi, mock_state, mock_activity_log, mocker):
    mocker.patch("app.actions.handlers.handle_transformed_data", AsyncMock(return_value={"error": "Sensors API returned error: HTTP 400"}))

    await action_get_gfw_integrated_alerts(integration=gfw_integration, action_config=action_config)

    mock_state.add_pending_job.assert_awaited_once()
    _, action_id, job_data = mock_state.add_pending_job.call_args.args
    assert action_id == "get_gfw_integrated_alerts"
    assert job_data["job_id"] == "job-1"
    assert job_data["last_send_error"] == "Sensors API returned error: HTTP 400"


@pytest.mark.asyncio
async def test_expired_download_link_reports_the_last_send_error(gfw_integration, action_config, mock_dataapi, mock_state, mock_activity_log, mocker):
    from app.actions.gfwclient import DownloadLinkExpiredException

    mock_state.get_pending_jobs = AsyncMock(return_value=[{
        "job_id": "job-1", "job_link": "https://gfw.example/job/job-1",
        "last_send_error": "Sensors API returned error: HTTP 400",
    }])
    mock_dataapi.download_job_results = AsyncMock(side_effect=DownloadLinkExpiredException("expired"))

    result = await action_get_gfw_integrated_alerts(integration=gfw_integration, action_config=action_config)

    assert result["jobs_failed"] == 1
    expired_log = next(c for c in mock_activity_log.call_args_list if "expired" in c.kwargs["title"])
    assert expired_log.kwargs["data"]["last_send_error"] == "Sensors API returned error: HTTP 400"
    assert "HTTP 400" in expired_log.kwargs["title"]


@pytest.mark.asyncio
async def test_our_own_gundi_auth_failure_is_not_swallowed_by_the_poll_loop(gfw_integration, action_config, mock_dataapi, mock_state, mock_activity_log, mocker):
    from gundi_client_v2.errors import AuthenticationError

    mocker.patch(
        "app.actions.handlers.handle_transformed_data",
        AsyncMock(side_effect=AuthenticationError("invalid_client", status_code=401)),
    )

    with pytest.raises(AuthenticationError):
        await action_get_gfw_integrated_alerts(integration=gfw_integration, action_config=action_config)
    mock_state.remove_pending_job.assert_not_called()
