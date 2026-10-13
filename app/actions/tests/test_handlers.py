from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from gundi_core.schemas.v2 import Integration

from app.actions.configurations import SampleDataConfig
from app.actions.gfwclient import (
    IntegratedAlert,
    IntegratedAlertsConfidenceEnum,
    NasaViirsFireAlert,
    NasaViirsFireAlertConfidenceEnum,
    DatasetResponseItem,
)
from app.actions.handlers import action_sample_data


INTEGRATION_ID = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"


def make_integrated(confidence):
    return IntegratedAlert.parse_obj({
        "latitude": 1.0, "longitude": 2.0,
        "gfw_integrated_alerts__confidence": confidence,
        "gfw_integrated_alerts__date": "2026-10-01",
    })


def make_fire(cat):
    return NasaViirsFireAlert.parse_obj({
        "latitude": 1.0, "longitude": 2.0,
        "confidence__cat": cat,
        "alert__date": "2026-10-01",
    })


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
        {
            "id": "431af42b-c431-40af-8b57-a349253e15df",
            "integration": INTEGRATION_ID,
            "action": {"id": "4b721b37-f4ca-4f20-b07c-2caadb095ecb", "type": "pull", "name": "Pull Events", "value": "pull_events"},
            "data": {
                "gfw_share_link_url": "https://www.globalforestwatch.org/dashboards/aoi/abc123",
                "include_fire_alerts": True,
                "fire_alerts_lowest_confidence": "high",
                "fire_lookback_days": 5,
                "include_integrated_alerts": True,
                "integrated_alerts_lowest_confidence": "highest",
                "integrated_alerts_lookback_days": 15,
            },
        },
    ]
    return Integration.parse_obj(data)


@pytest.fixture
def aoi_data():
    return {
        "type": "area",
        "id": "aoi-1",
        "attributes": {"name": "Test AOI", "geostore": "geo-1",
                       "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
                       "use": {}, "env": "production", "status": "saved", "public": False},
    }


@pytest.fixture
def mock_dataapi(mocker):
    api = MagicMock()
    api.get_nasa_viirs_fire_alerts = AsyncMock(return_value=[make_fire("l"), make_fire("n"), make_fire("h")])
    # Lookback windows are queried in date chunks; return alerts on the first chunk only
    # so counts reflect distinct alerts rather than the number of chunks.
    integrated_chunks = iter([[make_integrated("nominal"), make_integrated("highest")]])
    api.get_gfw_integrated_alerts = AsyncMock(side_effect=lambda **kwargs: next(integrated_chunks, []))
    api.get_dataset_metadata = AsyncMock(return_value=DatasetResponseItem(
        created_on="2026-01-01T00:00:00Z", updated_on="2026-10-11T00:00:00Z",
        dataset="x", version="v1", is_latest=True, is_mutable=True,
    ))
    mocker.patch("app.actions.handlers.DataAPI", return_value=api)
    return api


@pytest.fixture
def mock_state(mocker, aoi_data):
    state = MagicMock()
    state.get_aoi_data = AsyncMock(return_value=aoi_data)
    state.set_aoi_data = AsyncMock()
    state.get_state = AsyncMock(return_value=None)
    state.is_quiet_period = AsyncMock(return_value=False)
    state.get_pending_jobs = AsyncMock(return_value=[])
    mocker.patch("app.actions.handlers.state_manager", state)
    return state


@pytest.fixture
def mock_activity_log(mocker, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    return mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())


@pytest.mark.asyncio
async def test_sample_data_reports_available_vs_sent_for_each_feed(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    result = await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    fire = result["fire_alerts"]
    assert fire["lookback_days"] == 5
    assert fire["available_total"] == 3
    assert fire["sent_total"] == 1
    assert fire["available_by_confidence"] == {"low": 1, "nominal": 1, "high": 1}

    integrated = result["integrated_alerts"]
    assert integrated["lookback_days"] == 15
    assert integrated["available_total"] == 2
    assert integrated["sent_total"] == 1
    assert integrated["sent_by_confidence"] == {"highest": 1}

    assert result["aoi_id"] == "aoi-1"
    assert result["geostore_id"] == "geo-1"


@pytest.mark.asyncio
async def test_sample_data_queries_gfw_without_a_confidence_filter(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    fire_kwargs = mock_dataapi.get_nasa_viirs_fire_alerts.call_args.kwargs
    assert fire_kwargs["lowest_confidence"] == NasaViirsFireAlertConfidenceEnum.low
    assert fire_kwargs["geostore_id"] == "geo-1"

    integrated_kwargs = mock_dataapi.get_gfw_integrated_alerts.call_args.kwargs
    assert integrated_kwargs["lowest_confidence"] == IntegratedAlertsConfidenceEnum.nominal
    # 15-day window is queried in 10-day chunks, covering the whole range
    calls = mock_dataapi.get_gfw_integrated_alerts.call_args_list
    assert len(calls) == 2
    spans = sorted((c.kwargs["date_range"] for c in calls), key=lambda r: r[0])
    assert (spans[-1][1] - spans[0][0]).days == 15


@pytest.mark.asyncio
async def test_sample_data_lookback_override_applies_to_both_feeds(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    result = await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig(lookback_days=2))

    assert result["fire_alerts"]["lookback_days"] == 2
    assert result["integrated_alerts"]["lookback_days"] == 2
    start, end = mock_dataapi.get_gfw_integrated_alerts.call_args.kwargs["date_range"]
    assert (end - start).days == 2


@pytest.mark.asyncio
async def test_sample_data_isolates_a_failing_feed(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    mock_dataapi.get_nasa_viirs_fire_alerts.side_effect = RuntimeError("GFW timed out")

    result = await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    assert "GFW timed out" in result["fire_alerts"]["error"]
    assert result["integrated_alerts"]["available_total"] == 2


@pytest.mark.asyncio
async def test_sample_data_skips_disabled_feeds(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    pull_config = next(c for c in gfw_integration.configurations if c.action.value == "pull_events")
    pull_config.data["include_fire_alerts"] = False

    result = await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    assert result["fire_alerts"] == {"enabled": False}
    mock_dataapi.get_nasa_viirs_fire_alerts.assert_not_called()


@pytest.mark.asyncio
async def test_sample_data_includes_pipeline_state(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    mock_state.is_quiet_period = AsyncMock(side_effect=lambda integration_id, feed: feed == "fire_alerts")
    mock_state.get_pending_jobs = AsyncMock(return_value=[{"job_id": "job-1"}])
    mock_state.get_state = AsyncMock(return_value={
        "dataset": "x", "version": "v1", "latest_updated_on": "2026-10-10T00:00:00+00:00"
    })

    result = await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    assert result["fire_alerts"]["quiet_period_active"] is True
    assert result["integrated_alerts"]["quiet_period_active"] is False
    assert result["integrated_alerts"]["pending_jobs"] == 1
    assert result["fire_alerts"]["dataset_updated_on"] == "2026-10-11T00:00:00+00:00"
    assert result["fire_alerts"]["last_processed_dataset_update"] == "2026-10-10T00:00:00+00:00"


@pytest.mark.asyncio
async def test_sample_data_writes_one_activity_log_summary(gfw_integration, mock_dataapi, mock_state, mock_activity_log):
    await action_sample_data(integration=gfw_integration, action_config=SampleDataConfig())

    mock_activity_log.assert_awaited_once()
    kwargs = mock_activity_log.call_args.kwargs
    assert kwargs["action_id"] == "sample_data"
    assert "3 available" in kwargs["title"]
    assert "1 pass 'high'" in kwargs["title"]


@pytest.mark.asyncio
async def test_sample_data_reports_missing_pull_config(integration_v2, mock_dataapi, mock_state, mock_activity_log):
    # integration_v2 has a pull_events config but it is not a GFW one, and no GFW auth.
    result = await action_sample_data(integration=integration_v2, action_config=SampleDataConfig())

    assert "error" in result
    mock_dataapi.get_nasa_viirs_fire_alerts.assert_not_called()


@pytest.mark.asyncio
async def test_sample_data_can_run_without_redis_state(gfw_integration, mock_dataapi, mock_state, aoi_data):
    from app.actions.gfwclient import AOIData
    from app.actions.handlers import sample_integration_data

    mock_state.get_aoi_data = AsyncMock(side_effect=RuntimeError("redis down"))
    mock_state.get_state = AsyncMock(side_effect=RuntimeError("redis down"))
    mock_dataapi.aoi_from_url = AsyncMock(return_value="aoi-1")
    mock_dataapi.get_aoi = AsyncMock(return_value=AOIData.parse_obj(aoi_data))

    result = await sample_integration_data(gfw_integration, SampleDataConfig(), use_state=False)

    mock_state.get_aoi_data.assert_not_called()
    mock_state.get_state.assert_not_called()
    mock_state.is_quiet_period.assert_not_called()
    assert result["fire_alerts"]["available_total"] == 3
    assert result["fire_alerts"]["dataset_updated_on"] == "2026-10-11T00:00:00+00:00"
    assert "quiet_period_active" not in result["fire_alerts"]
    assert "state_errors" not in result["fire_alerts"]
