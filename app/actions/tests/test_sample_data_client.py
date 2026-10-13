"""
sample_integration_data against a real DataAPI client with GFW mocked at the
HTTP layer (respx), so the client's auth/API-key and retry contracts are
exercised rather than mocked away.
"""
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx
from gundi_core.schemas.v2 import Integration

from app.actions.configurations import SampleDataConfig
from app.actions.gfwclient import DataAPI
from app.actions.handlers import sample_integration_data
from app.actions.tests.test_gfwclient import f_api_keys_response, f_auth_token_response, fast_backoff  # noqa: F401


INTEGRATION_ID = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
QUERY_URL = f"{DataAPI.DATA_API_URL}/dataset/{{dataset}}/latest/query/json"


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
                "fire_alerts_lowest_confidence": "high",
                "fire_lookback_days": 10,            # two 7-day chunks
                "integrated_alerts_lowest_confidence": "highest",
                "integrated_alerts_lookback_days": 15,  # two 10-day chunks
            },
        },
    ]
    return Integration.parse_obj(data)


@pytest.fixture
def cached_aoi_state(mocker):
    # Cached AOI: the share-link lookup is skipped, so nothing has touched the
    # client's auth generator before the feeds fan out.
    state = MagicMock()
    state.get_aoi_data = AsyncMock(return_value={
        "type": "area", "id": "aoi-1",
        "attributes": {
            "name": "Test AOI", "geostore": "geo-1",
            "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
            "use": {}, "env": "production", "status": "saved", "public": False,
        },
    })
    state.get_state = AsyncMock(return_value=None)
    state.is_quiet_period = AsyncMock(return_value=False)
    state.get_pending_jobs = AsyncMock(return_value=[])
    mocker.patch("app.actions.handlers.state_manager", state)
    return state


def mock_auth(f_auth_token_response, f_api_keys_response):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").respond(status_code=200, json=f_api_keys_response)
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/nasa_viirs_fire_alerts/latest").respond(status_code=500)
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/gfw_integrated_alerts/latest").respond(status_code=500)


FIRE_ROW = {"latitude": 1.0, "longitude": 2.0, "confidence__cat": "h", "alert__date": "2026-10-01"}
INTEGRATED_ROW = {"latitude": 1.0, "longitude": 2.0, "gfw_integrated_alerts__confidence": "highest", "gfw_integrated_alerts__date": "2026-10-01"}


@pytest.mark.asyncio
@respx.mock
async def test_concurrent_feeds_share_one_authenticated_client(gfw_integration, cached_aoi_state, f_auth_token_response, f_api_keys_response):
    mock_auth(f_auth_token_response, f_api_keys_response)
    fire_route = respx.get(QUERY_URL.format(dataset="nasa_viirs_fire_alerts")).respond(status_code=200, json={"data": [FIRE_ROW]})
    integrated_route = respx.get(QUERY_URL.format(dataset="gfw_integrated_alerts")).respond(status_code=200, json={"data": [INTEGRATED_ROW]})

    result = await sample_integration_data(gfw_integration, SampleDataConfig(), use_state=True)

    assert "error" not in result
    assert "error" not in result["fire_alerts"], result["fire_alerts"].get("error")
    assert "error" not in result["integrated_alerts"], result["integrated_alerts"].get("error")
    assert fire_route.call_count == 2 and integrated_route.call_count == 2
    assert result["fire_alerts"]["available_total"] == 1       # same row from both chunks, deduped
    assert result["integrated_alerts"]["sent_total"] == 1


@pytest.mark.asyncio
@respx.mock
async def test_exhausted_query_retries_are_reported_as_a_feed_error(gfw_integration, cached_aoi_state, f_auth_token_response, f_api_keys_response, fast_backoff):
    mock_auth(f_auth_token_response, f_api_keys_response)
    respx.get(QUERY_URL.format(dataset="nasa_viirs_fire_alerts")).respond(status_code=503)
    respx.get(QUERY_URL.format(dataset="gfw_integrated_alerts")).respond(status_code=200, json={"data": [INTEGRATED_ROW]})

    result = await sample_integration_data(gfw_integration, SampleDataConfig(), use_state=True)

    assert "error" in result["fire_alerts"]
    assert "503" in result["fire_alerts"]["error"]
    assert "available_total" not in result["fire_alerts"]       # never "0 available" on a failed query
    assert result["integrated_alerts"]["available_total"] == 1


@pytest.mark.asyncio
@respx.mock
async def test_gfw_auth_failure_is_the_top_level_error(gfw_integration, cached_aoi_state, fast_backoff):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=401, json={"message": "bad credentials"})

    result = await sample_integration_data(gfw_integration, SampleDataConfig(), use_state=True)

    assert "error" in result and "fire_alerts" not in result
    assert "authenticat" in result["error"].lower()
