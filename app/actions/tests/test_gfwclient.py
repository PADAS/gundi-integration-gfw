from datetime import datetime, timedelta, timezone
import asyncio

import pytest
import httpx
import respx

from app.actions.gfwclient import DataAPI, DataAPIKeysResponse

@pytest.fixture
def f_api_keys_response():
    return {
        "data": [
            {
                "created_on": "2021-09-14T08:00:00.000Z",
                "updated_on": "2021-09-14T08:00:00.000Z",
                "user_id": "er_user",
                "expires_on": (datetime.now(tz=timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "api_key": "1234567890",
                "alias": "test_key",
                "email": "test@example.com",
                "organization": "EarthRanger",
                "domains": []
            }
        ]
    }

@pytest.fixture
def f_auth_token_response():
    return {
        "data": 
            {
                "access_token": "a fancy access token",
                "token_type": "bearer",
                "expires_in": 3600
            }
    }

@pytest.fixture
def f_create_api_key_response():
    return {
        "data": {
            "created_on": "2021-09-14T08:00:00.000Z",
            "updated_on": "2021-09-14T08:00:00.000Z",
            "user_id": "er_user",
            "expires_on": (datetime.now(tz=timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "api_key": "1234567890",
            "alias": "test_key",
            "email": "test@example.com",
            "organization": "EarthRanger",
            "domains": []
        }
    }

@pytest.fixture
def f_api_keys_with_one_expired_response():
    return {
        "data": [
            {
                "created_on": "2021-09-14T08:00:00.000Z",
                "updated_on": "2021-09-14T08:00:00.000Z",
                "user_id": "er_user",
                "expires_on": (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "api_key": "1234567890",
                "alias": "test_key",
                "email": "test@example.com",
                "organization": "EarthRanger",
                "domains": []
            }
        ]
    }


@pytest.mark.asyncio
@respx.mock
async def test_get_api_keys(f_api_keys_response, f_auth_token_response):
    '''
    Test the code to authenticate and fetch API Keys.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock successful lookup for apikeys
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").respond(
        status_code=200, json=f_api_keys_response
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_keys = await client.get_api_keys()
    assert api_keys == DataAPIKeysResponse.parse_obj(f_api_keys_response).data
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_get_a_valid_api_key_creates_new_when_none_exist(f_auth_token_response, f_create_api_key_response):
    '''
    Test that get_a_valid_api_key creates a new API key when none exist.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock 404 response for apikeys (no keys exist)
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").respond(status_code=404)
    
    # Mock successful API key creation
    create_api_key_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").respond(
        status_code=201, json=f_create_api_key_response
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_key = await client.get_a_valid_api_key()
    
    assert api_key.api_key == f_create_api_key_response["data"]["api_key"]
    assert create_api_key_route.called
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_get_a_valid_api_key_creates_new_when_all_expired(f_api_keys_with_one_expired_response, f_auth_token_response, f_create_api_key_response):
    '''
    Test that get_a_valid_api_key creates a new API key when all existing keys are expired.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock response with expired API keys
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").respond(
        status_code=200, json=f_api_keys_with_one_expired_response
    )
    
    # Mock successful API key creation
    create_api_key_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").respond(
        status_code=201, json=f_create_api_key_response
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_key = await client.get_a_valid_api_key()
    
    assert api_key.api_key == f_create_api_key_response["data"]["api_key"]
    assert create_api_key_route.called
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_get_a_valid_api_key_uses_existing_valid_key(f_api_keys_response, f_auth_token_response):
    '''
    Test that get_a_valid_api_key returns existing valid API key when available.
    '''
    access_token_route = respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock successful lookup for apikeys with valid keys
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").respond(
        status_code=200, json=f_api_keys_response
    )

    client = DataAPI(username="test@example.com", password="test_password")
    api_key = await client.get_a_valid_api_key()
    
    assert api_key.api_key == f_api_keys_response["data"][0]["api_key"]
    assert access_token_route.called


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts(f_api_keys_response, f_auth_token_response, f_create_api_key_response, f_get_alerts_response):

    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    
    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response),]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[httpx.Response(status_code=200, json=f_get_alerts_response)]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024,7,30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)
    alerts = await client.get_gfw_integrated_alerts(geostore_id="668c84df810f3b001fe61acf", date_range=(start_date, end_date), semaphore=sema)

    assert len(alerts) == len(f_get_alerts_response['data'])


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts_backs_off_3_times_then_gives_up(
        caplog,
        f_api_keys_response,
        f_auth_token_response,
        f_create_api_key_response
):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )

    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response), ]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    # Calling GFW DataApi 3 times with 504 response
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[httpx.Response(504), httpx.Response(504), httpx.Response(504)]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024, 7, 30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)

    alerts = await client.get_gfw_integrated_alerts(
        geostore_id="668c84df810f3b001fe61acf",
        date_range=(start_date, end_date), semaphore=sema
    )

    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 3
    assert len([log for log in caplog.messages if "Backing off" in log]) == 5  # 1 from get_api_keys + 4 from get_alerts
    assert len([log for log in caplog.messages if "Giving up" in log]) == 1
    assert alerts == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_integrated_alerts_backs_off_2_times_then_succeed(
        caplog,
        f_api_keys_response,
        f_auth_token_response,
        f_create_api_key_response,
        f_get_alerts_response
):
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)

    # Mock lookup for apikeys, before and after creation.
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )

    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response), ]
    )
    '''
    Test the code to fetch integrated alerts.
    '''
    dataset = 'gfw_integrated_alerts'
    # Calling GFW DataApi 2 times with 504 response, then 200
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest/query/json").mock(
        side_effect=[
            httpx.Response(504),
            httpx.Response(504),
            httpx.Response(status_code=200, json=f_get_alerts_response)
        ]
    )

    client = DataAPI(username="test@example.com", password="test_password")
    end_date = datetime(2024, 7, 30, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7)
    sema = asyncio.Semaphore(5)
    alerts = await client.get_gfw_integrated_alerts(
        geostore_id="668c84df810f3b001fe61acf",
        date_range=(start_date, end_date), semaphore=sema
    )
    assert len([i.response.status_code for i in respx.calls if i.response.status_code == 504]) == 2
    assert len([log for log in caplog.messages if "Backing off" in log]) == 5  # 1 from get_api_keys + 4 from get_alerts
    assert len(alerts) == len(f_get_alerts_response['data'])


@pytest.fixture
def f_dataset_metadata_response():
    return {
        "data": {
            "created_on": "2021-09-14T08:00:00.000Z",
            "updated_on": "2021-09-14T08:00:00.000Z",
            "dataset": "gfw_integrated_alerts",
            "version": "latest",
            "is_latest": True,
            "is_mutable": False
        }
    }


@pytest.mark.asyncio
@respx.mock
async def test_get_dataset_metadata_with_caching(
    f_api_keys_response, 
    f_auth_token_response, 
    f_create_api_key_response, 
    f_dataset_metadata_response,
    mocker
):
    """Test that dataset metadata is cached in Redis."""
    from app.actions.gfwclient import _metadata_state_manager
    
    # Mock the state manager methods
    mock_get_metadata = mocker.patch.object(_metadata_state_manager, 'get_metadata')
    mock_set_metadata = mocker.patch.object(_metadata_state_manager, 'set_metadata_with_ttl')
    
    # First call: no cached data, should fetch from API
    mock_get_metadata.return_value = None
    
    # Mock authentication
    respx.post(f"{DataAPI.DATA_API_URL}/auth/token").respond(status_code=200, json=f_auth_token_response)
    respx.get(f"{DataAPI.DATA_API_URL}/auth/apikeys").mock(
        side_effect=[httpx.Response(status_code=404), httpx.Response(status_code=200, json=f_api_keys_response)]
    )
    respx.post(f"{DataAPI.DATA_API_URL}/auth/apikey").mock(
        side_effect=[httpx.Response(status_code=201, json=f_create_api_key_response)]
    )
    
    # Mock dataset metadata API call
    dataset = "gfw_integrated_alerts"
    respx.get(f"{DataAPI.DATA_API_URL}/dataset/{dataset}/latest").respond(
        status_code=200, json=f_dataset_metadata_response
    )
    
    client = DataAPI(username="test@example.com", password="test_password")
    
    # First call should fetch from API and cache
    metadata1 = await client.get_dataset_metadata(dataset)
    assert metadata1.dataset == dataset
    
    # Verify that get_metadata was called once and set_metadata_with_ttl was called once
    mock_get_metadata.assert_called_once_with(dataset, "latest")
    mock_set_metadata.assert_called_once()
    
    # Reset the mock for the second call
    mock_get_metadata.reset_mock()
    mock_set_metadata.reset_mock()
    
    # Second call: return cached data
    mock_get_metadata.return_value = f_dataset_metadata_response["data"]
    
    # Second call should use cached data (no additional API call)
    metadata2 = await client.get_dataset_metadata(dataset)
    assert metadata2.dataset == dataset
    
    # Verify that get_metadata was called again but set_metadata_with_ttl was not
    mock_get_metadata.assert_called_once_with(dataset, "latest")
    mock_set_metadata.assert_not_called()
    
    # Verify that the API was only called once (first call only)
    api_calls = [call for call in respx.calls if f"/dataset/{dataset}/latest" in str(call.request.url)]
    assert len(api_calls) == 1

